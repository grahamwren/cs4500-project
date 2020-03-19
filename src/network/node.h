#pragma once

#include "../thread.h"
#include "command.h"
#include "packet.h"
#include "sock.h"
#include <iostream>
#include <vector>

using namespace std;

/**
 * a class which handles network communication for an application which extends
 * it
 * authors: @grahamwren @jagen31
 */
class Node : public Thread {
public:
  std::atomic<bool> should_continue;
  const IpV4Addr my_addr;
  const IpV4Addr server_addr;
  ListenSock listen_s;
  vector<IpV4Addr> peers;

  Node(IpV4Addr a)
      : should_continue(true), my_addr(a), server_addr(0), listen_s(a) {}
  Node(IpV4Addr a, IpV4Addr server_a)
      : should_continue(true), my_addr(a), server_addr(server_a), listen_s(a) {
    peers.push_back(server_a);
  }

  void run() {
    setup();
    while (should_continue) {
      cout << "Current peers: [";
      for (int i = 0; i < peers.size(); i++) {
        peers[i].print();
        cout << ", ";
      }
      cout << "]" << endl;

      DataSock ds = listen_s.accept_connection(); // move
      Packet *pkt = ds.get_pkt();
      handle_pkt(ds, *pkt);
      ds.close();
      delete pkt;
    }
  }

  void teardown() {
    for (int i = 0; i < peers.size(); i++) {
      Packet kill_pkt(my_addr, peers[i], PacketType::SHUTDOWN);
      Packet *resp = DataSock::fetch(kill_pkt);
      if (resp->hdr.type != PacketType::OK) {
        cout << "ERROR: failed to shutdown peer: ";
        peers[i].print();
        cout << endl;
      }
      delete resp;
    }
    // process done
    exit(0);
  }

  void setup() {
    while (listen_s.listen() == -1) {
      cout << "failed" << endl;
      sleep(100);
    }

    // if has a server_addr, register with server
    if (server_addr != 0) {
      Packet req(my_addr, server_addr, PacketType::REGISTER);
      Packet *resp = DataSock::fetch(req);
      if (resp->hdr.type != PacketType::OK) {
        cout << "ERROR: Register req from ";
        my_addr.print();
        cout << " to ";
        server_addr.print();
        cout << " failed with ";
        PacketHeader::print_type(resp->hdr.type);
        cout << endl;
      }
      IpV4Addr *ips = (IpV4Addr *)resp->data;
      int n_addrs = resp->hdr.data_len() / sizeof(IpV4Addr);
      peers.reserve(peers.size() + n_addrs);
      for (int i = 0; i < n_addrs; i++) {
        peers.push_back(ips[i]);
      }
      delete resp;
    }
  }

  void handle_pkt(DataSock &sock, Packet &pkt) {
    switch (pkt.hdr.type) {
    case PacketType::REGISTER:
      handle_register_pkt(sock, pkt);
      break;
    case PacketType::NEW_PEER:
      handle_new_peer_pkt(sock, pkt);
      break;
    case PacketType::DATA:
      handle_data_pkt(sock, pkt);
      break;
    case PacketType::HELLO:
      handle_hello_pkt(sock, pkt);
      break;
    case PacketType::SHUTDOWN:
      handle_shutdown_pkt(sock, pkt);
      break;
    default:
      assert(false); // fail if missing type
    }
  }

  void handle_register_pkt(DataSock &sock, Packet &req) {
    const IpV4Addr &src = req.hdr.src_addr;
    int n_peers = peers.size();
    IpV4Addr ips[n_peers];
    for (int i = 0; i < n_peers; i++) {
      ips[i] = peers[i];
    }
    Packet resp(my_addr, src, PacketType::OK, sizeof(IpV4Addr) * n_peers,
                (unsigned char *)ips);
    sock.send_pkt(resp);

    auto it = find(peers.begin(), peers.end(), src);
    /* if not found in peers list */
    if (it == peers.end()) {
      /* notify peers of new peer */
      for (int i = 0; i < n_peers; i++) {
        IpV4Addr &peer_a = peers[i];
        Packet new_peer_req(my_addr, peer_a, PacketType::NEW_PEER,
                            sizeof(IpV4Addr), (unsigned char *)&src);
        Packet *new_peer_resp = DataSock::fetch(new_peer_req);
        if (new_peer_resp->hdr.type != PacketType::OK) {
          cout << "ERROR: New peer req from ";
          my_addr.print();
          cout << " to ";
          peer_a.print();
          cout << " failed with ";
          PacketHeader::print_type(new_peer_resp->hdr.type);
          cout << endl;
        }
        delete new_peer_resp;
      }
      /* add new peer to peers list */
      peers.push_back(const_cast<IpV4Addr &>(src));
    }
  }

  void handle_new_peer_pkt(DataSock &sock, Packet &req) {
    /* assume data is an IpV4Addr */
    assert(req.data);
    assert(req.hdr.data_len() == sizeof(IpV4Addr));
    IpV4Addr &a = *(IpV4Addr *)req.data;
    auto it = find(peers.begin(), peers.end(), a);
    /* if not found in peers list */
    if (it == peers.end()) {
      peers.push_back(a);
      /* say hello */
      Packet hello_pkt(my_addr, a, PacketType::HELLO);
      Packet *resp = DataSock::fetch(hello_pkt);
      assert(resp->hdr.type == PacketType::OK);
      delete resp;
    }
    Packet resp(my_addr, req.hdr.src_addr, PacketType::OK);
    sock.send_pkt(resp);
  }

  void handle_shutdown_pkt(DataSock &sock, Packet &req) {
    Packet ok_resp(my_addr, req.hdr.src_addr, PacketType::OK);
    sock.send_pkt(ok_resp);
    should_continue = false;
  }

  void handle_hello_pkt(DataSock &sock, Packet &req) {
    Packet ok_resp(my_addr, req.hdr.src_addr, PacketType::OK);
    sock.send_pkt(ok_resp);
  }

  void handle_data_pkt(DataSock &sock, Packet &pkt) {
    Command *cmd = new Command(pkt.hdr.data_len(), pkt.data);
    handle_cmd(cmd);
  }

  /**
   * method to be overriden by children to handle commands
   */
  virtual void handle_cmd(Command *cmd) { delete cmd; }
};
