#pragma once

#include "cursor.h"
#include "packet.h"
#include "sized_ptr.h"
#include "sock.h"
#include "thread.h"
#include <algorithm>
#include <cstring>
#include <iostream>
#include <shared_mutex>
#include <vector>

using namespace std;

/**
 * a class which handles network communication for an application which extends
 * it
 * authors: @grahamwren @jagen31
 */
class Node : public Thread {
protected:
  std::atomic<bool> should_continue;
  const IpV4Addr my_addr;
  ListenSock listen_s;
  shared_mutex peers_mtx;
  vector<IpV4Addr> peers;
  function<bool(ReadCursor &)> data_handler;

  void run() {
    while (should_continue) {
      cout << "waiting for packet..." << endl;
      const DataSock ds = listen_s.accept_connection();
      const Packet pkt = ds.get_pkt();
      cout << "Async: received packet: " << pkt << endl;
      handle_pkt(ds, pkt);
      check_if_new_peer(pkt.hdr.src_addr);
    }
  }

  bool check_if_new_peer(const IpV4Addr &ip) {
    /* acquire write lock */
    unique_lock lock(peers_mtx);

    /* if not me and not in peers list */
    if (ip != my_addr && find(peers.begin(), peers.end(), ip) == peers.end()) {
      peers.emplace_back(ip);

      cout << "New peer(" << ip << ") ";
      print_peers();

      /* notify peers of new peer */
      for (auto peer : peers) {
        /* skip new peer */
        if (peer == ip)
          continue;

        Packet req(my_addr, peer, PacketType::NEW_PEER, sizeof(IpV4Addr),
                   (uint8_t *)&ip);
        const Packet resp = DataSock::fetch(req);
        assert(resp.hdr.type == PacketType::OK);
      }
      return true;
    }
    return false;
  }

  void handle_pkt(const DataSock &sock, const Packet &pkt) {
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

  void handle_register_pkt(const DataSock &sock, const Packet &req) {
    shared_lock lock(peers_mtx);
    const IpV4Addr &src = req.hdr.src_addr;
    const int n_peers = peers.size();
    IpV4Addr ips[n_peers];
    for (int i = 0; i < n_peers; i++) {
      ips[i] = peers[i];
    }
    Packet resp(my_addr, src, PacketType::OK, sizeof(IpV4Addr) * n_peers,
                (uint8_t *)ips);
    sock.send_pkt(resp);
  }

  void handle_new_peer_pkt(const DataSock &sock, const Packet &req) {
    unique_lock lock(peers_mtx);
    /* assume data is an IpV4Addr */
    assert(req.data.ptr);
    assert(req.hdr.data_len() == sizeof(IpV4Addr));

    Packet resp(my_addr, req.hdr.src_addr, PacketType::OK);
    sock.send_pkt(resp);

    const IpV4Addr &a = *(IpV4Addr *)req.data.ptr;
    auto it = find(peers.begin(), peers.end(), a);
    /* if not found in peers list */
    if (it == peers.end()) {
      /* add peer */
      peers.emplace_back(a);

      /* say hello */
      Packet hello_pkt(my_addr, a, PacketType::HELLO);
      const Packet resp = DataSock::fetch(hello_pkt);
      cout << "HELLO Response: " << resp << endl;
      assert(resp.hdr.type == PacketType::OK);
    } else {
      cout << "Already seen peer: " << a << endl;
    }
  }

  void handle_shutdown_pkt(const DataSock &sock, const Packet &req) {
    Packet ok_resp(my_addr, req.hdr.src_addr, PacketType::OK);
    sock.send_pkt(ok_resp);
    should_continue = false;
  }

  void handle_hello_pkt(const DataSock &sock, const Packet &req) const {
    Packet ok_resp(my_addr, req.hdr.src_addr, PacketType::OK);
    sock.send_pkt(ok_resp);
  }

  void handle_data_pkt(const DataSock &sock, const Packet &req) const {
    ReadCursor rc(req.data);
    if (data_handler(rc)) {
      Packet ok_resp(my_addr, req.hdr.src_addr, PacketType::OK);
      sock.send_pkt(ok_resp);
    } else {
      Packet err_resp(my_addr, req.hdr.src_addr, PacketType::ERR);
      sock.send_pkt(err_resp);
    }
  }

  void print_peers() {
    cout << "Peers[";
    if (peers.size() > 0)
      cout << peers[0];
    for (int i = 1; i < peers.size(); i++)
      cout << ", " << peers[i];
    cout << ']' << endl;
  }

public:
  Node(IpV4Addr a)
      : should_continue(true), my_addr(a), listen_s(a),
        data_handler([](ReadCursor &rc) { return true; }) {
    listen_s.listen();
  }

  void set_data_handler(function<bool(ReadCursor &)> handler) {
    data_handler = handler;
  }

  const IpV4Addr &addr() { return my_addr; }

  void teardown() {
    shared_lock lock(peers_mtx);
    for (int i = 0; i < peers.size(); i++) {
      Packet kill_pkt(my_addr, peers[i], PacketType::SHUTDOWN);
      const Packet resp = DataSock::fetch(kill_pkt);
      cout << "KILL Response: " << resp << endl;
      if (resp.hdr.type != PacketType::OK) {
        cout << "ERROR: failed to shutdown peer: " << peers[i] << endl;
      }
    }
    cout << "killed cluster, killing self" << endl;

    Packet kill_pkt(my_addr, my_addr, PacketType::SHUTDOWN);
    const Packet resp = DataSock::fetch(kill_pkt);
    cout << "KILL Response: " << resp << endl;
    if (resp.hdr.type != PacketType::OK) {
      cout << "ERROR: failed to kill self" << endl;
    }
  }

  void register_with(const IpV4Addr &server_a) {
    unique_lock lock(peers_mtx);

    /* add server_a to peers */
    peers.emplace_back(server_a);

    /* try to register with server */
    Packet req(my_addr, server_a, PacketType::REGISTER);
    const Packet resp = DataSock::fetch(req);

    cout << "REG Response: " << resp << endl;
    assert(resp.hdr.type == PacketType::OK);

    IpV4Addr *ips = (IpV4Addr *)resp.data.ptr;
    int n_addrs = resp.hdr.data_len() / sizeof(IpV4Addr);
    peers.insert(peers.end(), ips, ips + n_addrs);
    print_peers();
  }

  const vector<IpV4Addr> &cluster() {
    shared_lock lock(peers_mtx);
    return peers;
  }

  ReadCursor send_data(const IpV4Addr &dest, sized_ptr<uint8_t> data) {
    Packet req(my_addr, dest, PacketType::DATA, data.len, data.ptr);
    Packet resp = DataSock::fetch(req);
    cout << "Recv data resp: " << resp << endl;
    return ReadCursor(resp.data);
  }
};
