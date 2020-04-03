#pragma once

#include "cursor.h"
#include "kv/command.h"
#include "packet.h"
#include "sized_ptr.h"
#include "sock.h"
#include "thread.h"
#include <algorithm>
#include <cstring>
#include <iostream>
#include <optional>
#include <set>
#include <shared_mutex>

using namespace std;

#ifndef NODE_LOG
#define NODE_LOG true
#endif

/**
 * a class which handles network communication
 * authors: @grahamwren @jagen31
 */
class Node {
public:
  typedef function<bool(const IpV4Addr &, ReadCursor &, WriteCursor &)>
      handler_fn_t;

protected:
  bool should_continue;
  const IpV4Addr my_addr;
  ListenSock listen_s;
  set<IpV4Addr> peers;
  handler_fn_t data_handler;

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
    case PacketType::GET_PEERS:
      handle_get_peers_pkt(sock, pkt);
      break;
    case PacketType::SHUTDOWN:
      handle_shutdown_pkt(sock, pkt);
      break;
    default:
      assert(false); // fail if missing type
    }
  }

  void handle_get_peers_pkt(const DataSock &sock, const Packet &req) {
    const IpV4Addr &src = req.hdr.src_addr;
    const int n_peers = peers.size();
    IpV4Addr ips[n_peers];
    int i = 0;
    for (auto peer : peers) {
      ips[i++] = peer;
    }

    sized_ptr<uint8_t> resp_d(sizeof(IpV4Addr) * n_peers, (uint8_t *)ips);
    Packet resp(my_addr, src, PacketType::OK, resp_d);
    sock.send_pkt(resp);
  }

  void handle_register_pkt(const DataSock &sock, const Packet &req) {
    const IpV4Addr &src = req.hdr.src_addr;
    const int n_peers = peers.size();
    IpV4Addr ips[n_peers];
    int i = 0;
    for (auto peer : peers) {
      ips[i++] = peer;
    }

    sized_ptr<uint8_t> resp_d(sizeof(IpV4Addr) * n_peers, (uint8_t *)ips);
    Packet resp(my_addr, src, PacketType::OK, resp_d);
    sock.send_pkt(resp);

    bool seen_peer = src == my_addr || peers.find(src) != peers.end();
    if (seen_peer)
      return;

    peers.emplace(src);

    if (NODE_LOG) {
      cout << "New peer(" << src << ") ";
      print_peers();
    }

    /* notify peers of new peer */
    for (IpV4Addr peer : peers) {
      if (peer != my_addr && peer != src) {
        sized_ptr<uint8_t> d(sizeof(IpV4Addr), (uint8_t *)&src);
        Packet pkt(my_addr, peer, PacketType::NEW_PEER, d);
        const Packet resp = DataSock::fetch(pkt);
        assert(resp.ok());
      }
    }
  }

  void handle_new_peer_pkt(const DataSock &sock, const Packet &req) {
    /* assume data is an IpV4Addr */
    assert(req.data->ptr());
    assert(req.hdr.data_len() == sizeof(IpV4Addr));

    Packet resp(my_addr, req.hdr.src_addr, PacketType::OK);
    sock.send_pkt(resp);

    const IpV4Addr &a = *(IpV4Addr *)req.data->ptr();
    /* if my_addr then ignore */
    if (a == my_addr)
      return;

    auto it = peers.find(a);
    /* if not found in peers list */
    if (it == peers.end()) {
      /* add peer */
      peers.emplace(a);
      print_peers();
    } else {
      cout << "ERROR: already seen peer " << a << endl;
    }
  }

  void handle_shutdown_pkt(const DataSock &sock, const Packet &req) {
    Packet ok_resp(my_addr, req.hdr.src_addr, PacketType::OK);
    sock.send_pkt(ok_resp);
    should_continue = false;
  }

  void handle_data_pkt(const DataSock &sock, const Packet &req) const {
    ReadCursor rc(req.data->data());
    WriteCursor wc;
    if (data_handler(req.hdr.src_addr, rc, wc)) {
      Packet ok_resp(my_addr, req.hdr.src_addr, PacketType::OK,
                     sized_ptr<uint8_t>(wc.length(), wc.bytes()));
      sock.send_pkt(ok_resp);
    } else {
      Packet err_resp(my_addr, req.hdr.src_addr, PacketType::ERR,
                      sized_ptr<uint8_t>(wc.length(), wc.bytes()));
      sock.send_pkt(err_resp);
    }
  }

  void register_with(const IpV4Addr &server_a) {
    /* add server_a to peers */
    peers.emplace(server_a);

    /* try to register with server */
    Packet req(my_addr, server_a, PacketType::REGISTER);
    const Packet resp = DataSock::fetch(req);
    if (NODE_LOG)
      cout << "Node.recv(" << resp << ")" << endl;

    assert(resp.hdr.type == PacketType::OK);

    IpV4Addr *ips = (IpV4Addr *)resp.data->ptr();
    int n_addrs = resp.hdr.data_len() / sizeof(IpV4Addr);
    peers.insert(ips, ips + n_addrs);
    print_peers();
 }

  void print_peers() const {
    if (!NODE_LOG)
      return;
    cout << "Peers[";
    auto it = peers.begin();
    if (it != peers.end())
      cout << *(it++);

    for (; it != peers.end(); it++)
      cout << ", " << *it;
    cout << ']' << endl;
  }

public:
  Node(const IpV4Addr &a)
      : should_continue(true), my_addr(a), listen_s(a),
        data_handler([](IpV4Addr src, ReadCursor &rc, WriteCursor &wc) {
          return true;
        }) {
    peers.emplace(a);
    listen_s.listen();
  }
  Node(const IpV4Addr &a, const IpV4Addr &server_a) : Node(a) {
    register_with(server_a);
  }

  void set_data_handler(handler_fn_t handler) { data_handler = handler; }

  const IpV4Addr &addr() const { return my_addr; }

  void start() {
    while (should_continue) {
      if (NODE_LOG)
        cout << "waiting for packet..." << endl;
      const DataSock ds = listen_s.accept_connection();
      const Packet pkt = ds.get_pkt();
      if (NODE_LOG)
        cout << "Node.asyncRecv(" << pkt << ")" << endl;
      handle_pkt(ds, pkt);
    }
  }
};
