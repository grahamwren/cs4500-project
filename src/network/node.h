#pragma once

#include "cursor.h"
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

/**
 * a class which handles network communication for an application which extends
 * it
 * authors: @grahamwren @jagen31
 */
class Node : public Thread {
public:
  typedef function<optional<sized_ptr<uint8_t>>(IpV4Addr, ReadCursor &)>
      handler_fn_type;

protected:
  std::atomic<bool> should_continue;
  const IpV4Addr my_addr;
  ListenSock listen_s;
  shared_mutex peers_mtx;
  set<IpV4Addr> peers;
  handler_fn_type data_handler;

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
    if (ip != my_addr && peers.find(ip) == peers.end()) {
      peers.emplace(ip);

      cout << "New peer(" << ip << ") ";
      print_peers();

      /* notify peers of new peer */
      for (auto peer : peers) {
        /* skip new peer and self */
        if (peer == ip || peer == my_addr)
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
    int i = 0;
    for (auto peer : peers) {
      ips[i++] = peer;
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
    auto it = peers.find(a);
    /* if not found in peers list */
    if (it == peers.end()) {
      /* add peer */
      peers.emplace(a);

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
    auto res = data_handler(req.hdr.src_addr, rc);
    if (res.has_value()) {
      Packet ok_resp(my_addr, req.hdr.src_addr, PacketType::OK, res.value());
      sock.send_pkt(ok_resp);
    } else {
      Packet err_resp(my_addr, req.hdr.src_addr, PacketType::ERR);
      sock.send_pkt(err_resp);
    }
  }

  void print_peers() {
    cout << "Peers[";
    for (auto peer : peers)
      cout << peer << ", ";
    cout << ']' << endl;
  }

public:
  Node(IpV4Addr a)
      : should_continue(true), my_addr(a), listen_s(a),
        data_handler([](IpV4Addr src, ReadCursor &rc) {
          return sized_ptr<uint8_t>(0, nullptr);
        }) {
    peers.emplace(a);
    listen_s.listen();
  }

  void set_data_handler(handler_fn_type handler) { data_handler = handler; }

  const IpV4Addr &addr() { return my_addr; }

  void teardown() {
    shared_lock lock(peers_mtx);

    /* includes self */
    for (auto peer : peers) {
      Packet kill_pkt(my_addr, peer, PacketType::SHUTDOWN);
      const Packet resp = DataSock::fetch(kill_pkt);
      cout << "KILL Response: " << resp << endl;
      if (resp.hdr.type != PacketType::OK) {
        cout << "ERROR: failed to shutdown peer: " << peer << endl;
      }
    }
    cout << "killed cluster" << endl;
  }

  void register_with(const IpV4Addr &server_a) {
    assert(my_addr != server_a);
    unique_lock lock(peers_mtx);

    /* add server_a to peers */
    peers.emplace(server_a);

    /* try to register with server */
    Packet req(my_addr, server_a, PacketType::REGISTER);
    const Packet resp = DataSock::fetch(req);

    cout << "REG Response: " << resp << endl;
    assert(resp.hdr.type == PacketType::OK);

    IpV4Addr *ips = (IpV4Addr *)resp.data.ptr;
    int n_addrs = resp.hdr.data_len() / sizeof(IpV4Addr);
    peers.insert(ips, ips + n_addrs);
    print_peers();
  }

  /**
   * sorted set of ips in cluster, including self
   */
  const set<IpV4Addr> &cluster() {
    shared_lock lock(peers_mtx);
    return peers;
  }

  bool send_data_to_cluster(sized_ptr<uint8_t> data) {
    shared_lock lock(peers_mtx);
    bool success = true;
    for (auto peer : peers) {
      /* skip self */
      if (peer == my_addr)
        continue;

      Packet req(my_addr, peer, PacketType::DATA, data.len, data.ptr);
      const Packet resp = DataSock::fetch(req);
      cout << "Recv data resp: " << resp << endl;
      success = success && resp.ok();
    }
    return success;
  }

  ReadCursor send_data(const IpV4Addr &dest, sized_ptr<uint8_t> data) {
    Packet req(my_addr, dest, PacketType::DATA, data.len, data.ptr);
    Packet resp = DataSock::fetch(req);
    cout << "Recv data resp: " << resp << endl;
    return ReadCursor(resp.data);
  }
};
