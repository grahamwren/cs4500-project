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
  mutable shared_mutex peers_mtx;
  set<IpV4Addr> peers;
  handler_fn_type data_handler;

  void run() {
    while (should_continue) {
      cout << "waiting for packet..." << endl;
      const DataSock ds = listen_s.accept_connection();
      const Packet pkt = ds.get_pkt();
      cout << "Node.asyncRecv(" << pkt << ")" << endl;
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

        sized_ptr<uint8_t> d(sizeof(IpV4Addr), (uint8_t *)&ip);
        Packet req(my_addr, peer, PacketType::NEW_PEER, d);
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

  void handle_register_pkt(const DataSock &sock, const Packet &req) const {
    shared_lock lock(peers_mtx);
    const IpV4Addr &src = req.hdr.src_addr;
    const int n_peers = peers.size();
    IpV4Addr ips[n_peers];
    int i = 0;
    for (auto peer : peers) {
      ips[i++] = peer;
    }
    lock.unlock();

    sized_ptr<uint8_t> d(sizeof(IpV4Addr) * n_peers, (uint8_t *)ips);
    Packet resp(my_addr, src, PacketType::OK, d);
    sock.send_pkt(resp);
  }

  void handle_new_peer_pkt(const DataSock &sock, const Packet &req) {
    /* assume data is an IpV4Addr */
    assert(req.data.ptr());
    assert(req.hdr.data_len() == sizeof(IpV4Addr));

    Packet resp(my_addr, req.hdr.src_addr, PacketType::OK);
    sock.send_pkt(resp);

    const IpV4Addr &a = *(IpV4Addr *)req.data.ptr();

    /* say hello */
    Packet hello(my_addr, a, PacketType::HELLO);
    Packet h_resp = DataSock::fetch(hello);
    assert(h_resp.ok());

    unique_lock lock(peers_mtx);
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

  void handle_hello_pkt(const DataSock &sock, const Packet &req) const {
    Packet ok_resp(my_addr, req.hdr.src_addr, PacketType::OK);
    sock.send_pkt(ok_resp);
  }

  void handle_data_pkt(const DataSock &sock, const Packet &req) const {
    ReadCursor rc(req.data.data());
    auto res = data_handler(req.hdr.src_addr, rc);
    if (res.has_value()) {
      Packet ok_resp(my_addr, req.hdr.src_addr, PacketType::OK, res.value());
      sock.send_pkt(ok_resp);
    } else {
      Packet err_resp(my_addr, req.hdr.src_addr, PacketType::ERR);
      sock.send_pkt(err_resp);
    }
  }

  void print_peers() const {
    cout << "Peers[";
    auto it = peers.begin();
    if (it != peers.end())
      cout << *(it++);

    for (; it != peers.end(); it++)
      cout << ", " << *it;
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

  const IpV4Addr &addr() const { return my_addr; }

  void teardown() {
    shared_lock lock(peers_mtx);

    /* includes self */
    for (auto peer : peers) {
      Packet kill_pkt(my_addr, peer, PacketType::SHUTDOWN);
      const Packet resp = DataSock::fetch(kill_pkt);
      cout << "Node.recv(" << resp << ")" << endl;
      if (resp.hdr.type != PacketType::OK) {
        cout << "ERROR: failed to shutdown peer: " << peer << endl;
      }
    }
    cout << "killed cluster" << endl;
  }

  void register_with(const IpV4Addr &server_a) {
    // assert(my_addr != server_a);
    unique_lock lock(peers_mtx, defer_lock);

    /* add server_a to peers */
    lock.lock();
    peers.emplace(server_a);
    lock.unlock();

    /* try to register with server */
    Packet req(my_addr, server_a, PacketType::REGISTER);
    const Packet resp = DataSock::fetch(req);

    cout << "Node.recv(" << resp << ")" << endl;
    assert(resp.hdr.type == PacketType::OK);

    IpV4Addr *ips = (IpV4Addr *)resp.data.ptr();
    int n_addrs = resp.hdr.data_len() / sizeof(IpV4Addr);
    lock.lock();
    peers.insert(ips, ips + n_addrs);
    lock.unlock();
    print_peers();
  }

  /**
   * sorted set of ips in cluster, including self
   */
  const set<IpV4Addr> &cluster() const { return peers; }

  bool send_cmd_to_cluster(const Command &cmd) const {
    WriteCursor wc;
    cmd.serialize(wc);

    shared_lock lock(peers_mtx);
    /* allocate storage for placement-new */
    uint8_t storage[peers.size() * sizeof(Packet)]; // approx 1 packet per peer
    Packet *pkts_to_send = reinterpret_cast<Packet *>(storage);
    int num_pkts = 0;
    for (IpV4Addr peer : peers) {
      /* skip self */
      if (peer != my_addr) {
        /* construct Packet in-place in allocated storage */
        new (pkts_to_send + num_pkts++)
            Packet(my_addr, peer, PacketType::DATA, wc);
      }
    }
    lock.unlock(); // try to avoid locking while doing networking

    bool success = true;
    for (int i = 0; i < num_pkts; i++) {
      const Packet resp = DataSock::fetch(pkts_to_send[i]);
      cout << "Node.recv(" << resp << ")" << endl;
      success = success && resp.ok();
    }
    return success;
  }

  DataChunk send_cmd(const IpV4Addr &dest, const Command &cmd) const {
    WriteCursor wc;
    cmd.serialize(wc);
    Packet req(my_addr, dest, PacketType::DATA, wc);
    Packet resp = DataSock::fetch(req);
    cout << "Node.recv(" << resp << ")" << endl;
    return DataChunk(move(resp.data));
  }
};
