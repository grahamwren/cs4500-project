#pragma once

#include "chunk_key.h"
#include "command.h"
#include "data_chunk.h"
#include "network/node.h"
#include <iostream>
#include <map>
#include <shared_mutex>

using namespace std;

class KV {
private:
  Node node;

  shared_mutex om_mtx;
  map<string, IpV4Addr> ownership_mapping;

  shared_mutex data_mtx;
  map<ChunkKey, DataChunk> data;

protected:
  /**
   * invoked by node, in seperate thread
   */
  optional<sized_ptr<uint8_t>> handler(IpV4Addr src, const Command &c) {
    cout << "KV.asyncRecv(" << src << ", " << c << ")" << endl;
    switch (c.type) {
    case Command::Type::GET:
      return handle_get_cmd(src, c);
    case Command::Type::PUT:
      return handle_put_cmd(src, c);
    case Command::Type::OWN:
      return handle_own_cmd(src, c);
    }
  }

  optional<sized_ptr<uint8_t>> handle_get_cmd(IpV4Addr src, const Command &c) {
    shared_lock lock(data_mtx);
    if (data.find(c.key) != data.end()) {
      return data.at(c.key);
    } else {
      return nullopt;
    }
  }
  optional<sized_ptr<uint8_t>> handle_put_cmd(IpV4Addr src, const Command &c) {
    unique_lock lock(data_mtx);
    data.emplace(make_pair(c.key, c.data));
    return sized_ptr<uint8_t>(0, nullptr);
  }
  optional<sized_ptr<uint8_t>> handle_own_cmd(IpV4Addr src, const Command &c) {
    unique_lock lock(om_mtx);
    if (ownership_mapping.find(c.name) != ownership_mapping.end() &&
        ownership_mapping.at(c.name) != src) {
      cout << "ERROR: node " << src << " claimed ownership for DF "
           << c.name.c_str() << " already owned by "
           << ownership_mapping.at(c.name) << endl;
    }
    ownership_mapping.emplace(make_pair(c.name, src));
    return sized_ptr<uint8_t>(0, nullptr);
  }

  /**
   * return the peer IP for this chunk.
   * Assumes thread already owns shared_lock(om_mtx)
   */
  optional<IpV4Addr> find_peer_for_chunk(const ChunkKey &ck) const {
    auto owner = ownership_mapping.find(ck.name);
    if (owner == ownership_mapping.end()) {
      return nullopt;
    }
    IpV4Addr owner_ip = ::get<1>(*owner);
    const set<IpV4Addr> &cluster = node.cluster();
    auto peer = cluster.find(owner_ip);
    int offset = ck.chunk_idx % cluster.size();
    /* TODO: use modulo to make this O(1) */
    for (int i = 0; i < offset; i++) {
      peer++;
      /* if off the end, reset to start */
      if (peer == cluster.end())
        peer = cluster.begin();
    }
    return *peer;
  }

public:
  KV(IpV4Addr a) : node(a) {
    node.set_data_handler([&](IpV4Addr src, ReadCursor &c) {
      return this->handler(src, Command::unpack(c));
    });
    node.start();
  }
  KV(IpV4Addr a, IpV4Addr server_a) : KV(a) { node.register_with(server_a); }
  ~KV() { node.join(); }

  /**
   * application interface
   */
  DataChunk get(const ChunkKey &k) {
    cout << "KV.get(" << k << ")" << endl;
    shared_lock olock(om_mtx);
    /* find peer who owns this DF */
    optional<IpV4Addr> addr = find_peer_for_chunk(k);
    /* assert addr found */
    assert(addr);

    /* if this node owns the chunk */
    if (*addr == node.addr()) {
      shared_lock dlock(data_mtx);
      cout << "returning " << k << " => " << data.at(k) << endl;
      return data.at(k);
    } else {
      Command get_cmd(k);
      WriteCursor wc;
      get_cmd.serialize(wc);
      cout << "KV.send(" << *addr << ", " << get_cmd << ")" << endl;
      return node.send_data(*addr, wc);
    }
  }

  void put(const ChunkKey &k, const DataChunk &dc) {
    cout << "KV.put(" << k << ", " << dc << ")" << endl;
    unique_lock olock(om_mtx);
    /* check if someone already owns this DF */
    optional<IpV4Addr> addr = find_peer_for_chunk(k);
    /* if not, claim ownership */
    if (!addr) {
      addr = node.addr();
      ownership_mapping.emplace(make_pair(k.name, node.addr()));
      Command own_cmd(k.name);
      WriteCursor wc;
      own_cmd.serialize(wc);
      cout << "KV.send(all, " << own_cmd << ")" << endl;
      bool res = node.send_data_to_cluster(wc);
      assert(res);
    }

    /* if this node owns the chunk */
    if (*addr == node.addr()) {
      unique_lock dlock(data_mtx);
      cout << "storing " << k << " => " << dc << endl;
      data.emplace(make_pair(k, dc));
    } else {
      Command put_cmd(k, dc);
      WriteCursor wc;
      put_cmd.serialize(wc);
      cout << "KV.send(" << *addr << ", " << put_cmd << ")" << endl;
      node.send_data(*addr, wc);
    }
  }

  void teardown() { node.teardown(); }
};
