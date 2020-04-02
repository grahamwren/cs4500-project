#pragma once

#include "chunk_key.h"
#include "command.h"
#include "data_chunk.h"
#include "lru_cache.h"
#include "network/node.h"
#include <iostream>
#include <shared_mutex>
#include <unordered_map>

using namespace std;

#ifndef KV_LOG
#define KV_LOG true
#endif

class KV {
private:
  Node node;

  mutable shared_mutex om_mtx;
  unordered_map<string, IpV4Addr> ownership_mapping;

  /**
   * get the owner for this named data
   * Uses a shared_lock on om_mtx
   */
  optional<IpV4Addr> get_owner(const string &name) const {
    shared_lock lock(om_mtx);
    auto it = ownership_mapping.find(name);
    if (it == ownership_mapping.end()) {
      return nullopt;
    } else {
      return it->second;
    }
  }

  /**
   * set the owner for this named data
   * Uses a unique_lock on om_mtx
   */
  void put_owner(const string &name, IpV4Addr owner) {
    unique_lock lock(om_mtx);
    ownership_mapping.emplace(name, owner);
  }

  mutable shared_mutex data_mtx;
  unordered_map<ChunkKey, shared_ptr<DataChunk>> data;

  /**
   * get data chunk for this key
   * Uses shared_lock on data_mtx
   */
  weak_ptr<DataChunk> get_local_data(const ChunkKey &key) const {
    shared_lock lock(data_mtx);
    return weak_ptr<DataChunk>(data.at(key));
  }

  weak_ptr<DataChunk> get_network_data(const ChunkKey &key) const {

    optional<IpV4Addr> addr = find_peer_for_chunk(key);
    assert(addr);

    return cache.fetch(key, [&,this](const ChunkKey &key){
      Command get_cmd(key);
      if (KV_LOG)
        cout << "KV.send(" << *addr << ", " << get_cmd << ")" << endl;
      return node.send_cmd(*addr, get_cmd);
    });
  }

  /**
   * put data chunk for this key
   * Uses unique_lock on data_mtx
   */
  void put_data(const ChunkKey key, unique_ptr<DataChunk> &&data_up) {
    unique_lock lock(data_mtx);
    data.emplace(key, shared_ptr<DataChunk>(move(data_up)));
  }

  mutable LRUCache<ChunkKey, DataChunk> cache;

protected:
  /**
   * invoked by node, in seperate thread
   */
  optional<sized_ptr<uint8_t>> handler(IpV4Addr src, Command &c) {
    if (KV_LOG)
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
    shared_ptr<DataChunk> dc = get_local_data(c.key).lock();
    return dc->data();
  }
  optional<sized_ptr<uint8_t>> handle_put_cmd(IpV4Addr src, Command &c) {
    put_data(c.key, move(c.data));
    return sized_ptr<uint8_t>(0, nullptr);
  }
  optional<sized_ptr<uint8_t>> handle_own_cmd(IpV4Addr src, const Command &c) {
    if (ownership_mapping.find(c.name) != ownership_mapping.end() &&
        ownership_mapping.at(c.name) != src) {
      cout << "ERROR: node " << src << " claimed ownership for DF "
           << c.name.c_str() << " already owned by "
           << ownership_mapping.at(c.name) << endl;
      return nullopt; // node returns an PacketType::ERR
    }
    return sized_ptr<uint8_t>(0, nullptr);
  }

  /**
   * return the peer IP for this chunk.
   */
  optional<IpV4Addr> find_peer_for_chunk(const ChunkKey &ck) const {
    const set<IpV4Addr> &cluster = node.cluster();
    auto owner_ip = get_owner(ck.name);
    if (owner_ip) {
      auto it = cluster.find(*owner_ip);
      int offset = ck.chunk_idx % cluster.size();
      /* TODO: use 🤷 to make this O(1) */
      for (int i = 0; i < offset; i++) {
        it++;
        /* if off the end, reset to start */
        if (it == cluster.end())
          it = cluster.begin();
      }
      return *it;
    } else {
      return nullopt;
    }
  }

public:
  KV(const IpV4Addr &a) : node(a) {
    node.set_data_handler([&](IpV4Addr src, ReadCursor &c) {
      Command cmd(c);
      return this->handler(src, cmd);
    });
    node.start();
  }
  KV(const IpV4Addr &a, const IpV4Addr &server_a) : KV(a) {
    node.register_with(server_a);
  }
  ~KV() { node.join(); }

  /**
   * application interface
   */
  weak_ptr<DataChunk> get(const ChunkKey &k) const {
    if (KV_LOG)
      cout << "KV.get(" << k << ")" << endl;

    return is_local(k) ? get_local_data(k) : get_network_data(k);
  }

  void put(const ChunkKey &k, unique_ptr<DataChunk> &&dc) {
    if (KV_LOG)
      cout << "KV.put(" << k << ", " << *dc << ")" << endl;
    /* check if someone already owns this DF */
    optional<IpV4Addr> addr = find_peer_for_chunk(k);
    /* if not, claim ownership */
    if (!addr) {
      addr = node.addr();
      put_owner(k.name, node.addr());
      Command own_cmd(k.name);
      if (KV_LOG)
        cout << "KV.send(all, " << own_cmd << ")" << endl;

      WriteCursor wc;
      own_cmd.serialize(wc);
      bool res = node.send_cmd_to_cluster(own_cmd);
      assert(res);
    }

    /* if this node owns the chunk */
    if (*addr == node.addr()) {
      put_data(k, move(dc));
    } else {
      Command put_cmd(k, move(dc));
      if (KV_LOG)
        cout << "KV.send(" << *addr << ", " << put_cmd << ")" << endl;
      node.send_cmd(*addr, put_cmd);
    }
  }

  bool is_local(ChunkKey k) const {
    return data.find(k) != data.end();
  }

  void teardown() { node.teardown(); }
};
