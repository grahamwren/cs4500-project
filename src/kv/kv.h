#pragma once

#include "chunk_key.h"
#include "command.h"
#include "data_cache.h"
#include "data_chunk.h"
#include "network/node.h"
#include <iostream>
#include <map>
#include <shared_mutex>

using namespace std;

class KV {
private:
  Node node;

  mutable shared_mutex om_mtx;
  map<string, IpV4Addr> ownership_mapping;

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
    ownership_mapping.emplace(make_pair(name, owner));
  }

  mutable shared_mutex data_mtx;
  map<ChunkKey, DataChunk> data;

  /**
   * get data chunk for this key
   * Uses shared_lock on data_mtx
   */
  optional<reference_wrapper<const DataChunk>>
  get_data(const ChunkKey &key) const {
    shared_lock lock(data_mtx);
    auto it = data.find(key);
    if (it == data.end()) {
      return nullopt;
    } else {
      return it->second;
    }
  }

  /**
   * put data chunk for this key
   * Uses unique_lock on data_mtx
   */
  void put_data(const ChunkKey key, sized_ptr<uint8_t> dc) {
    unique_lock lock(data_mtx);
    data.emplace(key, dc);
  }

  mutable DataCache data_cache;

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
    auto dc = get_data(c.key);
    if (dc) {
      return dc->get().data();
    } else {
      return nullopt;
    }
  }
  optional<sized_ptr<uint8_t>> handle_put_cmd(IpV4Addr src, const Command &c) {
    put_data(c.key, c.data.data());
    return sized_ptr<uint8_t>(0, nullptr);
  }
  optional<sized_ptr<uint8_t>> handle_own_cmd(IpV4Addr src, const Command &c) {
    if (ownership_mapping.find(c.name) != ownership_mapping.end() &&
        ownership_mapping.at(c.name) != src) {
      cout << "ERROR: node " << src << " claimed ownership for DF "
           << c.name.c_str() << " already owned by "
           << ownership_mapping.at(c.name) << endl;
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
  KV(IpV4Addr a) : node(a) {
    node.set_data_handler([&](IpV4Addr src, ReadCursor &c) {
      Command cmd(c);
      return this->handler(src, cmd);
    });
    node.start();
  }
  KV(IpV4Addr a, IpV4Addr server_a) : KV(a) { node.register_with(server_a); }
  ~KV() { node.join(); }

  /**
   * application interface
   */
  const DataChunk &get(const ChunkKey &k) const {
    cout << "KV.get(" << k << ")" << endl;
    if (data_cache.has_key(k)) {
      cout << "KV.cache_hit(" << k << ", " << data_cache.get(k) << ")" << endl;
      return data_cache.get(k);
    }

    /* find peer who owns this DF */
    optional<IpV4Addr> addr = find_peer_for_chunk(k);
    /* assert addr found */
    assert(addr);

    /* if this node owns the chunk */
    if (*addr == node.addr()) {
      auto chunk = get_data(k);
      assert(chunk);
      return chunk->get();
    } else {
      Command get_cmd(k);
      cout << "KV.send(" << *addr << ", " << get_cmd << ")" << endl;
      data_cache.insert(k, node.send_cmd(*addr, get_cmd));
      return data_cache.get(k);
    }
  }

  void put(const ChunkKey &k, const DataChunk &dc) {
    cout << "KV.put(" << k << ", " << dc << ")" << endl;
    /* check if someone already owns this DF */
    optional<IpV4Addr> addr = find_peer_for_chunk(k);
    /* if not, claim ownership */
    if (!addr) {
      addr = node.addr();
      put_owner(k.name, node.addr());
      Command own_cmd(k.name);
      cout << "KV.send(all, " << own_cmd << ")" << endl;

      WriteCursor wc;
      own_cmd.serialize(wc);
      bool res = node.send_cmd_to_cluster(own_cmd);
      assert(res);
    }

    /* if this node owns the chunk */
    if (*addr == node.addr()) {
      put_data(k, dc.data());
    } else {
      Command put_cmd(k, dc);
      cout << "KV.send(" << *addr << ", " << put_cmd << ")" << endl;
      node.send_cmd(*addr, put_cmd);
    }
  }

  void teardown() { node.teardown(); }
};
