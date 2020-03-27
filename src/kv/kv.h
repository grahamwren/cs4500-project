#pragma once

#include "chunk_key.h"
#include "command.h"
#include "data_chunk.h"
#include "network/node.h"
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
    switch (c.type) {
    case Command::Type::GET:
      return handle_get_cmd(c);
    case Command::Type::PUT:
      return handle_put_cmd(c);
    case Command::Type::OWN:
      return handle_own_cmd(src, c);
    }
  }

  optional<sized_ptr<uint8_t>> handle_get_cmd(const Command &c) {
    shared_lock lock(data_mtx);
    if (data.find(c.key) != data.end()) {
      WriteCursor wc;
      data.at(c.key).serialize(wc);
      return wc;
    } else {
      return nullopt;
    }
  }
  optional<sized_ptr<uint8_t>> handle_put_cmd(const Command &c) {
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
    auto peer = ownership_mapping.find(ck.name);
    if (peer == ownership_mapping.end()) {
      return nullopt;
    }
    /* TODO: use modulo to make this O(1) */
    for (int i = 0; i < ck.chunk_idx; i++) {
      peer++;
      /* if off the end, reset to start */
      if (peer == ownership_mapping.end())
        peer = ownership_mapping.begin();
    }
    return ::get<1>(*peer);
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
  optional<DataChunk> get(ChunkKey &k) {
    shared_lock olock(om_mtx);
    /* find peer who owns this DF */
    optional<IpV4Addr> addr = find_peer_for_chunk(k);
    /* if not found, return nullopt */
    if (!addr)
      return nullopt;

    /* if this node owns the chunk */
    if (*addr == node.addr()) {
      shared_lock dlock(data_mtx);
      return data.at(k);
    } else {
      Command get_cmd(k);
      WriteCursor wc;
      get_cmd.serialize(wc);
      ReadCursor rc(node.send_data(*addr, wc));
      if (has_next(rc)) {
        return DataChunk(rc);
      } else {
        return nullopt;
      }
    }
  }

  void put(const ChunkKey &k, const DataChunk &dc) {
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
      bool res = node.send_data_to_cluster(wc);
      assert(res);
    }

    /* if this node owns the chunk */
    if (*addr == node.addr()) {
      unique_lock dlock(data_mtx);
      data.emplace(make_pair(k, dc));
    } else {
      Command put_cmd(k, dc);
      WriteCursor wc;
      put_cmd.serialize(wc);
      node.send_data(*addr, wc);
    }
  }
};
