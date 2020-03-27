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
  map<ChunkKey, IpV4Addr> ownership_mapping;

  shared_mutex data_mtx;
  map<ChunkKey, DataChunk> data;

public:
  KV(IpV4Addr a, IpV4Addr server_a) : node(a, server_a) { node.start(); }
  KV(IpV4Addr a) : node(a) {
    node.set_handler(handler);
    node.start();
  }
  ~KV() {
    node.teardown();
    node.join();
  }

  /**
   * invoked by node, in seperate thread
   */
  void handler(Command &c) {}

  /**
   * application interface
   */
  DataChunk *get(Key &k) {}

  void put(Key &k, DataChunk &dc) {}
};
