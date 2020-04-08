#pragma once

#include "chunk_key.h"
#include "command.h"
#include "data_chunk.h"
#include "kv_store.h"
#include "lru_cache.h"
#include "network/node.h"
#include "schema.h"
#include <iostream>
#include <shared_mutex>
#include <unordered_map>

using namespace std;

#ifndef KV_LOG
#define KV_LOG false
#endif

class KV {
private:
  Node node;
  KVStore data_store;

protected:
  /**
   * invoked by node
   */
  void handler(const IpV4Addr &src, unique_ptr<Command> cmd,
               const Node::respond_fn_t &respond) {
    if (KV_LOG)
      cout << "KV.asyncRecv(" << src << ", " << *cmd << ")" << endl;
    return cmd->run(data_store, src, respond);
  }

public:
  KV(const IpV4Addr &a) : node(a) { start(); }
  KV(const IpV4Addr &a, const IpV4Addr &server_a) : node(a, server_a) {
    start();
  }
  void start() {
    node.set_data_handler(
        [&](IpV4Addr src, ReadCursor &req, const Node::respond_fn_t &resp) {
          return this->handler(src, Command::unpack(req), resp);
        });
    node.start();
  }
};
