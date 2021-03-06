#pragma once

#include "chunk_key.h"
#include "command.h"
#include "data_chunk.h"
#include "kv_store.h"
#include "lib/schema.h"
#include "network/node.h"
#include <iostream>
#include <shared_mutex>
#include <unordered_map>

using namespace std;

#ifndef KV_LOG
#define KV_LOG false
#endif

/**
 * A Key-Value (KV) storage Node, runs independently in the cluster. Handles
 * DATA Packets sent to the owned Node.
 *
 * authors: @grahamwren, @jagen31
 */
class KV {
private:
  Node node;
  KVStore data_store;

protected:
  /* used by the data_handler */
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
