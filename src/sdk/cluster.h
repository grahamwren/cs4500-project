#pragma once

#include "dataframe_chunk.h"
#include "df_info.h"
#include "kv/command.h"
#include "kv/key.h"
#include "network/packet.h"
#include "network/sock.h"
#include <iostream>
#include <memory>
#include <optional>
#include <set>
#include <tuple>

#ifndef CLUSTER_LOG
#define CLUSTER_LOG false
#endif

using namespace std;

class Cluster {
private:
  set<IpV4Addr> nodes;
  unordered_map<const Key, DFInfo> dataframes;

protected:
  const IpV4Addr &find_node_for_chunk(const Key &key, int chunk_idx) const {
    assert(has_df_info(key));
    const DFInfo &df_info = dataframes.find(key)->second;
    auto it = nodes.find(df_info.owner);
    int offset = chunk_idx % nodes.size();
    for (int i = 0; i < offset; i++) {
      it++;
      if (it == nodes.end())
        it = nodes.begin();
    }
    return *it;
  }

  void get_nodes_in_cluster(const IpV4Addr &addr) {
    Packet req(0, addr, PacketType::GET_PEERS);
    Packet resp = DataSock::fetch(req);
    assert(resp.ok());

    /* assume response is list of ips */
    int n_ips = resp.data->len() / sizeof(IpV4Addr);
    IpV4Addr *ips = reinterpret_cast<IpV4Addr *>(resp.data->ptr());
    for (int i = 0; i < n_ips; i++) {
      nodes.emplace(ips[i]);
    }

    if (CLUSTER_LOG) {
      cout << "Cluster[";
      for (const IpV4Addr &ip : nodes) {
        cout << ip << ",";
      }
      cout << "]" << endl;
    }
  }

  void get_ownership_in_cluster() {
    for (const IpV4Addr &ip : nodes) {
      GetOwnedCommand cmd;
      optional<DataChunk> data = send_cmd(ip, cmd);
      assert(data);

      // load ownership into map for each key in resp
      ReadCursor rc(data->data());
      while (has_next(rc)) {
        Key key = yield<Key>(rc);
        /* if haven't seen this key, add to dataframes */
        if (dataframes.find(key) == dataframes.end()) {
          Schema scm = yield<Schema>(rc);
          dataframes.emplace(key, DFInfo(key, scm, ip));
        } else { // else skip
          yield<Schema>(rc);
        }
      }
    }
  }

  void connect_to_cluster(const IpV4Addr &addr) {
    get_nodes_in_cluster(addr);
    get_ownership_in_cluster();
  }

  optional<DataChunk> send_cmd(const IpV4Addr &ip, const Command &cmd) const {
    if (CLUSTER_LOG)
      cout << "Cluster.send(" << cmd << ")" << endl;
    WriteCursor wc;
    cmd.serialize(wc);
    Packet req(0, ip, PacketType::DATA, wc);
    Packet resp = DataSock::fetch(req);
    if (CLUSTER_LOG)
      cout << "Cluster.recv(" << resp << ")" << endl;
    if (resp.ok())
      return move(*resp.data);
    else
      return nullopt;
  }

public:
  Cluster(const IpV4Addr &register_a) { connect_to_cluster(register_a); }

  optional<DataFrameChunk> get(const Key &key, int index) const {
    const IpV4Addr &ip = find_node_for_chunk(key, index);
    GetCommand get_cmd(key, index);
    optional<DataChunk> result = send_cmd(ip, get_cmd);
    if (result) {
      const DFInfo &df_info = dataframes.find(key)->second;
      DataFrameChunk dfc(df_info.schema, index);
      ReadCursor rc(result->len(), result->ptr());
      dfc.fill(rc);
      return move(dfc);
    } else
      return nullopt;
  }

  bool put(const Key &key, const DataFrameChunk &dfc) const {
    if (has_df_info(key)) {
      const IpV4Addr &ip = find_node_for_chunk(key, dfc.chunk_idx());
      WriteCursor wc;
      dfc.serialize(wc);
      PutCommand put_cmd(key, make_unique<DataChunk>(wc)); // TODO: copies data
      optional<DataChunk> result = send_cmd(ip, put_cmd);
      return !!result;
    } else
      return false;
  }

  void map(const Key &key, shared_ptr<Rower> rower) const {
    if (has_df_info(key)) {
      MapCommand cmd(key, rower);
      for (const IpV4Addr &ip : nodes) {
        optional<DataChunk> result = send_cmd(ip, cmd);
        if (result) {
          ReadCursor rc(result->len(), result->ptr());
          rower->join_serialized(rc);
          if (CLUSTER_LOG)
            cout << "Cluster.map(partial_result: " << *rower << ")" << endl;
        }
      }
    }
  }

  bool create(const Key &key, const Schema &schema) {
    /* return failure if DF already exists for this Key */
    if (has_df_info(key))
      return false;

    /* pick IP to own this DF */
    const IpV4Addr &ip = *nodes.begin();
    /* add to dataframes info mapping */
    dataframes.emplace(key, DFInfo(key, schema, ip));

    bool success = true;
    NewCommand cmd(key, schema);
    for (const IpV4Addr &node : nodes) {
      optional<DataChunk> dc = send_cmd(node, cmd);
      success = success && dc;
    }
    return success;
  }

  bool shutdown() const {
    bool success = true;
    for (const IpV4Addr &ip : nodes) {
      Packet req(0, ip, PacketType::SHUTDOWN);
      if (CLUSTER_LOG)
        cout << "Cluster.send(" << req << ")" << endl;
      Packet resp = DataSock::fetch(req);
      if (CLUSTER_LOG)
        cout << "Cluster.recv(" << resp << ")" << endl;
      success = success && resp.ok();
    }
    if (CLUSTER_LOG)
      cout << "Cluster.shutdown(success: " << (success ? "true" : "false")
           << ")" << endl;
    return success;
  }

  const DFInfo &get_df_info(const Key &key) const {
    assert(has_df_info(key));
    return dataframes.find(key)->second;
  }

  bool has_df_info(const Key &key) const {
    return dataframes.find(key) != dataframes.end();
  }
};
