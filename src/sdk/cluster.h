#pragma once

#include "dataframe_chunk.h"
#include "df_info.h"
#include "kv/command.h"
#include "kv/key.h"
#include "network/packet.h"
#include "network/sock.h"
#include "parser.h"
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
  /**
   * seek in the nodes set, starting at the given ip, forward by dist treating
   * nodes as a ring-buffer. Undefined behavior if the given ip does not exist
   * in the nodes set.
   */
  const IpV4Addr &seek_in_nodes(const IpV4Addr &start_ip, int dist) const {
    auto start = nodes.find(start_ip);
    assert(start != nodes.end());

    int offset = dist % nodes.size();
    for (int i = 0; i < offset; i++) {
      start++;
      if (start == nodes.end()) {
        start = nodes.begin();
      }
    }
    return *start;
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

  void connect_to_cluster(const IpV4Addr &addr) {
    get_nodes_in_cluster(addr);
    bool ownership_res = get_ownership_in_cluster();
    assert(ownership_res);
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

  bool get_ownership_in_cluster(const optional<Key> &query_key = nullopt) {
    GetDFInfoCommand cmd(query_key);
    for (const IpV4Addr &ip : nodes) {
      optional<DataChunk> data = send_cmd(ip, cmd);
      if (!data)
        return false;

      ReadCursor rc(data->data());
      vector<GetDFInfoCommand::result_t> results;
      GetDFInfoCommand::unpack_results(rc, results);

      /* load ownership into dataframes map for each key in resp */
      for (auto &e : results) {
        const Key &key = ::get<Key>(e);
        const optional<Schema> &scm = ::get<optional<Schema>>(e);
        int largest_chunk_idx = ::get<int>(e);

        /* if not seen before */
        if (dataframes.find(key) == dataframes.end()) {
          if (scm) {
            /* if response had a Schema, then this IP is the owner */
            dataframes.emplace(key, DFInfo(key, *scm, ip, largest_chunk_idx));
          } else {
            /* not the owner, just add Key and largest_chunk_idx */
            dataframes.emplace(key, DFInfo(key, largest_chunk_idx));
          }
        } else { // else seen before
          DFInfo &df_info = dataframes.find(key)->second;
          df_info.try_update_largest_chunk_idx(largest_chunk_idx);
          if (scm) {
            df_info.schema = *scm;
            df_info.owner = ip;
          }
        }
      }
    }
    return true;
  }

  /**
   * get a chunk by Key and index, returns an optional which will be nullopt if
   * the Key or chunk do not exist in the cluster.
   */
  optional<DataFrameChunk> get(const Key &key, int index) const {
    auto df_info_opt = get_df_info(key);
    if (df_info_opt) {
      const DFInfo &df_info = df_info_opt->get();
      const IpV4Addr &ip = seek_in_nodes(df_info.get_owner(), index);
      GetCommand get_cmd(key, index);
      optional<DataChunk> result = send_cmd(ip, get_cmd);
      if (result) {
        ReadCursor rc(result->len(), result->ptr());
        return DataFrameChunk(df_info.get_schema(), rc);
      }
    }
    return nullopt;
  }

  /**
   * put a chunk into the cluster for the given key and at the given chunk_idx.
   * Undefined behavior of DF with given Key has not been created in the cluster
   * yet. Returns whether the put was successful or not
   */
  bool put(const Key &key, int chunk_idx, const DataFrameChunk &dfc) {
    auto df_info_opt = get_df_info(key);
    if (df_info_opt) {
      DFInfo &df_info = df_info_opt->get();
      /* update DFInfo for this DF if this is a new chunk_idx */
      df_info.try_update_largest_chunk_idx(chunk_idx);

      const IpV4Addr &ip = seek_in_nodes(df_info.get_owner(), chunk_idx);
      WriteCursor wc;
      dfc.serialize(wc);

      /* TODO: DataChunk constructor copies data */
      PutCommand put_cmd(ChunkKey(key, chunk_idx), make_unique<DataChunk>(wc));
      optional<DataChunk> result = send_cmd(ip, put_cmd);
      return !!result;
    } else
      return false;
  }

  /**
   * map the given Rower over the cluster, the order with which results are
   * joined is undefined
   */
  void map(const Key &key, shared_ptr<Rower> rower) const {
    if (get_df_info(key)) {
      MapCommand cmd(key, rower);
      for (const IpV4Addr &ip : nodes) {
        optional<DataChunk> result = send_cmd(ip, cmd);
        if (result) {
          ReadCursor rc(result->len(), result->ptr());
          rower->join_serialized(rc);
          if (CLUSTER_LOG)
            cout << "Cluster.map(partial_result: " << *rower << ")" << endl;
        } else {
          if (CLUSTER_LOG)
            cout << "ERROR: cluster map failed for node: " << ip << endl;
        }
      }
    }
  }

  /**
   * removes the dataframe from the cluster by key
   */
  bool remove(const Key &key) {
    dataframes.erase(key);

    bool success = true;
    DeleteCommand cmd(key);
    for (const IpV4Addr &node : nodes) {
      optional<DataChunk> dc = send_cmd(node, cmd);
      success = success && dc;
    }
    return success;
  }

  bool create(const Key &key, const Schema &schema) {
    /* return failure if DF already exists for this Key */
    if (get_df_info(key))
      return false;

    /* pick IP to own this DF */
    const IpV4Addr &ip = *nodes.begin();
    /* add to dataframes info mapping */
    dataframes.emplace(key, DFInfo(key, schema, ip, 0));

    bool success = true;
    NewCommand cmd(key, schema);
    for (const IpV4Addr &node : nodes) {
      optional<DataChunk> dc = send_cmd(node, cmd);
      success = success && dc;
    }
    return success;
  }

  bool load_file(const Key &key, const char *filename) {
    /* if key already exists in cluster, return failure */
    if (get_df_info(key))
      return false;

    FILE *fd = fopen(filename, "r");
    if (fd == 0) {
      if (CLUSTER_LOG)
        cout << "ERROR: failed to find file: " << filename << endl;
      return false;
    }

    fseek(fd, 0, SEEK_END);
    long length = ftell(fd);
    fseek(fd, 0, SEEK_SET);

    if (CLUSTER_LOG)
      cout << "Cluster.read_file(file: " << filename << ", len: " << length
           << ")" << endl;

    char *buf = new char[length];
    fread(buf, length, 1, fd);
    fclose(fd);

    Parser parser(length, buf);
    Schema scm;
    parser.infer_schema(scm);

    if (CLUSTER_LOG)
      cout << "Cluster.infer_schema(scm: " << scm << ")" << endl;

    /* create DF in cluster */
    create(key, scm);

    for (int ci = 0; true; ci++) {
      DataFrameChunk dfc(scm, ci * DF_CHUNK_SIZE);
      bool success = parser.parse_n_lines(DF_CHUNK_SIZE, dfc);
      if (ci % 1000 == 0)
        cout << "parsed chunk: " << ci << endl;
      if (success)
        put(key, ci, dfc);

      for (int i = 0; i < scm.width(); i++) {
        if (scm.col_type(i) == Data::Type::STRING) {
          for (int y = 0; y < dfc.nrows(); y++) {
            if (!dfc.is_missing(dfc.get_start() + y, i)) {
              delete dfc.get_string(dfc.get_start() + y, i);
            }
          }
        }
      }
      if (!success || !dfc.is_full())
        break;
    }

    delete[] buf;
    return true;
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

  optional<reference_wrapper<const DFInfo>> get_df_info(const Key &key) const {
    auto it = dataframes.find(key);
    if (it == dataframes.end())
      return nullopt;
    return it->second;
  }
  optional<reference_wrapper<DFInfo>> get_df_info(const Key &key) {
    auto it = dataframes.find(key);
    if (it == dataframes.end())
      return nullopt;
    return it->second;
  }

  void get_keys(vector<Key> &dest) const {
    for (auto &e : dataframes) {
      dest.push_back(e.first);
    }
  }
};
