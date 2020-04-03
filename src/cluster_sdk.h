#pragma once

#include "network/sock.h"

class ClusterSDK {
  set<IpV4Addr> nodes;
  unordered_map<Key, IpV4Addr> ownership_map;

  /**
   * return the peer IP for this chunk.
   */
  optional<IpV4Addr> find_peer_for_chunk(const Key &k, int index) const {
    auto owner_ip = ownership_map.find(k);
    if (owner_ip != ownership_map.end()) {
      auto it = nodes.find(owner_ip->second);
      int offset = index % nodes.size();
      /* TODO: use 🤷 to make this O(1) */
      for (int i = 0; i < offset; i++) {
        it++;
        /* if off the end, reset to start */
        if (it == nodes.end())
          it = nodes.begin();
      }
      return *it;
    } else {
      return nullopt;
    }
  }

protected:
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
  }

  void get_ownership_in_cluster() {
    for (const IpV4Addr &ip : nodes) {
      GetOwnedCommand cmd;
      WriteCursor wc;
      cmd.serialize(wc);
      Packet req(0, ip, PacketType::DATA,
                 sized_ptr<uint8_t>(wc.length(), wc.bytes()));
      Packet resp = DataSock::fetch(req);

      // load ownership into map for each key in resp
      ReadCursor rc(resp.data->data());
      int len = yield<int>(rc);
      for (int i = 0; i < len; ++i) {
        Key key = yield<Key>(rc);
        yield<Schema>(rc); // skip schema
        ownership_map[key] = ip;
      }
    }
  }

  void connect_to_cluster(const IpV4Addr &addr) {
    get_nodes_in_cluster(addr);
    get_ownership_in_cluster();
  }

public: 
  ClusterSDK (const IpV4Addr &register_a) { connect_to_cluster(register_a); }

  unique_ptr<DataChunk> get(const Key &key, int index) const {

		optional<IpV4Addr> ip = find_peer_for_chunk(key, index);

		if (!ip) assert(false);
    
    GetCommand get_cmd(key, index);
    WriteCursor wc;
    get_cmd.serialize(wc);
    Packet req(0, *ip, PacketType::DATA,
               sized_ptr<uint8_t>(wc.length(), wc.bytes()));
    Packet resp = DataSock::fetch(req);
    assert(resp.ok());

    return move(resp.data);
  }

  void put(const Key &key, unique_ptr<DataChunk> &&dc) const {
		auto ip = ownership_map.find(key);

    if (ip == ownership_map.end()) {
      assert(false);
    }

    PutCommand put_cmd(key, move(dc));
    WriteCursor wc;
    put_cmd.serialize(wc);
    Packet req(0, ip->second, PacketType::DATA,
               sized_ptr<uint8_t>(wc.length(), wc.bytes()));
    Packet resp = DataSock::fetch(req);
    assert(resp.ok());
  }
};
