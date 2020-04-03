#pragma once

#include "network/sock.h"

class ClusterAPI {
protected:
  void get_nodes_in_cluster(const IpV4Addr &addr) {
    Packet req(0, addr, PacketType::GET_PEERS);
    Packet resp = DataSock::fetch(req);
    assert(resp.ok());

    /* assume response is list of ips */
    int n_ips = resp.data.len() / sizeof(Ip4Addr);
    IpV4Addr *ips = reinterpret_cast<IpV4Addr *>(resp.data.ptr());
    for (int i = 0; i < n_ips; i++) {
      nodes.emplace(ips[i]);
    }
  }

  void get_ownership_in_cluster() {
    for (const IpV4Addr &ip : nodes) {
      GetOwnCommand cmd;
      WriteCursor wc;
      cmd.serialize(wc);
      Packet req(0, ip, PacketType::DATA,
                 sized_ptr<uint8_t>(wc.length(), wc.bytes()));
      Packet resp = DataSock::fetch(req);

      // load ownership into map for each key in resp
    }
  }

  void connect_to_cluster(const IpV4Addr &addr) {
    get_nodes_in_cluster(addr);
    get_ownership_in_cluster();
  }

public:
  ClusterAPI(const IpV4Addr &register_a) { connect_to_cluster(register_a); }

private:
  set<IpV4Addr> nodes;
  unordered_map<Key, IpV4Addr> ownership_map;
};
