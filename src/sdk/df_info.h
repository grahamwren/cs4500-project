#pragma once

#include "kv/key.h"
#include "network/packet.h"
#include "schema.h"
#include <algorithm>

class DFInfo {
private:
  const Key key;
  Schema schema;
  IpV4Addr owner;
  int largest_chunk_idx = -1;

  void try_update_largest_chunk_idx(int new_idx) {
    largest_chunk_idx = std::max(largest_chunk_idx, new_idx);
  }

public:
  DFInfo(const Key &k) : key(k) {}
  DFInfo(const Key &k, int largest_chunk_idx)
      : key(k), largest_chunk_idx(largest_chunk_idx) {}
  DFInfo(const Key &k, const Schema &scm, const IpV4Addr &owner,
         int largest_chunk_idx)
      : key(k), schema(scm), owner(owner),
        largest_chunk_idx(largest_chunk_idx) {}

  const Key &get_key() const { return key; }
  const Schema &get_schema() const { return schema; }
  const IpV4Addr &get_owner() const { return owner; }
  int get_largest_chunk_idx() const { return largest_chunk_idx; }

  // Cluster is allowed to access/modify private members
  friend class Cluster;
};
