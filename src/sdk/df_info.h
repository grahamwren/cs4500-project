#pragma once

#include "kv/key.h"
#include "lib/schema.h"
#include "network/packet.h"
#include <algorithm>
#include <atomic>

using namespace std;

/**
 * Container for information stored by the Cluster SDK to interact with a
 * DataFrame stored in the EAU2 cluster.
 *
 * authors: @grahamwren, @jagen31
 */
class DFInfo {
private:
  const Key key;
  Schema schema;
  IpV4Addr owner;
  atomic<int> largest_chunk_idx;

  void try_update_largest_chunk_idx(int new_idx) {
    int old_idx = largest_chunk_idx.load();
    while (!largest_chunk_idx.compare_exchange_weak(old_idx,
                                                    max(old_idx, new_idx)))
      ;
  }

public:
  DFInfo(const Key &k) : key(k), largest_chunk_idx(-1) {}
  DFInfo(const Key &k, int largest_chunk_idx)
      : key(k), largest_chunk_idx(largest_chunk_idx) {}
  DFInfo(const Key &k, const Schema &scm, const IpV4Addr &owner,
         int largest_chunk_idx)
      : key(k), schema(scm), owner(owner),
        largest_chunk_idx(largest_chunk_idx) {}
  DFInfo(const DFInfo &dfi)
      : key(dfi.key), schema(dfi.schema), owner(dfi.owner),
        largest_chunk_idx(dfi.largest_chunk_idx.load()) {}

  const Key &get_key() const { return key; }
  const Schema &get_schema() const { return schema; }
  const IpV4Addr &get_owner() const { return owner; }
  int get_largest_chunk_idx() const { return largest_chunk_idx.load(); }

  // Cluster is allowed to access/modify private members
  friend class Cluster;
};
