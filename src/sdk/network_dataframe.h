#pragma once

#include "dataframe_chunk.h"
#include <memory>

using namespace std;

class Cluster;

class NetworkDataFrame : public DataFrame {
private:
  Key key;
  Schema schema;
  Cluster &cluster;
  LRUCache<ChunkKey, DataFrameChunk> cache;

  shared_ptr<DataFrameChunk> get_chunk(int chunk_idx);

public:
  NetworkDataFrame(const Key &key, const Schema &scm, Cluster &cluster)
      : key(key), schema(scm), cluster(cluster) {}

  const Schema &get_schema() const { return schema; }

  void add_row(const Row &row) {
    // how many chunks are in this DF
    // fetch last chunk, if full, create new otherwise append
  }
};

#include "cluster.h"

shared_ptr<DataFrameChunk> NetworkDataFrame::get_chunk(int chunk_idx) {
  return cache
      .fetch(ChunkKey(key, chunk_idx),
             [&](const ChunkKey &ckey) {
               optional<DataFrameChunk> dfc =
                   cluster.get(ckey.key, ckey.chunk_idx);
               if (dfc) {
               }
             })
      .lock();
}
