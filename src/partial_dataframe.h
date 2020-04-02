#pragma once

#include "dataframe_chunk.h"

class PartialDataFrame : public DataFrame {
protected:
  const Schema schema;
  vector<DataFrameChunk> chunks;
  unordered_map<int, int> start_idx_map;

  const DataFrameChunk &get_chunk(int y) const {
    int chunk_idx = y / DF_CHUNK_SIZE;
    int nearest_start_idx = chunk_idx * DF_CHUNK_SIZE;
    auto it = start_idx_map.find(nearest_start_idx);
    /* assert chunk is in this PartialDF */
    assert(it != start_idx_map.end());
    return chunks[it->second];
  }

public:
  PartialDataFrame(const Schema &scm) : schema(scm) {}

  const Schema &get_schema() const { return schema; }

  int nrows() const {
    if (chunks.size())
      return ((chunks.size() - 1) * DF_CHUNK_SIZE) + chunks.back().nrows();
    return 0;
  }

  void add_row(const Row &row) {
    if (chunks.size() == 0) {
      chunks.emplace_back(schema, 0);
      start_idx_map.emplace(0, 0);
    }

    if (chunks.back().is_full()) {
      int new_start_idx = chunks.back().get_start() + DF_CHUNK_SIZE;
      chunks.emplace_back(schema, new_start_idx);
      /* add index of inserted element */
      start_idx_map.emplace(new_start_idx, chunks.size() - 1);
    }

    chunks.back().add_row(row);
  }

  int get_int(int y, int x) const {
    const DataFrameChunk &chunk = get_chunk(y);
    return chunk.get_int(y, x);
  }
  float get_float(int y, int x) const {
    const DataFrameChunk &chunk = get_chunk(y);
    return chunk.get_float(y, x);
  }
  bool get_bool(int y, int x) const {
    const DataFrameChunk &chunk = get_chunk(y);
    return chunk.get_bool(y, x);
  }
  string *get_string(int y, int x) const {
    const DataFrameChunk &chunk = get_chunk(y);
    return chunk.get_string(y, x);
  }
  bool is_missing(int y, int x) const {
    const DataFrameChunk &chunk = get_chunk(y);
    return chunk.is_missing(y, x);
  }

  void map(Rower &rower) const {
    for (const DataFrameChunk &chunk : chunks) {
      chunk.map(rower);
    }
  }

  void add_df_chunk(ReadCursor &c) {
    int end_idx = chunks.size() ? chunks.back().get_start() + DF_CHUNK_SIZE : 0;

    chunks.emplace_back(schema);
    DataFrameChunk &chunk = chunks.back();
    chunk.fill(c);

    assert(chunk.get_start() >= end_idx);
    start_idx_map.emplace(chunk.get_start(), chunks.size() - 1);
  }
};
