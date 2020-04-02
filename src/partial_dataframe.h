#pragma once

#include "dataframe_chunk.h"

class PartialDataFrame : public DataFrame {
protected:
  const Schema schema;
  vector<DataFrameChunk> chunks;

public:
  PartialDataFrame(const Schema &scm) : schema(scm) {}

  const Schema &get_schema() const { return schema; }

  int nrows() const {
    if (chunks.size())
      return ((chunks.size() - 1) * DF_CHUNK_SIZE) + chunks.back().nrows();
    return 0;
  }

  void add_row(const Row &row) {
    if (!chunks.size() || chunks.back().is_full())
      chunks.emplace_back(schema);
    chunks.back().add_row(row);
  }

  int get_int(int y, int x) const {
    int chunk_idx = y / DF_CHUNK_SIZE;
    int idx_in_chunk = y % DF_CHUNK_SIZE;
    assert(chunk_idx >= 0);
    assert(chunk_idx < chunks.size());
    return chunks[chunk_idx].get_int(idx_in_chunk, x);
  }
  float get_float(int y, int x) const {
    int chunk_idx = y / DF_CHUNK_SIZE;
    int idx_in_chunk = y % DF_CHUNK_SIZE;
    assert(chunk_idx >= 0);
    assert(chunk_idx < chunks.size());
    return chunks[chunk_idx].get_float(idx_in_chunk, x);
  }
  bool get_bool(int y, int x) const {
    int chunk_idx = y / DF_CHUNK_SIZE;
    int idx_in_chunk = y % DF_CHUNK_SIZE;
    assert(chunk_idx >= 0);
    assert(chunk_idx < chunks.size());
    return chunks[chunk_idx].get_bool(idx_in_chunk, x);
  }
  string *get_string(int y, int x) const {
    int chunk_idx = y / DF_CHUNK_SIZE;
    int idx_in_chunk = y % DF_CHUNK_SIZE;
    assert(chunk_idx >= 0);
    assert(chunk_idx < chunks.size());
    return chunks[chunk_idx].get_string(idx_in_chunk, x);
  }
  bool is_missing(int y, int x) const {
    int chunk_idx = y / DF_CHUNK_SIZE;
    int idx_in_chunk = y % DF_CHUNK_SIZE;
    assert(chunk_idx >= 0);
    assert(chunk_idx < chunks.size());
    return chunks[chunk_idx].is_missing(idx_in_chunk, x);
  }

  void add_df_chunk(ReadCursor &c) {
    chunks.emplace_back(schema);
    chunks.back().fill(c);
  }
};
