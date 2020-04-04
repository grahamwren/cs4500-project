#pragma once

#include "dataframe_chunk.h"
#include <algorithm>
#include <set>
#include <unordered_map>

using namespace std;

class PartialDataFrame : public DataFrame {
protected:
  const Schema schema;
  unordered_map<int, DataFrameChunk> chunks;

public:
  PartialDataFrame(const Schema &scm) : schema(scm) {}

  const Schema &get_schema() const { return schema; }

  bool has_chunk_by_row(int y) const {
    int chunk_idx = y / DF_CHUNK_SIZE;
    return has_chunk(chunk_idx);
  }

  const DataFrameChunk &get_chunk_by_row(int y) const {
    int chunk_idx = y / DF_CHUNK_SIZE;
    return get_chunk_by_chunk_idx(chunk_idx);
  }

  int get_int(int y, int x) const {
    const DataFrameChunk &chunk = get_chunk_by_row(y);
    return chunk.get_int(y, x);
  }
  float get_float(int y, int x) const {
    const DataFrameChunk &chunk = get_chunk_by_row(y);
    return chunk.get_float(y, x);
  }
  bool get_bool(int y, int x) const {
    const DataFrameChunk &chunk = get_chunk_by_row(y);
    return chunk.get_bool(y, x);
  }
  string *get_string(int y, int x) const {
    const DataFrameChunk &chunk = get_chunk_by_row(y);
    return chunk.get_string(y, x);
  }
  bool is_missing(int y, int x) const {
    const DataFrameChunk &chunk = get_chunk_by_row(y);
    return chunk.is_missing(y, x);
  }

  /**
   * Runs the given rower over the Chunks available in this PartialDataFrame
   */
  void map(Rower &rower) const {
    for (auto &e : chunks) {
      e.second.map(rower);
    }
  }

  /**
   * add a DFC at the given chunk_idx, undefined behavior if the chunk_idx
   * already exists in this PDF
   */
  void add_df_chunk(int chunk_idx, ReadCursor &c) {
    assert(!has_chunk(chunk_idx));
    chunks.emplace(chunk_idx, DataFrameChunk(schema, c));
  }

  /**
   * replace a DFC at the given chunk_idx, undefined behavior if the chunk_idx
   * does not exist in this PDF
   */
  void replace_df_chunk(int chunk_idx, ReadCursor &c) {
    assert(has_chunk(chunk_idx));
    chunks.erase(chunk_idx);
    add_df_chunk(chunk_idx, c);
  }

  bool has_chunk(int chunk_idx) const {
    return chunks.find(chunk_idx) != chunks.end();
  }

  const DataFrameChunk &get_chunk_by_chunk_idx(int chunk_idx) const {
    assert(has_chunk(chunk_idx));
    return chunks.find(chunk_idx)->second;
  }

  int nchunks() const { return chunks.size(); }

  /**
   * return largest chunk_idx in this PDF
   */
  int largest_chunk_idx() const {
    int largest_so_far = -1;
    for (auto &e : chunks) {
      largest_so_far = max(e.first, largest_so_far);
    }
    return largest_so_far;
  }
};
