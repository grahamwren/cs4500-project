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

  int get_int(int y, int x) const {
    int chunk_idx = y / DF_CHUNK_SIZE;
    int chunk_offset = y % DF_CHUNK_SIZE;
    return get_chunk(chunk_idx).get_int(chunk_offset, x);
  }
  float get_float(int y, int x) const {
    int chunk_idx = y / DF_CHUNK_SIZE;
    int chunk_offset = y % DF_CHUNK_SIZE;
    return get_chunk(chunk_idx).get_float(chunk_offset, x);
  }
  bool get_bool(int y, int x) const {
    int chunk_idx = y / DF_CHUNK_SIZE;
    int chunk_offset = y % DF_CHUNK_SIZE;
    return get_chunk(chunk_idx).get_bool(chunk_offset, x);
  }
  string *get_string(int y, int x) const {
    int chunk_idx = y / DF_CHUNK_SIZE;
    int chunk_offset = y % DF_CHUNK_SIZE;
    return get_chunk(chunk_idx).get_string(chunk_offset, x);
  }
  bool is_missing(int y, int x) const {
    int chunk_idx = y / DF_CHUNK_SIZE;
    int chunk_offset = y % DF_CHUNK_SIZE;
    return get_chunk(chunk_idx).is_missing(chunk_offset, x);
  }

  /**
   * Runs the given rower over the Chunks available in this PartialDataFrame
   */
  void map(Rower &rower) const {
    Row row(get_schema());
    for (auto &e : chunks) {
      auto &chunk = e.second;
      int start_i = e.first * DF_CHUNK_SIZE;
      for (int i = start_i; i < start_i + chunk.nrows(); i++) {
        fill_row(i, row);
        rower.accept(row);
      }
    }
  }

  /**
   * add a DFC at the given chunk_idx, undefined behavior if the chunk_idx
   * already exists in this PDF
   */
  void add_df_chunk(int chunk_idx, ReadCursor &c) {
    assert(!has_chunk(chunk_idx));
    auto &dfc = chunks.emplace(chunk_idx, schema).first->second;
    dfc.fill(c);
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

  const DataFrameChunk &get_chunk(int chunk_idx) const {
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
