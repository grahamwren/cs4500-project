#pragma once

#include "lib/dataframe_chunk.h"
#include <algorithm>
#include <set>
#include <thread>
#include <unordered_map>

using namespace std;

/**
 * Represents a set of DataFrameChunks which are stored on a Node. These chunks
 * are not necessarily contiguous, but all chunks must be full except for the
 * last one.
 * authors: @grahamwren, @jagen31
 */
class PartialDataFrame : public DataFrame {
protected:
  const Schema schema;
  unordered_map<int, DataFrameChunk> chunks;

  void pmap_helper(const vector<int> &c_idxs, Rower &rower) const {
    Row row(get_schema());
    for (int ci : c_idxs) {
      auto &chunk = chunks.at(ci);
      int start_i = ci * DF_CHUNK_SIZE;
      for (int i = start_i; i < start_i + chunk.nrows(); i++) {
        fill_row(i, row);
        rower.accept(row);
      }
    }
  }

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
    if (chunks.size() == 0)
      return;

    int threads_to_use = min((int)THREAD_COUNT, (int)chunks.size());

    /* distribute the chunks for the threads */
    vector<int> splits[threads_to_use];
    int i = 0;
    for (auto &e : chunks) {
      splits[i].push_back(e.first);
      i = (i + 1) % threads_to_use;
    }

    unique_ptr<Rower> rowers[threads_to_use - 1];
    vector<thread> threads;
    /* start threads_to_use - 1 threads */
    for (int i = 0; i < threads_to_use - 1; i++) {
      rowers[i] = rower.clone();
      Rower &rower = *rowers[i];
      vector<int> &split = splits[i];
      threads.emplace_back([&]() { pmap_helper(split, rower); });
    }

    /* run map on main thread with last split */
    pmap_helper(splits[threads_to_use - 1], rower);

    /* join all rowers to main */
    for (int i = 0; i < threads_to_use - 1; i++) {
      threads[i].join();
      rower.join(*rowers[i]);
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
