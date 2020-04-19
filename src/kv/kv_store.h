#pragma once

#include "key.h"
#include "lib/schema.h"
#include "partial_dataframe.h"
#include <cstdlib>
#include <functional>
#include <unordered_map>

using namespace std;

/**
 * DataStore for a KV Node. Encapsulates the DataFrameChunks (DFCs) stored on
 * the Node in PartialDataFrames (PDFs) to track the index of each chunk and the
 * Schema of the parent DataFrame
 *
 * authors: @grahamwren, @jagen31
 */
class KVStore {
public:
  /* return whether this Store knows about the given Key */
  bool has_pdf(const Key &key) const;
  /* get the PDF for the given Key */
  PartialDataFrame &get_pdf(const Key &key);
  /* create a PDF in the clsuter for the given Key and Schema */
  PartialDataFrame &add_pdf(const Key &key, const Schema &schema);
  /* remove the PDF for the given Key, and delete all associated DFCs */
  void remove_pdf(const Key &key);
  /* apply the given function to every PDF in the Store */
  void
      for_each(function<void(const pair<const Key, PartialDataFrame> &)>) const;

  /* storing Map results */

  /* return whether we have a result for the given result ID */
  bool has_map_result(int) const;
  /* get the result for the given ID, undefined behavior if missing result for
   * the given ID */
  const DataChunk &get_map_result(int) const;
  /* remove the result for the given ID, usually after responding with it */
  void remove_map_result(int);
  /* get a new result ID to respond to the client, gurantees that the returned
   * result ID will not conflict with another result ID requested later */
  int get_result_id();
  /* insert a map result for a given ID, undefined behavior if ID not from
   * get_result_id */
  void insert_map_result(int, DataChunk &&);

protected:
  unordered_map<const Key, PartialDataFrame> data;
  unordered_map<int, DataChunk> map_results;
};

bool KVStore::has_pdf(const Key &key) const {
  return data.find(key) != data.end();
}

PartialDataFrame &KVStore::get_pdf(const Key &key) {
  assert(has_pdf(key));
  return data.find(key)->second;
}

PartialDataFrame &KVStore::add_pdf(const Key &key, const Schema &schema) {
  assert(!has_pdf(key));
  data.emplace(key, schema);
  return get_pdf(key);
}

void KVStore::remove_pdf(const Key &key) { data.erase(key); }

void KVStore::for_each(
    function<void(const pair<const Key, PartialDataFrame> &)> fn) const {
  ::for_each(data.begin(), data.end(), fn);
}

bool KVStore::has_map_result(int result_id) const {
  return map_results.find(result_id) != map_results.end();
}

const DataChunk &KVStore::get_map_result(int result_id) const {
  assert(has_map_result(result_id));
  const DataChunk &dc = map_results.at(result_id);
  return dc;
}

void KVStore::remove_map_result(int result_id) { map_results.erase(result_id); }

int KVStore::get_result_id() {
  int result_id;
  bool added = false;
  while (!added) {
    /* insert nullptr to reserve result_id */
    auto e = map_results.try_emplace(rand(), DataChunk(0, nullptr));
    added = e.second;
    result_id = e.first->first;
  }
  return result_id;
}

void KVStore::insert_map_result(int result_id, DataChunk &&data) {
  assert(has_map_result(result_id));
  map_results.insert_or_assign(result_id, move(data));
}
