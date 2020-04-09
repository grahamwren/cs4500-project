#pragma once

#include "key.h"
#include "partial_dataframe.h"
#include "schema.h"
#include <cstdlib>
#include <functional>
#include <unordered_map>

using namespace std;

class KVStore {
public:
  bool has_pdf(const Key &key) const;
  PartialDataFrame &get_pdf(const Key &key);
  PartialDataFrame &add_pdf(const Key &key, const Schema &schema);
  void remove_pdf(const Key &key);
  void
      for_each(function<void(const pair<const Key, PartialDataFrame> &)>) const;

  bool has_map_result(int) const;
  const DataChunk &get_map_result(int) const;
  void remove_map_result(int);
  int get_result_id();
  void insert_map_result(int, const DataChunk &);

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

void KVStore::insert_map_result(int result_id, const DataChunk &data) {
  assert(has_map_result(result_id));
  map_results.insert_or_assign(result_id, data);
}
