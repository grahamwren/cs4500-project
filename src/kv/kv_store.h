#pragma once

#include "key.h"
#include "partial_dataframe.h"
#include "schema.h"
#include <unordered_map>

using namespace std;

class KVStore {
public:
  bool has_pdf(const Key &key) const;
  PartialDataFrame &get_pdf(const Key &key);
  PartialDataFrame &add_pdf(const Key &key, const Schema &schema);
  void
      for_each(function<void(const pair<const Key, PartialDataFrame> &)>) const;

protected:
  unordered_map<const Key, PartialDataFrame> data;
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

void KVStore::for_each(
    function<void(const pair<const Key, PartialDataFrame> &)> fn) const {
  ::for_each(data.begin(), data.end(), fn);
}
