#pragma once

#include "key.h"
#include "schema.h"
#include <unordered_map>

using namespace std;

class KVStore {
public:
  bool has_df(const Key &key) const;
  PartialDataFrame &get_pdf(const Key &key);
  PartialDataFrame &add_pdf(const Key &key, const Schema &schema);
  void for_each(function<void(const pair<Key, PartialDataFrame> &)>) const;

protected:
  unordered_map<const Key, PartialDataFrame> data;
};

bool KVStore::has_df(const Key &key) const {
  return data.find(key) != data.end();
}

PartialDataFrame &KVStore::get_pdf(const Key &key) { return data.find(key); }

PartialDataFrame &KVStore::add_pdf(const Key &key, const Schema &schema) {
  return data.emplace(key, schema);
}

void KVStore::for_each(
    function<void(const pair<const Key, PartialDataFrame> &)> fn) const {
  ::for_each(data.begin(), data.end(), fn);
}
