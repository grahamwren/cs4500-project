#pragma once

#include "dataframe.h"
#include "kv/kv.h"

class KDStore {
protected:
  KV data_store;

public:
  KDStore(KV &kv) : data_store(kv) {}

  DataFrame *get(Key key);

  void put(Key key, DataFrame &df);
};
