#pragma once
#include "kv.h"
#include "dataframe.h"

class KDStore {
public:
  KV _kv;
  KDStore(KV &kv) : _kv(kv) { }

  DataFrame *get(Key key);

  void put(Key key, DataFrame& df);
};

