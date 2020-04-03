#pragma once

#include "kv/kv_store.h"

TEST(TestKVStore, test_has_pdf) {
  KVStore kv_store;
  EXPECT_FALSE(kv_store.has_pdf(Key("apples")));
}
