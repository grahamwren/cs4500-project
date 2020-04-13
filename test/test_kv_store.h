#pragma once

#include "kv/kv_store.h"

TEST(TestKVStore, test_has_pdf) {
  KVStore kv_store;
  EXPECT_FALSE(kv_store.has_pdf(Key("apples")));
}

TEST(TestKVStore, test__add_pdf__get_pdf) {
  KVStore kv_store;
  kv_store.add_pdf("apples", "IIII");
  EXPECT_TRUE(kv_store.has_pdf("apples"));
  EXPECT_TRUE(kv_store.get_pdf("apples").get_schema() == Schema("IIII"));
}

TEST(TestKVStore, test__add_pdf__remove_pdf) {
  KVStore kv_store;
  kv_store.add_pdf("apples", "IIII");
  EXPECT_TRUE(kv_store.has_pdf("apples"));
  kv_store.remove_pdf("apples");
  EXPECT_FALSE(kv_store.has_pdf("apples"));
}

TEST(TestKVStore, test_map_results) {
  auto s = "Hello world";
  auto s2 = "apples oranges pears";
  KVStore kv_store;

  EXPECT_FALSE(kv_store.has_map_result(rand()));
  EXPECT_FALSE(kv_store.has_map_result(rand()));

  int id = kv_store.get_result_id();

  EXPECT_TRUE(kv_store.has_map_result(id));

  int id2 = kv_store.get_result_id();

  EXPECT_TRUE(kv_store.has_map_result(id));
  EXPECT_TRUE(kv_store.has_map_result(id2));

  kv_store.insert_map_result(
      id, DataChunk(sized_ptr(strlen(s), (uint8_t *)s), true));
  kv_store.insert_map_result(
      id2, DataChunk(sized_ptr(strlen(s2), (uint8_t *)s2), true));

  EXPECT_TRUE(kv_store.get_map_result(id) ==
              DataChunk(sized_ptr(strlen(s), (uint8_t *)s), true));
  EXPECT_TRUE(kv_store.get_map_result(id2) ==
              DataChunk(sized_ptr(strlen(s2), (uint8_t *)s2), true));
}
