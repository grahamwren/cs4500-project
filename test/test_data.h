#include "data.h"
#include "kv/data_chunk.h"

class TestData : public ::testing::Test {
public:
  void SetUp() {}

  void Teardown() {}
};

TEST_F(TestData, test_get) {
  Data d(1);
  EXPECT_EQ(d.get<int>(), 1);

  TypedData td(1);
  EXPECT_EQ(td.get<int>(), 1);
  EXPECT_EQ(td.type, Data::Type::INT);
}

TEST_F(TestData, test_set) {
  Data d(1);
  EXPECT_EQ(d.get<int>(), 1);
  d.set(22.5f);
  EXPECT_EQ(d.get<float>(), 22.5f);
  EXPECT_FALSE(d.is_missing());
  d.set();
  EXPECT_TRUE(d.is_missing());
  d.set(33.3f);
  EXPECT_EQ(d.get<float>(), 33.3f);
  EXPECT_FALSE(d.is_missing());

  TypedData td(1);
  EXPECT_EQ(td.get<int>(), 1);
  EXPECT_EQ(td.type, Data::Type::INT);
}

TEST(TestDataChunk, test_serialize) {
  DataChunk dc(11, (uint8_t *)"Hello world");
  WriteCursor wc;
  dc.serialize(wc);

  ReadCursor rc(wc.length(), wc.bytes());
  DataChunk dc2(rc);
  EXPECT_EQ(dc.len, dc2.len);
  EXPECT_STREQ((char *)dc.ptr, (char *)dc2.ptr);
}
