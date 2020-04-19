#include "kv/data_chunk.h"
#include "lib/data.h"

TEST(TestData, test_get) {
  Data d(1);
  EXPECT_EQ(d.get<int>(), 1);
}

TEST(TestData, test_set) {
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
}
