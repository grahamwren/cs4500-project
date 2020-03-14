#include "data.h"

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
