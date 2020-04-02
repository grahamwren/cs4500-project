#pragma once

#include "dataframe_chunk.h"

class TestDataFrameChunk : public ::testing::Test {
public:
  Schema *scm;
  DataFrame *df;
  Row *row;

  void SetUp() {
    scm = new Schema("ISFB");
    df = new DataFrameChunk(*scm);
    row = new Row(*scm);
  }

  void Teardown() {
    delete row;
    delete df;
    delete scm;
  }
};

TEST_F(TestDataFrameChunk, test__add_row__get__nrows) {
  /* ensure empty */
  EXPECT_EQ(df->nrows(), 0);

  /* add row */
  Row r(df->get_schema());
  r.set(0, 0);
  r.set(1, new string("apples"));
  r.set(2, 66.2f);
  r.set(3, false);
  df->add_row(r);
  EXPECT_EQ(df->nrows(), 1);
  EXPECT_EQ(df->get_int(0, 0), 0);
  EXPECT_STREQ(df->get_string(0, 1)->c_str(), "apples");
  EXPECT_EQ(df->get_float(0, 2), 66.2f);
  EXPECT_EQ(df->get_bool(0, 3), false);

  /* ensure length correct */
  EXPECT_EQ(df->nrows(), 1);

  /* add lots of rows */
  for (int i = 1; i < 1000; i++) {
    r.set(0, i);
    r.set(1, new string("apples"));
    r.set(2, i * 3.3f);
    r.set(3, i % 3 == 0);
    df->add_row(r);
  }
  for (int i = 1; i < 1000; i++) {
    EXPECT_EQ(df->get_int(i, 0), i);
    EXPECT_STREQ(df->get_string(i, 1)->c_str(), "apples");
    EXPECT_EQ(df->get_float(i, 2), i * 3.3f);
    EXPECT_EQ(df->get_bool(i, 3), i % 3 == 0);
  }

  /* ensure length still correct */
  EXPECT_EQ(df->nrows(), 1000);
}

TEST_F(TestDataFrameChunk, test_map) {
  /* fill df with 100 rows */
  Row r(df->get_schema());
  char buf[1024];
  for (int i = 0; i < 100; i++) {
    r.set(0, i);
    sprintf(buf, "hello %d", i);
    r.set(1, new string(buf));
    r.set(2, i * 3.3f);
    r.set(3, i % 3 == 0);
    df->add_row(r);
  }

  class CountDiv2 : public Rower {
  public:
    int count = 0;

    bool accept(const Row &r) {
      if (r.get<int>(0) % 2 == 0)
        count++;
      return true;
    }
  };

  CountDiv2 rower;
  df->map(rower);
  EXPECT_EQ(rower.count, 50);
}

TEST_F(TestDataFrameChunk, test_equals) {
  /* fill df with 100 rows */
  Row r(df->get_schema());
  char buf[1024];
  for (int i = 0; i < 5; i++) {
    r.set(0, i);
    sprintf(buf, "hello %d", i);
    r.set(1, new string(buf));
    r.set(2, i * 3.3f);
    r.set(3, i % 3 == 0);
    df->add_row(r);
  }

  DataFrame *df2 = new SimpleDataFrame(*scm);
  for (int i = 0; i < 5; i++) {
    r.set(0, i);
    sprintf(buf, "hello %d", i);
    r.set(1, new string(buf));
    r.set(2, i * 3.3f);
    r.set(3, i % 3 == 0);
    df2->add_row(r);
  }

  EXPECT_TRUE(df->equals(*df2));
}
