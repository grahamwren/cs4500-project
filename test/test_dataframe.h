#pragma once

#include "sample_rowers.h"
#include "simple_dataframe.h"

class TestDataFrame : public ::testing::Test {
public:
  Schema *scm;
  DataFrame *df;
  Row *row;

  void SetUp() {
    scm = new Schema("ISFB");
    df = new SimpleDataFrame(*scm);
    row = new Row(*scm);
  }

  void Teardown() {
    delete row;
    delete df;
    delete scm;
  }
};

TEST_F(TestDataFrame, test__add_row__get__nrows) {
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

TEST_F(TestDataFrame, test_set) {
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

  EXPECT_EQ(df->get_int(22, 0), 22);
  EXPECT_EQ(df->get_float(22, 2), 22 * 3.3f);
  df->set(22, 0, 333);
  df->set(22, 2, 4.7f);
  EXPECT_EQ(df->get_int(22, 0), 333);
  EXPECT_EQ(df->get_float(22, 2), 4.7f);
}

TEST_F(TestDataFrame, test_map) {
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

  CountDiv2 rower(0);
  df->map(rower);
  EXPECT_EQ(rower.get_count(), 50);
}

TEST_F(TestDataFrame, test_filter) {
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

  class FilterDiv2 : public Rower {
  public:
    bool accept(const Row &r) { return r.get<int>(0) % 2 == 0; }
  };

  FilterDiv2 rower;
  DataFrame *results = df->filter(rower);
  EXPECT_EQ(results->nrows(), 50);
  EXPECT_EQ(results->get_int(0, 0), 0);
  EXPECT_EQ(results->get_int(1, 0), 2);
  EXPECT_EQ(results->get_int(2, 0), 4);
  delete results;
}

TEST_F(TestDataFrame, test_equals) {
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
  df->set(4, 1, new string("333"));
  EXPECT_FALSE(df->equals(*df2));
  df2->set(4, 1, new string("333"));
  EXPECT_TRUE(df2->equals(*df));
}
