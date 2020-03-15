#pragma once

#include "dataframe.h"

class TestDataFrame : public ::testing::Test {
public:
  Schema *scm;
  DataFrame *df;
  Row *row;

  void SetUp() {
    scm = new Schema("ISFB");
    df = new DataFrame(*scm);
    row = new Row(*scm);
  }

  void Teardown() {
    delete row;
    delete df;
    delete scm;
  }
};

TEST_F(TestDataFrame, test__add_column__ncols) {
  EXPECT_EQ(scm->width(), 4);
  EXPECT_EQ(df->ncols(), 4);

  /* adding column, should only affect internal schema */
  df->add_column('I');
  EXPECT_EQ(scm->width(), 4);             // does not affect external schema
  EXPECT_EQ(df->get_schema().width(), 5); // affects internals schema
  /* ensure ncols grows */
  EXPECT_EQ(df->ncols(), 5);

  string name("apples");
  df->add_column('B', &name);
  EXPECT_EQ(df->get_schema().width(), 6); // affects internals schema
  /* ensure ncols grows again */
  EXPECT_EQ(df->ncols(), 6);

  /* mutate external name should not affect col_name */
  name[0] = 'b';
  /* affects external name */
  EXPECT_STREQ(name.c_str(), "bpples");
  /* does not affect internal name */
  EXPECT_STREQ(df->get_schema().col_name(5)->c_str(), "apples");
}

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
  EXPECT_EQ(df->get<int>(0, 0), 0);
  EXPECT_STREQ(df->get<string *>(1, 0)->c_str(), "apples");
  EXPECT_EQ(df->get<float>(2, 0), 66.2f);
  EXPECT_EQ(df->get<bool>(3, 0), false);

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
    EXPECT_EQ(df->get<int>(0, i), i);
    EXPECT_STREQ(df->get<string *>(1, i)->c_str(), "apples");
    EXPECT_EQ(df->get<float>(2, i), i * 3.3f);
    EXPECT_EQ(df->get<bool>(3, i), i % 3 == 0);
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

  EXPECT_EQ(df->get<int>(0, 22), 22);
  EXPECT_EQ(df->get<float>(2, 22), 22 * 3.3f);
  df->set<int>(0, 22, 333);
  df->set<float>(2, 22, 4.7f);
  EXPECT_EQ(df->get<int>(0, 22), 333);
  EXPECT_EQ(df->get<float>(2, 22), 4.7f);
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
  EXPECT_EQ(results->get<int>(0, 0), 0);
  EXPECT_EQ(results->get<int>(0, 1), 2);
  EXPECT_EQ(results->get<int>(0, 2), 4);
  delete results;
}
