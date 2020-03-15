#pragma once

#include "row.h"

class TestRow : public ::testing::Test {
public:
  Schema *scm;
  Row *row;

  void SetUp() {
    scm = new Schema("IFSB");
    row = new Row(*scm);
    row->set(0, 22);
    row->set(1, 44.5f);
    row->set(2, new string("apples"));
    row->set(3, true);
  }

  void Teardown() {}
};

TEST_F(TestRow, test_width) { EXPECT_EQ(row->width(), 4); }
