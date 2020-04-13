#pragma once

#include "lib/row.h"

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

TEST_F(TestRow, test_pack_idx_scm_data) {
  WriteCursor wc;
  row->pack_idx_scm_data(wc);
  ReadCursor rc = wc;
  Row r2(rc);

  EXPECT_EQ(row->get<int>(0), r2.get<int>(0));
  EXPECT_EQ(row->get<float>(1), r2.get<float>(1));
  EXPECT_STREQ(row->get<string *>(2)->c_str(), r2.get<string *>(2)->c_str());
  EXPECT_EQ(row->get<bool>(3), r2.get<bool>(3));
}

TEST_F(TestRow, test_pack_idx_data) {
  WriteCursor wc;
  row->pack_idx_data(wc);
  ReadCursor rc = wc;
  Row r2(row->get_schema(), rc);

  EXPECT_EQ(row->get<int>(0), r2.get<int>(0));
  EXPECT_EQ(row->get<float>(1), r2.get<float>(1));
  EXPECT_STREQ(row->get<string *>(2)->c_str(), r2.get<string *>(2)->c_str());
  EXPECT_EQ(row->get<bool>(3), r2.get<bool>(3));
}
