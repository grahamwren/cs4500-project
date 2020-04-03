#pragma once

#include "data.h"
#include "parser.h"
#include "schema.h"
#include "simple_dataframe.h"

class TestParser : public ::testing::Test {
public:
  const char *file;
  Schema *scm;
  DataFrame *df;
  void SetUp() {
    file = "<1><0><33>\n"
           "<0><44.5><\"apples\"><1><>\n"
           "<1><><oranges><0>\n";
    scm = new Schema("BFSBM");
    df = new SimpleDataFrame(*scm);
    Row r(*scm);
    r.set(0, true);
    r.set(1, (float)0);
    r.set(2, new string("33"));
    r.set_missing(3);
    r.set_missing(4);
    df->add_row(r);
    r.set(0, false);
    r.set(1, 44.5f);
    r.set(2, new string("apples"));
    r.set(3, true);
    r.set_missing(4);
    df->add_row(r);
    r.set(0, true);
    r.set_missing(1);
    r.set(2, new string("oranges"));
    r.set(3, false);
    r.set_missing(4);
    df->add_row(r);
  }

  void Teardown() {}
};

TEST_F(TestParser, test__infer_schema) {
  Parser p(file);
  Schema n_scm;
  p.infer_schema(n_scm);

  EXPECT_EQ(n_scm.width(), 5);
  EXPECT_EQ(n_scm.col_type(0), Data::Type::BOOL);
  EXPECT_EQ(n_scm.col_type(1), Data::Type::FLOAT);
  EXPECT_EQ(n_scm.col_type(2), Data::Type::STRING);
  EXPECT_EQ(n_scm.col_type(3), Data::Type::BOOL);
  EXPECT_EQ(n_scm.col_type(4), Data::Type::MISSING);
  EXPECT_TRUE(n_scm == *scm);
}

TEST_F(TestParser, test__parse_file) {
  Parser p(file);
  SimpleDataFrame n_df(*scm);
  p.parse_file(n_df);
  EXPECT_TRUE(n_df.equals(*df));
}
