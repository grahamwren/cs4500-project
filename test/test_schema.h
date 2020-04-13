#pragma once

#include "lib/schema.h"

class TestSchema : public ::testing::Test {
public:
  Schema *scm;
  void SetUp() {
    scm = new Schema;
    scm->add_column('I');
    scm->add_column('S');
  }

  void Teardown() { delete scm; }
};

TEST_F(TestSchema, test_add_column_col_type) {
  EXPECT_EQ(scm->col_type(0), Data::Type::INT);
  scm->add_column('F');
  EXPECT_EQ(scm->col_type(2), Data::Type::FLOAT);
}

TEST_F(TestSchema, test_width) {
  EXPECT_EQ(scm->width(), 2);
  scm->add_column('I');
  EXPECT_EQ(scm->width(), 3);
}

TEST_F(TestSchema, test_equals) {
  Schema other;
  EXPECT_FALSE(other == *scm);
  other.add_column('I');
  other.add_column('S');
  EXPECT_TRUE(other == *scm);
}
