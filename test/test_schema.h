#include "schema.h"

class TestSchema : public ::testing::Test {
public:
  Schema *scm;
  void SetUp() {
    scm = new Schema("");
    scm->add_column('I', new string("id"));
    scm->add_column('S', new string("name"));
  }

  void Teardown() { delete scm; }
};

TEST_F(TestSchema, test_add_column_col_name) {
  EXPECT_STREQ(scm->col_name(0)->c_str(), "id");
  scm->add_column('I', new string("age"));
  EXPECT_STREQ(scm->col_name(2)->c_str(), "age");
}

TEST_F(TestSchema, test_col_idx_width) {
  EXPECT_EQ(scm->col_idx(new string("id")), 0);
  EXPECT_EQ(scm->width(), 2);
  scm->add_column('I', new string("age"));
  EXPECT_EQ(scm->col_idx(new string("age")), 2);
  EXPECT_EQ(scm->width(), 3);
}

TEST_F(TestSchema, test_equals) {
  Schema other("");
  EXPECT_FALSE(other.equals(*scm));
  other.add_column('I', new string("id"));
  other.add_column('S', new string("name"));
  EXPECT_TRUE(other.equals(*scm));
}
