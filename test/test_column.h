#include "column.h"

class TestColumn : public ::testing::Test {
public:
  Column *ic;
  Column *sc;
  void SetUp() {
    ic = new TypedColumn<int>();
    ic->as<int>().push(0);
    ic->as<int>().push(1);
    ic->as<int>().push(2);
    ic->as<int>().push(3);
    ic->as<int>().push(4);
    ic->as<int>().push(5);

    sc = new TypedColumn<string *>();
    sc->as<string *>().push(new string("10"));
    sc->as<string *>().push(new string("11"));
    sc->as<string *>().push(new string("12"));
    sc->as<string *>().push(new string("13"));
    sc->as<string *>().push(new string("14"));
    sc->as<string *>().push(new string("15"));
  }

  void Teardown() {
    delete ic;
    delete sc;
  }
};

TEST_F(TestColumn, test_size) {
  EXPECT_EQ(ic->size(), 6);
  EXPECT_EQ(sc->size(), 6);
  ic->as<int>().push(22);
  EXPECT_EQ(ic->size(), 7);
  sc->as<string *>().push(new string("22"));
  EXPECT_EQ(sc->size(), 7);
}

TEST_F(TestColumn, test_equals_push) {
  Column *c = new TypedColumn<int>();
  for (int i = 0; i < 6; i++)
    c->as<int>().push(i);
  EXPECT_TRUE(ic->equals(c));

  Column *strc = new TypedColumn<string *>();
  for (int i = 10; i < 16; i++) {
    char buf[3];
    sprintf(buf, "%d", i);
    strc->as<string *>().push(new string(buf));
  }
  EXPECT_TRUE(strc->equals(sc));
}

TEST_F(TestColumn, test_get_set) {
  EXPECT_EQ(ic->as<int>().get(0), 0);
  for (int i = 1; i < 6; i++)
    EXPECT_EQ(ic->as<int>().get(i), i);
  ic->as<int>().push(22);
  EXPECT_EQ(ic->as<int>().get(6), 22);
  ic->as<int>().set(2, 4);
  EXPECT_EQ(ic->as<int>().get(2), 4);

  EXPECT_STREQ(sc->as<string *>().get(0)->c_str(), "10");
  char buf[3];
  for (int i = 0; i < 6; i++) {
    sprintf(buf, "1%d", i);
    EXPECT_STREQ(sc->as<string *>().get(i)->c_str(), buf);
  }
  sc->as<string *>().push(new string("22"));
  EXPECT_STREQ(sc->as<string *>().get(6)->c_str(), "22");
  sc->as<string *>().set(2, new string("4"));
  EXPECT_STREQ(sc->as<string *>().get(2)->c_str(), "4");
}
