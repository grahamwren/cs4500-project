#include "column.h"

class TestColumn : public ::testing::Test {
public:
  Column *ic;
  Column *sc;
  void SetUp() {
    ic = new TypedColumn<int>();
    sc = new TypedColumn<string *>();

    ic->push(0);
    ic->push(1);
    ic->push(2);
    ic->push(3);
    ic->push(4);
    ic->push(5);

    sc->push(new string("10"));
    sc->push(new string("11"));
    sc->push(new string("12"));
    sc->push(new string("13"));
    sc->push(new string("14"));
    sc->push(new string("15"));
  }

  void Teardown() {
    delete ic;
    delete sc;
  }
};

TEST_F(TestColumn, test_length) {
  EXPECT_EQ(ic->length(), 6);
  EXPECT_EQ(sc->length(), 6);
  ic->push(22);
  EXPECT_EQ(ic->length(), 7);
  sc->push(new string("22"));
  EXPECT_EQ(sc->length(), 7);
}

TEST_F(TestColumn, test_equals_push) {
  Column *c = new TypedColumn<int>();
  for (int i = 0; i < 6; i++)
    c->push(i);
  EXPECT_TRUE(ic->equals(*c));

  Column *strc = new TypedColumn<string *>();
  for (int i = 10; i < 16; i++) {
    char buf[3];
    sprintf(buf, "%d", i);
    strc->push(new string(buf));
  }
  EXPECT_TRUE(strc->equals(*sc));
}

TEST_F(TestColumn, test_get_set) {
  EXPECT_EQ(ic->get_int(0), 0);
  for (int i = 1; i < 6; i++)
    EXPECT_EQ(ic->get_int(i), i);
  ic->push(22);
  EXPECT_EQ(ic->get_int(6), 22);
  ic->set(2, 4);
  EXPECT_EQ(ic->get_int(2), 4);

  EXPECT_STREQ(sc->get_string(0)->c_str(), "10");
  char buf[3];
  for (int i = 0; i < 6; i++) {
    sprintf(buf, "1%d", i);
    EXPECT_STREQ(sc->get_string(i)->c_str(), buf);
  }
  sc->push(new string("22"));
  EXPECT_STREQ(sc->get_string(6)->c_str(), "22");
  sc->set(2, new string("4"));
  EXPECT_STREQ(sc->get_string(2)->c_str(), "4");
}

TEST_F(TestColumn, test_serialize_fill_int) {
  WriteCursor wc;
  ic->serialize(wc);
  ReadCursor rc(wc.length(), wc.bytes());
  Column *ic2 = new TypedColumn<int>();
  ic2->fill(ic->length(), rc);
  EXPECT_TRUE(ic->equals(*ic2));
}

TEST_F(TestColumn, test_serialize_fill_string) {
  WriteCursor wc;
  sc->serialize(wc);
  ReadCursor rc(wc.length(), wc.bytes());
  Column *sc2 = new TypedColumn<string *>();
  sc2->fill(sc->length(), rc);
  EXPECT_TRUE(sc->equals(*sc2));
}
