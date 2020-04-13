#pragma once

#include "lib/cursor.h"
#include <string>
#include <tuple>

using namespace std;

TEST(TestCursor, test_pack_yield) {
  WriteCursor wc(1024);

  pack<int>(wc, 2);
  pack(wc, 4.5f);
  pack(wc, true);
  pack(wc, sized_ptr(5, "hello"));
  string s("applesauce");
  pack<const string &>(wc, s);

  ReadCursor rc = wc;
  EXPECT_EQ(yield<int>(rc), 2);
  EXPECT_EQ(yield<float>(rc), 4.5f);
  EXPECT_EQ(yield<bool>(rc), true);
  int len;
  const char *start_ptr;
  tie(len, start_ptr) = yield<sized_ptr<const char>>(rc);
  EXPECT_EQ(len, 5);
  EXPECT_TRUE(strncmp(start_ptr, "hello", len) == 0);
  EXPECT_STREQ(yield<string>(rc).c_str(), string("applesauce").c_str());
}
