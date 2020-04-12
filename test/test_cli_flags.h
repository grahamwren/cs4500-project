#pragma once

#ifndef SS
#define SS(str) const_cast<char *>(str)
#endif

#include "utils/cli_flags.h"

TEST(TestCliFlags, test_parse) {
  CliFlags cli;
  cli.add_flag("--ip").add_flag("--server-ip");
  char *args[4] = {SS("--ip"), SS("some ip"), SS("--server-ip"),
                   SS("some other ip")};
  cli.parse(4, args);
  EXPECT_STREQ(cli.get_flag("--ip")->c_str(), "some ip");
  EXPECT_STREQ(cli.get_flag("--server-ip")->c_str(), "some other ip");
}

TEST(TestCliFlags, test_parse__other_order) {
  CliFlags cli;
  cli.add_flag("--ip").add_flag("--server-ip");
  char *args[4] = {SS("--server-ip"), SS("some other ip"), SS("--ip"),
                   SS("some ip")};
  cli.parse(4, args);
  EXPECT_STREQ(cli.get_flag("--ip")->c_str(), "some ip");
  EXPECT_STREQ(cli.get_flag("--server-ip")->c_str(), "some other ip");
  EXPECT_EQ(cli.get_flag("--not-a-flag"), nullopt);
}

TEST(TestCliFlags, test_parse__missing_flag) {
  CliFlags cli;
  cli.add_flag("--ip").add_flag("--server-ip");
  char *args[2] = {SS("--server-ip"), SS("some other ip")};
  cli.parse(2, args);
  EXPECT_STREQ(cli.get_flag("--server-ip")->c_str(), "some other ip");
  EXPECT_EQ(cli.get_flag("--ip"), nullopt);
  EXPECT_EQ(cli.get_flag("--not-a-flag"), nullopt);
}
