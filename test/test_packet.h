#pragma once

#include "network/packet.h"
#include <arpa/inet.h>

TEST(TestIpV4Addr, test_uint32) {
  auto ip_str = "192.168.0.1";
  IpV4Addr ip(ip_str);

  /* expect (uint32_t) conversion to be equiv to inet_addr */
  EXPECT_EQ(ip, ntohl(inet_addr(ip_str)));

  /* expect partity between construction and conversion to/from uint23_t */
  IpV4Addr ip2((uint32_t)ip);
  EXPECT_EQ(ip, ip2);
}

TEST(TestIpV4Addr, test_c_str) {
  auto ip_str = "192.168.0.1";
  IpV4Addr ip(ip_str);

  std::stringstream s;
  s << ip;
  /* expect (str) conversion to be equiv to construction from str */
  EXPECT_STREQ(ip_str, s.str().c_str());
}
