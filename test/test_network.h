#include "kv/kv.h"
#include "network/node.h"
#include "network/packet.h"
#include "network/sock.h"
#include "sdk/cluster.h"

TEST(TestNetwork, compiles) {
  IpV4Addr ip_1("127.0.0.1");
  Node n_1(ip_1);
}
