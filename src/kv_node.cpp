#include "kv/kv.h"
#include "utils/cli_flags.h"

/**
 * Main executable for Nodes in the EAU2 cluster
 */
int main(int argc, char **argv) {
  CliFlags cli;
  cli.add_flag("--ip").add_flag("--server-ip").parse(argc, argv, true);
  auto ip = cli.get_flag("--ip");
  auto server_ip = cli.get_flag("--server-ip");
  assert(ip); // "--ip" flag required

  if (server_ip) {
    KV kv(ip->c_str(), server_ip->c_str());
  } else {
    KV kv(ip->c_str());
  }
}
