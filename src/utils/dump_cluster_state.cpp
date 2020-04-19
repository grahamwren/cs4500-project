#include "lib/rowers.h"
#include "sdk/application.h"
#include "utils/cli_flags.h"

/**
 * Application for printing what DataFrame are currently available in the
 * Cluster. Outputs the Key, the Schema, and the number of chunks for each
 * DataFrame in the cluster
 *
 * authors: @grahamwren, @jagen31
 */
class DumpClusterState : public Application {
public:
  DumpClusterState(const IpV4Addr &ip) : Application(ip) {}

  void run() {
    vector<Key> keys;
    cluster.get_keys(keys);
    cout << "Cluster:" << endl;
    for (auto &k : keys) {
      auto df_info = cluster.get_df_info(k);
      if (df_info) {
        cout << "  DataFrame(" << k.name << "):" << endl;
        cout << "    Schema: " << df_info->get().get_schema() << endl;
        cout << "    Owner: " << df_info->get().get_owner() << endl;
        cout << "    Num Chunks: " << df_info->get().get_largest_chunk_idx() + 1
             << endl;
      }
    }
  }
};

int main(int argc, char **argv) {
  CliFlags cli;
  cli.add_flag("--ip").parse(argc, argv, true);
  auto ip = cli.get_flag("--ip");
  assert(ip); // "ip" flag required

  DumpClusterState(ip->c_str()).run();
}
