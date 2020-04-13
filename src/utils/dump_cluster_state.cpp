#include "lib/rowers.h"
#include "sdk/application.h"

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

IpV4Addr get_ip(int argc, char **argv) {
  assert(argc >= 3);

  if (strcmp(argv[1], "--ip") == 0) {
    return IpV4Addr(argv[2]);
  } else if (argc > 4 && strcmp(argv[3], "--ip") == 0) {
    return IpV4Addr(argv[4]);
  }
  assert(false);
}

int main(int argc, char **argv) {
  cout << "starting with: ";
  for (int i = 0; i < argc; i++) {
    cout << argv[i] << ' ';
  }
  cout << endl;

  DumpClusterState d(get_ip(argc, argv));
  d.run();
}
