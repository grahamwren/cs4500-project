#include "application.h"
#include "rowers.h"

class LinusLoadData : public Application {
public:
  Key commits_key;
  // Key users_key;
  // Key projects_key;

  LinusLoadData(const IpV4Addr &ip)
      : Application(ip), commits_key("commits")
  //, users_key("users"), projects_key("projects")
  {}

  void ensure_keys_removed() {
    cluster.remove(commits_key);
    // cluster.remove(users_key);
    // cluster.remove(projects_key);
  }

  void fill_cluster() {
    bool import_res;
    // import_res = cluster.load_file(projects_key, "./datasets/projects.ltgt");
    // cout << "Loaded: projects" << endl;
    // assert(import_res);
    import_res = cluster.load_file(commits_key, "./datasets/commits.ltgt");
    cout << "Loaded: commits" << endl;
    assert(import_res);
    // import_res = cluster.load_file(users_key, "./datasets/users.ltgt");
    // cout << "Loaded: users" << endl;
    // assert(import_res);
  }

  void run() {
    ensure_keys_removed();
    fill_cluster();
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

  LinusLoadData d(get_ip(argc, argv));
  d.run();
}
