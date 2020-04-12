#include "application.h"
#include "rowers.h"

class WordCountDemo : public Application {
public:
  Key data_key;
  WordCountDemo(const IpV4Addr &ip) : Application(ip), data_key("file") {}

  void fill_cluster() {
    cluster.remove(data_key);
    bool import_res = cluster.load_file(data_key, "100k.sor");
    assert(import_res);
  }

  void run() {
    fill_cluster();
    auto rower = make_shared<WordCountRower>(0);
    cluster.map(data_key, rower);
    for (auto &e : rower->get_results()) {
      cout << e.first << ": ";
      cout << e.second << endl;
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

  WordCountDemo d(get_ip(argc, argv));
  d.run();
}
