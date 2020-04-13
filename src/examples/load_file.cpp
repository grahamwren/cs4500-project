#include "lib/rowers.h"
#include "sdk/application.h"
#include "utils/cli_flags.h"

class LoadFile : public Application {
public:
  Key data_key;
  string filename;

  LoadFile(const IpV4Addr &ip, const string &key, const string &filename)
      : Application(ip), data_key(key), filename(filename) {}

  void ensure_key_removed() { cluster.remove(data_key); }

  void load_file() {
    bool import_res = cluster.load_file(data_key, filename.c_str());
    cout << "Loaded: " << data_key.name.c_str() << endl;
    assert(import_res);
  }

  void run() {
    ensure_key_removed();
    load_file();
  }
};

int main(int argc, char **argv) {
  CliFlags cli;
  cli.add_flag("--ip").add_flag("--key").add_flag("--file").parse(argc, argv);
  auto ip = cli.get_flag("--ip");
  auto key = cli.get_flag("--key");
  auto filename = cli.get_flag("--file");

  /* all flags are required */
  assert(ip);
  assert(key);
  assert(filename);

  LoadFile(ip->c_str(), *key, *filename).run();
}
