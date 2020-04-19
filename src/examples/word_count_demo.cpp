#include "lib/rowers.h"
#include "sdk/application.h"
#include "utils/cli_flags.h"

/**
 * Count the usages of the words in the SOR file stored at "easy-data/100k.sor"
 *
 * authors: @grahamwren, @jagen31
 */
class WordCountDemo : public Application {
public:
  Key data_key;
  WordCountDemo(const IpV4Addr &ip) : Application(ip), data_key("file") {}

  void fill_cluster() {
    cluster.remove(data_key);
    bool import_res = cluster.load_file(data_key, "easy-data/100k.sor");
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

int main(int argc, char **argv) {
  CliFlags cli;
  cli.add_flag("--ip").parse(argc, argv, true);
  auto ip = cli.get_flag("--ip");
  /* ip flag is required */
  assert(ip);

  WordCountDemo(ip->c_str()).run();
}
