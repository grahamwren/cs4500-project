#include "application.h"

class WordCountDemo : public Application {
  Key data_key;
  WordCountDemo(const IpV4Addr &ip)
      : Application(ip), data_key(string("file")) {}

  void fill_cluster() {
    optional<NetworkDataFrame> net_df = cluster.create(data_key, Schema("S"));
    if (net_df) {
    }
  }
};
