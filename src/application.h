#include "kv/kv.h"

class Application {
protected:
  KV kv;

public:
  Application(const IpV4Addr &ip, const IpV4Addr &server_a)
      : kv(ip, server_a) {}
  ~Application() { kv.teardown(); }
};
