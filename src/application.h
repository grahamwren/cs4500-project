#include "kv/kv.h"

class Application {
protected:
  KV kv;

public:
  Application(IpV4Addr ip, IpV4Addr server_a) : kv(ip, server_a) {}
  ~Application() { kv.teardown(); }
};
