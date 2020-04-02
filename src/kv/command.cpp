#include "kv.h"

optional<sized_ptr<uint8_t>> GetCommand::run(KV &kv, IpV4Addr &src) {
  return kv.run_get_cmd(*key);
}

optional<sized_ptr<uint8_t>> PutCommand::run(KV &kv, IpV4Addr& src) {
  return kv.run_put_cmd(*key, move(data));
}

optional<sized_ptr<uint8_t>> NewCommand::run(KV &kv, IpV4Addr &src) {
  return kv.run_new_cmd(*name, *scm, src);
}

