#pragma once

#include "kv/key.h"
#include "network/packet.h"
#include "schema.h"

class DFInfo {
public:
  const Key key;
  const Schema schema;
  const IpV4Addr owner;
  DFInfo(const Key &k, const Schema &scm, const IpV4Addr &owner)
      : key(k), schema(scm), owner(owner) {}
};
