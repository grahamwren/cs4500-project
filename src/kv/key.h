#pragma once

#include "cursor.h"
#include <string>

using namespace std;

/**
 * represents the name of a DataFrame distributed throughout the cluster
 */
class Key {
public:
  string name;
  Key(const string &name) : name(name) {}
};

template <> inline void pack<Key>(WriteCursor &c, Key &key) {
  pack(c, key.name);
}

template <> inline void pack<const Key>(WriteCursor &c, const Key &key) {
  pack(c, key.name);
}
