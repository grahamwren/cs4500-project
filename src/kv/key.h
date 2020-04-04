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
  Key() = default;
  Key(const Key &) = default;
  Key(const char *name) : name(name) {}
  Key(const string &name) : name(name) {}

  bool operator==(const Key &other) const {
    return name.compare(other.name) == 0;
  }

  bool operator<(const Key &other) const {
    return name.compare(other.name) < 0;
  }
};

template <> inline void pack<const Key &>(WriteCursor &c, const Key &key) {
  pack<const string &>(c, key.name);
}

template <> inline Key yield(ReadCursor &c) { return Key(yield<string>(c)); }

ostream &operator<<(ostream &output, const Key &k) {
  output << "{ name: " << k.name.c_str() << " }";
  return output;
}

template <> struct hash<const Key> {
  size_t operator()(const Key &key) const noexcept {
    return hash<string>{}(key.name);
  }
};
