#pragma once

#include "kv/key.h"
#include "lib/cursor.h"
#include <iostream>
#include <string>

using namespace std;

class ChunkKey {
public:
  Key key;
  int chunk_idx;

  ChunkKey() = default;
  ChunkKey(const ChunkKey &) = default;
  ChunkKey(const char *key, int idx) : key(key), chunk_idx(idx) {}
  ChunkKey(const string &key, int idx) : key(key), chunk_idx(idx) {}
  ChunkKey(const Key &key, int idx) : key(key), chunk_idx(idx) {}

  bool operator<(const ChunkKey &rhs) const {
    return key < rhs.key || (key == rhs.key && chunk_idx < rhs.chunk_idx);
  }

  bool operator==(const ChunkKey &other) const {
    return chunk_idx == other.chunk_idx && key == other.key;
  }
};

template <>
inline void pack<const ChunkKey &>(WriteCursor &c, const ChunkKey &ckey) {
  pack<const Key &>(c, ckey.key);
  pack(c, ckey.chunk_idx);
}

template <> inline ChunkKey yield(ReadCursor &c) {
  return ChunkKey(yield<Key>(c), yield<int>(c));
}

ostream &operator<<(ostream &output, const ChunkKey &k) {
  output << "{ key: " << k.key << ", idx: " << k.chunk_idx << " }";
  return output;
}

template <> struct hash<ChunkKey> {
  size_t operator()(const ChunkKey &key) const noexcept {
    return hash<const Key>{}(key.key) ^ (hash<int>{}(key.chunk_idx) << 1);
  }
};
