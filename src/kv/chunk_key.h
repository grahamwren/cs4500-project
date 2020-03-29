#pragma once

#include "cursor.h"
#include <iostream>
#include <string>

using namespace std;

class ChunkKey {
public:
  string name;
  int chunk_idx;

  ChunkKey() = default;
  ChunkKey(const char *ns, int idx) : name(ns), chunk_idx(idx) {}
  ChunkKey(const string &ns, int idx) : name(ns), chunk_idx(idx) {}
  ChunkKey(ReadCursor &c) : name(yield<string>(c)), chunk_idx(yield<int>(c)) {}
  ChunkKey(const ChunkKey &) = default;

  bool operator<(const ChunkKey &rhs) const {
    int nr = name.compare(rhs.name);
    if (nr == 0) {
      return chunk_idx < rhs.chunk_idx;
    } else {
      return nr < 0;
    }
  }

  void serialize(WriteCursor &c) const {
    pack<const string &>(c, name);
    pack(c, chunk_idx);
  }

  bool operator==(const ChunkKey &other) const {
    return chunk_idx == other.chunk_idx && name.compare(other.name) == 0;
  }
};

ostream &operator<<(ostream &output, const ChunkKey &k) {
  output << "{ name: " << k.name.c_str() << ", idx: " << k.chunk_idx << " }";
  return output;
}
