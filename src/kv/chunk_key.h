#pragma once

#include "cursor.h"
#include <string>

using namespace std;

class ChunkKey {
public:
  string name;
  int chunk_idx;

  ChunkKey(const ChunkKey &) = default;
  ChunkKey(ReadCursor &c) : name(yield<string>(c)), chunk_idx(yield<int>(c)) {}

  bool operator<(const ChunkKey &rhs) const {
    int nr = name.compare(rhs.name);
    if (nr == 0) {
      return chunk_idx < rhs.chunk_idx;
    } else {
      return nr < 0;
    }
  }

  void serialize(WriteCursor &c) {
    pack<const string &>(c, name);
    pack(c, chunk_idx);
  }
};
