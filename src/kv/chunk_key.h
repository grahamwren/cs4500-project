#pragma once

#include "cursor.h"
#include <string>

using namespace std;

class ChunkKey {
public:
  string name;
  int chunk_idx;

  ChunkKey(Cursor &c) : name(yield<string>(c)), chunk_idx(yield<int>(c)) {}

  bool operator<(const ChunkKey &rhs) {
    int nr = name.compare(rhs.name);
    if (nr == 0) {
      return chunk_idx < rhs.chunk_idx;
    } else {
      return nr < 0;
    }
  }

  void serialize(WriteCursor &c) {
    pack(c, name);
    pack(c, chunk_idx);
  }
};
