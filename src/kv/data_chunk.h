#pragma once

#include "sized_ptr.h"
#include <cstdint>
#include <cstring>

class DataChunk : public sized_ptr<uint8_t> {
public:
  DataChunk(int n, uint8_t *b) : sized_ptr(n, new uint8_t[n]) {
    if (b)
      memcpy(ptr, b, len);
  }
  DataChunk() : DataChunk(0, nullptr) {}
  DataChunk(sized_ptr<uint8_t> sp) : DataChunk(sp.len, sp.ptr) {}
  DataChunk(const DataChunk &dc) : DataChunk(dc.len, dc.ptr) {}
  DataChunk(DataChunk &&c) : sized_ptr(c.len, c.ptr) { c.ptr = nullptr; }
  DataChunk(ReadCursor &c) : DataChunk(yield<sized_ptr<uint8_t>>(c)) {}
  ~DataChunk() { delete[] ptr; }

  DataChunk &operator=(DataChunk &&other) {
    len = other.len;
    delete[] ptr;
    ptr = other.ptr;
    other.ptr = nullptr;
    return *this;
  }

  void serialize(WriteCursor &c) { pack(c, (sized_ptr<uint8_t>)*this); }
};
