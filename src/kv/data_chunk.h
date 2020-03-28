#pragma once

#include "sized_ptr.h"
#include <cstdint>
#include <cstring>
#include <iostream>

class DataChunk : public sized_ptr<uint8_t> {
public:
  DataChunk(int n, uint8_t *b) : sized_ptr(n, new uint8_t[n]) {
    if (b)
      memcpy(ptr, b, len);
  }
  DataChunk() : DataChunk(0, nullptr) {}
  DataChunk(sized_ptr<uint8_t> sp) : DataChunk(sp.len, sp.ptr) {}
  DataChunk(const DataChunk &dc) : DataChunk(dc.len, dc.ptr) {}
  DataChunk(DataChunk &&c) : DataChunk(c.len, c.ptr) { c.ptr = nullptr; }
  DataChunk(ReadCursor &c) : DataChunk(yield<sized_ptr<uint8_t>>(c)) {}
  ~DataChunk() { delete[] ptr; }

  DataChunk &operator=(DataChunk &&other) {
    len = other.len;
    delete[] ptr;
    ptr = other.ptr;
    other.ptr = nullptr;
    return *this;
  }

  void serialize(WriteCursor &c) const { pack(c, (sized_ptr<uint8_t>)*this); }
};

ostream &operator<<(ostream &output, const DataChunk &dc) {
  output << "data: [";
  char buf[10];
  if (dc.len) {
    sprintf(buf, "0x%x", dc[0]);
    output << buf;
  }
  for (int i = 1; i < dc.len; i++) {
    sprintf(buf, ",0x%x", dc[i]);
    output << buf;
  }
  output << "]";
  return output;
}
