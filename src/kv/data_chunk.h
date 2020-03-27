#pragma once

#include "sized_ptr.h"
#include <cstdint>
#include <cstring>

class DataChunk : public sized_ptr<uint8_t> {
public:
  DataChunk(int n, uint8_t *b) : sized_ptr(len, new uint8_t[len]) {
    memcpy(ptr, b, len);
  }
  DataChunk(DataChunk &&c) : sized_ptr(c.len, c.ptr) { c.ptr = nullptr; }
  ~DataChunk() { delete[] bytes; }
};
