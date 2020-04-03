#pragma once

#include "cursor.h"
#include "sized_ptr.h"
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>

/**
 * an implementation of sized_ptr that owns it's data
 */
class DataChunk {
private:
  sized_ptr<uint8_t> _data;

public:
  DataChunk() : _data(0, nullptr) {}
  DataChunk(int n, unique_ptr<uint8_t> &&b) : _data(n, b.release()) {}
  DataChunk(int n, uint8_t *b) : _data(n, new uint8_t[n]) {
    if (b) {
      memcpy(_data.ptr, b, _data.len);
    }
  }

  /* allow copy from sized_ptr */
  DataChunk(const sized_ptr<uint8_t> &sp) : DataChunk(sp.len, sp.ptr) {}
  DataChunk(ReadCursor &c) : DataChunk(yield<sized_ptr<uint8_t>>(c)) {}

  /* disallow copy from DataChunk */
  DataChunk(const DataChunk &dc) = delete;

  DataChunk(DataChunk &&c) : _data(c.len(), c.ptr()) {
    c._data.len = 0;
    c._data.ptr = nullptr;
  }
  ~DataChunk() { delete[] ptr(); }

  DataChunk &operator=(DataChunk &&other) {
    _data.len = other.len();
    other._data.len = 0;

    _data.ptr = other.ptr();
    other._data.ptr = nullptr;
    return *this;
  }

  const sized_ptr<uint8_t> &data() const { return _data; }
  int len() const { return _data.len; }
  uint8_t *ptr() const { return _data.ptr; }

  uint8_t operator[](int i) const { return _data[i]; }

  bool operator==(const DataChunk &other) const {
    return len() == other.len() &&
           std::equal(ptr(), ptr() + len(), other.ptr());
  }
};

ostream &operator<<(ostream &output, const DataChunk &dc) {
  output << dc.data();
  return output;
}
