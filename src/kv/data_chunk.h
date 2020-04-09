#pragma once

#include "cursor.h"
#include "sized_ptr.h"
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>

using namespace std;

/**
 * an implementation of sized_ptr that owns it's data
 */
class DataChunk {
private:
  int _len;
  shared_ptr<uint8_t> bytes;

public:
  DataChunk() : _len(0), bytes(nullptr) {}
  DataChunk(int n, const shared_ptr<uint8_t> &b) : _len(n), bytes(b) {}
  /* only copy data from sized_ptr */
  DataChunk(const sized_ptr<uint8_t> &sp, bool borrow = false)
      : _len(sp.len),
        bytes(borrow ? sp.ptr : new uint8_t[_len], [borrow](uint8_t *ptr) {
          if (!borrow) // only delete ptr if we didn't borrow it
            delete[] ptr;
        }) {
    /* only copy if not borrowing, and there is data to copy */
    if (!borrow && sp.ptr && sp.len > 0) {
      if (sp.len > 4096) // warn about big copies
        cout << "WARNING: copying " << sp.len << endl;
      memcpy(bytes.get(), sp.ptr, sp.len);
    }
  }
  DataChunk(ReadCursor &c) : DataChunk(yield<sized_ptr<uint8_t>>(c)) {}

  sized_ptr<uint8_t> data() const { return sized_ptr(len(), bytes.get()); }
  int len() const { return _len; }
  const shared_ptr<uint8_t> &ptr() const { return bytes; }

  uint8_t operator[](int i) const { return bytes.get()[i]; }

  bool operator==(const DataChunk &other) const {
    return len() == other.len() &&
           equal(ptr().get(), ptr().get() + len(), other.ptr().get());
  }
};

ostream &operator<<(ostream &output, const DataChunk &dc) {
  output << dc.data();
  return output;
}
