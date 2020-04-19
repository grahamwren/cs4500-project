#pragma once

#include "lib/cursor.h"
#include "lib/sized_ptr.h"
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
  DataChunk(WriteCursor &&wc) : _len(wc.length()), bytes(wc.data.release()) {
    wc.data.reset(nullptr);
    wc._length = 0;
    wc._capacity = 0;
  }
  DataChunk(ReadCursor &c, bool borrow = false)
      : DataChunk(yield<sized_ptr<uint8_t>>(c), borrow) {}
  DataChunk(const sized_ptr<uint8_t> &sp, bool borrow = false)
      : _len(sp.len),
        bytes(borrow ? sp.ptr : new uint8_t[_len], [=](uint8_t *ptr) {
          if (!borrow) // only delete ptr if we are NOT borrowing
            delete[] ptr;
        }) {
    /* don't copy if borrowing */
    if (!borrow && _len > 0) {
      /* warn for big copies, likely should be moves */
      if (_len > 4096)
        cout << "WARNING: copying " << _len << " bytes" << endl;
      memcpy(bytes.get(), sp.ptr, _len);
    }
  }

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
