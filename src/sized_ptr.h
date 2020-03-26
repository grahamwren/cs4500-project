#pragma once

#include <cassert>
#include <cstring>
#include <tuple>

template <typename E> struct sized_ptr {
  int len;
  E *ptr;

  sized_ptr(int n, E *p) : len(n), ptr(p) {}
  sized_ptr(sized_ptr<E> const &o) : len(o.len), ptr(o.ptr) {}

  /**
   * fill the given buffer by copying values our of this pointer and null
   * terminate it. Destination buf must be allocated to be at least len + 1,
   * otherwise undefined
   */
  void fill(E buf[]) const {
    strncpy(buf, ptr, len);
    buf[len] = '\0';
  }

  operator sized_ptr<const E>() { return sized_ptr<const E>(len, ptr); }
  operator std::tuple<int &, E *&>() { return {len, ptr}; }
  E &operator[](int i) {
    assert(i < len);
    return ptr[i];
  }
  E &operator*() {
    assert(len > 0);
    return *ptr;
  }
};
