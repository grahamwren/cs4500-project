#pragma once

#include <cassert>
#include <tuple>

template <typename E> struct sized_ptr {
  int len;
  E *ptr;

  sized_ptr(int n, E *p) : len(n), ptr(p) {}
  sized_ptr(sized_ptr<E> const &o) : len(o.len), ptr(o.ptr) {}
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
