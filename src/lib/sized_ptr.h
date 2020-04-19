#pragma once

#include <cassert>
#include <cstring>
#include <iostream>
#include <tuple>

/**
 * a convenience struct for dealing with pointers and their lengths
 *
 * authors: @grahamwren, @jagen31
 */
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

  operator sized_ptr<const E>() const { return sized_ptr<const E>(len, ptr); }
  operator std::tuple<int &, E *&>() { return {len, ptr}; }
  E &operator[](int i) const {
    assert(i < len);
    return ptr[i];
  }
  E &operator*() const {
    assert(len > 0);
    return *ptr;
  }
};

inline void interpret_byte(std::ostream &output, uint8_t byte) {
  if (32 <= byte && byte <= 126)
    output << (char)byte;
  else
    output << (uint32_t)byte;
}

inline std::ostream &operator<<(std::ostream &output,
                                const sized_ptr<uint8_t> &sp) {
  output << "{ " << sp.len << ", [";
  if (sp.len) {
    interpret_byte(output, sp[0]);
  }
  for (int i = 1; i < sp.len && i < 20; i++) {
    output << ",";
    interpret_byte(output, sp[i]);
  }
  if (sp.len > 20) {
    output << ", ... " << sp.len - 20 << " more bytes";
  }
  output << "] }";
  return output;
}
