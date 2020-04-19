#pragma once

#include "sized_ptr.h"
#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <inttypes.h>
#include <iostream>
#include <memory>
#include <stack>
#include <string>
#include <type_traits>
#include <unistd.h>
#include <vector>

/**
 * A class to encapsulate reading from a byte array. Constructed from a length
 * and a bytes ptr, allows yield-ing and peeking objects of many types. Matches
 * the pack behavior of WriteCursor.
 *
 * Also supports checkpoint-ing the cursor position so that a parser can create
 * a checkpoint and then try to do some parsing and if it fails simply roll the
 * cursor position back to the checkpoint it created.
 */
class ReadCursor {
public:
  uint8_t const *bytes = nullptr;
  uint8_t *cursor = nullptr;
  uint8_t const *bytes_end = nullptr;
  std::stack<uint8_t *> *checkpoints;

  ReadCursor(long len, uint8_t *b)
      : bytes(b), cursor(b), bytes_end(b + len),
        checkpoints(new std::stack<uint8_t *>()) {}
  ReadCursor(const sized_ptr<uint8_t> data) : ReadCursor(data.len, data.ptr) {}
  ReadCursor(ReadCursor &&c)
      : bytes(c.bytes), cursor(c.cursor), bytes_end(c.bytes_end),
        checkpoints(c.checkpoints) {
    c.checkpoints = nullptr;
  }
  ~ReadCursor() { delete checkpoints; }
  long length() const { return bytes_end - bytes; }
};

template <typename T> inline T yield(ReadCursor &c) {
  static_assert(std::is_trivially_copyable_v<T> && !std::is_pointer_v<T>,
                "Type must be trivially copyable");
  T const *type_cursor = reinterpret_cast<T const *>(c.cursor);
  c.cursor += sizeof(T);
  return type_cursor[0];
}

template <> inline sized_ptr<uint8_t> yield(ReadCursor &c) {
  int len = yield<int>(c);
  uint8_t *start_ptr = reinterpret_cast<uint8_t *>(c.cursor);
  c.cursor += len * sizeof(uint8_t);
  return sized_ptr(len, start_ptr);
}

template <> inline sized_ptr<char> yield(ReadCursor &c) {
  int len = yield<int>(c);
  char *start_ptr = reinterpret_cast<char *>(c.cursor);
  c.cursor += len * sizeof(char);
  return sized_ptr(len, start_ptr);
}

template <> inline sized_ptr<const char> yield(ReadCursor &c) {
  return yield<sized_ptr<char>>(c);
}

template <> inline std::string yield(ReadCursor &c) {
  sized_ptr<const char> sp = yield<sized_ptr<const char>>(c);
  return std::string(sp.ptr, sp.len - 1);
}

template <> inline std::string *yield(ReadCursor &c) {
  sized_ptr<const char> sp = yield<sized_ptr<const char>>(c);
  return new std::string(sp.ptr, sp.len - 1);
}

template <typename T> inline void unyield(ReadCursor &c) {
  static_assert(std::is_trivially_copyable_v<T> && !std::is_pointer_v<T>,
                "Type must be trivially copyable");
  c.cursor -= sizeof(T);
}

template <typename T> inline T peek(const ReadCursor &c) {
  static_assert(std::is_trivially_copyable_v<T> && !std::is_pointer_v<T>,
                "Type must be trivially copyable");
  T const *type_cursor = reinterpret_cast<T const *>(c.cursor);
  return type_cursor[0];
}

template <typename T> inline bool is_next(const ReadCursor &c, T val) {
  static_assert(std::is_trivially_copyable_v<T> && !std::is_pointer_v<T>,
                "Type must be trivially copyable");
  return memcmp(c.cursor, &val, sizeof(T)) == 0;
}

template <typename T> inline bool expect(ReadCursor &c, T val) {
  static_assert(std::is_trivially_copyable_v<T> && !std::is_pointer_v<T>,
                "Type must be trivially copyable");
  if (is_next<T>(c, val)) {
    c.cursor += sizeof(T);
    return true;
  }
  return false;
}

inline bool empty(const ReadCursor &c) { return c.cursor >= c.bytes_end; }
inline bool has_next(const ReadCursor &c) { return c.cursor < c.bytes_end; }

inline void checkpoint(ReadCursor &c) { c.checkpoints->push(c.cursor); }
inline void commit(ReadCursor &c) { c.checkpoints->pop(); }
inline void rollback(ReadCursor &c) {
  c.cursor = c.checkpoints->top();
  c.checkpoints->pop();
}

/**
 * Encapsulates a dynamically growing bytes buffer for packing objects into.
 * Users can choose to specialize the pack<T>(WriteCursor &, T) method to
 * implement their own custom packing logic.
 */
class WriteCursor {
private:
  long _capacity = 0;
  long _length = 0;
  std::unique_ptr<uint8_t, std::function<void(uint8_t *)>> data;

  static int get_next_page_size(int len) {
    return std::ceil(len / (float)getpagesize()) * getpagesize();
  }

  friend class DataChunk;

public:
  WriteCursor()
      : _capacity(0), _length(0),
        data((uint8_t *)std::malloc(0), [](uint8_t *ptr) { free(ptr); }) {}
  WriteCursor(int len) : WriteCursor() { ensure_space(len); }
  void reset() { _length = 0; }

  operator sized_ptr<uint8_t>() { return sized_ptr(length(), begin()); }
  operator ReadCursor() { return ReadCursor(length(), begin()); }

  /**
   * Reserve memory manually growing by the page size until large enough.
   */
  void ensure_space(int extra_cap) {
    int req_cap = extra_cap + length();
    if (req_cap > capacity()) {
      int new_cap = get_next_page_size(req_cap);
      void *new_ptr = std::realloc(data.get(), new_cap);
      assert(new_ptr); // out of memory 🤷
      data.release();
      data.reset((uint8_t *)new_ptr);
      _capacity = new_cap;
    }
  }

  template <typename T> void write(T item) {
    static_assert(std::is_trivially_copyable_v<T> && !std::is_pointer_v<T>,
                  "Type must be trivially copyable");
    assert(capacity() - length() >= sizeof(T));
    uint8_t *item_ptr = reinterpret_cast<uint8_t *>(&item);
    std::copy(item_ptr, item_ptr + sizeof(T), end());
    _length += sizeof(T);
  }

  template <typename T> void write(int len, T *start) {
    static_assert(std::is_trivially_copyable_v<T> && !std::is_pointer_v<T>,
                  "Type must be trivially copyable");
    assert(capacity() - length() >= len * sizeof(T));
    const uint8_t *start_ptr = reinterpret_cast<const uint8_t *>(start);
    const uint8_t *end_ptr = start_ptr + (len * sizeof(T));
    std::copy(start_ptr, end_ptr, end());
    _length += len * sizeof(T);
  }

  int length() const { return _length; }
  long capacity() const { return _capacity; }
  uint8_t *begin() { return data.get(); }
  uint8_t *end() { return begin() + length(); }
};

template <typename T> inline void pack(WriteCursor &c, T val) {
  c.ensure_space(sizeof(T));
  c.write(val);
}

template <> inline void pack(WriteCursor &c, sized_ptr<uint8_t> ptr) {
  c.ensure_space(sizeof(int) + (sizeof(uint8_t) * ptr.len));
  c.write((int)ptr.len);
  c.write(ptr.len, ptr.ptr);
}

template <> inline void pack(WriteCursor &c, sized_ptr<int> ptr) {
  c.ensure_space(sizeof(int) + (sizeof(int) * ptr.len));
  c.write((int)ptr.len);
  c.write(ptr.len, ptr.ptr);
}

template <> inline void pack(WriteCursor &c, sized_ptr<const char> ptr) {
  c.ensure_space(sizeof(int) + (sizeof(char) * ptr.len));
  c.write((int)ptr.len);
  c.write(ptr.len, ptr.ptr);
}

template <> inline void pack(WriteCursor &c, sized_ptr<char> ptr) {
  pack(c, (const sized_ptr<const char> &)ptr);
}

template <> inline void pack(WriteCursor &c, std::string *const val) {
  pack<sized_ptr<const char>>(c, sized_ptr(val->size() + 1, val->c_str()));
}

template <> inline void pack(WriteCursor &c, const std::string &val) {
  pack<sized_ptr<const char>>(c, sized_ptr(val.size() + 1, val.c_str()));
}
