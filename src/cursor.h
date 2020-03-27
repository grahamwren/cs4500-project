#pragma once

#include "sized_ptr.h"
#include <cmath>
#include <cstring>
#include <inttypes.h>
#include <stack>
#include <string>
#include <type_traits>
#include <unistd.h>
#include <vector>

class ReadCursor {
public:
  uint8_t const *bytes = nullptr;
  uint8_t *cursor = nullptr;
  uint8_t const *bytes_end = nullptr;
  std::stack<uint8_t *> *checkpoints;

  ReadCursor(int len, uint8_t *b)
      : bytes(b), cursor(b), bytes_end(b + len),
        checkpoints(new std::stack<uint8_t *>()) {}
  ReadCursor(sized_ptr<uint8_t> data) : ReadCursor(data.len, data.ptr) {}
  ReadCursor(ReadCursor &&c)
      : bytes(c.bytes), cursor(c.cursor), bytes_end(c.bytes_end),
        checkpoints(c.checkpoints) {
    c.checkpoints = nullptr;
  }
  ~ReadCursor() { delete checkpoints; }
  int length() const { return bytes_end - bytes; }
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
  return std::string(sp.ptr, sp.len);
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

class WriteCursor {
private:
  std::vector<uint8_t> data;

  static int get_next_page_size(int len) {
    return ceil(len / (float)getpagesize()) * getpagesize();
  }

public:
  WriteCursor() = default;
  WriteCursor(int len) { ensure_space(len); }

  operator sized_ptr<uint8_t>() { return sized_ptr(length(), bytes()); }

  /**
   * handle bytes vector resizing manually. Reserve memory manually growing by
   * the page size until large enough.
   *
   * reserving by page size might be overriden by vector's interal growth factor
   */
  void ensure_space(int extra_cap) {
    int rem_cap = data.capacity() - data.size();
    if (extra_cap > rem_cap) {
      int new_cap = get_next_page_size(data.capacity() + extra_cap);
      data.reserve(new_cap);
    }
  }

  template <typename T> void write(T item) {
    static_assert(std::is_trivially_copyable_v<T> && !std::is_pointer_v<T>,
                  "Type must be trivially copyable");
    uint8_t *item_ptr = reinterpret_cast<uint8_t *>(&item);
    data.insert(data.end(), item_ptr, item_ptr + sizeof(T));
  }

  int length() const { return data.size(); }
  uint8_t *bytes() { return &data[0]; }
};

template <typename T> inline void pack(WriteCursor &c, T val) {
  c.ensure_space(sizeof(T));
  c.write(val);
}

template <> inline void pack(WriteCursor &c, sized_ptr<uint8_t> ptr) {
  c.ensure_space(sizeof(int) + (sizeof(uint8_t) * ptr.len));
  c.write(ptr.len);
  for (int i = 0; i < ptr.len; i++) {
    c.write(ptr[i]);
  }
}

template <> inline void pack(WriteCursor &c, sized_ptr<const char> ptr) {
  c.ensure_space(sizeof(int) + (sizeof(char) * ptr.len));
  c.write(ptr.len);
  for (int i = 0; i < ptr.len; i++) {
    c.write(ptr[i]);
  }
}

template <> inline void pack(WriteCursor &c, sized_ptr<char> ptr) {
  pack(c, (sized_ptr<const char>)ptr);
}

template <> inline void pack(WriteCursor &c, const std::string &val) {
  pack<sized_ptr<const char>>(c, sized_ptr(val.size(), val.c_str()));
}
