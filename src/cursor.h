#pragma once

#include <cstring>
#include <inttypes.h>
#include <stack>

class Cursor {
public:
  uint8_t const *bytes = nullptr;
  uint8_t *cursor = nullptr;
  uint8_t const *bytes_end = nullptr;
  std::stack<uint8_t *> *checkpoints;

  Cursor(int len, uint8_t *b)
      : bytes(b), cursor(b), bytes_end(b + len),
        checkpoints(new std::stack<uint8_t *>()) {}
  Cursor(Cursor &&c)
      : bytes(c.bytes), cursor(c.cursor), bytes_end(c.bytes_end),
        checkpoints(c.checkpoints) {
    c.checkpoints = nullptr;
  }
  ~Cursor() { delete checkpoints; }
};

template <typename T> inline T yield(Cursor &c) {
  T const *type_cursor = reinterpret_cast<T const *>(c.cursor);
  c.cursor += sizeof(T);
  return type_cursor[0];
}

template <typename T> inline void unyield(Cursor &c) { c.cursor -= sizeof(T); }

template <typename T> inline T peek(const Cursor &c) {
  T const *type_cursor = reinterpret_cast<T const *>(c.cursor);
  return type_cursor[0];
}

template <typename T> inline bool is_next(const Cursor &c, T val) {
  return memcmp(c.cursor, &val, sizeof(T)) == 0;
}

template <typename T> inline bool expect(Cursor &c, T val) {
  if (is_next<T>(c, val)) {
    c.cursor += sizeof(T);
    return true;
  }
  return false;
}

inline bool empty(const Cursor &c) { return c.cursor >= c.bytes_end; }
inline bool has_next(const Cursor &c) { return c.cursor < c.bytes_end; }

inline void checkpoint(Cursor &c) { c.checkpoints->push(c.cursor); }
inline void commit(Cursor &c) { c.checkpoints->pop(); }
inline void rollback(Cursor &c) {
  c.cursor = c.checkpoints->top();
  c.checkpoints->pop();
}
