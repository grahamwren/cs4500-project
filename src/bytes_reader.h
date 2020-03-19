#pragma once

#include <cassert>
#include <cstring>
#include <stack>
#include <string>

using namespace std;

/**
 * a class which consumes a bytes array of a known length and allows extraction
 * of known types through yield and peek methods
 * authors: @grahamwren @jagen31
 */
class BytesReader {
public:
  int cursor = 0;
  int length = 0;
  const uint8_t *bytes = nullptr; // EXTERNAL
  stack<int> checkpoints;

  /**
   * construct a reader with a bytes array and a length
   */
  BytesReader(int len, const uint8_t *input) : length(len), bytes(input) {}

  /**
   * yield primitive
   * Returns sizeof(T) bytes as a value of T and moves the iterator forward
   * sizeof(T) bytes. Behavior undefined if does not contain sizeof(T) bytes
   * still left in the iter.
   */
  template <typename T> T yield() {
    static_assert(is_literal_type_v<T> && !is_pointer_v<T>);

    T val = peek<T>();
    cursor += sizeof(T);
    return val;
  }

  /**
   * copy the next char[] into the dst buffer, dst must be at least the length
   * of the char[] + 1. Move the cursor past this char[].
   */
  void yield_c_arr(char *const &dst) {
    const int &len = peek<int>();
    peek_c_arr(dst);
    /* move forward: sizeof len + len of char[] (not including null term) */
    cursor += sizeof(int) + (len * sizeof(char));
  }

  /**
   * allocates a string object to contain the next char[] type object in the
   * buffer
   *
   * TODO: should be a move, not an allocation
   */
  string *yield_string_ptr() {
    string *s = peek_string();
    /* move forward: sizeof(len) + len of string (not including null term) */
    cursor += sizeof(int) + (s->size() * sizeof(char));
    return s;
  }

  /**
   * expect the next bytes to be the given value, only move cursor forward if
   * found value
   */
  template <typename T> bool expect(T val) {
    static_assert(is_literal_type_v<T> && !is_pointer_v<T>);

    T next_t = peek<T>();
    if (val == next_t) {
      cursor += sizeof(T);
      return true;
    }
    return false;
  }

  /**
   * checkpoint the current cursor position
   */
  void checkpoint() { checkpoints.push(cursor); }

  /**
   * commit the last checkpoint
   */
  void commit() { checkpoints.pop(); }

  /**
   * reset the cursor position to the last checkpoint
   */
  void rollback() {
    cursor = checkpoints.top();
    commit();
  }

  /**
   * peek primitives
   * Returns sizeof(T) bytes as a value of T and does NOT move the iterator
   * forward. Behavior undefined if does not contain sizeof(T) bytes still left
   * in the iter.
   */
  template <typename T> T peek() const {
    static_assert(is_literal_type_v<T> && !is_pointer_v<T>);
    assert(cursor + sizeof(T) <= length);

    const T *type_ptr = reinterpret_cast<const T *>(bytes + cursor);
    return *type_ptr;
  }

  /**
   * copies the next c_arr into the given dst array, adds a null terminator to
   * denote the length. `dst` array must be at least the required length + 1,
   * otherwise undefined behavior
   */
  void peek_c_arr(char *const &dst) const {
    const int &len = peek<int>();
    // skip len int
    const uint8_t *const &c_arr_start = bytes + cursor + sizeof(int);
    memcpy(dst, c_arr_start, len);
    dst[len] = '\0'; // add null-terminator
  }

  /**
   * allocates a string and copies the next char[] into it. Returned string
   * owned by caller
   *
   * TODO: should be a move rather than a heap allocation. However, CwC
   * missing move constructors
   */
  string *peek_string() const {
    const int &len = peek<int>();
    char buf[len + 1];
    peek_c_arr(buf);
    return new string(buf, len);
  }

  bool empty() const { return cursor == length; }
  bool has_next() const { return !empty(); }
  int cursor_pos() const { return cursor; }
};

template char BytesReader::peek<char>() const;
template char BytesReader::yield<char>();
template bool BytesReader::expect<char>(char);
