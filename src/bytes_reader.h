#pragma once

// lang::CwC

#include "lib.h"

/**
 * a class which consumes a bytes array of a known length and allows extraction
 * of known types through yield and peek methods
 * authors: @grahamwren @jagen31
 */
class BytesReader {
public:
  int position = 0;
  int length = 0;
  const uint8_t *bytes = nullptr; // EXTERNAL

  /**
   * construct a reader with a bytes array and a length
   */
  BytesReader(int len, const uint8_t *input) : length(len), bytes(input) {}

  /**
   * yield_*, where * is the type T.
   * Returns sizeof(T) bytes as a value of T and moves the iterator forward
   * sizeof(T) bytes. Behavior undefined if does not contain sizeof(T) bytes
   * still left in the iter.
   */
  const char &yield_char() {
    const char &val = peek_char();
    position += sizeof(char);
    return val;
  }

  const bool &yield_bool() {
    const bool &val = peek_bool();
    position += sizeof(bool);
    return val;
  }

  const int &yield_int() {
    const int &val = peek_int();
    position += sizeof(int);
    return val;
  }

  const float &yield_float() {
    const float &val = peek_float();
    position += sizeof(float);
    return val;
  }

  const double &yield_double() {
    const double &val = peek_double();
    position += sizeof(double);
    return val;
  }

  /**
   * copy the next char[] into the dst buffer, dst must be at least the length
   * of the char[] + 1. Move the position past this char[].
   */
  void yield_c_arr(char *const &dst) {
    const int &len = peek_int();
    peek_c_arr(dst);
    /* move forward: sizeof len + len of char[] (not including null term) */
    position += sizeof(int) + (len * sizeof(char));
  }

  /**
   * allocates a String object to contain the next char[] type object in the
   * buffer
   *
   * TODO: should be a move, not an allocation
   */
  String *yield_string_ptr() {
    String *s = peek_string();
    /* move forward: sizeof(len) + len of string (not including null term) */
    position += sizeof(int) + (s->size() * sizeof(char));
    return s;
  }

  /**
   * peek_*, where * is the type T.
   * Returns sizeof(T) bytes as a value of T and does NOT move the iterator
   * forward. Behavior undefined if does not contain sizeof(T) bytes still left
   * in the iter.
   */
  const char &peek_char() const {
    assert(position + sizeof(char) <= length);
    const char *char_ptr = reinterpret_cast<const char *>(bytes + position);
    return *char_ptr;
  }

  const bool &peek_bool() const {
    assert(position + sizeof(bool) <= length);
    const bool *bool_ptr = reinterpret_cast<const bool *>(bytes + position);
    return *bool_ptr;
  }

  const int &peek_int() const {
    assert(position + sizeof(int) <= length);
    const int *int_ptr = reinterpret_cast<const int *>(bytes + position);
    return *int_ptr;
  }

  const float &peek_float() const {
    assert(position + sizeof(float) <= length);
    const float *float_ptr = reinterpret_cast<const float *>(bytes + position);
    return *float_ptr;
  }

  const double &peek_double() const {
    assert(position + sizeof(double) <= length);
    const double *double_ptr =
        reinterpret_cast<const double *>(bytes + position);
    return *double_ptr;
  }

  /**
   * copies the next c_arr into the given dst array, adds a null terminator to
   * denote the length. `dst` array must be at least the required length + 1,
   * otherwise undefined behavior
   */
  void peek_c_arr(char *const &dst) const {
    const int &len = peek_int();
    // skip len int
    const uint8_t *const &c_arr_start = bytes + position + sizeof(int);
    memcpy(dst, c_arr_start, len);
    dst[len] = '\0'; // add null-terminator
  }

  /**
   * allocates a String and copies the next char[] into it. Returned String
   * owned by caller
   *
   * TODO: should be a move rather than a heap allocation. However, CwC
   * missing move constructors
   */
  String *peek_string() const {
    const int &len = peek_int();
    char buf[len + 1];
    peek_c_arr(buf);
    return new String(buf, len);
  }

  bool empty() const { return position == length; }
  bool has_next() const { return !empty(); }
};
