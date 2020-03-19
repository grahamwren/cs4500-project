#pragma once

#include <cstring>

/**
 * a class which encapsulates a dynamically growing bytes buffer and manages the
 * byte representation of all of our primitive types
 * authors: @grahamwren @jagen31
 */
class BytesWriter {
public:
  int _write_idx = 0;
  int _capacity = 0;
  uint8_t *_bytes = nullptr;

  /**
   * construct a writer with the given initial size
   */
  BytesWriter(int init) { ensure_space(init); }
  ~BytesWriter() { delete[] _bytes; }
  int length() { return _write_idx; }
  const uint8_t *bytes() { return _bytes; }

  /**
   * ensure buffer has enough capacity for the requested size
   */
  void ensure_space(int requested_size) {
    if (requested_size > _capacity) {
      int old_cap = _capacity;
      while (requested_size > _capacity) {
        _capacity += 1024; // grow by 1KB until large enough
      }

      uint8_t *old = _bytes;
      _bytes = new uint8_t[_capacity];
      memcpy(old, _bytes, old_cap); // copy old _capacity worth of data
      delete[] old;
    }
  }

  /**
   * writes the given value into the current write position in the buffer and
   * moves the buffer's write position forward
   */
  void pack(char c) {
    ensure_space(_write_idx + sizeof(char));
    memcpy(_bytes + _write_idx, &c, sizeof(char));
    _write_idx += sizeof(char);
  }

  void pack(int c) {
    ensure_space(_write_idx + sizeof(int));
    memcpy(_bytes + _write_idx, &c, sizeof(int));
    _write_idx += sizeof(int);
  }

  void pack(bool c) {
    ensure_space(_write_idx + sizeof(bool));
    memcpy(_bytes + _write_idx, &c, sizeof(bool));
    _write_idx += sizeof(bool);
  }

  void pack(float c) {
    ensure_space(_write_idx + sizeof(float));
    memcpy(_bytes + _write_idx, &c, sizeof(float));
    _write_idx += sizeof(float);
  }

  void pack(double c) {
    ensure_space(_write_idx + sizeof(double));
    memcpy(_bytes + _write_idx, &c, sizeof(double));
    _write_idx += sizeof(double);
  }

  /**
   * pack a char[] of a specified length
   */
  void pack(int len, const char *c_arr) {
    pack(len);
    ensure_space(_write_idx + len);
    // don't include null-term since len is known
    memcpy(_bytes + _write_idx, c_arr, len);
    _write_idx += len;
  }

  /**
   * pack a null-terminated char[]
   */
  void pack(const char *c_str) { pack(strlen(c_str), c_str); }

  /**
   * pack a string object
   */
  void pack(string *str) { pack(str->size(), str->c_str()); }
};
