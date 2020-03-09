#pragma once

#include "../lib.h"
#include "bool_block_list.h"
#include "column.h"
#include "float_block_list.h"
#include "int_block_list.h"
#include "string_ptr_block_list.h"
#include <stdarg.h>

/*************************************************************************
 * IntColumn::
 * Holds primitive int values, unwrapped.
 * authors: @grahamwren, @jagen31
 */
class IntColumn : public Column {
public:
  IntList *_data;

  IntColumn() { _data = new IntBlockList(); }
  IntColumn(int n, ...) : IntColumn() {
    va_list args;
    va_start(args, n);
    for (int i = 0; i < size(); i++) {
      int element = va_arg(args, int);
      push_back(element);
    }
    va_end(args);
  };
  ~IntColumn() { delete _data; }

  /**
   * Returns the int at idx; undefined on invalid idx.
   */
  int get(size_t idx) {
    assert(0 <= idx);
    assert(idx < size());
    return _data->get(idx);
  }

  /**
   * Sets the int at idx; undefined on invalid idx.
   */
  void set(size_t idx, int val) {
    assert(0 <= idx);
    assert(idx < size());
    _data->set(idx, val);
  }

  // impl Column

  IntColumn *as_int() { return this; };
  void push_back(int val) { _data->push(val); }
  size_t size() { return _data->size(); };
  char get_type() { return 'I'; };

  bool equals(Object *o) {
    IntColumn *other = dynamic_cast<IntColumn *>(o);

    if (other == nullptr) {
      return false;
    }

    return _data->equals(other->_data);
  }

  Object *clone() {
    IntColumn *new_column = new IntColumn();
    new_column->_data = dynamic_cast<IntList *>(_data->clone());
    return new_column;
  }
};

/*************************************************************************
 * FloatColumn::
 * Holds primitive float values, unwrapped.
 * authors: @grahamwren, @jagen31
 */
class FloatColumn : public Column {
public:
  FloatList *_data;

  FloatColumn() { _data = new FloatBlockList(); }
  FloatColumn(int n, ...) : FloatColumn() {
    va_list args;
    va_start(args, n);
    for (int i = 0; i < size(); i++) {
      float element = va_arg(args, float);
      push_back(element);
    }
    va_end(args);
  };
  ~FloatColumn() { delete _data; }

  /**
   * Returns the float at idx; undefined on invalid idx.
   */
  float get(size_t idx) {
    assert(0 <= idx);
    assert(idx < size());
    return _data->get(idx);
  }

  /**
   * Sets the float at idx; undefined on invalid idx.
   */
  void set(size_t idx, float val) {
    assert(0 <= idx);
    assert(idx < size());
    _data->set(idx, val);
  }

  // impl Column

  FloatColumn *as_float() { return this; };
  void push_back(float val) { _data->push(val); }
  size_t size() { return _data->size(); };
  char get_type() { return 'F'; };

  bool equals(Object *o) {
    FloatColumn *other = dynamic_cast<FloatColumn *>(o);

    if (other == nullptr) {
      return false;
    }

    return _data->equals(other->_data);
  }

  Object *clone() {
    FloatColumn *new_column = new FloatColumn();
    new_column->_data = dynamic_cast<FloatList *>(_data->clone());
    return new_column;
  }
};

/*************************************************************************
 * BoolColumn::
 * Holds primitive bool values, unwrapped.
 * authors: @grahamwren, @jagen31
 */
class BoolColumn : public Column {
public:
  BoolList *_data;

  BoolColumn() { _data = new BoolBlockList(); }
  BoolColumn(int n, ...) : BoolColumn() {
    va_list args;
    va_start(args, n);
    for (int i = 0; i < size(); i++) {
      bool element = va_arg(args, bool);
      push_back(element);
    }
    va_end(args);
  };
  ~BoolColumn() { delete _data; }

  /**
   * Returns the bool at idx; undefined on invalid idx.
   */
  bool get(size_t idx) {
    assert(0 <= idx);
    assert(idx < size());
    return _data->get(idx);
  }

  /**
   * Sets the bool at idx; undefined on invalid idx.
   */
  void set(size_t idx, bool val) {
    assert(0 <= idx);
    assert(idx < size());
    _data->set(idx, val);
  }

  // impl Column

  BoolColumn *as_bool() { return this; };
  void push_back(bool val) { _data->push(val); }
  size_t size() { return _data->size(); };
  char get_type() { return 'B'; };

  bool equals(Object *o) {
    BoolColumn *other = dynamic_cast<BoolColumn *>(o);

    if (other == nullptr) {
      return false;
    }

    return _data->equals(other->_data);
  }

  Object *clone() {
    BoolColumn *new_column = new BoolColumn();
    new_column->_data = dynamic_cast<BoolList *>(_data->clone());
    return new_column;
  }
};

/*************************************************************************
 * StringColumn::
 * Holds primitive String* values, unwrapped.
 * authors: @grahamwren, @jagen31
 */
class StringColumn : public Column {
public:
  StringPtrList *_data;

  StringColumn() { _data = new StringPtrBlockList(); }
  StringColumn(int n, ...) : StringColumn() {
    va_list args;
    va_start(args, n);
    for (int i = 0; i < size(); i++) {
      String *element = va_arg(args, String *);
      push_back(element);
    }
    va_end(args);
  };
  ~StringColumn() { delete _data; }

  /**
   * Returns the String* at idx; undefined on invalid idx.
   */
  String *get(size_t idx) {
    assert(0 <= idx);
    assert(idx < size());
    return _data->get(idx);
  }

  /**
   * Sets the String* at idx; undefined on invalid idx.
   */
  void set(size_t idx, String *val) {
    assert(0 <= idx);
    assert(idx < size());
    _data->set(idx, val);
  }

  // impl Column

  StringColumn *as_string() { return this; };
  void push_back(String *val) { _data->push(val); }
  size_t size() { return _data->size(); };
  char get_type() { return 'S'; };

  bool equals(Object *o) {
    StringColumn *other = dynamic_cast<StringColumn *>(o);

    if (other == nullptr) {
      return false;
    }

    size_t len = _data->size();
    size_t other_len = other->_data->size();

    if (len != other_len)
      return false;

    for (int i = 0; i < len; ++i) {
      String *current = _data->get(i);
      String *other_current = other->_data->get(i);
      if (current == nullptr) {
        if (current != other_current)
          return false;
      } else {
        if (!current->equals(other_current))
          return false;
      }
    }

    return true;
  }

  Object *clone() {
    StringColumn *new_column = new StringColumn();
    new_column->_data = dynamic_cast<StringPtrList *>(_data->clone());
    return new_column;
  }
};
