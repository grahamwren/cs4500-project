#pragma once

// lang::CwC

#include "../lib.h"
#include "char_list.h"
#include "string_ptr_list.h"

/*************************************************************************
 * Schema::
 * A schema is a description of the contents of a data frame, the schema
 * knows the number of columns and number of rows, the type of each column,
 * optionally columns and rows can be named by strings.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'F'.
 */
class Schema : public Object {
public:
  CharList *_types;
  StringPtrList *_columns;
  StringColumn *_rows;

  Schema(int width, int _length) {
    _types = new CharList(width);
    _columns = new StringPtrList(width);
    _rows = new StringColumn();
  }

  /**
   * Create an empty schema
   */
  Schema() : Schema(10, 50) {}

  /**
   * Copying constructor
   */
  Schema(Schema &from) : Schema(from._types->size(), from._rows->size()) {
    CharList *s_types = from._types;
    StringPtrList *s_columns = from._columns;
    int width = s_types->size();
    for (int i = 0; i < width; i++) {
      _types->push(s_types->get(i));
      _columns->push(s_columns->get(i));
    }

    StringColumn *s_rows = from._rows;
    int length = s_rows->size();
    for (int i = 0; i < length; i++) {
      _rows->push_back(s_rows->get(i));
    }
  }

  /**
   * Create a schema from a string of types. A string that contains
   * characters other than those identifying the four type results in
   * undefined behavior. The argument is external, a nullptr argument is
   * undefined.
   */
  Schema(const char *types) : Schema(strlen(types), 50) {
    int len = strlen(types);
    for (int i = 0; i < len; i++) {
      add_column(types[i], nullptr);
    }
  }

  /**
   * Add a column of the given type and name (can be nullptr), name
   * is external.
   */
  void add_column(char typ, String *name) {
    assert(typ == 'I' || typ == 'B' || typ == 'F' || typ == 'S');
    _types->push(typ);
    _columns->push(name);
  }

  /**
   * Add a row with a name (possibly nullptr), name is external.
   */
  void add_row(String *name) { _rows->push_back(name); }

  /**
   * Return name of row at idx; nullptr indicates no name. An idx >= length
   * is undefined.
   */
  String *row_name(size_t idx) {
    assert(idx < length());

    return _rows->get(idx);
  }

  /**
   * Return name of column at idx; nullptr indicates no name given.
   * An idx >= width is undefined.
   */
  String *col_name(size_t idx) {
    assert(idx < width());

    return _columns->get(idx);
  }

  /**
   * Return type of column at idx. An idx >= width is undefined behavior.
   */
  char col_type(size_t idx) {
    assert(idx < width());

    return _types->get(idx);
  }

  /**
   * Given a column name return its index, or -1.
   */
  int col_idx(const char *name) {
    int width = _columns->size();
    for (int i = 0; i < width; i++) {
      String *s = _columns->get(i);
      if (s == nullptr ? name == nullptr : strcmp(s->c_str(), name) == 0) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Given a row name return its index, or -1.
   */
  int row_idx(const char *name) {
    int length = _rows->size();
    for (int i = 0; i < length; i++) {
      String *s = _rows->get(i);
      if (s == nullptr ? name == nullptr : strcmp(s->c_str(), name) == 0) {
        return i;
      }
    }
    return -1;
  }

  /**
   * The number of columns
   */
  size_t width() { return _columns->size(); }

  /**
   * The number of rows
   */
  size_t length() { return _rows->size(); }

  bool equals(Object *o) {
    Schema *other = dynamic_cast<Schema *>(o);

    if (other == nullptr) {
      return false;
    }

    size_t len = _columns->size();
    size_t other_len = other->_columns->size();

    if (len != other_len)
      return false;

    for (int i = 0; i < len; ++i) {
      String *col = _columns->get(i);
      String *other_col = other->_columns->get(i);

      if (col == nullptr) {
        if (col != other_col)
          return false;
      } else {
        if (!col->equals(other_col))
          return false;
      }
    }

    return _types->equals(other->_types) && _rows->equals(other->_rows);
  }

  Object *clone() {
    Schema *new_schema = new Schema(width(), length());
    new_schema->_types = dynamic_cast<CharList *>(_types->clone());
    new_schema->_columns = dynamic_cast<StringPtrList *>(_columns->clone());
    new_schema->_rows = dynamic_cast<StringColumn *>(_rows->clone());
    return new_schema;
  }
};
