#pragma once

// lang::CwC

#include "list.h"

/*************************************************************************
 * Schema::
 * A schema is a description of the contents of a data frame, the schema
 * knows the number of columns and number of rows, the type of each column,
 * optionally columns can be named by strings.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'F'.
 */
class Schema {
  List<char> types;
  List<string *> columns;

public:
  Schema() = default;

  /**
   * Copying constructor
   */
  Schema(Schema &from) : types(from.types), columns(from.columns) {}

  /**
   * Create a schema from a string of types. A string that contains
   * characters other than those identifying the four type results in
   * undefined behavior. The argument is external, a nullptr argument is
   * undefined.
   */
  Schema(const char *types_carr) {
    int len = strlen(types_carr);
    for (int i = 0; i < len; i++) {
      add_column(types_carr[i], nullptr);
    }
  }

  /**
   * Add a column of the given type and name (can be nullptr), name
   * is external.
   */
  void add_column(char type, string *name) {
    assert(type == 'I' || type == 'B' || type == 'F' || type == 'S');
    types.push(type);
    columns.push(name);
  }

  /**
   * Return name of column at idx; nullptr indicates no name given.
   * An idx >= width is undefined.
   */
  string *col_name(size_t idx) {
    assert(idx < width());
    return columns.get(idx);
  }

  /**
   * Return type of column at idx. An idx >= width is undefined behavior.
   */
  char col_type(size_t idx) {
    assert(idx < width());

    return types.get(idx);
  }

  /**
   * Given a column name return its index, or -1.
   */
  int col_idx(const char *name) {
    int width = columns.size();
    for (int i = 0; i < width; i++) {
      string *s = columns.get(i);
      if (s == nullptr ? name == nullptr : s->compare(name) == 0) {
        return i;
      }
    }
    return -1;
  }

  /**
   * The number of columns
   */
  size_t width() { return columns.size(); }

  /**
   * The number of rows
   */
  size_t length() { return types.size(); }

  bool equals(Schema &other) {
    return columns.equals(other.columns, [](const string *l, const string *r) {
      return l->compare(*r) == 0;
    }) && types.equals(other.types);
  }
};
