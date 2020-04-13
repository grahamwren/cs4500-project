#pragma once

#include "cursor.h"
#include "data.h"
#include <cassert>
#include <cstring>
#include <iostream>
#include <vector>

using namespace std;

/*************************************************************************
 * Schema::
 * A schema is a description of the contents of a data frame, the schema
 * knows the number of columns and the type of each column.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'F'.
 */
class Schema {
protected:
  vector<Data::Type> types;

public:
  Schema() = default;

  /**
   * Copying constructor
   */
  Schema(const Schema &from) : types(from.types) {}

  /**
   * Create a schema from a string of types. A string that contains
   * characters other than those identifying the four type results in
   * undefined behavior. The argument is external, a nullptr argument is
   * undefined.
   */
  Schema(const char *types_carr) {
    int len = strlen(types_carr);
    for (int i = 0; i < len; i++) {
      types.push_back(Data::char_as_type(types_carr[i]));
    }
  }

  /**
   * Add a column of the given type
   */
  void add_column(Data::Type type) { types.push_back(type); }

  /**
   * Add a column of the given type char: [I, F, ...]
   */
  void add_column(char c) { types.push_back(Data::char_as_type(c)); }

  /**
   * set a type in this Schema, used by parser when deducing types
   */
  void set_type(int i, Data::Type type) {
    assert(width() > i);
    types[i] = type;
  }

  /**
   * Return type of column at idx. An idx >= width is undefined behavior.
   */
  Data::Type col_type(int idx) const {
    assert(idx < width());
    return types[idx];
  }

  /**
   * The number of columns
   */
  int width() const { return types.size(); }

  bool operator==(const Schema &other) const {
    return types.size() == other.types.size() &&
           equal(types.begin(), types.end(), other.types.begin());
  }

  /**
   * copies a null-terminated char* representation of this schema into the given
   * dest buffer, if buffer smaller than width() +1 behavior undefined
   */
  void c_str(char *dest) const {
    for (int i = 0; i < width(); i++) {
      dest[i] = Data::type_as_char(col_type(i));
    }
    dest[width()] = '\0';
  }
};

template <>
inline void pack<const Schema &>(WriteCursor &c, const Schema &scm) {
  char buf[scm.width() + 1];
  scm.c_str(buf);
  pack(c, sized_ptr<char>(scm.width() + 1, buf));
}

template <> inline Schema yield(ReadCursor &c) {
  return Schema(yield<sized_ptr<char>>(c).ptr);
}

inline ostream &operator<<(ostream &output, const Schema &scm) {
  char buf[scm.width() + 1];
  scm.c_str(buf);
  output << "Schema[" << buf << "]";
  return output;
}
