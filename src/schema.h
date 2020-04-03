#pragma once

#include "data.h"
#include "parser.h"
#include <cassert>
#include <cstring>
#include <iostream>
#include <vector>

using namespace std;

/*************************************************************************
 * Schema::
 * A schema is a description of the contents of a data frame, the schema
 * knows the number of columns and number of rows, the type of each column,
 * optionally columns can be named by strings.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'F'.
 */
class Schema {
protected:
  vector<Data::Type> types;
  vector<string *> columns; // OWNED

  void add_col_name(const string *s) {
    string *n_s = s ? new string(*s) : nullptr;
    columns.push_back(n_s);
  }

public:
  Schema() = default;

  /**
   * Copying constructor
   */
  Schema(const Schema &from) : types(from.types) {
    for (int i = 0; i < from.width(); i++) {
      add_col_name(from.col_name(i));
    }
  }

  Schema(Schema &&scm) : types(scm.types) {
    for (int i = 0; i < scm.columns.size(); i++) {
      columns.push_back(scm.columns[i]);
      scm.columns[i] = nullptr;
    }
  }

  /**
   * move assignment operator
   */
  Schema &operator=(Schema &&scm) {
    if (this == &scm)
      return *this;

    types = scm.types;
    /* delete all of our column names */
    while (columns.size()) {
      delete columns.back();
      columns.pop_back();
    }
    /* get and clear all the column names from scm */
    for (int i = 0; i < scm.columns.size(); i++) {
      columns.push_back(scm.columns[i]);
      scm.columns[i] = nullptr;
    }
    return *this;
  }

  /**
   * Create a schema from a string of types. A string that contains
   * characters other than those identifying the four type results in
   * undefined behavior. The argument is external, a nullptr argument is
   * undefined.
   */
  Schema(const char *types_carr) {
    int len = strlen(types_carr);
    for (int i = 0; i < len; i++) {
      add_column(Data::char_as_type(types_carr[i]), nullptr);
    }
  }

  ~Schema() {
    for (int i = 0; i < width(); i++)
      delete col_name(i);
  }

  /**
   * Add a column of the given type and name (can be nullptr), name
   * is external.
   */
  void add_column(char type, string *name) {
    add_column(Data::char_as_type(type), name);
  }

  /**
   * Add a column of the given type and name (can be nullptr), name
   * is external.
   */
  void add_column(Data::Type type, string *name) {
    types.push_back(type);
    add_col_name(name);
  }

  void set_type(int i, Data::Type type) {
    assert(width() > i);
    types[i] = type;
  }

  /**
   * Return name of column at idx; nullptr indicates no name given.
   * An idx >= width is undefined.
   */
  const string *col_name(int idx) const {
    assert(idx < width());
    return columns[idx];
  }

  /**
   * Return type of column at idx. An idx >= width is undefined behavior.
   */
  Data::Type col_type(int idx) const {
    assert(idx < width());

    return types[idx];
  }

  /**
   * Given a column name return its index, or -1.
   */
  int col_idx(const string *name) const {
    int width = columns.size();
    for (int i = 0; i < width; i++) {
      string *s = columns[i];
      if (s == nullptr && name == nullptr)
        return i;
      else if (name && s && name->compare(*s) == 0)
        return i;
    }
    return -1;
  }

  /**
   * The number of columns
   */
  int width() const {
    assert(types.size() == columns.size());
    return types.size();
  }

  bool operator==(const Schema &other) const {
    return types.size() == other.types.size() &&
           equal(types.begin(), types.end(), other.types.begin()) &&
           columns.size() == other.columns.size() &&
           equal(columns.begin(), columns.end(), other.columns.begin(),
                 [](const string *l, const string *r) {
                   return l == nullptr ? r == nullptr : l->compare(*r) == 0;
                 });
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
