#pragma once

#include "data.h"
#include "schema.h"
#include <cassert>
#include <cstring>
#include <inttypes.h>
#include <stdlib.h>

using namespace std;

/*************************************************************************
 * Row::
 * This class represents a single row of _data constructed according to a
 * _dataframe's schema. The purpose of this class is to make it easier to add
 * read/write complete rows. Internally a _dataframe hold _data in columns.
 * Rows have pointer equality.
 *
 * authors: @grahamwren, @jagen31
 */
class Row {
  const Schema scm;
  int idx = 0;
  vector<Data> data;

public:
  static void unpack_data(ReadCursor &c, Row &dest) {
    const Schema &scm = dest.get_schema();
    /* collect data based on schema */
    for (int i = 0; i < scm.width(); ++i) {
      switch (scm.col_type(i)) {
      case Data::Type::BOOL: {
        dest.set(i, yield<bool>(c));
        break;
      }
      case Data::Type::INT: {
        dest.set(i, yield<int>(c));
        break;
      }
      case Data::Type::FLOAT: {
        dest.set(i, yield<float>(c));
        break;
      }
      case Data::Type::STRING: {
        sized_ptr<char> ptr = yield<sized_ptr<char>>(c);
        string *s = new string(ptr.ptr, ptr.len);
        dest.set(i, s);
        break;
      }
      default:
        assert(false); // unknown type in row
      }
    }
  }

  /**
   * Build a row following a schema with a start index.
   */
  Row(int i, const Schema &scm) : scm(scm), idx(i), data(scm.width()) {}

  /**
   * Build a row following a schema with default start index.
   */
  Row(const Schema &scm) : Row(0, scm) {}

  /**
   * copy constructor
   */
  Row(const Row &o) : scm(o.get_schema()), idx(o.get_idx()), data(o.data) {}

  /**
   * unpack a [idx, data...] representation of a Row
   */
  Row(const Schema &s, ReadCursor &c)
      : scm(s), idx(yield<int>(c)), data(s.width()) {
    unpack_data(c, *this);
  }

  /**
   * unpack a [idx, schema..., data...] representation of a Row
   */
  Row(ReadCursor &c) : idx(yield<int>(c)) {
    Schema &schema = const_cast<Schema &>(scm); // borrow our schema as mutable

    sized_ptr<char> types = yield<sized_ptr<char>>(c);
    char buf[types.len + 1];
    types.fill(buf);
    schema = Schema(buf); // move

    data.resize(schema.width()); // init enough elements to hold row

    unpack_data(c, *this);
  }

  /**
   * Number of fields in the row.
   */
  int width() const { return scm.width(); }

  /**
   * Set/get the index of this row (ie. its position in the _dataframe. This is
   * only used for informational purposes, unused otherwise
   */
  int get_idx() const { return idx; }
  void set_idx(int i) { idx = i; }

  /**
   * Getters: get the value at the given column. If the column is not
   * of the requested type, the result is undefined.
   */
  template <typename T> T get(int col) const { return data[col].get<T>(); }
  bool is_missing(int i) const { return data[i].is_missing(); }

  /**
   * get schema for this row
   */
  const Schema &get_schema() const { return scm; }

  /**
   * Setters: set the given column with the given value. Setting a column with
   * a value of the wrong type is undefined.
   */
  void set(int col, int val) {
    assert(col < width());
    data[col].set(val);
  }

  void set(int col, float val) {
    assert(col < width());
    data[col].set(val);
  }

  void set(int col, bool val) {
    assert(col < width());
    data[col].set(val);
  }

  void set(int col, string *val) {
    assert(col < width());
    assert(val); // cannot be nullptr, should be missing

    data[col].set(val);
  }

  void set(int col, const Data &val) {
    assert(col < width());

    data[col] = val;
  }

  void set_missing(int col) { data[col].set(); }

  bool equals(const Row &other) const {
    const Schema &schema = get_schema();

    bool accept = true;
    string *s;
    string *os;
    if (schema == other.get_schema()) {
      for (int x = 0; x < schema.width(); x++) {
        if (is_missing(x)) {
          accept = other.is_missing(x);
        } else {
          switch (schema.col_type(x)) {
          case Data::Type::INT:
            accept = get<int>(x) == other.get<int>(x);
            break;
          case Data::Type::FLOAT:
            accept = get<float>(x) == other.get<float>(x);
            break;
          case Data::Type::BOOL:
            accept = get<bool>(x) == other.get<bool>(x);
            break;
          case Data::Type::STRING:
            s = get<string *>(x);
            os = other.get<string *>(x);
            accept = s == nullptr || os == nullptr
                         ? s == os
                         : s->compare(*other.get<string *>(x)) == 0;
            break;
          case Data::Type::MISSING:
            accept = true;
            break;
          default:
            assert(false);
          }
        }
        if (!accept)
          return false;
      }
      return true;
    } else {
      return false;
    }
  }

  void pack_idx_scm_data(WriteCursor &c) const {
    pack(c, idx);
    char types[scm.width() + 1];
    scm.c_str(types);
    pack(c, sized_ptr(scm.width(), types));
    pack_data(c);
  }

  void pack_idx_data(WriteCursor &c) const {
    pack(c, idx);
    pack_data(c);
  }

  void pack_data(WriteCursor &c) const {
    for (int i = 0; i < width(); i++) {
      switch (scm.col_type(i)) {
      case Data::Type::INT: {
        pack(c, get<int>(i));
        break;
      }
      case Data::Type::BOOL: {
        pack(c, get<bool>(i));
        break;
      }
      case Data::Type::FLOAT: {
        pack(c, get<float>(i));
        break;
      }
      case Data::Type::STRING: {
        string *s = get<string *>(i);
        if (s)
          pack(c, sized_ptr(s->size(), s->c_str()));
        else
          pack(c, sized_ptr<char>(0, 0));
        break;
      }
      default:
        assert(false); // unknown type
      }
    }
  }

  void print(bool with_meta = false) const {
    if (with_meta) {
      cout << "Row<" << this << ">(scm: '";
      for (int i = 0; i < width(); i++) {
        cout << scm.col_type(i);
      }
      cout << "', data: [";
    }

    for (int i = 0; i < width(); i++) {
      cout << '<';
      if (!is_missing(i)) {
        switch (Data::type_as_char(scm.col_type(i))) {
        case 'I':
          cout << get<int>(i);
          break;
        case 'F':
          cout << fixed;
          cout << get<float>(i);
          break;
        case 'B':
          cout << get<bool>(i);
          break;
        case 'S':
          string *s = get<string *>(i);
          assert(s);
          cout << '"' << get<string *>(i)->c_str() << '"';
          break;
        }
      }
      cout << '>';
    }
    if (with_meta) {
      cout << "])";
    }
    cout << endl;
  }
};

template int Row::get<int>(int) const;
template float Row::get<float>(int) const;
template bool Row::get<bool>(int) const;
template string *Row::get<string *>(int) const;
