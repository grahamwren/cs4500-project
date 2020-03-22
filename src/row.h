#pragma once

#include "bytes_reader.h"
#include "bytes_writer.h"
#include "data.h"
#include "schema.h"
#include <cassert>
#include <cstring>
#include <inttypes.h>
#include <stdlib.h>

using namespace std;

class Schema;
class BytesReader;
class BytesWriter;

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
  const Schema &scm;
  int idx = 0;
  vector<Data> data;

public:
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
    if (schema.equals(other.get_schema())) {
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

  void pack(BytesWriter &writer) const {
    writer.pack('R');
    writer.pack(idx);
    writer.pack((int)width());
    char types[scm.width() + 1];
    scm.c_str(types);
    writer.pack(types);
    for (int i = 0; i < width(); i++) {
      switch (types[i]) {
      case 'I': {
        writer.pack(get<int>(i));
        break;
      }
      case 'B': {
        writer.pack(get<bool>(i));
        break;
      }
      case 'F': {
        writer.pack(get<float>(i));
        break;
      }
      case 'S': {
        writer.pack(get<string *>(i));
        break;
      }
      default:
        assert(false); // unknown type
      }
    }
  }

  /**
   * allocates a Row based on the next bytes in the reader
   */
  static Row *unpack(const Schema &scm, BytesReader &reader) {
    assert(reader.yield<char>() == 'R');
    int idx = reader.yield<int>();
    int width = reader.yield<int>();

    /* collect types for schema */
    char types[width + 1];
    reader.yield_c_arr(types);

    /* assert unpacking correct schema */
    assert(scm.width() == width);
    for (int i = 0; i < width; i++) {
      assert(Data::char_as_type(types[i]) == scm.col_type(i));
    }

    Row *row = new Row(idx, scm);

    /* collect data based on schema */
    for (int i = 0; i < width; ++i) {
      switch (types[i]) {
      case 'B': {
        row->set(i, reader.yield<bool>());
        break;
      }
      case 'I': {
        row->set(i, reader.yield<int>());
        break;
      }
      case 'F': {
        row->set(i, reader.yield<float>());
        break;
      }
      case 'S': {
        // TODO: Not clear who owns this allocated string
        row->set(i, reader.yield_string_ptr());
        break;
      }
      default:
        assert(false); // unknown type in row
      }
    }
    return row;
  }

  static Row *unpack(BytesReader &reader) {
    assert(reader.yield<char>() == 'R');
    int idx = reader.yield<int>();
    int width = reader.yield<int>();

    /* collect types for schema */
    char types[width + 1];
    reader.yield_c_arr(types);
    Schema *scm = new Schema(types);
    Row *row = new Row(idx, *scm);

    /* collect data based on schema */
    for (int i = 0; i < width; ++i) {
      switch (scm->col_type(i)) {
      case Data::Type::BOOL: {
        row->set(i, reader.yield<bool>());
        break;
      }
      case Data::Type::INT: {
        row->set(i, reader.yield<int>());
        break;
      }
      case Data::Type::FLOAT: {
        row->set(i, reader.yield<float>());
        break;
      }
      case Data::Type::STRING: {
        // TODO: Not clear who owns this allocated string
        row->set(i, reader.yield_string_ptr());
        break;
      }
      default:
        assert(false); // unknown type in row
      }
    }
    return row;
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
