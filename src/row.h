#pragma once

#include "bytes_reader.h"
#include "bytes_writer.h"
#include "data.h"
#include "object.h"
#include "schema.h"
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
  const int _width = 0;
  const Schema &scm;
  Data *data;
  int idx = 0;

public:
  /**
   * Build a row following a schema with a start index.
   */
  Row(int i, const Schema &scm) : _width(scm.width()), scm(scm), idx(i) {
    data = new Data[width()];
  }

  /**
   * Build a row following a schema with default start index.
   */
  Row(const Schema &scm) : Row(0, scm) {}

  /**
   * copy constructor
   */
  Row(const Row &o) : Row(o.idx, o.scm) {
    memcpy(data, o.data, width() * sizeof(Data));
  }

  ~Row() { delete[] data; }

  /**
   * Number of fields in the row.
   */
  int width() const { return _width; }

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
    data[col].set(val);
  }

  bool equals(const Row *other) const {
    if (other == nullptr || other->width() != width() ||
        !scm.equals(other->get_schema())) {
      return false;
    }

    for (int i = 0; i < width(); ++i) {
      char type = scm.col_type(i);
      if (is_missing(i) && other->is_missing(i)) {
        continue;
      } else if (is_missing(i) || other->is_missing(i)) {
        return false;
      } else if (type == 'B') {
        if (get<bool>(i) != other->get<bool>(i))
          return false;
      } else if (type == 'I') {
        if (get<int>(i) != other->get<int>(i))
          return false;
      } else if (type == 'F') {
        if (get<float>(i) != other->get<float>(i))
          return false;
      } else if (type == 'S') {
        string *s = get<string *>(i);
        string *other_s = other->get<string *>(i);
        if (s == nullptr || other_s == nullptr)
          return s == other_s;
        else {
          return s->compare(*other_s) == 0;
        }
      }
    }

    return true;
  }

  void pack(BytesWriter &writer) {
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
    assert(reader.yield_char() == 'R');
    int idx = reader.yield_int();
    int width = reader.yield_int();

    /* collect types for schema */
    char types[width + 1];
    reader.yield_c_arr(types);

    /* assert unpacking correct schema */
    assert(scm.width() == width);
    for (int i = 0; i < width; i++) {
      assert(types[i] == scm.col_type(i));
    }

    Row *row = new Row(idx, scm);

    /* collect data based on schema */
    for (int i = 0; i < width; ++i) {
      switch (scm.col_type(i)) {
      case 'B': {
        row->set(i, reader.yield_bool());
        break;
      }
      case 'I': {
        row->set(i, reader.yield_int());
        break;
      }
      case 'F': {
        row->set(i, reader.yield_float());
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
    assert(reader.yield_char() == 'R');
    int idx = reader.yield_int();
    int width = reader.yield_int();

    /* collect types for schema */
    char types[width + 1];
    reader.yield_c_arr(types);
    Schema *scm = new Schema(types);
    Row *row = new Row(idx, *scm);

    /* collect data based on schema */
    for (int i = 0; i < width; ++i) {
      switch (scm->col_type(i)) {
      case 'B': {
        row->set(i, reader.yield_bool());
        break;
      }
      case 'I': {
        row->set(i, reader.yield_int());
        break;
      }
      case 'F': {
        row->set(i, reader.yield_float());
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

  void print(bool with_meta = true) {
    if (with_meta) {
      cout << "Row<" << this << ">(scm: '";
      for (int i = 0; i < width(); i++) {
        cout << scm.col_type(i);
      }
      cout << "', data: [";
    }

    for (int i = 0; i < width(); i++) {
      cout << '<';
      switch (scm.col_type(i)) {
      case 'I':
        cout << get<int>(i);
        break;
      case 'F':
        cout << get<float>(i);
        break;
      case 'B':
        cout << get<bool>(i);
        break;
      case 'S':
        cout << '"' << get<string *>(i)->c_str() << '"';
        break;
      }
      cout << '>';
    }
    if (with_meta) {
      cout << "])";
    }
    cout << endl;
  }
};
