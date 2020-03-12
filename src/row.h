#pragma once

// lang::CwC

#include "bytes_reader.h"
#include "bytes_writer.h"
#include "fielder.h"
#include "object.h"
#include "schema.h"
#include <stdlib.h>

/*************************************************************************
 * Row::
 * This class represents a single row of _data constructed according to a
 * _dataframe's schema. The purpose of this class is to make it easier to add
 * read/write complete rows. Internally a _dataframe hold _data in columns.
 * Rows have pointer equality.
 *
 * authors: @grahamwren, @jagen31
 */
class Row : public Object {
public:
  union Data {
    bool b;
    int i;
    float f;
    string *s;
  };

  int _width;
  int _idx = 0;
  char *_types;
  Data *_data = nullptr;

  /**
   * Build a row following a schema.
   */
  Row(Schema &scm) {
    _width = scm.width();
    _types = new char[_width];
    _data = new Data[_width];

    for (size_t i = 0; i < _width; ++i) {
      _types[i] = scm.col_type(i);
    }
  }

  /**
   * Build row from members
   */
  Row(int w, int idx, char *types, Data *data)
      : _width(w), _idx(idx), _types(types), _data(data) {}

  /**
   * allocate space for empty row
   */
  Row() : Row(0, 0, nullptr, nullptr) {}

  Row(Row &&o) : Row(o._width, o._idx, o._types, o._data) {
    o._types = nullptr;
    o._data = nullptr;
  }

  ~Row() {
    delete[] _types;
    delete[] _data;
  }

  /**
   * Setters: set the given column with the given value. Setting a column with
   * a value of the wrong type is undefined.
   */
  void set(size_t col, int val) {
    assert(col < width());
    _data[col].i = val;
  }

  void set(size_t col, float val) {
    assert(col < width());
    _data[col].f = val;
  }

  void set(size_t col, bool val) {
    assert(col < width());
    _data[col].b = val;
  }

  /**
   * Acquire ownership of the string.
   */
  void set(size_t col, string *val) {
    assert(col < width());
    _data[col].s = val;
  }

  /**
   * Set/get the index of this row (ie. its position in the _dataframe. This is
   * only used for informational purposes, unused otherwise
   */
  void set_idx(size_t idx) { _idx = idx; }

  size_t get_idx() { return _idx; }

  /**
   * Getters: get the value at the given column. If the column is not
   * of the requested type, the result is undefined.
   */
  int get_int(size_t col) { return _data[col].i; }

  bool get_bool(size_t col) { return _data[col].b; }

  float get_float(size_t col) { return _data[col].f; }

  string *get_string(size_t col) { return _data[col].s; }
  /**
   * Number of fields in the row.
   */
  size_t width() { return _width; }

  /**
   * Type of the field at the given position. An idx >= width is  undefined.
   */
  char col_type(size_t idx) { return _types[idx]; }

  /**
   * Given a Fielder, visit every field of this row. The first argument is
   * index of the row in the _dataframe.
   * Calling this method before the row's fields have been set is undefined.
   */
  void visit(size_t idx, Fielder &f) {
    f.start(idx);
    for (size_t i = 0; i < width(); ++i) {
      char type = _types[i];
      Data value = _data[i];
      if (type == 'I') {
        f.accept(value.i);
      } else if (type == 'F') {
        f.accept(value.f);
      } else if (type == 'B') {
        f.accept(value.b);
      } else if (type == 'S') {
        f.accept(value.s);
      }
    }
    f.done();
  }

  bool equals(Object *o) {
    Row *other = dynamic_cast<Row *>(o);

    if (other == nullptr) {
      return false;
    }

    if (other->_width != _width) {
      return false;
    }

    for (int i = 0; i < _width; ++i) {
      char type = col_type(i);
      if (type != other->col_type(i)) {
        return false;
      }
      if (type == 'B') {
        if (get_bool(i) != other->get_bool(i)) {
          return false;
        }
      } else if (type == 'I') {
        if (get_int(i) != other->get_int(i)) {
          return false;
        }
      } else if (type == 'F') {
        if (get_float(i) != other->get_float(i)) {
          return false;
        }
      } else {
        if (get_string(i)->compare(*other->get_string(i)) != 0) {
          return false;
        }
      }
    }

    return true;
  }

  void pack(BytesWriter &writer) {
    writer.pack('R');
    writer.pack((int)width());
    writer.pack(width(), _types);
    for (int i = 0; i < width(); i++) {
      switch (_types[i]) {
      case 'I': {
        writer.pack(get_int(i));
        break;
      }
      case 'B': {
        writer.pack(get_bool(i));
        break;
      }
      case 'F': {
        writer.pack(get_float(i));
        break;
      }
      case 'S': {
        writer.pack(get_string(i));
        break;
      }
      default:
        assert(false); // unknown type
      }
    }
  }

  static Row unpack(BytesReader &reader) {
    assert(reader.yield_char() == 'R');
    int width = reader.yield_int();

    /* collect types for schema */
    char *types = new char[width + 1];
    reader.yield_c_arr(types);

    /* collect data based on schema */
    Data *data = new Data[width];
    for (int i = 0; i < width; ++i) {
      switch (types[i]) {
      case 'B': {
        data[i].b = reader.yield_bool();
        break;
      }
      case 'I': {
        data[i].i = reader.yield_int();
        break;
      }
      case 'F': {
        data[i].f = reader.yield_float();
        break;
      }
      case 'S': {
        // TODO: Not clear who owns this allocated string
        data[i].s = reader.yield_string_ptr();
        break;
      }
      default:
        assert(false); // unknown type in row
      }
    }

    Row r(width, 0, types, data);
    return r; // move
  }

  void debug() {
    p("Row<").p(this).p(">(types: '");
    for (int i = 0; i < width(); i++) {
      p(_types[i]);
    }
    p("', data: [");

    for (int i = 0; i < width(); i++) {
      p("<");
      switch (col_type(i)) {
      case 'I':
        p(get_int(i));
        break;
      case 'F':
        p(get_float(i));
        break;
      case 'B':
        p(get_bool(i));
        break;
      case 'S':
        p("\"").p(get_string(i)->c_str()).p("\"");
        break;
      }
      p(">");
    }
    pln("])");
  }
};
