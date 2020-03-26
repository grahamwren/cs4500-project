#pragma once

#ifndef THREAD_COUNT
#define THREAD_COUNT std::thread::hardware_concurrency()
#endif

#include "row.h"
#include "schema.h"

class Rower;

using namespace std;

/****************************************************************************
 * DataFrame::
 *
 * A DataFrame is table composed of columns of equal length. Each column
 * holds values of the same type (I, S, B, F). A dataframe has a schema that
 * describes it.
 * authors: @grahamwren, jagen31
 */
class DataFrame {

public:

  virtual ~DataFrame() {}
  /**
   * Returns the dataframe's schema. Modifying the schema after a dataframe
   * has been created is undefined.
   */
  virtual const Schema &get_schema() const = 0;

  /**
   * Add a row at the end of this dataframe. The row is expected to have
   * the right schema and be filled with values, otherwise undefined.
   */
  virtual void add_row(const Row &row) = 0;

  virtual void fill_row(int, Row &) const = 0;

  /**
   * Set the value at the given column and row to the given value.
   * If the column is not  of the right type or the indices are out of
   * bound, the result is undefined.
   *
   * not necessarily implemented by children
   */
  virtual void set(int y, int x, int) { assert(false); }
  virtual void set(int y, int x, float) { assert(false); }
  virtual void set(int y, int x, bool) { assert(false); }
  virtual void set(int y, int x, string *) { assert(false); }

  /**
   * Return the value at the given column and row. Accessing rows or
   * columns out of bounds, or request the wrong type is undefined.
   */
  virtual int get_int(int y, int x) const = 0;
  virtual float get_float(int y, int x) const = 0;
  virtual bool get_bool(int y, int x) const = 0;
  virtual string *get_string(int y, int x) const = 0;
  virtual bool is_missing(int y, int x) const = 0;

  /**
   * The number of rows in the dataframe.
   * Uses the first column length because from assignment.
   */
  virtual int nrows() const = 0;

  /**
   * The number of columns in the dataframe.
   */
  virtual int ncols() const = 0;

  /**
   * traverses the rows in this DataFrame in order, calling accept on the given
   * Rower with each row
   */
  virtual void map(Rower &rower) const = 0;

  /**
   * builds a new DataFrame based on the rows with the rower returns true from
   * accept
   */
  virtual DataFrame *filter(Rower &rower) const { assert(false); }

  bool equals(const DataFrame &other) const {
    const Schema &schema = get_schema();

    bool accept = true;
    string *s;
    string *os;
    if (schema.equals(other.get_schema()) && nrows() == other.nrows()) {
      for (int x = 0; x < ncols(); x++) {
        Data::Type t = schema.col_type(x);
        for (int y = 0; y < nrows(); y++) {
          if (is_missing(y, x)) {
            accept = other.is_missing(y, x);
          } else {
            switch (t) {
            case Data::Type::INT:
              accept = get_int(y, x) == other.get_int(y, x);
              break;
            case Data::Type::FLOAT:
              accept = get_float(y, x) == other.get_float(y, x);
              break;
            case Data::Type::BOOL:
              accept = get_bool(y, x) == other.get_bool(y, x);
              break;
            case Data::Type::STRING:
              s = get_string(y, x);
              os = other.get_string(y, x);
              accept = s == nullptr || os == nullptr ? s == os
                                                     : s->compare(*os) == 0;
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
      }
      return true;
    } else {
      return false;
    }
  }

  /**
   * print this dataframe in sor format
   */
  void print() const {
    Row r(get_schema());
    for (int y = 0; y < nrows(); y++) {
      fill_row(y, r);
      r.print();
    }
  }
};
