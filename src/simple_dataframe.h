#pragma once

#include "column.h"
#include "data.h"
#include "dataframe.h"
#include "row.h"
#include "rower.h"
#include "schema.h"
#include "thread.h"
#include <cassert>
#include <cmath>
#include <thread>

using namespace std;

#ifndef THREAD_COUNT
#define THREAD_COUNT std::thread::hardware_concurrency()
#endif

class Row;
class Column;
class Data;

/****************************************************************************
 * SimpleDataFrame::
 *
 * A DataFrame is table composed of columns of equal length. Each column
 * holds values of the same type (I, S, B, F). A dataframe has a schema that
 * describes it.
 * authors: @grahamwren, jagen31
 */
class SimpleDataFrame : public DataFrame {
protected:
  Schema schema;
  vector<Column *> columns;

  void init_col(Data::Type t) { columns.push_back(Column::create(t)); }

  /**
   * Set the fields of the given row object with values from the columns at
   * the given offset.  If the row is not form the same schema as the
   * dataframe, results are undefined.
   *
   * TODO: delete this in favor of VirtualRow
   */
  void fill_row(int idx, Row &row) const {
    assert(row.width() == schema.width());
    assert(row.width() == ncols());
    int len = ncols();
    row.set_idx(idx);
    for (int i = 0; i < len; ++i) {
      Data::Type type = schema.col_type(i);
      const Column &col = *columns[i];
      if (col.is_missing(idx))
        row.set_missing(i);
      else {
        switch (type) {
        case Data::Type::INT:
          row.set(i, col.get_int(idx));
          break;
        case Data::Type::FLOAT:
          row.set(i, col.get_float(idx));
          break;
        case Data::Type::BOOL:
          row.set(i, col.get_bool(idx));
          break;
        case Data::Type::STRING:
          row.set(i, col.get_string(idx));
          break;
        case Data::Type::MISSING:
          row.set_missing(i);
          break;
        default:
          assert(false); // unsupported column type
        }
      }
    }
  }

public:
  SimpleDataFrame() = default;

  /**
   * Create a data frame with the same columns as the given df but with no
   * rows or rownames
   */
  SimpleDataFrame(const SimpleDataFrame &df)
      : SimpleDataFrame(df.get_schema()) {}

  /**
   * Create a data frame from a schema and columns. All columns are created
   * empty. Copies passed in schema, takes ownership of column names.
   */
  SimpleDataFrame(const Schema &scm) : schema(scm) {
    for (int i = 0; i < scm.width(); i++) {
      init_col(scm.col_type(i));
    }
  }

  ~SimpleDataFrame() {
    for (int i = 0; i < ncols(); i++) {
      delete columns[i];
      columns[i] = nullptr;
    }
  }

  /**
   * Returns the dataframe's schema. Modifying the schema after a dataframe
   * has been created is undefined.
   */
  const Schema &get_schema() const { return schema; }

  /**
   * Add a row at the end of this dataframe. The row is expected to have
   * the right schema and be filled with values, otherwise undefined.
   */
  void add_row(const Row &row) {
    assert(row.width() == schema.width());
    assert(row.width() == ncols());
    int num_cols = schema.width();
    for (int i = 0; i < num_cols; ++i) {
      Column &col = *columns[i];
      if (row.is_missing(i)) {
        col.push();
      } else {
        Data::Type type = schema.col_type(i);
        switch (type) {
        case Data::Type::INT:
          col.push(row.get<int>(i));
          break;
        case Data::Type::BOOL:
          col.push(row.get<bool>(i));
          break;
        case Data::Type::FLOAT:
          col.push(row.get<float>(i));
          break;
        case Data::Type::STRING:
          col.push(row.get<string *>(i));
          break;
        case Data::Type::MISSING:
          col.push();
          break;
        default:
          assert(false);
        }
      }
    }
  }

  /**
   * Set the value at the given column and row to the given value.
   * If the column is not  of the right type or the indices are out of
   * bound, the result is undefined.
   */
  virtual void set(int y, int x, int val) {
    assert(x < ncols());
    assert(x >= 0);
    columns[x]->set(y, val);
  }
  virtual void set(int y, int x, float val) {
    assert(x < ncols());
    assert(x >= 0);
    columns[x]->set(y, val);
  }
  virtual void set(int y, int x, bool val) {
    assert(x < ncols());
    assert(x >= 0);
    columns[x]->set(y, val);
  }
  virtual void set(int y, int x, string *val) {
    assert(x < ncols());
    assert(x >= 0);
    columns[x]->set(y, val);
  }
  /* set missing at y, x */
  virtual void set(int y, int x) {
    assert(x < ncols());
    assert(x >= 0);
    columns[x]->set(y);
  }

  /**
   * Return the value at the given column and row. Accessing rows or
   * columns out of bounds, or request the wrong type is undefined.
   */
  virtual int get_int(int y, int x) const {
    assert(x < ncols());
    assert(x >= 0);
    return columns[x]->get_int(y);
  }
  virtual float get_float(int y, int x) const {
    assert(x < ncols());
    assert(x >= 0);
    return columns[x]->get_float(y);
  }
  virtual bool get_bool(int y, int x) const {
    assert(x < ncols());
    assert(x >= 0);
    return columns[x]->get_bool(y);
  }
  virtual string *get_string(int y, int x) const {
    assert(x < ncols());
    assert(x >= 0);
    return columns[x]->get_string(y);
  }
  virtual bool is_missing(int y, int x) const {
    assert(x < ncols());
    assert(x >= 0);
    return columns[x]->is_missing(y);
  }

  /**
   * The number of rows in the dataframe.
   * Uses the first column length because from assignment.
   */
  int nrows() const {
    if (ncols() < 1)
      return 0;

    return columns[0]->length();
  }

  /**
   * The number of columns in the dataframe.
   */
  int ncols() const { return columns.size(); }

  /**
   * Visit rows in order.
   */
  void map(Rower &r) const {
    int num_rows = nrows();
    Row row(schema);
    for (int i = 0; i < num_rows; ++i) {
      fill_row(i, row);
      r.accept(row);
    }
  }

  DataFrame *filter(Rower &r) const {
    DataFrame *result = new SimpleDataFrame(get_schema());
    int num_rows = nrows();
    Row row(schema);
    for (int i = 0; i < num_rows; ++i) {
      fill_row(i, row);
      if (r.accept(row))
        result->add_row(row);
    }
    return result;
  }

  /**
   * This method clones the Rower and executes the map in parallel. Join is
   * used at the end to merge the results.
   */
  void pmap(Rower &r) const {
    // TODO: VirtualRow
  }
};
