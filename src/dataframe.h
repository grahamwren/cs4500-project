#pragma once

#include "column.h"
#include "data.h"
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
 * DataFrame::
 *
 * A DataFrame is table composed of columns of equal length. Each column
 * holds values of the same type (I, S, B, F). A dataframe has a schema that
 * describes it.
 * authors: @grahamwren, jagen31
 */
class DataFrame {
protected:
  Schema schema;
  vector<Column *> columns;

  void init_col(Data::Type t) {
    switch (t) {
    case Data::Type::INT:
      columns.push_back(new TypedColumn<int>()); // OWNED
      break;
    case Data::Type::FLOAT:
      columns.push_back(new TypedColumn<float>()); // OWNED
      break;
    case Data::Type::BOOL:
      columns.push_back(new TypedColumn<bool>()); // OWNED
      break;
    case Data::Type::STRING:
      columns.push_back(new TypedColumn<string *>()); // OWNED
      break;
    case Data::Type::MISSING:
      columns.push_back(new TypedColumn<bool>());
      break;
    default:
      assert(false); // unsupported column type
    }
  }

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
      Column *col = columns[i];
      switch (type) {
      case Data::Type::INT:
        row.set(i, col->as<int>().get(idx));
        break;
      case Data::Type::FLOAT:
        row.set(i, col->as<float>().get(idx));
        break;
      case Data::Type::BOOL:
        row.set(i, col->as<bool>().get(idx));
        break;
      case Data::Type::STRING:
        row.set(i, col->as<string *>().get(idx));
        break;
      case Data::Type::MISSING:
        row.set_missing(i);
        break;
      default:
        assert(false); // unsupported column type
      }
    }
  }

public:
  DataFrame() = default;

  /**
   * Create a data frame with the same columns as the given df but with no
   * rows or rownames
   */
  DataFrame(const DataFrame &df) : DataFrame(df.get_schema()) {}

  /**
   * Create a data frame from a schema and columns. All columns are created
   * empty. Copies passed in schema, takes ownership of column names.
   */
  DataFrame(const Schema &scm) {
    for (int i = 0; i < scm.width(); i++) {
      add_column(scm.col_type(i), scm.col_name(i));
    }
  }

  virtual ~DataFrame() {
    for (auto it = columns.begin(); it != columns.end(); it++)
      delete *it; // clean-up owned columns
    for (int i = 0; i < schema.width(); i++) {
      delete schema.col_name(i);
    }
  }

  /**
   * Returns the dataframe's schema. Modifying the schema after a dataframe
   * has been created is undefined.
   */
  const Schema &get_schema() const { return schema; }

  /**
   * Adds a column this dataframe, updates the schema, and the new column
   * appears as the last column of the dataframe, the name is optional, but is
   * copied and owned by this DataFrame.
   */
  void add_column(Data::Type type, const string *ext_name) {
    init_col(type);
    string *name = ext_name ? new string(*ext_name) : nullptr;
    schema.add_column(type, name);
  }

  /**
   * Add a row at the end of this dataframe. The row is expected to have
   * the right schema and be filled with values, otherwise undefined.
   */
  void add_row(const Row &row) {
    assert(row.width() == schema.width());
    assert(row.width() == ncols());
    int num_cols = schema.width();
    for (int i = 0; i < num_cols; ++i) {
      Column *col = columns[i];
      if (row.is_missing(i)) {
        col->push_missing();
        continue;
      }
      Data::Type type = schema.col_type(i);
      switch (type) {
      case Data::Type::INT:
        col->as<int>().push(row.get<int>(i));
        break;
      case Data::Type::BOOL:
        col->as<bool>().push(row.get<bool>(i));
        break;
      case Data::Type::FLOAT:
        col->as<float>().push(row.get<float>(i));
        break;
      case Data::Type::STRING:
        col->as<string *>().push(row.get<string *>(i));
        break;
      case Data::Type::MISSING:
      default:
        assert(false);
      }
    }
  }

  /**
   * Set the value at the given column and row to the given value.
   * If the column is not  of the right type or the indices are out of
   * bound, the result is undefined.
   */
  template <typename T> void set(int col, int row, T val) {
    assert(col < columns.size());
    columns[col]->as<T>().set(row, val);
  }

  /**
   * Return the value at the given column and row. Accessing rows or
   * columns out of bounds, or request the wrong type is undefined.
   */
  template <typename T> T get(int col, int row) const {
    assert(col < columns.size());
    return columns[col]->as<T>().get(row);
  }

  /**
   * The number of rows in the dataframe.
   * Uses the first column length because from assignment.
   */
  int nrows() const {
    if (ncols() < 1)
      return 0;

    return columns[0]->size();
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

  /**
   * Create a new dataframe, constructed from rows for which the given Rower
   * returned true from its accept method.
   */
  DataFrame *filter(Rower &r) const {
    DataFrame *df = new DataFrame(*this);

    Row row(schema);
    int num_rows = nrows();
    for (int i = 0; i < num_rows; ++i) {
      fill_row(i, row);
      if (r.accept(row)) {
        df->add_row(row);
      }
    }

    return df;
  }

  /**
   * Print the dataframe in SoR format to standard output.
   */
  void print() const {
    Row row(schema);
    for (int i = 0; i < nrows(); ++i) {
      fill_row(i, row);
      row.print(false);
    }
  }

  /**
   * This method clones the Rower and executes the map in parallel. Join is
   * used at the end to merge the results.
   */
  void pmap(Rower &r) const {
    // TODO: VirtualRow
  }

  bool equals(const DataFrame &other) const {
    if (!schema.equals(other.schema)) {
      return false;
    }

    int len = ncols();
    if (len != other.ncols())
      return false;

    for (int i = 0; i < len; ++i) {
      Column *col = columns[i];
      Column *other_col = other.columns[i];
      if (!col->equals(*other_col))
        return false;
    }

    return true;
  }
};
