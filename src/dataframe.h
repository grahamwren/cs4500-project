#pragma once

#include "column.h"
#include "list.h"
#include "row.h"
#include "rower.h"
#include "schema.h"
#include "thread.h"
#include <cmath>

using namespace std;

#ifndef THREAD_COUNT
#define THREAD_COUNT 8
#endif

/****************************************************************************
 * DataFrame::
 *
 * A DataFrame is table composed of columns of equal length. Each column
 * holds values of the same type (I, S, B, F). A dataframe has a schema that
 * describes it.
 * authors: @grahamwren, jagen31
 */
class DataFrame : public Object {
  Schema schema;
  List<Column *> columns;

public:
  /**
   * RowerWorker: a class which encapsulates a Rower in a Thread
   */
  class RowerWorker : public Thread {
    DataFrame &data_frame;
    Rower *rowr;
    int start_i;
    int end_i;

  public:
    RowerWorker(DataFrame &df, Rower *r, int start_i, int end_i)
        : data_frame(df), rowr(r), start_i(start_i), end_i(end_i) {}

    void run() {
      Row r(data_frame.get_schema());
      for (int i = start_i; i < end_i; i++) {
        data_frame.fill_row(i, r);
        rowr->accept(r);
      }
    }

    Rower *rower() { return rowr; }
  };

  DataFrame() = default;

  /**
   * Create a data frame with the same columns as the given df but with no
   * rows or rownames
   */
  DataFrame(DataFrame &df) : DataFrame(df.get_schema()) {}

  /**
   * Create a data frame from a schema and columns. All columns are created
   * empty.
   */
  DataFrame(Schema &scm) : schema(scm) {}

  void init_col(char tc) { columns.push(new Column(Data::char_as_type(tc))); }

  /**
   * Returns the dataframe's schema. Modifying the schema after a dataframe
   * has been created is undefined.
   */
  Schema &get_schema() { return schema; }

  /**
   * Adds a column this dataframe, updates the schema, the new column
   * is external, and appears as the last column of the dataframe, the
   * name is optional and external. A nullptr colum is undefined.
   */
  void add_column(Column *col, string *name) {
    columns.push(col);
    schema.add_column(Data::type_as_char(col->type), name);
  }

  /**
   * Return the value at the given column and row. Accessing rows or
   * columns out of bounds, or request the wrong type is undefined.
   */
  template <typename T> T get(int col, int row) {
    assert(col < columns.size());
    return columns.get(col)->template get<T>(row);
  }

  /**
   * Return the offset of the given column name or -1 if no such col.
   */
  int get_col(string &col) {
    return schema.col_idx(const_cast<const char *>(col.c_str()));
  }

  /**
   * Set the value at the given column and row to the given value.
   * If the column is not  of the right type or the indices are out of
   * bound, the result is undefined.
   */
  template <typename T> void set(int col, int row, T val) {
    assert(col < columns.size());
    columns.get(col)->template set<T>(row, val);
  }

  /**
   * Set the fields of the given row object with values from the columns at
   * the given offset.  If the row is not form the same schema as the
   * dataframe, results are undefined.
   */
  void fill_row(size_t idx, Row &row) {
    assert(row.width() == schema.width());
    assert(row.width() == ncols());
    int len = ncols();
    row.set_idx(idx);
    for (int i = 0; i < len; ++i) {
      char type = schema.col_type(i);
      Column *col = columns.get(i);
      if (type == 'S') {
        row.set(i, col->get<string *>(idx));
      } else if (type == 'I') {
        row.set(i, col->get<int>(idx));
      } else if (type == 'F') {
        row.set(i, col->get<float>(idx));
      } else { // bool
        row.set(i, col->get<bool>(idx));
      }
    }
  }

  /**
   * Add a row at the end of this dataframe. The row is expected to have
   * the right schema and be filled with values, otherwise undefined. Does not
   * add a row to the schema because from assignment:
   * "Modifying the schema after a dataframe has been created in undefined"
   *
   * `row` is not used after the function returns.
   */
  void add_row(Row &row) {
    assert(row.width() == schema.width());
    assert(row.width() == ncols());
    int num_cols = schema.width();
    for (int i = 0; i < num_cols; ++i) {
      char type = schema.col_type(i);
      Column *col = columns.get(i);
      if (type == 'S') {
        col->push<string *>(row.get_string(i));
      } else if (type == 'I') {
        col->push<int>(row.get_int(i));
      } else if (type == 'F') {
        col->push<float>(row.get_float(i));
      } else if (type == 'B') {
        col->push<bool>(row.get_bool(i));
      } else {
        assert(false);
      }
    }
  }

  /**
   * The number of rows in the dataframe.
   * Uses the first column length because from assignment.
   */
  size_t nrows() {
    if (ncols() < 1)
      return 0;

    return columns.get(0)->size();
  }

  /**
   * The number of columns in the dataframe.
   */
  size_t ncols() { return columns.size(); }

  /**
   * Visit rows in order.
   */
  void map(Rower &r) {
    size_t num_rows = nrows();
    Row row(schema);
    for (size_t i = 0; i < num_rows; ++i) {
      fill_row(i, row);
      r.accept(row);
    }
  }

  /**
   * Create a new dataframe, constructed from rows for which the given Rower
   * returned true from its accept method.
   */
  DataFrame *filter(Rower &r) {
    DataFrame *df = new DataFrame(*this);

    Row row(schema);
    size_t num_rows = nrows();
    for (size_t i = 0; i < num_rows; ++i) {
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
  void print() {
    size_t width = ncols();
    size_t length = nrows();

    Row the_row(schema);
    for (size_t row = 0; row < length; ++row) {
      fill_row(row, the_row);
      for (size_t col = 0; col < width; ++col) {
        p("<");
        char type = schema.col_type(col);
        if (type == 'S') {
          p("\"");
          p(the_row.get_string(col)->c_str());
          p("\"");
        } else if (type == 'F') {
          p(the_row.get_float(col));
        } else if (type == 'B') {
          p(the_row.get_bool(col) ? 1 : 0);
        } else {
          p(the_row.get_int(col));
        }
        p(">");
      }
      pln();
    }
  }

  /**
   * This method clones the Rower and executes the map in parallel. Join is
   * used at the end to merge the results.
   */
  void pmap(Rower &r) {
    int total_rows = nrows();
    int chunk_len = ceil(total_rows / (float)THREAD_COUNT);
    RowerWorker **workers = new RowerWorker *[THREAD_COUNT];
    for (int i = 0; i < THREAD_COUNT; i++) {
      int start_i = fmin((chunk_len * i), total_rows);     // inclusive
      int end_i = fmin((start_i + chunk_len), total_rows); // exclusive
      Rower *rower = dynamic_cast<Rower *>(r.clone());
      workers[i] = new RowerWorker(*this, rower, start_i, end_i);
      workers[i]->start();
    }

    Rower *rowers[THREAD_COUNT];
    for (int i = 0; i < THREAD_COUNT; i++) {
      workers[i]->join();
      rowers[i] = workers[i]->rower();
      delete workers[i];
    }

    Rower *last_rower = nullptr;
    for (int i = THREAD_COUNT - 1; i >= 0; i--) {
      Rower *rower = rowers[i];
      if (last_rower) {
        rower->join_delete(last_rower);
      }
      last_rower = rower;
    }
    if (last_rower) {
      r.join_delete(last_rower);
    }

    delete[] workers;
  }

  bool equals(Object *o) {
    DataFrame *other = dynamic_cast<DataFrame *>(o);
    if (other == nullptr) {
      return false;
    }

    if (!schema.equals(other->schema)) {
      return false;
    }

    int len = ncols();
    int other_len = other->ncols();

    if (len != other_len)
      return false;

    for (int i = 0; i < len; ++i) {
      Column *col = columns.get(i);
      Column *other_col = other->columns.get(i);
      if (!col->equals(*other_col))
        return false;
    }

    return true;
  }
};
