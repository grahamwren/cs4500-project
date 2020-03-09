#pragma once

#include "../lib.h"
#include "column_ptr_list.h"

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
public:
  /**
   * RowerWorker: a class which encapsulates a Rower in a Thread
   */
  class RowerWorker : public Thread {
  public:
    DataFrame &_data_frame;
    Rower *_rower;
    int _start_i;
    int _end_i;

    RowerWorker(DataFrame &df, Rower *r, int start_i, int end_i)
        : _data_frame(df), _rower(r), _start_i(start_i), _end_i(end_i) {}

    void run() {
      Row r(_data_frame.get_schema());
      for (int i = _start_i; i < _end_i; i++) {
        _data_frame.fill_row(i, r);
        _rower->accept(r);
      }
    }

    Rower *rower() { return _rower; }
  };

  Schema *_schema;
  ColumnPtrList _columns;

  /**
   * Create a data frame with the same columns as the given df but with no
   * rows or rownames
   */
  DataFrame(DataFrame &df) : DataFrame(df.get_schema(), true) {}

  /**
   * Create a data frame from a schema and columns. All columns are created
   * empty.
   */
  DataFrame(Schema &schema) : DataFrame(schema, false) {}

  DataFrame(Schema &schema, bool dropRownames) {
    _schema = new Schema(schema.width(), schema.length());
    int num_cols = schema.width();
    for (int i = 0; i < num_cols; ++i) {
      char type = schema.col_type(i);
      String *name = schema.col_name(i);
      _schema->add_column(type, name);
      _init_col(type);
    }

    if (dropRownames)
      return;

    for (int i = 0; i < schema.length(); i++) {
      String *name = schema.row_name(i);
      _schema->add_row(name);
    }
  }

  ~DataFrame() { delete _schema; }

  void _init_col(char type) {
    if (type == 'S') {
      _columns.push(new StringColumn());
    } else if (type == 'F') {
      _columns.push(new FloatColumn());
    } else if (type == 'I') {
      _columns.push(new IntColumn());
    } else {
      _columns.push(new BoolColumn());
    }
  }

  /**
   * Returns the dataframe's schema. Modifying the schema after a dataframe
   * has been created is undefined.
   */
  Schema &get_schema() { return *_schema; }

  /**
   * Adds a column this dataframe, updates the schema, the new column
   * is external, and appears as the last column of the dataframe, the
   * name is optional and external. A nullptr colum is undefined.
   */
  void add_column(Column *col, String *name) {
    _columns.push(col);
    _schema->add_column(col->get_type(), name);
  }

  /**
   * Return the value at the given column and row. Accessing rows or
   * columns out of bounds, or request the wrong type is undefined.
   */
  int get_int(size_t col, size_t row) {
    assert(col < _columns.size());
    IntColumn *the_column = _columns.get(col)->as_int();
    assert(row < the_column->size());
    return the_column->get(row);
  }
  bool get_bool(size_t col, size_t row) {
    assert(col < _columns.size());
    BoolColumn *the_column = _columns.get(col)->as_bool();
    assert(row < the_column->size());
    return the_column->get(row);
  }
  float get_float(size_t col, size_t row) {
    assert(col < _columns.size());
    FloatColumn *the_column = _columns.get(col)->as_float();
    assert(row < the_column->size());
    return the_column->get(row);
  }
  String *get_string(size_t col, size_t row) {
    assert(col < _columns.size());
    StringColumn *the_column = _columns.get(col)->as_string();
    assert(row < the_column->size());
    return the_column->get(row);
  }

  /**
   * Return the offset of the given column name or -1 if no such col.
   */
  int get_col(String &col) {
    return _schema->col_idx(const_cast<const char *>(col.c_str()));
  }

  /**
   * Return the offset of the given row name or -1 if no such row.
   */
  int get_row(String &col) {
    return _schema->row_idx(const_cast<const char *>(col.c_str()));
  }

  /**
   * Set the value at the given column and row to the given value.
   * If the column is not  of the right type or the indices are out of
   * bound, the result is undefined.
   */
  void set(size_t i, size_t row, int val) {
    assert(i < _columns.size());
    IntColumn *col = _columns.get(i)->as_int();
    assert(row < col->size());
    col->set(row, val);
  }
  void set(size_t i, size_t row, bool val) {
    assert(i < _columns.size());
    BoolColumn *col = _columns.get(i)->as_bool();
    assert(row < col->size());
    col->set(row, val);
  }
  void set(size_t i, size_t row, float val) {
    assert(i < _columns.size());
    FloatColumn *col = _columns.get(i)->as_float();
    assert(row < col->size());
    col->set(row, val);
  }
  void set(size_t i, size_t row, String *val) {
    assert(i < _columns.size());
    StringColumn *col = _columns.get(i)->as_string();
    assert(row < col->size());
    col->set(row, val);
  }

  /**
   * Set the fields of the given row object with values from the columns at
   * the given offset.  If the row is not form the same schema as the
   * dataframe, results are undefined.
   */
  void fill_row(size_t idx, Row &row) {
    assert(row.width() == _schema->width());
    assert(row.width() == ncols());
    int len = ncols();
    row.set_idx(idx);
    for (int i = 0; i < len; ++i) {
      char type = _schema->col_type(i);
      Column *col = _columns.get(i);
      if (type == 'S') {
        row.set(i, col->as_string()->get(idx));
      } else if (type == 'I') {
        row.set(i, col->as_int()->get(idx));
      } else if (type == 'F') {
        row.set(i, col->as_float()->get(idx));
      } else { // bool
        row.set(i, col->as_bool()->get(idx));
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
    assert(row.width() == _schema->width());
    assert(row.width() == ncols());
    _add_row(row);
  }

  void _add_row(Row &row) {
    int num_cols = _schema->width();
    for (int i = 0; i < num_cols; ++i) {
      char type = _schema->col_type(i);
      Column *col = _columns.get(i);
      if (type == 'S') {
        col->as_string()->push_back(row.get_string(i));
      } else if (type == 'I') {
        col->as_int()->push_back(row.get_int(i));
      } else if (type == 'F') {
        col->as_float()->push_back(row.get_float(i));
      } else { // bool
        col->as_bool()->push_back(row.get_bool(i));
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

    return _columns.get(0)->size();
  }

  /**
   * The number of columns in the dataframe.
   */
  size_t ncols() { return _columns.size(); }

  /**
   * Visit rows in order.
   */
  void map(Rower &r) {
    size_t num_rows = nrows();
    Row row(*_schema);
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

    size_t num_rows = nrows();
    for (size_t i = 0; i < num_rows; ++i) {
      Row row(*_schema);
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

    for (size_t row = 0; row < length; ++row) {
      Row the_row(*_schema);
      fill_row(row, the_row);
      for (size_t col = 0; col < width; ++col) {
        p("<");
        char type = _schema->col_type(col);
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

    if (!_schema->equals(other->_schema)) {
      return false;
    }

    int len = ncols();
    int other_len = other->ncols();

    if (len != other_len)
      return false;

    for (int i = 0; i < len; ++i) {
      Column *col = _columns.get(i);
      Column *other_col = other->_columns.get(i);
      if (!col->equals(other_col))
        return false;
    }

    return true;
  }
};
