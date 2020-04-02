#pragma once

#include "column.h"
#include "data.h"
#include "dataframe.h"
#include <vector>

#ifndef DF_CHUNK_SIZE
#define DF_CHUNK_SIZE 4096
#endif

using namespace std;

class DataFrameChunk : public DataFrame {
private:
  const Schema &schema;
  vector<unique_ptr<Column>> columns;

public:
  DataFrameChunk(const Schema &scm) : schema(scm) {
    columns.reserve(schema.width());
    for (int i = 0; i < schema.width(); i++) {
      columns.emplace_back(Column::create(scm.col_type(i)));
    }
  }

  const Schema &get_schema() const { return schema; }

  void fill(ReadCursor &rc) {
    int len = yield<int>(rc);
    /* fill each column from left to right */
    for (int i = 0; i < schema.width(); i++) {
      columns[i]->fill(len, rc);
    }
  }

  void serialize(WriteCursor &wc) const {
    int len = nrows();
    pack(wc, len);
    for (int i = 0; i < schema.width(); i++) {
      assert(columns[i]->length() == len);
      columns[i]->serialize(wc);
    }
  }

  int get_int(int y, int x) const {
    assert(y >= 0);
    assert(y < nrows());
    assert(x >= 0);
    assert(x < ncols());

    const unique_ptr<Column> &col = columns[x];
    assert(!col->is_missing(y));
    return col->get_int(y);
  }

  float get_float(int y, int x) const {
    assert(y >= 0);
    assert(y < nrows());
    assert(x >= 0);
    assert(x < ncols());

    const unique_ptr<Column> &col = columns[x];
    assert(!col->is_missing(y));
    return col->get_float(y);
  }

  bool get_bool(int y, int x) const {
    assert(y >= 0);
    assert(y < nrows());
    assert(x >= 0);
    assert(x < ncols());

    const unique_ptr<Column> &col = columns[x];
    assert(!col->is_missing(y));
    return col->get_bool(y);
  }

  string *get_string(int y, int x) const {
    assert(y >= 0);
    assert(y < nrows());
    assert(x >= 0);
    assert(x < ncols());

    const unique_ptr<Column> &col = columns[x];
    assert(!col->is_missing(y));
    return col->get_string(y);
  }

  bool is_missing(int y, int x) const {
    assert(y >= 0);
    assert(y < nrows());
    assert(x >= 0);
    assert(x < ncols());

    const unique_ptr<Column> &col = columns[x];
    return col->is_missing(y);
  }

  bool is_full() const {
    assert(columns.size() > 0);
    return columns[0]->length() >= DF_CHUNK_SIZE;
  }

  void add_row(const Row &row) {
    assert(!is_full());
    for (int i = 0; i < schema.width(); i++) {
      unique_ptr<Column> &col = columns[i];
      if (row.is_missing(i)) {
        col->push();
      } else {
        switch (schema.col_type(i)) {
        case Data::Type::INT:
          col->push(row.get<int>(i));
          break;
        case Data::Type::FLOAT:
          col->push(row.get<float>(i));
          break;
        case Data::Type::BOOL:
          col->push(row.get<bool>(i));
          break;
        case Data::Type::STRING:
          col->push(row.get<string *>(i));
          break;
        default:
          assert(false);
        }
      }
    }
  }

  int nrows() const {
    if (columns.size())
      return columns[0]->length();
    return 0;
  }
};
