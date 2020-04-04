#pragma once

#include "column.h"
#include "data.h"
#include "dataframe.h"
#include <algorithm>
#include <memory>
#include <vector>

#ifndef DF_CHUNK_SIZE
#define DF_CHUNK_SIZE (4096 * 2)
#endif

using namespace std;

class DataFrameChunk : public DataFrame {
private:
  const Schema &schema;
  int start_idx;
  vector<unique_ptr<Column>> columns;

public:
  DataFrameChunk(const Schema &scm, int start_i)
      : schema(scm), start_idx(start_i) {
    assert(schema.width());
    columns.reserve(schema.width());
    for (int i = 0; i < schema.width(); i++) {
      columns.emplace_back(Column::create(scm.col_type(i)));
    }
  }

  DataFrameChunk(const Schema &scm) : DataFrameChunk(scm, 0) {}

  DataFrameChunk(DataFrameChunk &&) noexcept = default;
  DataFrameChunk(const DataFrameChunk &) = delete;

  const Schema &get_schema() const { return schema; }

  int get_start() const { return start_idx; }
  int chunk_idx() const { return get_start() / DF_CHUNK_SIZE; }

  void fill(ReadCursor &rc) {
    start_idx = yield<int>(rc);
    int len = yield<int>(rc);
    /* fill each column from left to right */
    for (int i = 0; i < schema.width(); i++) {
      columns[i]->fill(len, rc);
    }
  }

  void serialize(WriteCursor &wc) const {
    pack(wc, get_start());
    int len = nrows();
    pack(wc, len);
    for (int i = 0; i < schema.width(); i++) {
      assert(columns[i]->length() == len);
      columns[i]->serialize(wc);
    }
  }

  int get_int(int y, int x) const {
    assert(y >= get_start());          // assert within this chunk
    assert(y < nrows() + get_start()); // assert within this chunk
    assert(x >= 0);
    assert(x < ncols());

    const unique_ptr<Column> &col = columns[x];
    assert(!col->is_missing(y - get_start()));
    return col->get_int(y - get_start());
  }

  float get_float(int y, int x) const {
    assert(y >= get_start());          // assert within this chunk
    assert(y < nrows() + get_start()); // assert within this chunk
    assert(x >= 0);
    assert(x < ncols());

    const unique_ptr<Column> &col = columns[x];
    assert(!col->is_missing(y - get_start()));
    return col->get_float(y - get_start());
  }

  bool get_bool(int y, int x) const {
    assert(y >= get_start());          // assert within this chunk
    assert(y < nrows() + get_start()); // assert within this chunk
    assert(x >= 0);
    assert(x < ncols());

    const unique_ptr<Column> &col = columns[x];
    assert(!col->is_missing(y - get_start()));
    return col->get_bool(y - get_start());
  }

  string *get_string(int y, int x) const {
    assert(y >= get_start());          // assert within this chunk
    assert(y < nrows() + get_start()); // assert within this chunk
    assert(x >= 0);
    assert(x < ncols());

    const unique_ptr<Column> &col = columns[x];
    assert(!col->is_missing(y - get_start()));
    return col->get_string(y - get_start());
  }

  bool is_missing(int y, int x) const {
    assert(y >= get_start());          // assert within this chunk
    assert(y < nrows() + get_start()); // assert within this chunk
    assert(x >= 0);
    assert(x < ncols());

    const unique_ptr<Column> &col = columns[x];
    return col->is_missing(y - get_start());
  }

  bool is_full() const { return columns[0]->length() >= DF_CHUNK_SIZE; }

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

  int nrows() const { return columns[0]->length(); }

  void map(Rower &rower) const {
    Row row(get_schema());
    for (int i = get_start(); i < get_start() + nrows(); i++) {
      fill_row(i, row);
      rower.accept(row);
    }
  }

  bool operator==(const DataFrameChunk &other) const {
    return start_idx == other.start_idx && nrows() == other.nrows() &&
           get_schema() == other.get_schema() &&
           equal(columns.begin(), columns.end(), other.columns.begin(),
                 [](const unique_ptr<Column> &l, const unique_ptr<Column> &r) {
                   return l->equals(*r);
                 });
  }
};
