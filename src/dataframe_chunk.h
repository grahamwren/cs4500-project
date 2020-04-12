#pragma once

#include "column.h"
#include "data.h"
#include "dataframe.h"
#include <algorithm>
#include <memory>
#include <vector>

#ifndef DF_CHUNK_SIZE
#define DF_CHUNK_SIZE (4096 * 2 * 2 * 2 * 2)
#endif

using namespace std;

class DataFrameChunk : public DataFrame {
private:
  const Schema &schema;
  vector<unique_ptr<Column>> columns;

public:
  DataFrameChunk(const Schema &scm) : schema(scm) {
    assert(schema.width());
    columns.reserve(schema.width());
    for (int i = 0; i < schema.width(); i++) {
      columns.emplace_back(Column::create(scm.col_type(i)));
    }
  }

  DataFrameChunk(const Schema &scm, ReadCursor &c) : DataFrameChunk(scm) {
    fill(c);
  }

  void fill(ReadCursor &c) {
    int len = yield<int>(c);
    /* fill each column from left to right */
    for (int i = 0; i < schema.width(); i++) {
      columns[i]->fill(len, c);
    }
  }

  DataFrameChunk(DataFrameChunk &&) noexcept = default;
  DataFrameChunk(const DataFrameChunk &) = delete;

  ~DataFrameChunk() {
    for (int i = 0; i < get_schema().width(); i++) {
      if (get_schema().col_type(i) == Data::Type::STRING) {
        for (int y = 0; y < nrows(); y++) {
          if (!is_missing(y, i)) {
            delete get_string(y, i);
          }
        }
      }
    }
  }

  const Schema &get_schema() const { return schema; }

  void serialize(WriteCursor &wc) const {
    int len = nrows();
    pack(wc, len);
    for (int i = 0; i < schema.width(); i++) {
      assert(columns[i]->length() == len);
      columns[i]->serialize(wc);
    }
  }

  int get_int(int y, int x) const {
    assert(y >= 0);      // assert within this chunk
    assert(y < nrows()); // assert within this chunk
    assert(x >= 0);
    assert(x < ncols());

    const unique_ptr<Column> &col = columns[x];
    assert(!col->is_missing(y));
    return col->get_int(y);
  }

  float get_float(int y, int x) const {
    assert(y >= 0);      // assert within this chunk
    assert(y < nrows()); // assert within this chunk
    assert(x >= 0);
    assert(x < ncols());

    const unique_ptr<Column> &col = columns[x];
    assert(!col->is_missing(y));
    return col->get_float(y);
  }

  bool get_bool(int y, int x) const {
    assert(y >= 0);      // assert within this chunk
    assert(y < nrows()); // assert within this chunk
    assert(x >= 0);
    assert(x < ncols());

    const unique_ptr<Column> &col = columns[x];
    assert(!col->is_missing(y));
    return col->get_bool(y);
  }

  string *get_string(int y, int x) const {
    assert(y >= 0);      // assert within this chunk
    assert(y < nrows()); // assert within this chunk
    assert(x >= 0);
    assert(x < ncols());

    const unique_ptr<Column> &col = columns[x];
    assert(!col->is_missing(y));
    return col->get_string(y);
  }

  bool is_missing(int y, int x) const {
    assert(y >= 0);      // assert within this chunk
    assert(y < nrows()); // assert within this chunk
    assert(x >= 0);
    assert(x < ncols());

    const unique_ptr<Column> &col = columns[x];
    return col->is_missing(y);
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

  bool operator==(const DataFrameChunk &other) const {
    return nrows() == other.nrows() && get_schema() == other.get_schema() &&
           equal(columns.begin(), columns.end(), other.columns.begin(),
                 [](const unique_ptr<Column> &l, const unique_ptr<Column> &r) {
                   return l->equals(*r);
                 });
  }
};
