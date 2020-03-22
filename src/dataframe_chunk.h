#pragma once

#include "data.h"
#include "dataframe.h"
#include <vector>

#ifndef DF_CHUNK_SIZE
#define DF_CHUNK_SIZE 4096
#endif

using namespace std;

class ChunkColumn {
protected:
  int len;
  const Data::Type type;
  const vector<Data> data;

public:
  ChunkColumn(Data::Type t) : type(t), data(DF_CHUNK_SIZE) {}

  template <typename T> void push(T val) {
    assert(type == Data::get_type<T>());
    /* assert not yet full */
    assert(!is_full());
    data[len++].set(val);
  }
  void push() {
    /* assert not yet full */
    assert(!is_full());
    data[len++].set();
  }

  template <typename T> void set(int i, T val) {
    assert(type == Data::get_type<T>());
    assert(i > 0);
    assert(i < length());
    data[i].set(val);
  }
  void set(int i) {
    assert(i > 0);
    assert(i < length());
    data[i].set();
  }

  template <typename T> T get(int i) const {
    assert(type == Data::get_type<T>());
    return data[i].get<T>();
  }
  bool is_missing(int i) const { return data[i].is_missing(); }
  int length() const { return len; }
  bool is_full() const { return length() >= DF_CHUNK_SIZE; }
};

class DataFrameChunk : public DataFrame {
public:
  const Schema &schema;
  const vector<ChunkColumn> columns;

  DataFrameChunk(const Schema &scm) : schema(scm) {
    columns.reserve(schema.width());
    for (int i = 0; i < schema.width(); i++) {
      columns.emplace_back(schema.col_type(i));
    }
  }

  template <typename T> T get(int y, int x) const {
    assert(x > 0);
    assert(x < schema.width());
    return columns[x].get<T>(y);
  }

  bool is_full() const {
    assert(columns.size() > 0);
    return columns[0].is_full();
  }

  void add_row(const vector<Data> &data) const {
    assert(!is_full());
    int min_width = min(data.size(), schema.width());
    for (int i = 0; i < min_width; i++) {
      Data &d = data[i];
      Column &col = columns[i];
      if (d.is_missing()) {
        col.push();
      } else {
        switch (schema.col_type(i)) {
        case Data::Type::INT:
          col.push<int>(d.get<int>());
          break;
        case Data::Type::FLOAT:
          col.push<float>(d.get<float>());
          break;
        case Data::Type::BOOL:
          col.push<bool>(d.get<bool>());
          break;
        case Data::Type::STRING:
          col.push<string *>(d.get<string *>());
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
};
