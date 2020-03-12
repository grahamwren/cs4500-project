#pragma once

#include "data.h"
#include "list.h"
#include <variant>

using namespace std;

class Column {
  variant<List<int>, List<float>, List<bool>, List<string *>> data;

public:
  const Data::Type type;

  Column(Data::Type t);

  int size() const;
  bool equals(Column c);

  template <typename T> T get(int i) const;
  template <typename T> void set(int i, T val);
  template <typename T> void push(T val);
};
