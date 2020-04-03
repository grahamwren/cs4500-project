#pragma once

#include "row.h"
#include "rower.h"
#include <limits.h>

class CountDiv2 : public Rower {
public:
  int count = 0;
  int col;
  CountDiv2(int col) : col(col) {}

  bool accept(const Row &r) {
    if (r.get<int>(col) % 2 == 0)
      count++;
    return true;
  }
};

class MinMaxInt : public Rower {
public:
  int min_int = INT_MAX;
  int max_int = INT_MIN;
  int col;
  MinMaxInt(int col) : col(col) {}

  bool accept(const Row &r) {
    if (r.get<int>(col) < min_int)
      min_int = r.get<int>(col);
    if (r.get<int>(col) > max_int)
      max_int = r.get<int>(col);
    return true;
  }
};
