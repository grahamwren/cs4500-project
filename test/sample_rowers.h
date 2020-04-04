#pragma once

#include "row.h"
#include "rower.h"
#include <limits.h>

class CountDiv2 : public Rower {
private:
  int count = 0;

public:
  int col;
  CountDiv2(int col) : col(col) {}

  bool accept(const Row &r) {
    if (r.get<int>(col) % 2 == 0)
      count++;
    return true;
  }
  int get_count() const { return count; }
};

class MinMaxInt : public Rower {
private:
  int min_int = INT_MAX;
  int max_int = INT_MIN;

public:
  int col;
  MinMaxInt(int col) : col(col) {}

  bool accept(const Row &r) {
    if (r.get<int>(col) < min_int)
      min_int = r.get<int>(col);
    if (r.get<int>(col) > max_int)
      max_int = r.get<int>(col);
    return true;
  }

  int get_min() const { return min_int; }
  int get_max() const { return max_int; }
};
