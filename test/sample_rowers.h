#pragma once

#include "row.h"
#include "rower.h"

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
