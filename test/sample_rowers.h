#pragma once

#include "row.h"
#include "rower.h"
#include <algorithm>
#include <limits.h>

class CountDiv2 : public Rower {
private:
  int count = 0;

public:
  int col;
  CountDiv2(int col) : col(col) {}
  Type get_type() const { return (Type)33; }

  bool accept(const Row &r) {
    if (r.get<int>(col) % 2 == 0)
      count++;
    return true;
  }
  int get_count() const { return count; }
  void join(const Rower &o) {
    const CountDiv2 &other = dynamic_cast<const CountDiv2 &>(o);
    count += other.count;
  }
  unique_ptr<Rower> clone() const { return make_unique<CountDiv2>(col); };
  void out(ostream &output) const { output << count; }
};

class MinMaxInt : public Rower {
private:
  int min_int = INT_MAX;
  int max_int = INT_MIN;

public:
  int col;
  MinMaxInt(int col) : col(col) {}
  Type get_type() const { return (Type)33; }

  bool accept(const Row &r) {
    if (r.get<int>(col) < min_int)
      min_int = r.get<int>(col);
    if (r.get<int>(col) > max_int)
      max_int = r.get<int>(col);
    return true;
  }

  int get_min() const { return min_int; }
  int get_max() const { return max_int; }
  void join(const Rower &o) {
    const MinMaxInt &other = dynamic_cast<const MinMaxInt &>(o);
    min_int = min(min_int, other.min_int);
    max_int = max(max_int, other.max_int);
  }
  unique_ptr<Rower> clone() const { return make_unique<MinMaxInt>(col); };
  void out(ostream &output) const {
    output << "min: " << min_int << " max: " << max_int;
  }
};
