#pragma once

#include "cursor.h"
#include "row.h"
#include "rower.h"

class SumRower : public Rower {
private:
  int col;
  uint64_t sum_result;

public:
  SumRower(int col) : col(col), sum_result(0) {}
  SumRower(ReadCursor &c) : SumRower(yield<int>(c)) {}
  Type get_type() const { return Type::SUM; }

  bool accept(const Row &row) {
    sum_result += row.get<int>(col);
    return true;
  }

  void join(const Rower &o) {
    const SumRower &other = dynamic_cast<const SumRower &>(o);
    sum_result += other.sum_result;
  }

  void serialize(WriteCursor &c) const {
    pack(c, get_type());
    pack(c, col);
  }

  void serialize_results(WriteCursor &c) const { pack(c, sum_result); }

  void join_serialized(ReadCursor &c) { sum_result += yield<uint64_t>(c); }

  void out(ostream &output) const {
    output << "col: " << col << ", sum_result: " << sum_result;
  }

  uint64_t get_sum_result() const { return sum_result; }
};

inline unique_ptr<Rower> unpack_rower(ReadCursor &c) {
  Rower::Type type = yield<Rower::Type>(c);
  switch (type) {
  case Rower::Type::SUM:
    return make_unique<SumRower>(c);
  default:
    assert(false); // unsupported Rower::Type
  }
}
