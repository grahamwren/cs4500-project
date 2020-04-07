#pragma once

#include "cursor.h"
#include "row.h"
#include "rower.h"
#include <unordered_map>

using namespace std;

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

class WordCountRower : public Rower {
private:
  int col;
  unordered_map<string, int> results;

public:
  WordCountRower(int col) : col(col) {}
  WordCountRower(ReadCursor &c) : WordCountRower(yield<int>(c)) {}
  Type get_type() const { return Type::WORD_COUNT; }

  bool accept(const Row &row) {
    /* if missing, skip */
    if (row.is_missing(col))
      return true;

    string *s = row.get<string *>(col);

    if (s) {
      results.try_emplace(*s, 0);
      results.insert_or_assign(*s, results.at(*s) + 1);
    }
    return true;
  }

  void join(const Rower &o) {
    const WordCountRower &other = dynamic_cast<const WordCountRower &>(o);
    for (auto &e : other.results) {
      results.try_emplace(e.first, 0);
      results.insert_or_assign(e.first, results.at(e.first) + e.second);
    }
  }

  void serialize(WriteCursor &c) const {
    pack(c, get_type());
    pack(c, col);
  }

  void serialize_results(WriteCursor &c) const {
    for (auto &e : results) {
      pack<const string &>(c, e.first);
      pack<int>(c, e.second);
    }
  }

  void join_serialized(ReadCursor &c) {
    while (has_next(c)) {
      string s = yield<string>(c);
      int i = yield<int>(c);
      results.try_emplace(s, 0);
      results.insert_or_assign(s, results.at(s) + i);
    }
  }

  void out(ostream &output) const {
    output << "col: " << col << ", results_count: " << results.size();
  }

  const unordered_map<string, int> &get_results() const { return results; }
};

/**
 * rower for searching and returning distinct int column values
 */
class SearchIntIntRower : public Rower {
private:
  /* arguments */
  set<int> result_cols;
  set<int> search_cols;
  set<int> terms;

  bool has_term(int cand) const { return terms.find(cand) != terms.end(); }

  set<int> results; // results

public:
  SearchIntIntRower(const set<int> &result_cols, const set<int> &search_cols,
                    const set<int> &terms)
      : result_cols(result_cols), search_cols(search_cols), terms(terms) {}
  SearchIntIntRower(ReadCursor &c) {
    int sclen = yield<int>(c);
    for (int i = 0; i < sclen; i++)
      search_cols.emplace(yield<int>(c));

    int tlen = yield<int>(c);
    for (int i = 0; i < tlen; i++)
      terms.emplace(yield<int>(c));

    int rclen = yield<int>(c);
    for (int i = 0; i < rclen; i++)
      result_cols.emplace(yield<int>(c));
  }
  Type get_type() const { return Type::SEARCH_INT_INT; }

  bool accept(const Row &row) {
    for (int scol : search_cols) {
      int val = row.get<int>(scol);
      if (has_term(val)) {
        for (int rcol : result_cols) {
          results.emplace(row.get<int>(rcol));
        }

        /* once a search col has found a val,
         * skip the rest of the search_cols */
        break;
      }
    }
    return true;
  }

  void join(const Rower &o) {
    const SearchIntIntRower &other = dynamic_cast<const SearchIntIntRower &>(o);
    for (int r : other.results) {
      results.emplace(r);
    }
  }

  void serialize(WriteCursor &c) const {
    pack(c, get_type());
    pack<int>(c, search_cols.size());
    for (int scol : search_cols)
      pack(c, scol);

    pack<int>(c, terms.size());
    for (int term : terms)
      pack(c, term);

    pack<int>(c, result_cols.size());
    for (int rcol : result_cols)
      pack(c, rcol);
  }

  void serialize_results(WriteCursor &c) const {
    for (int r : results) {
      pack(c, r);
    }
  }

  void join_serialized(ReadCursor &c) {
    while (has_next(c)) {
      results.emplace(yield<int>(c));
    }
  }

  void out(ostream &output) const {
    output << "search_cols: " << search_cols.size()
           << ", terms: " << terms.size() << ", results: " << results.size();
  }

  set<int> &get_results() { return results; }
};

inline unique_ptr<Rower> unpack_rower(ReadCursor &c) {
  Rower::Type type = yield<Rower::Type>(c);
  switch (type) {
  case Rower::Type::SUM:
    return make_unique<SumRower>(c);
  case Rower::Type::WORD_COUNT:
    return make_unique<WordCountRower>(c);
  case Rower::Type::SEARCH_INT_INT:
    return make_unique<SearchIntIntRower>(c);
  default:
    assert(false); // unsupported Rower::Type
  }
}
