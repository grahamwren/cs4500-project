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
  int result_col;
  int search_col;
  set<int> terms;

  bool has_term(int cand) const { return terms.find(cand) != terms.end(); }

  set<int> results;     // old results and new results
  set<int> new_results; // just the new results

public:
  SearchIntIntRower(int result_col, int search_col, const set<int> &terms,
                    const set<int> &old_res)
      : result_col(result_col), search_col(search_col), terms(terms),
        results(old_res) {}
  SearchIntIntRower(ReadCursor &c)
      : result_col(yield<int>(c)), search_col(yield<int>(c)) {
    int tlen = yield<int>(c);
    for (int i = 0; i < tlen; i++)
      terms.emplace(yield<int>(c));

    int orlen = yield<int>(c);
    for (int i = 0; i < orlen; i++)
      results.emplace(yield<int>(c));
  }
  Type get_type() const { return Type::SEARCH_INT_INT; }

  bool accept(const Row &row) {
    int val = row.get<int>(search_col);
    if (has_term(val)) {
      int cand = row.get<int>(result_col);
      /* if a new result, append to both sets */
      if (results.find(cand) == results.end()) {
        results.emplace(cand);
        new_results.emplace(cand);
      }
    }
    return true;
  }

  void join(const Rower &o) {
    const SearchIntIntRower &other = dynamic_cast<const SearchIntIntRower &>(o);
    new_results.insert(other.new_results.begin(), other.new_results.end());
  }

  void serialize(WriteCursor &c) const {
    pack(c, get_type());
    pack<int>(c, search_col);
    pack<int>(c, result_col);

    pack<int>(c, terms.size());
    for (int term : terms)
      pack(c, term);

    /* pack old-results */
    pack<int>(c, results.size());
    for (int res : results)
      pack(c, res);
  }

  void serialize_results(WriteCursor &c) const {
    for (int r : new_results) {
      pack(c, r);
    }
  }

  void join_serialized(ReadCursor &c) {
    while (has_next(c)) {
      new_results.emplace(yield<int>(c));
    }
  }

  void out(ostream &output) const {
    output << "results: " << new_results.size() << ", query: SELECT "
           << result_col << " WHERE " << search_col << " IN [";
    auto t_it = terms.begin();
    if (terms.size() > 0)
      cout << *t_it++;
    for (int i = 1; i < 20 && i < terms.size(); i++)
      cout << "," << *t_it++;
    if (terms.size() > 20)
      cout << ", ... " << terms.size() - 20 << "more terms";
    cout << "] AND " << search_col << " NOT IN [";

    auto or_it = terms.begin();
    if (results.size() > 0)
      cout << *or_it++;
    for (int i = 1; i < 20 && i < results.size(); i++)
      cout << "," << *or_it++;
    if (results.size() > 20)
      cout << ", ... " << results.size() - 20 << "more old results";
    cout << "]";
  }

  set<int> &get_results() { return new_results; }
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
