#pragma once

#include <iostream>
#include <memory>

using namespace std;

class Row;
class WriteCursor;
class ReadCursor;

/*******************************************************************************
 *  Rower::
 *  An interface for iterating through each row of a data frame. The intent
 *  is that this class should subclassed and the accept() method be given
 *  a meaningful implementation. Rowers can be cloned for parallel execution.
 */
class Rower {
public:
  enum Type : uint8_t { SUM, WORD_COUNT, SEARCH_INT_INT };
  virtual ~Rower() {}
  virtual Type get_type() const { assert(false); }

  /**
   * This method is called once per row. The row object is on loan and
   * should not be retained as it is likely going to be reused in the next
   * call. The return value is used in filters to indicate that a row
   * should be kept.
   */
  virtual bool accept(const Row &r) = 0;

  /**
   * join results from the other rower into the results of this Rower
   */
  virtual void join(const Rower &other) { assert(false); }

  /**
   * serialize self, including type and any args into the write cursor
   *
   * TODO: same abstraction from Command is missing here, should be able to send
   * a Rower by { Type, Args }
   */
  virtual void serialize(WriteCursor &) const { assert(false); }

  /**
   * serialize the results of this Rower into the given WriteCursor
   */
  virtual void serialize_results(WriteCursor &) const { assert(false); }

  /**
   * join the result in this rower with the serialized results in the given
   * Cursor
   */
  virtual void join_serialized(ReadCursor &) { assert(false); }

  virtual void out(ostream &output) const { output << "out unimplemented"; }

  virtual unique_ptr<Rower> clone() const = 0;
};

inline ostream &operator<<(ostream &output, const Rower::Type type) {
  switch (type) {
  case Rower::Type::SUM:
    output << "SUM";
    break;
  case Rower::Type::WORD_COUNT:
    output << "WORD_COUNT";
    break;
  case Rower::Type::SEARCH_INT_INT:
    output << "SEARCH_INT_INT";
    break;
  default:
    output << "<unknown Rower::Type>";
    break;
  }
  return output;
}

inline ostream &operator<<(ostream &output, const Rower &rower) {
  output << "Rower(type: " << rower.get_type() << ", ";
  rower.out(output);
  output << ")";
  return output;
}
