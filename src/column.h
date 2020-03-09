#pragma once

// lang::CwC

#include "../lib.h"

class IntColumn;
class BoolColumn;
class FloatColumn;
class StringColumn;

/**************************************************************************
 * Column ::
 * Represents one column of a data frame which holds values of a single type.
 * This abstract class defines methods overriden in subclasses. There is
 * one subclass per element type. Columns are mutable, equality is pointer
 * equality.
 */
class Column : public Object {
public:
  /**
   * Type converters: Return same column under its actual type, or
   * nullptr if of the wrong type.
   */
  virtual IntColumn *as_int() { return nullptr; }
  virtual BoolColumn *as_bool() { return nullptr; }
  virtual FloatColumn *as_float() { return nullptr; }
  virtual StringColumn *as_string() { return nullptr; }

  /**
   * Type appropriate push_back methods. Calling the wrong method is
   * undefined behavior.
   */
  virtual void push_back(int val) {}
  virtual void push_back(bool val) {}
  virtual void push_back(float val) {}
  virtual void push_back(String *val) {}

  /**
   * Returns the number of elements in the column.
   */
  virtual size_t size() = 0;

  /**
   * Return the type of this column as a char: 'S', 'B', 'I' and 'F'.
   */
  virtual char get_type() = 0;
};
