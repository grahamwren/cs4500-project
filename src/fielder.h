#pragma once

#include "object.h"

// lang:CwC

/*****************************************************************************
 * Fielder::
 * field vistor invoked by Row.
 */
class Fielder : public Object {
public:
  /**
   * Called before visiting a row, the argument is the row offset in the
   * dataframe.
   */
  virtual void start(size_t r) {}

  /**
   * Called for fields of the argument's type with the value of the field.
   */
  virtual void accept(bool b) {}
  virtual void accept(float f) {}
  virtual void accept(int i) {}
  virtual void accept(string *s) {}

  /**
   * Called when all fields have been seen.
   */
  virtual void done() {}
};
