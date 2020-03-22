#pragma once

#include "cursor.h"
#include "data.h"
#include <cassert>
#include <charconv>
#include <cstring>
#include <inttypes.h>
#include <iostream>
#include <utility>

#ifndef MAX_VAL_LEN
#define MAX_VAL_LEN 1024
#endif

using namespace std;

class Schema;
class DataFrame;
class Row;

class Parser {
public:
  Cursor cursor;
  bool debug = false;

  Parser(int len, char *d);
  Parser(const char *d);

  bool out_of_input() const;
  int parse_pos() const;

  bool parse_file(DataFrame &dest);

  bool infer_schema(Schema &scm);

  /**
   * parse_row: parse a row without a known schema
   */
  bool parse_row_types(Schema &);

  /**
   * parse_row: parse a row based on the given schema
   * @arg row_schema null-terminated char array representing the schema of the
   *                 row
   */
  bool parse_row(const Schema &scm, Row &dest);

  /**
   * @brief parse a val in brackets (i.e. "<...>") of an expected type
   *
   * @arg type the expected type of the val
   *
   * @return the ParseResult<Data>with either contain `success: false` to
   * indicate a failed parse, or will contain a Data in the result
   */
  bool parse_val(const Data::Type type, Data &dest);

  /**
   * parse a value in brackets (i.e. "<...>") by trying to infer the type
   * matched by first successful parse in this order:
   *   [MISSING, BOOL, INT, FLOAT, STRING]
   */
  bool parse_val_type(Data::Type &);

  /**
   * parses a missing (i.e. ">?")
   */
  bool parse_missing(Data &dest);

  /**
   * parses an int (i.e. "[+-]?[[:digit:]]+>?")
   */
  bool parse_int(Data &dest);

  /**
   * parses a bool (i.e. "[01]>?")
   */
  bool parse_bool(Data &dest);

  /**
   * parses a float (i.e. "[+-]?[[:digit:]]+(\.[[:digit]]+)?>?")
   */
  bool parse_float(Data &dest);

  /**
   * parses a string (i.e. '("[^"]*"|[^>]*)>?')
   */
  bool parse_string(Data &dest);
};
