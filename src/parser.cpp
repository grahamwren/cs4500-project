#include "parser.h"
#include "dataframe.h"
#include "row.h"
#include "schema.h"

Parser::Parser(int len, char *d)
    : cursor(len, reinterpret_cast<uint8_t *>(d)) {}
Parser::Parser(const char *d) : Parser(strlen(d), const_cast<char *>(d)) {}

bool Parser::out_of_input() const { return empty(cursor); }
int Parser::parse_pos() const { return cursor.cursor - cursor.bytes; }

bool Parser::parse_file(DataFrame &dest) {
  checkpoint(cursor);

  bool accept = false;
  Row row(dest.get_schema());
  while (has_next(cursor) && parse_row(dest.get_schema(), row)) {
    accept = true; // parsed at least one row
    dest.add_row(row);
  }
  accept = accept && empty(cursor);

  if (accept) {
    commit(cursor);
  } else {
    rollback(cursor);
  }
  return accept;
}

bool Parser::parse_n_lines(int n, DataFrame &dest) {
  checkpoint(cursor);

  bool accept = false;
  Row row(dest.get_schema());
  int row_count = 0;
  while (row_count < n && has_next(cursor) &&
         parse_row(dest.get_schema(), row)) {
    accept = true; // parsed at least one row
    dest.add_row(row);
    row_count++;
  }
  accept = accept && (empty(cursor) || row_count == n);

  if (accept) {
    commit(cursor);
  } else {
    rollback(cursor);
  }
  return accept;
}

bool Parser::infer_schema(Schema &scm) {
  checkpoint(cursor); // set checkpoint to rollback to

  /* consume 500 lines or entire input to infer schema */
  for (int i = 0; i < 500 && has_next(cursor); i++) {
    bool accepted = parse_row_types(scm);

    /* break if failed to parse */
    if (!accepted) {
      break;
    }
  }

  rollback(cursor); // rollback to start of buffer
  return true;
}

/**
 * parse_row_types: parse types from row, combining types if some already exist
 */
bool Parser::parse_row_types(Schema &scm) {
  checkpoint(cursor);

  Data::Type t;
  bool accept = false;
  for (int i = 0; has_next(cursor) && parse_val_type(t); i++) {
    accept = true;
    if (scm.width() > i) {
      t = Data::combine(scm.col_type(i), t);
      scm.set_type(i, t);
    } else {
      assert(scm.width() == i); // no skipping
      scm.add_column(t);
    }
  }

  /* accept if we've read the entire input or found a new-line */
  accept = accept && (empty(cursor) || expect<char>(cursor, '\n'));

  if (accept) {
    commit(cursor);
  } else {
    rollback(cursor);
  }
  return accept;
}

/**
 * parse_row: parse a row based on the given schema
 * @arg row_schema null-terminated char array representing the schema of the
 *                 row
 */
bool Parser::parse_row(const Schema &scm, Row &dest) {
  checkpoint(cursor);

  Data d;
  bool accept = false;
  for (int i = 0; i < scm.width(); i++) {
    if (parse_val(scm.col_type(i), d)) {
      accept = true;
      dest.set(i, d);
    } else {
      dest.set_missing(i);
    }
  }

  /* accept if we've read the entire input or found a new-line */
  accept = accept && (empty(cursor) || expect<char>(cursor, '\n'));

  if (accept) {
    commit(cursor);
  } else {
    rollback(cursor);
  }
  return accept;
}

/**
 * @brief parse a val in brackets (i.e. "<...>") of an expected type
 *
 * @arg type the expected type of the val
 *
 * @return the ParseResult<Data> with either contain `accept: false` to
 * indicate a failed parse, or will contain a Data in the result
 */
bool Parser::parse_val(const Data::Type type, Data &dest) {
  checkpoint(cursor);

  bool accept = false;
  if (expect<char>(cursor, '<')) {
    switch (type) {
    case Data::Type::BOOL:
      accept = parse_bool(dest);
      break;
    case Data::Type::INT:
      accept = parse_int(dest);
      break;
    case Data::Type::FLOAT:
      accept = parse_float(dest);
      break;
    case Data::Type::STRING:
      accept = parse_string(dest);
      break;
    case Data::Type::MISSING:
      accept = parse_missing(dest);
      break;
    default:
      assert(false); // unknown type
    }
    if (!accept) {
      accept = parse_missing(dest);
    }
  }
  accept = accept && expect<char>(cursor, '>');

  if (accept) {
    commit(cursor);
  } else {
    rollback(cursor);
  }
  return accept;
}

/**
 * parse a value in brackets (i.e. "<...>") by trying to infer the type
 * matched by first successful parse in this order:
 *   [MISSING, BOOL, INT, FLOAT, STRING]
 */
bool Parser::parse_val_type(Data::Type &type) {
  checkpoint(cursor);

  Data data;
  bool accept = expect<char>(cursor, '<') &&
                ((parse_missing(data) && expect<char>(cursor, '>') &&
                  (type = Data::Type::MISSING)) ||
                 (parse_bool(data) && expect<char>(cursor, '>') &&
                  (type = Data::Type::BOOL)) ||
                 (parse_int(data) && expect<char>(cursor, '>') &&
                  (type = Data::Type::INT)) ||
                 (parse_float(data) && expect<char>(cursor, '>') &&
                  (type = Data::Type::FLOAT)) ||
                 (parse_string(data) && expect<char>(cursor, '>') &&
                  (type = Data::Type::STRING)));

  if (accept && type == Data::Type::STRING)
    delete data.get<string *>();

  if (accept) {
    commit(cursor);
  } else {
    rollback(cursor);
  }
  return accept;
}

/**
 * parses a missing (i.e. ">?")
 */
bool Parser::parse_missing(Data &dest) {
  dest.set();
  return empty(cursor) || peek<char>(cursor) == '>';
}

/**
 * parses an int (i.e. "[+-]?[[:digit:]]+>?")
 */
bool Parser::parse_int(Data &dest) {
  checkpoint(cursor);

  int i = 0;
  char val[MAX_VAL_LEN];
  bool accept = false;

  char c = peek<char>(cursor); // read first digit
  /* if is: "[+-[:digit:]]", set accept if we read a digit */
  if (c == '+' || c == '-' || (accept = isdigit(c))) {
    val[i++] = yield<char>(cursor);
  } else { // if is: "[^+-[:digit:]]", fail immediately
    rollback(cursor);
    return accept;
  }

  /* read: "[[:digit:]]*" */
  while (has_next(cursor) && isdigit(peek<char>(cursor))) {
    /* since we've read a digit we can accept this input */
    accept = true;
    val[i++] = yield<char>(cursor);
  }
  /* successful if accepting and we've consumed entire val */
  accept = accept && (empty(cursor) || peek<char>(cursor) == '>');

  /* if can be safely converted to an int */
  if (accept) {
    val[i] = '\0'; // ensure null terminated
    int result;
    from_chars(val, val + i, result);
    dest.set(result);
    commit(cursor);
  } else {
    rollback(cursor);
  }
  return accept;
}

/**
 * parses a bool (i.e. "[01]>?")
 */
bool Parser::parse_bool(Data &dest) {
  checkpoint(cursor);

  bool accept = false;
  if (expect<char>(cursor, '1')) {
    accept = true;
    dest.set(true);
  } else if (expect<char>(cursor, '0')) {
    accept = true;
    dest.set(false);
  }
  /* successful if we parsed a bool and done with the input */
  accept = accept && (empty(cursor) || peek<char>(cursor) == '>');

  if (accept) {
    commit(cursor);
  } else {
    rollback(cursor);
  }
  return accept;
}

/**
 * parses a float (i.e. "[+-]?[[:digit:]]+(\.[[:digit]]+)?>?")
 */
bool Parser::parse_float(Data &dest) {
  checkpoint(cursor);

  int i = 0;
  char val[MAX_VAL_LEN];
  bool accept = false;

  const char &c = peek<char>(cursor); // read first digit
  /* if is: "[+-[:digit:]]", set accept if we read a digit */
  if (c == '+' || c == '-' || (accept = isdigit(c))) {
    val[i++] = yield<char>(cursor);
  } else { // if is: "[^+-[:digit:]]", fail immediately
    rollback(cursor);
    return accept;
  }

  /* read: "[[:digit:]]*" */
  while (has_next(cursor) && isdigit(peek<char>(cursor))) {
    /* since we've read a digit we can accept this input */
    accept = true;
    val[i++] = yield<char>(cursor);
  }

  /* TODO: above duplicated from parse_int */

  /* if: "(?<=[[:digit:]])\." */
  if (accept && peek<char>(cursor) == '.') {
    val[i++] = yield<char>(cursor);
    accept = false; // reset accept to require numbers after the dec
    /* read: "[[:digit:]]+" */
    while (has_next(cursor) && isdigit(peek<char>(cursor))) {
      accept = true;
      val[i++] = yield<char>(cursor);
    }
  }

  /* if can be safely converted to a float */
  if (accept) {
    val[i] = '\0'; // ensure null terminated
    dest.set(strtof(val, nullptr));
    /* successful if we've consumed entire val */
    accept = empty(cursor) || peek<char>(cursor) == '>';
  }

  if (accept) {
    commit(cursor);
  } else {
    rollback(cursor);
  }
  return accept;
}

/**
 * parses a string (i.e. '("[^"]*"|[^>]*)>?')
 */
bool Parser::parse_string(Data &dest) {
  checkpoint(cursor);

  char *start;
  int i = 0;
  bool accept = false;
  /* check for quoted string */
  if (expect<char>(cursor, '"')) {
    accept = true;
    start = reinterpret_cast<char *>(cursor.cursor);
    while (has_next(cursor) && yield<char>(cursor) != '"') {
      i++;
    }
  } else {
    start = reinterpret_cast<char *>(cursor.cursor);
    while (has_next(cursor) && yield<char>(cursor) != '>') {
      i++;
    }
    // if we stopped because we found a '>'
    if (has_next(cursor)) {
      unyield<char>(cursor); // move back over '>'
    }
  }

  accept = (accept || i > 0) && (empty(cursor) || peek<char>(cursor) == '>');
  if (accept) {
    string *val = new string(start, i);
    dest.set(val);
  }
  if (accept) {
    commit(cursor);
  } else {
    rollback(cursor);
  }
  return accept;
}
