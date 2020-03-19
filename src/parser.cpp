#include "parser.h"
#include "dataframe.h"
#include "row.h"
#include "schema.h"

Parser::Parser(int len, char *d) : input(len, reinterpret_cast<uint8_t *>(d)) {}
Parser::Parser(const char *d) : Parser(strlen(d), const_cast<char *>(d)) {}

bool Parser::empty() const { return input.empty(); }
int Parser::parse_pos() const { return input.cursor_pos(); }

bool Parser::parse_file(DataFrame &dest) {
  input.checkpoint();

  bool accept = false;
  Row row(dest.get_schema());
  for (int i = 0; input.has_next() && parse_row(dest.get_schema(), row); i++) {
    accept = true;
    dest.add_row(row);
  }
  accept = accept && input.empty();

  if (accept) {
    input.commit();
  } else {
    input.rollback();
  }
  return accept;
}

bool Parser::infer_schema(Schema &scm) {
  input.checkpoint(); // set checkpoint to rollback to

  /* only consume 500 lines to infer schema */
  for (int i = 0; i < 500; i++) {
    bool accepted = parse_row_types(scm);

    /* break if less then 500 rows: out of input or failed to parse row */
    if (empty() || !accepted) {
      break;
    }
  }

  input.rollback(); // rollback to start of buffer
  return true;
}

/**
 * parse_row: parse types from row, combining types if some already exist
 */
bool Parser::parse_row_types(Schema &scm) {
  input.checkpoint();

  TypedData d;
  bool accept = false;
  for (int i = 0; input.has_next() && parse_val(d); i++) {
    accept = true;
    if (scm.width() > i) {
      Data::Type t = Data::combine(scm.col_type(i), d.type);
      scm.set_type(i, t);
    } else {
      assert(scm.width() == i); // no skipping
      scm.add_column(d.type, nullptr);
    }
  }

  /* accept if we've read the entire input or found a new-line */
  accept = accept && (input.empty() || input.expect<char>('\n'));

  if (accept) {
    input.commit();
  } else {
    input.rollback();
  }
  return accept;
}

/**
 * parse_row: parse a row based on the given schema
 * @arg row_schema null-terminated char array representing the schema of the
 *                 row
 */
bool Parser::parse_row(const Schema &scm, Row &dest) {
  input.checkpoint();

  Data d;
  bool accept = false;
  for (int i = 0; i < scm.width(); i++) {
    if (parse_val(scm.col_type(i), d)) {
      accept = true;
      dest.set(i, d);
    }
  }

  /* accept if we've read the entire input or found a new-line */
  accept = accept && (input.empty() || input.expect<char>('\n'));

  if (accept) {
    input.commit();
  } else {
    input.rollback();
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
  input.checkpoint();

  bool accept = false;
  if (input.expect<char>('<')) {
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
  accept = accept && input.expect<char>('>');

  if (accept) {
    input.commit();
  } else {
    input.rollback();
  }
  return accept;
}

/**
 * parse a value in brackets (i.e. "<...>") by trying to infer the type
 * matched by first successful parse in this order:
 *   [MISSING, BOOL, INT, FLOAT, STRING]
 */
bool Parser::parse_val(TypedData &dest) {
  input.checkpoint();

  bool accept = input.expect<char>('<') &&
                ((parse_missing(dest.data) && input.expect<char>('>') &&
                  (dest.type = Data::Type::MISSING)) ||
                 (parse_bool(dest.data) && input.expect<char>('>') &&
                  (dest.type = Data::Type::BOOL)) ||
                 (parse_int(dest.data) && input.expect<char>('>') &&
                  (dest.type = Data::Type::INT)) ||
                 (parse_float(dest.data) && input.expect<char>('>') &&
                  (dest.type = Data::Type::FLOAT)) ||
                 (parse_string(dest.data) && input.expect<char>('>') &&
                  (dest.type = Data::Type::STRING)));

  if (accept) {
    input.commit();
  } else {
    input.rollback();
  }
  return accept;
}

/**
 * parses a missing (i.e. ">?")
 */
bool Parser::parse_missing(Data &dest) {
  dest.set();
  return input.empty() || input.peek<char>() == '>';
}

/**
 * parses an int (i.e. "[+-]?[[:digit:]]+>?")
 */
bool Parser::parse_int(Data &dest) {
  input.checkpoint();

  int i = 0;
  char val[MAX_VAL_LEN];
  bool accept = false;

  char c = input.peek<char>(); // read first digit
  /* if is: "[+-[:digit:]]", set accept if we read a digit */
  if (c == '+' || c == '-' || (accept = isdigit(c))) {
    val[i++] = input.yield<char>();
  } else { // if is: "[^+-[:digit:]]", fail immediately
    input.rollback();
    return accept;
  }

  /* read: "[[:digit:]]*" */
  while (input.has_next() && isdigit(input.peek<char>())) {
    /* since we've read a digit we can accept this input */
    accept = true;
    val[i++] = input.yield<char>();
  }
  /* successful if accepting and we've consumed entire val */
  accept = accept && (input.empty() || input.peek<char>() == '>');

  /* if can be safely converted to an int */
  if (accept) {
    val[i] = '\0'; // ensure null terminated
    dest.set((int)strtoimax(val, nullptr, 10));
    input.commit();
  } else {
    input.rollback();
  }
  return accept;
}

/**
 * parses a bool (i.e. "[01]>?")
 */
bool Parser::parse_bool(Data &dest) {
  input.checkpoint();

  bool accept = false;
  if (input.expect<char>('1')) {
    accept = true;
    dest.set(true);
  } else if (input.expect<char>('0')) {
    accept = true;
    dest.set(false);
  }
  /* successful if we parsed a bool and done with the input */
  accept = accept && (input.empty() || input.peek<char>() == '>');

  if (accept) {
    input.commit();
  } else {
    input.rollback();
  }
  return accept;
}

/**
 * parses a float (i.e. "[+-]?[[:digit:]]+(\.[[:digit]]+)?>?")
 */
bool Parser::parse_float(Data &dest) {
  input.checkpoint();

  int i = 0;
  char val[MAX_VAL_LEN];
  bool accept = false;

  const char &c = input.peek<char>(); // read first digit
  /* if is: "[+-[:digit:]]", set accept if we read a digit */
  if (c == '+' || c == '-' || (accept = isdigit(c))) {
    val[i++] = input.yield<char>();
  } else { // if is: "[^+-[:digit:]]", fail immediately
    input.rollback();
    return accept;
  }

  /* read: "[[:digit:]]*" */
  while (input.has_next() && isdigit(input.peek<char>())) {
    /* since we've read a digit we can accept this input */
    accept = true;
    val[i++] = input.yield<char>();
  }

  /* TODO: above duplicated from parse_int */

  /* if: "(?<=[[:digit:]])\." */
  if (accept && input.peek<char>() == '.') {
    val[i++] = input.yield<char>();
    accept = false; // reset accept to require numbers after the dec
    /* read: "[[:digit:]]+" */
    while (input.has_next() && isdigit(input.peek<char>())) {
      accept = true;
      val[i++] = input.yield<char>();
    }
  }

  /* if can be safely converted to a float */
  if (accept) {
    val[i] = '\0'; // ensure null terminated
    dest.set(strtof(val, nullptr));
    /* successful if we've consumed entire val */
    accept = input.empty() || input.peek<char>() == '>';
  }

  if (accept) {
    input.commit();
  } else {
    input.rollback();
  }
  return accept;
}

/**
 * parses a string (i.e. '("[^"]*"|[^>]*)>?')
 */
bool Parser::parse_string(Data &dest) {
  input.checkpoint();

  int i = 0;
  char buf[MAX_VAL_LEN];
  bool accept;
  /* check for quoted string */
  if (input.expect<char>('"')) {
    while (input.has_next() && input.peek<char>() != '"') {
      buf[i++] = input.yield<char>();
    }
    accept = input.expect<char>('"');
  } else {
    while (input.has_next() && input.peek<char>() != '>') {
      buf[i++] = input.yield<char>();
    }
    accept = true;
  }

  if (accept) {
    buf[i] = '\0'; // ensure null-terminated
    string *val = new string(buf);
    dest.set(val);
    accept = accept && (input.empty() || input.peek<char>() == '>');
  }
  if (accept) {
    input.commit();
  } else {
    input.rollback();
  }
  return accept;
}
