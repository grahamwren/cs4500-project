#include "data.h"

Data::Type char_as_type(const char c) {
  switch (c) {
  case 'I':
    return Data::Type::INT;
  case 'B':
    return Data::Type::BOOL;
  case 'F':
    return Data::Type::FLOAT;
  case 'S':
    return Data::Type::STRING;
  case 'M':
    return Data::Type::MISSING;
  }
  assert(false);
}

char Data::type_as_char(const Data::Type &t) {
  switch (t) {
  case Data::Type::INT:
    return 'I';
  case Data::Type::BOOL:
    return 'B';
  case Data::Type::FLOAT:
    return 'F';
  case Data::Type::STRING:
    return 'S';
  case Data::Type::MISSING:
    return 'M';
  }
}

template <typename T> T Data::get() const {
  if constexpr (is_same_v<T, int>) {
    if (type == Data::Type::INT)
      return i;
    else if (is_missing())
      return NULL;
    else
      assert(false);
  } else if constexpr (is_same_v<T, bool>) {
    if (type == Data::Type::BOOL)
      return b;
    else if (is_missing())
      return NULL;
    else
      assert(false);
  } else if constexpr (is_same_v<T, float>) {
    if (type == Data::Type::FLOAT)
      return f;
    else if (is_missing())
      return NULL;
    else
      assert(false);
  } else if constexpr (is_same_v<T, string *>) {
    if (type == Data::Type::STRING)
      return s;
    else if (is_missing())
      return NULL;
    else
      assert(false);
  } else if constexpr (is_same_v<T, Data::Type>) {
    return type;
  } else {
    assert(false);
  }
}

bool Data::is_missing() const { return type == Data::Type::MISSING; }
