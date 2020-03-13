#pragma once

#include <string>

using namespace std;

class Data {
public:
  enum Type { MISSING = 0, BOOL = 1, INT = 2, FLOAT = 3, STRING = 4 };
  static char type_as_char(const Type &t);
  static Type char_as_type(const char c);
  template <typename T> static Data::Type get_type() noexcept {
    if constexpr (is_same_v<T, int>)
      return Data::Type::INT;
    else if constexpr (is_same_v<T, float>)
      return Data::Type::FLOAT;
    else if constexpr (is_same_v<T, bool>)
      return Data::Type::BOOL;
    else if constexpr (is_same_v<T, string *>)
      return Data::Type::STRING;
    else
      assert(false);
  }

private:
  Type type;
  union {
    int i;
    bool b;
    float f;
    string *s;
  };

public:
  template <typename T> T get() const;
  bool is_missing() const;
};
