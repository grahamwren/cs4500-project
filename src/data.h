#pragma once

#include <string>

using namespace std;

class Data {
public:
  enum Type { MISSING = 0, BOOL = 1, INT = 2, FLOAT = 3, STRING = 4 };
  static char type_as_char(const Type &t);
  static Type char_as_type(const char c);

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
