#pragma once

#include <string>

using namespace std;

class Data {
protected:
  bool missing;
  union {
    int i;
    bool b;
    float f;
    string *s;
  };

public:
  enum Type : uint8_t { MISSING = 0, BOOL = 1, INT = 2, FLOAT = 3, STRING = 4 };
  static char type_as_char(const Type t) {
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

  static Type char_as_type(const char c) {
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

  Data() : missing(true) {}
  Data(int val) : missing(false), i(val) {}
  Data(float val) : missing(false), f(val) {}
  Data(bool val) : missing(false), b(val) {}
  Data(string *val) : missing(false), s(val) {}
  Data(Data &d) = default;
  Data(Data &&d) = default;
  bool is_missing() const { return missing; }

  template <typename T> T get() const {
    if constexpr (is_same_v<T, int>) {
      if (is_missing())
        return NULL;
      else
        return i;
    } else if constexpr (is_same_v<T, bool>) {
      if (is_missing())
        return NULL;
      else
        return b;
    } else if constexpr (is_same_v<T, float>) {
      if (is_missing())
        return NULL;
      else
        return f;
    } else if constexpr (is_same_v<T, string *>) {
      if (is_missing())
        return NULL;
      else
        return s;
    } else {
      assert(false);
    }
  }

  void set(int val) {
    missing = false;
    i = val;
  }
  void set(float val) {
    missing = false;
    f = val;
  }
  void set(bool val) {
    missing = false;
    b = val;
  }
  void set(string *val) {
    missing = false;
    s = val;
  }
  void set() { missing = true; }
};

class TypedData {
public:
  const Data::Type type;
  const Data data;
  TypedData() : data(), type(Data::Type::MISSING) {}
  TypedData(int val) : data(val), type(Data::Type::INT) {}
  TypedData(float val) : data(val), type(Data::Type::FLOAT) {}
  TypedData(bool val) : data(val), type(Data::Type::BOOL) {}
  TypedData(string *val) : data(val), type(Data::Type::STRING) {}

  template <typename T> T get() { return data.get<T>(); }
};
