#pragma once

#include <cassert>
#include <iostream>
#include <string>
#include <variant>

using namespace std;

class Data {
protected:
  bool missing;
  variant<int, float, bool, string *> data;

public:
  enum Type : uint8_t { MISSING = 1, BOOL = 2, INT = 3, FLOAT = 4, STRING = 5 };
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
    default:
      assert(false);
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
    cerr << "Failing to find type for char: " << c << endl;
    assert(false);
  }

  template <typename T> static Data::Type get_type() noexcept { assert(false); }

  static Type combine(const Type &left, const Type &right) {
    return left > right ? left : right;
  }

  Data() : missing(true) {}
  Data(int val) : missing(false), data(val) {}
  Data(float val) : missing(false), data(val) {}
  Data(bool val) : missing(false), data(val) {}
  Data(string *val) : missing(false), data(val) {}
  Data(const Data &d) = default;
  Data(Data &&d) = default;

  constexpr void operator=(const Data &d) {
    missing = d.missing;
    data = d.data;
  }

  bool is_missing() const { return missing; }

  template <typename T> T get() const {
    assert(!is_missing()); // assert not missing
    return std::get<T>(data);
  }

  void set(int val) {
    missing = false;
    data = val;
  }
  void set(float val) {
    missing = false;
    data = val;
  }
  void set(bool val) {
    missing = false;
    data = val;
  }
  void set(string *val) {
    assert(val); // cannot be nullptr, should be missing

    missing = false;
    data = val;
  }
  void set() { missing = true; }
};

class TypedData {
public:
  Data data;
  Data::Type type;
  TypedData() : data(), type(Data::Type::MISSING) {}
  TypedData(int val) : data(val), type(Data::Type::INT) {}
  TypedData(float val) : data(val), type(Data::Type::FLOAT) {}
  TypedData(bool val) : data(val), type(Data::Type::BOOL) {}
  TypedData(string *val) : data(val), type(Data::Type::STRING) {}

  template <typename T> T get() { return data.get<T>(); }
};

template <> inline Data::Type Data::get_type<int>() noexcept {
  return Data::Type::INT;
}
template <> inline Data::Type Data::get_type<float>() noexcept {
  return Data::Type::FLOAT;
}
template <> inline Data::Type Data::get_type<bool>() noexcept {
  return Data::Type::BOOL;
}
template <> inline Data::Type Data::get_type<string *>() noexcept {
  return Data::Type::STRING;
}
