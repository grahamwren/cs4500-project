#pragma once

#include "data.h"
#include <cassert>
#include <iostream>
#include <variant>
#include <vector>

using namespace std;

template <typename T> class TypedColumn;

class Column {
protected:
  virtual const TypedColumn<int> *as_int() const { return nullptr; }
  virtual const TypedColumn<float> *as_float() const { return nullptr; }
  virtual const TypedColumn<bool> *as_bool() const { return nullptr; }
  virtual const TypedColumn<string *> *as_string() const { return nullptr; }

public:
  const Data::Type type;
  vector<bool> missings;

  Column(Data::Type t) : type(t) {}
  virtual ~Column() {}

  template <typename T> TypedColumn<T> &as() const {
    if constexpr (is_same_v<T, int>) {
      assert(type == Data::Type::INT);
      return *const_cast<TypedColumn<T> *>(as_int());
    } else if constexpr (is_same_v<T, float>) {
      assert(type == Data::Type::FLOAT);
      return *const_cast<TypedColumn<T> *>(as_float());
    } else if constexpr (is_same_v<T, bool>) {
      assert(type == Data::Type::BOOL);
      return *const_cast<TypedColumn<T> *>(as_bool());
    } else if constexpr (is_same_v<T, string *>) {
      assert(type == Data::Type::STRING);
      return *const_cast<TypedColumn<T> *>(as_string());
    } else {
      assert(false);
    }
  }

  int size() const { return missings.size(); }
  bool equals(const Column *c) const {
    if (c == nullptr)
      return false;
    return equals(*c);
  }
  virtual bool equals(const Column &c) const {
    return size() == c.size() &&
           equal(missings.begin(), missings.end(), c.missings.begin());
  }
  virtual void print() const = 0;
  virtual void push_missing() = 0;

  bool is_missing(int i) const {
    assert(i < missings.size());
    return missings[i];
  }
};

template <typename T> class TypedColumn : public Column {
protected:
  vector<T> data;
  const TypedColumn<int> *as_int() const {
    if constexpr (is_same_v<T, int>)
      return this;
    else
      return nullptr;
  }
  const TypedColumn<float> *as_float() const {
    if constexpr (is_same_v<T, float>)
      return this;
    else
      return nullptr;
  }
  const TypedColumn<bool> *as_bool() const {
    if constexpr (is_same_v<T, bool>)
      return this;
    else
      return nullptr;
  }
  const TypedColumn<string *> *as_string() const {
    if constexpr (is_same_v<T, string *>)
      return this;
    else
      return nullptr;
  }

  void push_missing() {
    assert(data.size() == missings.size());
    missings.push_back(true);
    data.push_back((T)NULL);
  }

public:
  TypedColumn() : Column(Data::get_type<T>()) {}

  bool equals(const Column &c) const {
    // missings equal
    if (!Column::equals(*this))
      return false;

    // type and length equal
    if (type != c.type || Data::get_type<T>() != c.type || size() != c.size())
      return false;

    TypedColumn<T> const &other = c.as<T>();
    if constexpr (is_same_v<T, string *>) {
      return equal(data.begin(), data.end(), other.data.begin(),
                   [](T l, T r) { return l->compare(*r) == 0; });
    } else
      return equal(data.begin(), data.end(), other.data.begin());
  }

  T get(int i) const { return data[i]; }
  void set(int i, T val) {
    assert(i < data.size());
    missings[i] = false;
    data[i] = val;
  }
  void push(T val) {
    assert(missings.size() == data.size());
    missings.push_back(false);
    data.push_back(val);
  }

  void print() const {
    auto stringify = [](T val) {
      if constexpr (is_same_v<T, string *>)
        return val->c_str();
      else
        return val;
    };

    cout << "Column[";
    if (size() > 1)
      cout << stringify(get(0));
    for (int i = 1; i < size(); i++)
      cout << ", " << stringify(get(i));
    cout << "]" << endl;
  }
};
