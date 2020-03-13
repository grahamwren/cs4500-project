#pragma once

#include "data.h"
#include "list.h"
#include <variant>

using namespace std;

template <typename T> class TypedColumn;

class Column {
protected:
  virtual const TypedColumn<int> *as_int() const = 0;
  virtual const TypedColumn<float> *as_float() const = 0;
  virtual const TypedColumn<bool> *as_bool() const = 0;
  virtual const TypedColumn<string *> *as_string() const = 0;

public:
  const Data::Type type;

  Column(Data::Type t) : type(t) {}
  virtual ~Column() {}

  template <typename T> TypedColumn<T> &as() const {
    assert(type == Data::get_type<T>());
    if constexpr (is_same_v<T, int>)
      return *const_cast<TypedColumn<T> *>(as_int());
    else if constexpr (is_same_v<T, float>)
      return *const_cast<TypedColumn<T> *>(as_float());
    else if constexpr (is_same_v<T, bool>)
      return *const_cast<TypedColumn<T> *>(as_bool());
    else if constexpr (is_same_v<T, string *>)
      return *const_cast<TypedColumn<T> *>(as_string());
    else
      assert(false);
  }

  virtual int size() const = 0;
  virtual bool equals(const Column *c) const = 0;
  virtual bool equals(const Column &c) const = 0;
  virtual void print() const = 0;
};

template <typename T> class TypedColumn : public Column {
protected:
  vector<T> data;
  virtual const TypedColumn<int> *as_int() const {
    if constexpr (is_same_v<T, int>)
      return this;
    else
      return nullptr;
  }
  virtual const TypedColumn<float> *as_float() const {
    if constexpr (is_same_v<T, float>)
      return this;
    else
      return nullptr;
  }
  virtual const TypedColumn<bool> *as_bool() const {
    if constexpr (is_same_v<T, bool>)
      return this;
    else
      return nullptr;
  }
  virtual const TypedColumn<string *> *as_string() const {
    if constexpr (is_same_v<T, string *>)
      return this;
    else
      return nullptr;
  }

public:
  TypedColumn() : Column(Data::get_type<T>()) {}

  int size() const { return data.size(); }
  bool equals(const Column *c) const {
    if (c == nullptr)
      return false;
    return equals(*c);
  }
  bool equals(const Column &c) const {
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
  void set(int i, T val) { data[i] = val; }
  void push(T val) { data.push_back(val); }

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
