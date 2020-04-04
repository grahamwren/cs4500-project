#pragma once

#include "cursor.h"
#include "data.h"
#include <cassert>
#include <iostream>
#include <variant>
#include <vector>

using namespace std;

class Column {
protected:
  const Data::Type type;
  vector<bool> missings;

public:
  Column(const Data::Type t) : type(t) {}
  virtual ~Column() {}

  Data::Type get_type() const { return type; }

  static Column *create(Data::Type);

  virtual void fill(int, ReadCursor &) = 0;
  virtual void serialize(WriteCursor &) = 0;

  virtual void push(int val) = 0;
  virtual void push(float val) = 0;
  virtual void push(bool val) = 0;
  virtual void push(string *val) = 0;
  virtual void push() = 0;

  virtual void set(int y, int val) = 0;
  virtual void set(int y, float val) = 0;
  virtual void set(int y, bool val) = 0;
  virtual void set(int y, string *val) = 0;
  virtual void set(int y) = 0;

  virtual int get_int(int y) const = 0;
  virtual float get_float(int y) const = 0;
  virtual bool get_bool(int y) const = 0;
  virtual string *get_string(int y) const = 0;

  virtual bool equals(const Column &c) const {
    return type == c.type && length() == c.length();
  }
  bool is_missing(int y) const { return missings[y]; }
  int length() const { return missings.size(); }
};

template <typename T> class TypedColumn : public Column {
protected:
  vector<T> data;

public:
  TypedColumn() : Column(Data::get_type<T>()) {}

  void fill(int len, ReadCursor &c) {
    data.reserve(len);
    for (int i = 0; i < len; i++) {
      bool missing = yield<uint8_t>(c) == 1;
      if (missing) {
        push();
      } else {
        push(yield<T>(c));
      }
    }
  }

  void serialize(WriteCursor &c) {
    for (int i = 0; i < length(); i++) {
      if (is_missing(i)) {
        /* pack single byte for missing */
        pack(c, (uint8_t)1);
      } else {
        /* pack single byte for missing */
        pack(c, (uint8_t)0);
        pack(c, (T)data[i]);
      }
    }
  }

  /* this is annoying, they should already be here from parent */
  void push(int val) { assert(false); }
  void push(float val) { assert(false); }
  void push(bool val) { assert(false); }
  void push(string *val) { assert(false); }
  void push() {
    data.push_back((T)NULL);
    missings.push_back(true);
  }

  void set(int y, int val) { assert(false); }
  void set(int y, float val) { assert(false); }
  void set(int y, bool val) { assert(false); }
  void set(int y, string *val) { assert(false); }
  void set(int y) {
    data[y] = (T)NULL;
    missings[y] = true;
  }

  int get_int(int y) const { assert(false); }
  float get_float(int y) const { assert(false); }
  bool get_bool(int y) const { assert(false); }
  string *get_string(int y) const { assert(false); }

  bool equals(const Column &c) const {
    if (!Column::equals(c)) {
      return false;
    }

    /* Column::equals checks type and length fields so cast should be safe */
    const TypedColumn<T> &other = dynamic_cast<const TypedColumn<T> &>(c);
    for (int i = 0; i < length(); i++) {
      if ((is_missing(i) && other.is_missing(i)) ||
          (data[i] == other.data[i])) {
        continue;
      }
      return false;
    }
    return true;
  }
};

template <> void TypedColumn<int>::push(int val) {
  missings.push_back(false);
  data.push_back(val);
}
template <> void TypedColumn<float>::push(float val) {
  missings.push_back(false);
  data.push_back(val);
}
template <> void TypedColumn<bool>::push(bool val) {
  missings.push_back(false);
  data.push_back(val);
}
template <> void TypedColumn<string *>::push(string *val) {
  assert(val);
  missings.push_back(false);
  data.push_back(val);
}

template <> void TypedColumn<int>::set(int y, int val) {
  data[y] = val;
  missings[y] = false;
}
template <> void TypedColumn<float>::set(int y, float val) {
  data[y] = val;
  missings[y] = false;
}
template <> void TypedColumn<bool>::set(int y, bool val) {
  data[y] = val;
  missings[y] = false;
}
template <> void TypedColumn<string *>::set(int y, string *val) {
  assert(val);
  data[y] = val;
  missings[y] = false;
}

template <> int TypedColumn<int>::get_int(int y) const { return data[y]; }
template <> float TypedColumn<float>::get_float(int y) const { return data[y]; }
template <> bool TypedColumn<bool>::get_bool(int y) const { return data[y]; }
template <> string *TypedColumn<string *>::get_string(int y) const {
  assert(data[y]);
  return data[y];
}
template <> bool TypedColumn<string *>::equals(const Column &c) const {
  if (!Column::equals(c)) {
    return false;
  }

  /* Column::equals checks type and length fields so cast should be safe */
  const TypedColumn<string *> &other =
      dynamic_cast<const TypedColumn<string *> &>(c);
  for (int i = 0; i < length(); i++) {
    if ((is_missing(i) && other.is_missing(i)) ||
        (data[i]->compare(*other.data[i]) == 0)) {
      continue;
    }
    return false;
  }
  return true;
}

Column *Column::create(Data::Type t) {
  switch (t) {
  case Data::Type::INT:
    return new TypedColumn<int>();
  case Data::Type::FLOAT:
    return new TypedColumn<float>();
  case Data::Type::BOOL:
    return new TypedColumn<bool>();
  case Data::Type::STRING:
    return new TypedColumn<string *>();
  case Data::Type::MISSING:
    return new TypedColumn<bool>();
  default:
    assert(false);
  }
}
