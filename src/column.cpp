#include "column.h"

Column::Column(Data::Type t) : type(t) {}

int Column::size() const {
  switch (type) {
  case Data::Type::INT:
    List<int> &l = get<List<int>>(data);
    return l.size();
  case Data::Type::FLOAT:
    List<float> &l = get<List<float>>(data);
    return l.size();
  case Data::Type::BOOL:
    List<bool> &l = get<List<bool>>(data);
    return l.size();
  case Data::Type::STRING:
    List<string *> &l = get<List<string *>>(data);
    return l.size();
  default:
    assert(false);
  }
}

bool Column::equals(Column const &c) const {
  if (type != c.type)
    return false;

  switch (type) {
  case Data::Type::INT:
    List<int> &l = get<List<int>>(data);
    return l.equals(get<List<T>>(c.data));
  case Data::Type::FLOAT:
    List<float> &l = get<List<float>>(data);
    return l.equals(get<List<T>>(c.data));
  case Data::Type::BOOL:
    List<bool> &l = get<List<bool>>(data);
    return l.equals(get<List<T>>(c.data));
  case Data::Type::STRING:
    List<string *> &l = get<List<string *>>(data);
    return l.equals(get<List<string *>>(c.data),
                    [](string *left, string *r) { return left->compare(*r); });
  default:
    assert(false);
  }
}

T Column::get(int i) const {
  List<T> &l = get<List<T>>(data);
  l.get(i);
}

void set(int i, T val) {
  List<T> &l = get<List<T>>(data);
  l.set(i, val);
}

void push(T val) {
  List<T> &l = get<List<T>>(data);
  l.push(val);
}
