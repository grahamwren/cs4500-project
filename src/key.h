#pragma once
#include <string>
using namespace std;

class Key {
public:
  string key;
  int index;

  Key(string key, int index) : key(key), index(index) {}
};

inline bool operator==(const Key& l, const Key& r) {
  return l.key == r.key && l.index == r.index;
}

