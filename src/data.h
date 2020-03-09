#pragma once

#include "lib/string.h"

class Data {
public:
  bool missing = false;
};

class Bool : public Data {
public:
  bool val;
};

class Int : public Data {
public:
  int val;
};

class Float : public Data {
public:
  float val;
};

class StringPtr : public Data {
public:
  String *val;
};
