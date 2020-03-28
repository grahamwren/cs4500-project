#pragma once

#include "chunk_key.h"
#include "data_chunk.h"
#include <cstring>
#include <iostream>

class Command {
public:
  enum Type : uint8_t { GET, PUT, OWN };

  const Type type;
  union {
    struct {          // <------+
      ChunkKey key;   // <------|-- get
      DataChunk data; //        +-- put
    };                // <------+
    string name;      // <-- own
  };

  Command(const ChunkKey &k) : type(Type::GET), key(k) {}
  Command(const ChunkKey &k, const DataChunk &dc)
      : type(Type::PUT), key(k), data(dc) {}
  Command(const string &ns) : type(Type::OWN), name(ns) {}

  static Command unpack(ReadCursor &c) {
    Type t = yield<Type>(c);
    if (t == Type::GET) {
      ChunkKey key(c);
      return Command(key);
    }
    if (t == Type::PUT) {
      ChunkKey key(c);
      DataChunk data(c);
      return Command(key, data);
    }
    if (t == Type::OWN) {
      string name = yield<string>(c);
      return Command(name);
    }
    assert(false); // unknown type
  }

  ~Command() {}

  void serialize(WriteCursor &wc) {
    pack(wc, type);
    switch (type) {
    case Type::GET:
      key.serialize(wc);
      break;
    case Type::PUT:
      key.serialize(wc);
      data.serialize(wc);
      break;
    case Type::OWN:
      pack<const string &>(wc, name);
      break;
    }
  }
};

ostream &operator<<(ostream &output, const Command::Type &t) {
  switch (t) {
  case Command::Type::GET:
    output << "GET";
    break;
  case Command::Type::PUT:
    output << "PUT";
    break;
  case Command::Type::OWN:
    output << "OWN";
    break;
  default:
    output << "<unknown Command::Type>";
  }
  return output;
}
ostream &operator<<(ostream &output, const Command &cmd) {
  output << "Command<" << (void *)&cmd << ">(type: " << cmd.type << ", ";
  switch (cmd.type) {
  case Command::Type::GET:
    output << cmd.key;
    break;
  case Command::Type::PUT:
    output << cmd.key << ", " << cmd.data;
    break;
  case Command::Type::OWN:
    output << "name: " << cmd.name.c_str();
    break;
  default:
    output << "<unknown Command::Type>";
  }
  output << ")";
  return output;
}
