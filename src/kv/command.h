#pragma once

#include "chunk_key.h"
#include "data_chunk.h"
#include "sized_ptr.h"
#include <cstring>
#include <iostream>

class Command {
public:
  enum Type : uint8_t { GET, PUT, OWN };

  const Type type;
  ChunkKey key;   // <------+-- get
  DataChunk data; //     <--+-- put
  string name;    // <-- own

  Command(const ChunkKey &k) : type(Type::GET), key(k) {}
  Command(const ChunkKey &k, const DataChunk &dc)
      : type(Type::PUT), key(k), data(dc.data()) {}
  Command(const string &ns) : type(Type::OWN), name(ns) {}
  Command(ReadCursor &c) : type(yield<Type>(c)) {
    switch (type) {
    case Type::GET:
      key = ChunkKey(c);
      break;
    case Type::PUT:
      key = ChunkKey(c);
      data = DataChunk(c);
      break;
    case Type::OWN:
      name = yield<string>(c);
      break;
    default:
      cout << "EXPECTED: OneOf[" << Type::GET << "," << Type::PUT << ","
           << Type::OWN << "] was: " << type << endl;
      assert(false); // unknown type/malformed bytes
    }
  }

  void serialize(WriteCursor &wc) const {
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

  bool operator==(const Command &other) const {
    if (type == other.type) {
      switch (type) {
      case Type::GET:
        return key == other.key;
      case Type::PUT:
        return key == other.key && data == other.data;
      case Type::OWN:
        return name.compare(other.name) == 0;
      }
    }
    return false;
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
    output << "key: " << cmd.key << ", data: " << cmd.data;
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
