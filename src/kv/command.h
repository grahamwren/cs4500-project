#pragma once

#include "chunk_key.h"
#include "data_chunk.h"
#include <cstring>

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

  Command(ReadCursor &c) : type(yield<Command::Type>(c)) {
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
    }
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
