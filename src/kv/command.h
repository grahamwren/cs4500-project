#pragma once

#include "chunk_key.h"
#include "data_chunk.h"
#include <cstring>

class Command {
public:
  enum Type : uint8_t { GET, PUT, DIRECTORY };

  const Type type;
  union {
    struct {                   // <------+
      ChunkKey key;            // <------|-- get
      DataChunk data;          //        +-- put
    };                         // <------+
    vector<ChunkKey> dir_keys; // <-- directory
  };

  Command(ChunkKey &k) : type(Type::GET), key(k) {}
  Command(ChunkKey &k, DataChunk &dc)
      : type(Type::PUT), key(k), DataChunk(data) {}
  Command(vector<ChunkKey> &&keys) : type(Type::DIRECTORY), dir_keys(keys) {}

  Command(ReadCursor &c) : type(yield<uint8_t>(c)) {
    switch (type) {
    case Type::GET:
      key = ChunkKey(c);
      break;
    case Type::PUT:
      key = ChunkKey(c);
      data = DataChunk(c);
      break;
    case Type::DIRECTORY:
      int len = yield<int>(c);
      dir_keys.reserve(len);
      for (int i = 0; i < len; i++) {
        dir_keys.emplace_back(c);
      }
      break;
    }
  }

  void serialize(BytesWriter &writer) {
    writer.pack(type);
    switch (type) {
    case Type::GET:
      key.serialize(writer);
      break;
    case Type::PUT:
      key.serialize(writer);
      data.serialize(writer);
      break;
    case Type::DIRECTORY:
      writer.pack((int)dir_keys.size());
      for (auto it = dir_keys.begin(); it != dir_keys.end(); it++) {
        it->serialize(writer);
      }
      break;
    }
  }
};
