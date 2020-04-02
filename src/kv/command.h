#pragma once

#include "chunk_key.h"
#include "data_chunk.h"
#include "sized_ptr.h"
#include "schema.h"
#include <cstring>
#include <iostream>
#include <functional>

class KV;
class IpV4Addr;

class Command {

public:
  enum Type : uint8_t { GET, PUT, NEW };

  virtual ~Command() {}

  // temporary return type
  virtual optional<sized_ptr<uint8_t>> run(KV &kv, IpV4Addr &src) = 0;

  virtual Type get_type() const = 0;

  virtual void serialize(WriteCursor &wc) const = 0;

  virtual ostream &print(ostream &output) const = 0;
};

class GetCommand : public Command {

  const ChunkKey *key;

public:

  GetCommand(const ChunkKey *key): key(key) {}
  virtual ~GetCommand() { delete key; }

  optional<sized_ptr<uint8_t>> run(KV &kv, IpV4Addr &src);

  Type get_type() const {
    return Type::GET;
  }

  void serialize(WriteCursor &wc) const {
    key->serialize(wc);
  }

  ostream &print(ostream &output) const {
    output << key;
    return output;
  }
};

class PutCommand : public Command {

  const ChunkKey *key;
  std::unique_ptr<DataChunk> data;

public:

  PutCommand(const ChunkKey *key, std::unique_ptr<DataChunk> &&dc): key(key) {}
  virtual ~PutCommand() { delete key; }

  Type get_type() const {
    return Type::PUT;
  }

  optional<sized_ptr<uint8_t>> run(KV &kv, IpV4Addr &src);

  void serialize(WriteCursor &wc) const {
    key->serialize(wc);
    data->serialize(wc);
  }
  
  ostream &print(ostream &output) const {
    output << "key: " << *key << ", data: " << *data;
    return output;
  }
};

class NewCommand : public Command {

  const std::string *name;
  Schema *scm;

public:

  NewCommand(const std::string *name, Schema *scm): name(name), scm(scm) {}
  virtual ~NewCommand() { delete name; delete scm; }

  optional<sized_ptr<uint8_t>> run(KV &kv, IpV4Addr &src);

  Type get_type() const {
    return Type::PUT;
  }

  void serialize(WriteCursor &wc) const {
    pack<const std::string &>(wc, *name);
    // something with schema
  }
  
  ostream &print(ostream &output) const {
    char scm_str[scm->width()];
    scm->c_str(scm_str);
    output << "name: " << *name << ", schema: " << scm_str;
    return output;
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
  case Command::Type::NEW:
    output << "NEW";
    break;
  default:
    output << "<unknown Command::Type>";
  }
  return output;
}

ostream &operator<<(ostream &output, const Command &c) {
  output << "Command<" << (void *)&c << ">(type: " << c.get_type() << ", ";
  c.print(output);
  output << ")";
  return output;
}

shared_ptr<Command> deserialize_command(ReadCursor &c) {
  Command::Type type(yield<Command::Type>(c));

  if (type == Command::Type::GET) {
      Command* command = dynamic_cast<Command*>(new GetCommand(new ChunkKey(c)));
      return std::shared_ptr<Command>(command);
  }

  if (type == Command::Type::PUT) {
      ChunkKey* key = new ChunkKey(c);
      std::unique_ptr<DataChunk> data(new DataChunk(c));
      Command* command = dynamic_cast<Command*>(new PutCommand(key, move(data)));
      return std::shared_ptr<Command>(move(command));
  }

  if (type == Command::Type::NEW) {
      std::string* name = new std::string(yield<std::string>(c));
      // placeholder
      Schema* s = new Schema("F");
      Command* command = dynamic_cast<Command*>(new NewCommand(name, s));
      return std::shared_ptr<Command>(command);
  }

  cout << "EXPECTED: OneOf[" << Command::Type::GET << "," << Command::Type::PUT << ","
       << Command::Type::NEW << "] was: " << type << endl;
  assert(false); // unknown type/malformed bytes
}

