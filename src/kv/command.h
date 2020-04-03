#pragma once

#include "chunk_key.h"
#include "data_chunk.h"
#include "kv_store.h"
#include "network/packet.h"
#include "schema.h"
#include "sized_ptr.h"
#include <iostream>
#include <memory>
#include <string>

using namespace std;

class Command {
public:
  enum Type : uint8_t { GET, PUT, NEW, GET_OWNED };
  virtual ~Command() {}
  static unique_ptr<Command> unpack(ReadCursor &);
  virtual bool run(KVStore &, const IpV4Addr &, WriteCursor &) const = 0;
  virtual Type get_type() const = 0;
  virtual void serialize(WriteCursor &) const = 0;
  virtual ostream &out(ostream &) const = 0;
  virtual bool equals(const Command &) const = 0;
  bool operator==(const Command &other) const { return equals(other); }
};

class GetCommand : public Command {
private:
  Key key;
  int chunk_idx;

public:
  GetCommand(const Key &key, int i) : key(key), chunk_idx(i) {}
  GetCommand(ReadCursor &c) : key(yield<Key>(c)), chunk_idx(yield<int>(c)) {}
  Type get_type() const { return Type::GET; }

  bool run(KVStore &kv, const IpV4Addr &src, WriteCursor &dest) const {
    if (kv.has_pdf(key)) {
      PartialDataFrame &pdf = kv.get_pdf(key);
      if (pdf.has_chunk(chunk_idx)) {
        const DataFrameChunk &dfc = pdf.get_chunk_by_chunk_idx(chunk_idx);
        dfc.serialize(dest);
        return true;
      }
    }
    return false;
  }

  void serialize(WriteCursor &wc) const {
    pack(wc, get_type());
    pack<const Key &>(wc, key);
    pack(wc, chunk_idx);
  }

  ostream &out(ostream &output) const {
    output << "GET(key: " << key << ", chunk_idx: " << chunk_idx << ")";
    return output;
  }

  bool equals(const Command &o) const {
    if (get_type() == o.get_type()) {
      const GetCommand &other = dynamic_cast<const GetCommand &>(o);
      return chunk_idx == other.chunk_idx && key == other.key;
    }
    return false;
  }
};

class PutCommand : public Command {
private:
  Key key;
  std::unique_ptr<DataChunk> data;

public:
  PutCommand(const Key &key, unique_ptr<DataChunk> &&dc)
      : key(key), data(move(dc)) {}
  PutCommand(ReadCursor &c)
      : key(yield<Key>(c)), data(new DataChunk(yield<sized_ptr<uint8_t>>(c))) {}

  Type get_type() const { return Type::PUT; }

  bool run(KVStore &kv, const IpV4Addr &src, WriteCursor &dest) const {
    if (kv.has_pdf(key)) {
      PartialDataFrame &pdf = kv.get_pdf(key);
      ReadCursor rc(data->len(), data->ptr());
      pdf.add_df_chunk(rc);
      return true;
    }
    return false;
  }

  void serialize(WriteCursor &wc) const {
    pack(wc, get_type());
    pack<const Key &>(wc, key);
    pack(wc, data->data());
  }

  ostream &out(ostream &output) const {
    output << "PUT(key: " << key << ", data: " << *data << ")";
    return output;
  }

  bool equals(const Command &o) const {
    if (get_type() == o.get_type()) {
      const PutCommand &other = dynamic_cast<const PutCommand &>(o);
      return key == other.key &&
             (data && other.data ? *data == *other.data : data == other.data);
    }
    return false;
  }
};

class NewCommand : public Command {
private:
  Key key;
  Schema scm;

public:
  NewCommand(const Key &key, const Schema &scm) : key(key), scm(scm) {}
  NewCommand(ReadCursor &c) : key(yield<Key>(c)), scm(yield<Schema>(c)) {}

  bool run(KVStore &kv, const IpV4Addr &src, WriteCursor &dest) const {
    if (kv.has_pdf(key))
      return false;

    kv.add_pdf(key, scm);
    return true;
  }

  Type get_type() const { return Type::NEW; }

  void serialize(WriteCursor &wc) const {
    pack(wc, get_type());
    pack<const Key &>(wc, key);
    pack<const Schema &>(wc, scm);
  }

  ostream &out(ostream &output) const {
    output << "NEW(key: " << key << ", scm: " << scm << ")";
    return output;
  }

  bool equals(const Command &o) const {
    if (get_type() == o.get_type()) {
      const NewCommand &other = dynamic_cast<const NewCommand &>(o);
      return key == other.key && scm == other.scm;
    }
    return false;
  }
};

class GetOwnedCommand : public Command {
public:
  GetOwnedCommand() = default;
  GetOwnedCommand(ReadCursor &c) {}
  Type get_type() const { return Type::GET_OWNED; }

  bool run(KVStore &kv, const IpV4Addr &src, WriteCursor &dest) const {
    /* pack all the keys for PDFs which have chunk 0 */
    kv.for_each([&](const pair<const Key, PartialDataFrame> &e) {
      if (e.second.has_chunk(0)) {
        pack<const Key &>(dest, e.first);
        pack<const Schema &>(dest, e.second.get_schema());
      }
    });
    return true;
  }

  void serialize(WriteCursor &wc) const { pack(wc, get_type()); }

  ostream &out(ostream &output) const {
    output << "GET_OWNED()";
    return output;
  }

  bool equals(const Command &o) const { return get_type() == o.get_type(); }
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
  c.out(output);
  return output;
}

unique_ptr<Command> Command::unpack(ReadCursor &c) {
  Command::Type type = yield<Command::Type>(c);
  switch (type) {
  case Command::Type::GET:
    return make_unique<GetCommand>(c);
  case Command::Type::PUT:
    return make_unique<PutCommand>(c);
  case Command::Type::NEW:
    return make_unique<NewCommand>(c);
  case Command::Type::GET_OWNED:
    return make_unique<GetOwnedCommand>(c);
  default:
    assert(false); // unknown type
  }
}
