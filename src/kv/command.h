#pragma once

#include "chunk_key.h"
#include "data_chunk.h"
#include "kv_store.h"
#include "network/node.h"
#include "network/packet.h"
#include "rowers.h"
#include "schema.h"
#include "sized_ptr.h"
#include <iostream>
#include <memory>
#include <string>

using namespace std;

class Command {
public:
  enum Type : uint8_t { GET, PUT, NEW, GET_DF_INFO, MAP, DELETE };
  virtual ~Command() {}
  static unique_ptr<Command> unpack(ReadCursor &);
  virtual void run(KVStore &, const IpV4Addr &,
                   const Node::respond_fn_t &) const = 0;
  virtual Type get_type() const = 0;
  virtual void serialize(WriteCursor &) const = 0;
  virtual ostream &out(ostream &) const = 0;
  virtual bool equals(const Command &) const = 0;
  bool operator==(const Command &other) const { return equals(other); }
};

class GetCommand : public Command {
private:
  ChunkKey ckey;

public:
  GetCommand(const ChunkKey &ckey) : ckey(ckey) {}
  GetCommand(const Key &key, int i) : ckey(key, i) {}
  GetCommand(ReadCursor &c) : ckey(yield<Key>(c), yield<int>(c)) {}
  Type get_type() const { return Type::GET; }

  void run(KVStore &kv, const IpV4Addr &src,
           const Node::respond_fn_t &respond) const {
    if (kv.has_pdf(ckey.key)) {
      PartialDataFrame &pdf = kv.get_pdf(ckey.key);
      if (pdf.has_chunk(ckey.chunk_idx)) {
        const DataFrameChunk &dfc = pdf.get_chunk(ckey.chunk_idx);
        WriteCursor wc;
        dfc.serialize(wc);
        return respond(true, sized_ptr(wc.length(), wc.bytes()));
      }
    }
    return respond(false);
  }

  void serialize(WriteCursor &wc) const {
    pack(wc, get_type());
    pack<const ChunkKey &>(wc, ckey);
  }

  ostream &out(ostream &output) const {
    output << ckey;
    return output;
  }

  bool equals(const Command &o) const {
    if (get_type() == o.get_type()) {
      const GetCommand &other = dynamic_cast<const GetCommand &>(o);
      return ckey == other.ckey;
    }
    return false;
  }
};

class PutCommand : public Command {
private:
  ChunkKey chunk_key;
  std::unique_ptr<DataChunk> data;

public:
  PutCommand(const ChunkKey &chunk_key, unique_ptr<DataChunk> &&dc)
      : chunk_key(chunk_key), data(move(dc)) {}
  PutCommand(ReadCursor &c)
      : chunk_key(yield<ChunkKey>(c)),
        data(new DataChunk(yield<sized_ptr<uint8_t>>(c))) {}

  Type get_type() const { return Type::PUT; }

  void run(KVStore &kv, const IpV4Addr &src,
           const Node::respond_fn_t &respond) const {
    if (!kv.has_pdf(chunk_key.key))
      return respond(false); // returns and responds ERR
    else
      respond(true); // just respond OK

    PartialDataFrame &pdf = kv.get_pdf(chunk_key.key);
    ReadCursor rc(data->len(), data->ptr());
    if (pdf.has_chunk(chunk_key.chunk_idx)) {
      pdf.replace_df_chunk(chunk_key.chunk_idx, rc);
    } else {
      pdf.add_df_chunk(chunk_key.chunk_idx, rc);
    }
  }

  void serialize(WriteCursor &wc) const {
    pack(wc, get_type());
    pack<const ChunkKey &>(wc, chunk_key);
    pack(wc, data->data());
  }

  ostream &out(ostream &output) const {
    output << "key: " << chunk_key << ", data: " << *data;
    return output;
  }

  bool equals(const Command &o) const {
    if (get_type() == o.get_type()) {
      const PutCommand &other = dynamic_cast<const PutCommand &>(o);
      return chunk_key == other.chunk_key &&
             (data && other.data ? *data == *other.data : data == other.data);
    }
    return false;
  }
};

class DeleteCommand : public Command {
private:
  Key key;

public:
  DeleteCommand(const Key &key) : key(key) {}
  DeleteCommand(ReadCursor &c) : key(yield<Key>(c)) {}

  void run(KVStore &kv, const IpV4Addr &src,
           const Node::respond_fn_t &respond) const {
    kv.remove_pdf(key);
    return respond(true);
  }

  Type get_type() const { return Type::DELETE; }

  void serialize(WriteCursor &wc) const {
    pack(wc, get_type());
    pack<const Key &>(wc, key);
  }

  ostream &out(ostream &output) const {
    output << "key: " << key;
    return output;
  }

  bool equals(const Command &o) const {
    if (get_type() == o.get_type()) {
      const DeleteCommand &other = dynamic_cast<const DeleteCommand &>(o);
      return key == other.key;
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

  void run(KVStore &kv, const IpV4Addr &src,
           const Node::respond_fn_t &respond) const {
    if (kv.has_pdf(key))
      return respond(false);

    kv.add_pdf(key, scm);
    return respond(true);
  }

  Type get_type() const { return Type::NEW; }

  void serialize(WriteCursor &wc) const {
    pack(wc, get_type());
    pack<const Key &>(wc, key);
    pack<const Schema &>(wc, scm);
  }

  ostream &out(ostream &output) const {
    output << "key: " << key << ", scm: " << scm;
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

class GetDFInfoCommand : public Command {
private:
  optional<Key> query_key;

public:
  typedef tuple<Key, optional<Schema>, int> result_t;

  GetDFInfoCommand() = default;
  GetDFInfoCommand(const optional<Key> &key) : query_key(key) {}
  GetDFInfoCommand(const Key &key) : query_key(key) {}
  GetDFInfoCommand(ReadCursor &c) {
    if (yield<bool>(c))
      query_key = yield<Key>(c);
  }
  Type get_type() const { return Type::GET_DF_INFO; }

  void run(KVStore &kv, const IpV4Addr &src,
           const Node::respond_fn_t &respond) const {
    /* if command has query_key, just get info for this query_key */
    if (query_key) {
      if (kv.has_pdf(*query_key)) {
        const PartialDataFrame &pdf = kv.get_pdf(*query_key);
        WriteCursor wc;
        pack<const Key &>(wc, *query_key);
        if (pdf.has_chunk(0)) {
          pack<bool>(wc, true); // pack true if this Node has chunk 0
          pack<const Schema &>(wc, pdf.get_schema());
        } else {
          /* pack false because not the owner of this DF */
          pack<bool>(wc, false);
        }
        pack<int>(wc, pdf.largest_chunk_idx());
        return respond(true, sized_ptr(wc.length(), wc.bytes()));
      }
      return respond(false);
    } else {
      WriteCursor wc;
      /* traverse all keys checking for owned DF */
      kv.for_each([&](const pair<const Key, PartialDataFrame> &e) {
        const Key &key = e.first;
        const PartialDataFrame &pdf = e.second;

        pack<const Key &>(wc, key);
        if (pdf.has_chunk(0)) {
          pack<bool>(wc, true); // pack true if this Node has chunk 0
          pack<const Schema &>(wc, pdf.get_schema());
        } else {
          /* pack false because not the owner of this DF */
          pack<bool>(wc, false);
        }
        pack<int>(wc, pdf.largest_chunk_idx());
      });
      return respond(true, sized_ptr(wc.length(), wc.bytes()));
    }
  }

  void serialize(WriteCursor &wc) const {
    pack(wc, get_type());
    pack(wc, !!query_key);
    if (query_key)
      pack<const Key &>(wc, *query_key);
  }

  ostream &out(ostream &output) const { return output; }

  bool equals(const Command &o) const { return get_type() == o.get_type(); }

  static void unpack_results(ReadCursor &c, vector<result_t> &results) {
    while (has_next(c)) {
      Key key = yield<Key>(c);
      if (yield<bool>(c)) {
        results.emplace_back(make_tuple(key, yield<Schema>(c), yield<int>(c)));
      } else {
        results.emplace_back(make_tuple(key, nullopt, yield<int>(c)));
      }
    }
  }
};

class MapCommand : public Command {
private:
  Key key;
  shared_ptr<Rower> rower;

public:
  MapCommand(const Key &key, shared_ptr<Rower> rower) : key(key), rower(rower) {
    assert(rower);
  }
  MapCommand(ReadCursor &c) : key(yield<Key>(c)), rower(unpack_rower(c)) {}
  Type get_type() const { return Type::MAP; }

  void run(KVStore &kv, const IpV4Addr &src,
           const Node::respond_fn_t &respond) const {
    if (kv.has_pdf(key)) {
      PartialDataFrame &pdf = kv.get_pdf(key);
      pdf.map(*rower); // local map
      WriteCursor wc;
      rower->serialize_results(wc);
      return respond(true, sized_ptr(wc.length(), wc.bytes()));
    }
    return respond(false);
  }

  void serialize(WriteCursor &wc) const {
    pack(wc, get_type());
    pack<const Key &>(wc, key);
    rower->serialize(wc);
  }

  ostream &out(ostream &output) const {
    output << "(key: " << key << ", rower: " << rower->get_type() << ")";
    return output;
  }

  bool equals(const Command &o) const {
    if (get_type() == o.get_type()) {
      const MapCommand &other = dynamic_cast<const MapCommand &>(o);
      return key == other.key && rower->get_type() == other.rower->get_type();
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
  case Command::Type::NEW:
    output << "NEW";
    break;
  case Command::Type::DELETE:
    output << "DELETE";
    break;
  case Command::Type::MAP:
    output << "MAP";
    break;
  case Command::Type::GET_DF_INFO:
    output << "GET_DF_INFO";
    break;
  default:
    output << "<unknown Command::Type>";
  }
  return output;
}

ostream &operator<<(ostream &output, const Command &c) {
  output << "Command(type: " << c.get_type() << ", ";
  c.out(output);
  output << ")";
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
  case Command::Type::DELETE:
    return make_unique<DeleteCommand>(c);
  case Command::Type::GET_DF_INFO:
    return make_unique<GetDFInfoCommand>(c);
  case Command::Type::MAP:
    return make_unique<MapCommand>(c);
  default:
    cout << "ERROR: unknown command type: " << (unsigned int)type << endl;
    assert(false); // unknown type
  }
}
