#pragma once

#include "chunk_key.h"
#include "data_chunk.h"
#include "kv_store.h"
#include "lib/rowers.h"
#include "lib/schema.h"
#include "lib/sized_ptr.h"
#include "network/node.h"
#include "network/packet.h"
#include <iostream>
#include <memory>
#include <string>

using namespace std;

/**
 * shared parent class for all Commands which you can issue to a KVNode
 *
 * authors: @grahamwren, @jagen31
 */
class Command {
protected:
  /* TODO: clearly an abstraction missing here. Could issue commands by Type and
   * Args object */
  virtual void serialize_args(WriteCursor &) const = 0;

public:
  enum Type : uint8_t {
    GET,
    PUT,
    NEW,
    GET_DF_INFO,
    START_MAP,
    FETCH_MAP_RESULT,
    DELETE
  };
  virtual ~Command() {}
  static unique_ptr<Command> unpack(ReadCursor &);
  /* execute the command, usually by fetching data from or mutating the KVStore
   * object */
  virtual void run(KVStore &, const IpV4Addr &,
                   const Node::respond_fn_t &) const = 0;
  virtual Type get_type() const = 0;
  virtual ostream &out(ostream &) const = 0;
  virtual bool equals(const Command &) const = 0;

  void serialize(WriteCursor &c) const {
    pack(c, get_type());
    serialize_args(c);
  }
  bool operator==(const Command &other) const { return equals(other); }
};

/**
 * Request a DataFrameChunk from a node in the cluster, responds with an OK and
 * the serialized DataFrameChunk in the body if is available in the node,
 * otherwise responds with an ERR and no data in the body
 *
 * authors: @grahamwren, @jagen31
 */
class GetCommand : public Command {
private:
  ChunkKey ckey;

protected:
  void serialize_args(WriteCursor &wc) const {
    pack<const ChunkKey &>(wc, ckey);
  }

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
        return respond(true, move(wc));
      }
    }
    return respond(false);
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

/**
 * Put a DataFrameChunk in a Node. Args are a ChunkKey and the DataChunk to be
 * put in the Node. Responds with an OK and no data when successful, otherwise
 * an ERR and no data. A NewCommand for this Key must have already been send to
 * the Node.
 *
 * authors: @grahamwren, @jagen31
 */
class PutCommand : public Command {
private:
  ChunkKey chunk_key;
  DataChunk data;

protected:
  void serialize_args(WriteCursor &wc) const {
    pack<const ChunkKey &>(wc, chunk_key);
    pack(wc, data.data());
  }

public:
  PutCommand(const ChunkKey &chunk_key, const DataChunk &dc)
      : chunk_key(chunk_key), data(dc) {}
  PutCommand(ReadCursor &c)
      : chunk_key(yield<ChunkKey>(c)),
        /* borrow data from ReadCursor 🤞 */
        data(yield<sized_ptr<uint8_t>>(c), true) {}

  Type get_type() const { return Type::PUT; }

  void run(KVStore &kv, const IpV4Addr &src,
           const Node::respond_fn_t &respond) const {
    if (!kv.has_pdf(chunk_key.key))
      return respond(false); // returns and responds ERR
    else
      respond(true); // just respond OK

    PartialDataFrame &pdf = kv.get_pdf(chunk_key.key);
    ReadCursor rc(data.data());
    if (pdf.has_chunk(chunk_key.chunk_idx)) {
      pdf.replace_df_chunk(chunk_key.chunk_idx, rc);
    } else {
      pdf.add_df_chunk(chunk_key.chunk_idx, rc);
    }
  }

  ostream &out(ostream &output) const {
    output << "key: " << chunk_key << ", data: " << data;
    return output;
  }

  bool equals(const Command &o) const {
    if (get_type() == o.get_type()) {
      const PutCommand &other = dynamic_cast<const PutCommand &>(o);
      return chunk_key == other.chunk_key && data == other.data;
    }
    return false;
  }
};

/**
 * Delete all DataFrameChunks for the given Key in the Node. Responds with OK
 * and no data.
 *
 * authors: @grahamwren, @jagen31
 */
class DeleteCommand : public Command {
private:
  Key key;

protected:
  void serialize_args(WriteCursor &wc) const { pack<const Key &>(wc, key); }

public:
  DeleteCommand(const Key &key) : key(key) {}
  DeleteCommand(ReadCursor &c) : key(yield<Key>(c)) {}

  void run(KVStore &kv, const IpV4Addr &src,
           const Node::respond_fn_t &respond) const {
    kv.remove_pdf(key);
    return respond(true);
  }

  Type get_type() const { return Type::DELETE; }

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

/**
 * Notify a Node of a new DF in the cluster. Includes the Key and the Schema.
 * Must be sent to a Node before a PutCommand with this Key can be sent to the
 * Node.
 *
 * authors: @grahamwren, @jagen31
 */
class NewCommand : public Command {
private:
  Key key;
  Schema scm;

protected:
  void serialize_args(WriteCursor &wc) const {
    pack<const Key &>(wc, key);
    pack<const Schema &>(wc, scm);
  }

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

/**
 * Get information about a particular DataFrame on the Node, or about every
 * DataFrame stored on the Node. Sent with no Args, response is information on
 * all DataFrames on the Node. Sent with a Key as an Arg, response in
 * information on just the DataFrame associated with the given Key.
 *
 * authors: @grahamwren, @jagen31
 */
class GetDFInfoCommand : public Command {
private:
  optional<Key> query_key;

protected:
  void serialize_args(WriteCursor &wc) const {
    pack(wc, !!query_key);
    if (query_key)
      pack<const Key &>(wc, *query_key);
  }

public:
  typedef tuple<Key, optional<Schema>, int> result_t;

  GetDFInfoCommand() = default;
  GetDFInfoCommand(const optional<Key> &key) : query_key(key) {}
  GetDFInfoCommand(const Key &key) : query_key(key) {}
  GetDFInfoCommand(ReadCursor &c)
      : query_key(yield<bool>(c) ? optional(yield<Key>(c)) : nullopt) {}
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
        return respond(true, move(wc));
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
      return respond(true, move(wc));
    }
  }

  ostream &out(ostream &output) const { return output; }

  bool equals(const Command &o) const { return get_type() == o.get_type(); }

  /* used to unpack the results from this Command */
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

/**
 * Start the given Rower on the DataFrame associated with the given Key. Applies
 * the Rower to all chunks stored on the Node. Responds with OK and a result ID
 * in the body if the Rower is successfully started. To get the results of the
 * map, issue a FetchMapResult command with the provided result ID. After
 * responding, this Command blocks the main thread of the Node until it is
 * successful so the Node will NOT respond to packets or commands until it is
 * done mapping.
 *
 * authors: @grahamwren, @jagen31
 */
class StartMapCommand : public Command {
private:
  Key key;
  shared_ptr<Rower> rower;

protected:
  void serialize_args(WriteCursor &wc) const {
    pack<const Key &>(wc, key);
    rower->serialize(wc);
  }

public:
  StartMapCommand(const Key &key, shared_ptr<Rower> rower)
      : key(key), rower(rower) {
    assert(rower);
  }
  StartMapCommand(ReadCursor &c) : key(yield<Key>(c)), rower(unpack_rower(c)) {}
  Type get_type() const { return Type::START_MAP; }

  void run(KVStore &kv, const IpV4Addr &src,
           const Node::respond_fn_t &respond) const {
    if (!kv.has_pdf(key)) {
      return respond(false); // return and respond ERR
    }
    int result_id = kv.get_result_id();
    WriteCursor wc;
    pack(wc, result_id);
    /* respond OK early with result_id */
    respond(true, move(wc));

    WriteCursor wc2;

    /* compute map result */
    PartialDataFrame &pdf = kv.get_pdf(key);
    pdf.map(*rower); // local map
    rower->serialize_results(wc2);

    /* store result under result_id */
    kv.insert_map_result(result_id, move(wc2));
  }

  ostream &out(ostream &output) const {
    output << "key: " << key << ", rower: " << *rower;
    return output;
  }

  bool equals(const Command &o) const {
    if (get_type() == o.get_type()) {
      const StartMapCommand &other = dynamic_cast<const StartMapCommand &>(o);
      return key == other.key && rower->get_type() == other.rower->get_type();
    }
    return false;
  }
};

/**
 * Command to send to fetch the result of a previous StartMapCommand, takes an
 * argument of a result_id and looks in the KVStore for data stored under that
 * id. Responds with OK and the serialized result of the Rower if successful.
 *
 * authors: @grahamwren, @jagen31
 */
class FetchMapResultCommand : public Command {
private:
  int result_id;

protected:
  void serialize_args(WriteCursor &wc) const { pack<int>(wc, result_id); }

public:
  FetchMapResultCommand(int result_id) : result_id(result_id) {}
  FetchMapResultCommand(ReadCursor &c) : result_id(yield<int>(c)) {}
  Type get_type() const { return Type::FETCH_MAP_RESULT; }

  void run(KVStore &kv, const IpV4Addr &src,
           const Node::respond_fn_t &respond) const {
    if (kv.has_map_result(result_id)) {
      respond(true, kv.get_map_result(result_id));
      kv.remove_map_result(result_id);
    } else {
      respond(false);
    }
  }

  ostream &out(ostream &output) const {
    output << "result_id: " << result_id;
    return output;
  }

  bool equals(const Command &o) const {
    if (get_type() == o.get_type()) {
      const FetchMapResultCommand &other =
          dynamic_cast<const FetchMapResultCommand &>(o);
      return result_id == other.result_id;
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
  case Command::Type::START_MAP:
    output << "START_MAP";
    break;
  case Command::Type::FETCH_MAP_RESULT:
    output << "FETCH_MAP_RESULT";
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
  output << "Command(type: " << c.get_type() << " {";
  c.out(output);
  output << "})";
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
  case Command::Type::START_MAP:
    return make_unique<StartMapCommand>(c);
  case Command::Type::FETCH_MAP_RESULT:
    return make_unique<FetchMapResultCommand>(c);
  default:
    cout << "ERROR: unknown command type: " << (unsigned int)type << endl;
    assert(false); // unknown type
  }
}
