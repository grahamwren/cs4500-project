#pragma once

#include "kv/command.h"
#include <algorithm>
#include <iostream>

using namespace std;

TEST(TestGetCommand, test_serialize_unpack) {
  GetCommand cmd(Key("apples"), 0);
  WriteCursor wc;
  cmd.serialize(wc);

  ReadCursor rc(wc.length(), wc.bytes());
  unique_ptr<Command> cmd2 = Command::unpack(rc);
  EXPECT_TRUE(cmd == *cmd2);
}

TEST(TestPutCommand, test_serialize_unpack) {
  auto s = "Hello world";
  DataChunk dc(strlen(s) + 1, (uint8_t *)s);
  PutCommand cmd(ChunkKey("apples", 0), make_unique<DataChunk>(dc.data()));
  WriteCursor wc;
  cmd.serialize(wc);

  ReadCursor rc(wc.length(), wc.bytes());
  unique_ptr<Command> cmd2 = Command::unpack(rc);
  EXPECT_TRUE(cmd == *cmd2);
}

TEST(TestNewCommand, test_serialize_unpack) {
  NewCommand cmd(Key("apples"), Schema("IFSB"));
  WriteCursor wc;
  cmd.serialize(wc);

  ReadCursor rc(wc.length(), wc.bytes());
  unique_ptr<Command> cmd2 = Command::unpack(rc);
  EXPECT_TRUE(cmd == *cmd2);
}

TEST(TestMapCommand, test_serialize_unpack) {
  shared_ptr<SumRower> rower = make_shared<SumRower>(0);
  MapCommand cmd(Key("apples"), rower);
  WriteCursor wc;
  cmd.serialize(wc);

  ReadCursor rc(wc.length(), wc.bytes());
  unique_ptr<Command> cmd2 = Command::unpack(rc);
  EXPECT_TRUE(cmd == *cmd2);
}

TEST(TestGetOwnedCommand, test_serialize_unpack) {
  GetDFInfoCommand cmd;
  WriteCursor wc;
  cmd.serialize(wc);

  ReadCursor rc(wc.length(), wc.bytes());
  unique_ptr<Command> cmd2 = Command::unpack(rc);
  EXPECT_TRUE(cmd == *cmd2);
}

class TestCommandRun : public ::testing::Test {
public:
  char buf[BUFSIZ];
  KVStore *kv;

  void SetUp() {
    kv = new KVStore;
    /* create "owned" PDFs in kv by adding chunks starting at 0 */
    for (int i = 0; i < 4; i++) {
      sprintf(buf, "owned %d", i);
      Key key(buf);
      Schema scm("IFSB");
      PartialDataFrame &pdf = kv->add_pdf(key, scm);

      /* create chunk at 0 */
      DataFrameChunk dfc(scm);
      Row row(scm);
      for (int i = 0; i < 100; i++) {
        row.set(0, i);
        row.set(1, i * 0.5f);
        row.set(2, new string("iii"));
        row.set(3, i % 2 == 0);
        dfc.add_row(row);
      }

      WriteCursor wc;
      dfc.serialize(wc);
      ReadCursor rc(wc.length(), wc.bytes());
      pdf.add_df_chunk(0, rc);
    }

    /* create non-owned PDFs by adding DFC with non-zero start_idx */
    for (int i = 0; i < 4; i++) {
      int chunk_idx = i + 1; // skip chunk_idx == 0

      sprintf(buf, "not-owned %d", i);
      Key key(buf);
      Schema scm("IFSB");
      PartialDataFrame &pdf = kv->add_pdf(key, scm);

      /* create chunk at chunk_idx */
      DataFrameChunk dfc(scm);
      Row row(scm);
      for (int i = 0; i < 100; i++) {
        row.set(0, i);
        row.set(1, i * 0.5f);
        row.set(2, new string("iii"));
        row.set(3, i % 2 == 0);
        dfc.add_row(row);
      }
      WriteCursor wc;
      dfc.serialize(wc);
      ReadCursor rc(wc.length(), wc.bytes());
      pdf.add_df_chunk(chunk_idx, rc);
    }
  }
  void Teardown() { delete kv; }
};

TEST_F(TestCommandRun, test_put) {
  Key key(string("not-owned 0"));
  const PartialDataFrame &pdf = kv->get_pdf(key);
  int chunk_idx = 2; // insert chunk to follow chunk_idx: 1
  /* expect chunk not in PDF yet */
  EXPECT_FALSE(pdf.has_chunk(chunk_idx));
  const Schema &scm = pdf.get_schema();

  /* create chunk at chunk_idx */
  DataFrameChunk dfc(scm);
  Row row(scm);
  for (int i = 0; i < 100; i++) {
    row.set(0, i);
    row.set(1, i * 0.5f);
    row.set(2, new string("iii"));
    row.set(3, i % 2 == 0);
    dfc.add_row(row);
  }
  WriteCursor wc;
  dfc.serialize(wc);
  PutCommand cmd(ChunkKey(key, chunk_idx),
                 make_unique<DataChunk>(wc.length(), wc.bytes()));
  WriteCursor dest;
  /* command should return true to show success */
  EXPECT_TRUE(cmd.run(*kv, 0, dest));
  /* successful put results in no output */
  EXPECT_EQ(dest.length(), 0);

  /* expect chunk to have been added to PDF */
  EXPECT_TRUE(pdf.has_chunk(chunk_idx));
  /* expect chunk to be the one we added */
  EXPECT_TRUE(pdf.get_chunk(chunk_idx) == dfc);
}

TEST_F(TestCommandRun, test_get) {
  Key key(string("not-owned 0"));
  GetCommand cmd(key, 1);
  WriteCursor dest;
  /* command should return true to show success */
  EXPECT_TRUE(cmd.run(*kv, 0, dest));
  ReadCursor rc(dest.length(), dest.bytes());
  DataFrameChunk dfc(kv->get_pdf(key).get_schema(), rc);
  EXPECT_TRUE(dfc == kv->get_pdf(key).get_chunk(1));

  /* chunk at 0 should not exist because is "not-owned" so command should return
   * false for ERR */
  GetCommand err_cmd(key, 0);
  WriteCursor dest2;
  EXPECT_FALSE(err_cmd.run(*kv, 0, dest2));
  EXPECT_EQ(dest2.length(), 0);
}

TEST_F(TestCommandRun, test_new) {
  Key new_key(string("new df"));
  Schema scm("IIII");
  NewCommand cmd(new_key, scm);
  WriteCursor dest;
  /* command should return true to show success, new command should have no
   * output */
  EXPECT_TRUE(cmd.run(*kv, 0, dest));
  EXPECT_EQ(dest.length(), 0);

  /* expect PDF was added to KVStore */
  EXPECT_TRUE(kv->has_pdf(new_key));
  /* expect has correct Schema */
  EXPECT_TRUE(kv->get_pdf(new_key).get_schema() == scm);
}

TEST_F(TestCommandRun, test_map) {
  Key key(string("owned 0"));
  shared_ptr<SumRower> rower = make_shared<SumRower>(0);
  MapCommand cmd(key, rower);
  WriteCursor dest;
  EXPECT_TRUE(cmd.run(*kv, 0, dest));

  ReadCursor rc(dest.length(), dest.bytes());
  EXPECT_EQ(rower->get_sum_result(), yield<uint64_t>(rc));
  EXPECT_TRUE(empty(rc));
}

TEST_F(TestCommandRun, test_get_df_info) {
  GetDFInfoCommand cmd;
  WriteCursor dest;
  /* command should return true to show success, running command writes output
   * into dest */
  EXPECT_TRUE(cmd.run(*kv, 0, dest));

  ReadCursor rc = dest;
  vector<GetDFInfoCommand::result_t> results;
  GetDFInfoCommand::unpack_results(rc, results);
  EXPECT_TRUE(empty(rc));

  /* expect results for all PDFs in KV */
  EXPECT_EQ(results.size(), 8);

  /* expect results to contain all "owned \d" keys to contain Schemas */
  for (int i = 0; i < 4; i++) {
    sprintf(buf, "owned %d", i);
    /* search for element with "owned %d" Key */
    auto e = find_if(results.begin(), results.end(),
                     [&](auto &e) { return get<Key>(e) == Key(buf); });

    /* expect element to have been found */
    EXPECT_TRUE(e != results.end());
    EXPECT_TRUE(get<Key>(*e) == Key(buf));  // expect key to match
    EXPECT_TRUE(get<optional<Schema>>(*e)); // expect to have Schema
    EXPECT_EQ(get<int>(*e), 0);             // expect largest chunk to be 0
  }

  /* expect results to contain all "not-owned \d" keys to contain Schemas */
  for (int i = 0; i < 4; i++) {
    sprintf(buf, "not-owned %d", i);
    /* search for element with "not-owned %d" Key */
    auto e = find_if(results.begin(), results.end(),
                     [&](auto &e) { return get<Key>(e) == Key(buf); });

    /* expect element to have been found */
    EXPECT_TRUE(e != results.end());
    EXPECT_TRUE(get<Key>(*e) == Key(buf));   // expect key to match
    EXPECT_FALSE(get<optional<Schema>>(*e)); // expect to not have Schema
    EXPECT_EQ(get<int>(*e), i + 1);          // expect largest chunk to be i + 1
  }
}

TEST_F(TestCommandRun, test_get_df_info__with_query_key__owned) {
  GetDFInfoCommand cmd(Key("owned 0"));
  WriteCursor dest;
  /* command should return true to show success, running command writes output
   * into dest */
  EXPECT_TRUE(cmd.run(*kv, 0, dest));

  ReadCursor rc = dest;
  vector<GetDFInfoCommand::result_t> results;
  GetDFInfoCommand::unpack_results(rc, results);
  EXPECT_TRUE(empty(rc));

  /* expect results for one PDF */
  EXPECT_EQ(results.size(), 1);

  /* expect single result to be for Key("owned 0") */
  auto e = results.begin();
  EXPECT_TRUE(get<Key>(*e) == Key("owned 0")); // expect key to match
  EXPECT_TRUE(get<optional<Schema>>(*e));      // expect to have Schema
  EXPECT_EQ(get<int>(*e), 0);                  // expect largest chunk to be 0
}

TEST_F(TestCommandRun, test_get_df_info__with_query_key__not_owned) {
  GetDFInfoCommand cmd(Key("not-owned 0"));
  WriteCursor dest;
  /* command should return true to show success, running command writes output
   * into dest */
  EXPECT_TRUE(cmd.run(*kv, 0, dest));

  ReadCursor rc = dest;
  vector<GetDFInfoCommand::result_t> results;
  GetDFInfoCommand::unpack_results(rc, results);
  EXPECT_TRUE(empty(rc));

  /* expect results for one PDF */
  EXPECT_EQ(results.size(), 1);

  /* expect single result to be for Key("owned 0") */
  auto e = results.begin();
  EXPECT_TRUE(get<Key>(*e) == Key("not-owned 0")); // expect key to match
  EXPECT_FALSE(get<optional<Schema>>(*e));         // expect to not have Schema
  EXPECT_EQ(get<int>(*e), 1); // expect largest chunk to be 1
}
