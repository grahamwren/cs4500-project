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
  PutCommand cmd(Key("apples"), make_unique<DataChunk>(dc.data()));
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
  GetOwnedCommand cmd;
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
    /* create "owned" PDFs in kv by adding rows starting at 0 */
    for (int i = 0; i < 4; i++) {
      sprintf(buf, "owned %d", i);
      Key key(buf);
      Schema scm("IFSB");
      PartialDataFrame &pdf = kv->add_pdf(key, scm);

      Row row(scm);
      for (int i = 0; i < 100; i++) {
        row.set(0, i);
        row.set(1, i * 0.5f);
        row.set(2, new string("iii"));
        row.set(3, i % 2 == 0);
        pdf.add_row(row);
      }
    }

    /* create non-owned PDFs by adding DFC with non-zero start_idx */
    for (int i = 0; i < 4; i++) {
      int chunk_idx = i + 1; // skip chunk_idx == 0

      sprintf(buf, "not-owned %d", i);
      Key key(buf);
      Schema scm("IFSB");
      PartialDataFrame &pdf = kv->add_pdf(key, scm);

      /* create chunk at chunk_idx */
      DataFrameChunk dfc(scm, chunk_idx * DF_CHUNK_SIZE);
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
      pdf.add_df_chunk(rc);
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
  DataFrameChunk dfc(scm, chunk_idx * DF_CHUNK_SIZE);
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
  PutCommand cmd(key, make_unique<DataChunk>(wc.length(), wc.bytes()));
  WriteCursor dest;
  /* command should return true to show success */
  EXPECT_TRUE(cmd.run(*kv, 0, dest));
  /* successful put results in no output */
  EXPECT_EQ(dest.length(), 0);

  /* expect chunk to have been added to PDF */
  EXPECT_TRUE(pdf.has_chunk(chunk_idx));
  /* expect chunk to be the one we added */
  EXPECT_TRUE(pdf.get_chunk_by_chunk_idx(chunk_idx) == dfc);
}

TEST_F(TestCommandRun, test_get) {
  Key key(string("not-owned 0"));
  GetCommand cmd(key, 1);
  WriteCursor dest;
  /* command should return true to show success */
  EXPECT_TRUE(cmd.run(*kv, 0, dest));
  DataFrameChunk dfc(kv->get_pdf(key).get_schema(), 1);
  ReadCursor rc(dest.length(), dest.bytes());
  dfc.fill(rc);
  EXPECT_TRUE(dfc == kv->get_pdf(key).get_chunk_by_chunk_idx(1));

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

TEST_F(TestCommandRun, test_get_owned) {
  GetOwnedCommand cmd;
  WriteCursor dest;
  /* command should return true to show success, running command writes output
   * into dest */
  EXPECT_TRUE(cmd.run(*kv, 0, dest));

  ReadCursor rc(dest.length(), dest.bytes());
  vector<Key> key_results;
  vector<Schema> scm_results;
  /* yield results until rc is empty */
  for (int i = 0; has_next(rc); i++) {
    key_results.emplace_back(yield<Key>(rc));
    scm_results.emplace_back(yield<Schema>(rc));
  }
  EXPECT_TRUE(empty(rc));

  /* expect results to only be the keys for owned PDFs
   * (which have chunk_idx: 0) */
  EXPECT_EQ(key_results.size(), 4);
  EXPECT_EQ(scm_results.size(), 4);

  /* expect results to contain all "owned \d" keys */
  for (int i = 0; i < 4; i++) {
    sprintf(buf, "owned %i", i);
    Key k = Key(string(buf)); // -Wvexing-parse
    EXPECT_FALSE(find(key_results.begin(), key_results.end(), k) ==
                 key_results.end());
  }
}
