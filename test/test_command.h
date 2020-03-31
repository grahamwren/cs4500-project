#include "kv/command.h"
#include <iostream>

using namespace std;

TEST(TestCommand, serialize) {
  string name("apples");
  ChunkKey ck(name, 0);
  auto data = "Hello world";
  unique_ptr<DataChunk> dc(new DataChunk(strlen(data) + 1, (uint8_t *)data));

  Command get_cmd(ck);
  WriteCursor wc;
  get_cmd.serialize(wc);
  ReadCursor rc(wc.length(), wc.bytes());
  Command dser_get(rc);
  EXPECT_TRUE(get_cmd == dser_get);

  Command put_cmd(ck, move(dc));
  WriteCursor wc2;
  put_cmd.serialize(wc2);
  ReadCursor rc2(wc2.length(), wc2.bytes());
  Command dser_put(rc2);
  EXPECT_TRUE(put_cmd == dser_put);

  Command own_cmd(name);
  WriteCursor wc3;
  own_cmd.serialize(wc3);
  ReadCursor rc3(wc3.length(), wc3.bytes());
  Command dser_own(rc3);
  EXPECT_TRUE(own_cmd == dser_own);
}
