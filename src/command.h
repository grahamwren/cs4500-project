#include "serialize.h"

unsigned char DATA = 0;
unsigned char GET = 1;
unsigned char SEND = 2;

class Command : public Object {
public:
  virtual int pack(unsigned char *buf);
}

class DataCommand {
public:
  String *_data;

  DataCommand(String *data) { _data = data; }

  int pack(unsigned char *buf) {
    Serializer s;
    buf[0] = DATA;
    return 1 + s.pack(_data, buf + 1);
  }
}

class GetRowCommand {
public:
  int _row_idx;

  GetRowCommand(int row_idx) { _row_idx = row_idx; }

  int pack(unsigned char *buf) {
    Serializer s;
    buf[0] = GET;
    return 1 + s.pack(_row_idx, buf + 1);
  }
}

class SendRowCommand {
public:
  Row *_row;

  SendRowCommand(Row *row) { _row = row; }

  int pack(unsigned char *buf) {
    Serializer s;
    buf[0] = SEND;
    return 1 + pack(_row, buf + 1);
  }
}
