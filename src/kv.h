#pragma once
#include "key.h"
#include "bytes_reader.h"
#include "bytes_writer.h"

class KV {
public:

  BytesReader* get(Key key);

  void put(Key key, BytesWriter writer);
};

