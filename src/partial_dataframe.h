#pragma once

#include "dataframe_chunk.h"

class PartialDataFrame : public DataFrame {
protected:
  const Schema schema;
  vector<DataFrameChunk> chunks;

public:
  PartialDataFrame(const Schema &scm) : schema(scm), chunks(1, schema) {}
};
