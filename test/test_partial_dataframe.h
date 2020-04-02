#pragma once

#include "partial_dataframe.h"
#include "sample_rowers.h"

TEST(TestPartialDataFrame, test_add_row__get) {
  Schema schema("IFB");
  PartialDataFrame pdf(schema);

  Row row(schema);
  for (int i = 0; i < 6 * 1000; i++) {
    row.set(0, i);
    row.set(1, i * 0.5f);
    row.set(2, i % 5 == 0);
    pdf.add_row(row);
  }

  for (int i = 0; i < 6 * 1000; i++) {
    EXPECT_EQ(pdf.get_int(i, 0), i);
    EXPECT_EQ(pdf.get_float(i, 1), i * 0.5f);
    EXPECT_EQ(pdf.get_bool(i, 2), i % 5 == 0);
  }
}

TEST(TestPartialDataFrame, test_add_row__map) {
  Schema schema("IFB");
  PartialDataFrame pdf(schema);

  Row row(schema);
  for (int i = 0; i < 10 * 1000; i++) {
    row.set(0, i);
    row.set(1, i * 0.99999999999f);
    row.set(2, i % 5 == 0);
    pdf.add_row(row);
  }

  CountDiv2 rower(0);
  pdf.map(rower);
  EXPECT_EQ(rower.count, 5 * 1000);
}

TEST(TestPartialDataFrame, test_add_df_chunk) {
  Schema schema("IFB");
  PartialDataFrame pdf(schema);

  DataFrameChunk dfc(schema);
  Row row(schema);
  for (int i = 0; i < 1000; i++) {
    row.set(0, i);          // 4 bytes
    row.set(1, i * 0.5f);   // 4 bytes
    row.set(2, i % 5 == 0); // 4 bytes
    dfc.add_row(row);       // 12 bytes * 1000
  }

  WriteCursor wc(12 * 1000);
  dfc.serialize(wc);
  ReadCursor rc(wc.length(), wc.bytes());
  pdf.add_df_chunk(rc);

  for (int i = 0; i < 1000; i++) {
    EXPECT_EQ(pdf.get_int(i, 0), i);
    EXPECT_EQ(pdf.get_float(i, 1), i * 0.5f);
    EXPECT_EQ(pdf.get_bool(i, 2), i % 5 == 0);
  }
}
