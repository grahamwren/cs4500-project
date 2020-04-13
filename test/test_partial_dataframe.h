#pragma once

#include "kv/partial_dataframe.h"
#include "sample_rowers.h"

TEST(TestPartialDataFrame, test_add_df_chunk__get) {
  Schema schema("IFB");
  PartialDataFrame pdf(schema);

  DataFrameChunk dfc(schema);
  Row row(schema);
  for (int i = 0; i < DF_CHUNK_SIZE; i++) {
    row.set(0, i);
    row.set(1, i * 0.5f);
    row.set(2, i % 5 == 0);
    dfc.add_row(row);
  }

  WriteCursor wc(12 * 1000);
  dfc.serialize(wc);
  ReadCursor rc = wc;
  pdf.add_df_chunk(0, rc);

  DataFrameChunk dfc2(schema);
  for (int i = DF_CHUNK_SIZE; i < DF_CHUNK_SIZE * 1.5; i++) {
    row.set(0, i);
    row.set(1, i * 0.5f);
    row.set(2, i % 5 == 0);
    dfc2.add_row(row);
  }

  wc.reset();
  dfc2.serialize(wc);
  ReadCursor rc2 = wc;
  pdf.add_df_chunk(1, rc2);

  for (int i = 0; i < DF_CHUNK_SIZE * 1.5; i++) {
    EXPECT_EQ(pdf.get_int(i, 0), i);
    EXPECT_EQ(pdf.get_float(i, 1), i * 0.5f);
    EXPECT_EQ(pdf.get_bool(i, 2), i % 5 == 0);
  }
}

TEST(TestPartialDataFrame, test_add_df_chunk__map) {
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
  ReadCursor rc = wc;
  pdf.add_df_chunk(0, rc);

  for (int i = 0; i < 1000; i++) {
    EXPECT_EQ(pdf.get_int(i, 0), i);
    EXPECT_EQ(pdf.get_float(i, 1), i * 0.5f);
    EXPECT_EQ(pdf.get_bool(i, 2), i % 5 == 0);
  }

  CountDiv2 rower(0);
  pdf.map(rower);
  EXPECT_EQ(rower.get_count(), 500);
}

TEST(TestPartialDataFrame, test_map_non_contiguous) {
  Schema schema("IFB");
  PartialDataFrame pdf(schema);

  Row row(schema);
  /* add two chunks worth of rows */
  for (int ci = 0; ci < 2; ci++) {
    DataFrameChunk dfc(schema);
    for (int i = 0; i < DF_CHUNK_SIZE; i++) {
      row.set(0, i);
      row.set(1, i * 0.99999999999f);
      row.set(2, i % 5 == 0);
      dfc.add_row(row);
    }
    WriteCursor wc(9 * DF_CHUNK_SIZE);
    dfc.serialize(wc);
    ReadCursor rc = wc;
    pdf.add_df_chunk(ci, rc);
  }

  /* skip chunk: 3 */

  DataFrameChunk dfc(schema);
  for (int i = DF_CHUNK_SIZE * 4; i < DF_CHUNK_SIZE * 5; i++) {
    row.set(0, i);          // 4 bytes
    row.set(1, i * 0.5f);   // 4 bytes
    row.set(2, i % 5 == 0); // 1 bytes
    dfc.add_row(row);       // 9 bytes * DF_CHUNK_SIZE
  }

  WriteCursor wc(9 * DF_CHUNK_SIZE);
  dfc.serialize(wc);
  ReadCursor rc = wc;
  pdf.add_df_chunk(4, rc);

  for (int i = DF_CHUNK_SIZE * 4; i < DF_CHUNK_SIZE * 5; i++) {
    EXPECT_EQ(pdf.get_int(i, 0), i);
    EXPECT_EQ(pdf.get_float(i, 1), i * 0.5f);
    EXPECT_EQ(pdf.get_bool(i, 2), i % 5 == 0);
  }

  MinMaxInt rower(0);
  pdf.map(rower);
  EXPECT_EQ(rower.get_min(), 0);
  EXPECT_EQ(rower.get_max(), DF_CHUNK_SIZE * 5 - 1);
}
