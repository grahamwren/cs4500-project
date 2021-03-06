#include <gtest/gtest.h>

#include "test_cli_flags.h"
#include "test_column.h"
#include "test_command.h"
#include "test_cursor.h"
#include "test_data.h"
#include "test_dataframe.h"
#include "test_dataframe_chunk.h"
#include "test_kv_store.h"
#include "test_network.h"
#include "test_packet.h"
#include "test_parser.h"
#include "test_partial_dataframe.h"
#include "test_row.h"
#include "test_schema.h"

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
