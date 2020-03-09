#include <gtest/gtest.h>

#include "test_block_list.h"
#include "test_bytes_writer.h"
#include "test_bytes_reader.h"
#include "test_dataframe.h"
#include "test_list.h"
#include "test_queue.h"

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
