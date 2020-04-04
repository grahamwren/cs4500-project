#include "application.h"
#include "sdk/cluster.h"
#include <iostream>
#include <set>

using namespace std;

#ifndef NUM_ROWS
#define NUM_ROWS (1000 * 1000)
#endif

class Demo : public Application {
public:
  Key data_key;

  Demo(const IpV4Addr &ip) : Application(ip), data_key(string("main")) {}

  void fill_cluster() {
    /* create DF */
    cluster.create(data_key, Schema("ISFB"));
    const DFInfo &df_info = cluster.get_df_info(data_key)->get();

    /* add rows via DFC */
    Row row(df_info.get_schema());
    // future<bool> last_put;
    int chunk_i = 0;
    int row_i = 0;
    while (row_i < NUM_ROWS) {
      DataFrameChunk dfc(df_info.get_schema(), row_i);
      while (!dfc.is_full() && row_i < NUM_ROWS) {
        row.set(0, row_i);
        row.set(1, new string("apples"));
        row.set(2, row_i * 0.5f);
        row.set(3, row_i % 13 == 0);
        dfc.add_row(row);
        row_i++;
      }
      cluster.put(data_key, chunk_i, dfc);
      chunk_i++;
    }
  }

  void read_from_cluster() {
    for (int ci = 0; true; ci++) {
      optional<DataFrameChunk> dfc = cluster.get(data_key, ci);
      if (dfc) {
        for (int i = 0; i < dfc->nrows(); i++) {
          int row_i = dfc->get_start() + i;
          if (dfc->get_int(row_i, 0) != row_i) {
            cout << "Chunk(" << ci << ") failed" << endl;
            cout << "expected: " << row_i << " was: " << dfc->get_int(row_i, 0)
                 << endl;
            exit(-1);
          }
        }
      } else
        break;
    }
    cout << "Full data match, DONE" << endl;
  }

  void map_cluster() {
    shared_ptr<SumRower> rower = make_shared<SumRower>(0);
    cluster.map(data_key, rower);
    uint64_t expected_results =
        ((NUM_ROWS * (uint64_t)(NUM_ROWS + 1)) / 2) - NUM_ROWS;
    if (rower->get_sum_result() != expected_results)
      cout << "SumRower failed: expected: " << expected_results
           << ", actual: " << rower->get_sum_result() << endl;
  }

  void run() {
    fill_cluster();
    read_from_cluster();
    map_cluster();
  }
};

IpV4Addr get_ip(int argc, char **argv) {
  assert(argc >= 3);

  if (strcmp(argv[1], "--ip") == 0) {
    return IpV4Addr(argv[2]);
  } else if (argc > 4 && strcmp(argv[3], "--ip") == 0) {
    return IpV4Addr(argv[4]);
  }
  assert(false);
}

int main(int argc, char **argv) {
  cout << "starting with: ";
  for (int i = 0; i < argc; i++) {
    cout << argv[i] << ' ';
  }
  cout << endl;

  Demo d(get_ip(argc, argv));
  d.run();
}
