#include "sdk/cluster.h"
#include <iostream>
#include <set>

using namespace std;

#ifndef NUM_ELEMENTS
#define NUM_ELEMENTS (1000 * 1000)
#endif

class Demo {
public:
  Key data_key;
  Cluster cluster;

  Demo(const IpV4Addr &ip) : data_key(string("main")), cluster(ip) {}

  ~Demo() { cluster.shutdown(); }

  void fill_cluster() {
    /* create DF */
    cluster.create(data_key, Schema("I"));
    const DFInfo &df_info = cluster.get_df_info(data_key);

    /* add rows via PDF */
    PartialDataFrame pdf(df_info.schema);
    Row row(df_info.schema);
    for (int i = 0; i < NUM_ELEMENTS; i++) {
      row.set(0, i);
      pdf.add_row(row);
    }

    /* add chunks to cluster */
    for (int ci = 0; ci < pdf.nchunks(); ci++) {
      const DataFrameChunk &dfc = pdf.get_chunk_by_chunk_idx(ci);
      cluster.put(data_key, dfc);
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
        ((NUM_ELEMENTS * (uint64_t)(NUM_ELEMENTS + 1)) / 2) - NUM_ELEMENTS;
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
