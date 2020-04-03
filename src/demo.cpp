#include "kv/command.h"
#include "network/sock.h"
#include <iostream>
#include <set>

using namespace std;

static int NE = 1000 * 1000;

class Demo {
public:
  const Key data_key;
  const Schema scm;
  set<IpV4Addr> cluster;
  Demo(const IpV4Addr &ip) : data_key(string("main")), scm("I") {
    Packet get_cluster(0, ip, PacketType::GET_PEERS);
    Packet resp = DataSock::fetch(get_cluster);
    assert(resp.ok());
    /* assume response is IpV4Addr[] */
    int n_peers = resp.data->len() / sizeof(IpV4Addr);
    IpV4Addr *ips = reinterpret_cast<IpV4Addr *>(resp.data->ptr());
    cout << "Cluster was: [";
    for (int i = 0; i < n_peers; i++) {
      cluster.emplace(ips[i]);
      cout << ips[i] << ",";
    }
    cout << "]" << endl;
  }

  void fill_cluster() {
    PartialDataFrame pdf(scm);
    NewCommand new_df(data_key, pdf.get_schema());
    for (const IpV4Addr &ip : cluster) {
      send_cmd(ip, new_df);
    }
    Row row(pdf.get_schema());
    for (int i = 0; i < NE; i++) {
      row.set(0, i);
      pdf.add_row(row);
    }
    auto ip_it = cluster.begin();
    for (int ci = 0; ci < pdf.nchunks(); ci++) {
      WriteCursor wc;
      pdf.get_chunk_by_chunk_idx(ci).serialize(wc);
      PutCommand cmd(data_key, make_unique<DataChunk>(wc.length(), wc.bytes()));
      send_cmd(*ip_it++, cmd);
      if (ip_it == cluster.end())
        ip_it = cluster.begin();
    }
  }

  void read_from_cluster() {
    auto ip_it = cluster.begin();
    for (int ci = 0; true; ci++) {
      int start_idx = ci * DF_CHUNK_SIZE;
      DataFrameChunk dfc(scm, start_idx);
      GetCommand get_cmd(data_key, ci);
      optional<unique_ptr<DataChunk>> data = send_cmd(*ip_it++, get_cmd);
      if (!data) {
        break;
      }

      ReadCursor rc((*data)->len(), (*data)->ptr());
      dfc.fill(rc);

      for (int ri = 0; ri < dfc.nrows(); ri++) {
        int row_i = start_idx + ri;
        if (dfc.get_int(row_i, 0) != row_i) {
          cout << "Chunk(" << ci << ")"
               << " match ☠️" << endl;
          cout << "expected: " << row_i << ", got: " << dfc.get_int(row_i, 0)
               << endl;
          exit(-1);
        }
      }
      cout << "Chunk(" << ci << ") matches ✅" << endl;
      if (ip_it == cluster.end())
        ip_it = cluster.begin();
    }
  }

  void run() {
    fill_cluster();
    read_from_cluster();
  }

  optional<unique_ptr<DataChunk>> send_cmd(const IpV4Addr &ip,
                                           const Command &cmd) {
    WriteCursor wc;
    cmd.serialize(wc);
    Packet data_pkt(0, ip, PacketType::DATA, wc);
    cout << "DEMO.send(" << ip << ", " << cmd << ")" << endl;
    Packet resp = DataSock::fetch(data_pkt);
    cout << "DEMO.recv(" << resp << ")" << endl;
    if (resp.ok())
      return move(resp.data);
    else
      return nullopt;
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
