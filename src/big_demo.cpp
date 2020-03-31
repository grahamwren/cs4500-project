#include "application.h"
#include <chrono>
#include <iostream>

static int NE = 100;

void rand_string(string &dest) {
  /* use strings around 1MB */
  int len = 1024 * 1024;
  char c;
  for (int i = 0; i < len; i++) {
    c = (char)((rand() % 95) + 32); // ascii chars between 32 and 126
    if (c == '"')                   // don't allow double quotes in strings
      c = '\'';
    dest.append(1, c);
  }
}

class BigDemo : public Application {
public:
  const string data_name;
  vector<string> strs;
  BigDemo(IpV4Addr ip, IpV4Addr sip) : Application(ip, sip), data_name("main") {
    strs.resize(NE);
    for (int i = 0; i < NE; i++) {
      rand_string(strs[i]);
    }
  }

  void run() {
    int bytes_to_write = 0;
    vector<DataChunk> data_to_send;
    data_to_send.reserve(NE);
    for (int i = 0; i < NE; i++) {
      WriteCursor wc;
      pack<const string &>(wc, strs[i]);
      data_to_send.emplace_back(wc.length(), wc.bytes());
      bytes_to_write += wc.length();
    }
    cout << "writing: " << bytes_to_write << endl;

    /* write elements into cluster */
    auto w_start = chrono::high_resolution_clock::now();
    int i = 0;
    for (const DataChunk &dc : data_to_send) {
      ChunkKey k(data_name, i++);
      kv.put(k, dc);
    }
    auto w_end = chrono::high_resolution_clock::now();
    chrono::duration<double, milli> w_diff = w_end - w_start;
    cout << "write time: " << w_diff.count() << " ms, " << NE << " elements"
         << endl;

    vector<shared_ptr<DataChunk>> results;
    results.reserve(NE);
    auto r_start = chrono::high_resolution_clock::now();
    for (int i = 0; i < NE; i++) {
      ChunkKey key(data_name, i);
      results.emplace_back(kv.get(key).lock());

      /* backtrack 10 elements to test cache */
      for (int j = i; j > i - 10 && j > 0; j--) {
        ChunkKey b_key(data_name, j);
        kv.get(b_key);
      }
    }
    auto r_end = chrono::high_resolution_clock::now();
    chrono::duration<double, milli> r_diff = r_end - r_start;
    cout << "read time: " << r_diff.count() << " ms, " << NE
         << " elements, each 10 times" << endl;

    for (int i = 0; i < NE; i++) {
      shared_ptr<DataChunk> data = results[i];
      ReadCursor rc(data->data());
      string s = yield<string>(rc);
      if (s.compare(strs[i]) != 0) {
        cout << "Failed on: " << strs[i].c_str() << endl;
        exit(-1);
      }
    }
    cout << "Full data match, DONE" << endl;
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

IpV4Addr get_server_ip(int argc, char **argv) {
  assert(argc > 4);
  if (strcmp(argv[1], "--server-ip") == 0) {
    return IpV4Addr(argv[2]);
  } else if (strcmp(argv[3], "--server-ip") == 0) {
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

  BigDemo d(get_ip(argc, argv), get_server_ip(argc, argv));
  d.run();
}
