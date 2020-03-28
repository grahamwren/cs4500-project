#include "application.h"

static int NE = 1000;

class Demo : public Application {
public:
  const string data_name;
  vector<string> strs;
  Demo(IpV4Addr ip, IpV4Addr sip) : Application(ip, sip), data_name("main") {
    char buf[1024];
    for (int i = 0; i < NE; i++) {
      sprintf(buf, "Hello world %d", i);
      strs.emplace_back(buf);
    }
  }

  void run() {
    for (int i = 0; i < NE; i++) {
      ChunkKey k(data_name, i);
      WriteCursor wc;
      pack<const string &>(wc, strs[i]);
      DataChunk data(wc.length(), wc.bytes());
      kv.put(k, data);
    }
    for (int i = 0; i < NE; i++) {
      ChunkKey k(string("main"), i);
      DataChunk data = kv.get(k);
      ReadCursor rc(data);
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

  Demo d(get_ip(argc, argv), get_server_ip(argc, argv));
  d.run();
}
