#include "network/node.h"
#include <thread>

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

  IpV4Addr my_a = get_ip(argc, argv);
  Node n(my_a);
  n.start();

  IpV4Addr server_a(0);
  if (argc > 3) {
    server_a = get_server_ip(argc, argv);
    n.register_with(server_a);
  }
  n.set_data_handler([](ReadCursor &rc) {
    int len = rc.length();
    char buf[len + 1];
    for (int i = 0; i < len; i++) {
      buf[i] = yield<char>(rc);
    }
    buf[len] = '\0';
    cout << "Handler got: " << buf << endl;
    return true;
  });

  std::this_thread::sleep_for(100ms);

  auto cluster = n.cluster();
  for (auto peer : cluster) {
    n.send_data(peer, sized_ptr(11, (uint8_t *)"Hello world"));
  }

  if (argc > 5 && strcmp(argv[5], "--shutdown") == 0) {
    cout << "starting shutdown" << endl;
    n.teardown();
  }

  n.join();
  cout << "DONE" << endl;
}
