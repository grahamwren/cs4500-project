#include "network/node.h"

int main(int argc, char **argv) {
  cout << "started: ";
  for (int i = 0; i < argc; i++)
    cout << argv[i] << " ";
  cout << endl;

  assert(argc == 3 || argc == 5);
  if (argc == 3 && strcmp(argv[1], "--ip") == 0) {
    IpV4Addr my_a(argv[2]);
    Node n(my_a);
    n.run();
  } else {
    IpV4Addr my_a;
    IpV4Addr server_a;
    if (strcmp(argv[1], "--ip") == 0 && strcmp(argv[3], "--server-ip") == 0) {
      my_a = IpV4Addr(argv[2]);
      server_a = IpV4Addr(argv[4]);
    } else if (strcmp(argv[1], "--server-ip") == 0 &&
               strcmp(argv[3], "--ip") == 0) {
      server_a = IpV4Addr(argv[2]);
      my_a = IpV4Addr(argv[4]);
    }

    Node n(my_a, server_a);
    n.run();
  }
}
