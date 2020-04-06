#pragma once

#include "sdk/cluster.h"

class Application {
protected:
  Cluster cluster;

public:
  Application(const IpV4Addr &ip) : cluster(ip) {}
};
