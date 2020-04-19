#pragma once

#include "sdk/cluster.h"

/**
 * base class for all applications to interact with an EAU2 cluster, has one
 * cluster SDK to interact with cluster.
 *
 * authors: @grahamwren, @jagen31
 */
class Application {
protected:
  Cluster cluster;

public:
  Application(const IpV4Addr &ip) : cluster(ip) {}
};
