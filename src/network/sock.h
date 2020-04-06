#pragma once

#include "packet.h"
#include <arpa/inet.h>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

using namespace std;

#ifndef PROTO_PORT
#define PROTO_PORT 1080
#endif

#ifndef SOCK_LOG
#define SOCK_LOG false
#endif

#ifndef MAX_DATA_SIZE
#define MAX_DATA_SIZE 32768
#endif

/**
 * a class to encapsulate a socket handle
 * authors: @grahamwren @jagen31
 */
class Sock {
protected:
  int sock_fd = -1; // uninitialized value should error
public:
  const IpV4Addr &addr;
  Sock(int socket, const IpV4Addr &a) : sock_fd(socket), addr(a) {}
  Sock(const IpV4Addr &a) : addr(a) {}
  ~Sock() { close(); }
  struct sockaddr_in get_addr(const IpV4Addr &a) const {
    struct sockaddr_in address;
    memset(&address, 0, sizeof(struct sockaddr_in)); // 0 memory

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(addr);
    address.sin_port = htons(PROTO_PORT);
    return address;
  }
  int close() {
    int cres = -1;
    if (sock_fd > 0) {
      cres = ::close(sock_fd);
      sock_fd = -1;
    }
    return cres;
  }
};

/**
 * a class to encapsulate a connection to a TCP stream socket
 * authors: @grahamwren @jagen31
 */
class DataSock : public Sock {
public:
  DataSock(int fd, const IpV4Addr &server_addr) : Sock(fd, server_addr) {}
  DataSock(const IpV4Addr &server_addr) : Sock(server_addr) {}

  int connect() {
    /* create socket */
    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    assert(sock_fd != -1);

    /* create and setup sockaddr */
    struct sockaddr_in address = get_addr(addr);

    if (SOCK_LOG)
      cout << "DataSock.connect(" << addr << ")" << endl;
    /* connect socket */
    int cres = ::connect(sock_fd, (struct sockaddr *)&address, sizeof(address));
    if (cres < 0) {
      cout << "ERROR: failed to connect " << (int)errno << " to " << addr
           << endl;
    }
    return cres;
  }

  /**
   * send a Packet over this DataSock
   */
  int send_pkt(const Packet &pkt) const {
    if (SOCK_LOG)
      cout << "DataSock.send(" << pkt << ")" << endl;

    /* send header first */
    uint8_t buffer[sizeof(PacketHeader)];
    pkt.hdr.pack(buffer);
    size_t sres = send(sock_fd, buffer, sizeof(PacketHeader), 0);
    if (sres == -1)
      return sres; // failed

    int sent_bytes = 0;
    while (sent_bytes < pkt.hdr.data_len()) {
      int send_len = fmin(MAX_DATA_SIZE, pkt.hdr.data_len() - sent_bytes);
      sres = send(sock_fd, pkt.data->ptr() + sent_bytes, send_len, 0);
      sent_bytes += send_len;
      if (sres == -1)
        return sres; // failed
    }

    return sres;
  };

  /**
   * try to receive a Packet over this DataSock,
   * blocks until packet is available.
   *
   * Returns empty optional if checksum invalid
   */
  Packet get_pkt() const {
    uint8_t hdr_buf[sizeof(PacketHeader)];
    int rres = recv(sock_fd, hdr_buf, sizeof(PacketHeader), MSG_WAITALL);
    assert(rres != -1);
    PacketHeader *hdr_ptr = PacketHeader::parse(hdr_buf);
    /* assert checksum was valid, kinda unsafe but whatever */
    assert(hdr_ptr);

    PacketHeader &hdr = *hdr_ptr;

    if (SOCK_LOG)
      cout << "DataSock.recv_hdr(hdr: " << hdr << ")" << endl;

    if (hdr.data_len() > 0) {
      unique_ptr<uint8_t> recv_buf(new uint8_t[hdr.data_len()]);
      int recv_bytes = 0;
      while (recv_bytes < hdr.data_len()) {
        int recv_len = fmin(MAX_DATA_SIZE, hdr.data_len() - recv_bytes);
        rres =
            recv(sock_fd, recv_buf.get() + recv_bytes, recv_len, MSG_WAITALL);
        recv_bytes += recv_len;
        assert(rres != -1);
        if (SOCK_LOG)
          cout << "DataSock.recv_data(" << recv_bytes << " out of "
               << hdr.data_len() << ")" << endl;
      }
      return Packet(hdr, move(recv_buf));
    } else {
      return Packet(hdr);
    }
  };

  static Packet fetch(const Packet &pkt) {
    DataSock ds(pkt.hdr.dst_addr);
    ds.connect();
    ds.send_pkt(pkt);
    return ds.get_pkt();
  }
};

/**
 * a class to encapsulate a TCP stream listen socket
 * authors: @grahamwren @jagen31
 */
class ListenSock : public Sock {
public:
  /**
   * create a socket for listening on at the given address
   */
  ListenSock(const IpV4Addr &a) : Sock(a) {}

  int listen() {
    /* create socket */
    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    assert(sock_fd != -1);

    /* create and setup sockaddr */
    IpV4Addr a(0);
    struct sockaddr_in address = get_addr(a);

    /* bind socket, use assert to handle error case */
    int bres = ::bind(sock_fd, (struct sockaddr *)&address, sizeof(address));
    assert(bres != -1);

    /* start listening with connection queue of 50 */
    int lres = ::listen(sock_fd, 50);

    if (lres < 0) {
      cout << "ERROR: failed to start listening on " << addr << endl;
    }

    return lres;
  }

  /**
   * looks for new connection requests
   * blocks until one is available
   */
  const DataSock accept_connection() const {
    int data_sock_fd = accept(sock_fd, 0, 0);
    if (SOCK_LOG)
      cout << "ListenSock.accept(" << addr << ")" << endl;
    assert(data_sock_fd != -1);
    return DataSock(data_sock_fd, addr);
  }
};
