#pragma once

#include "packet.h"
#include <arpa/inet.h>
#include <iostream>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>

using namespace std;

#ifndef PROTO_PORT
#define PROTO_PORT 1080
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
  struct addrinfo *get_addr() {
    struct addrinfo *address_info = new addrinfo;
    memset(address_info, 0, sizeof(struct addrinfo)); // 0 memory
    address_info->ai_family = PF_UNSPEC;

    struct addrinfo hints;
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC;     /* Allow IPv4 or IPv6 */
    hints.ai_socktype = SOCK_STREAM; /* Stream socket */
    hints.ai_flags = AI_PASSIVE;     /* For wildcard IP address */
    hints.ai_protocol = 0;           /* Any protocol */
    hints.ai_canonname = NULL;
    hints.ai_addr = NULL;
    hints.ai_next = NULL;
    /* not sure why need to do this */
    char a_str[16];
    addr.c_str(a_str);
    int res = getaddrinfo(a_str, NULL, &hints, &address_info);
    if (res != 0) {
      cout << gai_strerror(res) << endl;
      exit(-1);
    }

    return address_info;
  }
  int close() { return ::close(sock_fd); }
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
    cout << "Connecting to ";
    addr.print();
    cout << endl;

    /* create socket */
    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    assert(sock_fd != -1);

    /* create and setup sockaddr */
    struct addrinfo *address_info = get_addr();

    /* connect socket, use assert to handle error case */
    int cres =
        ::connect(sock_fd, address_info->ai_addr, address_info->ai_addrlen);
    if (cres < 0) {
      cout << "Error connecting: " << (int)errno << endl;
    }
    freeaddrinfo(address_info);
    return cres;
  }

  /**
   * send a Packet over this DataSock
   */
  int send_pkt(Packet &pkt) const {
    cout << "Sending pkt: ";
    pkt.print();
    cout << endl;

    unsigned char buffer[pkt.hdr.pkt_len];
    pkt.pack(buffer);
    size_t sres = send(sock_fd, buffer, pkt.hdr.pkt_len, 0);
    if (sres == -1) {
      cout << "ERROR: failed to send pkt, error: " << (int)errno << endl;
    }
    return sres;
  };

  /**
   * try to receive a Packet over this DataSock,
   * blocks until packet is available.
   * caller owns the returned Packet
   */
  Packet *get_pkt() const {
    unsigned char hdr_buf[sizeof(PacketHeader)];
    int rres = recv(sock_fd, hdr_buf, sizeof(PacketHeader), MSG_WAITALL);
    assert(rres != -1);
    PacketHeader *hdr = PacketHeader::parse(hdr_buf);

    /* if checksum was invalid, return nullptr */
    if (hdr == nullptr)
      return nullptr;

    Packet *pkt = new Packet(*hdr);

    if (hdr->data_len() > 0) {
      /* read data_len from socket, blocks until data_len has been read */
      rres = recv(sock_fd, pkt->data, hdr->data_len(), MSG_WAITALL);
      assert(rres != -1);
    }

    cout << "Received pkt: ";
    pkt->print();
    cout << endl;

    return pkt;
  };

  static Packet *fetch(Packet &pkt) {
    DataSock ds(pkt.hdr.dst_addr);
    ds.connect();
    ds.send_pkt(pkt);
    Packet *resp = ds.get_pkt();
    ds.close();
    return resp;
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
    /* create and setup sockaddr */
    struct addrinfo *info;
    /* try each addr until bind succeeds */
    for (info = get_addr(); info != nullptr; info = info->ai_next) {
      /* create socket */
      sock_fd = socket(info->ai_family, info->ai_socktype, info->ai_protocol);
      if (sock_fd == -1)
        continue;
      // cout << "trying to bind: "
      //      << inet_ntoa(((struct sockaddr_in *)(info->ai_addr))->sin_addr)
      //      << endl;
      if (::bind(sock_fd, info->ai_addr, info->ai_addrlen) == 0)
        break; // success

      ::close(sock_fd);
    }
    freeaddrinfo(info);

    if (info == nullptr) {
      cout << "Failed to bind" << endl;
      exit(-1);
    } else {
      cout << "bound" << endl;
    }

    /* start listening with connection queue of 50 */
    int lres = ::listen(sock_fd, 50);

    if (lres < 0) {
      cout << "Failed to start listening on: ";
      addr.print();
      cout << " " << errno;
      cout << endl;
    } else {
      cout << "Listening on: ";
      addr.print();
      cout << endl;
    }

    return lres;
  }

  /**
   * looks for new connection requests
   * blocks until one is available
   */
  DataSock accept_connection() const {
    int data_sock = accept(sock_fd, 0, 0);
    assert(data_sock != -1);

    DataSock sock(data_sock, addr);

    cout << "Accepted connection on: ";
    addr.print();
    cout << endl;

    return sock; // move
  }
};
