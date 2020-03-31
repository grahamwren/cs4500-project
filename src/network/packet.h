#pragma once

#include "kv/data_chunk.h"
#include <cstring>
#include <iostream>

using namespace std;

/**
 * POD class to represent IP addresses
 * authors: @grahamwren @jagen31
 */
class IpV4Addr {
public:
  uint_fast8_t part1;
  uint_fast8_t part2;
  uint_fast8_t part3;
  uint_fast8_t part4;

  IpV4Addr(uint_fast8_t p1, uint_fast8_t p2, uint_fast8_t p3, uint_fast8_t p4)
      : part1(p1), part2(p2), part3(p3), part4(p4) {}

  IpV4Addr(const char *addr) {
    assert(addr);
    unsigned short p1;
    unsigned short p2;
    unsigned short p3;
    unsigned short p4;
    int n = sscanf(addr, "%hu.%hu.%hu.%hu", &p1, &p2, &p3, &p4);
    assert(n == 4);
    part1 = p1;
    part2 = p2;
    part3 = p3;
    part4 = p4;
  }

  /**
   * default constructor, value 0
   */
  IpV4Addr() : IpV4Addr(0, 0, 0, 0) {}

  IpV4Addr(uint32_t i)
      : IpV4Addr((i & 0xff000000) >> 24, (i & 0x00ff0000) >> 16,
                 (i & 0x0000ff00) >> 8, i & 0x000000ff) {}
  IpV4Addr(int i) : IpV4Addr((uint32_t)i) {}

  operator uint32_t() const {
    return (part1 << 24) | (part2 << 16) | (part3 << 8) | part4;
  }
  bool operator<(const IpV4Addr &other) const {
    return (uint32_t)(*this) < (uint32_t)other;
  }

  bool equals(const IpV4Addr &o) const {
    return part1 == o.part1 && part2 == o.part2 && part3 == o.part3 &&
           part4 == o.part4;
  }
};

static_assert(sizeof(IpV4Addr) == 4, "IpV4Addr was incorrect size");

enum class PacketType { REGISTER, NEW_PEER, HELLO, OK, ERR, DATA, SHUTDOWN };
/**
 * POD class to represent a literal header to be sent or consumed over a socket
 * authors: @grahamwren @jagen31
 */
class PacketHeader {
public:
  const unsigned int pkt_len; // 4 bytes ─┐
  unsigned int hdr_checksum;  // 4 bytes  │
  const IpV4Addr src_addr;    // 4 bytes  │
  const IpV4Addr dst_addr;    // 4 bytes  │
  const PacketType type;      // 4 bytes ─┴─ 20 bytes

  PacketHeader(IpV4Addr src, IpV4Addr dst, int packet_len, PacketType t)
      : pkt_len(packet_len), src_addr(src), dst_addr(dst), type(t) {
    /* packet must be at least header sized */
    assert(packet_len >= sizeof(PacketHeader));
    hdr_checksum = this->compute_checksum();
  }
  PacketHeader(const PacketHeader &h)
      : PacketHeader(h.src_addr, h.dst_addr, h.pkt_len, h.type) {}

  /**
   * reinterprets the passed in input into a PacketHeader
   * does NOT allocate
   */
  static PacketHeader *parse(uint8_t *input) {
    PacketHeader *pkt_hdr = (PacketHeader *)input;
    return pkt_hdr->valid_checksum() ? pkt_hdr : nullptr;
  }

  /**
   * pack this header into the buffer
   */
  void pack(uint8_t *buf) const { memcpy(buf, this, sizeof(PacketHeader)); }

  /**
   * compute the checksum of this header using ip packet checksum algo
   */
  unsigned int compute_checksum() const {
    size_t result = 0;
    const unsigned int *raw_pkt = (const unsigned int *)this;
    for (int i = 0; i < 4; i++) {
      if (i == 1) // skip current checksum
        continue;

      result += raw_pkt[i];
      if (result > 0xffffffff)
        // if grows past max int, subtract max int
        result -= 0xffffffff;
    }
    return ~result; // return 1's complement
  };

  bool valid_checksum() const { return hdr_checksum == compute_checksum(); };
  int data_len() const { return pkt_len - sizeof(PacketHeader); }
};

static_assert(sizeof(PacketHeader) == 20, "PacketHeader was incorrect size");

/**
 * Packet: represents a packet which is received over a socket or which can be
 * sent over a socket. Contains a header and some byte data.
 * authors: @grahamwren @jagen31
 */
class Packet {
public:
  PacketHeader hdr;           // 20 bytes
  unique_ptr<DataChunk> data; // OWNED

  // build on receive
  Packet(const PacketHeader &h) : hdr(h), data(new DataChunk) {}
  Packet(const PacketHeader &h, unique_ptr<uint8_t> &&src_data)
      : hdr(h), data(new DataChunk(hdr.data_len(), move(src_data))) {}

  // build to send
  Packet(IpV4Addr src, IpV4Addr dst, PacketType type)
      : hdr(src, dst, sizeof(PacketHeader), type), data(new DataChunk) {}
  Packet(IpV4Addr src, IpV4Addr dst, PacketType type,
         const sized_ptr<uint8_t> &d)
      : hdr(src, dst, d.len + sizeof(PacketHeader), type),
        data(new DataChunk(d)) {}

  /**
   * write the bytes representation of this Packet into the given buffer
   */
  void pack(uint8_t *buf) const {
    hdr.pack(buf);
    memcpy(buf + sizeof(PacketHeader), data->ptr(), data->len());
  }

  bool ok() const { return hdr.type == PacketType::OK; }
  bool error() const { return hdr.type == PacketType::ERR; }
};

ostream &operator<<(ostream &output, const IpV4Addr &ip) {
  output << (int)ip.part1 << '.' << (int)ip.part2 << '.' << (int)ip.part3 << '.'
         << (int)ip.part4;
  return output;
}

ostream &operator<<(ostream &output, const PacketType type) {
  switch (type) {
  case PacketType::REGISTER:
    output << "REGISTER";
    break;
  case PacketType::NEW_PEER:
    output << "NEW_PEER";
    break;
  case PacketType::HELLO:
    output << "HELLO";
    break;
  case PacketType::OK:
    output << "OK";
    break;
  case PacketType::ERR:
    output << "ERR";
    break;
  case PacketType::DATA:
    output << "DATA";
    break;
  case PacketType::SHUTDOWN:
    output << "SHUTDOWN";
    break;
  default:
    output << "<PacketType unknown>";
  }
  return output;
}

ostream &operator<<(ostream &output, const PacketHeader &hdr) {
  output << "PacketHeader<" << (void *)&hdr << ">(pkt_len: " << hdr.pkt_len
         << ", hdr_checksum: " << hdr.hdr_checksum << ", src: " << hdr.src_addr
         << ", dst: " << hdr.dst_addr << ", type: " << hdr.type << ")";
  return output;
}

ostream &operator<<(ostream &output, const Packet &pkt) {
  output << "Packet<" << (void *)&pkt << ">(header: " << pkt.hdr
         << ", data: " << *(pkt.data) << ")";
  return output;
}
