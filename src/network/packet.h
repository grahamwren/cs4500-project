#pragma once

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
  PacketHeader hdr;        // 20 bytes
  sized_ptr<uint8_t> data; // owned

  /* explicity delete copy and move constructors to ensure copy-elision */
  Packet(Packet &&) = delete;
  Packet(const Packet &) = delete;

  // build on receive
  Packet(PacketHeader const &h) : hdr(h), data(0, nullptr) {}
  Packet(PacketHeader const &h, uint8_t *src_data)
      : hdr(h), data(hdr.data_len(), new uint8_t[hdr.data_len()]) {
    memcpy(data.ptr, src_data, hdr.data_len());
  }

  // build to send
  Packet(IpV4Addr src, IpV4Addr dst, PacketType type, int data_len,
         uint8_t *input)
      : hdr(src, dst, data_len + sizeof(PacketHeader), type),
        data(data_len, new uint8_t[hdr.data_len()]) {
    memcpy(data.ptr, input, hdr.data_len());
  }
  Packet(IpV4Addr src, IpV4Addr dst, PacketType type, const char *input)
      : Packet(src, dst, type, strlen(input) + 1, (uint8_t *)input) {}
  Packet(IpV4Addr src, IpV4Addr dst, PacketType type)
      : Packet(src, dst, type, 0, nullptr) {}

  ~Packet() {
    delete[] data.ptr;
    data.len = 0;
    data.ptr = nullptr;
  }

  /**
   * write the bytes representation of this Packet into the given buffer
   */
  void pack(uint8_t *buf) const {
    hdr.pack(buf);
    memcpy(buf + sizeof(PacketHeader), data.ptr, data.len);
  }
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
  output << "Packet<" << (void *)&pkt << ">(header: " << pkt.hdr << ", data: [";

  char bytes_s[6];
  if (pkt.hdr.data_len() > 0) {
    sprintf(bytes_s, "0x%x", pkt.data[0]);
    output << bytes_s;
  }

  for (int i = 1; i < pkt.hdr.data_len(); i++) {
    sprintf(bytes_s, ",0x%x", pkt.data[i]);
    output << bytes_s;
  }
  output << "])";
  return output;
}
