#pragma once

#include <iostream>

/**
 * POD class to represent IP addresses
 * authors: @grahamwren @jagen31
 */
class IpV4Addr {
public:
  unsigned short part1 : 8;
  unsigned short part2 : 8;
  unsigned short part3 : 8;
  unsigned short part4 : 8;

  IpV4Addr(unsigned short p1, unsigned short p2, unsigned short p3,
           unsigned short p4)
      : part1(p1), part2(p2), part3(p3), part4(p4) {}

  IpV4Addr(const char *addr) {
    unsigned short p1;
    unsigned short p2;
    unsigned short p3;
    unsigned short p4;
    // assert matches 4 segments
    assert(sscanf(addr, "%hu.%hu.%hu.%hu", &p1, &p2, &p3, &p4) == 4);
    part1 = p1;
    part2 = p2;
    part3 = p3;
    part4 = p4;
  }

  /**
   * convert from an integer representation of this ip
   */
  IpV4Addr(int int_ip) {
    part1 = (int_ip & 0xff000000) >> 12;
    part2 = (int_ip & 0x00ff0000) >> 8;
    part3 = (int_ip & 0x0000ff00) >> 4;
    part4 = (int_ip & 0x000000ff);
  }

  /**
   * default constructor, value 0
   */
  IpV4Addr() : IpV4Addr(0) {}

  /**
   * writes the char* representation of this address into the given buffer,
   * buf must be at least 17 bytes long, otherwise undefined
   */
  void c_str(char *buf) const {
    sprintf(buf, "%hu.%hu.%hu.%hu", part1, part2, part3, part4);
  }

  /**
   * convert to the integer representation of this ip
   *
   * TODO: this should match implementation of inet_aton
   */
  operator int() const {
    int sum = 0;
    sum += part1 << 12;
    sum += part2 << 8;
    sum += part3 << 4;
    sum += part4;
    return sum;
  };

  void print(bool print_meta = true) const {
    if (print_meta)
      cout << "IPV4<";
    cout << part1 << "." << part2 << "." << part3 << "." << part4;
    if (print_meta)
      cout << ">";
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

  PacketHeader(PacketHeader &h)
      : PacketHeader(h.src_addr, h.dst_addr, h.pkt_len, h.type) {}
  PacketHeader(IpV4Addr src, IpV4Addr dst, int packet_len, PacketType t)
      : pkt_len(packet_len), src_addr(src), dst_addr(dst), type(t) {
    /* packet must be at least header sized */
    assert(packet_len >= sizeof(PacketHeader));
    hdr_checksum = this->compute_checksum();
  }

  /**
   * reinterprets the passed in input into a PacketHeader
   * does NOT allocate
   */
  static PacketHeader *parse(unsigned char *input) {
    PacketHeader *pkt_hdr = (PacketHeader *)input;
    return pkt_hdr->valid_checksum() ? pkt_hdr : nullptr;
  }

  /**
   * pack this header into the buffer
   */
  void pack(unsigned char *buf) const {
    memcpy(buf, this, sizeof(PacketHeader));
  }

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

  void print() const {
    cout << "PacketHeader<" << (void *)this << ">(pkt_len: " << pkt_len
         << ", hdr_checksum: " << hdr_checksum << ", src: ";
    src_addr.print();
    cout << ", dst: ";
    dst_addr.print();
    cout << ", type: ";
    PacketHeader::print_type(type);
    cout << ")";
  }

  static void print_type(const PacketType &type) {
    switch (type) {
    case PacketType::REGISTER:
      cout << "REGISTER";
      break;
    case PacketType::NEW_PEER:
      cout << "NEW_PEER";
      break;
    case PacketType::HELLO:
      cout << "HELLO";
      break;
    case PacketType::OK:
      cout << "OK";
      break;
    case PacketType::ERR:
      cout << "ERR";
      break;
    case PacketType::DATA:
      cout << "DATA";
      break;
    case PacketType::SHUTDOWN:
      cout << "SHUTDOWN";
      break;
    default:
      cout << "<PacketType unknown>";
    }
  }
};

static_assert(sizeof(PacketHeader) == 20, "PacketHeader was incorrect size");

/**
 * Packet: represents a packet which is received over a socket or which can be
 * sent over a socket. Contains a header and some byte data.
 * authors: @grahamwren @jagen31
 */
class Packet {
public:
  PacketHeader hdr;              // 20 bytes
  unsigned char *data = nullptr; // owned

  // build on receive
  Packet(PacketHeader &h) : hdr(h) { data = new unsigned char[hdr.data_len()]; }

  // build to send
  Packet(IpV4Addr src, IpV4Addr dst, PacketType type, int data_len,
         unsigned char *input)
      : hdr(src, dst, data_len + sizeof(PacketHeader), type) {
    data = new unsigned char[hdr.data_len()];
    memcpy(data, input, hdr.data_len());
  }
  Packet(IpV4Addr src, IpV4Addr dst, PacketType type, const char *input)
      : Packet(src, dst, type, strlen(input) + 1, (unsigned char *)input) {}
  Packet(IpV4Addr src, IpV4Addr dst, PacketType type)
      : Packet(src, dst, type, 0, nullptr) {}

  ~Packet() {
    delete[] data;
    data = nullptr;
  }

  /**
   * write the bytes representation of this Packet into the given buffer
   */
  void pack(unsigned char *buf) const {
    hdr.pack(buf);
    memcpy(buf + sizeof(PacketHeader), data, hdr.data_len());
  }

  void print() const {
    cout << "Packet<" << (void *)this << ">(header: ";
    hdr.print();
    cout << ", data: [";
    for (int i = 0; i < hdr.data_len(); i++) {
      printf("0x%x,", data[i]);
    }
    cout << "])";
  };
};
