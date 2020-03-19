#pragma once


/**
 * Stub class to represent Application data running on top of the network
 * authors: @grahamwren @jagen31
 */
class Command {
public:
  int len;
  unsigned char *data; // owned
  Command(int len, unsigned char *bytes) {
    data = new unsigned char[len];
    memcpy(data, bytes, len);
  }
  ~Command() { delete[] data; }
};
