#pragma once

class DataCache {
public:
  bool has_key(const ChunkKey &k) const { return _key == k; }
  const DataChunk &get(const ChunkKey &k) const {
    assert(k == _key);
    return _data;
  }
  void insert(const ChunkKey &k, DataChunk &&d) {
    _key = k;
    if (_data.len())
      delete _data.ptr();
    _data = move(d);
  }

private:
  ChunkKey _key;
  DataChunk _data;
};
