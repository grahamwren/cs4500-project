#pragma once

#include <algorithm>
#include <list>
#include <memory>
#include <unordered_map>
#include <functional>

using namespace std;

template <typename K, typename V, int cache_size = 32> class LRUCache {
  static_assert(cache_size > 0, "Cache must be at least one element");

public:
  typedef K key_t;
  typedef V value_t;
  typedef function<unique_ptr<value_t>(const key_t &)> fetch_fun_t;

  /**
   * fetch value via the cache. Returns a weak_ptr because the cache could evict
   * and delete the value at any time.
   *
   * if key already in the cache
   * - fn is not executed, weak_ptr to the value is returned out of the cache,
   *   key is promoted to first in the cache
   *
   * else
   * - fn is executed to retrieve the required value, resultant value is stored
   *   in the cache, key is set as the first in the cache, and a weak_ptr
   *   to it is returned
   */
  weak_ptr<value_t> fetch(const key_t &k, fetch_fun_t fn) {
    if (has(k))
      return get_from_cache(k);
    else {
      return add_to_cache(k, fn(k));
    }
  }

private:
  mutable list<reference_wrapper<const key_t>> keys;
  unordered_map<key_t, shared_ptr<value_t>> cache;

  /**
   * returns whether the cache contains the requested key
   */
  bool has(const key_t &k) const { return cache.find(k) != cache.end(); }

  /**
   * get a weak_ptr to a value stored in the cache, assumes key is in cache
   */
  weak_ptr<value_t> get_from_cache(const key_t &key) {
    auto it = find_if(keys.begin(), keys.end(),
                      [&, key](auto k) { return k.get() == key; });
    assert(it != keys.end()); // assert found value

    if (it != keys.begin()) {
      // move key to front of list
      const key_t &move_key = it->get();
      keys.erase(it);
      keys.push_front(move_key);
    }
    return weak_ptr<value_t>(cache.at(key));
  }

  /**
   * adds the passed value to the cache as a shared_ptr and returns a weak_ptr
   * to that object
   */
  weak_ptr<value_t> add_to_cache(const key_t &key, shared_ptr<value_t> val) {
    assert(!has(key));                   // assert does not yet have key
    assert(keys.size() == cache.size()); // assert sizes are in sync

    if (keys.size() >= cache_size) {
      /* evict */
      const key_t &key_to_evict = keys.back();
      keys.pop_back();
      cache.erase(key_to_evict);
    }

    const auto &[it, res] = cache.emplace(key, val); // copies key
    assert(res);                                     // assert was created

    keys.push_front(it->first); // push reference to key created in cache
    return weak_ptr<value_t>(it->second);
  }
};
