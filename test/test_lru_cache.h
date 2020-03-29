#pragma once

#include "lru_cache.h"
#include <cstring>
#include <string>

using namespace std;

static constexpr int CSIZ = 128;

TEST(TestLRUCache, test_fetch) {
  LRUCache<int, string, CSIZ> scache;
  int alloc_count = 0;
  int last_alloc_count = alloc_count; // 0
  auto i_to_s = [&](const int k) {
    alloc_count++; // captured
    char buf[10];
    sprintf(buf, "%d", k);
    return unique_ptr<string>(new string(buf));
  };

  char buf[BUFSIZ];
  /* fill cache up to cache_size */
  for (int i = 0; i < CSIZ; i++) {
    auto s_ptr = scache.fetch(i, i_to_s).lock();
    sprintf(buf, "%d", i);
    EXPECT_STREQ(s_ptr->c_str(), buf);
  }
  /* expect as many allocations as are in cache */
  EXPECT_EQ(alloc_count, last_alloc_count + CSIZ);
  last_alloc_count = alloc_count;

  /* cache order now: [ CSIZ, CSIZ - 1, ... , 0 ] */

  /* ensure alloc_count does not change for same keys within cache_size */
  for (int i = 0; i < CSIZ; i++) {
    auto s_ptr = scache.fetch(i, i_to_s).lock();
    sprintf(buf, "%d", i);
    EXPECT_STREQ(s_ptr->c_str(), buf);
  }
  /* expect alloc_count to still be the same */
  EXPECT_EQ(alloc_count, last_alloc_count);

  /* cache order unchanged */

  /* ensure alloc_count increases when past cache_size */
  for (int i = CSIZ; i < CSIZ + 2; i++) {
    auto s_ptr = scache.fetch(i, i_to_s).lock();
    sprintf(buf, "%d", i);
    EXPECT_STREQ(s_ptr->c_str(), buf);
  }
  /* expect an extra two allocations */
  EXPECT_EQ(alloc_count, last_alloc_count + 2);
  last_alloc_count = alloc_count;

  /* cache order now: [ CSIZ + 1, CSIZ, ... , 2 ] */

  /* ensure first two elements were evicted from cache */
  for (int i = 0; i < 2; i++) {
    auto s_ptr = scache.fetch(i, i_to_s).lock();
    sprintf(buf, "%d", i);
    EXPECT_STREQ(s_ptr->c_str(), buf);
  }
  /* expect two more allocations to account for evicted elements */
  EXPECT_EQ(alloc_count, last_alloc_count + 2);
  last_alloc_count = alloc_count;

  /* cache order now: [ 2, 1, CSIZ + 1, CSIZ, ... , 4 ] */

  /* promote two earlier elements to front of cache */
  for (int i = CSIZ; i < CSIZ + 2; i++) {
    auto s_ptr = scache.fetch(i, i_to_s).lock();
    sprintf(buf, "%d", i);
    EXPECT_STREQ(s_ptr->c_str(), buf);
  }
  /* expect alloc_count unchanged */
  EXPECT_EQ(alloc_count, last_alloc_count);

  /* cache order now: [ CSIZ + 1, CSIZ, 2, 1,  ... , 4 ] */

  const int NS = CSIZ * 2; // new start, guaranteed greater than CSIZ
  /* fill cache with new values except last two */
  for (int i = 0; i < CSIZ - 2; i++) {
    auto s_ptr = scache.fetch(NS + i, i_to_s).lock();
    sprintf(buf, "%d", NS + i);
    EXPECT_STREQ(s_ptr->c_str(), buf);
  }
  /* expect CSIZ - 2 more allocations */
  EXPECT_EQ(alloc_count, last_alloc_count + (CSIZ - 2));
  last_alloc_count = alloc_count;

  /* cache order now: [ NS + (CSIZ - 2), ... , CSIZ + 1, CSIZ ] */

  /* request last two values which should still be in the cache */
  for (int i = CSIZ; i < CSIZ + 2; i++) {
    auto s_ptr = scache.fetch(i, i_to_s).lock();
    sprintf(buf, "%d", i);
    EXPECT_STREQ(s_ptr->c_str(), buf);
  }
  /* expect alloc_count to be unchanged */
  EXPECT_EQ(alloc_count, last_alloc_count);
}
