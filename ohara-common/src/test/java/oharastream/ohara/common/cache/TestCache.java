/*
 * Copyright 2019 is-land
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package oharastream.ohara.common.cache;

import com.google.common.collect.ImmutableBiMap;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.util.CommonUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestCache extends OharaTest {
  @Test(expected = NullPointerException.class)
  public void nullTimeout() {
    Cache.<String, String>builder().timeout(null);
  }

  @Test(expected = NullPointerException.class)
  public void nullFetcher() {
    Cache.<String, String>builder().fetcher(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativeSize() {
    Cache.<String, String>builder().maxSize(-1);
  }

  @Test(expected = NullPointerException.class)
  public void getNull() {
    Cache.<String, String>builder()
        .timeout(Duration.ofSeconds(2))
        .fetcher(key -> CommonUtils.randomString())
        .build()
        .get(null);
  }

  @Test
  public void testBuilder() throws InterruptedException {
    String value = CommonUtils.randomString();
    AtomicInteger count = new AtomicInteger(0);
    Cache<String, String> cache =
        Cache.<String, String>builder()
            .timeout(Duration.ofSeconds(2))
            .fetcher(
                key -> {
                  count.incrementAndGet();
                  return value;
                })
            .build();
    Assert.assertEquals(0, count.get());
    Assert.assertEquals(value, cache.get("key"));
    Assert.assertEquals(1, count.get());
    Assert.assertEquals(value, cache.get("key"));
    Assert.assertEquals(1, count.get());
    Assert.assertEquals(value, cache.get("key2"));
    Assert.assertEquals(2, count.get());
    TimeUnit.SECONDS.sleep(5);
    Assert.assertEquals(value, cache.get("ket"));
    Assert.assertEquals(3, count.get());
  }

  @Test
  public void testPut() {
    AtomicInteger count = new AtomicInteger(0);
    Cache<String, String> cache =
        Cache.<String, String>builder()
            .timeout(Duration.ofSeconds(2))
            .fetcher(
                key -> {
                  count.incrementAndGet();
                  return CommonUtils.randomString();
                })
            .build();

    cache.get("key");
    Assert.assertEquals(1, count.get());
    cache.put("key2", "ad");
    cache.get("key2");
    Assert.assertEquals(1, count.get());
    cache.put(ImmutableBiMap.of("key3", "v", "key4", "v2"));
    cache.get("key3");
    Assert.assertEquals(1, count.get());
    cache.get("key4");
    Assert.assertEquals(1, count.get());
  }

  @Test
  public void getNonBlockingOnGet() throws InterruptedException {
    String key = CommonUtils.randomString();
    String value = CommonUtils.randomString();
    CountDownLatch latch = new CountDownLatch(1);
    // in first call we don't do blocking action.
    AtomicInteger count = new AtomicInteger(0);
    Cache<String, String> cache =
        Cache.<String, String>builder()
            .timeout(Duration.ofSeconds(2))
            .fetcher(
                inputKey -> {
                  if (count.getAndIncrement() == 0) return value;
                  try {
                    latch.await();
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  }
                  return value;
                })
            .build();
    ExecutorService service = Executors.newFixedThreadPool(2);
    try {
      Assert.assertEquals(value, cache.get(key));
      // sleep until the timeout
      TimeUnit.SECONDS.sleep(3);
      AtomicBoolean firstGet = new AtomicBoolean(false);
      // this thread should be blocked since the latch
      service.execute(
          () -> {
            try {
              cache.get(key);
            } finally {
              firstGet.set(true);
            }
          });
      TimeUnit.SECONDS.sleep(2);
      // the getter is blocked since it faces the expired data. It is blocked until the data is
      // updated.
      Assert.assertFalse(firstGet.get());
      AtomicBoolean secondGet = new AtomicBoolean(false);
      // this thread should be blocked since the latch
      service.execute(
          () -> {
            try {
              cache.get(key);
            } finally {
              secondGet.set(true);
            }
          });
      TimeUnit.SECONDS.sleep(2);
      // this getter is NOT blocked since the first get is updating data and this one get the older
      // stuff.
      Assert.assertTrue(secondGet.get());
      latch.countDown();
      TimeUnit.SECONDS.sleep(2);
      Assert.assertTrue(firstGet.get());
    } finally {
      service.shutdownNow();
      Assert.assertTrue(service.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testClear() {
    Cache<String, String> cache =
        Cache.<String, String>builder()
            .timeout(Duration.ofSeconds(2))
            .fetcher(key -> CommonUtils.randomString())
            .build();

    cache.get(CommonUtils.randomString());
    Assert.assertEquals(1, cache.size());
    cache.clear();
    Assert.assertEquals(0, cache.size());
  }

  @Test
  public void testUnmodifiableSnapshot() {
    Cache<String, String> cache =
        Cache.<String, String>builder()
            .timeout(Duration.ofSeconds(2))
            .fetcher(key -> CommonUtils.randomString())
            .build();

    String key = CommonUtils.randomString();
    cache.get(key);
    Assert.assertEquals(1, cache.size());
    Assert.assertThrows(UnsupportedOperationException.class, () -> cache.snapshot().remove(key));
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> cache.snapshot().put(key, CommonUtils.randomString()));
  }
}
