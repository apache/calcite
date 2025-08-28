/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.ops;

import org.apache.calcite.adapter.ops.util.CloudOpsCacheManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Performance tests for cache functionality under various load scenarios.
 * These tests measure cache performance, throughput, and behavior under stress.
 */
@Tag("performance")
public class CachePerformanceTest {
  private static final Logger logger = LoggerFactory.getLogger(CachePerformanceTest.class);

  private CloudOpsCacheManager cacheManager;
  private AtomicInteger apiCallCount;
  private AtomicLong totalApiLatency;

  @BeforeEach
  public void setUp() {
    logger.info("=== Setting up Cache Performance Test ===");

    // Use shorter TTL for performance tests to see cache behavior more quickly
    cacheManager = new CloudOpsCacheManager(1, false); // 1 minute TTL, debug disabled for performance
    apiCallCount = new AtomicInteger(0);
    totalApiLatency = new AtomicLong(0);

    logger.info("Cache manager initialized: {}", cacheManager.getConfigSummary());
  }

  @Test public void testCachePerformanceWithoutConcurrency() {
    logger.info("=== Testing Cache Performance (Single Threaded) ===");

    int iterations = 1000;
    int uniqueKeys = 50; // 20:1 hit ratio expected

    long startTime = System.currentTimeMillis();

    for (int i = 0; i < iterations; i++) {
      String key = "single:perf:test:" + (i % uniqueKeys);

      List<Map<String, Object>> result = cacheManager.getOrCompute(key, () -> {
        // Simulate API call latency
        try {
          Thread.sleep(ThreadLocalRandom.current().nextInt(10, 50));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }

        apiCallCount.incrementAndGet();
        return Arrays.asList(Map.of("key", key, "data", "test-data-" + System.currentTimeMillis()));
      });

      assertNotNull(result);
      assertEquals(1, result.size());
    }

    long duration = System.currentTimeMillis() - startTime;
    CloudOpsCacheManager.CacheMetrics metrics = cacheManager.getCacheMetrics();

    double hitRatio = (double) metrics.hitCount / metrics.requestCount;
    double avgTimePerRequest = (double) duration / iterations;

    logger.info("✅ Single-threaded performance:");
    logger.info("   Total time: {}ms for {} iterations", duration, iterations);
    logger.info("   Avg time per request: {:.2f}ms", avgTimePerRequest);
    logger.info("   API calls made: {} (expected ~{})", apiCallCount.get(), uniqueKeys);
    logger.info("   Cache hit ratio: {:.2f}% (expected ~95%)", hitRatio * 100);
    logger.info("   Final metrics: {}", metrics);

    // Performance assertions
    assertTrue(hitRatio > 0.8, "Hit ratio should be > 80%");
    assertTrue(avgTimePerRequest < 10, "Average time per request should be < 10ms with caching");
    assertEquals(uniqueKeys, apiCallCount.get(), "Should make exactly one API call per unique key");
  }

  @Test public void testCachePerformanceWithHighConcurrency() throws Exception {
    logger.info("=== Testing Cache Performance (High Concurrency) ===");

    int threadCount = 20;
    int iterationsPerThread = 100;
    int uniqueKeys = 25; // Each thread will hit same keys, testing concurrent cache access

    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    AtomicInteger totalRequests = new AtomicInteger(0);

    long startTime = System.currentTimeMillis();

    List<CompletableFuture<Void>> futures = new ArrayList<>();

    for (int thread = 0; thread < threadCount; thread++) {
      final int threadId = thread;
      CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
        for (int i = 0; i < iterationsPerThread; i++) {
          String key = "concurrent:perf:test:" + (i % uniqueKeys);

          List<Map<String, Object>> result = cacheManager.getOrCompute(key, () -> {
            // Simulate API call latency
            long apiStart = System.currentTimeMillis();
            try {
              Thread.sleep(ThreadLocalRandom.current().nextInt(20, 100));
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            long apiTime = System.currentTimeMillis() - apiStart;
            totalApiLatency.addAndGet(apiTime);

            int callCount = apiCallCount.incrementAndGet();
            logger.debug("Thread {} - API call #{} for key: {}", threadId, callCount, key);

            return Arrays.asList(Map.of("key", key, "thread", threadId, "call", callCount));
          });

          assertNotNull(result);
          totalRequests.incrementAndGet();
        }
      }, executor);

      futures.add(future);
    }

    // Wait for all threads to complete
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(60, TimeUnit.SECONDS);
    executor.shutdown();

    long duration = System.currentTimeMillis() - startTime;
    CloudOpsCacheManager.CacheMetrics metrics = cacheManager.getCacheMetrics();

    int expectedTotalRequests = threadCount * iterationsPerThread;
    double hitRatio = (double) metrics.hitCount / metrics.requestCount;
    double avgTimePerRequest = (double) duration / expectedTotalRequests;
    double avgApiLatency = totalApiLatency.get() > 0 ? (double) totalApiLatency.get() / apiCallCount.get() : 0;

    logger.info("✅ High-concurrency performance:");
    logger.info("   Threads: {}, Iterations per thread: {}, Unique keys: {}",
               threadCount, iterationsPerThread, uniqueKeys);
    logger.info("   Total time: {}ms for {} requests", duration, expectedTotalRequests);
    logger.info("   Avg time per request: {:.2f}ms", avgTimePerRequest);
    logger.info("   API calls made: {} (expected ~{})", apiCallCount.get(), uniqueKeys);
    logger.info("   Cache hit ratio: {:.2f}%", hitRatio * 100);
    logger.info("   Avg API latency: {:.2f}ms", avgApiLatency);
    logger.info("   Final metrics: {}", metrics);

    // Performance assertions for concurrent access
    assertEquals(expectedTotalRequests, totalRequests.get(), "All requests should complete");
    assertTrue(hitRatio > 0.8, "Hit ratio should be > 80% even with concurrency");
    assertTrue(avgTimePerRequest < 20, "Average time per request should be reasonable with caching");
    assertTrue(apiCallCount.get() >= uniqueKeys, "Should make at least one call per unique key");
    assertTrue(apiCallCount.get() <= uniqueKeys * 2, "Shouldn't make too many redundant API calls");
  }

  @Test public void testCacheMemoryAndEvictionPerformance() {
    logger.info("=== Testing Cache Memory and Eviction Performance ===");

    int totalEntries = 2000; // This will exceed the default max cache size of 1000

    long startTime = System.currentTimeMillis();

    // Fill cache beyond its capacity
    for (int i = 0; i < totalEntries; i++) {
      String key = "memory:test:" + i;

      List<Map<String, Object>> largeData = createLargeTestData(i, 10); // 10 entries per cache item

      cacheManager.getOrCompute(key, () -> {
        apiCallCount.incrementAndGet();
        return largeData;
      });

      // Log progress every 500 entries
      if (i > 0 && i % 500 == 0) {
        CloudOpsCacheManager.CacheMetrics intermediateMetrics = cacheManager.getCacheMetrics();
        logger.debug("Progress: {} entries added, cache size: {}, evictions: {}",
                    i, intermediateMetrics.size, intermediateMetrics.evictionCount);
      }
    }

    long duration = System.currentTimeMillis() - startTime;
    CloudOpsCacheManager.CacheMetrics finalMetrics = cacheManager.getCacheMetrics();

    logger.info("✅ Memory and eviction performance:");
    logger.info("   Total entries attempted: {}", totalEntries);
    logger.info("   Final cache size: {} (max capacity: 1000)", finalMetrics.size);
    logger.info("   Total evictions: {}", finalMetrics.evictionCount);
    logger.info("   API calls made: {}", apiCallCount.get());
    logger.info("   Total time: {}ms", duration);
    logger.info("   Avg time per entry: {:.2f}ms", (double) duration / totalEntries);

    // Memory management assertions
    assertTrue(finalMetrics.size <= 1000, "Cache size should not exceed maximum");
    assertTrue(finalMetrics.evictionCount > 0, "Should have evictions when exceeding capacity");
    assertEquals(totalEntries, apiCallCount.get(), "Should make one API call per entry (no cache hits expected)");
  }

  @Test public void testCachePerformanceWithVariousKeySizes() {
    logger.info("=== Testing Cache Performance with Various Key Sizes ===");

    List<String> shortKeys = Arrays.asList("a", "b", "c", "d", "e");
    List<String> mediumKeys = Arrays.asList("medium:key:1", "medium:key:2", "medium:key:3");
    List<String> longKeys =
        Arrays.asList("very:long:comprehensive:cache:key:with:many:segments:for:testing:performance:1",
        "very:long:comprehensive:cache:key:with:many:segments:for:testing:performance:2");

    int iterationsPerKeyType = 200;

    // Test short keys
    long startTime = System.currentTimeMillis();
    testKeyPerformance("short", shortKeys, iterationsPerKeyType);
    long shortKeyTime = System.currentTimeMillis() - startTime;

    // Test medium keys
    startTime = System.currentTimeMillis();
    testKeyPerformance("medium", mediumKeys, iterationsPerKeyType);
    long mediumKeyTime = System.currentTimeMillis() - startTime;

    // Test long keys
    startTime = System.currentTimeMillis();
    testKeyPerformance("long", longKeys, iterationsPerKeyType);
    long longKeyTime = System.currentTimeMillis() - startTime;

    CloudOpsCacheManager.CacheMetrics finalMetrics = cacheManager.getCacheMetrics();

    logger.info("✅ Key size performance comparison:");
    logger.info("   Short keys (avg len=1): {}ms for {} iterations", shortKeyTime, iterationsPerKeyType);
    logger.info("   Medium keys (avg len=13): {}ms for {} iterations", mediumKeyTime, iterationsPerKeyType);
    logger.info("   Long keys (avg len=65): {}ms for {} iterations", longKeyTime, iterationsPerKeyType);
    logger.info("   Final cache metrics: {}", finalMetrics);

    // Performance should be relatively consistent regardless of key size (within reason)
    double maxTimeVariation = Math.max(Math.max(shortKeyTime, mediumKeyTime), longKeyTime) * 2.0;
    double minTime = Math.min(Math.min(shortKeyTime, mediumKeyTime), longKeyTime);

    assertTrue(maxTimeVariation / minTime < 5.0,
              "Performance variation should not exceed 5x between different key sizes");
  }

  private void testKeyPerformance(String keyType, List<String> keys, int iterations) {
    AtomicInteger localCallCount = new AtomicInteger(0);

    for (int i = 0; i < iterations; i++) {
      String key = keyType + ":" + keys.get(i % keys.size()) + ":" + (i / keys.size());
      final int iteration = i; // Make variable effectively final for lambda

      cacheManager.getOrCompute(key, () -> {
        localCallCount.incrementAndGet();
        apiCallCount.incrementAndGet();
        return Arrays.asList(Map.of("type", keyType, "iteration", iteration));
      });
    }

    logger.debug("{} key type: {} API calls for {} iterations",
               keyType, localCallCount.get(), iterations);
  }

  private List<Map<String, Object>> createLargeTestData(int id, int size) {
    List<Map<String, Object>> data = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      data.add(
          Map.of(
          "id", id + "-" + i,
          "name", "test-item-" + id + "-" + i,
          "description", "This is test data item " + i + " for cache entry " + id,
          "timestamp", System.currentTimeMillis(),
          "random", ThreadLocalRandom.current().nextInt(1000)));
    }
    return data;
  }
}
