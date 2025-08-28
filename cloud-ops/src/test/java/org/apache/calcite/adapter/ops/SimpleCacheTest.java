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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple test focused only on basic caching functionality.
 */
@Tag("unit")
public class SimpleCacheTest {
  private static final Logger logger = LoggerFactory.getLogger(SimpleCacheTest.class);

  private CloudOpsCacheManager cacheManager;
  private AtomicInteger apiCallCount;

  @BeforeEach
  public void setUp() {
    logger.info("=== Setting up Simple Cache Test ===");
    cacheManager = new CloudOpsCacheManager(5, true); // 5 minute TTL, debug enabled
    apiCallCount = new AtomicInteger(0);
  }

  @Test public void testBasicCacheHitAndMiss() {
    logger.info("=== Testing Basic Cache Hit and Miss ===");

    String cacheKey = "test:basic:cache";
    List<Map<String, Object>> testData =
        Arrays.asList(Map.of("id", "1", "name", "test1"),
        Map.of("id", "2", "name", "test2"));

    // First call - should be a cache miss
    List<Map<String, Object>> result1 = cacheManager.getOrCompute(cacheKey, () -> {
      apiCallCount.incrementAndGet();
      logger.debug("API call executed - count: {}", apiCallCount.get());
      return testData;
    });

    assertEquals(testData, result1);
    assertEquals(1, apiCallCount.get(), "API should be called once on cache miss");

    // Second call - should be a cache hit
    List<Map<String, Object>> result2 = cacheManager.getOrCompute(cacheKey, () -> {
      apiCallCount.incrementAndGet();
      logger.debug("This should not execute - count: {}", apiCallCount.get());
      return Arrays.asList(Map.of("id", "999", "name", "should-not-appear"));
    });

    assertEquals(testData, result2);
    assertEquals(1, apiCallCount.get(), "API should not be called on cache hit");

    logger.info("✅ Basic cache test passed: 1 miss, 1 hit");
  }

  @Test public void testCacheKeyGeneration() {
    logger.info("=== Testing Cache Key Generation ===");

    // Test basic key generation
    String key1 = CloudOpsCacheManager.buildCacheKey("azure", "clusters", "sub1", "sub2");
    assertEquals("azure:clusters:sub1:sub2", key1);

    // Test key with null parameters
    String key2 = CloudOpsCacheManager.buildCacheKey("gcp", "storage", "project1", null, "project2");
    assertEquals("gcp:storage:project1:project2", key2);

    // Test key uniqueness
    String key3 = CloudOpsCacheManager.buildCacheKey("aws", "ec2", "account1");
    String key4 = CloudOpsCacheManager.buildCacheKey("aws", "ec2", "account2");
    assertNotEquals(key3, key4);

    logger.info("✅ Cache key generation: All tests passed");
  }

  @Test public void testCacheInvalidation() {
    logger.info("=== Testing Cache Invalidation ===");

    String cacheKey = "test:invalidation";
    List<Map<String, Object>> originalData = Arrays.asList(Map.of("test", "original"));

    // Populate cache
    List<Map<String, Object>> result1 = cacheManager.getOrCompute(cacheKey, () -> {
      apiCallCount.incrementAndGet();
      return originalData;
    });
    assertEquals(originalData, result1);
    assertEquals(1, apiCallCount.get());

    // Invalidate cache
    cacheManager.invalidate(cacheKey);

    // Should call API again after invalidation
    List<Map<String, Object>> newData = Arrays.asList(Map.of("test", "new"));
    List<Map<String, Object>> result2 = cacheManager.getOrCompute(cacheKey, () -> {
      apiCallCount.incrementAndGet();
      return newData;
    });

    assertEquals(newData, result2);
    assertEquals(2, apiCallCount.get(), "API should be called again after invalidation");

    logger.info("✅ Cache invalidation test passed");
  }

  @Test public void testCacheMetrics() {
    logger.info("=== Testing Cache Metrics ===");

    // Generate some cache activity
    for (int i = 0; i < 10; i++) {
      String key = "metrics:test:" + (i % 3); // This creates hits for repeated keys
      final int id = i; // Make variable effectively final for lambda
      cacheManager.getOrCompute(key, () -> {
        apiCallCount.incrementAndGet();
        return Arrays.asList(Map.of("id", id));
      });
    }

    CloudOpsCacheManager.CacheMetrics metrics = cacheManager.getCacheMetrics();

    assertTrue(metrics.requestCount >= 10, "Should have at least 10 requests");
    assertTrue(metrics.hitCount > 0, "Should have some cache hits");
    assertTrue(metrics.missCount > 0, "Should have some cache misses");
    assertTrue(metrics.size <= 3, "Cache should contain at most 3 unique entries");

    logger.info("✅ Cache metrics: {}", metrics);
    logger.info("   Hit rate: {:.1f}%", metrics.hitRate * 100);
    logger.info("   API calls made: {}", apiCallCount.get());

    // We expect 3 unique keys, so API should be called 3 times
    assertEquals(3, apiCallCount.get(), "API should be called once per unique key");
  }

  @Test public void testCacheConfiguration() {
    logger.info("=== Testing Cache Configuration ===");

    String configSummary = cacheManager.getConfigSummary();
    assertTrue(configSummary.contains("TTL=5min"));
    assertTrue(configSummary.contains("Debug=true"));

    logger.info("✅ Cache configuration: {}", configSummary);
  }
}
