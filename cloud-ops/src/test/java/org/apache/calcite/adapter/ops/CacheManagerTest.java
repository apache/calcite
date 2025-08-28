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
import org.apache.calcite.adapter.ops.util.CloudOpsCacheValidator;

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
 * Unit tests for CloudOpsCacheManager functionality.
 */
@Tag("unit")
public class CacheManagerTest {
  private static final Logger logger = LoggerFactory.getLogger(CacheManagerTest.class);

  private CloudOpsCacheManager cacheManager;
  private AtomicInteger apiCallCount;

  @BeforeEach
  public void setUp() {
    logger.info("=== Setting up Cache Manager Test ===");
    cacheManager = new CloudOpsCacheManager(5, true); // 5 minute TTL, debug enabled
    apiCallCount = new AtomicInteger(0);
  }

  @Test public void testBasicCacheOperations() {
    logger.info("=== Testing Basic Cache Operations ===");

    String cacheKey = "test:basic:operation";
    List<Map<String, Object>> testData =
        Arrays.asList(Map.of("id", "1", "name", "test1"),
        Map.of("id", "2", "name", "test2"));

    // Test cache miss - should execute supplier
    List<Map<String, Object>> result1 = cacheManager.getOrCompute(cacheKey, () -> {
      apiCallCount.incrementAndGet();
      logger.debug("API call executed - count: {}", apiCallCount.get());
      return testData;
    });

    assertEquals(testData, result1);
    assertEquals(1, apiCallCount.get(), "API should be called once on cache miss");

    // Test cache hit - should NOT execute supplier
    List<Map<String, Object>> result2 = cacheManager.getOrCompute(cacheKey, () -> {
      apiCallCount.incrementAndGet();
      logger.debug("API call executed - count: {}", apiCallCount.get());
      return Arrays.asList(Map.of("id", "999", "name", "should-not-appear"));
    });

    assertEquals(testData, result2);
    assertEquals(1, apiCallCount.get(), "API should not be called on cache hit");

    logger.info("✅ Basic cache operations: Miss={}, Hit={}", 1, 1);
  }

  @Test public void testCacheKeyGeneration() {
    logger.info("=== Testing Cache Key Generation ===");

    // Test simple cache key
    String key1 = CloudOpsCacheManager.buildCacheKey("azure", "clusters", "sub1", "sub2");
    assertEquals("azure:clusters:sub1:sub2", key1);

    // Test cache key with null parameters
    String key2 = CloudOpsCacheManager.buildCacheKey("gcp", "storage", "project1", null, "project2");
    assertEquals("gcp:storage:project1:project2", key2);

    // Test cache key uniqueness
    String key3 = CloudOpsCacheManager.buildCacheKey("aws", "ec2", "account1");
    String key4 = CloudOpsCacheManager.buildCacheKey("aws", "ec2", "account2");
    assertNotEquals(key3, key4);

    logger.info("✅ Cache key generation: Simple={}, Null-handling={}, Uniqueness=verified",
               key1, key2);
  }

  @Test public void testCacheInvalidation() {
    logger.info("=== Testing Cache Invalidation ===");

    String cacheKey = "test:invalidation:key";
    List<Map<String, Object>> testData = Arrays.asList(Map.of("test", "data"));

    // Populate cache
    List<Map<String, Object>> result1 = cacheManager.getOrCompute(cacheKey, () -> {
      apiCallCount.incrementAndGet();
      return testData;
    });
    assertEquals(testData, result1);
    assertEquals(1, apiCallCount.get());

    // Invalidate specific key
    cacheManager.invalidate(cacheKey);

    // Should execute supplier again after invalidation
    List<Map<String, Object>> result2 = cacheManager.getOrCompute(cacheKey, () -> {
      apiCallCount.incrementAndGet();
      return Arrays.asList(Map.of("new", "data"));
    });

    assertEquals("data", result2.get(0).get("new"));
    assertEquals(2, apiCallCount.get(), "API should be called again after invalidation");

    logger.info("✅ Cache invalidation: Before={}, After invalidation={}",
               testData.size(), result2.size());
  }

  @Test public void testCacheMetrics() {
    logger.info("=== Testing Cache Metrics ===");

    String key1 = "metrics:test:1";
    String key2 = "metrics:test:2";

    // Generate some cache activity
    cacheManager.getOrCompute(key1, () -> Arrays.asList(Map.of("id", "1"))); // Miss
    cacheManager.getOrCompute(key1, () -> Arrays.asList(Map.of("id", "999"))); // Hit
    cacheManager.getOrCompute(key2, () -> Arrays.asList(Map.of("id", "2"))); // Miss
    cacheManager.getOrCompute(key2, () -> Arrays.asList(Map.of("id", "999"))); // Hit

    CloudOpsCacheManager.CacheMetrics metrics = cacheManager.getCacheMetrics();

    assertTrue(metrics.requestCount >= 4, "Should have at least 4 requests");
    assertTrue(metrics.hitCount >= 2, "Should have at least 2 hits");
    assertTrue(metrics.missCount >= 2, "Should have at least 2 misses");
    assertTrue(metrics.hitRate > 0, "Hit rate should be > 0");
    assertTrue(metrics.size >= 2, "Cache should contain at least 2 entries");

    logger.info("✅ Cache metrics: {}", metrics);
    logger.info("   Performance assessment: {}", metrics.getPerformanceAssessment());
    logger.info("   Is performing: {}", metrics.isPerforming());
  }

  @Test public void testCacheEviction() {
    logger.info("=== Testing Cache Eviction ===");

    // Create cache with very small size for testing eviction
    CloudOpsCacheManager smallCache = new CloudOpsCacheManager(5, true);

    // This test verifies that the cache doesn't grow indefinitely
    // but actual eviction testing would require filling beyond maxSize=1000
    // For this test, we just verify cache size tracking works

    for (int i = 0; i < 10; i++) {
      String key = "eviction:test:" + i;
      final int id = i; // Make variable effectively final for lambda
      smallCache.getOrCompute(key, () -> Arrays.asList(Map.of("id", String.valueOf(id))));
    }

    CloudOpsCacheManager.CacheMetrics metrics = smallCache.getCacheMetrics();
    assertEquals(10, metrics.size, "Cache should contain all 10 entries (below eviction threshold)");

    logger.info("✅ Cache eviction test: Size={}, Evictions={}", metrics.size, metrics.evictionCount);
  }

  @Test public void testCacheShouldCacheDecision() {
    logger.info("=== Testing shouldCache Decision Logic ===");

    // Test with no filters or pagination - should cache
    assertTrue(CloudOpsCacheManager.shouldCache(null, null));

    // Test with low filter count - should cache
    // Note: This test is simplified since we don't have actual filter/pagination handlers
    // In a real scenario, you'd create mock handlers with specific configurations

    logger.info("✅ shouldCache decision logic: Basic cases validated");
  }

  @Test public void testCacheConfiguration() {
    logger.info("=== Testing Cache Configuration ===");

    // Test default configuration
    CloudOpsCacheManager defaultCache = new CloudOpsCacheManager(5, false);
    String defaultConfig = defaultCache.getConfigSummary();
    assertTrue(defaultConfig.contains("TTL=5min"));
    assertTrue(defaultConfig.contains("Debug=false"));

    // Test custom configuration
    CloudOpsCacheManager customCache = new CloudOpsCacheManager(10, true);
    String customConfig = customCache.getConfigSummary();
    assertTrue(customConfig.contains("TTL=10min"));
    assertTrue(customConfig.contains("Debug=true"));

    logger.info("✅ Cache configuration: Default={}, Custom={}", defaultConfig, customConfig);
  }

  @Test public void testCacheErrorHandling() {
    logger.info("=== Testing Cache Error Handling ===");

    String cacheKey = "error:test:key";

    // Test with supplier that throws exception
    // The cache manager should propagate the exception from the API call
    assertThrows(RuntimeException.class, () -> {
      cacheManager.getOrCompute(cacheKey, () -> {
        apiCallCount.incrementAndGet();
        throw new RuntimeException("Test exception");
      });
    }, "Exception should be propagated from API call");

    assertEquals(1, apiCallCount.get(), "API call should have been attempted");

    logger.info("✅ Cache error handling: Exception properly propagated");
  }

  @Test public void testCacheValidation() {
    logger.info("=== Testing Cache Validation ===");

    // Test valid configuration
    CloudOpsConfig validConfig =
        new CloudOpsConfig(Arrays.asList("azure"), null, null, null, true, 5, false);

    CloudOpsCacheValidator.CacheValidationResult result =
        CloudOpsCacheValidator.validateCacheConfig(validConfig);

    assertTrue(result.isValid());
    logger.info("✅ Valid config validation: {}", result);

    // Test invalid configuration
    CloudOpsConfig invalidConfig =
        new CloudOpsConfig(Arrays.asList("azure"), null, null, null, true, -1, false);

    CloudOpsCacheValidator.CacheValidationResult invalidResult =
        CloudOpsCacheValidator.validateCacheConfig(invalidConfig);

    assertFalse(invalidResult.isValid());
    assertFalse(invalidResult.getErrors().isEmpty());
    logger.info("✅ Invalid config validation: {}", invalidResult);
  }
}
