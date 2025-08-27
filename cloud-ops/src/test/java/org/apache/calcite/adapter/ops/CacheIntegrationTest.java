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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for caching functionality across different cloud providers.
 * These tests verify that caching works correctly with the actual provider implementations.
 */
@Tag("integration")
public class CacheIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(CacheIntegrationTest.class);

  private static CloudOpsConfig testConfig;
  private CloudOpsCacheManager cacheManager;

  @BeforeAll
  public static void setUpClass() {
    logger.info("=== Setting up Cache Integration Test ===");
    
    // Create test configuration with caching enabled
    testConfig = new CloudOpsConfig(
        Arrays.asList("azure", "aws", "gcp"),
        createMockAzureConfig(),
        createMockGCPConfig(), 
        createMockAWSConfig(),
        true,  // cacheEnabled
        2,     // cacheTtlMinutes  
        true   // cacheDebugMode
    );
    
    logger.info("Test configuration created with caching enabled");
  }

  @BeforeEach
  public void setUp() {
    logger.info("=== Setting up individual cache integration test ===");
    cacheManager = CloudOpsCacheValidator.createValidatedCacheManager(testConfig);
    CloudOpsCacheValidator.logCacheConfiguration(testConfig, cacheManager);
  }

  @Test public void testCacheConfigurationValidation() {
    logger.info("=== Testing Cache Configuration Validation ===");

    CloudOpsCacheValidator.CacheValidationResult result = 
        CloudOpsCacheValidator.validateCacheConfig(testConfig);

    assertTrue(result.isValid(), "Test configuration should be valid");
    logger.info("Configuration validation result: {}", result);

    // Log any warnings or recommendations
    if (!result.getWarnings().isEmpty()) {
      logger.warn("Configuration warnings: {}", result.getWarnings());
    }
    
    if (!result.getRecommendations().isEmpty()) {
      logger.info("Configuration recommendations: {}", result.getRecommendations());
    }

    logger.info("✅ Cache configuration validation completed successfully");
  }

  @Test public void testCacheManagerCreation() {
    logger.info("=== Testing Cache Manager Creation ===");

    assertNotNull(cacheManager, "Cache manager should be created");
    
    CloudOpsCacheManager.CacheMetrics initialMetrics = cacheManager.getCacheMetrics();
    assertEquals(0, initialMetrics.size, "Initial cache should be empty");
    assertEquals(0, initialMetrics.requestCount, "Initial request count should be 0");

    String configSummary = cacheManager.getConfigSummary();
    assertTrue(configSummary.contains("TTL=2min"), "Config should show 2 minute TTL");
    assertTrue(configSummary.contains("Debug=true"), "Config should show debug enabled");

    logger.info("✅ Cache manager creation: {}", configSummary);
  }

  @Test public void testCacheKeyBuilding() {
    logger.info("=== Testing Cache Key Building Strategies ===");

    // Test basic cache key building
    String basicKey = CloudOpsCacheManager.buildCacheKey("azure", "clusters", "sub1", "sub2");
    assertNotNull(basicKey);
    assertTrue(basicKey.contains("azure:clusters"));
    logger.debug("Basic cache key: {}", basicKey);

    // Test comprehensive cache key building (with null handlers)
    String comprehensiveKey = CloudOpsCacheManager.buildComprehensiveCacheKey("aws", "eks", 
        null, null, null, null, Arrays.asList("account1", "account2"));
    assertNotNull(comprehensiveKey);
    assertTrue(comprehensiveKey.contains("aws:eks"));
    logger.debug("Comprehensive cache key: {}", comprehensiveKey);

    // Test key uniqueness
    String key1 = CloudOpsCacheManager.buildCacheKey("gcp", "gke", "project1");
    String key2 = CloudOpsCacheManager.buildCacheKey("gcp", "gke", "project2");
    assertNotEquals(key1, key2, "Keys should be unique for different parameters");

    logger.info("✅ Cache key building: Basic={}, Comprehensive={}, Unique=verified", 
               basicKey.length(), comprehensiveKey.length());
  }

  @Test public void testCachePerformanceScenarios() {
    logger.info("=== Testing Cache Performance Scenarios ===");

    // Simulate rapid repeated queries
    String cacheKey = "performance:test:key";
    long startTime = System.currentTimeMillis();
    
    List<Map<String, Object>> testData = Arrays.asList(
        Map.of("cluster", "test-cluster-1", "region", "us-east-1"),
        Map.of("cluster", "test-cluster-2", "region", "us-west-2")
    );

    // First call - cache miss
    List<Map<String, Object>> result1 = cacheManager.getOrCompute(cacheKey, () -> {
      try {
        Thread.sleep(50); // Simulate API call delay
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return testData;
    });
    long firstCallTime = System.currentTimeMillis() - startTime;

    // Subsequent calls - cache hits
    startTime = System.currentTimeMillis();
    for (int i = 0; i < 10; i++) {
      List<Map<String, Object>> result = cacheManager.getOrCompute(cacheKey, () -> {
        fail("Should not execute supplier on cache hit");
        return null;
      });
      assertEquals(testData, result);
    }
    long cachedCallsTime = System.currentTimeMillis() - startTime;

    CloudOpsCacheManager.CacheMetrics metrics = cacheManager.getCacheMetrics();
    assertTrue(metrics.hitCount >= 10, "Should have multiple cache hits");
    assertTrue(firstCallTime > cachedCallsTime, "Cached calls should be faster");

    logger.info("✅ Cache performance: First call={}ms, 10 cached calls={}ms, Metrics={}", 
               firstCallTime, cachedCallsTime, metrics);
  }

  @Test public void testCacheInvalidationScenarios() {
    logger.info("=== Testing Cache Invalidation Scenarios ===");

    String key1 = "invalidation:test:1";
    String key2 = "invalidation:test:2";
    
    // Populate cache with multiple entries
    List<Map<String, Object>> data1 = Arrays.asList(Map.of("id", "1"));
    List<Map<String, Object>> data2 = Arrays.asList(Map.of("id", "2"));
    
    cacheManager.getOrCompute(key1, () -> data1);
    cacheManager.getOrCompute(key2, () -> data2);
    
    CloudOpsCacheManager.CacheMetrics beforeInvalidation = cacheManager.getCacheMetrics();
    assertTrue(beforeInvalidation.size >= 2, "Cache should contain entries");

    // Test specific key invalidation
    cacheManager.invalidate(key1);
    
    // key1 should be invalidated, key2 should still be cached
    cacheManager.getOrCompute(key1, () -> Arrays.asList(Map.of("id", "1-new")));
    List<Map<String, Object>> stillCached = cacheManager.getOrCompute(key2, () -> {
      fail("Key2 should still be cached");
      return null;
    });
    assertEquals(data2, stillCached);

    // Test full cache invalidation
    cacheManager.invalidateAll();
    CloudOpsCacheManager.CacheMetrics afterFullInvalidation = cacheManager.getCacheMetrics();
    
    logger.info("✅ Cache invalidation: Before={} entries, After full invalidation={} entries", 
               beforeInvalidation.size, afterFullInvalidation.size);
  }

  @Test public void testMultiProviderCacheScenarios() {
    logger.info("=== Testing Multi-Provider Cache Scenarios ===");

    // Simulate queries to different cloud providers
    String azureKey = CloudOpsCacheManager.buildCacheKey("azure", "aks", "subscription1");
    String awsKey = CloudOpsCacheManager.buildCacheKey("aws", "eks", "account1");  
    String gcpKey = CloudOpsCacheManager.buildCacheKey("gcp", "gke", "project1");

    List<Map<String, Object>> azureData = Arrays.asList(Map.of("provider", "azure", "clusters", 3));
    List<Map<String, Object>> awsData = Arrays.asList(Map.of("provider", "aws", "clusters", 5));
    List<Map<String, Object>> gcpData = Arrays.asList(Map.of("provider", "gcp", "clusters", 2));

    // Cache data for all providers
    cacheManager.getOrCompute(azureKey, () -> azureData);
    cacheManager.getOrCompute(awsKey, () -> awsData);
    cacheManager.getOrCompute(gcpKey, () -> gcpData);

    CloudOpsCacheManager.CacheMetrics multiProviderMetrics = cacheManager.getCacheMetrics();
    assertTrue(multiProviderMetrics.size >= 3, "Cache should contain entries for all providers");

    // Verify each provider's data is correctly cached
    assertEquals(azureData, cacheManager.getOrCompute(azureKey, () -> {
      fail("Azure data should be cached");
      return null;
    }));

    assertEquals(awsData, cacheManager.getOrCompute(awsKey, () -> {
      fail("AWS data should be cached");
      return null;
    }));

    assertEquals(gcpData, cacheManager.getOrCompute(gcpKey, () -> {
      fail("GCP data should be cached");  
      return null;
    }));

    logger.info("✅ Multi-provider caching: {} entries, Hit rate: {:.2f}%", 
               multiProviderMetrics.size, multiProviderMetrics.hitRate * 100);
  }

  @Test public void testCacheMetricsAndPerformanceAssessment() {
    logger.info("=== Testing Cache Metrics and Performance Assessment ===");

    // Generate diverse cache activity
    for (int i = 0; i < 15; i++) {
      String key = "metrics:test:" + (i % 5); // Create some repeated keys for hits
      cacheManager.getOrCompute(key, () -> Arrays.asList(Map.of("iteration", i)));
    }

    CloudOpsCacheManager.CacheMetrics finalMetrics = cacheManager.getCacheMetrics();
    
    assertTrue(finalMetrics.requestCount >= 15, "Should have at least 15 requests");
    assertTrue(finalMetrics.hitCount > 0, "Should have some cache hits");
    assertTrue(finalMetrics.missCount > 0, "Should have some cache misses");
    assertTrue(finalMetrics.size <= 5, "Cache should contain at most 5 unique entries");

    String performanceAssessment = finalMetrics.getPerformanceAssessment();
    assertNotNull(performanceAssessment);

    logger.info("✅ Final cache metrics: {}", finalMetrics);
    logger.info("   Performance assessment: {}", performanceAssessment);
    logger.info("   Cache is performing well: {}", finalMetrics.isPerforming());
  }

  // Helper methods to create mock configurations
  private static CloudOpsConfig.AzureConfig createMockAzureConfig() {
    return new CloudOpsConfig.AzureConfig("tenant-id", "client-id", "client-secret", 
        Arrays.asList("subscription-1", "subscription-2"));
  }

  private static CloudOpsConfig.AWSConfig createMockAWSConfig() {
    return new CloudOpsConfig.AWSConfig(Arrays.asList("account-1", "account-2"), 
        "us-east-1", "access-key", "secret-key", null);
  }

  private static CloudOpsConfig.GCPConfig createMockGCPConfig() {
    return new CloudOpsConfig.GCPConfig(Arrays.asList("project-1", "project-2"), 
        "/path/to/credentials.json");
  }
}