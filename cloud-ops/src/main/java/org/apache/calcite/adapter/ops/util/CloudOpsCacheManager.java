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
package org.apache.calcite.adapter.ops.util;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Centralized cache manager for Cloud Ops API calls using Caffeine.
 * Provides configurable TTL caching with comprehensive metrics and debug logging.
 */
public class CloudOpsCacheManager {
  private static final Logger logger = LoggerFactory.getLogger(CloudOpsCacheManager.class);

  private final Cache<String, List<Map<String, Object>>> cache;
  private final Duration cacheTtl;
  private final boolean debugMode;
  
  // Default cache settings
  private static final int DEFAULT_TTL_MINUTES = 5;
  private static final int MAX_CACHE_SIZE = 1000;

  public CloudOpsCacheManager(int ttlMinutes, boolean debugMode) {
    this.cacheTtl = Duration.ofMinutes(ttlMinutes > 0 ? ttlMinutes : DEFAULT_TTL_MINUTES);
    this.debugMode = debugMode;
    
    this.cache = Caffeine.newBuilder()
        .maximumSize(MAX_CACHE_SIZE)
        .expireAfterWrite(this.cacheTtl)
        .recordStats()
        .build();

    if (logger.isInfoEnabled()) {
      logger.info("CloudOpsCacheManager initialized: TTL={}min, MaxSize={}, Debug={}", 
                 this.cacheTtl.toMinutes(), MAX_CACHE_SIZE, debugMode);
    }
  }

  /**
   * Get cached result or compute if not present.
   */
  public List<Map<String, Object>> getOrCompute(String cacheKey, 
                                               Supplier<List<Map<String, Object>>> apiCall) {
    long startTime = System.currentTimeMillis();
    
    try {
      List<Map<String, Object>> result = cache.get(cacheKey, key -> {
        if (debugMode && logger.isDebugEnabled()) {
          logger.debug("Cache MISS for key: {} - executing API call", cacheKey);
        }
        return apiCall.get();
      });

      long duration = System.currentTimeMillis() - startTime;
      
      if (debugMode && logger.isDebugEnabled()) {
        boolean wasFromCache = cache.getIfPresent(cacheKey) != null;
        logger.debug("Cache {} for key: {} - {} results retrieved in {}ms", 
                   wasFromCache ? "HIT" : "MISS", cacheKey, 
                   result != null ? result.size() : 0, duration);
      }

      return result;
    } catch (Exception e) {
      logger.warn("Error retrieving cached data for key '{}': {}", cacheKey, e.getMessage());
      // On cache error, execute API call directly
      return apiCall.get();
    }
  }

  /**
   * Manually put result into cache.
   */
  public void put(String cacheKey, List<Map<String, Object>> result) {
    cache.put(cacheKey, result);
    
    if (debugMode && logger.isDebugEnabled()) {
      logger.debug("Cache PUT for key: {} - {} results cached", cacheKey, 
                 result != null ? result.size() : 0);
    }
  }

  /**
   * Invalidate specific cache entry.
   */
  public void invalidate(String cacheKey) {
    cache.invalidate(cacheKey);
    
    if (debugMode && logger.isDebugEnabled()) {
      logger.debug("Cache INVALIDATE for key: {}", cacheKey);
    }
  }

  /**
   * Invalidate all cache entries.
   */
  public void invalidateAll() {
    long sizeBefore = cache.estimatedSize();
    cache.invalidateAll();
    
    if (logger.isInfoEnabled()) {
      logger.info("Cache INVALIDATE ALL - {} entries cleared", sizeBefore);
    }
  }

  /**
   * Get current cache statistics.
   */
  public CacheMetrics getCacheMetrics() {
    CacheStats stats = cache.stats();
    return new CacheMetrics(
        cache.estimatedSize(),
        stats.requestCount(),
        stats.hitCount(),
        stats.missCount(),
        stats.hitRate(),
        stats.missRate(),
        stats.averageLoadPenalty(),
        stats.evictionCount()
    );
  }

  /**
   * Build cache key for API calls.
   */
  public static String buildCacheKey(String provider, String operation, Object... params) {
    StringBuilder keyBuilder = new StringBuilder();
    keyBuilder.append(provider).append(":").append(operation);
    
    for (Object param : params) {
      if (param != null) {
        keyBuilder.append(":").append(param.toString());
      }
    }
    
    return keyBuilder.toString();
  }

  /**
   * Build cache key for filtered API calls (includes filter hash for uniqueness).
   */
  public static String buildFilteredCacheKey(String provider, String operation, 
                                           CloudOpsFilterHandler filterHandler, Object... params) {
    StringBuilder keyBuilder = new StringBuilder();
    keyBuilder.append(provider).append(":").append(operation);
    
    // Add filter hash for uniqueness
    if (filterHandler != null && filterHandler.hasPushableFilters()) {
      int filterHash = filterHandler.getPushableFilters().hashCode();
      keyBuilder.append(":filters:").append(filterHash);
    }
    
    for (Object param : params) {
      if (param != null) {
        keyBuilder.append(":").append(param.toString());
      }
    }
    
    return keyBuilder.toString();
  }

  /**
   * Build cache key for paginated API calls (includes pagination parameters).
   */
  public static String buildPaginatedCacheKey(String provider, String operation,
                                            CloudOpsPaginationHandler paginationHandler, Object... params) {
    StringBuilder keyBuilder = new StringBuilder();
    keyBuilder.append(provider).append(":").append(operation);
    
    // Add pagination parameters for uniqueness
    if (paginationHandler != null && paginationHandler.hasPagination()) {
      keyBuilder.append(":offset:").append(paginationHandler.getOffset());
      keyBuilder.append(":limit:").append(paginationHandler.getLimit());
    }
    
    for (Object param : params) {
      if (param != null) {
        keyBuilder.append(":").append(param.toString());
      }
    }
    
    return keyBuilder.toString();
  }

  /**
   * Build comprehensive cache key including all optimization parameters.
   */
  public static String buildComprehensiveCacheKey(String provider, String operation,
                                                CloudOpsProjectionHandler projectionHandler,
                                                CloudOpsSortHandler sortHandler,
                                                CloudOpsPaginationHandler paginationHandler,
                                                CloudOpsFilterHandler filterHandler,
                                                Object... params) {
    StringBuilder keyBuilder = new StringBuilder();
    keyBuilder.append(provider).append(":").append(operation);
    
    // Add projection hash
    if (projectionHandler != null && !projectionHandler.isSelectAll()) {
      int projectionHash = projectionHandler.getProjectedFieldNames().hashCode();
      keyBuilder.append(":proj:").append(projectionHash);
    }
    
    // Add sort hash
    if (sortHandler != null && sortHandler.hasSort()) {
      int sortHash = sortHandler.getSortFields().hashCode();
      keyBuilder.append(":sort:").append(sortHash);
    }
    
    // Add pagination parameters
    if (paginationHandler != null && paginationHandler.hasPagination()) {
      keyBuilder.append(":page:").append(paginationHandler.getOffset())
                .append(":").append(paginationHandler.getLimit());
    }
    
    // Add filter hash
    if (filterHandler != null && filterHandler.hasPushableFilters()) {
      int filterHash = filterHandler.getPushableFilters().hashCode();
      keyBuilder.append(":filt:").append(filterHash);
    }
    
    // Add additional parameters
    for (Object param : params) {
      if (param != null) {
        keyBuilder.append(":").append(param.toString());
      }
    }
    
    return keyBuilder.toString();
  }

  /**
   * Check if caching is beneficial for the current query.
   * Caching may not be beneficial for heavily filtered or paginated queries.
   */
  public static boolean shouldCache(CloudOpsFilterHandler filterHandler, 
                                  CloudOpsPaginationHandler paginationHandler) {
    // Don't cache if we have many specific filters (results may be too specific)
    if (filterHandler != null && filterHandler.getPushableFilters().size() > 3) {
      return false;
    }
    
    // Don't cache if pagination is very specific (e.g., high offset)
    if (paginationHandler != null && paginationHandler.hasPagination() && 
        paginationHandler.getOffset() > 100) {
      return false;
    }
    
    return true;
  }

  /**
   * Get cache configuration summary.
   */
  public String getConfigSummary() {
    return String.format("TTL=%dmin, MaxSize=%d, Debug=%s", 
                        cacheTtl.toMinutes(), MAX_CACHE_SIZE, debugMode);
  }

  /**
   * Cache performance metrics.
   */
  public static class CacheMetrics {
    public final long size;
    public final long requestCount;
    public final long hitCount;
    public final long missCount;
    public final double hitRate;
    public final double missRate;
    public final double averageLoadTime;
    public final long evictionCount;

    public CacheMetrics(long size, long requestCount, long hitCount, long missCount,
                       double hitRate, double missRate, double averageLoadTime, 
                       long evictionCount) {
      this.size = size;
      this.requestCount = requestCount;
      this.hitCount = hitCount;
      this.missCount = missCount;
      this.hitRate = hitRate;
      this.missRate = missRate;
      this.averageLoadTime = averageLoadTime;
      this.evictionCount = evictionCount;
    }

    @Override public String toString() {
      return String.format(
          "CacheMetrics[size=%d, requests=%d, hits=%d (%.1f%%), misses=%d (%.1f%%), " +
          "avgLoad=%.2fms, evictions=%d]",
          size, requestCount, hitCount, hitRate * 100, missCount, missRate * 100,
          averageLoadTime / 1_000_000, evictionCount);
    }

    /**
     * Check if cache performance is good (high hit rate).
     */
    public boolean isPerforming() {
      return requestCount > 10 && hitRate > 0.5; // >50% hit rate with sufficient requests
    }

    /**
     * Get performance assessment.
     */
    public String getPerformanceAssessment() {
      if (requestCount < 10) {
        return "Insufficient data";
      } else if (hitRate > 0.8) {
        return "Excellent";
      } else if (hitRate > 0.6) {
        return "Good";
      } else if (hitRate > 0.4) {
        return "Fair";
      } else {
        return "Poor";
      }
    }
  }
}