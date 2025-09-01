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
package org.apache.calcite.adapter.file.statistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LRU cache for HyperLogLog sketches to avoid repeated disk I/O.
 * Thread-safe implementation with configurable maximum size and TTL.
 */
public class HLLSketchCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(HLLSketchCache.class);

  // Default cache configuration
  private static final int DEFAULT_MAX_SIZE = 1000;
  private static final long DEFAULT_TTL_MS = 30 * 60 * 1000; // 30 minutes

  // Singleton instance with per-schema caches
  private static volatile HLLSketchCache instance;
  private static final Object INSTANCE_LOCK = new Object();

  // Per-schema cache storage
  private final Map<String, Map<String, CacheEntry>> schemaCaches = new ConcurrentHashMap<>();
  private final Map<String, Map<String, String>> schemaCaseIndexes = new ConcurrentHashMap<>();

  private final int maxSize;
  private final long ttlMs;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  // Note: cache and caseInsensitiveIndex are now per-schema
  // (see schemaCaches and schemaCaseIndexes)

  // Track cache statistics
  private final CacheStats stats = new CacheStats();

  private HLLSketchCache(int maxSize, long ttlMs) {
    this.maxSize = maxSize;
    this.ttlMs = ttlMs;

    LOGGER.info("HLL sketch cache initialized: maxSize={}, ttlMs={}", maxSize, ttlMs);
  }

  /**
   * Get or create the cache for a specific schema.
   */
  private Map<String, CacheEntry> getSchemaCache(String schemaName) {
    return schemaCaches.computeIfAbsent(schemaName, k -> {
      LOGGER.debug("Creating cache for schema: {}", k);
      // Create LRU LinkedHashMap with access-order for this schema
      return new LinkedHashMap<String, CacheEntry>(16, 0.75f, true) {
        @Override protected boolean removeEldestEntry(Map.Entry<String, CacheEntry> eldest) {
          return size() > HLLSketchCache.this.maxSize;
        }
      };
    });
  }

  /**
   * Get or create the case-insensitive index for a specific schema.
   */
  private Map<String, String> getSchemaCaseIndex(String schemaName) {
    return schemaCaseIndexes.computeIfAbsent(schemaName, k -> new HashMap<>());
  }

  /**
   * Get the singleton cache instance.
   */
  public static HLLSketchCache getInstance() {
    if (instance == null) {
      synchronized (INSTANCE_LOCK) {
        if (instance == null) {
          int maxSize =
              Integer.getInteger("calcite.file.statistics.hll.cache.size", DEFAULT_MAX_SIZE);
          long ttlMs = Long.getLong("calcite.file.statistics.hll.cache.ttl.ms", DEFAULT_TTL_MS);
          instance = new HLLSketchCache(maxSize, ttlMs);
        }
      }
    }
    return instance;
  }

  /**
   * Get HLL sketch from cache.
   * @param schemaName The schema name (for proper isolation)
   * @param tableName The table name
   * @param columnName The column name
   * @return The HLL sketch or null if not found
   */
  public HyperLogLogSketch getSketch(String schemaName, String tableName, String columnName) {
    // Use table.column as key within the schema-specific cache
    String key = tableName + "." + columnName;
    String fullKey = schemaName + "." + key; // For logging

    LOGGER.debug("Looking up HLL sketch: {}", fullKey);

    Map<String, CacheEntry> cache = getSchemaCache(schemaName);
    Map<String, String> caseInsensitiveIndex = getSchemaCaseIndex(schemaName);

    lock.readLock().lock();
    try {
      // Try exact match first
      CacheEntry entry = cache.get(key);
      if (entry != null && !entry.isExpired()) {
        stats.recordHit();
        LOGGER.debug("Found HLL sketch (exact match): {} with estimate {}",
            fullKey, entry.sketch.getEstimate());
        return entry.sketch;
      }

      // Try case-insensitive match
      String lowercaseKey = key.toLowerCase();
      String actualKey = caseInsensitiveIndex.get(lowercaseKey);
      if (actualKey != null) {
        entry = cache.get(actualKey);
        if (entry != null && !entry.isExpired()) {
          stats.recordHit();
          LOGGER.debug("Found HLL sketch via case-insensitive lookup: {} -> {} with estimate {}",
                      fullKey, actualKey, entry.sketch.getEstimate());
          return entry.sketch;
        }
      }

      // Log what keys are in the cache for debugging
      if (LOGGER.isDebugEnabled() && cache.size() < 20) {
        LOGGER.debug("Cache miss for {} in schema {}. Current cache keys: {}",
            key, schemaName, cache.keySet());
      }
    } finally {
      lock.readLock().unlock();
    }

    // Cache miss or expired - load from disk
    stats.recordMiss();
    LOGGER.debug("Cache miss for HLL sketch: {}", fullKey);
    return loadFromDisk(fullKey, tableName, columnName);
  }

  /**
   * Put HLL sketch into cache.
   * @param schemaName The schema name (for proper isolation)
   * @param tableName The table name
   * @param columnName The column name
   * @param sketch The HLL sketch to cache
   */
  public void putSketch(String schemaName, String tableName, String columnName,
      HyperLogLogSketch sketch) {
    // Use table.column as key within the schema-specific cache
    String key = tableName + "." + columnName;
    String fullKey = schemaName + "." + key; // For logging

    Map<String, CacheEntry> cache = getSchemaCache(schemaName);
    Map<String, String> caseInsensitiveIndex = getSchemaCaseIndex(schemaName);

    lock.writeLock().lock();
    try {
      cache.put(key, new CacheEntry(sketch, System.currentTimeMillis()));

      // Add to case-insensitive index, but warn if there's a conflict
      String lowercaseKey = key.toLowerCase();
      String existingKey = caseInsensitiveIndex.get(lowercaseKey);
      if (existingKey != null && !existingKey.equals(key)) {
        LOGGER.warn("Case-sensitive naming conflict detected in schema {}! "
                   + "Both '{}' and '{}' map to lowercase '{}'. "
                   + "Case-insensitive lookups may return incorrect results.",
                   schemaName, existingKey, key, lowercaseKey);
      }
      caseInsensitiveIndex.put(lowercaseKey, key);

      // Debug logging to track what's being stored
      LOGGER.debug("Stored HLL sketch: {} (estimate: {})", fullKey, sketch.getEstimate());
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Remove entry from cache.
   * @param schemaName The schema name
   * @param tableName The table name
   * @param columnName The column name
   */
  public void invalidate(String schemaName, String tableName, String columnName) {
    String key = tableName + "." + columnName;

    Map<String, CacheEntry> cache = getSchemaCache(schemaName);
    Map<String, String> caseInsensitiveIndex = getSchemaCaseIndex(schemaName);

    lock.writeLock().lock();
    try {
      cache.remove(key);
      caseInsensitiveIndex.remove(key.toLowerCase());
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Clear all cached entries.
   */
  public void invalidateAll() {
    lock.writeLock().lock();
    try {
      schemaCaches.clear();
      schemaCaseIndexes.clear();
      LOGGER.info("All HLL sketch caches cleared");
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Clear cache for a specific schema.
   */
  public void invalidateSchema(String schemaName) {
    lock.writeLock().lock();
    try {
      schemaCaches.remove(schemaName);
      schemaCaseIndexes.remove(schemaName);
      LOGGER.info("HLL sketch cache cleared for schema: {}", schemaName);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Get cache statistics.
   */
  public CacheStats getStats() {
    return stats.snapshot();
  }

  /**
   * Get current cache size.
   */
  public int size() {
    lock.readLock().lock();
    try {
      int totalSize = 0;
      for (Map<String, CacheEntry> cache : schemaCaches.values()) {
        totalSize += cache.size();
      }
      return totalSize;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Remove expired entries from cache.
   */
  public void cleanup() {
    lock.writeLock().lock();
    try {
      long now = System.currentTimeMillis();
      int totalRemoved = 0;

      for (Map<String, CacheEntry> cache : schemaCaches.values()) {
        int sizeBefore = cache.size();
        cache.entrySet().removeIf(entry -> entry.getValue().isExpired(now));
        int removed = sizeBefore - cache.size();
        totalRemoved += removed;
      }

      if (totalRemoved > 0) {
        LOGGER.debug("Removed {} expired HLL sketches from cache", totalRemoved);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  private HyperLogLogSketch loadFromDisk(String key, String tableName, String columnName) {
    // Get cache directory from system property
    String cacheDirPath = System.getProperty("calcite.file.statistics.cache.directory");
    if (cacheDirPath == null) {
      return null;
    }

    File cacheDir = new File(cacheDirPath);
    File sketchFile = new File(cacheDir, tableName + "_" + columnName + ".hll");

    if (!sketchFile.exists()) {
      return null;
    }

    try {
      HyperLogLogSketch sketch = StatisticsCache.loadHLLSketch(sketchFile);

      // Note: We can't put it back into cache here without knowing the schema
      // The caller should use putSketch() if they want to cache it

      return sketch;
    } catch (IOException e) {
      LOGGER.warn("Failed to load HLL sketch from {}: {}", sketchFile, e.getMessage());
      return null;
    }
  }

  /**
   * Cache entry with timestamp for TTL.
   */
  private class CacheEntry {
    final HyperLogLogSketch sketch;
    final long timestamp;

    CacheEntry(HyperLogLogSketch sketch, long timestamp) {
      this.sketch = sketch;
      this.timestamp = timestamp;
    }

    boolean isExpired() {
      return isExpired(System.currentTimeMillis());
    }

    boolean isExpired(long now) {
      return (now - timestamp) > ttlMs;
    }
  }

  /**
   * Cache statistics tracking.
   */
  public static class CacheStats {
    private volatile long hits = 0;
    private volatile long misses = 0;

    void recordHit() {
      hits++;
    }

    void recordMiss() {
      misses++;
    }

    public long getHits() {
      return hits;
    }

    public long getMisses() {
      return misses;
    }

    public long getRequests() {
      return hits + misses;
    }

    public double getHitRate() {
      long total = getRequests();
      return total == 0 ? 0.0 : (double) hits / total;
    }

    CacheStats snapshot() {
      CacheStats copy = new CacheStats();
      copy.hits = this.hits;
      copy.misses = this.misses;
      return copy;
    }

    @Override public String toString() {
      return String.format("CacheStats{hits=%d, misses=%d, hitRate=%.2f%%}",
                          hits, misses, getHitRate() * 100);
    }
  }
}
