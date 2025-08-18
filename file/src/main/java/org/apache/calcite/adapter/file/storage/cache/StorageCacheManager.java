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
package org.apache.calcite.adapter.file.storage.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manager for storage provider caches.
 * Coordinates cache instances for different storage types and provides
 * centralized configuration and cleanup.
 */
public class StorageCacheManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageCacheManager.class);
  
  // Singleton instance
  private static volatile StorageCacheManager instance;
  
  private final File baseCacheDirectory;
  // Schema-aware caches: schemaName -> storageType -> cache
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, PersistentStorageCache>> schemaCaches = new ConcurrentHashMap<>();
  private final ScheduledExecutorService cleanupExecutor;
  
  // Default cache settings
  private static final long DEFAULT_TTL_MS = TimeUnit.HOURS.toMillis(1); // 1 hour
  private static final long CLEANUP_INTERVAL_MINUTES = 30;
  
  private StorageCacheManager(File baseCacheDirectory) {
    this.baseCacheDirectory = baseCacheDirectory;
    
    // Create base cache directory
    if (!baseCacheDirectory.exists()) {
      baseCacheDirectory.mkdirs();
    }
    
    // Schedule periodic cleanup
    this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "storage-cache-cleanup");
      t.setDaemon(true);
      return t;
    });
    
    cleanupExecutor.scheduleAtFixedRate(
        this::cleanupAllCaches, 
        CLEANUP_INTERVAL_MINUTES, 
        CLEANUP_INTERVAL_MINUTES, 
        TimeUnit.MINUTES
    );
    
    LOGGER.info("Initialized storage cache manager at: {}", baseCacheDirectory);
  }
  
  /**
   * Initializes the global cache manager.
   * Should be called once at startup with the parquetCacheDirectory.
   * 
   * @param cacheDirectory Base cache directory (typically parquetCacheDirectory/storage_cache)
   */
  public static void initialize(File cacheDirectory) {
    if (instance == null) {
      synchronized (StorageCacheManager.class) {
        if (instance == null) {
          File storageCacheDir = new File(cacheDirectory, "storage_cache");
          instance = new StorageCacheManager(storageCacheDir);
          LOGGER.info("Storage cache manager initialized");
        }
      }
    }
  }
  
  /**
   * Gets the singleton cache manager instance.
   * 
   * @return Cache manager instance
   * @throws IllegalStateException if not initialized
   */
  public static StorageCacheManager getInstance() {
    if (instance == null) {
      throw new IllegalStateException("StorageCacheManager not initialized. Call initialize() first.");
    }
    return instance;
  }
  
  /**
   * Gets or creates a cache for a storage type.
   * 
   * @param storageType Storage type identifier
   * @return Persistent cache for the storage type
   */
  public PersistentStorageCache getCache(String storageType) {
    return getCache(null, storageType, DEFAULT_TTL_MS);
  }
  
  /**
   * Gets or creates a cache for a storage type with custom TTL.
   * 
   * @param storageType Storage type identifier
   * @param ttlMs Default TTL in milliseconds
   * @return Persistent cache for the storage type
   */
  public PersistentStorageCache getCache(String storageType, long ttlMs) {
    return getCache(null, storageType, ttlMs);
  }
  
  /**
   * Gets or creates a cache for a storage type with schema awareness.
   * 
   * @param schemaName Schema name (optional)
   * @param storageType Storage type identifier
   * @param ttlMs Default TTL in milliseconds
   * @return Persistent cache for the storage type
   */
  public PersistentStorageCache getCache(String schemaName, String storageType, long ttlMs) {
    String effectiveSchema = schemaName != null ? schemaName : "default";
    ConcurrentHashMap<String, PersistentStorageCache> caches = 
        schemaCaches.computeIfAbsent(effectiveSchema, k -> new ConcurrentHashMap<>());
    
    return caches.computeIfAbsent(storageType, type -> {
      // Create schema-specific cache directory
      File schemaCacheDir = new File(baseCacheDirectory, "schema_" + effectiveSchema);
      PersistentStorageCache cache = new PersistentStorageCache(schemaCacheDir, type, ttlMs);
      LOGGER.debug("Created cache for storage type '{}' in schema '{}': {}", 
                  type, effectiveSchema, schemaCacheDir);
      return cache;
    });
  }
  
  /**
   * Cleans up expired entries in all caches.
   */
  private void cleanupAllCaches() {
    try {
      int totalCaches = 0;
      for (ConcurrentHashMap<String, PersistentStorageCache> caches : schemaCaches.values()) {
        for (PersistentStorageCache cache : caches.values()) {
          cache.cleanupExpired();
          totalCaches++;
        }
      }
      LOGGER.debug("Completed cache cleanup for {} caches across {} schemas", 
                  totalCaches, schemaCaches.size());
    } catch (Exception e) {
      LOGGER.error("Error during cache cleanup", e);
    }
  }
  
  /**
   * Manually triggers cleanup for all caches.
   */
  public void cleanup() {
    cleanupAllCaches();
  }
  
  /**
   * Clears all caches (mainly for testing).
   */
  public void clearAll() {
    for (ConcurrentHashMap<String, PersistentStorageCache> caches : schemaCaches.values()) {
      for (PersistentStorageCache cache : caches.values()) {
        cache.clear();
      }
    }
    schemaCaches.clear();
    LOGGER.debug("Cleared all storage caches");
  }
  
  /**
   * Shuts down the cache manager and cleanup scheduler.
   */
  public void shutdown() {
    cleanupExecutor.shutdown();
    try {
      if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        cleanupExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      cleanupExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    }
    LOGGER.info("Storage cache manager shut down");
  }
  
  /**
   * Gets cache statistics for monitoring.
   * 
   * @return Map of storage type to cache entry count
   */
  public java.util.Map<String, Integer> getCacheStats() {
    java.util.Map<String, Integer> stats = new java.util.HashMap<>();
    schemaCaches.forEach((schema, caches) -> {
      caches.forEach((type, cache) -> {
        // This is a simplified stat - could be enhanced with more metrics
        String key = schema + ":" + type;
        stats.put(key, cache.getCacheSize());
      });
    });
    return stats;
  }
}