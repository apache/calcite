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

import org.apache.calcite.adapter.file.storage.StorageProvider;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Persistent cache for storage provider data and metadata.
 * Provides thread-safe, restart-survivable caching for remote storage providers.
 *
 * <p>Features:
 * <ul>
 *   <li>Persistent storage of cached data and metadata</li>
 *   <li>File locking for concurrent access safety</li>
 *   <li>TTL (Time To Live) support for cache expiration</li>
 *   <li>ETag and last-modified validation</li>
 *   <li>Configurable cache directory per storage type</li>
 * </ul>
 *
 * <p>Cache structure:
 * <pre>
 * cacheDirectory/
 * ├── {storageType}/
 * │   ├── metadata.json         # File metadata cache
 * │   ├── data/                 # Cached file contents
 * │   │   ├── {hash1}.dat
 * │   │   ├── {hash2}.dat
 * │   │   └── ...
 * │   └── metadata.json.lock    # Lock file for metadata
 * </pre>
 */
public class PersistentStorageCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(PersistentStorageCache.class);
  private static final String METADATA_FILE = "metadata.json";
  private static final String DATA_DIR = "data";
  private static final ObjectMapper MAPPER = new ObjectMapper()
      .enable(SerializationFeature.INDENT_OUTPUT);

  private final File cacheDirectory;
  private final String storageType;
  private final long defaultTtlMs;
  private final File metadataFile;
  private final File dataDirectory;
  private final Map<String, CacheEntry> inMemoryCache = new ConcurrentHashMap<>();

  /**
   * Cache entry containing metadata and data location.
   */
  public static class CacheEntry {
    public String path;
    public StorageProvider.FileMetadata metadata;
    public String dataHash;
    public long cachedAt;
    public long ttlMs;

    public CacheEntry() {} // For Jackson

    public CacheEntry(String path, StorageProvider.FileMetadata metadata,
                     String dataHash, long ttlMs) {
      this.path = path;
      this.metadata = metadata;
      this.dataHash = dataHash;
      this.cachedAt = System.currentTimeMillis();
      this.ttlMs = ttlMs;
    }

    public boolean isExpired() {
      if (ttlMs <= 0) {
        return false; // No expiration
      }
      return System.currentTimeMillis() - cachedAt > ttlMs;
    }
  }

  /**
   * Creates a persistent cache for the given storage type.
   *
   * @param cacheDirectory Base cache directory
   * @param storageType Storage type identifier (e.g., "http", "s3")
   * @param defaultTtlMs Default TTL in milliseconds (0 = no expiration)
   */
  public PersistentStorageCache(File cacheDirectory, String storageType, long defaultTtlMs) {
    this.cacheDirectory = cacheDirectory;
    this.storageType = storageType;
    this.defaultTtlMs = defaultTtlMs;

    // Create storage-specific cache directory
    File storageCacheDir = new File(cacheDirectory, storageType);
    if (!storageCacheDir.exists()) {
      storageCacheDir.mkdirs();
    }

    this.metadataFile = new File(storageCacheDir, METADATA_FILE);
    this.dataDirectory = new File(storageCacheDir, DATA_DIR);
    if (!dataDirectory.exists()) {
      dataDirectory.mkdirs();
    }

    // Load existing metadata
    loadMetadata();

    LOGGER.debug("Initialized persistent cache for storage type '{}' at: {}",
        storageType, storageCacheDir);
  }

  /**
   * Gets cached data for a path if available and not expired.
   *
   * @param path The file path
   * @return Cached data or null if not cached/expired
   */
  public byte[] getCachedData(String path) {
    CacheEntry entry = inMemoryCache.get(path);
    if (entry == null || entry.isExpired()) {
      return null;
    }

    // Load data from disk
    File dataFile = new File(dataDirectory, entry.dataHash + ".dat");
    if (!dataFile.exists()) {
      // Data file missing, remove from cache
      inMemoryCache.remove(path);
      return null;
    }

    try {
      return Files.readAllBytes(dataFile.toPath());
    } catch (IOException e) {
      LOGGER.warn("Failed to read cached data for path '{}': {}", path, e.getMessage());
      return null;
    }
  }

  /**
   * Gets cached metadata for a path if available and not expired.
   *
   * @param path The file path
   * @return Cached metadata or null if not cached/expired
   */
  public StorageProvider.FileMetadata getCachedMetadata(String path) {
    CacheEntry entry = inMemoryCache.get(path);
    if (entry == null || entry.isExpired()) {
      return null;
    }
    return entry.metadata;
  }

  /**
   * Caches data and metadata for a path.
   *
   * @param path The file path
   * @param data The file data
   * @param metadata The file metadata
   * @param ttlMs TTL in milliseconds (0 = use default)
   */
  public void cacheData(String path, byte[] data, StorageProvider.FileMetadata metadata, long ttlMs) {
    try {
      // Generate hash for data filename
      String dataHash = generateHash(path + "_" + System.currentTimeMillis());

      // Write data to disk
      File dataFile = new File(dataDirectory, dataHash + ".dat");
      Files.write(dataFile.toPath(), data);

      // Create cache entry
      long effectiveTtl = ttlMs > 0 ? ttlMs : defaultTtlMs;
      CacheEntry entry = new CacheEntry(path, metadata, dataHash, effectiveTtl);

      // Update in-memory cache
      CacheEntry oldEntry = inMemoryCache.put(path, entry);

      // Clean up old data file if exists
      if (oldEntry != null && !oldEntry.dataHash.equals(dataHash)) {
        File oldDataFile = new File(dataDirectory, oldEntry.dataHash + ".dat");
        if (oldDataFile.exists()) {
          oldDataFile.delete();
        }
      }

      // Save metadata
      saveMetadata();

      LOGGER.debug("Cached data for path '{}' with TTL {}ms", path, effectiveTtl);

    } catch (Exception e) {
      LOGGER.error("Failed to cache data for path '{}': {}", path, e.getMessage());
    }
  }

  /**
   * Removes a path from the cache.
   *
   * @param path The file path to remove
   */
  public void evict(String path) {
    CacheEntry entry = inMemoryCache.remove(path);
    if (entry != null) {
      // Remove data file
      File dataFile = new File(dataDirectory, entry.dataHash + ".dat");
      if (dataFile.exists()) {
        dataFile.delete();
      }

      // Save metadata
      saveMetadata();

      LOGGER.debug("Evicted cache entry for path '{}'", path);
    }
  }

  /**
   * Clears all cached data (mainly for testing).
   */
  public void clear() {
    // Remove all data files
    File[] dataFiles = dataDirectory.listFiles();
    if (dataFiles != null) {
      for (File dataFile : dataFiles) {
        if (dataFile.getName().endsWith(".dat")) {
          dataFile.delete();
        }
      }
    }

    // Clear in-memory cache
    inMemoryCache.clear();

    // Remove metadata file
    if (metadataFile.exists()) {
      metadataFile.delete();
    }

    LOGGER.debug("Cleared all cache data for storage type '{}'", storageType);
  }

  /**
   * Cleans up expired entries.
   */
  public void cleanupExpired() {
    inMemoryCache.entrySet().removeIf(entry -> {
      if (entry.getValue().isExpired()) {
        // Remove data file
        File dataFile = new File(dataDirectory, entry.getValue().dataHash + ".dat");
        if (dataFile.exists()) {
          dataFile.delete();
        }
        LOGGER.debug("Cleaned up expired cache entry for path '{}'", entry.getKey());
        return true;
      }
      return false;
    });

    // Save updated metadata
    if (!inMemoryCache.isEmpty()) {
      saveMetadata();
    }
  }

  /**
   * Loads metadata from disk with file locking.
   */
  private void loadMetadata() {
    if (!metadataFile.exists()) {
      return;
    }

    File lockFile = new File(metadataFile.getParentFile(), metadataFile.getName() + ".lock");

    try (RandomAccessFile raf = new RandomAccessFile(lockFile, "rw");
         FileChannel channel = raf.getChannel()) {

      // Acquire shared lock for reading
      try (FileLock lock = channel.lock(0, Long.MAX_VALUE, true)) {
        @SuppressWarnings("unchecked")
        Map<String, CacheEntry> loaded =
            MAPPER.readValue(
                metadataFile, MAPPER.getTypeFactory().constructMapType(HashMap.class,
                String.class, CacheEntry.class));

        // Validate entries - remove if data file missing
        for (Map.Entry<String, CacheEntry> entry : loaded.entrySet()) {
          CacheEntry cacheEntry = entry.getValue();
          File dataFile = new File(dataDirectory, cacheEntry.dataHash + ".dat");
          if (dataFile.exists()) {
            inMemoryCache.put(entry.getKey(), cacheEntry);
          } else {
            LOGGER.debug("Removing cache entry for missing data file: {}", entry.getKey());
          }
        }

        LOGGER.debug("Loaded {} cache entries for storage type '{}'",
            inMemoryCache.size(), storageType);
      }
    } catch (Exception e) {
      LOGGER.error("Failed to load cache metadata for storage type '{}': {}",
          storageType, e.getMessage());
    }
  }

  /**
   * Saves metadata to disk with file locking.
   */
  private void saveMetadata() {
    File tempFile = new File(metadataFile.getParentFile(), metadataFile.getName() + ".tmp");
    File lockFile = new File(metadataFile.getParentFile(), metadataFile.getName() + ".lock");

    try (RandomAccessFile raf = new RandomAccessFile(lockFile, "rw");
         FileChannel channel = raf.getChannel()) {

      // Acquire exclusive lock
      try (FileLock lock = channel.lock()) {
        // Write to temp file first
        MAPPER.writeValue(tempFile, inMemoryCache);

        // Atomically move temp file to actual file
        Files.move(tempFile.toPath(), metadataFile.toPath(),
                   StandardCopyOption.REPLACE_EXISTING,
                   StandardCopyOption.ATOMIC_MOVE);

        LOGGER.debug("Saved {} cache entries for storage type '{}'",
            inMemoryCache.size(), storageType);
      }
    } catch (IOException e) {
      LOGGER.error("Failed to save cache metadata for storage type '{}': {}",
          storageType, e.getMessage());
      // Clean up temp file if it exists
      if (tempFile.exists()) {
        tempFile.delete();
      }
    }
  }

  /**
   * Generates a hash for data file naming.
   */
  private String generateHash(String input) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] hash = md.digest(input.getBytes());
      StringBuilder hexString = new StringBuilder();
      for (byte b : hash) {
        String hex = Integer.toHexString(0xff & b);
        if (hex.length() == 1) {
          hexString.append('0');
        }
        hexString.append(hex);
      }
      return hexString.toString().substring(0, 16); // Use first 16 chars
    } catch (NoSuchAlgorithmException e) {
      // Fallback to simple hash
      return String.valueOf(Math.abs(input.hashCode()));
    }
  }

  /**
   * Gets the number of entries in the cache.
   * @return cache size
   */
  public int getCacheSize() {
    return inMemoryCache.size();
  }
}
