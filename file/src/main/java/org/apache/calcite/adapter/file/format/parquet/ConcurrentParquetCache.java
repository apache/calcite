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
package org.apache.calcite.adapter.file.format.parquet;

import org.apache.calcite.adapter.file.cache.RedisDistributedLock;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread-safe Parquet cache manager that handles concurrent access from multiple
 * JDBC connections.
 */
public class ConcurrentParquetCache {
  // In-process lock map for threads within the same JVM
  private static final ConcurrentHashMap<String, Lock> LOCK_MAP = new ConcurrentHashMap<>();

  private ConcurrentParquetCache() {
    // Utility class should not be instantiated
  }

  // Lock acquisition timeout
  private static final long LOCK_TIMEOUT_SECONDS = 30;

  /**
   * Convert a file to Parquet with proper concurrency control.
   * Uses Redis locks if available, otherwise falls back to file system locks.
   */
  public static File convertWithLocking(File sourceFile, File cacheDir,
      ConversionCallback callback) throws Exception {

    String lockKey = sourceFile.getAbsolutePath();

    // Try Redis distributed lock first
    RedisDistributedLock redisLock = RedisDistributedLock.createIfAvailable(lockKey);
    if (redisLock != null) {
      try {
        if (redisLock.tryLock(TimeUnit.SECONDS.toMillis(LOCK_TIMEOUT_SECONDS))) {
          return performConversion(sourceFile, cacheDir, callback);
        } else {
          throw new IOException("Timeout waiting for Redis lock on: " + sourceFile);
        }
      } finally {
        redisLock.close();
      }
    }

    // Fall back to local locks
    Lock processLock = LOCK_MAP.computeIfAbsent(lockKey, k -> new ReentrantLock());

    boolean acquired = false;
    try {
      // Try to acquire in-process lock with timeout
      acquired = processLock.tryLock(LOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      if (!acquired) {
        throw new IOException("Timeout waiting for lock on: " + sourceFile);
      }

      return performConversionWithFileLock(sourceFile, cacheDir, callback);
    } finally {
      if (acquired) {
        processLock.unlock();
      }
      // Clean up lock file periodically (could be done in a background thread)
      cleanupOldLockFiles(cacheDir);
    }
  }

  private static File performConversion(File sourceFile, File cacheDir,
      ConversionCallback callback) throws Exception {
    // Ensure cache directory exists
    if (!cacheDir.exists()) {
      cacheDir.mkdirs();
    }

    File parquetFile = ParquetConversionUtil.getCachedParquetFile(sourceFile, cacheDir);

    // Double-check if conversion is still needed
    if (!ParquetConversionUtil.needsConversion(sourceFile, parquetFile)) {
      return parquetFile;
    }

    // Perform the actual conversion to a temp file
    File tempFile = new File(parquetFile.getAbsolutePath() + ".tmp."
        + Thread.currentThread().threadId());

    try {
      callback.convert(tempFile);

      // Atomic rename (on most filesystems)
      Files.move(tempFile.toPath(), parquetFile.toPath(),
          java.nio.file.StandardCopyOption.REPLACE_EXISTING,
          java.nio.file.StandardCopyOption.ATOMIC_MOVE);

    } finally {
      // Clean up temp file if it still exists
      if (tempFile.exists()) {
        tempFile.delete();
      }
    }

    return parquetFile;
  }

  private static File performConversionWithFileLock(File sourceFile, File cacheDir,
      ConversionCallback callback) throws Exception {
    // Ensure cache directory exists
    if (!cacheDir.exists()) {
      cacheDir.mkdirs();
    }

    File parquetFile = ParquetConversionUtil.getCachedParquetFile(sourceFile, cacheDir);
    File lockFile = new File(parquetFile.getAbsolutePath() + ".lock");

    // Use file lock for cross-JVM synchronization
    try (FileChannel channel =
         FileChannel.open(lockFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
         FileLock fileLock = channel.tryLock()) {

      if (fileLock == null) {
        throw new IOException("Could not acquire file lock for: " + parquetFile);
      }

      // Use the common conversion logic
      return performConversion(sourceFile, cacheDir, callback);
    }
  }

  /**
   * Clean up stale lock files older than 1 hour.
   */
  private static void cleanupOldLockFiles(File cacheDir) {
    if (!cacheDir.exists()) {
      return;
    }

    long oneHourAgo = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
    File[] lockFiles = cacheDir.listFiles((dir, name) -> name.endsWith(".lock"));

    if (lockFiles != null) {
      for (File lockFile : lockFiles) {
        if (lockFile.lastModified() < oneHourAgo) {
          lockFile.delete();
        }
      }
    }
  }

  /**
   * Callback interface for the actual conversion logic.
   */
  @FunctionalInterface
  public interface ConversionCallback {
    void convert(File targetFile) throws Exception;
  }
}
