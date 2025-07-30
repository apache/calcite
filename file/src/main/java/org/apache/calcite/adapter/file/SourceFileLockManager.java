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
package org.apache.calcite.adapter.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Manages read locks on source files to prevent concurrent processing
 * by multiple Calcite instances.
 *
 * Uses shared (read) locks to allow multiple readers but coordinate
 * processing activities.
 */
public class SourceFileLockManager {
  // Track open channels to prevent GC while locks are held
  private static final ConcurrentHashMap<String, LockInfo> ACTIVE_LOCKS =
      new ConcurrentHashMap<>();

  private SourceFileLockManager() {
    // Utility class should not be instantiated
  }

  private static final long DEFAULT_LOCK_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);
  private static final long LOCK_RETRY_INTERVAL_MS = 100;

  /**
   * Acquire a shared read lock on a source file.
   * Returns a LockHandle that must be closed when done.
   */
  public static LockHandle acquireReadLock(File sourceFile) throws IOException {
    return acquireReadLock(sourceFile, DEFAULT_LOCK_TIMEOUT_MS);
  }

  /**
   * Acquire a shared read lock with custom timeout.
   */
  public static LockHandle acquireReadLock(File sourceFile, long timeoutMs)
      throws IOException {
    String path = sourceFile.getCanonicalPath();
    long deadline = System.currentTimeMillis() + timeoutMs;

    // Try Redis lock first if available
    RedisDistributedLock redisLock =
        RedisDistributedLock.createIfAvailable("source:read:" + path);
    if (redisLock != null) {
      try {
        if (redisLock.tryLock(timeoutMs)) {
          return new RedisLockHandle(path, redisLock);
        }
      } catch (Exception e) {
        // Fall back to file locks
        redisLock.close();
      }
    }

    // Use file-based shared lock
    while (System.currentTimeMillis() < deadline) {
      try {
        RandomAccessFile raf = new RandomAccessFile(sourceFile, "r");
        FileChannel channel = raf.getChannel();

        // Try to acquire shared lock (multiple readers allowed)
        FileLock lock = channel.tryLock(0, Long.MAX_VALUE, true);

        if (lock != null) {
          LockInfo info = new LockInfo(raf, channel, lock);
          ACTIVE_LOCKS.put(path, info);
          return new FileLockHandle(path, info);
        }

        // Close and retry if lock not available
        channel.close();
        raf.close();

      } catch (OverlappingFileLockException e) {
        // This JVM already has a lock on this file
        LockInfo existing = ACTIVE_LOCKS.get(path);
        if (existing != null) {
          existing.incrementRefCount();
          return new FileLockHandle(path, existing);
        }
      }

      try {
        Thread.sleep(
            Math.min(LOCK_RETRY_INTERVAL_MS,
            deadline - System.currentTimeMillis()));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while waiting for lock on: " + sourceFile);
      }
    }

    throw new IOException("Timeout acquiring lock on: " + sourceFile);
  }

  /**
   * Acquire an exclusive write lock for file modifications.
   */
  public static LockHandle acquireWriteLock(File sourceFile, long timeoutMs)
      throws IOException {
    String path = sourceFile.getCanonicalPath();
    long deadline = System.currentTimeMillis() + timeoutMs;

    // Try Redis lock first
    RedisDistributedLock redisLock =
        RedisDistributedLock.createIfAvailable("source:write:" + path);
    if (redisLock != null) {
      try {
        if (redisLock.tryLock(timeoutMs)) {
          return new RedisLockHandle(path, redisLock);
        }
      } catch (Exception e) {
        redisLock.close();
      }
    }

    // Use file-based exclusive lock
    while (System.currentTimeMillis() < deadline) {
      try {
        RandomAccessFile raf = new RandomAccessFile(sourceFile, "rw");
        FileChannel channel = raf.getChannel();

        // Try to acquire exclusive lock
        FileLock lock = channel.tryLock(0, Long.MAX_VALUE, false);

        if (lock != null) {
          LockInfo info = new LockInfo(raf, channel, lock);
          ACTIVE_LOCKS.put(path, info);
          return new FileLockHandle(path, info);
        }

        channel.close();
        raf.close();

      } catch (OverlappingFileLockException e) {
        // Cannot upgrade shared to exclusive in same JVM
        throw new IOException("Cannot acquire write lock - file already locked in this JVM");
      }

      try {
        Thread.sleep(
            Math.min(LOCK_RETRY_INTERVAL_MS,
            deadline - System.currentTimeMillis()));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while waiting for write lock");
      }
    }

    throw new IOException("Timeout acquiring write lock on: " + sourceFile);
  }

  /**
   * Handle for a file lock that must be closed when done.
   */
  public interface LockHandle extends AutoCloseable {
    boolean isValid();
    void close();
  }

  /**
   * Information about an active file lock.
   */
  private static class LockInfo {
    final RandomAccessFile raf;
    final FileChannel channel;
    final FileLock lock;
    int refCount = 1;

    LockInfo(RandomAccessFile raf, FileChannel channel, FileLock lock) {
      this.raf = raf;
      this.channel = channel;
      this.lock = lock;
    }

    synchronized void incrementRefCount() {
      refCount++;
    }

    synchronized boolean decrementRefCount() {
      return --refCount <= 0;
    }
  }

  /**
   * File-based lock handle.
   */
  private static class FileLockHandle implements LockHandle {
    private final String path;
    private final LockInfo info;
    private volatile boolean closed = false;

    FileLockHandle(String path, LockInfo info) {
      this.path = path;
      this.info = info;
    }

    @Override public boolean isValid() {
      return !closed && info.lock.isValid();
    }

    @Override public void close() {
      if (closed) {
        return;
      }
      closed = true;

      if (info.decrementRefCount()) {
        // Last reference, actually close
        try {
          info.lock.release();
        } catch (IOException e) {
          // Log but don't throw
        }
        try {
          info.channel.close();
        } catch (IOException e) {
          // Log but don't throw
        }
        try {
          info.raf.close();
        } catch (IOException e) {
          // Log but don't throw
        }
        ACTIVE_LOCKS.remove(path);
      }
    }
  }

  /**
   * Redis-based lock handle.
   */
  private static class RedisLockHandle implements LockHandle {
    private final String path;
    private final RedisDistributedLock redisLock;
    private volatile boolean closed = false;

    RedisLockHandle(String path, RedisDistributedLock redisLock) {
      this.path = path;
      this.redisLock = redisLock;
    }

    @Override public boolean isValid() {
      return !closed;
    }

    @Override public void close() {
      if (closed) {
        return;
      }
      closed = true;
      redisLock.close();
    }
  }
}
