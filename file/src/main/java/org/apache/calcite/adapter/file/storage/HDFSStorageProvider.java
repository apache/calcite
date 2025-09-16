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
package org.apache.calcite.adapter.file.storage;

import org.apache.calcite.adapter.file.storage.cache.PersistentStorageCache;
import org.apache.calcite.adapter.file.storage.cache.StorageCacheManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Storage provider implementation for Hadoop Distributed File System (HDFS).
 * Leverages existing Hadoop dependencies to provide enterprise-grade distributed storage access.
 */
public class HDFSStorageProvider implements StorageProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(HDFSStorageProvider.class);

  private final FileSystem hdfsFileSystem;
  private final Configuration hadoopConfig;
  private final String defaultUri;

  // Persistent cache for restart-survivable caching
  private final PersistentStorageCache persistentCache;

  /**
   * Creates an HDFS storage provider with default configuration.
   * Attempts to load configuration from classpath (core-site.xml, hdfs-site.xml).
   */
  public HDFSStorageProvider() throws IOException {
    this(new Configuration());
  }

  /**
   * Creates an HDFS storage provider with the specified Hadoop configuration.
   *
   * @param config Hadoop configuration containing HDFS settings
   * @throws IOException if HDFS connection cannot be established
   */
  public HDFSStorageProvider(Configuration config) throws IOException {
    this.hadoopConfig = new Configuration(config);
    
    // Get the default filesystem URI from configuration
    this.defaultUri = hadoopConfig.get("fs.defaultFS", "hdfs://localhost:9000");
    
    try {
      // Initialize HDFS filesystem
      this.hdfsFileSystem = FileSystem.get(URI.create(defaultUri), hadoopConfig);
      
      LOGGER.info("Initialized HDFS storage provider with URI: {}", defaultUri);
      
      // Log authentication information for debugging
      if (UserGroupInformation.isSecurityEnabled()) {
        LOGGER.info("Kerberos authentication enabled, current user: {}", 
            UserGroupInformation.getCurrentUser());
      } else {
        LOGGER.info("Simple authentication, current user: {}", 
            UserGroupInformation.getCurrentUser().getShortUserName());
      }
      
    } catch (Exception e) {
      throw new IOException("Failed to initialize HDFS filesystem with URI: " + defaultUri, e);
    }

    // Initialize persistent cache if cache manager is available
    PersistentStorageCache cache = null;
    try {
      cache = StorageCacheManager.getInstance().getCache("hdfs");
    } catch (IllegalStateException e) {
      // Cache manager not initialized, persistent cache will be null
      LOGGER.debug("Storage cache manager not available, persistent caching disabled");
    }
    this.persistentCache = cache;
  }

  /**
   * Creates an HDFS storage provider with explicit FileSystem instance.
   * Useful for testing with mock filesystems.
   *
   * @param fileSystem Pre-configured HDFS FileSystem instance
   * @param config Hadoop configuration
   */
  public HDFSStorageProvider(FileSystem fileSystem, Configuration config) {
    this.hdfsFileSystem = fileSystem;
    this.hadoopConfig = config;
    this.defaultUri = config.get("fs.defaultFS", "hdfs://localhost:9000");

    // Initialize persistent cache
    PersistentStorageCache cache = null;
    try {
      cache = StorageCacheManager.getInstance().getCache("hdfs");
    } catch (IllegalStateException e) {
      // Cache manager not initialized
    }
    this.persistentCache = cache;
  }

  @Override public List<FileEntry> listFiles(String path, boolean recursive) throws IOException {
    Path hdfsPath = createHadoopPath(path);
    List<FileEntry> entries = new ArrayList<>();

    try {
      if (!hdfsFileSystem.exists(hdfsPath)) {
        LOGGER.debug("Path does not exist: {}", hdfsPath);
        return entries;
      }

      if (recursive) {
        // Use listFiles for recursive listing
        org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> iterator = 
            hdfsFileSystem.listFiles(hdfsPath, true);
        
        while (iterator.hasNext()) {
          org.apache.hadoop.fs.LocatedFileStatus status = iterator.next();
          entries.add(createFileEntry(status));
        }
      } else {
        // Use listStatus for non-recursive listing
        FileStatus[] statuses = hdfsFileSystem.listStatus(hdfsPath);
        for (FileStatus status : statuses) {
          entries.add(createFileEntry(status));
        }
      }

      LOGGER.debug("Listed {} entries from HDFS path: {}", entries.size(), hdfsPath);
      return entries;

    } catch (IOException e) {
      LOGGER.error("Failed to list files in HDFS path: {}", hdfsPath, e);
      throw new IOException("Failed to list files in HDFS path: " + path, e);
    }
  }

  @Override public FileMetadata getMetadata(String path) throws IOException {
    Path hdfsPath = createHadoopPath(path);

    try {
      FileStatus status = hdfsFileSystem.getFileStatus(hdfsPath);
      
      // HDFS doesn't have ETags like S3, so we'll use modification time as a pseudo-ETag
      String etag = String.valueOf(status.getModificationTime());
      
      return new FileMetadata(
          path,
          status.getLen(),
          status.getModificationTime(),
          inferContentType(path),
          etag
      );

    } catch (IOException e) {
      LOGGER.error("Failed to get metadata for HDFS path: {}", hdfsPath, e);
      throw new IOException("Failed to get metadata for HDFS path: " + path, e);
    }
  }

  @Override public InputStream openInputStream(String path) throws IOException {
    Path hdfsPath = createHadoopPath(path);

    try {
      InputStream stream = hdfsFileSystem.open(hdfsPath);
      LOGGER.debug("Opened input stream for HDFS path: {}", hdfsPath);
      return stream;

    } catch (IOException e) {
      LOGGER.error("Failed to open input stream for HDFS path: {}", hdfsPath, e);
      throw new IOException("Failed to open input stream for HDFS path: " + path, e);
    }
  }

  @Override public Reader openReader(String path) throws IOException {
    return new InputStreamReader(openInputStream(path), StandardCharsets.UTF_8);
  }

  @Override public boolean exists(String path) throws IOException {
    Path hdfsPath = createHadoopPath(path);

    try {
      boolean exists = hdfsFileSystem.exists(hdfsPath);
      LOGGER.debug("HDFS path exists check: {} -> {}", hdfsPath, exists);
      return exists;

    } catch (IOException e) {
      LOGGER.error("Failed to check existence of HDFS path: {}", hdfsPath, e);
      throw new IOException("Failed to check existence of HDFS path: " + path, e);
    }
  }

  @Override public boolean isDirectory(String path) throws IOException {
    Path hdfsPath = createHadoopPath(path);

    try {
      if (!hdfsFileSystem.exists(hdfsPath)) {
        return false;
      }

      FileStatus status = hdfsFileSystem.getFileStatus(hdfsPath);
      return status.isDirectory();

    } catch (IOException e) {
      LOGGER.error("Failed to check if HDFS path is directory: {}", hdfsPath, e);
      throw new IOException("Failed to check if HDFS path is directory: " + path, e);
    }
  }

  @Override public String getStorageType() {
    return "hdfs";
  }

  @Override public String resolvePath(String basePath, String relativePath) {
    // Handle HDFS URI resolution
    if (relativePath.startsWith("hdfs://")) {
      return relativePath; // Already absolute
    }

    // Remove hdfs:// prefix from base path for resolution, then add it back
    String basePathNormalized = basePath;
    String uriPrefix = "";
    
    if (basePath.startsWith("hdfs://")) {
      int pathStart = basePath.indexOf("/", 8); // Skip hdfs://host:port
      if (pathStart != -1) {
        uriPrefix = basePath.substring(0, pathStart);
        basePathNormalized = basePath.substring(pathStart);
      }
    }

    // Ensure base path ends with /
    if (!basePathNormalized.endsWith("/")) {
      basePathNormalized = basePathNormalized + "/";
    }

    // Remove leading / from relative path
    if (relativePath.startsWith("/")) {
      relativePath = relativePath.substring(1);
    }

    String resolved = basePathNormalized + relativePath;
    
    // Add back URI prefix if it existed
    if (!uriPrefix.isEmpty()) {
      resolved = uriPrefix + resolved;
    }

    return resolved;
  }

  @Override public void writeFile(String path, byte[] content) throws IOException {
    Path hdfsPath = createHadoopPath(path);

    try {
      // Create parent directories if they don't exist
      Path parent = hdfsPath.getParent();
      if (parent != null && !hdfsFileSystem.exists(parent)) {
        hdfsFileSystem.mkdirs(parent);
      }

      // Write content to HDFS
      try (OutputStream outputStream = hdfsFileSystem.create(hdfsPath, true);
           ByteArrayInputStream inputStream = new ByteArrayInputStream(content)) {
        
        byte[] buffer = new byte[8192];
        int bytesRead;
        while ((bytesRead = inputStream.read(buffer)) != -1) {
          outputStream.write(buffer, 0, bytesRead);
        }
      }

      LOGGER.debug("Wrote {} bytes to HDFS path: {}", content.length, hdfsPath);

    } catch (IOException e) {
      LOGGER.error("Failed to write file to HDFS path: {}", hdfsPath, e);
      throw new IOException("Failed to write file to HDFS path: " + path, e);
    }
  }

  @Override public void writeFile(String path, InputStream content) throws IOException {
    Path hdfsPath = createHadoopPath(path);

    try {
      // Create parent directories if they don't exist
      Path parent = hdfsPath.getParent();
      if (parent != null && !hdfsFileSystem.exists(parent)) {
        hdfsFileSystem.mkdirs(parent);
      }

      // Write content to HDFS
      try (OutputStream outputStream = hdfsFileSystem.create(hdfsPath, true)) {
        byte[] buffer = new byte[8192];
        int bytesRead;
        long totalBytes = 0;
        
        while ((bytesRead = content.read(buffer)) != -1) {
          outputStream.write(buffer, 0, bytesRead);
          totalBytes += bytesRead;
        }
        
        LOGGER.debug("Wrote {} bytes to HDFS path: {}", totalBytes, hdfsPath);
      }

    } catch (IOException e) {
      LOGGER.error("Failed to write file to HDFS path: {}", hdfsPath, e);
      throw new IOException("Failed to write file to HDFS path: " + path, e);
    }
  }

  @Override public void createDirectories(String path) throws IOException {
    Path hdfsPath = createHadoopPath(path);

    try {
      boolean created = hdfsFileSystem.mkdirs(hdfsPath);
      if (created) {
        LOGGER.debug("Created HDFS directories: {}", hdfsPath);
      } else {
        LOGGER.debug("HDFS directories already exist or failed to create: {}", hdfsPath);
      }

    } catch (IOException e) {
      LOGGER.error("Failed to create HDFS directories: {}", hdfsPath, e);
      throw new IOException("Failed to create HDFS directories: " + path, e);
    }
  }

  @Override public boolean delete(String path) throws IOException {
    Path hdfsPath = createHadoopPath(path);

    try {
      if (!hdfsFileSystem.exists(hdfsPath)) {
        return false; // File doesn't exist
      }

      boolean deleted = hdfsFileSystem.delete(hdfsPath, true); // recursive=true
      LOGGER.debug("Deleted HDFS path: {} -> {}", hdfsPath, deleted);
      return deleted;

    } catch (IOException e) {
      LOGGER.error("Failed to delete HDFS path: {}", hdfsPath, e);
      throw new IOException("Failed to delete HDFS path: " + path, e);
    }
  }

  @Override public void copyFile(String source, String destination) throws IOException {
    Path sourcePath = createHadoopPath(source);
    Path destPath = createHadoopPath(destination);

    try {
      // Create parent directories for destination
      Path destParent = destPath.getParent();
      if (destParent != null && !hdfsFileSystem.exists(destParent)) {
        hdfsFileSystem.mkdirs(destParent);
      }

      // Copy file using Hadoop's built-in copy utility
      org.apache.hadoop.fs.FileUtil.copy(hdfsFileSystem, sourcePath, 
                                        hdfsFileSystem, destPath, 
                                        false, hadoopConfig);

      LOGGER.debug("Copied HDFS file from {} to {}", sourcePath, destPath);

    } catch (IOException e) {
      LOGGER.error("Failed to copy HDFS file from {} to {}", sourcePath, destPath, e);
      throw new IOException("Failed to copy HDFS file from " + source + " to " + destination, e);
    }
  }

  /**
   * Gets the underlying Hadoop FileSystem for advanced operations.
   * 
   * @return The Hadoop FileSystem instance
   */
  public FileSystem getFileSystem() {
    return hdfsFileSystem;
  }

  /**
   * Gets the Hadoop configuration used by this provider.
   * 
   * @return The Hadoop Configuration instance
   */
  public Configuration getConfiguration() {
    return hadoopConfig;
  }

  /**
   * Creates a Hadoop Path from a string path, handling HDFS URIs.
   */
  private Path createHadoopPath(String path) {
    if (path.startsWith("hdfs://")) {
      return new Path(path);
    } else {
      // Relative path, resolve against default filesystem
      return new Path(path);
    }
  }

  /**
   * Creates a FileEntry from a Hadoop FileStatus.
   */
  private FileEntry createFileEntry(FileStatus status) {
    return new FileEntry(
        status.getPath().toString(),
        status.getPath().getName(),
        status.isDirectory(),
        status.getLen(),
        status.getModificationTime()
    );
  }

  /**
   * Infers content type from file extension.
   * Basic implementation - can be enhanced with more sophisticated MIME type detection.
   */
  private String inferContentType(String path) {
    String lowerPath = path.toLowerCase();
    if (lowerPath.endsWith(".json")) {
      return "application/json";
    } else if (lowerPath.endsWith(".csv")) {
      return "text/csv";
    } else if (lowerPath.endsWith(".parquet")) {
      return "application/octet-stream";
    } else if (lowerPath.endsWith(".txt")) {
      return "text/plain";
    } else if (lowerPath.endsWith(".xml")) {
      return "application/xml";
    } else {
      return "application/octet-stream";
    }
  }

  /**
   * Closes the HDFS filesystem connection.
   * Should be called when the storage provider is no longer needed.
   */
  public void close() throws IOException {
    if (hdfsFileSystem != null) {
      hdfsFileSystem.close();
      LOGGER.info("Closed HDFS filesystem connection");
    }
  }
}