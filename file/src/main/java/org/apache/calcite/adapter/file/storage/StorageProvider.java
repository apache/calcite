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

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.List;

/**
 * Storage provider interface for abstracting file access across different storage systems.
 * Implementations can provide access to local files, S3, HTTP, SharePoint, etc.
 */
public interface StorageProvider {

  /**
   * Lists files in a directory or container.
   *
   * @param path The directory or container path
   * @param recursive Whether to include subdirectories
   * @return List of file entries
   * @throws IOException If an I/O error occurs
   */
  List<FileEntry> listFiles(String path, boolean recursive) throws IOException;

  /**
   * Gets metadata for a single file.
   *
   * @param path The file path
   * @return File metadata
   * @throws IOException If an I/O error occurs
   */
  FileMetadata getMetadata(String path) throws IOException;

  /**
   * Opens an input stream for reading file content.
   *
   * @param path The file path
   * @return Input stream for the file
   * @throws IOException If an I/O error occurs
   */
  InputStream openInputStream(String path) throws IOException;

  /**
   * Opens a reader for reading text file content.
   *
   * @param path The file path
   * @return Reader for the file
   * @throws IOException If an I/O error occurs
   */
  Reader openReader(String path) throws IOException;

  /**
   * Checks if a path exists.
   *
   * @param path The path to check
   * @return true if the path exists
   * @throws IOException If an I/O error occurs
   */
  boolean exists(String path) throws IOException;

  /**
   * Checks if a path is a directory.
   *
   * @param path The path to check
   * @return true if the path is a directory
   * @throws IOException If an I/O error occurs
   */
  boolean isDirectory(String path) throws IOException;

  /**
   * Gets the storage type identifier.
   *
   * @return Storage type (e.g., "local", "s3", "http")
   */
  String getStorageType();

  /**
   * Resolves a relative path against a base path.
   *
   * @param basePath The base path
   * @param relativePath The relative path
   * @return The resolved absolute path
   */
  String resolvePath(String basePath, String relativePath);

  /**
   * Writes content to a file.
   * Creates the file if it doesn't exist, overwrites if it does.
   *
   * @param path The file path
   * @param content The content to write
   * @throws IOException If an I/O error occurs
   * @throws UnsupportedOperationException If this storage provider is read-only
   */
  default void writeFile(String path, byte[] content) throws IOException {
    throw new UnsupportedOperationException(
        "Write operations are not supported by " + getStorageType() + " storage provider");
  }

  /**
   * Writes content from an input stream to a file.
   * Creates the file if it doesn't exist, overwrites if it does.
   *
   * @param path The file path
   * @param content The input stream to write from
   * @throws IOException If an I/O error occurs
   * @throws UnsupportedOperationException If this storage provider is read-only
   */
  default void writeFile(String path, InputStream content) throws IOException {
    throw new UnsupportedOperationException(
        "Write operations are not supported by " + getStorageType() + " storage provider");
  }

  /**
   * Creates directories for the given path.
   * Creates parent directories as needed.
   *
   * @param path The directory path to create
   * @throws IOException If an I/O error occurs
   * @throws UnsupportedOperationException If this storage provider is read-only
   */
  default void createDirectories(String path) throws IOException {
    throw new UnsupportedOperationException(
        "Directory creation is not supported by " + getStorageType() + " storage provider");
  }

  /**
   * Deletes a file or directory.
   *
   * @param path The path to delete
   * @return true if the file was deleted, false if it didn't exist
   * @throws IOException If an I/O error occurs
   * @throws UnsupportedOperationException If this storage provider is read-only
   */
  default boolean delete(String path) throws IOException {
    throw new UnsupportedOperationException(
        "Delete operations are not supported by " + getStorageType() + " storage provider");
  }

  /**
   * Copies a file from source to destination.
   *
   * @param source The source file path
   * @param destination The destination file path
   * @throws IOException If an I/O error occurs
   * @throws UnsupportedOperationException If this storage provider is read-only
   */
  default void copyFile(String source, String destination) throws IOException {
    throw new UnsupportedOperationException(
        "Copy operations are not supported by " + getStorageType() + " storage provider");
  }

  /**
   * Checks if a file has changed compared to cached metadata.
   * This method helps avoid unnecessary updates by comparing current file state
   * with previously cached metadata.
   *
   * @param path The path to the file
   * @param cachedMetadata Previously cached metadata to compare against
   * @return true if the file has changed, false if it's the same
   * @throws IOException if an I/O error occurs
   */
  default boolean hasChanged(String path, FileMetadata cachedMetadata) throws IOException {
    if (cachedMetadata == null) {
      return true; // No cached data, assume changed
    }

    try {
      FileMetadata currentMetadata = getMetadata(path);

      // Compare size first (quick check)
      if (currentMetadata.getSize() != cachedMetadata.getSize()) {
        return true;
      }

      // Compare ETags if available (most reliable for S3, HTTP)
      if (currentMetadata.getEtag() != null && cachedMetadata.getEtag() != null) {
        return !currentMetadata.getEtag().equals(cachedMetadata.getEtag());
      }

      // Compare last modified times
      // Allow small time differences (< 1 second) to handle filesystem precision issues
      long timeDiff = Math.abs(currentMetadata.getLastModified()
          - cachedMetadata.getLastModified());
      return timeDiff > 1000;

    } catch (IOException e) {
      // If we can't get current metadata, assume changed
      return true;
    }
  }

  /**
   * File entry representing a file in a directory listing.
   */
  class FileEntry {
    private final String path;
    private final String name;
    private final boolean isDirectory;
    private final long size;
    private final long lastModified;

    public FileEntry(String path, String name, boolean isDirectory,
                     long size, long lastModified) {
      this.path = path;
      this.name = name;
      this.isDirectory = isDirectory;
      this.size = size;
      this.lastModified = lastModified;
    }

    public String getPath() {
      return path;
    }

    public String getName() {
      return name;
    }

    public boolean isDirectory() {
      return isDirectory;
    }

    public long getSize() {
      return size;
    }

    public long getLastModified() {
      return lastModified;
    }
  }

  /**
   * File metadata containing detailed information about a file.
   */
  class FileMetadata {
    private final String path;
    private final long size;
    private final long lastModified;
    private final String contentType;
    private final String etag;

    public FileMetadata(String path, long size, long lastModified,
                        String contentType, String etag) {
      this.path = path;
      this.size = size;
      this.lastModified = lastModified;
      this.contentType = contentType;
      this.etag = etag;
    }

    public String getPath() {
      return path;
    }

    public long getSize() {
      return size;
    }

    public long getLastModified() {
      return lastModified;
    }

    public String getContentType() {
      return contentType;
    }

    public String getEtag() {
      return etag;
    }
  }
}
