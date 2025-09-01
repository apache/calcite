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

import org.apache.calcite.util.Source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;
import java.util.Optional;

/**
 * A Source implementation that reads from a StorageProvider.
 * This allows remote files to be used wherever Source objects are expected.
 */
public class StorageProviderSource implements Source {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageProviderSource.class);
  private final StorageProvider.FileEntry fileEntry;
  private final StorageProvider storageProvider;
  private final String displayPath;  // Path for display/relative calculations
  private final String actualPath;   // Actual path for file operations

  /**
   * Creates a StorageProviderSource.
   *
   * @param fileEntry The file entry from the storage provider
   * @param storageProvider The storage provider for accessing the file
   */
  public StorageProviderSource(StorageProvider.FileEntry fileEntry,
                               StorageProvider storageProvider) {
    this.fileEntry = fileEntry;
    this.storageProvider = storageProvider;
    this.displayPath = fileEntry.getPath();
    this.actualPath = fileEntry.getPath();
  }

  /**
   * Creates a StorageProviderSource with separate display and actual paths.
   * Used for relative path calculations while preserving file access.
   *
   * @param fileEntry The file entry from the storage provider (for display)
   * @param actualPath The actual path to use for file operations
   * @param storageProvider The storage provider for accessing the file
   */
  private StorageProviderSource(StorageProvider.FileEntry fileEntry,
                                String actualPath,
                                StorageProvider storageProvider) {
    this.fileEntry = fileEntry;
    this.storageProvider = storageProvider;
    this.displayPath = fileEntry.getPath();
    this.actualPath = actualPath;
  }

  @Override public URL url() {
    // Storage provider files don't have regular URLs
    // Return null since these aren't accessible via standard URLs
    return null;
  }

  @Override public String path() {
    return displayPath;
  }

  @Override public InputStream openStream() throws IOException {
    LOGGER.debug("Opening stream for actualPath: {}, displayPath: {}, storageProvider: {}",
        actualPath, displayPath, storageProvider.getClass().getSimpleName());
    return storageProvider.openInputStream(actualPath);
  }

  @Override public Reader reader() throws IOException {
    LOGGER.debug("Opening reader for actualPath: {}, displayPath: {}, storageProvider: {}",
        actualPath, displayPath, storageProvider.getClass().getSimpleName());
    return storageProvider.openReader(actualPath);
  }

  @Override public String protocol() {
    return storageProvider.getStorageType();
  }

  @Override public Source append(Source child) {
    // Append the child path to this path
    String basePath = fileEntry.getPath();
    String childPath = child.path();

    // Remove leading slash from child if present
    if (childPath.startsWith("/")) {
      childPath = childPath.substring(1);
    }

    // Ensure base path ends with /
    if (!basePath.endsWith("/")) {
      basePath = basePath + "/";
    }

    String combinedPath = basePath + childPath;

    // Create a new FileEntry for the combined path
    StorageProvider.FileEntry newEntry =
        new StorageProvider.FileEntry(combinedPath,
        child.path().substring(child.path().lastIndexOf('/') + 1),
        false, 0, 0);

    return new StorageProviderSource(newEntry, storageProvider);
  }

  @Override public Source relative(Source source) {
    // Calculate relative path for display only
    String basePath = source.path();
    String thisPath = displayPath;

    // Normalize paths for comparison
    if (basePath.startsWith("/") && !thisPath.startsWith("/")) {
      basePath = basePath.substring(1);
    } else if (!basePath.startsWith("/") && thisPath.startsWith("/")) {
      thisPath = thisPath.substring(1);
    }

    if (thisPath.startsWith(basePath)) {
      String relativePath = thisPath.substring(basePath.length());
      if (relativePath.startsWith("/")) {
        relativePath = relativePath.substring(1);
      }

      // Create a new entry with relative path for display only
      // IMPORTANT: Keep the ORIGINAL actualPath for file operations!
      StorageProvider.FileEntry relativeEntry =
          new StorageProvider.FileEntry(relativePath,
          fileEntry.getName(),
          fileEntry.isDirectory(),
          fileEntry.getSize(),
          fileEntry.getLastModified());

      // Pass the ORIGINAL actualPath, not the relative one
      return new StorageProviderSource(relativeEntry, this.actualPath, storageProvider);
    }

    return this;
  }

  @Override public Source trim(String suffix) {
    String path = displayPath;
    if (path.endsWith(suffix)) {
      String trimmedPath = path.substring(0, path.length() - suffix.length());
      String trimmedName = fileEntry.getName();
      if (trimmedName.endsWith(suffix)) {
        trimmedName = trimmedName.substring(0, trimmedName.length() - suffix.length());
      }

      StorageProvider.FileEntry trimmedEntry =
          new StorageProvider.FileEntry(trimmedPath,
          trimmedName,
          fileEntry.isDirectory(),
          fileEntry.getSize(),
          fileEntry.getLastModified());

      // Keep the actual path unchanged for file operations
      return new StorageProviderSource(trimmedEntry, actualPath, storageProvider);
    }
    return this;
  }

  @Override public Source trimOrNull(String suffix) {
    String path = displayPath;
    if (path.endsWith(suffix)) {
      return trim(suffix);
    }
    return null;
  }

  /**
   * Gets the storage provider for this source.
   */
  public StorageProvider getStorageProvider() {
    return storageProvider;
  }

  /**
   * Gets the file entry for this source.
   */
  public StorageProvider.FileEntry getFileEntry() {
    return fileEntry;
  }

  @Override public Optional<File> fileOpt() {
    // Storage provider files don't have local File representations
    // Return a StorageProviderFile for compatibility
    return Optional.of(new StorageProviderFile(fileEntry, storageProvider));
  }

  @Override public File file() {
    // Return a StorageProviderFile for compatibility
    return new StorageProviderFile(fileEntry, storageProvider);
  }
}
