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

import org.apache.calcite.adapter.file.storage.StorageProvider;

import java.io.File;
import java.io.IOException;
import java.net.URI;

/**
 * A File wrapper for remote files accessed through a StorageProvider.
 * This class allows remote files to be used in places where File objects are expected.
 * Note that most File operations will not work correctly since the file doesn't exist locally.
 * The main purpose is to provide a path and name for the file.
 */
public class StorageProviderFile extends File {
  private final StorageProvider.FileEntry fileEntry;
  private final StorageProvider storageProvider;

  /**
   * Creates a StorageProviderFile that represents a remote file.
   *
   * @param fileEntry The file entry from the storage provider
   * @param storageProvider The storage provider for accessing the file
   */
  public StorageProviderFile(StorageProvider.FileEntry fileEntry, StorageProvider storageProvider) {
    super(createTempPath(fileEntry));
    this.fileEntry = fileEntry;
    this.storageProvider = storageProvider;
  }

  /**
   * Creates a temporary path for the file.
   * This path doesn't actually exist on disk but provides a valid path string.
   */
  private static String createTempPath(StorageProvider.FileEntry fileEntry) {
    // Use the path from the file entry
    String path = fileEntry.getPath();
    // Ensure it's an absolute path
    if (!path.startsWith("/")) {
      path = "/" + path;
    }
    return path;
  }

  @Override
  public String getName() {
    return fileEntry.getName();
  }

  @Override
  public String getPath() {
    return fileEntry.getPath();
  }

  @Override
  public String getAbsolutePath() {
    String path = fileEntry.getPath();
    if (!path.startsWith("/")) {
      path = "/" + path;
    }
    return path;
  }

  @Override
  public String getCanonicalPath() throws IOException {
    return getAbsolutePath();
  }

  @Override
  public File getCanonicalFile() throws IOException {
    return this;
  }

  @Override
  public boolean exists() {
    try {
      return storageProvider.exists(fileEntry.getPath());
    } catch (IOException e) {
      return false;
    }
  }

  @Override
  public boolean isFile() {
    return !fileEntry.isDirectory();
  }

  @Override
  public boolean isDirectory() {
    return fileEntry.isDirectory();
  }

  @Override
  public long length() {
    return fileEntry.getSize();
  }

  @Override
  public long lastModified() {
    return fileEntry.getLastModified();
  }

  @Override
  public boolean canRead() {
    return true; // Assume readable if listed by storage provider
  }

  @Override
  public boolean canWrite() {
    return false; // Storage providers are read-only
  }

  @Override
  public boolean canExecute() {
    return false;
  }

  @Override
  public URI toURI() {
    try {
      // Create a custom URI for the storage provider file
      String scheme = storageProvider.getStorageType();
      String path = fileEntry.getPath();
      if (!path.startsWith("/")) {
        path = "/" + path;
      }
      return new URI(scheme, null, path, null);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create URI for storage provider file: " + e.getMessage(), e);
    }
  }

  /**
   * Gets the storage provider for this file.
   */
  public StorageProvider getStorageProvider() {
    return storageProvider;
  }

  /**
   * Gets the file entry for this file.
   */
  public StorageProvider.FileEntry getFileEntry() {
    return fileEntry;
  }
}