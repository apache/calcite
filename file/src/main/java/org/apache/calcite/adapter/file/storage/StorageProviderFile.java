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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * A File wrapper that provides unified interface for both local files 
 * and remote storage files accessed through a StorageProvider.
 */
public class StorageProviderFile extends File {
  private final StorageProvider.FileEntry fileEntry;
  private final StorageProvider storageProvider;
  private final boolean isLocal;

  // Constructor for local files
  private StorageProviderFile(String path) {
    super(path);
    this.fileEntry = null;
    this.storageProvider = null;
    this.isLocal = true;
  }

  // Constructor for remote files
  private StorageProviderFile(StorageProvider.FileEntry fileEntry, StorageProvider storageProvider) {
    super(createTempPath(fileEntry));
    this.fileEntry = fileEntry;
    this.storageProvider = storageProvider;
    this.isLocal = false;
  }

  private static String createTempPath(StorageProvider.FileEntry fileEntry) {
    String path = fileEntry.getPath();
    if (!path.startsWith("/")) {
      path = "/" + path;
    }
    return path;
  }

  @Override
  public boolean exists() {
    if (isLocal) {
      return super.exists();
    } else {
      try {
        return storageProvider.exists(fileEntry.getPath());
      } catch (IOException e) {
        return false;
      }
    }
  }

  @Override
  public long lastModified() {
    if (isLocal) {
      return super.lastModified();
    } else {
      return fileEntry.getLastModified();
    }
  }

  @Override
  public long length() {
    if (isLocal) {
      return super.length();
    } else {
      return fileEntry.getSize();
    }
  }

  @Override
  public boolean delete() {
    if (isLocal) {
      return super.delete();
    } else {
      try {
        return storageProvider.delete(fileEntry.getPath());
      } catch (IOException e) {
        return false;
      }
    }
  }

  @Override
  public boolean mkdirs() {
    if (isLocal) {
      return super.mkdirs();
    } else {
      try {
        String parent = getParent();
        if (parent != null) {
          storageProvider.createDirectories(parent);
        }
        return true;
      } catch (IOException e) {
        return false;
      }
    }
  }

  public void ensureParentDirs() throws IOException {
    if (isLocal) {
      File parent = getParentFile();
      if (parent != null && !parent.exists()) {
        parent.mkdirs();
      }
    } else {
      String parent = getParent();
      if (parent != null) {
        storageProvider.createDirectories(parent);
      }
    }
  }

  public InputStream openInputStream() throws IOException {
    if (isLocal) {
      return java.nio.file.Files.newInputStream(toPath());
    } else {
      return storageProvider.openInputStream(fileEntry.getPath());
    }
  }

  public void writeBytes(byte[] content) throws IOException {
    if (isLocal) {
      ensureParentDirs();
      java.nio.file.Files.write(toPath(), content);
    } else {
      storageProvider.writeFile(fileEntry.getPath(), content);
    }
  }

  public void writeInputStream(InputStream input) throws IOException {
    if (isLocal) {
      ensureParentDirs();
      try (java.io.FileOutputStream output = new java.io.FileOutputStream(this)) {
        byte[] buffer = new byte[8192];
        int bytesRead;
        while ((bytesRead = input.read(buffer)) != -1) {
          output.write(buffer, 0, bytesRead);
        }
      }
    } else {
      storageProvider.writeFile(fileEntry.getPath(), input);
    }
  }

  public boolean isLocal() {
    return isLocal;
  }

  public File getFile() {
    if (!isLocal) {
      throw new UnsupportedOperationException("Remote storage file cannot be converted to File object: " + getPath());
    }
    return this;
  }

  public StorageProvider getStorageProvider() {
    return storageProvider;
  }

  public StorageProvider.FileEntry getFileEntry() {
    return fileEntry;
  }

  /**
   * Create a StorageProviderFile for the given path using the appropriate storage provider.
   */
  public static StorageProviderFile create(String path, StorageProvider storageProvider) {
    if (storageProvider instanceof LocalFileStorageProvider || 
        (path.startsWith("/") && !path.startsWith("hdfs://")) || 
        path.matches("^[A-Za-z]:.*")) {
      // Local filesystem path
      return new StorageProviderFile(path);
    } else {
      throw new UnsupportedOperationException("Use constructor with FileEntry for remote files");
    }
  }

  /**
   * Create a StorageProviderFile for remote storage.
   */
  public static StorageProviderFile create(StorageProvider.FileEntry fileEntry, StorageProvider storageProvider) {
    return new StorageProviderFile(fileEntry, storageProvider);
  }
}
