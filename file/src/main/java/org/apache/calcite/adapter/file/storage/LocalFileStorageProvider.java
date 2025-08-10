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
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Storage provider implementation for local filesystem access.
 */
public class LocalFileStorageProvider implements StorageProvider {

  @Override public List<FileEntry> listFiles(String path, boolean recursive) throws IOException {
    List<FileEntry> entries = new ArrayList<>();
    File dir = new File(path);

    if (!dir.exists() || !dir.isDirectory()) {
      throw new IOException("Directory does not exist: " + path);
    }

    listFilesRecursive(dir, dir, entries, recursive);
    return entries;
  }

  private void listFilesRecursive(File baseDir, File currentDir,
                                  List<FileEntry> entries, boolean recursive) {
    File[] files = currentDir.listFiles();
    if (files != null) {
      for (File file : files) {
        String relativePath = baseDir.toPath().relativize(file.toPath()).toString();
        entries.add(
            new FileEntry(
            file.getAbsolutePath(),
            file.getName(),
            file.isDirectory(),
            file.isFile() ? file.length() : 0,
            file.lastModified()));

        if (recursive && file.isDirectory()) {
          listFilesRecursive(baseDir, file, entries, true);
        }
      }
    }
  }

  @Override public FileMetadata getMetadata(String path) throws IOException {
    File file = new File(path);
    if (!file.exists()) {
      throw new IOException("File does not exist: " + path);
    }

    String contentType = Files.probeContentType(file.toPath());
    if (contentType == null) {
      contentType = "application/octet-stream";
    }

    return new FileMetadata(
        file.getAbsolutePath(),
        file.length(),
        file.lastModified(),
        contentType,
        null); // Local files don't have ETags
  }

  @Override public InputStream openInputStream(String path) throws IOException {
    // Handle file:// URLs
    if (path.startsWith("file:")) {
      try {
        java.net.URI uri = new java.net.URI(path);
        File file = new File(uri);
        if (!file.exists()) {
          throw new IOException("File does not exist: " + path);
        }
        return new FileInputStream(file);
      } catch (java.net.URISyntaxException e) {
        throw new IOException("Invalid file URI: " + path, e);
      }
    }
    
    File file = new File(path);
    if (!file.exists()) {
      throw new IOException("File does not exist: " + path);
    }
    return new FileInputStream(file);
  }

  @Override public Reader openReader(String path) throws IOException {
    // Handle file:// URLs
    if (path.startsWith("file:")) {
      try {
        java.net.URI uri = new java.net.URI(path);
        File file = new File(uri);
        if (!file.exists()) {
          throw new IOException("File does not exist: " + path);
        }
        return new FileReader(file, StandardCharsets.UTF_8);
      } catch (java.net.URISyntaxException e) {
        throw new IOException("Invalid file URI: " + path, e);
      }
    }
    
    File file = new File(path);
    if (!file.exists()) {
      throw new IOException("File does not exist: " + path);
    }
    return new FileReader(file, StandardCharsets.UTF_8);
  }

  @Override public boolean exists(String path) {
    if (path.startsWith("file:")) {
      try {
        java.net.URI uri = new java.net.URI(path);
        return new File(uri).exists();
      } catch (java.net.URISyntaxException e) {
        return false;
      }
    }
    return new File(path).exists();
  }

  @Override public boolean isDirectory(String path) {
    if (path.startsWith("file:")) {
      try {
        java.net.URI uri = new java.net.URI(path);
        return new File(uri).isDirectory();
      } catch (java.net.URISyntaxException e) {
        return false;
      }
    }
    return new File(path).isDirectory();
  }

  @Override public String getStorageType() {
    return "local";
  }

  @Override public String resolvePath(String basePath, String relativePath) {
    if (relativePath == null || relativePath.isEmpty()) {
      return basePath;
    }

    // If relative path is actually absolute, return it
    Path relPath = Paths.get(relativePath);
    if (relPath.isAbsolute()) {
      return relativePath;
    }

    // Otherwise resolve against base
    Path base = Paths.get(basePath);
    if (base.toFile().isFile()) {
      base = base.getParent();
    }

    return base.resolve(relativePath).normalize().toString();
  }
}
