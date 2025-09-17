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
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
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

  @Override public void writeFile(String path, byte[] content) throws IOException {
    Path filePath = parsePath(path);
    
    // Create parent directories if they don't exist
    Path parentDir = filePath.getParent();
    if (parentDir != null && !Files.exists(parentDir)) {
      Files.createDirectories(parentDir);
    }
    
    // Write the file, creating or overwriting as needed
    Files.write(filePath, content, 
        StandardOpenOption.CREATE, 
        StandardOpenOption.TRUNCATE_EXISTING,
        StandardOpenOption.WRITE);
  }

  @Override public void writeFile(String path, InputStream content) throws IOException {
    Path filePath = parsePath(path);
    
    // Create parent directories if they don't exist
    Path parentDir = filePath.getParent();
    if (parentDir != null && !Files.exists(parentDir)) {
      Files.createDirectories(parentDir);
    }
    
    // Copy the input stream to the file
    try (OutputStream out = Files.newOutputStream(filePath,
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING,
        StandardOpenOption.WRITE)) {
      byte[] buffer = new byte[8192];
      int bytesRead;
      while ((bytesRead = content.read(buffer)) != -1) {
        out.write(buffer, 0, bytesRead);
      }
    }
  }

  @Override public void createDirectories(String path) throws IOException {
    Path dirPath = parsePath(path);
    Files.createDirectories(dirPath);
  }

  @Override public boolean delete(String path) throws IOException {
    Path filePath = parsePath(path);
    if (!Files.exists(filePath)) {
      return false;
    }
    
    // If it's a directory, ensure it's empty before deleting
    if (Files.isDirectory(filePath)) {
      // For now, only delete empty directories
      // For recursive delete, would need to walk the tree
      Files.delete(filePath);
    } else {
      Files.delete(filePath);
    }
    return true;
  }

  @Override public void copyFile(String source, String destination) throws IOException {
    Path sourcePath = parsePath(source);
    Path destPath = parsePath(destination);
    
    if (!Files.exists(sourcePath)) {
      throw new IOException("Source file does not exist: " + source);
    }
    
    // Create parent directories for destination if needed
    Path parentDir = destPath.getParent();
    if (parentDir != null && !Files.exists(parentDir)) {
      Files.createDirectories(parentDir);
    }
    
    // Copy the file, replacing if it exists
    Files.copy(sourcePath, destPath, StandardCopyOption.REPLACE_EXISTING);
  }

  @Override
  public void cleanupMacosMetadata(String directoryPath) throws IOException {
    File dir = new File(directoryPath);
    if (!dir.exists() || !dir.isDirectory()) {
      return; // Nothing to clean up
    }
    
    // Define patterns for files to clean up
    String[] patterns = {
        "._",        // macOS resource forks
        ".DS_Store", // macOS directory metadata
        ".crc",      // Hadoop checksum files (suffix check)
        "~",         // Backup files (suffix check)
        ".tmp"       // Temporary files (suffix check)
    };
    
    cleanupDirectory(dir, patterns);
  }
  
  /**
   * Recursively clean up unwanted metadata files in directory.
   */
  private void cleanupDirectory(File dir, String[] patterns) throws IOException {
    File[] files = dir.listFiles();
    if (files == null) {
      return;
    }
    
    for (File file : files) {
      if (file.isDirectory()) {
        // Recursively clean subdirectories
        cleanupDirectory(file, patterns);
      } else {
        // Check if file matches any cleanup pattern
        String fileName = file.getName();
        boolean shouldDelete = false;
        
        for (String pattern : patterns) {
          if (pattern.equals(".DS_Store") && fileName.equals(".DS_Store")) {
            shouldDelete = true;
            break;
          } else if (pattern.equals("._") && fileName.startsWith("._")) {
            shouldDelete = true;
            break;
          } else if (pattern.equals(".crc") && fileName.endsWith(".crc")) {
            shouldDelete = true;
            break;
          } else if (pattern.equals("~") && fileName.endsWith("~")) {
            shouldDelete = true;
            break;
          } else if (pattern.equals(".tmp") && fileName.endsWith(".tmp")) {
            shouldDelete = true;
            break;
          }
        }
        
        if (shouldDelete) {
          try {
            Files.delete(file.toPath());
          } catch (IOException e) {
            // Log but don't fail the whole operation for individual file cleanup issues
            System.err.println("Warning: Could not delete metadata file " + file.getPath() + ": " + e.getMessage());
          }
        }
      }
    }
  }

  /**
   * Helper method to parse a path string that might be a file:// URL.
   */
  private Path parsePath(String path) throws IOException {
    if (path.startsWith("file:")) {
      try {
        java.net.URI uri = new java.net.URI(path);
        return Paths.get(uri);
      } catch (java.net.URISyntaxException e) {
        throw new IOException("Invalid file URI: " + path, e);
      }
    }
    return Paths.get(path);
  }
}
