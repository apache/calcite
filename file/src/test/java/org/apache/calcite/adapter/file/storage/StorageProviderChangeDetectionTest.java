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

import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for storage provider change detection functionality.
 */
public class StorageProviderChangeDetectionTest {

  @TempDir
  File tempDir;

  @Test void testLocalFileChangeDetection() throws IOException, InterruptedException {
    File testFile = new File(tempDir, "test.csv");
    LocalFileStorageProvider provider = new LocalFileStorageProvider();

    // Create initial file
    try (FileWriter writer = new FileWriter(testFile, StandardCharsets.UTF_8)) {
      writer.write("id,name\n1,Alice\n2,Bob\n");
    }

    // Get initial metadata
    StorageProvider.FileMetadata initialMetadata = provider.getMetadata(testFile.getAbsolutePath());

    // Check no change
    assertFalse(provider.hasChanged(testFile.getAbsolutePath(), initialMetadata),
        "File should not be detected as changed immediately after reading metadata");

    // Wait a bit to ensure timestamp difference
    Thread.sleep(1100);

    // Modify file
    try (FileWriter writer = new FileWriter(testFile, StandardCharsets.UTF_8)) {
      writer.write("id,name\n1,Alice\n2,Bob\n3,Charlie\n");
    }

    // Check change detected
    assertTrue(provider.hasChanged(testFile.getAbsolutePath(), initialMetadata),
        "File should be detected as changed after modification");

    // Get new metadata
    StorageProvider.FileMetadata newMetadata = provider.getMetadata(testFile.getAbsolutePath());

    // Check no change with updated metadata
    assertFalse(provider.hasChanged(testFile.getAbsolutePath(), newMetadata),
        "File should not be detected as changed when compared with current metadata");
  }

  @Test void testChangeDetectionWithEtag() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();

    // Test with simulated metadata that includes ETags
    StorageProvider.FileMetadata metadata1 =
        new StorageProvider.FileMetadata("/test/file.csv", 1000, System.currentTimeMillis(),
        "text/csv", "etag-12345");

    StorageProvider.FileMetadata metadata2 =
        new StorageProvider.FileMetadata("/test/file.csv", 1000, System.currentTimeMillis(),
        "text/csv", "etag-67890");

    // Create a mock provider that uses our test metadata
    StorageProvider mockProvider = new StorageProvider() {
      private StorageProvider.FileMetadata currentMetadata = metadata2;

      @Override public List<FileEntry> listFiles(String path, boolean recursive) {
        throw new UnsupportedOperationException();
      }

      @Override public FileMetadata getMetadata(String path) {
        return currentMetadata;
      }

      @Override public java.io.InputStream openInputStream(String path) {
        throw new UnsupportedOperationException();
      }

      @Override public java.io.Reader openReader(String path) {
        throw new UnsupportedOperationException();
      }

      @Override public boolean exists(String path) {
        return true;
      }

      @Override public boolean isDirectory(String path) {
        return false;
      }

      @Override public String getStorageType() {
        return "mock";
      }

      @Override public String resolvePath(String basePath, String relativePath) {
        return basePath + "/" + relativePath;
      }
    };

    // ETags differ, so change should be detected
    assertTrue(mockProvider.hasChanged("/test/file.csv", metadata1),
        "Change should be detected when ETags differ");
  }

  @Test void testChangeDetectionEdgeCases() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    File testFile = new File(tempDir, "test.json");

    // Test with null cached metadata
    assertTrue(provider.hasChanged(testFile.getAbsolutePath(), null),
        "Should return true when cached metadata is null");

    // Test with non-existent file
    StorageProvider.FileMetadata fakeMetadata =
        new StorageProvider.FileMetadata(testFile.getAbsolutePath(), 100, System.currentTimeMillis(),
        "application/json", null);

    assertTrue(provider.hasChanged(testFile.getAbsolutePath(), fakeMetadata),
        "Should return true when file doesn't exist");
  }
}
