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
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Tag;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Tag;
/**
 * Test for storage provider implementations.
 */
@Tag("unit")public class StorageProviderTest {

  @TempDir
  File tempDir;

  @Test void testLocalFileStorageProvider() throws IOException {
    // Create test file
    File testFile = new File(tempDir, "test.txt");
    try (FileWriter writer = new FileWriter(testFile, StandardCharsets.UTF_8)) {
      writer.write("Hello, Storage Provider!");
    }

    StorageProvider provider = new LocalFileStorageProvider();

    // Test exists
    assertTrue(provider.exists(testFile.getAbsolutePath()));
    assertFalse(provider.exists(new File(tempDir, "nonexistent.txt").getAbsolutePath()));

    // Test isDirectory
    assertTrue(provider.isDirectory(tempDir.getAbsolutePath()));
    assertFalse(provider.isDirectory(testFile.getAbsolutePath()));

    // Test metadata
    StorageProvider.FileMetadata metadata = provider.getMetadata(testFile.getAbsolutePath());
    assertEquals(testFile.getAbsolutePath(), metadata.getPath());
    assertEquals(24, metadata.getSize()); // "Hello, Storage Provider!" is 24 bytes
    assertTrue(metadata.getLastModified() > 0);

    // Test reading
    try (InputStream is = provider.openInputStream(testFile.getAbsolutePath())) {
      byte[] content = is.readAllBytes();
      assertEquals("Hello, Storage Provider!", new String(content, StandardCharsets.UTF_8));
    }

    try (Reader reader = provider.openReader(testFile.getAbsolutePath())) {
      StringBuilder sb = new StringBuilder();
      int ch;
      while ((ch = reader.read()) != -1) {
        sb.append((char) ch);
      }
      assertEquals("Hello, Storage Provider!", sb.toString());
    }

    // Test listing files
    List<StorageProvider.FileEntry> entries = provider.listFiles(tempDir.getAbsolutePath(), false);
    assertEquals(1, entries.size());
    assertEquals("test.txt", entries.get(0).getName());
    assertFalse(entries.get(0).isDirectory());
  }

  @Test void testStorageProviderFactory() {
    // Test local file
    StorageProvider local1 = StorageProviderFactory.createFromUrl("/path/to/file.txt");
    assertEquals("local", local1.getStorageType());

    StorageProvider local2 = StorageProviderFactory.createFromUrl("file:///path/to/file.txt");
    assertEquals("local", local2.getStorageType());

    // Test HTTP
    StorageProvider http = StorageProviderFactory.createFromUrl("http://example.com/file.txt");
    assertEquals("http", http.getStorageType());

    StorageProvider https = StorageProviderFactory.createFromUrl("https://example.com/file.txt");
    assertEquals("http", https.getStorageType());

    // Test S3
    StorageProvider s3 = StorageProviderFactory.createFromUrl("s3://bucket/path/file.txt");
    assertEquals("s3", s3.getStorageType());

    // Test FTP
    StorageProvider ftp = StorageProviderFactory.createFromUrl("ftp://server/path/file.txt");
    assertEquals("ftp", ftp.getStorageType());
  }

  @Test void testStorageProviderFactoryByType() {
    // Test local
    StorageProvider local = StorageProviderFactory.createFromType("local", null);
    assertEquals("local", local.getStorageType());

    // Test SharePoint (requires config)
    Map<String, Object> config = new HashMap<>();
    config.put("siteUrl", "https://example.sharepoint.com");
    config.put("accessToken", "dummy-token");

    StorageProvider sharepoint = StorageProviderFactory.createFromType("sharepoint", config);
    assertEquals("sharepoint-rest", sharepoint.getStorageType());
  }

  @Test void testPathResolution() {
    StorageProvider provider = new LocalFileStorageProvider();

    // Test absolute path
    assertEquals("/absolute/path.txt",
        provider.resolvePath("/base/dir", "/absolute/path.txt"));

    // Test relative path
    assertEquals("/base/dir/relative.txt",
        provider.resolvePath("/base/dir", "relative.txt"));

    // Test relative with subdirectory
    assertEquals("/base/dir/sub/file.txt",
        provider.resolvePath("/base/dir", "sub/file.txt"));

    // Test base is file (current implementation doesn't check if it's a file)
    assertEquals("/base/file.txt/relative.txt",
        provider.resolvePath("/base/file.txt", "relative.txt"));
  }
}
