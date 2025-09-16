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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for HDFSStorageProvider using embedded HDFS cluster.
 * These tests require more time and resources, so they're tagged as integration tests.
 */
@Tag("integration")
public class HDFSStorageProviderIntegrationTest {

  private MiniDFSCluster miniCluster;
  private Configuration conf;
  private FileSystem hdfs;
  private HDFSStorageProvider storageProvider;

  @BeforeEach
  public void setUp() throws IOException {
    // Configure embedded HDFS cluster
    conf = new Configuration();
    conf.set("dfs.nameservices", "test-cluster");
    conf.set("hadoop.security.authentication", "simple");
    conf.set("hadoop.security.authorization", "false");
    
    // Set up mini cluster in a temporary directory
    java.io.File baseDir = new java.io.File(System.getProperty("java.io.tmpdir"), "hdfs-test");
    baseDir.mkdirs();
    
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
    builder.nameNodePort(0); // Use random port
    builder.numDataNodes(1);
    builder.build();
    
    miniCluster = builder.build();
    miniCluster.waitActive();
    
    // Get filesystem and create storage provider
    hdfs = miniCluster.getFileSystem();
    conf = hdfs.getConf();
    
    storageProvider = new HDFSStorageProvider(hdfs, conf);
    
    // Create test directory structure
    hdfs.mkdirs(new Path("/test"));
    hdfs.mkdirs(new Path("/test/data"));
    hdfs.mkdirs(new Path("/test/empty"));
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (storageProvider != null) {
      storageProvider.close();
    }
    if (hdfs != null) {
      hdfs.close();
    }
    if (miniCluster != null) {
      miniCluster.shutdown();
    }
  }

  @Test
  public void testCreateAndReadFile() throws IOException {
    String testPath = "/test/data/sample.txt";
    String content = "Hello HDFS Integration Test!";
    
    // Write file
    storageProvider.writeFile(testPath, content.getBytes(StandardCharsets.UTF_8));
    
    // Verify file exists
    assertTrue(storageProvider.exists(testPath));
    assertFalse(storageProvider.isDirectory(testPath));
    
    // Read file content
    try (InputStream inputStream = storageProvider.openInputStream(testPath);
         ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      
      byte[] buffer = new byte[1024];
      int bytesRead;
      while ((bytesRead = inputStream.read(buffer)) != -1) {
        outputStream.write(buffer, 0, bytesRead);
      }
      
      String readContent = outputStream.toString(StandardCharsets.UTF_8.name());
      assertEquals(content, readContent);
    }
  }

  @Test
  public void testFileMetadata() throws IOException {
    String testPath = "/test/data/metadata.json";
    String jsonContent = "{\"name\":\"test\",\"value\":123}";
    
    storageProvider.writeFile(testPath, jsonContent.getBytes(StandardCharsets.UTF_8));
    
    StorageProvider.FileMetadata metadata = storageProvider.getMetadata(testPath);
    
    assertNotNull(metadata);
    assertEquals(testPath, metadata.getPath());
    assertEquals(jsonContent.length(), metadata.getSize());
    assertEquals("application/json", metadata.getContentType());
    assertNotNull(metadata.getEtag());
    assertTrue(metadata.getLastModified() > 0);
  }

  @Test
  public void testDirectoryOperations() throws IOException {
    String dirPath = "/test/new-directory";
    
    // Create directory
    storageProvider.createDirectories(dirPath);
    
    // Verify directory exists
    assertTrue(storageProvider.exists(dirPath));
    assertTrue(storageProvider.isDirectory(dirPath));
    
    // Create files in directory
    String file1Path = dirPath + "/file1.txt";
    String file2Path = dirPath + "/file2.csv";
    
    storageProvider.writeFile(file1Path, "Content 1".getBytes(StandardCharsets.UTF_8));
    storageProvider.writeFile(file2Path, "name,value\ntest,123".getBytes(StandardCharsets.UTF_8));
    
    // List directory contents
    List<StorageProvider.FileEntry> entries = storageProvider.listFiles(dirPath, false);
    
    assertEquals(2, entries.size());
    
    // Verify entries (order may vary)
    boolean foundFile1 = false;
    boolean foundFile2 = false;
    
    for (StorageProvider.FileEntry entry : entries) {
      if (entry.getName().equals("file1.txt")) {
        foundFile1 = true;
        assertFalse(entry.isDirectory());
        assertEquals("Content 1".length(), entry.getSize());
      } else if (entry.getName().equals("file2.csv")) {
        foundFile2 = true;
        assertFalse(entry.isDirectory());
        assertEquals("name,value\ntest,123".length(), entry.getSize());
      }
    }
    
    assertTrue(foundFile1);
    assertTrue(foundFile2);
  }

  @Test
  public void testRecursiveDirectoryListing() throws IOException {
    // Create nested directory structure
    String basePath = "/test/recursive";
    storageProvider.createDirectories(basePath + "/level1/level2");
    
    // Create files at different levels
    storageProvider.writeFile(basePath + "/root-file.txt", "root".getBytes());
    storageProvider.writeFile(basePath + "/level1/level1-file.txt", "level1".getBytes());
    storageProvider.writeFile(basePath + "/level1/level2/level2-file.txt", "level2".getBytes());
    
    // Non-recursive listing should show only immediate children
    List<StorageProvider.FileEntry> nonRecursive = storageProvider.listFiles(basePath, false);
    assertEquals(2, nonRecursive.size()); // root-file.txt and level1/
    
    // Recursive listing should show all files
    List<StorageProvider.FileEntry> recursive = storageProvider.listFiles(basePath, true);
    assertEquals(3, recursive.size()); // All three files (directories not included in recursive file listing)
    
    // Verify all files are found
    boolean foundRootFile = false;
    boolean foundLevel1File = false;
    boolean foundLevel2File = false;
    
    for (StorageProvider.FileEntry entry : recursive) {
      if (entry.getPath().contains("root-file.txt")) {
        foundRootFile = true;
      } else if (entry.getPath().contains("level1-file.txt")) {
        foundLevel1File = true;
      } else if (entry.getPath().contains("level2-file.txt")) {
        foundLevel2File = true;
      }
    }
    
    assertTrue(foundRootFile);
    assertTrue(foundLevel1File);
    assertTrue(foundLevel2File);
  }

  @Test
  public void testDeleteOperations() throws IOException {
    String filePath = "/test/data/to-delete.txt";
    String dirPath = "/test/dir-to-delete";
    
    // Create file and directory
    storageProvider.writeFile(filePath, "delete me".getBytes());
    storageProvider.createDirectories(dirPath);
    storageProvider.writeFile(dirPath + "/nested-file.txt", "nested".getBytes());
    
    // Verify they exist
    assertTrue(storageProvider.exists(filePath));
    assertTrue(storageProvider.exists(dirPath));
    assertTrue(storageProvider.exists(dirPath + "/nested-file.txt"));
    
    // Delete file
    assertTrue(storageProvider.delete(filePath));
    assertFalse(storageProvider.exists(filePath));
    
    // Delete directory (should be recursive)
    assertTrue(storageProvider.delete(dirPath));
    assertFalse(storageProvider.exists(dirPath));
    assertFalse(storageProvider.exists(dirPath + "/nested-file.txt"));
    
    // Try to delete non-existent file
    assertFalse(storageProvider.delete("/test/nonexistent.txt"));
  }

  @Test
  public void testCopyOperation() throws IOException {
    String sourcePath = "/test/data/source.txt";
    String destPath = "/test/data/destination.txt";
    String content = "Content to copy";
    
    // Create source file
    storageProvider.writeFile(sourcePath, content.getBytes(StandardCharsets.UTF_8));
    
    // Copy file
    storageProvider.copyFile(sourcePath, destPath);
    
    // Verify both files exist
    assertTrue(storageProvider.exists(sourcePath));
    assertTrue(storageProvider.exists(destPath));
    
    // Verify content is identical
    try (InputStream sourceStream = storageProvider.openInputStream(sourcePath);
         InputStream destStream = storageProvider.openInputStream(destPath);
         ByteArrayOutputStream sourceContent = new ByteArrayOutputStream();
         ByteArrayOutputStream destContent = new ByteArrayOutputStream()) {
      
      sourceStream.transferTo(sourceContent);
      destStream.transferTo(destContent);
      
      assertEquals(sourceContent.toString(), destContent.toString());
    }
  }

  @Test
  public void testPathResolution() {
    String hdfsUri = hdfs.getUri().toString();
    
    // Test resolving relative paths with HDFS URIs
    String resolved = storageProvider.resolvePath(hdfsUri + "/base", "relative/file.txt");
    assertEquals(hdfsUri + "/base/relative/file.txt", resolved);
    
    // Test absolute path override
    String absolute = storageProvider.resolvePath("/base", hdfsUri + "/absolute/file.txt");
    assertEquals(hdfsUri + "/absolute/file.txt", absolute);
  }

  @Test
  public void testLargeFile() throws IOException {
    String largePath = "/test/data/large-file.txt";
    
    // Create 1MB of test data
    StringBuilder content = new StringBuilder();
    String chunk = "This is a chunk of data that will be repeated many times.\n";
    int chunks = 1024 * 1024 / chunk.length(); // ~1MB
    
    for (int i = 0; i < chunks; i++) {
      content.append(chunk);
    }
    
    byte[] largeContent = content.toString().getBytes(StandardCharsets.UTF_8);
    
    // Write large file
    storageProvider.writeFile(largePath, largeContent);
    
    // Verify file size
    StorageProvider.FileMetadata metadata = storageProvider.getMetadata(largePath);
    assertEquals(largeContent.length, metadata.getSize());
    
    // Read back and verify first few bytes
    try (InputStream inputStream = storageProvider.openInputStream(largePath)) {
      byte[] firstChunk = inputStream.readNBytes(chunk.length());
      String firstChunkStr = new String(firstChunk, StandardCharsets.UTF_8);
      assertEquals(chunk, firstChunkStr);
    }
  }

  @Test
  public void testHasChangedDetection() throws IOException {
    String testPath = "/test/data/change-detection.txt";
    String initialContent = "Initial content";
    
    // Create initial file
    storageProvider.writeFile(testPath, initialContent.getBytes());
    StorageProvider.FileMetadata initialMetadata = storageProvider.getMetadata(testPath);
    
    // File shouldn't have changed
    assertFalse(storageProvider.hasChanged(testPath, initialMetadata));
    
    // Wait a bit to ensure timestamp difference
    try {
      Thread.sleep(1100); // Wait more than 1 second for timestamp precision
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    
    // Modify file
    String newContent = "Modified content";
    storageProvider.writeFile(testPath, newContent.getBytes());
    
    // File should have changed
    assertTrue(storageProvider.hasChanged(testPath, initialMetadata));
    
    // Get new metadata - file shouldn't have changed compared to itself
    StorageProvider.FileMetadata newMetadata = storageProvider.getMetadata(testPath);
    assertFalse(storageProvider.hasChanged(testPath, newMetadata));
  }
}