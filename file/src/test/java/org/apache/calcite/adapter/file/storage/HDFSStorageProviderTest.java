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
import org.apache.hadoop.fs.permission.FsPermission;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.when;

/**
 * Unit tests for HDFSStorageProvider.
 */
@Tag("unit")
public class HDFSStorageProviderTest {

  @Mock
  private FileSystem mockFileSystem;

  @Mock
  private Configuration mockConfig;

  private HDFSStorageProvider storageProvider;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    when(mockConfig.get("fs.defaultFS", "hdfs://localhost:9000"))
        .thenReturn("hdfs://test-namenode:9000");
    
    storageProvider = new HDFSStorageProvider(mockFileSystem, mockConfig);
  }

  @Test
  public void testGetStorageType() {
    assertEquals("hdfs", storageProvider.getStorageType());
  }

  @Test 
  public void testExistsWhenFileExists() throws IOException {
    when(mockFileSystem.exists(any(Path.class))).thenReturn(true);
    
    assertTrue(storageProvider.exists("/test/file.txt"));
  }

  @Test
  public void testExistsWhenFileDoesNotExist() throws IOException {
    when(mockFileSystem.exists(any(Path.class))).thenReturn(false);
    
    assertFalse(storageProvider.exists("/test/nonexistent.txt"));
  }

  @Test
  public void testExistsHandlesIOException() throws IOException {
    when(mockFileSystem.exists(any(Path.class))).thenThrow(new IOException("HDFS error"));
    
    assertThrows(IOException.class, () -> storageProvider.exists("/test/file.txt"));
  }

  @Test
  public void testIsDirectoryWhenDirectory() throws IOException {
    FileStatus mockStatus = new FileStatus(0, true, 1, 1024, 12345L, 
        new Path("/test/dir"), FsPermission.getDefault(), "user", "group");
    when(mockFileSystem.exists(any(Path.class))).thenReturn(true);
    when(mockFileSystem.getFileStatus(any(Path.class))).thenReturn(mockStatus);
    
    assertTrue(storageProvider.isDirectory("/test/dir"));
  }

  @Test
  public void testIsDirectoryWhenFile() throws IOException {
    FileStatus mockStatus = new FileStatus(1024, false, 1, 1024, 12345L, 
        new Path("/test/file.txt"), FsPermission.getDefault(), "user", "group");
    when(mockFileSystem.exists(any(Path.class))).thenReturn(true);
    when(mockFileSystem.getFileStatus(any(Path.class))).thenReturn(mockStatus);
    
    assertFalse(storageProvider.isDirectory("/test/file.txt"));
  }

  @Test
  public void testIsDirectoryWhenPathDoesNotExist() throws IOException {
    when(mockFileSystem.exists(any(Path.class))).thenReturn(false);
    
    assertFalse(storageProvider.isDirectory("/test/nonexistent"));
  }

  @Test
  public void testGetMetadata() throws IOException {
    FileStatus mockStatus = new FileStatus(1024, false, 1, 1024, 12345L, 
        new Path("/test/file.txt"), FsPermission.getDefault(), "user", "group");
    when(mockFileSystem.getFileStatus(any(Path.class))).thenReturn(mockStatus);
    
    StorageProvider.FileMetadata metadata = storageProvider.getMetadata("/test/file.txt");
    
    assertNotNull(metadata);
    assertEquals("/test/file.txt", metadata.getPath());
    assertEquals(1024, metadata.getSize());
    assertEquals(12345L, metadata.getLastModified());
    assertEquals("text/plain", metadata.getContentType());
    assertEquals("12345", metadata.getEtag()); // Uses modification time as ETag
  }

  @Test
  public void testGetMetadataForJsonFile() throws IOException {
    FileStatus mockStatus = new FileStatus(512, false, 1, 1024, 67890L, 
        new Path("/test/data.json"), FsPermission.getDefault(), "user", "group");
    when(mockFileSystem.getFileStatus(any(Path.class))).thenReturn(mockStatus);
    
    StorageProvider.FileMetadata metadata = storageProvider.getMetadata("/test/data.json");
    
    assertEquals("application/json", metadata.getContentType());
    assertEquals("67890", metadata.getEtag());
  }

  @Test
  public void testListFilesNonRecursive() throws IOException {
    when(mockFileSystem.exists(any(Path.class))).thenReturn(true);
    
    FileStatus[] statuses = {
        new FileStatus(1024, false, 1, 1024, 12345L, 
            new Path("/test/file1.txt"), FsPermission.getDefault(), "user", "group"),
        new FileStatus(0, true, 1, 1024, 12346L, 
            new Path("/test/subdir"), FsPermission.getDefault(), "user", "group"),
        new FileStatus(2048, false, 1, 1024, 12347L, 
            new Path("/test/file2.csv"), FsPermission.getDefault(), "user", "group")
    };
    
    when(mockFileSystem.listStatus(any(Path.class))).thenReturn(statuses);
    
    List<StorageProvider.FileEntry> entries = storageProvider.listFiles("/test", false);
    
    assertEquals(3, entries.size());
    
    assertEquals("/test/file1.txt", entries.get(0).getPath());
    assertEquals("file1.txt", entries.get(0).getName());
    assertFalse(entries.get(0).isDirectory());
    assertEquals(1024, entries.get(0).getSize());
    
    assertEquals("/test/subdir", entries.get(1).getPath());
    assertEquals("subdir", entries.get(1).getName());
    assertTrue(entries.get(1).isDirectory());
    assertEquals(0, entries.get(1).getSize());
    
    assertEquals("/test/file2.csv", entries.get(2).getPath());
    assertEquals("file2.csv", entries.get(2).getName());
    assertEquals(2048, entries.get(2).getSize());
  }

  @Test
  public void testListFilesPathDoesNotExist() throws IOException {
    when(mockFileSystem.exists(any(Path.class))).thenReturn(false);
    
    List<StorageProvider.FileEntry> entries = storageProvider.listFiles("/nonexistent", false);
    
    assertTrue(entries.isEmpty());
  }

  @Test
  public void testOpenInputStream() throws IOException {
    byte[] testContent = "Hello HDFS".getBytes();
    InputStream mockInputStream = new ByteArrayInputStream(testContent);
    when(mockFileSystem.open(any(Path.class))).thenReturn(mockInputStream);
    
    InputStream inputStream = storageProvider.openInputStream("/test/file.txt");
    
    assertNotNull(inputStream);
    // Verify content can be read (basic test)
    int firstByte = inputStream.read();
    assertEquals('H', firstByte);
  }

  @Test
  public void testResolvePath() {
    // Test absolute HDFS path
    String resolved = storageProvider.resolvePath("hdfs://namenode:9000/base", "relative/path");
    assertEquals("hdfs://namenode:9000/base/relative/path", resolved);
    
    // Test already absolute path
    String absolutePath = storageProvider.resolvePath("/base", "hdfs://other:9000/absolute");
    assertEquals("hdfs://other:9000/absolute", absolutePath);
    
    // Test simple path resolution
    String simple = storageProvider.resolvePath("/base", "file.txt");
    assertEquals("/base/file.txt", simple);
    
    // Test path with trailing slash
    String withSlash = storageProvider.resolvePath("/base/", "file.txt");
    assertEquals("/base/file.txt", withSlash);
  }

  @Test
  public void testWriteFileByteArray() throws IOException {
    when(mockFileSystem.create(any(Path.class), anyBoolean())).thenReturn(
        new java.io.ByteArrayOutputStream());
    
    byte[] content = "Test content".getBytes();
    
    // Should not throw exception
    storageProvider.writeFile("/test/output.txt", content);
  }

  @Test
  public void testDelete() throws IOException {
    when(mockFileSystem.exists(any(Path.class))).thenReturn(true);
    when(mockFileSystem.delete(any(Path.class), anyBoolean())).thenReturn(true);
    
    assertTrue(storageProvider.delete("/test/file.txt"));
  }

  @Test
  public void testDeleteNonExistentFile() throws IOException {
    when(mockFileSystem.exists(any(Path.class))).thenReturn(false);
    
    assertFalse(storageProvider.delete("/test/nonexistent.txt"));
  }

  @Test
  public void testCreateDirectories() throws IOException {
    when(mockFileSystem.mkdirs(any(Path.class))).thenReturn(true);
    
    // Should not throw exception
    storageProvider.createDirectories("/test/new/directory");
  }

  @Test
  public void testGetFileSystemAndConfiguration() {
    assertEquals(mockFileSystem, storageProvider.getFileSystem());
    assertEquals(mockConfig, storageProvider.getConfiguration());
  }

  @Test
  public void testInferContentTypes() throws IOException {
    // Test various file extensions
    FileStatus csvStatus = new FileStatus(100, false, 1, 1024, 12345L, 
        new Path("/test/data.csv"), FsPermission.getDefault(), "user", "group");
    when(mockFileSystem.getFileStatus(new Path("/test/data.csv"))).thenReturn(csvStatus);
    
    StorageProvider.FileMetadata csvMetadata = storageProvider.getMetadata("/test/data.csv");
    assertEquals("text/csv", csvMetadata.getContentType());
    
    FileStatus parquetStatus = new FileStatus(100, false, 1, 1024, 12345L, 
        new Path("/test/data.parquet"), FsPermission.getDefault(), "user", "group");
    when(mockFileSystem.getFileStatus(new Path("/test/data.parquet"))).thenReturn(parquetStatus);
    
    StorageProvider.FileMetadata parquetMetadata = storageProvider.getMetadata("/test/data.parquet");
    assertEquals("application/octet-stream", parquetMetadata.getContentType());
  }
}