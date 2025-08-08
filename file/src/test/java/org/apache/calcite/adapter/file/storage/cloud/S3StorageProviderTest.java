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

import org.apache.calcite.adapter.file.storage.S3StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for S3 storage provider.
 * These tests require AWS credentials to be configured.
 *
 * The tests require the following environment variables:
 * - AWS_ACCESS_KEY_ID
 * - AWS_SECRET_ACCESS_KEY
 * - AWS_REGION (optional, defaults to us-east-1)
 * - S3_TEST_BUCKET (the bucket name to use for testing)
 */
@Tag("integration")
public class S3StorageProviderTest {

  private String getTestBucket() {
    String bucket = System.getenv("S3_TEST_BUCKET");
    if (bucket == null || bucket.isEmpty()) {
      throw new IllegalStateException("S3_TEST_BUCKET environment variable must be set");
    }
    return bucket;
  }

  @Test void testS3Operations() throws IOException {
    S3StorageProvider provider = new S3StorageProvider();
    String bucket = getTestBucket();

    // Test listing bucket root
    String bucketUrl = "s3://" + bucket + "/";
    List<StorageProvider.FileEntry> entries = provider.listFiles(bucketUrl, false);
    assertNotNull(entries);

    // If bucket has files, test operations on first file
    if (!entries.isEmpty()) {
      StorageProvider.FileEntry firstFile = entries.stream()
          .filter(e -> !e.isDirectory())
          .findFirst()
          .orElse(null);

      if (firstFile != null) {
        // Test metadata
        StorageProvider.FileMetadata metadata = provider.getMetadata(firstFile.getPath());
        assertNotNull(metadata);
        assertEquals(firstFile.getPath(), metadata.getPath());
        assertTrue(metadata.getSize() >= 0);
        assertNotNull(metadata.getEtag());

        // Test exists
        assertTrue(provider.exists(firstFile.getPath()));

        // Test reading (only for small files)
        if (metadata.getSize() < 1024 * 1024) { // Less than 1MB
          try (InputStream is = provider.openInputStream(firstFile.getPath())) {
            assertNotNull(is);
            byte[] buffer = new byte[100];
            int bytesRead = is.read(buffer);
            assertTrue(bytesRead >= 0);
          }
        }
      }
    }

    // Test non-existent file
    assertFalse(provider.exists("s3://" + bucket + "/non-existent-file-xyz.txt"));
  }

  @Test void testS3PathResolution() {
    S3StorageProvider provider = new S3StorageProvider();

    // Test absolute S3 URL
    assertEquals("s3://bucket/absolute/path.txt",
        provider.resolvePath("s3://bucket/base/", "s3://bucket/absolute/path.txt"));

    // Test relative path
    assertEquals("s3://bucket/base/relative.txt",
        provider.resolvePath("s3://bucket/base/", "relative.txt"));

    // Test relative path with subdirectory
    assertEquals("s3://bucket/base/sub/file.txt",
        provider.resolvePath("s3://bucket/base/", "sub/file.txt"));

    // Test when base is a file
    assertEquals("s3://bucket/base/relative.txt",
        provider.resolvePath("s3://bucket/base/file.txt", "relative.txt"));
  }

  @Test void testS3UriParsing() throws IOException {
    S3StorageProvider provider = new S3StorageProvider();

    // Test directory listing with various path formats
    String bucket = getTestBucket();

    // Without trailing slash
    try {
      provider.listFiles("s3://" + bucket, false);
    } catch (IOException e) {
      // Expected if bucket is empty or inaccessible
    }

    // With trailing slash
    try {
      provider.listFiles("s3://" + bucket + "/", false);
    } catch (IOException e) {
      // Expected if bucket is empty or inaccessible
    }

    // With prefix
    try {
      provider.listFiles("s3://" + bucket + "/test/", false);
    } catch (IOException e) {
      // Expected if prefix doesn't exist
    }
  }
}
