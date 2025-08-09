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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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
  
  private static Properties testProperties;
  
  static {
    testProperties = loadLocalProperties();
  }
  
  private static Properties loadLocalProperties() {
    Properties props = new Properties();
    File propsFile = new File("local-test.properties");
    if (!propsFile.exists()) {
      // Try relative to file module
      propsFile = new File("file/local-test.properties");
    }
    if (!propsFile.exists()) {
      // Try calcite/file path
      propsFile = new File("calcite/file/local-test.properties");
    }
    if (!propsFile.exists()) {
      // Try absolute path
      propsFile = new File("/Users/kennethstott/ndc-calcite/calcite-rs-jni/calcite/file/local-test.properties");
    }
    
    if (propsFile.exists()) {
      try (InputStream is = new FileInputStream(propsFile)) {
        props.load(is);
        System.out.println("Loaded S3 test properties from: " + propsFile.getAbsolutePath());
      } catch (IOException e) {
        System.err.println("Failed to load properties file: " + e.getMessage());
      }
    }
    return props;
  }
  
  private static String getConfig(String key) {
    // Try environment variable first
    String value = System.getenv(key);
    if (value == null || value.isEmpty()) {
      // Try system property
      value = System.getProperty(key);
    }
    if (value == null || value.isEmpty()) {
      // Try local properties file
      value = testProperties.getProperty(key);
    }
    return value;
  }

  private boolean isS3Available() {
    String bucket = getConfig("S3_TEST_BUCKET");
    String accessKey = getConfig("AWS_ACCESS_KEY_ID");
    String secretKey = getConfig("AWS_SECRET_ACCESS_KEY");
    
    // Also set region if available
    String region = getConfig("AWS_REGION");
    if (region != null && !region.isEmpty()) {
      System.setProperty("aws.region", region);
    }
    String defaultRegion = getConfig("AWS_DEFAULT_REGION");
    if (defaultRegion != null && !defaultRegion.isEmpty()) {
      System.setProperty("aws.region", defaultRegion);
    }
    
    return bucket != null && !bucket.isEmpty() 
        && accessKey != null && !accessKey.isEmpty()
        && secretKey != null && !secretKey.isEmpty();
  }

  private String getTestBucket() {
    String bucket = getConfig("S3_TEST_BUCKET");
    if (bucket == null || bucket.isEmpty()) {
      throw new IllegalStateException("S3_TEST_BUCKET must be set in environment, system properties, or local-test.properties");
    }
    return bucket;
  }

  @Test void testS3Operations() throws IOException {
    assumeTrue(isS3Available(), "S3 credentials not configured, skipping test");
    
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
    assumeTrue(isS3Available(), "S3 credentials not configured, skipping test");
    
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
