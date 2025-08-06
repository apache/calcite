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

import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for remote file refresh functionality.
 */
@Tag("integration")
public class RemoteFileRefreshTest {

  @BeforeEach
  public void checkNetworkAvailability() {
    // Check if network tests should be skipped
    String skipNetwork = System.getProperty("skipNetworkTests");
    if ("true".equalsIgnoreCase(skipNetwork)) {
      org.junit.jupiter.api.Assumptions.assumeFalse(true,
          "Network tests skipped. Set -DskipNetworkTests=false to enable.");
    }
    
    // Check for proxy configuration if needed
    String httpProxy = System.getProperty("http.proxy");
    if (httpProxy != null && !httpProxy.isEmpty()) {
      System.out.println("Using HTTP proxy: " + httpProxy);
    }
  }

  @Test public void testHttpMetadataFetch() throws Exception {
    // Test with a stable public URL
    String testUrl = "https://raw.githubusercontent.com/apache/calcite/main/README.md";
    RemoteFileMetadata metadata = RemoteFileMetadata.fetch(Sources.of(new URI(testUrl).toURL()));

    assertNotNull(metadata);
    assertNotNull(metadata.getUrl());
    assertEquals(testUrl, metadata.getUrl());

    // GitHub raw files usually provide ETag
    // Content length should be positive
    assertTrue(metadata.getContentLength() > 0 || metadata.getEtag() != null);
  }

  @Test public void testMetadataChangeDetection() {
    // Test change detection logic
    RemoteFileMetadata metadata1 =
        RemoteFileMetadata.createForTesting("http://example.com/file.csv",
        "\"abc123\"",  // ETag
        "Mon, 01 Jan 2024 00:00:00 GMT",
        1000,
        null);

    RemoteFileMetadata metadata2 =
        RemoteFileMetadata.createForTesting("http://example.com/file.csv",
        "\"abc123\"",  // Same ETag
        "Mon, 01 Jan 2024 00:00:00 GMT",
        1000,
        null);

    RemoteFileMetadata metadata3 =
        RemoteFileMetadata.createForTesting("http://example.com/file.csv",
        "\"def456\"",  // Different ETag
        "Mon, 01 Jan 2024 00:00:00 GMT",
        1000,
        null);

    // Same metadata should not indicate change
    assertFalse(metadata2.hasChanged(metadata1));

    // Different ETag should indicate change
    assertTrue(metadata3.hasChanged(metadata1));
  }

  @Test public void testContentLengthChangeDetection() {
    RemoteFileMetadata metadata1 =
        RemoteFileMetadata.createForTesting("http://example.com/file.csv",
        null,  // No ETag
        null,  // No Last-Modified
        1000,
        null);

    RemoteFileMetadata metadata2 =
        RemoteFileMetadata.createForTesting("http://example.com/file.csv",
        null,
        null,
        2000,  // Different size
        null);

    // Different content length should indicate change
    assertTrue(metadata2.hasChanged(metadata1));
  }

  @Test public void testHashComputation() throws IOException {
    String testContent = "test,data\n1,value1\n2,value2\n";
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter writer = new PrintWriter(baos, true, StandardCharsets.UTF_8);
    writer.print(testContent);
    writer.close();

    String hash =
        RemoteFileMetadata.computeHash(new java.io.ByteArrayInputStream(baos.toByteArray()));

    assertNotNull(hash);
    assertEquals(32, hash.length()); // MD5 hash is 32 hex characters
  }
}
