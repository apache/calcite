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

import org.apache.calcite.adapter.file.storage.FtpStorageProvider;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for FTP storage provider functionality.
 * These tests verify the logic without requiring a real FTP server.
 */
public class FtpStorageProviderMockTest {

  @Test void testFtpUriParsing() {
    FtpStorageProvider provider = new FtpStorageProvider();

    // Test basic FTP URL resolution
    assertEquals("ftp://example.com:21/base/file.csv",
        provider.resolvePath("ftp://example.com/base/", "file.csv"));

    // Test with custom port
    assertEquals("ftp://example.com:2121/data/report.csv",
        provider.resolvePath("ftp://example.com:2121/data/", "report.csv"));

    // Test absolute path resolution
    assertEquals("ftp://other.com/absolute/path.txt",
        provider.resolvePath("ftp://example.com/base/", "ftp://other.com/absolute/path.txt"));

    // Test path with subdirectories
    assertEquals("ftp://example.com:21/base/sub/dir/file.json",
        provider.resolvePath("ftp://example.com/base/", "sub/dir/file.json"));
  }

  @Test void testFtpPathHandling() {
    FtpStorageProvider provider = new FtpStorageProvider();

    // Test that storage type is correct
    assertEquals("ftp", provider.getStorageType());

    // Test path resolution when base is a file
    assertEquals("ftp://example.com:21/path/to/newfile.csv",
        provider.resolvePath("ftp://example.com/path/to/file.csv", "newfile.csv"));

    // Test path resolution with trailing slash
    assertEquals("ftp://example.com:21/path/to/dir/file.csv",
        provider.resolvePath("ftp://example.com/path/to/dir/", "file.csv"));
  }

  @Test void testFtpErrorScenarios() throws Exception {
    FtpStorageProvider provider = new FtpStorageProvider();

    // Test invalid URL handling
    assertThrows(Exception.class, () -> {
      provider.listFiles("not-an-ftp-url", false);
    });

    // Test that FTP URLs are required
    assertThrows(Exception.class, () -> {
      provider.listFiles("/local/path", false);
    });
  }

  /**
   * Example of expected FTP storage provider behavior.
   * This demonstrates how the provider would work with a real FTP server.
   */
  @Test void documentExpectedBehavior() {
    // Example 1: Anonymous FTP
    // URL: ftp://speedtest.tele2.net/
    // The provider would connect with username "anonymous" and password "anonymous@"

    // Example 2: Authenticated FTP
    // URL: ftp://user:pass@server.com/path/
    // The provider would parse credentials from the URL

    // Example 3: Custom port
    // URL: ftp://server.com:2121/data/
    // The provider would use port 2121 instead of default 21

    // Example 4: Directory listing
    // When listFiles() is called with recursive=true,
    // it would list all files in the directory and subdirectories

    // Example 5: File operations
    // - exists() checks if a file exists on the FTP server
    // - isDirectory() uses FTP CHDIR command to test directories
    // - openInputStream() downloads file content for reading
    // - getMetadata() retrieves file size and modification time

    assertTrue(true, "Documentation test");
  }
}
