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

import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Integration tests for SFTP storage provider.
 */
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SftpIntegrationTest {

  /**
   * Test with public SFTP servers - tries multiple servers for better reliability.
   */
  @Test @Order(1)
  @DisplayName("Test public SFTP server connectivity")
  void testRebexPublicServer() throws IOException {
    System.out.println("\n=== Testing Public SFTP Servers ===");

    // Try multiple servers in case one is down
    // Note: All these are read-only public test servers
    String[][] servers = {
        {"sftp://demo:demo@demo.wftpserver.com:2222/", "demo.wftpserver.com (port 2222)"},
        {"sftp://demo:password@test.rebex.net/", "test.rebex.net"},
        {"sftp://demo-user:demo-user@demo.wftpserver.com:2222/", "demo.wftpserver.com (alt user)"}
    };

    StorageProvider provider = null;
    String workingUrl = null;
    String serverName = null;
    
    for (String[] server : servers) {
      try {
        System.out.println("Trying " + server[1] + "...");
        provider = StorageProviderFactory.createFromUrl(server[0]);
        // Test connection by listing files
        List<StorageProvider.FileEntry> testEntries = provider.listFiles(server[0], false);
        if (testEntries != null) {
          workingUrl = server[0];
          serverName = server[1];
          System.out.println("✓ Successfully connected to " + serverName);
          break;
        }
      } catch (Exception e) {
        System.out.println("  Failed to connect to " + server[1] + ": " + e.getMessage());
      }
    }

    if (provider == null || workingUrl == null) {
      System.out.println("WARNING: Could not connect to any public SFTP test server");
      System.out.println("Skipping SFTP tests - this may be due to network issues or servers being down");
      return; // Skip test instead of failing
    }

    // List root directory
    List<StorageProvider.FileEntry> entries = provider.listFiles(workingUrl, false);
    assertNotNull(entries);
    assertTrue(entries.size() > 0, "Should have files in test directory");

    System.out.println("Files found on " + serverName + ":");
    Map<String, StorageProvider.FileEntry> fileMap = new HashMap<>();
    for (StorageProvider.FileEntry entry : entries) {
      System.out.printf(Locale.ROOT, "  %s %-20s %10d bytes  %s%n",
          entry.isDirectory() ? "[D]" : "[F]",
          entry.getName(),
          entry.getSize(),
          new java.util.Date(entry.getLastModified()));
      fileMap.put(entry.getName(), entry);
    }

    // Test specific file operations - look for any text file
    StorageProvider.FileEntry textFile = entries.stream()
        .filter(e -> !e.isDirectory() && 
                (e.getName().endsWith(".txt") || e.getName().endsWith(".md")))
        .findFirst()
        .orElse(entries.stream()
            .filter(e -> !e.isDirectory())
            .findFirst()
            .orElse(null));
    
    if (textFile != null) {
      testFileOperations(provider, textFile, workingUrl);
    }

    // Test directory operations
    StorageProvider.FileEntry dir = entries.stream()
        .filter(StorageProvider.FileEntry::isDirectory)
        .findFirst()
        .orElse(null);

    if (dir != null) {
      testDirectoryOperations(provider, dir, workingUrl);
    }
  }

  /**
   * Test with simple file operations.
   */
  @Test @Order(2)
  @DisplayName("Test SFTP file operations")
  void testFileOperations() throws Exception {
    System.out.println("\n=== Testing SFTP File Operations ===");

    // Try multiple servers - all are read-only public test servers
    String[] sftpUrls = {
        "sftp://demo:demo@demo.wftpserver.com:2222/",
        "sftp://demo:password@test.rebex.net/"
    };
    
    StorageProvider provider = null;
    String sftpUrl = null;
    
    for (String url : sftpUrls) {
      try {
        provider = StorageProviderFactory.createFromUrl(url);
        // Test connection
        provider.listFiles(url, false);
        sftpUrl = url;
        break;
      } catch (Exception e) {
        // Try next server
      }
    }
    
    if (provider == null || sftpUrl == null) {
      System.out.println("WARNING: Could not connect to any SFTP server - skipping test");
      return;
    }

    // Test URL resolution
    String resolvedUrl = provider.resolvePath(sftpUrl, "readme.txt");
    System.out.println("Resolved URL: " + resolvedUrl);
    assertTrue(resolvedUrl.contains("readme.txt"));

    // Test exists for root
    assertTrue(provider.exists(sftpUrl), "Root directory should exist");

    // Test isDirectory for root
    assertTrue(provider.isDirectory(sftpUrl), "Root should be a directory");

    // List files and test one
    List<StorageProvider.FileEntry> entries = provider.listFiles(sftpUrl, false);
    if (!entries.isEmpty()) {
      StorageProvider.FileEntry firstFile = entries.stream()
          .filter(e -> !e.isDirectory())
          .findFirst()
          .orElse(null);

      if (firstFile != null) {
        System.out.println("Testing file: " + firstFile.getName());

        // Test file exists
        assertTrue(provider.exists(firstFile.getPath()));

        // Test file is not directory
        assertFalse(provider.isDirectory(firstFile.getPath()));

        // Test metadata
        StorageProvider.FileMetadata metadata = provider.getMetadata(firstFile.getPath());
        assertNotNull(metadata);
        assertEquals(firstFile.getSize(), metadata.getSize());
      }
    }
  }

  /**
   * Performance test comparing SFTP with local files.
   */
  @Test @Order(3)
  @DisplayName("Test SFTP performance characteristics")
  void testPerformance() throws IOException {
    System.out.println("\n=== Testing SFTP Performance ===");

    // Try multiple servers - all are read-only public test servers
    String[] sftpUrls = {
        "sftp://demo:demo@demo.wftpserver.com:2222/",
        "sftp://demo:password@test.rebex.net/"
    };
    
    StorageProvider provider = null;
    String sftpUrl = null;
    
    for (String url : sftpUrls) {
      try {
        provider = StorageProviderFactory.createFromUrl(url);
        // Test connection
        provider.listFiles(url, false);
        sftpUrl = url;
        break;
      } catch (Exception e) {
        // Try next server
      }
    }
    
    if (provider == null || sftpUrl == null) {
      System.out.println("WARNING: Could not connect to any SFTP server - skipping test");
      return;
    }

    // Measure listing performance
    long startTime = System.nanoTime();
    List<StorageProvider.FileEntry> entries = provider.listFiles(sftpUrl, false);
    long listTime = System.nanoTime() - startTime;

    System.out.printf(Locale.ROOT, "Directory listing: %d files in %d ms%n",
        entries.size(),
        TimeUnit.NANOSECONDS.toMillis(listTime));

    // Find a reasonable size file for download test
    StorageProvider.FileEntry testFile = entries.stream()
        .filter(e -> !e.isDirectory() && e.getSize() > 0 && e.getSize() < 1_000_000)
        .findFirst()
        .orElse(null);

    if (testFile != null) {
      // Measure metadata retrieval
      startTime = System.nanoTime();
      StorageProvider.FileMetadata metadata = provider.getMetadata(testFile.getPath());
      long metadataTime = System.nanoTime() - startTime;

      System.out.printf(Locale.ROOT, "Metadata retrieval for %s: %d ms%n",
          testFile.getName(),
          TimeUnit.NANOSECONDS.toMillis(metadataTime));

      // Measure download
      startTime = System.nanoTime();
      try (InputStream stream = provider.openInputStream(testFile.getPath())) {
        byte[] buffer = new byte[8192];
        int totalBytes = 0;
        int bytesRead;
        while ((bytesRead = stream.read(buffer)) != -1) {
          totalBytes += bytesRead;
        }
        long downloadTime = System.nanoTime() - startTime;

        double throughputMBps = (totalBytes / 1024.0 / 1024.0) /
            (TimeUnit.NANOSECONDS.toSeconds(downloadTime) + 0.001);

        System.out.printf(Locale.ROOT, "Download %s: %d bytes in %d ms (%.2f MB/s)%n",
            testFile.getName(),
            totalBytes,
            TimeUnit.NANOSECONDS.toMillis(downloadTime),
            throughputMBps);
      }
    }
  }

  /**
   * Test with local SFTP server if available.
   * Run with: -Dsftp.test.local=true -Dsftp.test.host=localhost -Dsftp.test.user=testuser ...
   */
  @Test @Order(4)
  @DisplayName("Test with local SFTP server")
  void testLocalServer() throws IOException {
    System.out.println("\n=== Testing Local SFTP Server ===");

    // Skip if no local server is configured
    if (!"true".equals(System.getProperty("sftp.test.local"))) {
      System.out.println("Local SFTP server test skipped (set -Dsftp.test.local=true to enable)");
      return;
    }

    String host = System.getProperty("sftp.test.host", "localhost");
    String port = System.getProperty("sftp.test.port", "22");
    String user = System.getProperty("sftp.test.user", System.getProperty("user.name"));
    String pass = System.getProperty("sftp.test.pass", "");
    String path = System.getProperty("sftp.test.path", "/tmp");

    // Build URL
    String sftpUrl;
    if (!pass.isEmpty()) {
      sftpUrl = String.format(Locale.ROOT, "sftp://%s:%s@%s:%s%s", user, pass, host, port, path);
    } else {
      sftpUrl = String.format(Locale.ROOT, "sftp://%s@%s:%s%s", user, host, port, path);
    }

    System.out.println("Connecting to: " + sftpUrl.replace(pass, "***"));

    try {
      StorageProvider provider = StorageProviderFactory.createFromUrl(sftpUrl);

      // Test operations
      List<StorageProvider.FileEntry> entries = provider.listFiles(sftpUrl, false);
      System.out.println("Found " + entries.size() + " entries in " + path);
      
      entries.stream().limit(10).forEach(entry -> {
        System.out.printf(Locale.ROOT, "  %s %-30s %10d bytes%n",
            entry.isDirectory() ? "[D]" : "[F]",
            entry.getName(),
            entry.getSize());
      });
    } catch (Exception e) {
      System.out.println("Could not connect to local SFTP server: " + e.getMessage());
      System.out.println("Make sure local SFTP server is running and accessible");
      return; // Skip instead of failing
    }
  }

  /**
   * Test error handling and edge cases.
   */
  @Test @Order(5)
  @DisplayName("Test SFTP error handling")
  void testErrorHandling() {
    System.out.println("\n=== Testing Error Handling ===");

    // Test invalid host
    assertThrows(IOException.class, () -> {
      StorageProvider provider =
          StorageProviderFactory.createFromUrl("sftp://user:pass@invalid.host.example.com/");
      provider.listFiles("sftp://user:pass@invalid.host.example.com/", false);
    }, "Should fail with invalid host");

    // Test invalid credentials
    assertThrows(IOException.class, () -> {
      StorageProvider provider =
          StorageProviderFactory.createFromUrl("sftp://baduser:badpass@demo.wftpserver.com:2222/");
      provider.listFiles("sftp://baduser:badpass@demo.wftpserver.com:2222/", false);
    }, "Should fail with invalid credentials");

    // Test non-existent file
    assertThrows(IOException.class, () -> {
      StorageProvider provider =
          StorageProviderFactory.createFromUrl("sftp://demo:demo@demo.wftpserver.com:2222/");
      provider.getMetadata("sftp://demo:demo@demo.wftpserver.com:2222/nonexistent.file");
    }, "Should fail with non-existent file");

    System.out.println("✓ Error handling tests passed");
  }

  /**
   * Test configuration variations.
   */
  @Test @Order(6)
  @DisplayName("Test SFTP configuration options")
  void testConfigurations() {
    System.out.println("\n=== Testing Configuration Options ===");

    // Test with explicit configuration
    Map<String, Object> config = new HashMap<>();
    config.put("username", "demo");
    config.put("password", "password");
    config.put("strictHostKeyChecking", false);

    StorageProvider provider = StorageProviderFactory.createFromType("sftp", config);
    assertNotNull(provider);
    assertEquals("sftp", provider.getStorageType());

    // Test URL variations - just verify provider creation
    StorageProvider provider1 = StorageProviderFactory.createFromUrl("sftp://user@host.com/path");
    assertNotNull(provider1);

    StorageProvider provider2 = StorageProviderFactory.createFromUrl("sftp://user:pass@host.com:2222/");
    assertNotNull(provider2);

    StorageProvider provider3 = StorageProviderFactory.createFromUrl("sftp://host.com/path/to/file");
    assertNotNull(provider3);

    System.out.println("✓ Configuration tests passed");
  }

  /**
   * Test concurrent access.
   */
  @Test @Order(7)
  @DisplayName("Test concurrent SFTP access")
  void testConcurrentAccess() throws Exception {
    System.out.println("\n=== Testing Concurrent Access ===");

    // Try multiple servers - all are read-only public test servers
    String[] sftpUrls = {
        "sftp://demo:demo@demo.wftpserver.com:2222/",
        "sftp://demo:password@test.rebex.net/"
    };
    
    StorageProvider provider = null;
    String sftpUrl = null;
    
    for (String url : sftpUrls) {
      try {
        provider = StorageProviderFactory.createFromUrl(url);
        // Test connection
        provider.listFiles(url, false);
        sftpUrl = url;
        break;
      } catch (Exception e) {
        // Try next server
      }
    }
    
    if (provider == null || sftpUrl == null) {
      System.out.println("WARNING: Could not connect to any SFTP server - skipping test");
      return;
    }

    // Make final copies for lambda
    final StorageProvider finalProvider = provider;
    final String finalSftpUrl = sftpUrl;

    // Run multiple threads accessing SFTP simultaneously
    int threadCount = 5;
    List<Thread> threads = new ArrayList<>();
    List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());

    for (int i = 0; i < threadCount; i++) {
      final int threadId = i;
      Thread thread = new Thread(() -> {
        try {
          // Each thread lists files
          List<StorageProvider.FileEntry> entries = finalProvider.listFiles(finalSftpUrl, false);
          System.out.printf(Locale.ROOT, "Thread %d: Found %d files%n", threadId, entries.size());

          // Try to read a file if available
          StorageProvider.FileEntry file = entries.stream()
              .filter(e -> !e.isDirectory())
              .findFirst()
              .orElse(null);

          if (file != null) {
            try (InputStream stream = finalProvider.openInputStream(file.getPath())) {
              byte[] buffer = new byte[100];
              int bytesRead = stream.read(buffer);
              System.out.printf(Locale.ROOT, "Thread %d: Read %d bytes from %s%n",
                  threadId, bytesRead, file.getName());
            }
          }
        } catch (Exception e) {
          exceptions.add(e);
        }
      });

      threads.add(thread);
      thread.start();
    }

    // Wait for all threads
    for (Thread thread : threads) {
      thread.join(30000); // 30 second timeout
    }

    // Check for exceptions
    if (!exceptions.isEmpty()) {
      fail("Concurrent access failed with " + exceptions.size() + " exceptions: " +
           exceptions.get(0).getMessage());
    }

    System.out.println("✓ Concurrent access test passed");
  }

  // Helper methods

  private void testFileOperations(StorageProvider provider, StorageProvider.FileEntry file,
                                  String baseUrl) throws IOException {
    System.out.println("\nTesting file operations on: " + file.getName());

    // Build full path for file
    String fileUrl = baseUrl.endsWith("/") ? baseUrl + file.getName() : baseUrl + "/" + file.getName();

    // Test exists
    assertTrue(provider.exists(fileUrl), "File should exist");

    // Test is not directory
    assertFalse(provider.isDirectory(fileUrl), "File should not be a directory");

    // Test metadata
    StorageProvider.FileMetadata metadata = provider.getMetadata(fileUrl);
    assertEquals(file.getSize(), metadata.getSize(), "Size should match");
    assertTrue(metadata.getPath().endsWith(file.getName()), "Path should contain file name");

    // Test download
    try (InputStream stream = provider.openInputStream(fileUrl)) {
      byte[] buffer = new byte[Math.min(1024, (int)file.getSize())];
      int bytesRead = stream.read(buffer);
      assertTrue(bytesRead > 0, "Should read some bytes");

      System.out.println("  First few bytes: " +
          new String(buffer, 0, Math.min(bytesRead, 50), StandardCharsets.UTF_8).replace("\n", "\\n"));
    }

    System.out.println("✓ File operations successful");
  }

  private void testDirectoryOperations(StorageProvider provider, StorageProvider.FileEntry dir,
                                       String baseUrl) throws IOException {
    System.out.println("\nTesting directory operations on: " + dir.getName());

    // Build full path for directory
    String dirUrl = baseUrl.endsWith("/") ? baseUrl + dir.getName() : baseUrl + "/" + dir.getName();

    // Test exists
    assertTrue(provider.exists(dirUrl), "Directory should exist");

    // Test is directory
    assertTrue(provider.isDirectory(dirUrl), "Should be a directory");

    // List contents
    List<StorageProvider.FileEntry> contents = provider.listFiles(dirUrl, false);
    System.out.println("  Directory contains " + contents.size() + " items");

    System.out.println("✓ Directory operations successful");
  }

  private void testUrlParsing(String url, String expectedUser, String expectedHost,
                              int expectedPort, String expectedPath) {
    System.out.println("Testing URL: " + url);
    // URL parsing is tested implicitly through the provider
    // This is a simplified validation
    assertTrue(url.startsWith("sftp://"), "Should be SFTP URL");
    if (expectedUser != null) {
      assertTrue(url.contains(expectedUser + "@"), "Should contain username");
    }
    assertTrue(url.contains(expectedHost), "Should contain host");
    if (expectedPort != 22) {
      assertTrue(url.contains(":" + expectedPort), "Should contain port");
    }
  }
}
