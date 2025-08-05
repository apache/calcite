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

import org.apache.calcite.adapter.file.storage.SftpStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for SFTP storage provider.
 *
 * Note: Most tests are disabled by default as they require external SFTP servers.
 * Enable specific tests when you have access to an SFTP server.
 */
public class SftpStorageProviderTest {

  /**
   * Test using test.rebex.net public SFTP server.
   * This server allows anonymous access for testing.
   */
  @Test
  void testRebexPublicSftpServer() throws IOException {
    // test.rebex.net provides a public SFTP test server
    // Username: demo
    // Password: password
    String sftpUrl = "sftp://demo:password@test.rebex.net/";

    StorageProvider provider = StorageProviderFactory.createFromUrl(sftpUrl);
    assertEquals("sftp", provider.getStorageType());

    // List root directory
    List<StorageProvider.FileEntry> entries = provider.listFiles(sftpUrl, false);
    assertNotNull(entries);
    assertTrue(entries.size() > 0, "Should have some files in test directory");

    System.out.println("Files on test.rebex.net:");
    for (StorageProvider.FileEntry entry : entries) {
      System.out.printf(Locale.ROOT, "  %s %s (size: %d)%n",
          entry.isDirectory() ? "[DIR] " : "[FILE]",
          entry.getName(),
          entry.getSize());
    }

    // Find readme.txt which should exist
    StorageProvider.FileEntry readmeFile = entries.stream()
        .filter(e -> "readme.txt".equals(e.getName()))
        .findFirst()
        .orElse(null);

    if (readmeFile != null) {
      // Test file operations
      assertTrue(provider.exists(readmeFile.getPath()));
      assertFalse(provider.isDirectory(readmeFile.getPath()));

      // Get metadata
      StorageProvider.FileMetadata metadata = provider.getMetadata(readmeFile.getPath());
      assertNotNull(metadata);
      assertTrue(metadata.getSize() > 0);

      // Download content
      try (InputStream stream = provider.openInputStream(readmeFile.getPath())) {
        byte[] buffer = new byte[100];
        int bytesRead = stream.read(buffer);
        assertTrue(bytesRead > 0);
        System.out.println("First few bytes of readme.txt: " + new String(buffer, 0, bytesRead, StandardCharsets.UTF_8));
      }
    }
  }

  /**
   * Test using demo.wftpserver.com (another public test server).
   * Note: This server appears to be offline or changed credentials.
   * Keeping test disabled until a replacement is found.
   */
  @Test @Disabled("Server no longer available with these credentials")
  void testWftpPublicServer() throws IOException {
    // demo.wftpserver.com - appears to be offline
    // Previous credentials no longer work
    // Keeping for reference in case server comes back online
    String sftpUrl = "sftp://demo-user:demo-user@demo.wftpserver.com/";

    StorageProvider provider = StorageProviderFactory.createFromUrl(sftpUrl);

    List<StorageProvider.FileEntry> entries = provider.listFiles(sftpUrl, false);
    assertNotNull(entries);

    System.out.println("Files on demo.wftpserver.com:");
    entries.stream().limit(10).forEach(entry -> {
      System.out.printf(Locale.ROOT, "  %s %s%n",
          entry.isDirectory() ? "[DIR] " : "[FILE]",
          entry.getName());
    });
  }

  /**
   * Test factory creation with configuration.
   */
  @Test void testFactoryCreation() {
    Map<String, Object> config = new HashMap<>();
    config.put("username", "testuser");
    config.put("password", "testpass");
    config.put("strictHostKeyChecking", false);

    StorageProvider provider = StorageProviderFactory.createFromType("sftp", config);
    assertNotNull(provider);
    assertEquals("sftp", provider.getStorageType());
  }

  /**
   * Test URL parsing.
   */
  @Test void testUrlParsing() {
    SftpStorageProvider provider = new SftpStorageProvider();

    // Test basic URL
    String resolved = provider.resolvePath("sftp://user@host.com/base/", "file.txt");
    assertEquals("sftp://user@host.com/base/file.txt", resolved);

    // Test with port
    resolved = provider.resolvePath("sftp://user@host.com:2222/base/", "file.txt");
    assertEquals("sftp://user@host.com:2222/base/file.txt", resolved);

    // Test absolute path
    resolved = provider.resolvePath("sftp://user@host.com/base/", "sftp://other@host2.com/file.txt");
    assertEquals("sftp://other@host2.com/file.txt", resolved);
  }

  /**
   * Test with local SFTP server (if available).
   * Uncomment and modify to test with your own SFTP server.
   */
  @Test @Disabled("Requires local SFTP server - enable if you have one running")
  void testLocalSftpServer() throws IOException {
    // Example configuration for local testing
    String sftpUrl = "sftp://localhost/home/testuser/";

    // Or with explicit credentials
    // String sftpUrl = "sftp://testuser:password@localhost/home/testuser/";

    StorageProvider provider = StorageProviderFactory.createFromUrl(sftpUrl);

    List<StorageProvider.FileEntry> entries = provider.listFiles(sftpUrl, false);
    assertNotNull(entries);

    System.out.println("Local SFTP server files:");
    entries.forEach(entry -> {
      System.out.printf(Locale.ROOT, "  %s %s (size: %d)%n",
          entry.isDirectory() ? "[DIR] " : "[FILE]",
          entry.getName(),
          entry.getSize());
    });
  }

  /**
   * Test configuration for Calcite schema.
   */
  @Test void testSchemaConfiguration() {
    String schemaJson = "{\n"
  +
        "  \"version\": \"1.0\",\n"
  +
        "  \"defaultSchema\": \"SFTP_FILES\",\n"
  +
        "  \"schemas\": [{\n"
  +
        "    \"name\": \"SFTP_FILES\",\n"
  +
        "    \"type\": \"custom\",\n"
  +
        "    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
  +
        "    \"operand\": {\n"
  +
        "      \"directory\": \"sftp://demo:password@test.rebex.net/\",\n"
  +
        "      \"recursive\": false\n"
  +
        "    }\n"
  +
        "  }]\n"
  +
        "}";

    System.out.println("Example SFTP schema configuration:");
    System.out.println(schemaJson);

    // With storage configuration
    String advancedSchemaJson = "{\n"
  +
        "  \"version\": \"1.0\",\n"
  +
        "  \"defaultSchema\": \"SFTP_FILES\",\n"
  +
        "  \"schemas\": [{\n"
  +
        "    \"name\": \"SFTP_FILES\",\n"
  +
        "    \"type\": \"custom\",\n"
  +
        "    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
  +
        "    \"operand\": {\n"
  +
        "      \"storageType\": \"sftp\",\n"
  +
        "      \"storageConfig\": {\n"
  +
        "        \"username\": \"demo\",\n"
  +
        "        \"password\": \"password\",\n"
  +
        "        \"strictHostKeyChecking\": false\n"
  +
        "      },\n"
  +
        "      \"directory\": \"sftp://test.rebex.net/\",\n"
  +
        "      \"recursive\": false\n"
  +
        "    }\n"
  +
        "  }]\n"
  +
        "}";

    System.out.println("\nAdvanced SFTP schema configuration:");
    System.out.println(advancedSchemaJson);
  }
}
