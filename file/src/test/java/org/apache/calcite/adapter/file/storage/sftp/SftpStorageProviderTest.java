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
package org.apache.calcite.adapter.file.storage.sftp;

import org.apache.calcite.adapter.file.storage.SftpStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
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
 * These tests require network access to public SFTP test servers.
 * They may fail if the servers are down or change their configuration.
 */
@Tag("integration")
public class SftpStorageProviderTest {

  @BeforeEach
  public void checkSftpTestRequirements() {
    // Check if SFTP tests should be skipped
    String skipSftp = System.getProperty("skipSftpTests");
    if ("true".equalsIgnoreCase(skipSftp)) {
      org.junit.jupiter.api.Assumptions.assumeFalse(true,
          "SFTP tests skipped. Set -DskipSftpTests=false to enable.");
    }
    
    // Check for SFTP test server configuration
    String sftpTestServer = System.getProperty("sftp.test.server");
    if (sftpTestServer == null || sftpTestServer.isEmpty()) {
      System.out.println("WARNING: No SFTP test server configured. "
          + "Set -Dsftp.test.server=<host> to enable full SFTP testing.");
    }
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
}