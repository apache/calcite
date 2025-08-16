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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;

/**
 * Base class for file adapter tests that provides isolated test environments.
 * 
 * Each test gets:
 * - A unique cache directory that is cleaned up after the test
 * - A unique data directory with copies of test resources
 * - Proper connection configuration to use these isolated directories
 * 
 * This approach provides true test isolation without needing @Isolated annotation
 * and better simulates production scenarios where different instances have
 * separate cache directories.
 */
public abstract class IsolatedFileAdapterTest {
  
  @TempDir
  protected Path tempDir;
  
  protected File testDataDir;
  protected File testCacheDir;
  protected String testId;
  
  @BeforeEach
  public void setUpIsolatedEnvironment() throws IOException {
    // Create unique test ID
    testId = UUID.randomUUID().toString();
    
    // Create isolated directories
    testDataDir = tempDir.resolve("data-" + testId).toFile();
    testCacheDir = tempDir.resolve("cache-" + testId).toFile();
    
    testDataDir.mkdirs();
    testCacheDir.mkdirs();
    
    // Copy test resources if needed
    copyTestResources();
  }
  
  @AfterEach
  public void tearDownIsolatedEnvironment() throws IOException {
    // Clean up is automatic with @TempDir
    // But we can add explicit cleanup if needed
    deleteRecursively(testCacheDir);
    deleteRecursively(testDataDir);
  }
  
  /**
   * Create a connection with isolated cache directory.
   */
  protected Connection createIsolatedConnection(String model) throws SQLException {
    Properties info = new Properties();
    info.put("model", model);
    
    // Add cache directory configuration
    info.put("parquetCacheDir", testCacheDir.getAbsolutePath());
    
    return DriverManager.getConnection("jdbc:calcite:", info);
  }
  
  /**
   * Create a connection with isolated cache and data directories.
   */
  protected Connection createIsolatedConnection(Properties baseProperties) throws SQLException {
    Properties info = new Properties(baseProperties);
    
    // Add isolated directories
    info.put("parquetCacheDir", testCacheDir.getAbsolutePath());
    
    return DriverManager.getConnection("jdbc:calcite:", info);
  }
  
  /**
   * Create a model JSON string with isolated directories.
   */
  protected String createIsolatedModel(String schemaName, String flavor) {
    return "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: '" + schemaName + "',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: '" + schemaName + "',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + testDataDir.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        parquetCacheDir: '" + testCacheDir.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        flavor: '" + flavor + "'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";
  }
  
  /**
   * Copy test resources to isolated data directory.
   * Override this method to copy specific test files.
   */
  protected void copyTestResources() throws IOException {
    // Default implementation does nothing
    // Subclasses can override to copy specific test files
  }
  
  /**
   * Copy a resource file to the test data directory.
   */
  protected void copyResourceFile(String resourcePath, String targetName) throws IOException {
    Path source = new File(getClass().getResource(resourcePath).getFile()).toPath();
    Path target = testDataDir.toPath().resolve(targetName);
    Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
  }
  
  /**
   * Copy an entire resource directory to the test data directory.
   */
  protected void copyResourceDirectory(String resourcePath) throws IOException {
    Path source = new File(getClass().getResource(resourcePath).getFile()).toPath();
    copyDirectory(source, testDataDir.toPath());
  }
  
  private void copyDirectory(Path source, Path target) throws IOException {
    Files.walk(source).forEach(sourcePath -> {
      try {
        Path targetPath = target.resolve(source.relativize(sourcePath));
        if (Files.isDirectory(sourcePath)) {
          Files.createDirectories(targetPath);
        } else {
          Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to copy " + sourcePath, e);
      }
    });
  }
  
  private void deleteRecursively(File file) {
    if (file == null || !file.exists()) {
      return;
    }
    
    if (file.isDirectory()) {
      File[] children = file.listFiles();
      if (children != null) {
        for (File child : children) {
          deleteRecursively(child);
        }
      }
    }
    file.delete();
  }
}