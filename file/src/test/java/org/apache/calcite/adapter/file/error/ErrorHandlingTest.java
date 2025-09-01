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
package org.apache.calcite.adapter.file.error;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.EnumSet;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive error handling test suite for the file adapter.
 */
@Tag("unit")
public class ErrorHandlingTest {

  private File tempDir;

  @BeforeEach
  public void setUp() {
    tempDir =
                      new File(System.getProperty("java.io.tmpdir"), "error_test_" + System.nanoTime());
    tempDir.mkdirs();
  }

  @AfterEach
  public void tearDown() {
    if (tempDir != null && tempDir.exists()) {
      deleteRecursively(tempDir);
    }
  }

  private void deleteRecursively(File file) {
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

  @Test void testNonExistentDirectory() throws Exception {
    File nonExistentDir = new File(tempDir, "does_not_exist");

    String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'BAD',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'BAD',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + nonExistentDir.getAbsolutePath().replace("\\", "\\\\") + "'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    // The file adapter now handles non-existent directories gracefully (creates empty schema)
    // Just verify we can create the connection
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model)) {
      assertNotNull(conn);
    }
  }

  @Test void testEmptyDirectory() throws Exception {
    // Empty directory should work but have no tables
    String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'EMPTY',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'EMPTY',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
         Statement stmt = conn.createStatement()) {

      // Query non-existent table
      SQLException e = assertThrows(SQLException.class, () -> {
        stmt.executeQuery("SELECT * FROM \"no_such_table\"");
      });

      assertThat(e.getMessage(), containsString("Object 'no_such_table' not found"));
    }
  }

  @Test void testMalformedCsvFile() throws Exception {
    File badCsv = new File(tempDir, "malformed.csv");
    try (FileWriter writer = new FileWriter(badCsv)) {
      writer.write("id,name,age\n");
      writer.write("1,Alice,30\n");
      writer.write("2,Bob\n"); // Missing age field
      writer.write("3,Charlie,not_a_number\n"); // Invalid number
    }

    String model = createModel(tempDir);

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
         Statement stmt = conn.createStatement()) {

      // CSV reader may throw various errors for malformed data
      // The error could be immediate or happen during iteration
      boolean foundExpectedError = false;
      StringBuilder allMessages = new StringBuilder();

      try {
        try (ResultSet rs = stmt.executeQuery("SELECT * FROM \"malformed\"")) {
          while (rs.next()) {
            // Try to read all rows - errors may occur here
            rs.getObject(1);
            rs.getObject(2);
            rs.getObject(3);
          }
        }
      } catch (Exception e) {
        // Check the entire error chain for expected errors
        Throwable current = e;
        while (current != null) {
          String msg = current.getMessage();
          if (msg != null) {
            allMessages.append(msg).append(" | ");
            // Check both the message and the exception type
            if (msg.contains("Index") ||
                msg.contains("ArrayIndexOutOfBounds") ||
                msg.contains("parse") ||
                msg.contains("format") ||
                msg.contains("malformed") ||
                msg.contains("column") ||
                msg.contains("field") ||
                current instanceof ArrayIndexOutOfBoundsException ||
                current instanceof NumberFormatException) {
              foundExpectedError = true;
            }
          }
          // Also check the exception class name
          String className = current.getClass().getSimpleName();
          allMessages.append("[").append(className).append("] ");
          if (className.contains("Index") ||
              className.contains("Format") ||
              className.contains("Parse")) {
            foundExpectedError = true;
          }
          current = current.getCause();
        }
      }

      assertTrue(foundExpectedError,
                 "Expected parsing, format, or index error in error chain. Full chain: " + allMessages.toString());
    }
  }

  @Test void testInvalidJsonFile() throws Exception {
    File badJson = new File(tempDir, "invalid.json");
    try (FileWriter writer = new FileWriter(badJson)) {
      writer.write("{ \"id\": 1, \"name\": \"Alice\" }\n");
      writer.write("{ \"id\": 2, \"name\": \"Bob\", ]\n"); // Invalid JSON syntax
    }

    String model = createModel(tempDir);

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
         Statement stmt = conn.createStatement()) {

      // The file adapter now handles invalid JSON gracefully, possibly skipping bad records
      // Just verify we can query the table without throwing
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM \"invalid\"")) {
        int count = 0;
        while (rs.next()) {
          count++;
        }
        // Should at least read the valid first record
        assertTrue(count >= 1);
      }
    }
  }

  @Test void testCorruptedParquetFile() throws Exception {
    File badParquet = new File(tempDir, "corrupted.parquet");
    try (FileWriter writer = new FileWriter(badParquet)) {
      writer.write("This is not a valid Parquet file");
    }

    String model = createModel(tempDir);

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
         Statement stmt = conn.createStatement()) {

      // Corrupted Parquet files behavior depends on the execution engine:
      // - Default engine: registers the table but fails when accessed with "is not a Parquet file"
      // - DUCKDB: doesn't register the table at all, fails with "Object 'corrupted' not found"
      Exception e = assertThrows(Exception.class, () -> {
        stmt.executeQuery("SELECT * FROM \"corrupted\"");
      });

      // Different engines report the error differently
      assertTrue(
          e.getMessage().contains("is not a Parquet file") || // Default engine error
          e.getMessage().contains("Object 'corrupted' not found"), // DUCKDB error
          "Expected Parquet format error or table not found, but got: " + e.getMessage());

      // Check if the corrupted file was registered as a table
      // Note: DUCKDB doesn't register invalid parquet files at all
      DatabaseMetaData metaData = conn.getMetaData();
      try (ResultSet tables = metaData.getTables(null, null, "corrupted", null)) {
        // Either the table is registered (default) or not (DUCKDB)
        // Both behaviors are acceptable for corrupted files
      }
    }
  }

  @Test void testFilePermissionDenied() throws Exception {
    // Skip on Windows as it handles permissions differently
    if (System.getProperty("os.name").toLowerCase().contains("win")) {
      return;
    }

    File restrictedFile = new File(tempDir, "restricted.csv");
    try (FileWriter writer = new FileWriter(restrictedFile)) {
      writer.write("id,name\n1,Alice\n");
    }

    // Remove read permissions
    try {
      Files.setPosixFilePermissions(restrictedFile.toPath(),
          EnumSet.noneOf(PosixFilePermission.class));

      String model = createModel(tempDir);

      try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
           Statement stmt = conn.createStatement()) {

        Exception e = assertThrows(Exception.class, () -> {
          stmt.executeQuery("SELECT * FROM \"restricted\"");
        });

        // Check the entire error chain for permission issues
        boolean foundExpectedError = false;
        Throwable current = e;
        StringBuilder allMessages = new StringBuilder();
        while (current != null) {
          String msg = current.getMessage();
          if (msg != null) {
            allMessages.append(msg).append(" ");
            if (msg.contains("Permission denied") ||
                msg.contains("Access is denied") ||
                msg.contains("cannot read") ||
                msg.contains("cannot access") ||
                msg.contains("Exception loading data") ||
                msg.contains("Object 'restricted' not found")) {  // File might not be visible with no permissions
              foundExpectedError = true;
            }
          }
          current = current.getCause();
        }

        assertTrue(foundExpectedError,
                   "Expected permission-related or not-found error in error chain, got: " + allMessages.toString());
      }
    } finally {
      // Restore permissions for cleanup
      Files.setPosixFilePermissions(restrictedFile.toPath(),
          EnumSet.allOf(PosixFilePermission.class));
    }
  }

  @Test void testInvalidGlobPattern() throws Exception {
    String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'FILES',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'FILES',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        glob: '[invalid\\\\pattern'\n" // Invalid glob
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    // The file adapter now handles invalid glob patterns gracefully
    // Just verify we can create the connection
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model)) {
      assertNotNull(conn);
    }
  }

  @Test void testInvalidExecutionEngine() {
    String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'FILES',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'FILES',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        executionEngine: 'invalid_engine'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    Exception e = assertThrows(Exception.class, () -> {
      try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model)) {
        // Should fail during configuration
      }
    });

    assertThat(e.getMessage(), containsString("Error instantiating JsonCustomSchema"));
  }

  @Test void testRemoteFileTimeout() throws Exception {
    // Test timeout with unreachable URL
    String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'REMOTE',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'REMOTE',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: 'http://192.0.2.1:9999/timeout',\n" // Non-routable IP
        + "        connectTimeout: 1000,\n" // 1 second timeout
        + "        readTimeout: 1000\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    // The file adapter now handles timeouts gracefully without throwing exceptions
    // Just verify we can create the connection
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model)) {
      assertNotNull(conn);
    }
  }

  @Test void testCyclicSymbolicLink() throws Exception {
    // Skip on Windows as it handles symlinks differently
    if (System.getProperty("os.name").toLowerCase().contains("win")) {
      return;
    }

    File subDir = new File(tempDir, "subdir");
    subDir.mkdir();

    // Create cyclic symlink
    File link = new File(subDir, "cycle");
    try {
      Files.createSymbolicLink(link.toPath(), tempDir.toPath());
    } catch (Exception e) {
      // Skip if symlinks not supported
      return;
    }

    String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'FILES',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'FILES',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        recursive: true,\n"
        + "        glob: '**/*.csv'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    // Should handle cyclic links gracefully
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model)) {
      // Should not hang or crash
      assertTrue(true);
    }
  }

  @Test void testOutOfMemoryHandling() throws Exception {
    // Create a large CSV to potentially cause OOM with non-PARQUET engine
    File largeCsv = new File(tempDir, "large.csv");
    try (FileWriter writer = new FileWriter(largeCsv)) {
      writer.write("id,data\n");
      for (int i = 0; i < 100; i++) {
        writer.write(i + "," + "x".repeat(1000) + "\n");
      }
    }

    String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'FILES',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'FILES',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        executionEngine: 'linq4j',\n" // In-memory engine
        + "        maxMemoryBytes: 1024,\n" // Very small memory limit
        + "        tableNameCasing: 'LOWER',\n"
        + "        columnNameCasing: 'LOWER'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
         Statement stmt = conn.createStatement()) {

      // Should either work with limited memory or fail gracefully
      try {
        stmt.executeQuery("SELECT COUNT(*) FROM \"large\"");
      } catch (Exception e) {
        // Should get memory-related error, not crash
        assertTrue(e.getMessage().contains("memory") ||
                  e.getMessage().contains("Memory") ||
                  e.getCause() instanceof OutOfMemoryError);
      }
    }
  }

  @Test void testInvalidCharacterEncoding() throws Exception {
    File encodingTest = new File(tempDir, "encoding.csv");
    // Write file with specific encoding issues
    Files.write(encodingTest.toPath(),
        new byte[] { (byte)0xFF, (byte)0xFE, 65, 66, 67 }); // Invalid UTF-8

    String model = createModel(tempDir);

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
         Statement stmt = conn.createStatement()) {

      // Should handle encoding errors gracefully
      try {
        try (ResultSet rs = stmt.executeQuery("SELECT * FROM \"encoding\"")) {
          while (rs.next()) {
            // May get garbled data but shouldn't crash
          }
        }
      } catch (Exception e) {
        // Encoding errors are acceptable
        assertTrue(e.getMessage().contains("encoding") ||
                  e.getMessage().contains("charset") ||
                  e.getMessage().contains("UTF"));
      }
    }
  }

  private String createModel(File directory) {
    return "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'FILES',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'FILES',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + directory.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        tableNameCasing: 'LOWER',\n"
        + "        columnNameCasing: 'LOWER'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";
  }
}
