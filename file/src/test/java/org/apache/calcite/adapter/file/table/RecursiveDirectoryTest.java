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

import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for recursive directory scanning feature.
 */
@Tag("unit")
public class RecursiveDirectoryTest {
  @TempDir
  Path tempDir;

  @BeforeEach
  public void setUp() throws Exception {
    // Clear any static caches that might interfere with test isolation
    Sources.clearFileCache();
    System.gc();
    // Create nested directory structure with CSV files
    createNestedDirectoryStructure();
  }

  @AfterEach
  public void tearDown() throws Exception {
    // Clear caches after each test to prevent contamination
    Sources.clearFileCache();
    System.gc();
    Thread.sleep(100);
  }

  private void createNestedDirectoryStructure() throws Exception {
    // Root level file
    File rootCsv = new File(tempDir.toFile(), "root_data.csv");
    try (FileWriter writer = new FileWriter(rootCsv, StandardCharsets.UTF_8)) {
      writer.write("id:int,name:string,level:int\n");
      writer.write("1,Root Item,0\n");
    }

    // Level 1 directory
    File level1Dir = new File(tempDir.toFile(), "level1");
    level1Dir.mkdir();

    File level1Csv = new File(level1Dir, "level1_data.csv");
    try (FileWriter writer = new FileWriter(level1Csv, StandardCharsets.UTF_8)) {
      writer.write("id:int,name:string,level:int\n");
      writer.write("2,Level 1 Item,1\n");
    }

    // Level 2 directory
    File level2Dir = new File(level1Dir, "level2");
    level2Dir.mkdir();

    File level2Csv = new File(level2Dir, "level2_data.csv");
    try (FileWriter writer = new FileWriter(level2Csv, StandardCharsets.UTF_8)) {
      writer.write("id:int,name:string,level:int\n");
      writer.write("3,Level 2 Item,2\n");
    }

    // Another branch
    File branchDir = new File(tempDir.toFile(), "branch");
    branchDir.mkdir();

    File branchCsv = new File(branchDir, "branch_data.csv");
    try (FileWriter writer = new FileWriter(branchCsv, StandardCharsets.UTF_8)) {
      writer.write("id:int,name:string,level:int\n");
      writer.write("4,Branch Item,1\n");
    }
  }

  @Test public void testRecursiveDirectoryScanning() throws Exception {
    Properties props = new Properties();
    props.setProperty("recursive", "true");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      rootSchema.add("FILES",
          new FileSchema(rootSchema, "FILES", tempDir.toFile(), null,
              new ExecutionEngineConfig(), true, null, null));

      try (Statement statement = connection.createStatement()) {
        // Query all files from all levels
        ResultSet rs =
            statement.executeQuery("SELECT * FROM ("
            + "  SELECT * FROM \"FILES\".\"level1_level1_data\""
            + "  UNION ALL"
            + "  SELECT * FROM \"FILES\".\"level1_level2_level2_data\""
            + "  UNION ALL"
            + "  SELECT * FROM \"FILES\".\"branch_branch_data\""
            + ") ORDER BY \"id\"");

        // Should find 3 records (root file not processed)
        assertTrue(rs.next());
        assertThat(rs.getInt("id"), is(2));
        assertThat(rs.getString("name"), is("Level 1 Item"));
        assertThat(rs.getInt("level"), is(1));

        assertTrue(rs.next());
        assertThat(rs.getInt("id"), is(3));
        assertThat(rs.getString("name"), is("Level 2 Item"));
        assertThat(rs.getInt("level"), is(2));

        assertTrue(rs.next());
        assertThat(rs.getInt("id"), is(4));
        assertThat(rs.getString("name"), is("Branch Item"));
        assertThat(rs.getInt("level"), is(1));
      }
    }
  }

  @Test public void testNonRecursiveMode() throws Exception {
    // Test that without recursive flag, we only get root level files
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      rootSchema.add("FILES",
          new FileSchema(rootSchema, "FILES", tempDir.toFile(), null,
              new ExecutionEngineConfig(), false, null, null));

      try (Statement statement = connection.createStatement()) {
        // Count tables - should only find root level
        ResultSet tables = connection.getMetaData().getTables(null, "FILES", "%", null);
        int tableCount = 0;
        boolean foundRootData = false;
        boolean foundNestedData = false;

        while (tables.next()) {
          String tableName = tables.getString("TABLE_NAME");
          tableCount++;
          if (tableName.equals("root_data")) {
            foundRootData = true;
          }
          if (tableName.toUpperCase(Locale.ROOT).contains("LEVEL1") || tableName.toUpperCase(Locale.ROOT).contains("LEVEL2")
              || tableName.toUpperCase(Locale.ROOT).contains("BRANCH")) {
            foundNestedData = true;
          }
        }

        // In non-recursive mode, should only find root level files
        assertThat(foundRootData, is(true));
        assertThat(foundNestedData, is(false));
        assertThat(tableCount, is(1));
      }
    }
  }
}
