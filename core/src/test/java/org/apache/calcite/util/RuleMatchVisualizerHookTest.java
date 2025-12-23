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
package org.apache.calcite.util;

import org.apache.calcite.jdbc.CalciteConnection;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.calcite.plan.visualizer.RuleMatchVisualizer.DATA_FILE_PREFIX;
import static org.apache.calcite.plan.visualizer.RuleMatchVisualizer.HTML_FILE_PREFIX;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static java.util.Objects.requireNonNull;

/**
 * Tests for {@link RuleMatchVisualizerHook}.
 */
class RuleMatchVisualizerHookTest {

  @TempDir
  Path tempDir;

  @AfterEach
  void cleanup() {
    RuleMatchVisualizerHook.INSTANCE.disable();
  }

  @Test void testEnableDisable() {
    String vizDir = tempDir.toString();

    // Enable visualizer
    RuleMatchVisualizerHook.INSTANCE.enable(vizDir);

    // Directory should be created
    File dir = new File(vizDir);
    assertTrue(dir.exists());
    assertTrue(dir.isDirectory());

    // Disable visualizer
    RuleMatchVisualizerHook.INSTANCE.disable();
  }

  @Test void testEnableFromConnection() throws Exception {
    String vizDir = tempDir.resolve("viz").toString();

    // Create connection with visualizer property
    String url = "jdbc:calcite:ruleVisualizerDir=" + vizDir;
    try (Connection conn = DriverManager.getConnection(url)) {
      assertThat(conn, instanceOf(CalciteConnection.class));
      CalciteConnection calciteConn = (CalciteConnection) conn;

      // Check that the property is set
      String configuredDir = calciteConn.config().ruleVisualizerDir();
      assertThat(configuredDir, notNullValue());
      assertThat(configuredDir, is(vizDir));

      // The hook should be enabled automatically by the connection
      // Let's run a simple query to trigger the planner
      try (Statement stmt = conn.createStatement()) {
        String sql = "SELECT 1 FROM (VALUES (1))";
        try (ResultSet rs = stmt.executeQuery(sql)) {
          assertTrue(rs.next());
          assertThat(rs.getInt(1), is(1));
        }
      }
    }

    File dir = new File(vizDir);
    assertTrue(dir.exists());
    assertTrue(dir.isDirectory());
    Map<Boolean, List<File>> matched =
        Arrays.stream(requireNonNull(dir.listFiles()))
        .collect(Collectors.partitioningBy(f -> f.getName().contains(DATA_FILE_PREFIX)));

    List<File> dataFiles = matched.get(true);
    List<File> htmlFiles = matched.get(false);

    assertThat(dataFiles, hasSize(1));
    assertThat(htmlFiles, hasSize(1));
    assertThat(dataFiles.get(0).getName(), containsString(DATA_FILE_PREFIX));
    assertThat(htmlFiles.get(0).getName(), containsString(HTML_FILE_PREFIX));
  }

  @Test void testSystemProperty() {
    String vizDir = tempDir.resolve("sysprop").toString();

    // Set system property
    System.setProperty("calcite.visualizer.dir", vizDir);

    try {
      // Check system property
      RuleMatchVisualizerHook.INSTANCE.checkSystemProperty();

      // Directory should be created
      File dir = new File(vizDir);
      assertTrue(dir.exists());
      assertTrue(dir.isDirectory());
    } finally {
      // Clean up system property
      System.clearProperty("calcite.visualizer.dir");
      RuleMatchVisualizerHook.INSTANCE.disable();
    }
  }

  @Test void testMultipleQueries() throws Exception {
    String vizDir = tempDir.resolve("multi").toString();

    // Create connection with visualizer property
    String url = "jdbc:calcite:ruleVisualizerDir=" + vizDir;
    try (Connection conn = DriverManager.getConnection(url)) {
      try (Statement stmt = conn.createStatement()) {
        // Execute multiple queries
        for (int i = 1; i <= 3; i++) {
          String sql = "SELECT " + i;
          try (ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) == i);
          }
        }

        String sql = "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(x, y) WHERE x > 0";
        try (ResultSet rs = stmt.executeQuery(sql)) {
          assertTrue(rs.next());
        }
      }
    }

    // Directory should exist
    File dir = new File(vizDir);
    assertTrue(dir.exists());
    assertTrue(dir.isDirectory());

    Map<Boolean, List<File>> matched =
        Arrays.stream(requireNonNull(dir.listFiles()))
            .collect(Collectors.partitioningBy(f -> f.getName().contains(DATA_FILE_PREFIX)));

    List<File> dataFiles = matched.get(true);
    List<File> htmlFiles = matched.get(false);

    assertThat(dataFiles, hasSize(3));
    assertThat(htmlFiles, hasSize(3));
  }

  @Test void testConnectionWithoutVisualizer() throws Exception {
    // Create connection without visualizer property
    String url = "jdbc:calcite:";
    try (Connection conn = DriverManager.getConnection(url)) {
      assertThat(conn, instanceOf(CalciteConnection.class));
      CalciteConnection calciteConn = (CalciteConnection) conn;

      // Check that the property is not set
      String configuredDir = calciteConn.config().ruleVisualizerDir();
      assertNull(configuredDir);

      // Query should still work
      try (Statement stmt = conn.createStatement()) {
        String sql = "SELECT 1 FROM (VALUES (1))";
        try (ResultSet rs = stmt.executeQuery(sql)) {
          assertTrue(rs.next());
          assertThat(rs.getInt(1), is(1));
        }
      }
    }
  }
}
