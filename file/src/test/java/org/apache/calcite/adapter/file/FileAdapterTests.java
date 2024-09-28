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
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import com.google.common.collect.Ordering;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.PrintStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

import static org.apache.calcite.test.Matchers.isListOf;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import static java.util.Objects.requireNonNull;

/** Helpers for test suite of the File adapter. */
abstract class FileAdapterTests {
  private FileAdapterTests() {
  }

  static Fluent sql(String model, String sql) {
    return new Fluent(model, sql, FileAdapterTests::output);
  }

  /** Returns a function that checks the contents of a result set against an
   * expected string. */
  static Consumer<ResultSet> expect(final String... expected) {
    return resultSet -> {
      try {
        final List<String> lines = new ArrayList<>();
        collect(lines, resultSet);
        assertThat(lines, isListOf(expected));
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    };
  }

  /** Returns a function that checks the contents of a result set against an
   * expected string. */
  private static Consumer<ResultSet> expectUnordered(String... expected) {
    final List<String> expectedLines =
        Ordering.natural().immutableSortedCopy(Arrays.asList(expected));
    return resultSet -> {
      try {
        final List<String> lines = new ArrayList<>();
        collect(lines, resultSet);
        Collections.sort(lines);
        assertThat(lines, is(expectedLines));
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    };
  }

  private static void collect(List<String> result, ResultSet resultSet)
      throws SQLException {
    final StringBuilder buf = new StringBuilder();
    while (resultSet.next()) {
      buf.setLength(0);
      int n = resultSet.getMetaData().getColumnCount();
      String sep = "";
      for (int i = 1; i <= n; i++) {
        buf.append(sep)
            .append(resultSet.getMetaData().getColumnLabel(i))
            .append("=")
            .append(resultSet.getString(i));
        sep = "; ";
      }
      result.add(Util.toLinux(buf.toString()));
    }
  }

  static String toString(ResultSet resultSet) throws SQLException {
    StringBuilder buf = new StringBuilder();
    while (resultSet.next()) {
      int n = resultSet.getMetaData().getColumnCount();
      String sep = "";
      for (int i = 1; i <= n; i++) {
        buf.append(sep)
            .append(resultSet.getMetaData().getColumnLabel(i))
            .append("=")
            .append(resultSet.getObject(i));
        sep = "; ";
      }
      buf.append("\n");
    }
    return buf.toString();
  }

  static void checkSql(String sql, String model, Consumer<ResultSet> fn)
      throws SQLException {
    Connection connection = null;
    Statement statement = null;
    try {
      Properties info = new Properties();
      info.put("model", jsonPath(model));
      connection = DriverManager.getConnection("jdbc:calcite:", info);
      statement = connection.createStatement();
      final ResultSet resultSet =
          statement.executeQuery(
              sql);
      fn.accept(resultSet);
    } finally {
      close(connection, statement);
    }
  }

  static String jsonPath(String model) {
    return resourcePath(model + ".json");
  }

  static String resourcePath(String path) {
    final URL url =
        requireNonNull(FileAdapterTest.class.getResource("/" + path), "url");
    return Sources.of(url).file().getAbsolutePath();
  }

  private static void output(ResultSet resultSet, PrintStream out)
      throws SQLException {
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    while (resultSet.next()) {
      for (int i = 1;; i++) {
        out.print(resultSet.getString(i));
        if (i < columnCount) {
          out.print(", ");
        } else {
          out.println();
          break;
        }
      }
    }
  }

  private static void output(ResultSet resultSet) {
    try {
      output(resultSet, System.out);
    } catch (SQLException e) {
      throw TestUtil.rethrow(e);
    }
  }

  static void close(@Nullable Connection connection,
      @Nullable Statement statement) {
    if (statement != null) {
      try {
        statement.close();
      } catch (SQLException e) {
        // ignore
      }
    }
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        // ignore
      }
    }
  }

  /** Fluent API to perform test actions. */
  static class Fluent {
    private final String model;
    private final String sql;
    private final Consumer<ResultSet> expect;

    Fluent(String model, String sql, Consumer<ResultSet> expect) {
      this.model = model;
      this.sql = sql;
      this.expect = expect;
    }

    /** Runs the test. */
    Fluent ok() {
      try {
        checkSql(sql, model, expect);
        return this;
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    }

    /** Assigns a function to call to test whether output is correct. */
    Fluent checking(Consumer<ResultSet> expect) {
      return new Fluent(model, sql, expect);
    }

    /** Sets the rows that are expected to be returned from the SQL query. */
    Fluent returns(String... expectedLines) {
      return checking(expect(expectedLines));
    }

    /** Sets the rows that are expected to be returned from the SQL query,
     * in no particular order. */
    Fluent returnsUnordered(String... expectedLines) {
      return checking(expectUnordered(expectedLines));
    }
  }
}
