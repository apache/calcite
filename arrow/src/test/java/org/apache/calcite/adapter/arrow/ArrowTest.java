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

package org.apache.calcite.adapter.arrow;

import com.google.common.collect.Ordering;

import org.apache.arrow.vector.VectorSchemaRoot;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Source;

import org.apache.calcite.util.Sources;

import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.PrintStream;
import java.sql.*;
import java.util.*;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ArrowTest {

  /**
   * Test to read Arrow file and check it's field name and type
   */
  @Test void testArrowSchema() {
    Source source = Sources.of(ArrowTest.class.getResource("/bug"));
    ArrowSchema arrowSchema = new ArrowSchema(source.file().getAbsoluteFile());
    Map<String, Table> tableMap = arrowSchema.getTableMap();
    RelDataType relDataType = tableMap.get("TEST").getRowType(new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT));

    Assertions.assertEquals(relDataType.getFieldNames().get(0), "fieldOne");
    Assertions.assertEquals(relDataType.getFieldList().get(0).getType().toString(), "INTEGER");
    Assertions.assertEquals(relDataType.getFieldNames().get(1), "fieldTwo");
    Assertions.assertEquals(relDataType.getFieldList().get(1).getType().toString(), "VARCHAR");
    Assertions.assertEquals(relDataType.getFieldNames().get(2), "fieldThree");
    Assertions.assertEquals(relDataType.getFieldList().get(2).getType().toString(), "REAL");

  }

  private String resourcePath(String path) {
    return Sources.of(ArrowTest.class.getResource("/" + path)).file().getAbsolutePath();
  }

  /** Returns a function that checks the contents of a result set against an
   * expected string. */
  private static Consumer<ResultSet> expectUnordered(String... expected) {
    final List<String> expectedLines =
        Ordering.natural().immutableSortedCopy(Arrays.asList(expected));
    return resultSet -> {
      try {
        final List<String> lines = new ArrayList<>();
        ArrowTest.collect(lines, resultSet);
        Collections.sort(lines);
        assertEquals(expectedLines, lines);
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

  private void checkSql(String sql, String model, Consumer<ResultSet> fn)
      throws SQLException {
    Connection connection = null;
    Statement statement = null;
    try {
      Properties info = new Properties();
      info.put("model", jsonPath(model));
      connection = DriverManager.getConnection("jdbc:calcite:", info);
      statement = connection.createStatement();
      final ResultSet resultSet = statement.executeQuery(sql);
      fn.accept(resultSet);
    } finally {
      close(connection, statement);
    }
  }

  private String jsonPath(String model) {
    return resourcePath(model + ".json");
  }

  private void close(Connection connection, Statement statement) {
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

  private static Consumer<ResultSet> expect(final String... expected) {
    return resultSet -> {
      try {
        final List<String> lines = new ArrayList<>();
        ArrowTest.collect(lines, resultSet);
        assertEquals(Arrays.asList(expected).toString(), lines.toString());
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    };
  }

  private Fluent sql(String model, String sql) {
    return new Fluent(model, sql, this::output);
  }

  private Void output(ResultSet resultSet) {
    try {
      output(resultSet, System.out);
    } catch (SQLException e) {
      throw TestUtil.rethrow(e);
    }
    return null;
  }

  private void output(ResultSet resultSet, PrintStream out)
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

  @Test void testArrow() throws SQLException {
    final String sql = "select * from test\n";
    final String[] lines = {
        "fieldOne=1; fieldTwo=abc; fieldThree=1.2,"
        + " fieldOne=2; fieldTwo=def; fieldThree=3.4,"
        + " fieldOne=3; fieldTwo=xyz; fieldThree=5.6"
    };
    sql("bug", sql)
        .returns(lines)
        .ok();
  }

  /** Fluent API to perform test actions. */
  private class Fluent {
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
