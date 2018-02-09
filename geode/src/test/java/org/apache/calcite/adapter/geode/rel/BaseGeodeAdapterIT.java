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
package org.apache.calcite.adapter.geode.rel;

import org.apache.calcite.linq4j.function.Function1;

import org.junit.Assert;

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
import java.util.List;
import java.util.Properties;

/**
 * Geode Adapter IT Test
 */
public class BaseGeodeAdapterIT {

  /**
   * Returns a function that checks the contents of a result set against an
   * expected string.
   */
  private static Function1<ResultSet, Void> expect(final String... expected) {
    return new Function1<ResultSet, Void>() {
      public Void apply(ResultSet resultSet) {
        try {
          final List<String> lines = new ArrayList<>();
          BaseGeodeAdapterIT.collect(lines, resultSet);
          Assert.assertEquals(Arrays.asList(expected), lines);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
        return null;
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
      result.add(toLinux(buf.toString()));
    }
  }

  public static String toLinux(String s) {
    return s.replaceAll("\r\n", "\n");
  }

  protected void checkSql(String model, String sql) throws SQLException {
    checkSql(sql, model, output());
  }

  protected Function1<ResultSet, Void> output() {
    return new Function1<ResultSet, Void>() {
      public Void apply(ResultSet resultSet) {
        try {
          output(resultSet, System.out);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
        return null;
      }
    };
  }

  protected void checkSql(String model, String sql, final String... expected)
      throws SQLException {
    checkSql(sql, model, expect(expected));
  }

  protected void checkSql(String sql, String model, Function1<ResultSet, Void> fn)
      throws SQLException {
    Connection connection = null;
    Statement statement = null;
    try {
      Properties info = new Properties();
      info.put("model", jsonPath(model));
      connection = DriverManager.getConnection("jdbc:calcite:", info);
      statement = connection.createStatement();
      final ResultSet resultSet = statement.executeQuery(sql);
      fn.apply(resultSet);
    } finally {
      close(connection, statement);
    }
  }

  private String jsonPath(String model) {
    return resourcePath(model + ".json");
  }

  private String resourcePath(String path) {
    final URL url = GeodeAdapterIT.class.getResource("/" + path);
    String s = url.toString();
    if (s.startsWith("file:")) {
      s = s.substring("file:".length());
    }
    return s;
  }

  private void output(ResultSet resultSet, PrintStream out)
      throws SQLException {
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    while (resultSet.next()) {
      for (int i = 1; true; i++) {
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
}

// End BaseGeodeAdapterIT.java
