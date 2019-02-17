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
package org.apache.calcite.test;

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.function.UnaryOperator;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for Babel framework.
 */
public class BabelTest {

  static final String URL = "jdbc:calcite:";

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  static Connection connect() throws SQLException {
    return connect(UnaryOperator.identity());
  }

  static Connection connect(UnaryOperator<CalciteAssert.PropBuilder> propBuild)
      throws SQLException {
    final CalciteAssert.PropBuilder propBuilder =
        CalciteAssert.propBuilder()
            .set(CalciteConnectionProperty.PARSER_FACTORY,
                SqlBabelParserImpl.class.getName() + "#FACTORY");
    return DriverManager.getConnection(URL,
        propBuild.apply(propBuilder).build());
  }

  private Connection connectWithFun(String libraryList) throws SQLException {
    return connect(propBuilder ->
        propBuilder.set(CalciteConnectionProperty.FUN, libraryList));
  }

  @Test public void testInfixCast() throws SQLException {
    try (Connection connection = connectWithFun("standard,postgresql");
         Statement statement = connection.createStatement()) {
      checkInfixCast(statement, "integer", Types.INTEGER);
      checkInfixCast(statement, "varchar", Types.VARCHAR);
      checkInfixCast(statement, "boolean", Types.BOOLEAN);
      checkInfixCast(statement, "double", Types.DOUBLE);
      checkInfixCast(statement, "bigint", Types.BIGINT);
    }
  }

  private void checkInfixCast(Statement statement, String typeName, int sqlType)
      throws SQLException {
    final String sql = "SELECT x::" + typeName + "\n"
        + "FROM (VALUES ('1', '2')) as tbl(x, y)";
    try (ResultSet resultSet = statement.executeQuery(sql)) {
      final ResultSetMetaData metaData = resultSet.getMetaData();
      assertThat("Invalid column count", metaData.getColumnCount(), is(1));
      assertThat("Invalid column type", metaData.getColumnType(1),
          is(sqlType));
    }
  }
}

// End BabelTest.java
