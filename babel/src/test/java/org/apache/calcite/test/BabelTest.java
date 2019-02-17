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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for Babel framework.
 */
public class BabelTest {

  static final String URL = "jdbc:calcite:";

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  static Connection connect() throws SQLException {
    return DriverManager.getConnection(URL,
        CalciteAssert.propBuilder()
            .set(CalciteConnectionProperty.PARSER_FACTORY,
                SqlBabelParserImpl.class.getName() + "#FACTORY")
            .build());
  }

  static Connection connect(String libraries) throws SQLException {
    return DriverManager.getConnection(URL,
        CalciteAssert.propBuilder()
            .set(CalciteConnectionProperty.PARSER_FACTORY,
                SqlBabelParserImpl.class.getName() + "#FACTORY")
            .set(CalciteConnectionProperty.FUN, libraries)
            .build());
  }

  @Test public void testFoo() {
    assertThat(1 + 1, is(2));
  }

  @Test public void testPostgreSQLCastingOp() throws SQLException {
    Connection connection = connect("standard,postgresql");
    Statement statement = connection.createStatement();
    Object[][] sqlTypes = {
        { "integer", Types.INTEGER },
        { "varchar", Types.VARCHAR },
        { "boolean", Types.BOOLEAN },
        { "double", Types.DOUBLE },
        { "bigint", Types.BIGINT } };
    for (Object[] sqlType : sqlTypes) {
      String sql = "SELECT x::" + sqlType[0] + " FROM (VALUES ('1', '2')) as tbl(x,y)";
      assertTrue(statement.execute(sql));
      ResultSet resultSet = statement.getResultSet();

      ResultSetMetaData metaData = resultSet.getMetaData();
      assertEquals("Invalid column count", 1, metaData.getColumnCount());
      assertEquals("Invalid column type", (int) sqlType[1], metaData.getColumnType(1));
    }
  }
}

// End BabelTest.java
