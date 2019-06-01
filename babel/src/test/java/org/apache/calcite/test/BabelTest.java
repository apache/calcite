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

import static org.junit.Assert.assertEquals;

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

  @Test public void testCastingToText() throws SQLException {
    try (Connection connect = connect();
        Statement statement = connect.createStatement();
        ResultSet rs = statement
            .executeQuery("SELECT CAST(EXPR$0 AS text) FROM (VALUES (1, 2, 3))")) {
      ResultSetMetaData md = rs.getMetaData();
      assertEquals(Types.VARCHAR, md.getColumnType(1));
      assertEquals("TEXT", md.getColumnTypeName(1));
    }
  }
}

// End BabelTest.java
