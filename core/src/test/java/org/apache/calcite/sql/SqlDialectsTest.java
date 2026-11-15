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
package org.apache.calcite.sql;

import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link SqlDialects}.
 */
public class SqlDialectsTest {
  @Test void testCreateContextFromCalciteMetaData() throws SQLException {
    Connection connection =
        DriverManager.getConnection(Driver.CONNECT_STRING_PREFIX);
    DatabaseMetaData metaData = connection.getMetaData();

    SqlDialect.Context context = SqlDialects.createContext(metaData);
    assertThat(context.databaseProductName(),
        is(metaData.getDatabaseProductName()));
    assertThat(context.databaseMajorVersion(),
        is(metaData.getDatabaseMajorVersion()));
  }

  /** The fallback dialect for an unrecognized database product doubles
   * backslashes when quoting string literals: the backend's lexer is
   * unknown, so a trailing backslash must not be able to consume the
   * closing quote and shift the string boundary. */
  @Test void testUnknownProductFallbackEscapesBackslash() {
    SqlDialect dialect =
        SqlDialectFactoryImpl.ansiFallback(AnsiSqlDialect.DEFAULT_CONTEXT);
    // Quote doubling.
    assertThat(dialect.quoteStringLiteral("can't run"), is("'can''t run'"));
    // Backslashes are doubled defensively.
    assertThat(dialect.quoteStringLiteral("x\\"), is("'x\\\\'"));
    assertThat(dialect.quoteStringLiteral("x\\' extra"),
        is("'x\\\\'' extra'"));
  }

  /** An unrecognized JDBC product name (Calcite's own driver has no entry in
   * {@link SqlDialectFactoryImpl}) resolves to the fallback dialect. */
  @Test void testFactoryFallbackEscapesBackslash() throws SQLException {
    Connection connection =
        DriverManager.getConnection(Driver.CONNECT_STRING_PREFIX);
    SqlDialect dialect =
        new SqlDialectFactoryImpl().create(connection.getMetaData());
    assertThat(dialect.quoteStringLiteral("x\\"), is("'x\\\\'"));
  }
}
