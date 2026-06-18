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
import org.apache.calcite.sql.parser.SqlParser;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.containsString;
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

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6542">[CALCITE-6542]
   * Jdbc adapter support use a unified alias when generating SQL</a>. */
  @Test void testAlwaysUseExprAlias() throws Exception {
    String sql = "select avg(1) from table1";
    SqlParser.Config config = SqlParser.config();
    SqlParser parser = SqlParser.create(sql, config);
    SqlNode ast = parser.parseQuery();

    // Test with alwaysUseExprAlias = true
    SqlDialect.Context ctxTrue = SqlDialect.EMPTY_CONTEXT
        .withAlwaysUseExprAlias(true);
    SqlDialect dialectTrue = new SqlDialect(ctxTrue);
    String resultTrue = ast.toSqlString(dialectTrue).getSql();
    assertThat(resultTrue, containsString("EXPR$0"));

    // Test with alwaysUseExprAlias = false
    SqlDialect.Context ctxFalse = SqlDialect.EMPTY_CONTEXT
        .withAlwaysUseExprAlias(false);
    SqlDialect dialectFalse = new SqlDialect(ctxFalse);
    String resultFalse = ast.toSqlString(dialectFalse).getSql();
    // Should not contain EXPR$ when alwaysUseExprAlias is false
    Pattern exprPattern = Pattern.compile("EXPR\\$\\d+");
    assertThat(!exprPattern.matcher(resultFalse).find(), is(true));
  }

  @Test void testAlwaysUseExprAliasWithExistingAlias() throws Exception {
    String sql = "select avg(1) as myalias from table1";
    SqlParser.Config config = SqlParser.config();
    SqlParser parser = SqlParser.create(sql, config);
    SqlNode ast = parser.parseQuery();

    // Test with alwaysUseExprAlias = true - should keep the explicit alias
    SqlDialect.Context ctxTrue = SqlDialect.EMPTY_CONTEXT
        .withAlwaysUseExprAlias(true);
    SqlDialect dialectTrue = new SqlDialect(ctxTrue);
    String resultTrue = ast.toSqlString(dialectTrue).getSql();
    assertThat(resultTrue, containsString("MYALIAS"));
  }

  @Test void testAlwaysUseExprAliasWithMultipleItems() throws Exception {
    String sql = "select avg(1), sum(2), count(*) from table1";
    SqlParser.Config config = SqlParser.config();
    SqlParser parser = SqlParser.create(sql, config);
    SqlNode ast = parser.parseQuery();

    // Test with alwaysUseExprAlias = true
    SqlDialect.Context ctxTrue = SqlDialect.EMPTY_CONTEXT
        .withAlwaysUseExprAlias(true);
    SqlDialect dialectTrue = new SqlDialect(ctxTrue);
    String resultTrue = ast.toSqlString(dialectTrue).getSql();
    assertThat(resultTrue, containsString("EXPR$0"));
    assertThat(resultTrue, containsString("EXPR$1"));
    assertThat(resultTrue, containsString("EXPR$2"));
  }

  @Test void testAlwaysUseExprAliasWithMixedAliases() throws Exception {
    String sql = "select avg(1) as avg_val, sum(2), count(*) as cnt from table1";
    SqlParser.Config config = SqlParser.config();
    SqlParser parser = SqlParser.create(sql, config);
    SqlNode ast = parser.parseQuery();

    // Test with alwaysUseExprAlias = true
    SqlDialect.Context ctxTrue = SqlDialect.EMPTY_CONTEXT
        .withAlwaysUseExprAlias(true);
    SqlDialect dialectTrue = new SqlDialect(ctxTrue);
    String resultTrue = ast.toSqlString(dialectTrue).getSql();
    // Explicit aliases should be preserved
    assertThat(resultTrue, containsString("AVG_VAL"));
    assertThat(resultTrue, containsString("CNT"));
    // Only the item without explicit alias should get EXPR$
    assertThat(resultTrue, containsString("EXPR$1"));
  }
}
