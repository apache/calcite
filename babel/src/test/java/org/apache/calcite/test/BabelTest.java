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
import org.apache.calcite.sql.parser.SqlParserFixture;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Properties;
import java.util.function.UnaryOperator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * Unit tests for Babel framework.
 */
class BabelTest {

  static final String URL = "jdbc:calcite:";

  private static UnaryOperator<CalciteAssert.PropBuilder> useParserFactory() {
    return propBuilder ->
        propBuilder.set(CalciteConnectionProperty.PARSER_FACTORY,
            SqlBabelParserImpl.class.getName() + "#FACTORY");
  }

  private static UnaryOperator<CalciteAssert.PropBuilder> useLibraryList(
      String libraryList) {
    return propBuilder ->
        propBuilder.set(CalciteConnectionProperty.FUN, libraryList);
  }

  private static UnaryOperator<CalciteAssert.PropBuilder> useLenientOperatorLookup(
      boolean lenient) {
    return propBuilder ->
        propBuilder.set(CalciteConnectionProperty.LENIENT_OPERATOR_LOOKUP,
            Boolean.toString(lenient));
  }

  static Connection connect() throws SQLException {
    return connect(UnaryOperator.identity());
  }

  static Connection connect(UnaryOperator<CalciteAssert.PropBuilder> propBuild)
      throws SQLException {
    final CalciteAssert.PropBuilder propBuilder = CalciteAssert.propBuilder();
    final Properties info =
        propBuild.andThen(useParserFactory())
            .andThen(useLenientOperatorLookup(true))
            .apply(propBuilder)
            .build();
    return DriverManager.getConnection(URL, info);
  }

  @Test void testInfixCast() throws SQLException {
    try (Connection connection = connect(useLibraryList("standard,postgresql"));
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

  /** Tests that you can run tests via {@link Fixtures}. */
  @Test void testFixtures() {
    final SqlValidatorFixture v = Fixtures.forValidator();
    v.withSql("select ^1 + date '2002-03-04'^")
        .fails("(?s).*Cannot apply '\\+' to arguments of"
            + " type '<INTEGER> \\+ <DATE>'.*");

    v.withSql("select 1 + 2 as three")
        .type("RecordType(INTEGER NOT NULL THREE) NOT NULL");

    // 'as' as identifier is invalid with Core parser
    final SqlParserFixture p = Fixtures.forParser();
    p.sql("select ^as^ from t")
        .fails("(?s)Encountered \"as\".*");

    // 'as' as identifier is invalid if you use Babel's tester and Core parser
    p.sql("select ^as^ from t")
        .withTester(new BabelParserTest.BabelTesterImpl())
        .fails("(?s)Encountered \"as\".*");

    // 'as' as identifier is valid with Babel parser
    p.withConfig(c -> c.withParserFactory(SqlBabelParserImpl.FACTORY))
        .sql("select as from t")
        .ok("SELECT `AS`\n"
            + "FROM `T`");

    // Postgres cast is invalid with core parser
    p.sql("select 1 ^:^: integer as x")
        .fails("(?s).*Encountered \":\" at .*");
  }
}
