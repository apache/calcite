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

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserFixture;
import org.apache.calcite.sql.parser.SqlParserTest;
import org.apache.calcite.sql.parser.StringAndPos;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.tools.Hoist;

import com.google.common.base.Throwables;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests the "Babel" SQL parser, that understands all dialects of SQL.
 */
class BabelParserTest extends SqlParserTest {

  @Override public SqlParserFixture fixture() {
    return super.fixture()
        .withTester(new BabelTesterImpl())
        .withConfig(c -> c.withParserFactory(SqlBabelParserImpl.FACTORY));
  }

  @Test void testReservedWords() {
    assertThat(isReserved("escape"), is(false));
  }

  /** {@inheritDoc}
   *
   * <p>Copy-pasted from base method, but with some key differences.
   */
  @Override @Test protected void testMetadata() {
    SqlAbstractParserImpl.Metadata metadata = fixture().parser().getMetadata();
    assertThat(metadata.isReservedFunctionName("ABS"), is(true));
    assertThat(metadata.isReservedFunctionName("FOO"), is(false));

    assertThat(metadata.isContextVariableName("CURRENT_USER"), is(true));
    assertThat(metadata.isContextVariableName("CURRENT_CATALOG"), is(true));
    assertThat(metadata.isContextVariableName("CURRENT_SCHEMA"), is(true));
    assertThat(metadata.isContextVariableName("ABS"), is(false));
    assertThat(metadata.isContextVariableName("FOO"), is(false));

    assertThat(metadata.isNonReservedKeyword("A"), is(true));
    assertThat(metadata.isNonReservedKeyword("KEY"), is(true));
    assertThat(metadata.isNonReservedKeyword("SELECT"), is(false));
    assertThat(metadata.isNonReservedKeyword("FOO"), is(false));
    assertThat(metadata.isNonReservedKeyword("ABS"), is(true)); // was false

    assertThat(metadata.isKeyword("ABS"), is(true));
    assertThat(metadata.isKeyword("CURRENT_USER"), is(true));
    assertThat(metadata.isKeyword("CURRENT_CATALOG"), is(true));
    assertThat(metadata.isKeyword("CURRENT_SCHEMA"), is(true));
    assertThat(metadata.isKeyword("KEY"), is(true));
    assertThat(metadata.isKeyword("SELECT"), is(true));
    assertThat(metadata.isKeyword("HAVING"), is(true));
    assertThat(metadata.isKeyword("A"), is(true));
    assertThat(metadata.isKeyword("BAR"), is(false));

    assertThat(metadata.isReservedWord("SELECT"), is(true));
    assertThat(metadata.isReservedWord("CURRENT_CATALOG"), is(false)); // was true
    assertThat(metadata.isReservedWord("CURRENT_SCHEMA"), is(false)); // was true
    assertThat(metadata.isReservedWord("KEY"), is(false));

    String jdbcKeywords = metadata.getJdbcKeywords();
    assertThat(jdbcKeywords.contains(",COLLECT,"), is(false)); // was true
    assertThat(!jdbcKeywords.contains(",SELECT,"), is(true));
  }

  @Test void testSelect() {
    final String sql = "select 1 from t";
    final String expected = "SELECT 1\n"
        + "FROM `T`";
    sql(sql).ok(expected);
  }

  @Test void testYearIsNotReserved() {
    final String sql = "select 1 as year from t";
    final String expected = "SELECT 1 AS `YEAR`\n"
        + "FROM `T`";
    sql(sql).ok(expected);
  }

  /** Tests that there are no reserved keywords. */
  @Disabled
  @Test void testKeywords() {
    final String[] reserved = {"AND", "ANY", "END-EXEC"};
    final StringBuilder sql = new StringBuilder("select ");
    final StringBuilder expected = new StringBuilder("SELECT ");
    for (String keyword : keywords(null)) {
      // Skip "END-EXEC"; I don't know how a keyword can contain '-'
      if (!Arrays.asList(reserved).contains(keyword)) {
        sql.append("1 as ").append(keyword).append(", ");
        expected.append("1 as `").append(keyword.toUpperCase(Locale.ROOT))
            .append("`,\n");
      }
    }
    sql.setLength(sql.length() - 2); // remove ', '
    expected.setLength(expected.length() - 2); // remove ',\n'
    sql.append(" from t");
    expected.append("\nFROM t");
    sql(sql.toString()).ok(expected.toString());
  }

  /** In Babel, AS is not reserved. */
  @Test void testAs() {
    final String expected = "SELECT `AS`\n"
        + "FROM `T`";
    sql("select as from t").ok(expected);
  }

  /** In Babel, DESC is not reserved. */
  @Test void testDesc() {
    final String sql = "select desc\n"
        + "from t\n"
        + "order by desc asc, desc desc";
    final String expected = "SELECT `DESC`\n"
        + "FROM `T`\n"
        + "ORDER BY `DESC`, `DESC` DESC";
    sql(sql).ok(expected);
  }

  /**
   * This is a failure test making sure the LOOKAHEAD for WHEN clause is 2 in Babel, where
   * in core parser this number is 1.
   *
   * @see SqlParserTest#testCaseExpression()
   * @see <a href="https://issues.apache.org/jira/browse/CALCITE-2847">[CALCITE-2847]
   * Optimize global LOOKAHEAD for SQL parsers</a>
   */
  @Test void testCaseExpressionBabel() {
    sql("case x when 2, 4 then 3 ^when^ then 5 else 4 end")
        .fails("(?s)Encountered \"when then\" at .*");
  }

  /** In Redshift, DATE is a function. It requires special treatment in the
   * parser because it is a reserved keyword.
   * (Curiously, TIMESTAMP and TIME are not functions.) */
  @Test void testDateFunction() {
    final String expected = "SELECT `DATE`(`X`)\n"
        + "FROM `T`";
    sql("select date(x) from t").ok(expected);
  }

  /** In Redshift, PostgreSQL the DATEADD, DATEDIFF and DATE_PART functions have
   * ordinary function syntax except that its first argument is a time unit
   * (e.g. DAY). We must not parse that first argument as an identifier. */
  @Test void testRedshiftFunctionsWithDateParts() {
    final String sql = "SELECT DATEADD(day, 1, t),\n"
        + " DATEDIFF(week, 2, t),\n"
        + " DATE_PART(year, t) FROM mytable";
    final String expected = "SELECT `DATEADD`(DAY, 1, `T`),"
        + " `DATEDIFF`(WEEK, 2, `T`), `DATE_PART`(YEAR, `T`)\n"
        + "FROM `MYTABLE`";

    sql(sql).ok(expected);
  }

  /** Overrides, adding tests for DATEADD, DATEDIFF, DATE_PART functions
   * in addition to EXTRACT. */
  @Override protected void checkTimeUnitCodes(
      Map<String, TimeUnit> timeUnitCodes) {
    super.checkTimeUnitCodes(timeUnitCodes);

    SqlParserFixture f = fixture()
        .withConfig(config -> config.withTimeUnitCodes(timeUnitCodes));

    timeUnitCodes.forEach((abbrev, timeUnit) -> {
      String sql = "SELECT "
          + "DATEADD(" + abbrev + ", 1, '2022-06-03 15:30:00.000'),"
          + "DATEDIFF(" + abbrev + ", '2021-06-03 12:00:00.000', '2022-06-03 15:30:00.000'),"
          + "DATE_PART(" + abbrev + ", '2022-06-03 15:30:00.000')";
      String expected = "SELECT "
          + "`DATEADD`(" + timeUnit + ", 1, '2022-06-03 15:30:00.000'), "
          + "`DATEDIFF`(" + timeUnit + ", '2021-06-03 12:00:00.000', '2022-06-03 15:30:00.000'), "
          + "`DATE_PART`(" + timeUnit + ", '2022-06-03 15:30:00.000')";
      f.sql(sql).ok(expected);
    });
    f.sql("SELECT DATEADD(^A^, 1, NOW())")
        .fails("'A' is not a valid datetime format");
    if (timeUnitCodes.containsKey("S")) {
      f.sql("SELECT DATEADD(S^.^A, 1, NOW())")
          .fails("(?s).*Encountered \".\" at .*");
    }
  }

  /** PostgreSQL and Redshift allow TIMESTAMP literals that contain only a
   * date part. */
  @Test void testShortTimestampLiteral() {
    sql("select timestamp '1969-07-20'")
        .ok("SELECT TIMESTAMP '1969-07-20 00:00:00'");
    // PostgreSQL allows the following. We should too.
    sql("select ^timestamp '1969-07-20 1:2'^")
        .fails("Illegal TIMESTAMP literal '1969-07-20 1:2': not in format "
            + "'yyyy-MM-dd HH:mm:ss'"); // PostgreSQL gives 1969-07-20 01:02:00
    sql("select ^timestamp '1969-07-20:23:'^")
        .fails("Illegal TIMESTAMP literal '1969-07-20:23:': not in format "
            + "'yyyy-MM-dd HH:mm:ss'"); // PostgreSQL gives 1969-07-20 23:00:00
  }

  /** Tests parsing PostgreSQL-style "::" cast operator. */
  @Test void testParseInfixCast()  {
    checkParseInfixCast("integer");
    checkParseInfixCast("varchar");
    checkParseInfixCast("boolean");
    checkParseInfixCast("double");
    checkParseInfixCast("bigint");

    final String sql = "select -('12' || '.34')::VARCHAR(30)::INTEGER as x\n"
        + "from t";
    final String expected = ""
        + "SELECT (- ('12' || '.34') :: VARCHAR(30) :: INTEGER) AS `X`\n"
        + "FROM `T`";
    sql(sql).ok(expected);
  }

  private void checkParseInfixCast(String sqlType) {
    String sql = "SELECT x::" + sqlType + " FROM (VALUES (1, 2)) as tbl(x,y)";
    String expected = "SELECT `X` :: " + sqlType.toUpperCase(Locale.ROOT) + "\n"
        + "FROM (VALUES (ROW(1, 2))) AS `TBL` (`X`, `Y`)";
    sql(sql).ok(expected);
  }

  /** Tests parsing MySQL-style "<=>" equal operator. */
  @Test void testParseNullSafeEqual()  {
    // x <=> y
    final String projectSql = "SELECT x <=> 3 FROM (VALUES (1, 2)) as tbl(x,y)";
    sql(projectSql).ok("SELECT (`X` <=> 3)\n"
        + "FROM (VALUES (ROW(1, 2))) AS `TBL` (`X`, `Y`)");
    final String filterSql = "SELECT y FROM (VALUES (1, 2)) as tbl(x,y) WHERE x <=> null";
    sql(filterSql).ok("SELECT `Y`\n"
        + "FROM (VALUES (ROW(1, 2))) AS `TBL` (`X`, `Y`)\n"
        + "WHERE (`X` <=> NULL)");
    final String joinConditionSql = "SELECT tbl1.y FROM (VALUES (1, 2)) as tbl1(x,y)\n"
        + "LEFT JOIN (VALUES (null, 3)) as tbl2(x,y) ON tbl1.x <=> tbl2.x";
    sql(joinConditionSql).ok("SELECT `TBL1`.`Y`\n"
        + "FROM (VALUES (ROW(1, 2))) AS `TBL1` (`X`, `Y`)\n"
        + "LEFT JOIN (VALUES (ROW(NULL, 3))) AS `TBL2` (`X`, `Y`) ON (`TBL1`.`X` <=> `TBL2`.`X`)");
    // (a, b) <=> (x, y)
    final String rowComparisonSql = "SELECT y\n"
        + "FROM (VALUES (1, 2)) as tbl(x,y) WHERE (x,y) <=> (null,2)";
    sql(rowComparisonSql).ok("SELECT `Y`\n"
        + "FROM (VALUES (ROW(1, 2))) AS `TBL` (`X`, `Y`)\n"
        + "WHERE ((ROW(`X`, `Y`)) <=> (ROW(NULL, 2)))");
    // the higher precedence
    final String highPrecedenceSql = "SELECT x <=> 3 + 3 FROM (VALUES (1, 2)) as tbl(x,y)";
    sql(highPrecedenceSql).ok("SELECT (`X` <=> (3 + 3))\n"
        + "FROM (VALUES (ROW(1, 2))) AS `TBL` (`X`, `Y`)");
    // the lower precedence
    final String lowPrecedenceSql = "SELECT NOT x <=> 3 FROM (VALUES (1, 2)) as tbl(x,y)";
    sql(lowPrecedenceSql).ok("SELECT (NOT (`X` <=> 3))\n"
        + "FROM (VALUES (ROW(1, 2))) AS `TBL` (`X`, `Y`)");
  }

  @Test void testCreateTableWithNoCollectionTypeSpecified() {
    final String sql = "create table foo (bar integer not null, baz varchar(30))";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER NOT NULL, `BAZ` VARCHAR(30))";
    sql(sql).ok(expected);
  }

  @Test void testCreateSetTable() {
    final String sql = "create set table foo (bar int not null, baz varchar(30))";
    final String expected = "CREATE SET TABLE `FOO` (`BAR` INTEGER NOT NULL, `BAZ` VARCHAR(30))";
    sql(sql).ok(expected);
  }

  @Test void testCreateMultisetTable() {
    final String sql = "create multiset table foo (bar int not null, baz varchar(30))";
    final String expected = "CREATE MULTISET TABLE `FOO` "
        + "(`BAR` INTEGER NOT NULL, `BAZ` VARCHAR(30))";
    sql(sql).ok(expected);
  }

  @Test void testCreateVolatileTable() {
    final String sql = "create volatile table foo (bar int not null, baz varchar(30))";
    final String expected = "CREATE VOLATILE TABLE `FOO` "
        + "(`BAR` INTEGER NOT NULL, `BAZ` VARCHAR(30))";
    sql(sql).ok(expected);
  }

  /** Similar to {@link #testHoist()} but using custom parser. */
  @Test void testHoistMySql() {
    // SQL contains back-ticks, which require MySQL's quoting,
    // and DATEADD, which requires Babel.
    final String sql = "select 1 as x,\n"
        + "  'ab' || 'c' as y\n"
        + "from `my emp` /* comment with 'quoted string'? */ as e\n"
        + "where deptno < 40\n"
        + "and DATEADD(day, 1, hiredate) > date '2010-05-06'";
    final SqlDialect dialect = MysqlSqlDialect.DEFAULT;
    final Hoist.Hoisted hoisted =
        Hoist.create(Hoist.config()
            .withParserConfig(
                dialect.configureParser(SqlParser.config())
                    .withParserFactory(SqlBabelParserImpl::new)))
            .hoist(sql);

    // Simple toString converts each variable to '?N'
    final String expected = "select ?0 as x,\n"
        + "  ?1 || ?2 as y\n"
        + "from `my emp` /* comment with 'quoted string'? */ as e\n"
        + "where deptno < ?3\n"
        + "and DATEADD(day, ?4, hiredate) > ?5";
    assertThat(hoisted.toString(), is(expected));

    // Custom string converts variables to '[N:TYPE:VALUE]'
    final String expected2 = "select [0:DECIMAL:1] as x,\n"
        + "  [1:CHAR:ab] || [2:CHAR:c] as y\n"
        + "from `my emp` /* comment with 'quoted string'? */ as e\n"
        + "where deptno < [3:DECIMAL:40]\n"
        + "and DATEADD(day, [4:DECIMAL:1], hiredate) > [5:DATE:2010-05-06]";
    assertThat(hoisted.substitute(SqlParserTest::varToStr), is(expected2));
  }

  /**
   * Babel parser's global {@code LOOKAHEAD} is larger than the core
   * parser's. This causes different parse error message between these two
   * parsers. Here we define a looser error checker for Babel, so that we can
   * reuse failure testing codes from {@link SqlParserTest}.
   *
   * <p>If a test case is written in this file -- that is, not inherited -- it
   * is still checked by {@link SqlParserTest}'s checker.
   */
  public static class BabelTesterImpl extends TesterImpl {
    @Override protected void checkEx(String expectedMsgPattern,
        StringAndPos sap, @Nullable Throwable thrown) {
      if (thrown != null && thrownByBabelTest(thrown)) {
        super.checkEx(expectedMsgPattern, sap, thrown);
      } else {
        checkExNotNull(sap, thrown);
      }
    }

    private boolean thrownByBabelTest(Throwable ex) {
      Throwable rootCause = Throwables.getRootCause(ex);
      StackTraceElement[] stackTrace = rootCause.getStackTrace();
      for (StackTraceElement stackTraceElement : stackTrace) {
        String className = stackTraceElement.getClassName();
        if (Objects.equals(className, BabelParserTest.class.getName())) {
          return true;
        }
      }
      return false;
    }

    private void checkExNotNull(StringAndPos sap,
        @Nullable Throwable thrown) {
      if (thrown == null) {
        throw new AssertionError("Expected query to throw exception, "
            + "but it did not; query [" + sap.sql
            + "]");
      }
    }
  }
}
