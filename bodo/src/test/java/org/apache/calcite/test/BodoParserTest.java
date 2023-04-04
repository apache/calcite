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

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserFixture;
import org.apache.calcite.sql.parser.SqlParserTest;
import org.apache.calcite.sql.parser.StringAndPos;
import org.apache.calcite.sql.parser.bodo.SqlBodoParserImpl;
import org.apache.calcite.tools.Hoist;

import com.google.common.base.Throwables;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests the Bodo SQL parser. We use a separate parser to
 * allow us to reduce the amount of changes needed to extend the parser.
 *
 */
public class BodoParserTest extends SqlParserTest {

  @Override public SqlParserFixture fixture() {
    return super.fixture()
        .withTester(new BodoTesterImpl())
        .withConfig(c -> c.withParserFactory(SqlBodoParserImpl.FACTORY));
  }


  @Test void testSelect() {
    final String sql = "select 1 from t";
    final String expected = "SELECT 1\n"
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


  /**
   * This is a failure test making sure the LOOKAHEAD for WHEN clause is 2 in BODO, where
   * in core parser this number is 1.
   *
   * @see SqlParserTest#testCaseExpression()
   * @see <a href="https://issues.apache.org/jira/browse/CALCITE-2847">[CALCITE-2847]
   * Optimize global LOOKAHEAD for SQL parsers</a>
   */
  @Test void testCaseExpressionBodo() {
    sql("case x when 2, 4 then 3 ^when^ then 5 else 4 end")
        .fails("(?s)Encountered \"when then\" at .*");
  }

  /** In Redshift, DATE is a function. It requires special treatment in the
   * parser because it is a reserved keyword.
   * (Curiously, TIMESTAMP and TIME are not functions.) */
  @Test void testDateFunction() {
    final String expected = "SELECT DATE(`X`)\n"
        + "FROM `T`";
    sql("select date(x) from t").ok(expected);
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


  /** Similar to {@link #testHoist()} but using custom parser. */
  @Test void testHoistMySql() {
    // SQL contains back-ticks, which require MySQL's quoting,
    // and DATEADD, which requires Babel/Bodo.
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
                    .withParserFactory(SqlBodoParserImpl::new)))
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


  @Test void testCreateTable() {
    //Tests certain clauses that parse, but are currently unsupported (throw errors in validation)

    // Volatile is supported in SF, so we may get to it soonish
    final String q1 = "CREATE VOLATILE TABLE out_test AS select 1, 2, 3 from emp";
    final String q1_expected = "CREATE VOLATILE TABLE `OUT_TEST` AS\n"
        + "SELECT 1, 2, 3\n"
        + "FROM `EMP`";

    sql(q1).ok(q1_expected);

  }


  /**
   * Bodo's parser's global {@code LOOKAHEAD} is larger than the core
   * parser's. This causes different parse error message between these two
   * parsers. Here we define a looser error checker for Bodo, so that we can
   * reuse failure testing codes from {@link SqlParserTest}.
   *
   * <p>If a test case is written in this file -- that is, not inherited -- it
   * is still checked by {@link SqlParserTest}'s checker.
   */
  public static class BodoTesterImpl extends TesterImpl {
    @Override protected void checkEx(String expectedMsgPattern,
        StringAndPos sap, @Nullable Throwable thrown) {
      if (thrown != null && thrownByBodoTest(thrown)) {
        super.checkEx(expectedMsgPattern, sap, thrown);
      } else {
        checkExNotNull(sap, thrown);
      }
    }

    private boolean thrownByBodoTest(Throwable ex) {
      Throwable rootCause = Throwables.getRootCause(ex);
      StackTraceElement[] stackTrace = rootCause.getStackTrace();
      for (StackTraceElement stackTraceElement : stackTrace) {
        String className = stackTraceElement.getClassName();
        if (Objects.equals(className, BodoParserTest.class.getName())) {
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
