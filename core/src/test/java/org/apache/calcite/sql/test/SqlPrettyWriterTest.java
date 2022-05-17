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
package org.apache.calcite.sql.test;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.test.DiffRepository;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.apache.calcite.test.Matchers.isLinux;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Unit test for {@link SqlPrettyWriter}.
 *
 * <p>You must provide the system property "source.dir".
 */
class SqlPrettyWriterTest {
  /** Fixture that can be re-used by other tests. */
  public static final SqlPrettyWriterFixture FIXTURE =
      new SqlPrettyWriterFixture(null, "?", false, null, "${formatted}",
          w -> w);

  /** Fixture that is local to this test. */
  private static final SqlPrettyWriterFixture LOCAL_FIXTURE =
      FIXTURE.withDiffRepos(DiffRepository.lookup(SqlPrettyWriterTest.class));

  /** Returns the default fixture for tests. Sub-classes may override. */
  protected SqlPrettyWriterFixture fixture() {
    return LOCAL_FIXTURE;
  }

  /** Returns a fixture with a given SQL query. */
  public final SqlPrettyWriterFixture sql(String sql) {
    return fixture().withSql(sql);
  }

  /** Returns a fixture with a given SQL expression. */
  public final SqlPrettyWriterFixture expr(String sql) {
    return fixture().withSql(sql).withExpr(true);
  }

  /** Creates a fluent test for a SQL statement that has most common lexical
   * features. */
  private SqlPrettyWriterFixture simple() {
    return sql("select x as a, b as b, c as c, d,"
        + " 'mixed-Case string',"
        + " unquotedCamelCaseId,"
        + " \"quoted id\" "
        + "from"
        + " (select *"
        + " from t"
        + " where x = y and a > 5"
        + " group by z, zz"
        + " window w as (partition by c),"
        + "  w1 as (partition by c,d order by a, b"
        + "   range between interval '2:2' hour to minute preceding"
        + "    and interval '1' day following)) "
        + "order by gg");
  }

  /** Creates a fluent test for a SQL statement that contains "tableAlias.*". */
  private SqlPrettyWriterFixture tableDotStar() {
    return sql("select x as a, b, s.*, t.* "
        + "from"
        + " (select *"
        + " from t"
        + " where x = y and a > 5) "
        + "order by g desc, h asc, i");
  }

  // ~ Tests ----------------------------------------------------------------

  @Test void testDefault() {
    simple().check();
  }

  @Test void testIndent8() {
    simple()
        .expectingDesc("${desc}")
        .withWriter(w -> w.withIndentation(8))
        .check();
  }

  @Test void testClausesNotOnNewLine() {
    simple()
        .withWriter(w -> w.withClauseStartsLine(false))
        .check();
  }

  @Test void testTableDotStarClausesNotOnNewLine() {
    tableDotStar()
        .withWriter(w -> w.withClauseStartsLine(false))
        .check();
  }

  @Test void testSelectListItemsOnSeparateLines() {
    simple()
        .withWriter(w -> w.withSelectListItemsOnSeparateLines(true))
        .check();
  }

  @Test void testSelectListNoExtraIndentFlag() {
    simple()
        .withWriter(w -> w.withSelectListItemsOnSeparateLines(true)
            .withSelectListExtraIndentFlag(false)
            .withClauseEndsLine(true))
        .check();
  }

  @Test void testFold() {
    simple()
        .withWriter(w -> w.withLineFolding(SqlWriterConfig.LineFolding.FOLD)
            .withFoldLength(45))
        .check();
  }

  @Test void testChop() {
    simple()
        .withWriter(w -> w.withLineFolding(SqlWriterConfig.LineFolding.CHOP)
            .withFoldLength(45))
        .check();
  }

  @Test void testChopLeadingComma() {
    simple()
        .withWriter(w -> w.withLineFolding(SqlWriterConfig.LineFolding.CHOP)
            .withFoldLength(45)
            .withLeadingComma(true))
        .check();
  }

  @Test void testLeadingComma() {
    simple()
        .withWriter(w -> w.withLeadingComma(true)
            .withSelectListItemsOnSeparateLines(true)
            .withSelectListExtraIndentFlag(true))
        .check();
  }

  @Test void testClauseEndsLine() {
    simple()
        .withWriter(w -> w.withClauseEndsLine(true)
            .withLineFolding(SqlWriterConfig.LineFolding.WIDE)
            .withFoldLength(45))
        .check();
  }

  @Test void testClauseEndsLineTall() {
    simple()
        .withWriter(w -> w.withClauseEndsLine(true)
            .withLineFolding(SqlWriterConfig.LineFolding.TALL)
            .withFoldLength(45))
        .check();
  }

  @Test void testClauseEndsLineFold() {
    simple()
        .withWriter(w -> w.withClauseEndsLine(true)
            .withLineFolding(SqlWriterConfig.LineFolding.FOLD)
            .withFoldLength(45))
        .check();
  }

  /** Tests formatting a query with Looker's preferences. */
  @Test void testLooker() {
    simple()
        .withWriter(w -> w.withFoldLength(60)
            .withLineFolding(SqlWriterConfig.LineFolding.STEP)
            .withSelectFolding(SqlWriterConfig.LineFolding.TALL)
            .withFromFolding(SqlWriterConfig.LineFolding.TALL)
            .withWhereFolding(SqlWriterConfig.LineFolding.TALL)
            .withHavingFolding(SqlWriterConfig.LineFolding.TALL)
            .withClauseEndsLine(true))
        .check();
  }

  @Test void testKeywordsLowerCase() {
    simple()
        .withWriter(w -> w.withKeywordsLowerCase(true))
        .check();
  }

  @Test void testParenthesizeAllExprs() {
    simple()
        .withWriter(w -> w.withAlwaysUseParentheses(true))
        .check();
  }

  @Test void testOnlyQuoteIdentifiersWhichNeedIt() {
    simple()
        .withWriter(w -> w.withQuoteAllIdentifiers(false))
        .check();
  }

  @Test void testBlackSubQueryStyle() {
    // Note that ( is at the indent, SELECT is on the same line, and ) is
    // below it.
    simple()
        .withWriter(w -> w.withSubQueryStyle(SqlWriter.SubQueryStyle.BLACK))
        .check();
  }

  @Test void testBlackSubQueryStyleIndent0() {
    simple()
        .withWriter(w -> w.withSubQueryStyle(SqlWriter.SubQueryStyle.BLACK)
            .withIndentation(0))
        .check();
  }

  @Test void testValuesNewline() {
    sql("select * from (values (1, 2), (3, 4)) as t")
        .withWriter(w -> w.withValuesListNewline(true))
        .check();
  }

  @Test void testValuesLeadingCommas() {
    sql("select * from (values (1, 2), (3, 4)) as t")
        .withWriter(w -> w.withValuesListNewline(true)
            .withLeadingComma(true))
        .check();
  }

  @Disabled("default SQL parser cannot parse DDL")
  @Test void testExplain() {
    sql("explain select * from t")
        .check();
  }

  @Test void testCase() {
    // Note that CASE is rewritten to the searched form. Wish it weren't
    // so, but that's beyond the control of the pretty-printer.
    // todo: indent should be 4 not 8
    final String sql = "case 1\n"
        + " when 2 + 3 then 4\n"
        + " when case a when b then c else d end then 6\n"
        + " else 7\n"
        + "end";
    final String formatted = "CASE\n"
        + "WHEN 1 = 2 + 3\n"
        + "THEN 4\n"
        + "WHEN 1 = CASE\n"
        + "        WHEN `A` = `B`\n" // todo: indent should be 4 not 8
        + "        THEN `C`\n"
        + "        ELSE `D`\n"
        + "        END\n"
        + "THEN 6\n"
        + "ELSE 7\n"
        + "END";
    expr(sql)
        .withWriter(w -> w.withCaseClausesOnNewLines(true))
        .expectingFormatted(formatted)
        .check();
  }

  @Test void testCase2() {
    final String sql = "case 1"
        + " when 2 + 3 then 4"
        + " when case a when b then c else d end then 6"
        + " else 7 end";
    final String formatted = "CASE WHEN 1 = 2 + 3 THEN 4"
        + " WHEN 1 = CASE WHEN `A` = `B` THEN `C` ELSE `D` END THEN 6"
        + " ELSE 7 END";
    expr(sql)
        .expectingFormatted(formatted)
        .check();
  }

  @Test void testBetween() {
    // todo: remove leading
    expr("x not between symmetric y and z")
        .expectingFormatted("`X` NOT BETWEEN SYMMETRIC `Y` AND `Z`")
        .check();

    // space
  }

  @Test void testCast() {
    expr("cast(x + y as decimal(5, 10))")
        .expectingFormatted("CAST(`X` + `Y` AS DECIMAL(5, 10))")
        .check();
  }

  @Test void testLiteralChain() {
    final String sql = "'x' /* comment */ 'y'\n"
        + "  'z' ";
    final String formatted = "'x'\n"
        + "'y'\n"
        + "'z'";
    expr(sql).expectingFormatted(formatted).check();
  }

  @Test void testOverlaps() {
    final String sql = "(x,xx) overlaps (y,yy) or x is not null";
    final String formatted = "PERIOD (`X`, `XX`) OVERLAPS PERIOD (`Y`, `YY`)"
        + " OR `X` IS NOT NULL";
    expr(sql).expectingFormatted(formatted).check();
  }

  @Test void testUnion() {
    final String sql = "select * from t "
        + "union select * from ("
        + "  select * from u "
        + "  union select * from v) "
        + "union select * from w "
        + "order by a, b";
    sql(sql)
        .check();
  }

  @Test void testMultiset() {
    sql("values (multiset (select * from t))")
        .check();
  }

  @Test void testJoinComma() {
    final String sql = "select *\n"
        + "from x, y as y1, z, (select * from a, a2 as a3),\n"
        + " (select * from b) as b2\n"
        + "where p = q\n"
        + "and exists (select 1 from v, w)";
    sql(sql).check();
  }

  @Test void testInnerJoin() {
    sql("select * from x inner join y on x.k=y.k")
        .check();
  }

  @Test void testJoinTall() {
    sql("select * from x inner join y on x.k=y.k left join z using (a)")
        .withWriter(c -> c.withLineFolding(SqlWriterConfig.LineFolding.TALL))
        .check();
  }

  @Test void testJoinTallClauseEndsLine() {
    sql("select * from x inner join y on x.k=y.k left join z using (a)")
        .withWriter(c -> c.withLineFolding(SqlWriterConfig.LineFolding.TALL)
            .withClauseEndsLine(true))
        .check();
  }

  @Test void testJoinLateralSubQueryTall() {
    final String sql = "select *\n"
        + "from (select a from customers where b < c group by d) as c,\n"
        + " products,\n"
        + " lateral (select e from orders where exists (\n"
        + "    select 1 from promotions)) as t5\n"
        + "group by f";
    sql(sql)
        .withWriter(c -> c.withLineFolding(SqlWriterConfig.LineFolding.TALL))
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4401">[CALCITE-4401]
   * SqlJoin toString throws RuntimeException</a>. */
  @Test void testJoinClauseToString() {
    final String sql = "SELECT t.region_name, t0.o_totalprice\n"
        + "FROM (SELECT c_custkey, region_name\n"
        + "FROM tpch.out_tpch_vw__customer) AS t\n"
        + "INNER JOIN (SELECT o_custkey, o_totalprice\n"
        + "FROM tpch.out_tpch_vw__orders) AS t0 ON t.c_custkey = t0.o_custkey";

    final String expectedJoinString = "SELECT *\n"
        + "FROM (SELECT `C_CUSTKEY`, `REGION_NAME`\n"
        + "FROM `TPCH`.`OUT_TPCH_VW__CUSTOMER`) AS `T`\n"
        + "INNER JOIN (SELECT `O_CUSTKEY`, `O_TOTALPRICE`\n"
        + "FROM `TPCH`.`OUT_TPCH_VW__ORDERS`) AS `T0`"
        + " ON `T`.`C_CUSTKEY` = `T0`.`O_CUSTKEY`";

    sql(sql)
        .checkTransformedNode(root -> {
          assertThat(root, instanceOf(SqlSelect.class));
          SqlNode from = ((SqlSelect) root).getFrom();
          assertThat(from, notNullValue());
          assertThat(from.toString(), isLinux(expectedJoinString));
          return from;
        });
  }

  @Test void testWhereListItemsOnSeparateLinesOr() {
    final String sql = "select x"
        + " from y"
        + " where h is not null and i < j"
        + " or ((a or b) is true) and d not in (f,g)"
        + " or x <> z";
    sql(sql)
        .withWriter(w -> w.withSelectListItemsOnSeparateLines(true)
            .withSelectListExtraIndentFlag(false)
            .withWhereListItemsOnSeparateLines(true))
        .check();
  }

  @Test void testWhereListItemsOnSeparateLinesAnd() {
    final String sql = "select x"
        + " from y"
        + " where h is not null and (i < j"
        + " or ((a or b) is true)) and (d not in (f,g)"
        + " or v <> ((w * x) + y) * z)";
    sql(sql)
        .withWriter(w -> w.withSelectListItemsOnSeparateLines(true)
            .withSelectListExtraIndentFlag(false)
            .withWhereListItemsOnSeparateLines(true))
        .check();
  }

  /** As {@link #testWhereListItemsOnSeparateLinesAnd()}, but
   * with {@link SqlWriterConfig#clauseEndsLine ClauseEndsLine=true}. */
  @Test void testWhereListItemsOnSeparateLinesAndNewline() {
    final String sql = "select x"
        + " from y"
        + " where h is not null and (i < j"
        + " or ((a or b) is true)) and (d not in (f,g)"
        + " or v <> ((w * x) + y) * z)";
    sql(sql)
        .withWriter(w -> w.withSelectListItemsOnSeparateLines(true)
            .withSelectListExtraIndentFlag(false)
            .withWhereListItemsOnSeparateLines(true)
            .withClauseEndsLine(true))
        .check();
  }

  @Test void testUpdate() {
    final String sql = "update emp\n"
        + "set mgr = mgr + 1, deptno = 5\n"
        + "where deptno = 10 and name = 'Fred'";
    sql(sql)
        .check();
  }

  @Test void testUpdateNoLine() {
    final String sql = "update emp\n"
        + "set mgr = mgr + 1, deptno = 5\n"
        + "where deptno = 10 and name = 'Fred'";
    sql(sql)
        .withWriter(w -> w.withUpdateSetListNewline(false))
        .check();
  }

  @Test void testUpdateNoLine2() {
    final String sql = "update emp\n"
        + "set mgr = mgr + 1, deptno = 5\n"
        + "where deptno = 10 and name = 'Fred'";
    sql(sql)
        .withWriter(w -> w.withUpdateSetListNewline(false)
            .withClauseStartsLine(false))
        .check();
  }

  public static void main(String[] args) throws SqlParseException {
    final String sql = "select x as a, b as b, c as c, d,"
        + " 'mixed-Case string',"
        + " unquotedCamelCaseId,"
        + " \"quoted id\" "
        + "from"
        + " (select *"
        + " from t"
        + " where x = y and a > 5"
        + " group by z, zz"
        + " window w as (partition by c),"
        + "  w1 as (partition by c,d order by a, b"
        + "   range between interval '2:2' hour to minute preceding"
        + "    and interval '1' day following)) "
        + "order by gg desc nulls last, hh asc";
    final SqlNode node = SqlParser.create(sql).parseQuery();

    final SqlWriterConfig config = SqlPrettyWriter.config()
        .withLineFolding(SqlWriterConfig.LineFolding.STEP)
        .withSelectFolding(SqlWriterConfig.LineFolding.TALL)
        .withFromFolding(SqlWriterConfig.LineFolding.TALL)
        .withWhereFolding(SqlWriterConfig.LineFolding.TALL)
        .withHavingFolding(SqlWriterConfig.LineFolding.TALL)
        .withIndentation(4)
        .withClauseEndsLine(true);
    System.out.println(new SqlPrettyWriter(config).format(node));
  }
}
