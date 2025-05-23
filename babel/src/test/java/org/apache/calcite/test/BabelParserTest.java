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
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
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
import java.util.Objects;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;

/**
 * Tests the "Babel" SQL parser, that understands all dialects of SQL.
 */
class BabelParserTest extends SqlParserTest {

  @Override public SqlParserFixture fixture() {
    return super.fixture()
        .withTester(new BabelTesterImpl())
        .withConfig(c -> c.withParserFactory(SqlBabelParserImpl.FACTORY));
  }

  /** Tests that the Babel parser correctly parses a CAST to INTERVAL type
   * in PostgreSQL dialect. */
  @Test void testCastToInterval() {
    SqlParserFixture postgreF = fixture().withDialect(PostgresqlSqlDialect.DEFAULT);
    postgreF.sql("select cast(x as interval)")
        .ok("SELECT CAST(\"x\" AS INTERVAL SECOND)");
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

  @Test void testIdentifier() {
    // MySQL supports identifiers started with numbers
    SqlParserFixture mysqlF = fixture().withDialect(MysqlSqlDialect.DEFAULT);
    mysqlF.sql("select 1 as 1_c1 from t")
        .ok("SELECT 1 AS `1_c1`\n"
            + "FROM `t`");

    // PostgreSQL allows identifier
    // to begin with a letter (a-z, but also letters with diacritical marks and non-Latin letters)
    // or an underscore (_). Subsequent characters in an identifier
    // can be letters, underscores, digits (0-9), or dollar signs ($)
    SqlParserFixture postgreF = fixture().withDialect(PostgresqlSqlDialect.DEFAULT);
    postgreF.sql("select 1 as \200_$\251\377 from t")
        .ok("SELECT 1 AS \"\200_$\251\377\"\n"
            + "FROM \"t\"");
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

  /** The DATEADD, DATEDIFF (in Redshift, Snowflake) and DATE_PART (in PostgreSQL)
   * functions have ordinary function syntax  except that its first argument is a time unit
   * (e.g. DAY). We must not parse that first argument as an identifier. */
  @Test void testRedshiftFunctionsWithDateParts() {
    final String sql = "SELECT DATEADD(day, 1, t),\n"
        + " DATEDIFF(week, 2, t),\n"
        + " DATE_PART(year, t) FROM mytable";
    final String expected = "SELECT DATEADD(DAY, 1, `T`),"
        + " DATEDIFF(WEEK, 2, `T`), DATE_PART(YEAR, `T`)\n"
        + "FROM `MYTABLE`";

    sql(sql).ok(expected);
  }

  /** Overrides, adding tests for DATEADD, DATEDIFF, DATE_PART functions
   * in addition to EXTRACT. */
  @Test protected void testTimeUnitCodes() {
    super.testTimeUnitCodes();

    // As for FLOOR in the base class, so for DATEADD, DATEDIFF, DATE_PART.
    // Extensions such as 'y' remain as identifiers; they are resolved in the
    // validator.
    final String ts = "'2022-06-03 12:00:00.000'";
    final String ts2 = "'2022-06-03 15:30:00.000'";
    expr("DATEADD(year, 1, " + ts + ")")
        .ok("DATEADD(YEAR, 1, " + ts + ")");
    expr("DATEADD(y, 1, " + ts + ")")
        .ok("DATEADD(`Y`, 1, " + ts + ")");
    expr("DATEDIFF(year, 1, " + ts + ", " + ts2 + ")")
        .ok("DATEDIFF(YEAR, 1, '2022-06-03 12:00:00.000', "
            + "'2022-06-03 15:30:00.000')");
    expr("DATEDIFF(y, 1, " + ts + ", " + ts2 + ")")
        .ok("DATEDIFF(`Y`, 1, '2022-06-03 12:00:00.000', "
            + "'2022-06-03 15:30:00.000')");
    expr("DATE_PART(year, " + ts + ")")
        .ok("DATE_PART(YEAR, '2022-06-03 12:00:00.000')");
    expr("DATE_PART(y, " + ts + ")")
        .ok("DATE_PART(`Y`, '2022-06-03 12:00:00.000')");
  }

  /** PostgreSQL and Redshift allow TIMESTAMP literals that contain only a
   * date part. */
  @Test void testShortTimestampLiteral() {
    // Parser doesn't actually check the contents of the string. The validator
    // will convert it to '1969-07-20 00:00:00', when it has decided that
    // TIMESTAMP maps to the TIMESTAMP type.
    sql("select timestamp '1969-07-20'")
        .ok("SELECT TIMESTAMP '1969-07-20'");
    // PostgreSQL allows the following. We should too.
    sql("select ^timestamp '1969-07-20 1:2'^")
        .ok("SELECT TIMESTAMP '1969-07-20 1:2'");
    sql("select ^timestamp '1969-07-20:23:'^")
        .ok("SELECT TIMESTAMP '1969-07-20:23:'");
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

  @Test void testCreateTableMapType() {
    final String sql = "create table foo (bar map<integer, varchar>)";
    final String expected = "CREATE TABLE `FOO` (`BAR` MAP< INTEGER, VARCHAR >)";
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

  @Test void testCreateVariantTable() {
    final String sql = "create table foo (bar variant not null)";
    final String expected = "CREATE TABLE `FOO` "
        + "(`BAR` VARIANT NOT NULL)";
    sql(sql).ok(expected);
  }

  @Test void testArrayLiteralFromString() {
    sql("select array '{1,2,3}'")
        .ok("SELECT (ARRAY[1, 2, 3])");
    sql("select array '{{1,2,5}, {3,4,7}}'")
        .ok("SELECT (ARRAY[(ARRAY[1, 2, 5]), (ARRAY[3, 4, 7])])");
    sql("select array '{}'")
        .ok("SELECT (ARRAY[])");
    sql("select array '{\"1\", \"2\", \"3\"}'")
        .ok("SELECT (ARRAY['1', '2', '3'])");
    sql("select array '{null, 1, null, 2}'")
        .ok("SELECT (ARRAY[NULL, 1, NULL, 2])");

    sql("select array ^'null, 1, null, 2'^")
        .fails("Illegal array expression 'null, 1, null, 2'");
  }

  @Test void testArrayLiteralBigQuery() {
    final SqlParserFixture f = fixture().withDialect(BIG_QUERY);
    f.sql("select array '{1, 2}'")
        .ok("SELECT (ARRAY[1, 2])");
    f.sql("select array \"{1, 2}\"")
        .ok("SELECT (ARRAY[1, 2])");
    f.sql("select array '{\"a\", \"b\"}'")
        .ok("SELECT (ARRAY['a', 'b'])");
    f.sql("select array \"{\\\"a\\\", \\\"b\\\"}\"")
        .ok("SELECT (ARRAY['a', 'b'])");
  }

  @Test void testPostgresSqlShow() {
    SqlParserFixture f = fixture().withDialect(PostgresqlSqlDialect.DEFAULT);
    f.sql("SHOW autovacuum")
        .ok("SHOW \"autovacuum\"");
    f.sql("SHOW \"autovacuum\"")
        .same();
    f.sql("SHOW ALL")
        .ok("SHOW \"all\"");
    f.sql("SHOW TIME ZONE")
        .ok("SHOW \"timezone\"");
    f.sql("SHOW SESSION AUTHORIZATION")
        .ok("SHOW \"session_authorization\"");
    f.sql("SHOW TRANSACTION ISOLATION LEVEL")
        .ok("SHOW \"transaction_isolation\"");
  }

  @Test void testPostgresSqlSetOption() {
    SqlParserFixture f = fixture().withDialect(PostgresqlSqlDialect.DEFAULT);
    f.sql("SET SESSION autovacuum = true")
        .ok("SET \"autovacuum\" = TRUE");
    f.sql("SET SESSION autovacuum = DEFAULT")
        .ok("SET \"autovacuum\" = DEFAULT");
    f.sql("SET LOCAL autovacuum TO 'DEFAULT'")
        .ok("SET LOCAL \"autovacuum\" = 'DEFAULT'");

    f.sql("SET SESSION TIME ZONE DEFAULT")
        .ok("SET TIME ZONE DEFAULT");
    f.sql("SET SESSION TIME ZONE LOCAL")
        .ok("SET TIME ZONE LOCAL");
    f.sql("SET TIME ZONE 'PST8PDT'").same();
    f.sql("SET TIME ZONE INTERVAL '-08:00' HOUR TO MINUTE").same();
    f.sql("SET timezone = 'PST8PDT'")
            .ok("SET \"timezone\" = 'PST8PDT'");

    f.sql("SET SESSION AUTHORIZATION DEFAULT")
        .ok("SET \"session_authorization\" = DEFAULT");

    f.sql("SET search_path = public,public,\"$user\"")
        .ok("SET \"search_path\" = \"public\", \"public\", \"$user\"");
    f.sql("SET SCHEMA public,public,\"$user\"")
        .ok("SET \"search_path\" = \"public\", \"public\", \"$user\"");
    f.sql("SET NAMES iso_8859_15_to_utf8")
        .ok("SET \"client_encoding\" = \"iso_8859_15_to_utf8\"");

    f.sql("SET TRANSACTION READ ONLY").same();
    f.sql("SET TRANSACTION READ WRITE").same();
    f.sql("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE").same();
    f.sql("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE").same();
    f.sql("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ WRITE, NOT DEFERRABLE").same();

    f.sql("SET TRANSACTION SNAPSHOT '000003A1-1'").same();

    f.sql("SET ROLE NONE").same();
    f.sql("SET ROLE 'paul'").same();
  }

  @Test void testPostgresSqlReset() {
    SqlParserFixture f = fixture().withDialect(PostgresqlSqlDialect.DEFAULT);

    // RESET ROLE, RESET SESSION AUTHORIZATION, RESET TRANSACTION ISOLATION LEVEL,
    // and RESET TIME ZONE are simply syntactic sugar for a more unified syntax
    // RESET "<variable_name>".
    f.sql("RESET ALL").same();
    f.sql("RESET ROLE")
        .ok("RESET \"role\"");
    f.sql("RESET SESSION AUTHORIZATION")
        .ok("RESET \"session_authorization\"");
    f.sql("RESET TRANSACTION ISOLATION LEVEL")
        .ok("RESET \"transaction_isolation\"");
    f.sql("RESET TIME ZONE")
        .ok("RESET \"timezone\"");
    f.sql("RESET autovacuum")
        .ok("RESET \"autovacuum\"");
  }

  @Test void testPostgresSqlBegin() {
    SqlParserFixture f = fixture().withDialect(PostgresqlSqlDialect.DEFAULT);
    f.sql("BEGIN").same();
    f.sql("BEGIN READ ONLY").same();
    f.sql("BEGIN TRANSACTION READ WRITE")
        .ok("BEGIN READ WRITE");
    f.sql("BEGIN WORK ISOLATION LEVEL SERIALIZABLE")
        .ok("BEGIN ISOLATION LEVEL SERIALIZABLE");
    f.sql("BEGIN ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE").same();
    f.sql("BEGIN ISOLATION LEVEL SERIALIZABLE, READ WRITE, NOT DEFERRABLE").same();
  }

  @Test void testPostgresSqlCommit() {
    SqlParserFixture f = fixture().withDialect(PostgresqlSqlDialect.DEFAULT);
    f.sql("COMMIT").same();
    f.sql("COMMIT WORK")
        .ok("COMMIT");
    f.sql("COMMIT TRANSACTION")
        .ok("COMMIT");
    f.sql("COMMIT AND NO CHAIN")
        .ok("COMMIT");
    f.sql("COMMIT AND CHAIN").same();
  }

  @Test void testPostgresSqlRollback() {
    SqlParserFixture f = fixture().withDialect(PostgresqlSqlDialect.DEFAULT);
    f.sql("ROLLBACK").same();
    f.sql("ROLLBACK WORK")
        .ok("ROLLBACK");
    f.sql("ROLLBACK TRANSACTION")
        .ok("ROLLBACK");
    f.sql("ROLLBACK AND NO CHAIN")
        .ok("ROLLBACK");
    f.sql("ROLLBACK AND CHAIN").same();
  }

  @Test void testPostgresSqlDiscard() {
    SqlParserFixture f = fixture().withDialect(PostgresqlSqlDialect.DEFAULT);
    f.sql("DISCARD ALL").same();
    f.sql("DISCARD PLANS").same();
    f.sql("DISCARD SEQUENCES").same();
    f.sql("DISCARD TEMPORARY").same();
    f.sql("DISCARD TEMP").same();
  }

  @Test void testSparkLeftAntiJoin() {
    final SqlParserFixture f = fixture().withDialect(SparkSqlDialect.DEFAULT);
    final String sql = "select a.cid, a.cname, count(1) as amount\n"
        + "from geo.area1 as a\n"
        + "left anti join (select distinct cid, cname\n"
        + "from geo.area2\n"
        + "where cname = 'cityA') as b on a.cid = b.cid\n"
        + "group by a.cid, a.cname";
    final String expected = "SELECT `A`.`CID`, `A`.`CNAME`, COUNT(1) `AMOUNT`\n"
        + "FROM `GEO`.`AREA1` `A`\n"
        + "LEFT ANTI JOIN (SELECT DISTINCT `CID`, `CNAME`\n"
        + "FROM `GEO`.`AREA2`\n"
        + "WHERE (`CNAME` = 'cityA')) `B` ON (`A`.`CID` = `B`.`CID`)\n"
        + "GROUP BY `A`.`CID`, `A`.`CNAME`";
    f.sql(sql).ok(expected);
  }

  @Test void testLeftAntiJoin() {
    final String sql = "select a.cid, a.cname, count(1) as amount\n"
        + "from geo.area1 as a\n"
        + "left anti join (select distinct cid, cname\n"
        + "from geo.area2\n"
        + "where cname = 'cityA') as b on a.cid = b.cid\n"
        + "group by a.cid, a.cname";
    final String expected = "SELECT `A`.`CID`, `A`.`CNAME`, COUNT(1) AS `AMOUNT`\n"
        + "FROM `GEO`.`AREA1` AS `A`\n"
        + "LEFT ANTI JOIN (SELECT DISTINCT `CID`, `CNAME`\n"
        + "FROM `GEO`.`AREA2`\n"
        + "WHERE (`CNAME` = 'cityA')) AS `B` ON (`A`.`CID` = `B`.`CID`)\n"
        + "GROUP BY `A`.`CID`, `A`.`CNAME`";
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
    assertThat(hoisted, hasToString(expected));

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
