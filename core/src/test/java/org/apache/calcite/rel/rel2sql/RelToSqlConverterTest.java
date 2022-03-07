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
package org.apache.calcite.rel.rel2sql;

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.AggregateJoinTransposeRule;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialect.Context;
import org.apache.calcite.sql.SqlDialect.DatabaseProduct;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.dialect.JethroDataSqlDialect;
import org.apache.calcite.sql.dialect.MssqlSqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.MockSqlOperatorTable;
import org.apache.calcite.test.RelBuilderTest;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.calcite.avatica.util.TimeUnit.DAY;
import static org.apache.calcite.avatica.util.TimeUnit.MICROSECOND;
import static org.apache.calcite.test.Matchers.isLinux;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link RelToSqlConverter}.
 */
class RelToSqlConverterTest {

  /** Initiates a test case with a given SQL query. */
  private Sql sql(String sql) {
    return new Sql(CalciteAssert.SchemaSpec.JDBC_FOODMART, sql,
        CalciteSqlDialect.DEFAULT, SqlParser.Config.DEFAULT,
        UnaryOperator.identity(), null, ImmutableList.of());
  }

  private Sql sqlTest(String sql) {
    return new Sql(CalciteAssert.SchemaSpec.FOODMART_TEST, sql,
        CalciteSqlDialect.DEFAULT, SqlParser.Config.DEFAULT,
        UnaryOperator.identity(), null, ImmutableList.of());
  }

  /** Initiates a test case with a given {@link RelNode} supplier. */
  private Sql relFn(Function<RelBuilder, RelNode> relFn) {
    return sql("?").relFn(relFn);
  }

  private static Planner getPlanner(List<RelTraitDef> traitDefs,
      SqlParser.Config parserConfig, SchemaPlus schema,
      SqlToRelConverter.Config sqlToRelConf, Program... programs) {
    final MockSqlOperatorTable operatorTable =
            new MockSqlOperatorTable(SqlStdOperatorTable.instance());
    MockSqlOperatorTable.addRamp(operatorTable);
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(parserConfig)
        .defaultSchema(schema)
        .traitDefs(traitDefs)
        .sqlToRelConverterConfig(sqlToRelConf)
        .programs(programs)
        .operatorTable(operatorTable)
        .build();
    return Frameworks.getPlanner(config);
  }

  private static JethroDataSqlDialect jethroDataSqlDialect() {
    Context dummyContext = SqlDialect.EMPTY_CONTEXT
        .withDatabaseProduct(SqlDialect.DatabaseProduct.JETHRO)
        .withDatabaseMajorVersion(1)
        .withDatabaseMinorVersion(0)
        .withDatabaseVersion("1.0")
        .withIdentifierQuoteString("\"")
        .withNullCollation(NullCollation.HIGH)
        .withJethroInfo(JethroDataSqlDialect.JethroInfo.EMPTY);
    return new JethroDataSqlDialect(dummyContext);
  }

  private static MysqlSqlDialect mySqlDialect(NullCollation nullCollation) {
    return new MysqlSqlDialect(MysqlSqlDialect.DEFAULT_CONTEXT
        .withNullCollation(nullCollation));
  }

  /** Returns a collection of common dialects, and the database products they
   * represent. */
  private static Map<SqlDialect, DatabaseProduct> dialects() {
    return ImmutableMap.<SqlDialect, DatabaseProduct>builder()
        .put(SqlDialect.DatabaseProduct.BIG_QUERY.getDialect(),
            SqlDialect.DatabaseProduct.BIG_QUERY)
        .put(SqlDialect.DatabaseProduct.CALCITE.getDialect(),
            SqlDialect.DatabaseProduct.CALCITE)
        .put(SqlDialect.DatabaseProduct.DB2.getDialect(),
            SqlDialect.DatabaseProduct.DB2)
        .put(SqlDialect.DatabaseProduct.HIVE.getDialect(),
            SqlDialect.DatabaseProduct.HIVE)
        .put(jethroDataSqlDialect(),
            SqlDialect.DatabaseProduct.JETHRO)
        .put(SqlDialect.DatabaseProduct.MSSQL.getDialect(),
            SqlDialect.DatabaseProduct.MSSQL)
        .put(SqlDialect.DatabaseProduct.MYSQL.getDialect(),
            SqlDialect.DatabaseProduct.MYSQL)
        .put(mySqlDialect(NullCollation.HIGH),
            SqlDialect.DatabaseProduct.MYSQL)
        .put(SqlDialect.DatabaseProduct.ORACLE.getDialect(),
            SqlDialect.DatabaseProduct.ORACLE)
        .put(SqlDialect.DatabaseProduct.POSTGRESQL.getDialect(),
            SqlDialect.DatabaseProduct.POSTGRESQL)
        .put(DatabaseProduct.PRESTO.getDialect(),
            DatabaseProduct.PRESTO)
        .build();
  }

  /** Creates a RelBuilder. */
  private static RelBuilder relBuilder() {
    return RelBuilder.create(RelBuilderTest.config().build());
  }

  /** Converts a relational expression to SQL. */
  private String toSql(RelNode root) {
    return toSql(root, SqlDialect.DatabaseProduct.CALCITE.getDialect());
  }

  /** Converts a relational expression to SQL in a given dialect. */
  private static String toSql(RelNode root, SqlDialect dialect) {
    return toSql(root, dialect, c ->
        c.withAlwaysUseParentheses(false)
            .withSelectListItemsOnSeparateLines(false)
            .withUpdateSetListNewline(false)
            .withIndentation(0));
  }

  /** Converts a relational expression to SQL in a given dialect
   * and with a particular writer configuration. */
  private static String toSql(RelNode root, SqlDialect dialect,
      UnaryOperator<SqlWriterConfig> transform) {
    final RelToSqlConverter converter = new RelToSqlConverter(dialect);
    final SqlNode sqlNode = converter.visitRoot(root).asStatement();
    return sqlNode.toSqlString(c -> transform.apply(c.withDialect(dialect)))
        .getSql();
  }

  @Test public void testSimpleSelectWithOrderByAliasAsc() {
    final String query = "select sku+1 as a from \"product\" order by a";
    final String bigQueryExpected = "SELECT SKU + 1 AS A\nFROM foodmart.product\n"
        + "ORDER BY A IS NULL, A";
    final String hiveExpected = "SELECT SKU + 1 A\nFROM foodmart.product\n"
        + "ORDER BY A IS NULL, A";
    sql(query)
        .withBigQuery()
        .ok(bigQueryExpected)
        .withHive()
        .ok(hiveExpected);
  }

  @Test public void testSimpleSelectWithOrderByAliasDesc() {
    final String query = "select sku+1 as a from \"product\" order by a desc";
    final String bigQueryExpected = "SELECT SKU + 1 AS A\nFROM foodmart.product\n"
        + "ORDER BY A IS NULL DESC, A DESC";
    final String hiveExpected = "SELECT SKU + 1 A\nFROM foodmart.product\n"
        + "ORDER BY A IS NULL DESC, A DESC";
    sql(query)
        .withBigQuery()
        .ok(bigQueryExpected)
        .withHive()
        .ok(hiveExpected);
  }

  @Test void testSimpleSelectStarFromProductTable() {
    String query = "select * from \"product\"";
    sql(query).ok("SELECT *\nFROM \"foodmart\".\"product\"");
  }

  @Test void testAggregateFilterWhereToSqlFromProductTable() {
    String query = "select\n"
        + "  sum(\"shelf_width\") filter (where \"net_weight\" > 0),\n"
        + "  sum(\"shelf_width\")\n"
        + "from \"foodmart\".\"product\"\n"
        + "where \"product_id\" > 0\n"
        + "group by \"product_id\"";
    final String expected = "SELECT"
        + " SUM(\"shelf_width\") FILTER (WHERE \"net_weight\" > 0 IS TRUE),"
        + " SUM(\"shelf_width\")\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE \"product_id\" > 0\n"
        + "GROUP BY \"product_id\"";
    sql(query).ok(expected);
  }

  @Test void testAggregateFilterWhereToBigQuerySqlFromProductTable() {
    String query = "select\n"
        + "  sum(\"shelf_width\") filter (where \"net_weight\" > 0),\n"
        + "  sum(\"shelf_width\")\n"
        + "from \"foodmart\".\"product\"\n"
        + "where \"product_id\" > 0\n"
        + "group by \"product_id\"";
    final String expected = "SELECT SUM(CASE WHEN net_weight > 0 IS TRUE"
        + " THEN shelf_width ELSE NULL END), "
        + "SUM(shelf_width)\n"
        + "FROM foodmart.product\n"
        + "WHERE product_id > 0\n"
        + "GROUP BY product_id";
    sql(query).withBigQuery().ok(expected);
  }

  @Test void testPivotToSqlFromProductTable() {
    String query = "select * from (\n"
        + "  select \"shelf_width\", \"net_weight\", \"product_id\"\n"
        + "  from \"foodmart\".\"product\")\n"
        + "  pivot (sum(\"shelf_width\") as w, count(*) as c\n"
        + "    for (\"product_id\") in (10, 20))";
    final String expected = "SELECT \"net_weight\","
        + " SUM(\"shelf_width\") FILTER (WHERE \"product_id\" = 10) AS \"10_W\","
        + " COUNT(*) FILTER (WHERE \"product_id\" = 10) AS \"10_C\","
        + " SUM(\"shelf_width\") FILTER (WHERE \"product_id\" = 20) AS \"20_W\","
        + " COUNT(*) FILTER (WHERE \"product_id\" = 20) AS \"20_C\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"net_weight\"";
    // BigQuery does not support FILTER, so we generate CASE around the
    // arguments to the aggregate functions.
    final String expectedBigQuery = "SELECT net_weight,"
        + " SUM(CASE WHEN product_id = 10 "
        + "THEN shelf_width ELSE NULL END) AS `10_W`,"
        + " COUNT(CASE WHEN product_id = 10 THEN 1 ELSE NULL END) AS `10_C`,"
        + " SUM(CASE WHEN product_id = 20 "
        + "THEN shelf_width ELSE NULL END) AS `20_W`,"
        + " COUNT(CASE WHEN product_id = 20 THEN 1 ELSE NULL END) AS `20_C`\n"
        + "FROM foodmart.product\n"
        + "GROUP BY net_weight";
    sql(query).ok(expected)
        .withBigQuery().ok(expectedBigQuery);
  }

  @Test void testSimpleSelectQueryFromProductTable() {
    String query = "select \"product_id\", \"product_class_id\" from \"product\"";
    final String expected = "SELECT \"product_id\", \"product_class_id\"\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query).ok(expected);
  }

  @Test void testSelectQueryWithWhereClauseOfLessThan() {
    String query = "select \"product_id\", \"shelf_width\"\n"
        + "from \"product\" where \"product_id\" < 10";
    final String expected = "SELECT \"product_id\", \"shelf_width\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE \"product_id\" < 10";
    sql(query).ok(expected);
  }

  @Test void testSelectWhereNotEqualsOrNull() {
    String query = "select \"product_id\", \"shelf_width\"\n"
        + "from \"product\"\n"
        + "where \"net_weight\" <> 10 or \"net_weight\" is null";
    final String expected = "SELECT \"product_id\", \"shelf_width\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE \"net_weight\" <> 10 OR \"net_weight\" IS NULL";
    sql(query).ok(expected);
  }

  @Test void testSelectQueryWithWhereClauseOfBasicOperators() {
    String query = "select * from \"product\" "
        + "where (\"product_id\" = 10 OR \"product_id\" <= 5) "
        + "AND (80 >= \"shelf_width\" OR \"shelf_width\" > 30)";
    final String expected = "SELECT *\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE (\"product_id\" = 10 OR \"product_id\" <= 5) "
        + "AND (80 >= \"shelf_width\" OR \"shelf_width\" > 30)";
    sql(query).ok(expected);
  }


  @Test void testSelectQueryWithGroupBy() {
    String query = "select count(*) from \"product\" group by \"product_class_id\", \"product_id\"";
    final String expected = "SELECT COUNT(*)\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_class_id\", \"product_id\"";
    sql(query).ok(expected);
  }

  @Test void testSelectQueryWithHiveCube() {
    String query = "select \"product_class_id\", \"product_id\", count(*) "
        + "from \"product\" group by cube(\"product_class_id\", \"product_id\")";
    String expected = "SELECT product_class_id, product_id, COUNT(*)\n"
        + "FROM foodmart.product\n"
        + "GROUP BY product_class_id, product_id WITH CUBE";
    sql(query).withHive().ok(expected);
    SqlDialect sqlDialect = sql(query).withHive().dialect;
    assertTrue(sqlDialect.supportsGroupByWithCube());
  }

  @Test void testSelectQueryWithHiveRollup() {
    String query = "select \"product_class_id\", \"product_id\", count(*) "
        + "from \"product\" group by rollup(\"product_class_id\", \"product_id\")";
    String expected = "SELECT product_class_id, product_id, COUNT(*)\n"
        + "FROM foodmart.product\n"
        + "GROUP BY product_class_id, product_id WITH ROLLUP";
    sql(query).withHive().ok(expected);
    SqlDialect sqlDialect = sql(query).withHive().dialect;
    assertTrue(sqlDialect.supportsGroupByWithRollup());
  }

  @Test void testSelectQueryWithGroupByEmpty() {
    final String sql0 = "select count(*) from \"product\" group by ()";
    final String sql1 = "select count(*) from \"product\"";
    final String expected = "SELECT COUNT(*)\n"
        + "FROM \"foodmart\".\"product\"";
    final String expectedMySql = "SELECT COUNT(*)\n"
        + "FROM `foodmart`.`product`";
    final String expectedPresto = "SELECT COUNT(*)\n"
        + "FROM \"foodmart\".\"product\"";
    sql(sql0)
        .ok(expected)
        .withMysql()
        .ok(expectedMySql)
        .withPresto()
        .ok(expectedPresto);
    sql(sql1)
        .ok(expected)
        .withMysql()
        .ok(expectedMySql)
        .withPresto()
        .ok(expectedPresto);
  }

  @Test void testSelectQueryWithGroupByEmpty2() {
    final String query = "select 42 as c from \"product\" group by ()";
    final String expected = "SELECT 42 AS \"C\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY ()";
    final String expectedMySql = "SELECT 42 AS `C`\n"
        + "FROM `foodmart`.`product`\n"
        + "GROUP BY ()";
    final String expectedPresto = "SELECT 42 AS \"C\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY ()";
    sql(query)
        .ok(expected)
        .withMysql()
        .ok(expectedMySql)
        .withPresto()
        .ok(expectedPresto);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3097">[CALCITE-3097]
   * GROUPING SETS breaks on sets of size &gt; 1 due to precedence issues</a>,
   * in particular, that we maintain proper precedence around nested lists. */
  @Test void testGroupByGroupingSets() {
    final String query = "select \"product_class_id\", \"brand_name\"\n"
        + "from \"product\"\n"
        + "group by GROUPING SETS ((\"product_class_id\", \"brand_name\"),"
        + " (\"product_class_id\"))\n"
        + "order by 2, 1";
    final String expected = "SELECT \"product_class_id\", \"brand_name\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY GROUPING SETS((\"product_class_id\", \"brand_name\"),"
        + " \"product_class_id\")\n"
        + "ORDER BY \"brand_name\", \"product_class_id\"";
    sql(query)
        .withPostgresql()
        .ok(expected);
  }

  /** Tests GROUP BY ROLLUP of two columns. The SQL for MySQL has
   * "GROUP BY ... ROLLUP" but no "ORDER BY". */
  @Test void testSelectQueryWithGroupByRollup() {
    final String query = "select \"product_class_id\", \"brand_name\"\n"
        + "from \"product\"\n"
        + "group by rollup(\"product_class_id\", \"brand_name\")\n"
        + "order by 1, 2";
    final String expected = "SELECT \"product_class_id\", \"brand_name\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY ROLLUP(\"product_class_id\", \"brand_name\")\n"
        + "ORDER BY \"product_class_id\", \"brand_name\"";
    final String expectedMySql = "SELECT `product_class_id`, `brand_name`\n"
        + "FROM `foodmart`.`product`\n"
        + "GROUP BY `product_class_id`, `brand_name` WITH ROLLUP";
    final String expectedMySql8 = "SELECT `product_class_id`, `brand_name`\n"
        + "FROM `foodmart`.`product`\n"
        + "GROUP BY ROLLUP(`product_class_id`, `brand_name`)\n"
        + "ORDER BY `product_class_id` NULLS LAST, `brand_name` NULLS LAST";
    final String expectedHive = "SELECT product_class_id, brand_name\n"
        + "FROM foodmart.product\n"
        + "GROUP BY product_class_id, brand_name WITH ROLLUP";
    sql(query)
        .ok(expected)
        .withMysql()
        .ok(expectedMySql)
        .withMysql8()
        .ok(expectedMySql8)
        .withHive()
        .ok(expectedHive);
  }

  /** As {@link #testSelectQueryWithGroupByRollup()},
   * but ORDER BY columns reversed. */
  @Test void testSelectQueryWithGroupByRollup2() {
    final String query = "select \"product_class_id\", \"brand_name\"\n"
        + "from \"product\"\n"
        + "group by rollup(\"product_class_id\", \"brand_name\")\n"
        + "order by 2, 1";
    final String expected = "SELECT \"product_class_id\", \"brand_name\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY ROLLUP(\"product_class_id\", \"brand_name\")\n"
        + "ORDER BY \"brand_name\", \"product_class_id\"";
    final String expectedMySql = "SELECT `product_class_id`, `brand_name`\n"
        + "FROM `foodmart`.`product`\n"
        + "GROUP BY `brand_name`, `product_class_id` WITH ROLLUP";
    final String expectedHive = "SELECT product_class_id, brand_name\n"
        + "FROM foodmart.product\n"
        + "GROUP BY brand_name, product_class_id WITH ROLLUP";
    sql(query)
        .ok(expected)
        .withMysql()
        .ok(expectedMySql)
        .withHive()
        .ok(expectedHive);
  }

  @Test public void testSimpleSelectWithGroupByAlias() {
    final String query = "select 'literal' as \"a\", sku + 1 as b from"
        + " \"product\" group by 'literal', sku + 1";
    final String bigQueryExpected = "SELECT 'literal' AS a, SKU + 1 AS B\n"
        + "FROM foodmart.product\n"
        + "GROUP BY 1, B";
    sql(query)
        .withBigQuery()
        .ok(bigQueryExpected);
  }

  @Test public void testSimpleSelectWithGroupByAliasAndAggregate() {
    final String query = "select 'literal' as \"a\", sku + 1 as \"b\", sum(\"product_id\") from"
        + " \"product\" group by sku + 1, 'literal'";
    final String bigQueryExpected = "SELECT 'literal' AS a, SKU + 1 AS b, SUM(product_id)\n"
        + "FROM foodmart.product\n"
        + "GROUP BY b, 1";
    sql(query)
        .withBigQuery()
        .ok(bigQueryExpected);
  }


  @Test public void testDuplicateLiteralInSelectForGroupBy() {
    final String query = "select '1' as \"a\", sku + 1 as b, '1' as \"d\" from"
        + " \"product\" group by '1', sku + 1";
    final String expectedSql = "SELECT '1' a, SKU + 1 B, '1' d\n"
        + "FROM foodmart.product\n"
        + "GROUP BY '1', SKU + 1";
    final String bigQueryExpected = "SELECT '1' AS a, SKU + 1 AS B, '1' AS d\n"
        + "FROM foodmart.product\n"
        + "GROUP BY 1, B";
    sql(query)
        .withHive()
        .ok(expectedSql)
        .withSpark()
        .ok(expectedSql)
        .withBigQuery()
        .ok(bigQueryExpected);
  }

  /** Tests a query with GROUP BY and a sub-query which is also with GROUP BY.
   * If we flatten sub-queries, the number of rows going into AVG becomes
   * incorrect. */
  @Test void testSelectQueryWithGroupBySubQuery1() {
    final String query = "select \"product_class_id\", avg(\"product_id\")\n"
        + "from (select \"product_class_id\", \"product_id\", avg(\"product_class_id\")\n"
        + "from \"product\"\n"
        + "group by \"product_class_id\", \"product_id\") as t\n"
        + "group by \"product_class_id\"";
    final String expected = "SELECT \"product_class_id\", AVG(\"product_id\")\n"
        + "FROM (SELECT \"product_class_id\", \"product_id\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_class_id\", \"product_id\") AS \"t1\"\n"
        + "GROUP BY \"product_class_id\"";
    sql(query).ok(expected);
  }

  /** Tests query without GROUP BY but an aggregate function
   * and a sub-query which is with GROUP BY. */
  @Test void testSelectQueryWithGroupBySubQuery2() {
    final String query = "select sum(\"product_id\")\n"
        + "from (select \"product_class_id\", \"product_id\"\n"
        + "from \"product\"\n"
        + "group by \"product_class_id\", \"product_id\") as t";
    final String expected = "SELECT SUM(\"product_id\")\n"
        + "FROM (SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_class_id\", \"product_id\") AS \"t1\"";
    final String expectedMysql = "SELECT SUM(`product_id`)\n"
        + "FROM (SELECT `product_id`\n"
        + "FROM `foodmart`.`product`\n"
        + "GROUP BY `product_class_id`, `product_id`) AS `t1`";
    sql(query)
        .ok(expected)
        .withMysql()
        .ok(expectedMysql);

    // Equivalent sub-query that uses SELECT DISTINCT
    final String query2 = "select sum(\"product_id\")\n"
        + "from (select distinct \"product_class_id\", \"product_id\"\n"
        + "    from \"product\") as t";
    sql(query2)
        .ok(expected)
        .withMysql()
        .ok(expectedMysql);
  }

  /** CUBE of one column is equivalent to ROLLUP, and Calcite recognizes
   * this. */
  @Test void testSelectQueryWithSingletonCube() {
    final String query = "select \"product_class_id\", count(*) as c\n"
        + "from \"product\"\n"
        + "group by cube(\"product_class_id\")\n"
        + "order by 1, 2";
    final String expected = "SELECT \"product_class_id\", COUNT(*) AS \"C\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY ROLLUP(\"product_class_id\")\n"
        + "ORDER BY \"product_class_id\", \"C\"";
    final String expectedMySql = "SELECT `product_class_id`, COUNT(*) AS `C`\n"
        + "FROM `foodmart`.`product`\n"
        + "GROUP BY `product_class_id` WITH ROLLUP\n"
        + "ORDER BY `product_class_id` IS NULL, `product_class_id`,"
        + " `C` IS NULL, `C`";
    final String expectedHive = "SELECT product_class_id, COUNT(*) C\n"
        + "FROM foodmart.product\n"
        + "GROUP BY product_class_id WITH ROLLUP\n"
        + "ORDER BY product_class_id IS NULL, product_class_id,"
        + " C IS NULL, C";
    final String expectedPresto = "SELECT \"product_class_id\", COUNT(*) AS \"C\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY ROLLUP(\"product_class_id\")\n"
        + "ORDER BY \"product_class_id\" IS NULL, \"product_class_id\", "
        + "COUNT(*) IS NULL, COUNT(*)";
    sql(query)
        .ok(expected)
        .withMysql()
        .ok(expectedMySql)
        .withPresto()
        .ok(expectedPresto)
        .withHive()
        .ok(expectedHive);
  }

  /** As {@link #testSelectQueryWithSingletonCube()}, but no ORDER BY
   * clause. */
  @Test void testSelectQueryWithSingletonCubeNoOrderBy() {
    final String query = "select \"product_class_id\", count(*) as c\n"
        + "from \"product\"\n"
        + "group by cube(\"product_class_id\")";
    final String expected = "SELECT \"product_class_id\", COUNT(*) AS \"C\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY ROLLUP(\"product_class_id\")";
    final String expectedMySql = "SELECT `product_class_id`, COUNT(*) AS `C`\n"
        + "FROM `foodmart`.`product`\n"
        + "GROUP BY `product_class_id` WITH ROLLUP";
    final String expectedPresto = "SELECT \"product_class_id\", COUNT(*) AS \"C\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY ROLLUP(\"product_class_id\")";
    final String expectedHive = "SELECT product_class_id, COUNT(*) C\n"
        + "FROM foodmart.product\n"
        + "GROUP BY product_class_id WITH ROLLUP";
    sql(query)
        .ok(expected)
        .withMysql()
        .ok(expectedMySql)
        .withPresto()
        .ok(expectedPresto)
        .withHive()
        .ok(expectedHive);
  }

  /** Cannot rewrite if ORDER BY contains a column not in GROUP BY (in this
   * case COUNT(*)). */
  @Test void testSelectQueryWithRollupOrderByCount() {
    final String query = "select \"product_class_id\", \"brand_name\",\n"
        + " count(*) as c\n"
        + "from \"product\"\n"
        + "group by rollup(\"product_class_id\", \"brand_name\")\n"
        + "order by 1, 2, 3";
    final String expected = "SELECT \"product_class_id\", \"brand_name\","
        + " COUNT(*) AS \"C\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY ROLLUP(\"product_class_id\", \"brand_name\")\n"
        + "ORDER BY \"product_class_id\", \"brand_name\", \"C\"";
    final String expectedMySql = "SELECT `product_class_id`, `brand_name`,"
        + " COUNT(*) AS `C`\n"
        + "FROM `foodmart`.`product`\n"
        + "GROUP BY `product_class_id`, `brand_name` WITH ROLLUP\n"
        + "ORDER BY `product_class_id` IS NULL, `product_class_id`,"
        + " `brand_name` IS NULL, `brand_name`,"
        + " `C` IS NULL, `C`";
    final String expectedHive = "SELECT product_class_id, brand_name,"
        + " COUNT(*) C\n"
        + "FROM foodmart.product\n"
        + "GROUP BY product_class_id, brand_name WITH ROLLUP\n"
        + "ORDER BY product_class_id IS NULL, product_class_id,"
        + " brand_name IS NULL, brand_name,"
        + " C IS NULL, C";
    sql(query)
        .ok(expected)
        .withMysql()
        .ok(expectedMySql)
        .withHive()
        .ok(expectedHive);
  }

  /** As {@link #testSelectQueryWithSingletonCube()}, but with LIMIT. */
  @Test void testSelectQueryWithCubeLimit() {
    final String query = "select \"product_class_id\", count(*) as c\n"
        + "from \"product\"\n"
        + "group by cube(\"product_class_id\")\n"
        + "limit 5";
    final String expected = "SELECT \"product_class_id\", COUNT(*) AS \"C\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY ROLLUP(\"product_class_id\")\n"
        + "FETCH NEXT 5 ROWS ONLY";
    // If a MySQL 5 query has GROUP BY ... ROLLUP, you cannot add ORDER BY,
    // but you can add LIMIT.
    final String expectedMySql = "SELECT `product_class_id`, COUNT(*) AS `C`\n"
        + "FROM `foodmart`.`product`\n"
        + "GROUP BY `product_class_id` WITH ROLLUP\n"
        + "LIMIT 5";
    final String expectedPresto = "SELECT \"product_class_id\", COUNT(*) AS \"C\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY ROLLUP(\"product_class_id\")\n"
        + "LIMIT 5";
    final String expectedHive = "SELECT product_class_id, COUNT(*) C\n"
        + "FROM foodmart.product\n"
        + "GROUP BY product_class_id WITH ROLLUP\n"
        + "LIMIT 5";
    sql(query)
        .ok(expected)
        .withMysql()
        .ok(expectedMySql)
        .withPresto()
        .ok(expectedPresto)
        .withHive()
        .ok(expectedHive);
  }

  @Test void testSelectQueryWithMinAggregateFunction() {
    String query = "select min(\"net_weight\") from \"product\" group by \"product_class_id\" ";
    final String expected = "SELECT MIN(\"net_weight\")\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_class_id\"";
    sql(query).ok(expected);
  }

  @Test void testSelectQueryWithMinAggregateFunction1() {
    String query = "select \"product_class_id\", min(\"net_weight\") from"
        + " \"product\" group by \"product_class_id\"";
    final String expected = "SELECT \"product_class_id\", MIN(\"net_weight\")\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_class_id\"";
    sql(query).ok(expected);
  }

  @Test void testSelectQueryWithSumAggregateFunction() {
    String query =
        "select sum(\"net_weight\") from \"product\" group by \"product_class_id\" ";
    final String expected = "SELECT SUM(\"net_weight\")\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_class_id\"";
    sql(query).ok(expected);
  }

  @Test void testSelectQueryWithMultipleAggregateFunction() {
    String query = "select sum(\"net_weight\"), min(\"low_fat\"), count(*)"
        + " from \"product\" group by \"product_class_id\" ";
    final String expected = "SELECT SUM(\"net_weight\"), MIN(\"low_fat\"),"
        + " COUNT(*)\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_class_id\"";
    sql(query).ok(expected);
  }

  @Test void testSelectQueryWithMultipleAggregateFunction1() {
    String query = "select \"product_class_id\","
        + " sum(\"net_weight\"), min(\"low_fat\"), count(*)"
        + " from \"product\" group by \"product_class_id\" ";
    final String expected = "SELECT \"product_class_id\","
        + " SUM(\"net_weight\"), MIN(\"low_fat\"), COUNT(*)\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_class_id\"";
    sql(query).ok(expected);
  }

  @Test void testSelectQueryWithGroupByAndProjectList() {
    String query = "select \"product_class_id\", \"product_id\", count(*) "
        + "from \"product\" group by \"product_class_id\", \"product_id\"  ";
    final String expected = "SELECT \"product_class_id\", \"product_id\","
        + " COUNT(*)\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_class_id\", \"product_id\"";
    sql(query).ok(expected);
  }

  /*@Test public void testGroupByAliasReplacementWithGroupByExpression() {
    String query = "select \"product_class_id\" + \"product_id\" as product_id, "
        + "\"product_id\" + 2 as prod_id, count(1) as num_records"
        + " from \"product\""
        + " group by \"product_class_id\" + \"product_id\", \"product_id\" + 2";
    final String expected = "SELECT product_class_id + product_id AS PRODUCT_ID,"
        + " product_id + 2 AS PROD_ID,"
        + " COUNT(*) AS NUM_RECORDS\n"
        + "FROM foodmart.product\n"
        + "GROUP BY product_class_id + product_id, PROD_ID";
    sql(query).withBigQuery().ok(expected);
  }

  @Test public void testGroupByAliasReplacementWithGroupByExpression2() {
    String query = "select "
        + "(case when \"product_id\" = 1 then \"product_id\" else 1234 end)"
        + " as product_id, count(1) as num_records from \"product\""
        + " group by (case when \"product_id\" = 1 then \"product_id\" else 1234 end)";
    final String expected = "SELECT "
        + "CASE WHEN product_id = 1 THEN product_id ELSE 1234 END AS PRODUCT_ID,"
        + " COUNT(*) AS NUM_RECORDS\n"
        + "FROM foodmart.product\n"
        + "GROUP BY CASE WHEN product_id = 1 THEN product_id ELSE 1234 END";
    sql(query).withBigQuery().ok(expected);
  }*/

  @Test void testCastDecimal1() {
    final String query = "select -0.0000000123\n"
        + " from \"expense_fact\"";
    final String expected = "SELECT -1.23E-8\n"
        + "FROM \"foodmart\".\"expense_fact\"";
    sql(query).ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2713">[CALCITE-2713]
   * JDBC adapter may generate casts on PostgreSQL for VARCHAR type exceeding
   * max length</a>. */
  @Test void testCastLongVarchar1() {
    final String query = "select cast(\"store_id\" as VARCHAR(10485761))\n"
        + " from \"expense_fact\"";
    final String expectedPostgreSQL = "SELECT CAST(\"store_id\" AS VARCHAR(256))\n"
        + "FROM \"foodmart\".\"expense_fact\"";
    sql(query)
        .withPostgresqlModifiedTypeSystem()
        .ok(expectedPostgreSQL);

    final String expectedOracle = "SELECT CAST(\"store_id\" AS VARCHAR(512))\n"
        + "FROM \"foodmart\".\"expense_fact\"";
    sql(query)
        .withOracleModifiedTypeSystem()
        .ok(expectedOracle);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2713">[CALCITE-2713]
   * JDBC adapter may generate casts on PostgreSQL for VARCHAR type exceeding
   * max length</a>. */
  @Test void testCastLongVarchar2() {
    final String query = "select cast(\"store_id\" as VARCHAR(175))\n"
        + " from \"expense_fact\"";
    final String expectedPostgreSQL = "SELECT CAST(\"store_id\" AS VARCHAR(175))\n"
        + "FROM \"foodmart\".\"expense_fact\"";
    sql(query)
        .withPostgresqlModifiedTypeSystem()
        .ok(expectedPostgreSQL);

    final String expectedOracle = "SELECT CAST(\"store_id\" AS VARCHAR(175))\n"
        + "FROM \"foodmart\".\"expense_fact\"";
    sql(query)
        .withOracleModifiedTypeSystem()
        .ok(expectedOracle);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1174">[CALCITE-1174]
   * When generating SQL, translate SUM0(x) to COALESCE(SUM(x), 0)</a>. */
  @Test void testSum0BecomesCoalesce() {
    final Function<RelBuilder, RelNode> fn = b -> b.scan("EMP")
        .aggregate(b.groupKey(),
            b.aggregateCall(SqlStdOperatorTable.SUM0, b.field(3))
                .as("s"))
        .build();
    final String expectedMysql = "SELECT COALESCE(SUM(`MGR`), 0) AS `s`\n"
        + "FROM `scott`.`EMP`";
    final String expectedPostgresql = "SELECT COALESCE(SUM(\"MGR\"), 0) AS \"s\"\n"
        + "FROM \"scott\".\"EMP\"";
    relFn(fn)
        .withPostgresql()
        .ok(expectedPostgresql)
        .withMysql()
        .ok(expectedMysql);
  }

  /** As {@link #testSum0BecomesCoalesce()} but for windowed aggregates. */
  @Test void testWindowedSum0BecomesCoalesce() {
    final String query = "select\n"
        + "  AVG(\"net_weight\") OVER (order by \"product_id\" rows 3 preceding)\n"
        + "from \"foodmart\".\"product\"";
    final String expectedPostgresql = "SELECT CASE WHEN (COUNT(\"net_weight\")"
        + " OVER (ORDER BY \"product_id\" ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)) > 0 "
        + "THEN COALESCE(SUM(\"net_weight\")"
        + " OVER (ORDER BY \"product_id\" ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 0)"
        + " ELSE NULL END / (COUNT(\"net_weight\")"
        + " OVER (ORDER BY \"product_id\" ROWS BETWEEN 3 PRECEDING AND CURRENT ROW))\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query)
        .withPostgresql()
        .ok(expectedPostgresql);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2722">[CALCITE-2722]
   * SqlImplementor createLeftCall method throws StackOverflowError</a>. */
  @Test void testStack() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .filter(
            b.or(
                IntStream.range(1, 10000)
                    .mapToObj(i -> b.equals(b.field("EMPNO"), b.literal(i)))
                    .collect(Collectors.toList())))
        .build();
    final SqlDialect dialect = SqlDialect.DatabaseProduct.CALCITE.getDialect();
    final RelNode root = relFn.apply(relBuilder());
    final RelToSqlConverter converter = new RelToSqlConverter(dialect);
    final SqlNode sqlNode = converter.visitRoot(root).asStatement();
    final String sqlString = sqlNode.accept(new SqlShuttle())
        .toSqlString(dialect).getSql();
    assertThat(sqlString, notNullValue());
  }

  @Test void testAntiJoin() {
    final RelBuilder builder = relBuilder();
    final RelNode root = builder
        .scan("DEPT")
        .scan("EMP")
        .join(
            JoinRelType.ANTI, builder.equals(
              builder.field(2, 1, "DEPTNO"),
              builder.field(2, 0, "DEPTNO")))
        .project(builder.field("DEPTNO"))
        .build();
    final String expectedSql = "SELECT \"DEPTNO\"\n"
        + "FROM \"scott\".\"DEPT\"\n"
        + "WHERE NOT EXISTS (SELECT 1\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "WHERE \"DEPT\".\"DEPTNO\" = \"EMP\".\"DEPTNO\")";
    assertThat(toSql(root), isLinux(expectedSql));
  }

  @Test void testSemiJoin() {
    final RelBuilder builder = relBuilder();
    final RelNode root = builder
        .scan("DEPT")
        .scan("EMP")
        .join(
            JoinRelType.SEMI, builder.equals(
              builder.field(2, 1, "DEPTNO"),
              builder.field(2, 0, "DEPTNO")))
        .project(builder.field("DEPTNO"))
        .build();
    final String expectedSql = "SELECT \"DEPTNO\"\n"
        + "FROM \"scott\".\"DEPT\"\n"
        + "WHERE EXISTS (SELECT 1\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "WHERE \"DEPT\".\"DEPTNO\" = \"EMP\".\"DEPTNO\")";
    assertThat(toSql(root), isLinux(expectedSql));
  }

  @Test void testSemiJoinFilter() {
    final RelBuilder builder = relBuilder();
    final RelNode root = builder
        .scan("DEPT")
        .scan("EMP")
        .filter(
            builder.call(SqlStdOperatorTable.GREATER_THAN,
              builder.field(builder.peek().getRowType().getField("EMPNO", false, false).getIndex()),
              builder.literal((short) 10)))
        .join(
            JoinRelType.SEMI, builder.equals(
            builder.field(2, 1, "DEPTNO"),
            builder.field(2, 0, "DEPTNO")))
        .project(builder.field("DEPTNO"))
        .build();
    final String expectedSql = "SELECT \"DEPTNO\"\n"
        + "FROM \"scott\".\"DEPT\"\n"
        + "WHERE EXISTS (SELECT 1\n"
        + "FROM (SELECT *\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "WHERE \"EMPNO\" > 10) AS \"t\"\n"
        + "WHERE \"DEPT\".\"DEPTNO\" = \"t\".\"DEPTNO\")";
    assertThat(toSql(root), isLinux(expectedSql));
  }

  @Test void testSemiJoinProject() {
    final RelBuilder builder = relBuilder();
    final RelNode root = builder
        .scan("DEPT")
        .scan("EMP")
        .project(
            builder.field(builder.peek().getRowType().getField("EMPNO", false, false).getIndex()),
            builder.field(builder.peek().getRowType().getField("DEPTNO", false, false).getIndex()))
        .join(
            JoinRelType.SEMI, builder.equals(
              builder.field(2, 1, "DEPTNO"),
              builder.field(2, 0, "DEPTNO")))
        .project(builder.field("DEPTNO"))
        .build();
    final String expectedSql = "SELECT \"DEPTNO\"\n"
        + "FROM \"scott\".\"DEPT\"\n"
        + "WHERE EXISTS (SELECT 1\n"
        + "FROM (SELECT \"EMPNO\", \"DEPTNO\"\n"
        + "FROM \"scott\".\"EMP\") AS \"t\"\n"
        + "WHERE \"DEPT\".\"DEPTNO\" = \"t\".\"DEPTNO\")";
    assertThat(toSql(root), isLinux(expectedSql));
  }

  @Test void testSemiNestedJoin() {
    final RelBuilder builder = relBuilder();
    final RelNode base = builder
        .scan("EMP")
        .scan("EMP")
        .join(
            JoinRelType.INNER, builder.equals(
              builder.field(2, 0, "EMPNO"),
              builder.field(2, 1, "EMPNO")))
        .build();
    final RelNode root = builder
        .scan("DEPT")
        .push(base)
        .join(
            JoinRelType.SEMI, builder.equals(
              builder.field(2, 1, "DEPTNO"),
              builder.field(2, 0, "DEPTNO")))
        .project(builder.field("DEPTNO"))
        .build();
    final String expectedSql = "SELECT \"DEPTNO\"\n"
        + "FROM \"scott\".\"DEPT\"\n"
        + "WHERE EXISTS (SELECT 1\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "INNER JOIN \"scott\".\"EMP\" AS \"EMP0\" ON \"EMP\".\"EMPNO\" = \"EMP0\".\"EMPNO\"\n"
        + "WHERE \"DEPT\".\"DEPTNO\" = \"EMP\".\"DEPTNO\")";
    assertThat(toSql(root), isLinux(expectedSql));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2792">[CALCITE-2792]
   * Stackoverflow while evaluating filter with large number of OR conditions</a>. */
  @Disabled
  @Test void testBalancedBinaryCall() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .filter(
            b.and(
                b.or(IntStream.range(0, 4)
                    .mapToObj(i -> b.equals(b.field("EMPNO"), b.literal(i)))
                    .collect(Collectors.toList())),
                b.or(IntStream.range(5, 8)
                    .mapToObj(i -> b.equals(b.field("DEPTNO"), b.literal(i)))
                    .collect(Collectors.toList()))))
        .build();
    final String expected = "SELECT *\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "WHERE \"EMPNO\" IN (0, 1, 2, 3) AND \"DEPTNO\" IN (5, 6, 7)";
    relFn(relFn).ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1946">[CALCITE-1946]
   * JDBC adapter should generate sub-SELECT if dialect does not support nested
   * aggregate functions</a>. */
  @Test void testNestedAggregates() {
    // PostgreSQL, MySQL, Vertica do not support nested aggregate functions, so
    // for these, the JDBC adapter generates a SELECT in the FROM clause.
    // Oracle can do it in a single SELECT.
    final String query = "select\n"
        + "    SUM(\"net_weight1\") as \"net_weight_converted\"\n"
        + "  from ("
        + "    select\n"
        + "       SUM(\"net_weight\") as \"net_weight1\"\n"
        + "    from \"foodmart\".\"product\"\n"
        + "    group by \"product_id\")";
    final String expectedOracle = "SELECT SUM(SUM(\"net_weight\")) \"net_weight_converted\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_id\"";
    final String expectedMySQL = "SELECT SUM(`net_weight1`) AS `net_weight_converted`\n"
        + "FROM (SELECT SUM(`net_weight`) AS `net_weight1`\n"
        + "FROM `foodmart`.`product`\n"
        + "GROUP BY `product_id`) AS `t1`";
    final String expectedPostgresql = "SELECT SUM(\"net_weight1\") AS \"net_weight_converted\"\n"
        + "FROM (SELECT SUM(\"net_weight\") AS \"net_weight1\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_id\") AS \"t1\"";
    final String expectedVertica = expectedPostgresql;
    final String expectedBigQuery = "SELECT SUM(net_weight1) AS net_weight_converted\n"
        + "FROM (SELECT SUM(net_weight) AS net_weight1\n"
        + "FROM foodmart.product\n"
        + "GROUP BY product_id) AS t1";
    final String expectedHive = "SELECT SUM(net_weight1) net_weight_converted\n"
        + "FROM (SELECT SUM(net_weight) net_weight1\n"
        + "FROM foodmart.product\n"
        + "GROUP BY product_id) t1";
    final String expectedSpark = expectedHive;
    sql(query)
        .withOracle()
        .ok(expectedOracle)
        .withMysql()
        .ok(expectedMySQL)
        .withVertica()
        .ok(expectedVertica)
        .withPostgresql()
        .ok(expectedPostgresql)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withHive()
        .ok(expectedHive)
        .withSpark()
        .ok(expectedSpark);
  }

  @Test public void testAnalyticalFunctionInAggregate() {
    final String query = "select\n"
        + "MAX(\"rnk\") AS \"rnk1\""
        + "  from ("
        + "    select\n"
        + "    rank() over (order by \"hire_date\") AS \"rnk\""
        + "    from \"foodmart\".\"employee\"\n)";
    final String expectedSql = "SELECT MAX(RANK() OVER (ORDER BY \"hire_date\")) AS \"rnk1\"\n"
        + "FROM \"foodmart\".\"employee\"";
    final String expectedHive = "SELECT MAX(rnk) rnk1\n"
        + "FROM (SELECT RANK() OVER (ORDER BY hire_date NULLS LAST) rnk\n"
        + "FROM foodmart.employee) t";
    final String expectedSpark = "SELECT MAX(rnk) rnk1\n"
        + "FROM (SELECT RANK() OVER (ORDER BY hire_date IS NULL, hire_date) rnk\n"
        + "FROM foodmart.employee) t";
    final String expectedBigQuery = "SELECT MAX(rnk) AS rnk1\n"
        + "FROM (SELECT RANK() OVER (ORDER BY hire_date IS NULL, hire_date) AS rnk\n"
        + "FROM foodmart.employee) AS t";
    sql(query)
        .ok(expectedSql)
        .withHive2()
        .ok(expectedHive)
        .withSpark()
        .ok(expectedSpark)
        .withBigQuery()
        .ok(expectedBigQuery);
  }

  @Test public void testAnalyticalFunctionInAggregate1() {
    final String query = "select\n"
        + "MAX(\"rnk\") AS \"rnk1\""
        + "  from ("
        + "    select\n"
        + "    case when rank() over (order by \"hire_date\") = 1"
        + "    then 100"
        + "    else 200"
        + "    end as \"rnk\""
        + "    from \"foodmart\".\"employee\"\n)";
    final String expectedSql = "SELECT MAX(CASE WHEN (RANK() OVER (ORDER BY \"hire_date\")) = 1 "
        + "THEN 100 ELSE 200 END) AS \"rnk1\"\n"
        + "FROM \"foodmart\".\"employee\"";
    final String expectedHive = "SELECT MAX(rnk) rnk1\n"
        + "FROM (SELECT CASE WHEN (RANK() OVER (ORDER BY hire_date NULLS LAST)) = 1"
        + " THEN 100 ELSE 200 END rnk\n"
        + "FROM foodmart.employee) t";
    final String expectedSpark = "SELECT MAX(rnk) rnk1\n"
        + "FROM (SELECT CASE WHEN (RANK() OVER (ORDER BY hire_date IS NULL, hire_date)) = 1 "
        + "THEN 100 ELSE 200 END rnk\n"
        + "FROM foodmart.employee) t";
    final String expectedBigQuery = "SELECT MAX(rnk) AS rnk1\n"
        + "FROM (SELECT CASE WHEN (RANK() OVER (ORDER BY hire_date IS NULL, hire_date)) = 1 "
        + "THEN 100 ELSE 200 END AS rnk\n"
        + "FROM foodmart.employee) AS t";
    sql(query)
        .ok(expectedSql)
        .withHive2()
        .ok(expectedHive)
        .withSpark()
        .ok(expectedSpark)
        .withBigQuery()
        .ok(expectedBigQuery);
  }

  @Test public void testAnalyticalFunctionInGroupByWhereAnalyticalFunctionIsInputOfOtherFunction() {
    final String query = "select\n"
        + "\"rnk\""
        + "  from ("
        + "    select\n"
        + "    CASE WHEN \"salary\"=20 THEN MAX(\"salary\") OVER(PARTITION BY \"position_id\") END AS \"rnk\""
        + "    from \"foodmart\".\"employee\"\n) group by \"rnk\"";
    final String expectedSql = "SELECT CASE WHEN CAST(\"salary\" AS DECIMAL(14, 4)) = 20 THEN"
        + " MAX(\"salary\") OVER (PARTITION BY \"position_id\" RANGE BETWEEN UNBOUNDED "
        + "PRECEDING AND UNBOUNDED FOLLOWING) ELSE NULL END AS \"rnk\"\n"
        + "FROM \"foodmart\".\"employee\"\n"
        + "GROUP BY CASE WHEN CAST(\"salary\" AS DECIMAL(14, 4)) = 20 THEN MAX"
        + "(\"salary\") OVER (PARTITION BY \"position_id\" RANGE BETWEEN UNBOUNDED "
        + "PRECEDING AND UNBOUNDED FOLLOWING) ELSE NULL END";
    final String expectedHive = "SELECT CASE WHEN CAST(salary AS DECIMAL(14, 4)) = 20 THEN MAX"
        + "(salary) OVER (PARTITION BY position_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED "
        + "FOLLOWING) ELSE NULL END rnk\n"
        + "FROM foodmart.employee\n"
        + "GROUP BY CASE WHEN CAST(salary AS DECIMAL(14, 4)) = 20 THEN MAX(salary) OVER "
        + "(PARTITION BY position_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) "
        + "ELSE NULL END";
    final String expectedSpark = expectedHive;
    final String expectedBigQuery = "SELECT rnk\n"
        + "FROM (SELECT CASE WHEN CAST(salary AS NUMERIC) = 20 THEN MAX(salary) OVER "
        + "(PARTITION BY position_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) "
        + "ELSE NULL END AS rnk\n"
        + "FROM foodmart.employee) AS t\n"
        + "GROUP BY rnk";
    final  String mssql = "SELECT CASE WHEN CAST([salary] AS DECIMAL(14, 4)) = 20 THEN MAX("
            + "[salary]) OVER (PARTITION BY [position_id] ORDER BY [salary] ROWS BETWEEN UNBOUNDED "
            + "PRECEDING AND UNBOUNDED FOLLOWING) ELSE NULL END AS [rnk]\n"
            + "FROM [foodmart].[employee]\n"
            + "GROUP BY CASE WHEN CAST([salary] AS DECIMAL(14, 4)) = 20 THEN MAX([salary]) OVER "
            + "(PARTITION BY [position_id] ORDER BY [salary] ROWS BETWEEN UNBOUNDED PRECEDING AND "
            + "UNBOUNDED FOLLOWING) ELSE NULL END";
    sql(query)
        .ok(expectedSql)
        .withHive()
        .ok(expectedHive)
        .withSpark()
        .ok(expectedSpark)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withMssql()
        .ok(mssql);
  }

  @Test public void testAnalyticalFunctionInGroupByWhereAnalyticalFunctionIsInput() {
    final String query = "select\n"
        + "\"rnk\""
        + "  from ("
        + "    select\n"
        + "    case when row_number() over (PARTITION by \"hire_date\") = 1 THEN 100 else 200 END AS \"rnk\""
        + "    from \"foodmart\".\"employee\"\n) group by \"rnk\"";
    final String expectedSql = "SELECT CASE WHEN (ROW_NUMBER() OVER (PARTITION BY \"hire_date\"))"
        + " = 1 THEN 100 ELSE 200 END AS \"rnk\"\n"
        + "FROM \"foodmart\".\"employee\"\n"
        + "GROUP BY CASE WHEN"
        + " (ROW_NUMBER() OVER (PARTITION BY \"hire_date\")) = 1 THEN 100 ELSE 200 END";
    final String expectedHive = "SELECT CASE WHEN (ROW_NUMBER() OVER (PARTITION BY hire_date)) = "
        + "1 THEN 100 ELSE 200 END rnk\n"
        + "FROM foodmart.employee\n"
        + "GROUP BY CASE WHEN (ROW_NUMBER() "
        + "OVER (PARTITION BY hire_date)) = 1 THEN 100 ELSE 200 END";
    final String expectedSpark = expectedHive;
    final String expectedBigQuery = "SELECT rnk\n"
        + "FROM (SELECT CASE WHEN (ROW_NUMBER() OVER "
        + "(PARTITION BY hire_date)) = 1 THEN 100 ELSE 200 END AS rnk\n"
        + "FROM foodmart.employee) AS t\n"
        + "GROUP BY rnk";
    final  String mssql = "SELECT CASE WHEN (ROW_NUMBER() OVER (PARTITION BY [hire_date])) = 1 "
        + "THEN 100 ELSE 200 END AS [rnk]\n"
        + "FROM [foodmart].[employee]\nGROUP BY CASE WHEN "
        + "(ROW_NUMBER() OVER (PARTITION BY [hire_date])) = 1 THEN 100 ELSE 200 END";
    sql(query)
        .ok(expectedSql)
        .withHive()
        .ok(expectedHive)
        .withSpark()
        .ok(expectedSpark)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withMssql()
        .ok(mssql);
  }
  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2628">[CALCITE-2628]
   * JDBC adapter throws NullPointerException while generating GROUP BY query
   * for MySQL</a>.
   *
   * <p>MySQL does not support nested aggregates, so {@link RelToSqlConverter}
   * performs some extra checks, looking for aggregates in the input
   * sub-query, and these would fail with {@code NullPointerException}
   * and {@code ClassCastException} in some cases. */
  @Test void testNestedAggregatesMySqlTable() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .aggregate(b.groupKey(),
            b.count(false, "c", b.field(3)))
        .build();
    final String expectedSql = "SELECT COUNT(`MGR`) AS `c`\n"
        + "FROM `scott`.`EMP`";
    relFn(relFn).withMysql().ok(expectedSql);
  }

  /** As {@link #testNestedAggregatesMySqlTable()}, but input is a sub-query,
   * not a table. */
  @Test void testNestedAggregatesMySqlStar() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .filter(b.equals(b.field("DEPTNO"), b.literal(10)))
        .aggregate(b.groupKey(),
            b.count(false, "c", b.field(3)))
        .build();
    final String expectedSql = "SELECT COUNT(`MGR`) AS `c`\n"
        + "FROM `scott`.`EMP`\n"
        + "WHERE `DEPTNO` = 10";
    relFn(relFn).withMysql().ok(expectedSql);
  }

  @Test public void testTableFunctionScanWithUnnest() {
    final RelBuilder builder = relBuilder();
    String[] array = {"abc", "bcd", "fdc"};
    RelNode root = builder.functionScan(SqlStdOperatorTable.UNNEST, 0,
            builder.makeArrayLiteral(Arrays.asList(array))).project(builder.field(0)).build();
    final SqlDialect dialect = DatabaseProduct.BIG_QUERY.getDialect();
    final String expectedSql = "SELECT *\nFROM UNNEST(ARRAY['abc', 'bcd', 'fdc'])\nAS EXPR$0";
    assertThat(toSql(root, dialect), isLinux(expectedSql));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3207">[CALCITE-3207]
   * Fail to convert Join RelNode with like condition to sql statement </a>.
   */
  @Test void testJoinWithLikeConditionRel2Sql() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .scan("DEPT")
        .join(JoinRelType.LEFT,
            b.and(
                b.call(SqlStdOperatorTable.EQUALS,
                    b.field(2, 0, "DEPTNO"),
                    b.field(2, 1, "DEPTNO")),
                b.call(SqlStdOperatorTable.LIKE,
                    b.field(2, 1, "DNAME"),
                    b.literal("ACCOUNTING"))))
        .build();
    final String expectedSql = "SELECT *\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "LEFT JOIN \"scott\".\"DEPT\" "
        + "ON \"EMP\".\"DEPTNO\" = \"DEPT\".\"DEPTNO\" "
        + "AND \"DEPT\".\"DNAME\" LIKE 'ACCOUNTING'";
    relFn(relFn).ok(expectedSql);
  }

  @Test void testSelectQueryWithGroupByAndProjectList1() {
    String query = "select count(*) from \"product\"\n"
        + "group by \"product_class_id\", \"product_id\"";

    final String expected = "SELECT COUNT(*)\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_class_id\", \"product_id\"";
    sql(query).ok(expected);
  }

  @Test void testSelectQueryWithGroupByHaving() {
    String query = "select count(*) from \"product\" group by \"product_class_id\","
        + " \"product_id\"  having \"product_id\"  > 10";
    final String expected = "SELECT COUNT(*)\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_class_id\", \"product_id\"\n"
        + "HAVING \"product_id\" > 10";
    sql(query).ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1665">[CALCITE-1665]
   * Aggregates and having cannot be combined</a>. */
  @Test void testSelectQueryWithGroupByHaving2() {
    String query = " select \"product\".\"product_id\",\n"
        + "    min(\"sales_fact_1997\".\"store_id\")\n"
        + "    from \"product\"\n"
        + "    inner join \"sales_fact_1997\"\n"
        + "    on \"product\".\"product_id\" = \"sales_fact_1997\".\"product_id\"\n"
        + "    group by \"product\".\"product_id\"\n"
        + "    having count(*) > 1";

    String expected = "SELECT \"product\".\"product_id\", "
        + "MIN(\"sales_fact_1997\".\"store_id\")\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "INNER JOIN \"foodmart\".\"sales_fact_1997\" "
        + "ON \"product\".\"product_id\" = \"sales_fact_1997\".\"product_id\"\n"
        + "GROUP BY \"product\".\"product_id\"\n"
        + "HAVING COUNT(*) > 1";
    sql(query).ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1665">[CALCITE-1665]
   * Aggregates and having cannot be combined</a>. */
  @Test void testSelectQueryWithGroupByHaving3() {
    String query = " select * from (select \"product\".\"product_id\",\n"
        + "    min(\"sales_fact_1997\".\"store_id\")\n"
        + "    from \"product\"\n"
        + "    inner join \"sales_fact_1997\"\n"
        + "    on \"product\".\"product_id\" = \"sales_fact_1997\".\"product_id\"\n"
        + "    group by \"product\".\"product_id\"\n"
        + "    having count(*) > 1) where \"product_id\" > 100";

    String expected = "SELECT *\n"
        + "FROM (SELECT \"product\".\"product_id\","
        + " MIN(\"sales_fact_1997\".\"store_id\")\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "INNER JOIN \"foodmart\".\"sales_fact_1997\" ON \"product\".\"product_id\" = "
        + "\"sales_fact_1997\".\"product_id\"\n"
        + "GROUP BY \"product\".\"product_id\"\n"
        + "HAVING COUNT(*) > 1) AS \"t2\"\n"
        + "WHERE \"t2\".\"product_id\" > 100";
    sql(query).ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3811">[CALCITE-3811]
   * JDBC adapter generates SQL with invalid field names if Filter's row type
   * is different from its input</a>. */
  @Test void testHavingAlias() {
    final RelBuilder builder = relBuilder();
    builder.scan("EMP")
        .project(builder.alias(builder.field("DEPTNO"), "D"))
        .aggregate(builder.groupKey(builder.field("D")),
            builder.countStar("emps.count"))
        .filter(
            builder.call(SqlStdOperatorTable.LESS_THAN,
                builder.field("emps.count"), builder.literal(2)));

    final LogicalFilter filter = (LogicalFilter) builder.build();
    assertThat(filter.getRowType().getFieldNames().toString(),
        is("[D, emps.count]"));

    // Create a LogicalAggregate similar to the input of filter, but with different
    // field names.
    final LogicalAggregate newAggregate =
        (LogicalAggregate) builder.scan("EMP")
            .project(builder.alias(builder.field("DEPTNO"), "D2"))
            .aggregate(builder.groupKey(builder.field("D2")),
                builder.countStar("emps.count"))
            .build();
    assertThat(newAggregate.getRowType().getFieldNames().toString(),
        is("[D2, emps.count]"));

    // Change filter's input. Its row type does not change.
    filter.replaceInput(0, newAggregate);
    assertThat(filter.getRowType().getFieldNames().toString(),
        is("[D, emps.count]"));

    final RelNode root =
        builder.push(filter)
            .project(builder.alias(builder.field("D"), "emps.deptno"))
            .build();
    final String expectedMysql = "SELECT `D2` AS `emps.deptno`\n"
        + "FROM (SELECT `DEPTNO` AS `D2`, COUNT(*) AS `emps.count`\n"
        + "FROM `scott`.`EMP`\n"
        + "GROUP BY `D2`\n"
        + "HAVING `emps.count` < 2) AS `t1`";
    final String expectedPostgresql = "SELECT \"DEPTNO\" AS \"emps.deptno\"\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "GROUP BY \"DEPTNO\"\n"
        + "HAVING COUNT(*) < 2";
    final String expectedBigQuery = "SELECT D2 AS `emps.deptno`\n"
        + "FROM (SELECT DEPTNO AS D2, COUNT(*) AS `emps.count`\n"
        + "FROM scott.EMP\n"
        + "GROUP BY D2\n"
        + "HAVING `emps.count` < 2) AS t1";
    relFn(b -> root)
        .withMysql().ok(expectedMysql)
        .withPostgresql().ok(expectedPostgresql)
        .withBigQuery().ok(expectedBigQuery);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3896">[CALCITE-3896]
   * JDBC adapter, when generating SQL, changes target of ambiguous HAVING
   * clause with a Project on Filter on Aggregate</a>.
   *
   * <p>The alias is ambiguous in dialects such as MySQL and BigQuery that
   * have {@link SqlConformance#isHavingAlias()} = true. When the HAVING clause
   * tries to reference a column, it sees the alias instead. */
  @Test void testHavingAliasSameAsColumnIgnoringCase() {
    checkHavingAliasSameAsColumn(true);
  }

  @Test void testHavingAliasSameAsColumn() {
    checkHavingAliasSameAsColumn(false);
  }

  private void checkHavingAliasSameAsColumn(boolean upperAlias) {
    final String alias = upperAlias ? "GROSS_WEIGHT" : "gross_weight";
    final String query = "select \"product_id\" + 1,\n"
        + "  sum(\"gross_weight\") as \"" + alias + "\"\n"
        + "from \"product\"\n"
        + "group by \"product_id\"\n"
        + "having sum(\"product\".\"gross_weight\") < 200";
    // PostgreSQL has isHavingAlias=false, case-sensitive=true
    final String expectedPostgresql = "SELECT \"product_id\" + 1,"
        + " SUM(\"gross_weight\") AS \"" + alias + "\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_id\"\n"
        + "HAVING SUM(\"gross_weight\") < 200";
    // MySQL has isHavingAlias=true, case-sensitive=true
    final String expectedMysql = "SELECT `product_id` + 1, `" + alias + "`\n"
        + "FROM (SELECT `product_id`, SUM(`gross_weight`) AS `" + alias + "`\n"
        + "FROM `foodmart`.`product`\n"
        + "GROUP BY `product_id`\n"
        + "HAVING `" + alias + "` < 200) AS `t1`";
    // BigQuery has isHavingAlias=true, case-sensitive=false
    final String expectedBigQuery = upperAlias
        ? "SELECT product_id + 1, GROSS_WEIGHT\n"
            + "FROM (SELECT product_id, SUM(gross_weight) AS GROSS_WEIGHT\n"
            + "FROM foodmart.product\n"
            + "GROUP BY product_id\n"
            + "HAVING GROSS_WEIGHT < 200) AS t1"
        // Before [CALCITE-3896] was fixed, we got
        // "HAVING SUM(gross_weight) < 200) AS t1"
        // which on BigQuery gives you an error about aggregating aggregates
        : "SELECT product_id + 1, gross_weight\n"
            + "FROM (SELECT product_id, SUM(gross_weight) AS gross_weight\n"
            + "FROM foodmart.product\n"
            + "GROUP BY product_id\n"
            + "HAVING gross_weight < 200) AS t1";
    sql(query)
        .withPostgresql().ok(expectedPostgresql)
        .withMysql().ok(expectedMysql)
        .withBigQuery().ok(expectedBigQuery);
  }

  @Test void testHaving4() {
    final String query = "select \"product_id\"\n"
        + "from (\n"
        + "  select \"product_id\", avg(\"gross_weight\") as agw\n"
        + "  from \"product\"\n"
        + "  where \"net_weight\" < 100\n"
        + "  group by \"product_id\")\n"
        + "where agw > 50\n"
        + "group by \"product_id\"\n"
        + "having avg(agw) > 60\n";
    final String expected = "SELECT \"product_id\"\n"
        + "FROM (SELECT \"product_id\", AVG(\"gross_weight\") AS \"AGW\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE \"net_weight\" < 100\n"
        + "GROUP BY \"product_id\"\n"
        + "HAVING AVG(\"gross_weight\") > 50) AS \"t2\"\n"
        + "GROUP BY \"product_id\"\n"
        + "HAVING AVG(\"AGW\") > 60";
    sql(query).ok(expected);
  }

  @Test void testSelectQueryWithOrderByClause() {
    String query = "select \"product_id\" from \"product\"\n"
        + "order by \"net_weight\"";
    final String expected = "SELECT \"product_id\", \"net_weight\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "ORDER BY \"net_weight\"";
    sql(query).ok(expected);
  }

  @Test void testSelectQueryWithOrderByClause1() {
    String query =
        "select \"product_id\", \"net_weight\" from \"product\" order by \"net_weight\"";
    final String expected = "SELECT \"product_id\", \"net_weight\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "ORDER BY \"net_weight\"";
    sql(query).ok(expected);
  }

  @Test void testSelectQueryWithTwoOrderByClause() {
    String query = "select \"product_id\" from \"product\"\n"
        + "order by \"net_weight\", \"gross_weight\"";
    final String expected = "SELECT \"product_id\", \"net_weight\","
        + " \"gross_weight\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "ORDER BY \"net_weight\", \"gross_weight\"";
    sql(query).ok(expected);
  }

  @Test void testSelectQueryWithAscDescOrderByClause() {
    String query = "select \"product_id\" from \"product\" "
        + "order by \"net_weight\" asc, \"gross_weight\" desc, \"low_fat\"";
    final String expected = "SELECT"
        + " \"product_id\", \"net_weight\", \"gross_weight\", \"low_fat\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "ORDER BY \"net_weight\", \"gross_weight\" DESC, \"low_fat\"";
    sql(query).ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3440">[CALCITE-3440]
   * RelToSqlConverter does not properly alias ambiguous ORDER BY</a>. */
  @Test void testOrderByColumnWithSameNameAsAlias() {
    String query = "select \"product_id\" as \"p\",\n"
        + " \"net_weight\" as \"product_id\"\n"
        + "from \"product\"\n"
        + "order by 1";
    final String expected = "SELECT \"product_id\" AS \"p\","
        + " \"net_weight\" AS \"product_id\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "ORDER BY \"p\"";
    sql(query).ok(expected);
  }

  @Test void testOrderByColumnWithSameNameAsAlias2() {
    // We use ordinal "2" because the column name "product_id" is obscured
    // by alias "product_id".
    String query = "select \"net_weight\" as \"product_id\",\n"
        + "  \"product_id\" as \"product_id\"\n"
        + "from \"product\"\n"
        + "order by \"product\".\"product_id\"";
    final String expected = "SELECT \"net_weight\" AS \"product_id\","
        + " \"product_id\" AS \"product_id0\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "ORDER BY \"product_id0\"";
    final String expectedMysql = "SELECT `net_weight` AS `product_id`,"
        + " `product_id` AS `product_id0`\n"
        + "FROM `foodmart`.`product`\n"
        + "ORDER BY `product_id0` IS NULL, `product_id0`";
    sql(query).ok(expected)
        .withMysql().ok(expectedMysql);
  }

  @Test void testHiveSelectCharset() {
    String query = "select \"hire_date\", cast(\"hire_date\" as varchar(10)) "
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT hire_date, CAST(hire_date AS VARCHAR(10))\n"
        + "FROM foodmart.reserve_employee";
    sql(query).withHive().ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3282">[CALCITE-3282]
   * HiveSqlDialect unparse Interger type as Int in order
   * to be compatible with Hive1.x</a>. */
  @Test void testHiveCastAsInt() {
    String query = "select cast( cast(\"employee_id\" as varchar) as int) "
        + "from \"foodmart\".\"reserve_employee\" ";
    final String expected = "SELECT CAST(CAST(employee_id AS VARCHAR) AS INT)\n"
        + "FROM foodmart.reserve_employee";
    sql(query).withHive().ok(expected);
  }

  @Test void testBigQueryCast() {
    String query = "select cast(cast(\"employee_id\" as varchar) as bigint), "
        + "cast(cast(\"employee_id\" as varchar) as smallint), "
        + "cast(cast(\"employee_id\" as varchar) as tinyint), "
        + "cast(cast(\"employee_id\" as varchar) as integer), "
        + "cast(cast(\"employee_id\" as varchar) as float), "
        + "cast(cast(\"employee_id\" as varchar) as char), "
        + "cast(cast(\"employee_id\" as varchar) as binary), "
        + "cast(cast(\"employee_id\" as varchar) as varbinary), "
        + "cast(cast(\"employee_id\" as varchar) as timestamp), "
        + "cast(cast(\"employee_id\" as varchar) as double), "
        + "cast(cast(\"employee_id\" as varchar) as decimal), "
        + "cast(cast(\"employee_id\" as varchar) as date), "
        + "cast(cast(\"employee_id\" as varchar) as time), "
        + "cast(cast(\"employee_id\" as varchar) as boolean) "
        + "from \"foodmart\".\"reserve_employee\" ";
    final String expected = "SELECT CAST(CAST(employee_id AS STRING) AS INT64), "
        + "CAST(CAST(employee_id AS STRING) AS INT64), "
        + "CAST(CAST(employee_id AS STRING) AS INT64), "
        + "CAST(CAST(employee_id AS STRING) AS INT64), "
        + "CAST(CAST(employee_id AS STRING) AS FLOAT64), "
        + "CAST(CAST(employee_id AS STRING) AS STRING), "
        + "CAST(CAST(employee_id AS STRING) AS BYTES), "
        + "CAST(CAST(employee_id AS STRING) AS BYTES), "
        + "CAST(CAST(employee_id AS STRING) AS DATETIME), "
        + "CAST(CAST(employee_id AS STRING) AS FLOAT64), "
        + "CAST(CAST(employee_id AS STRING) AS NUMERIC), "
        + "CAST(CAST(employee_id AS STRING) AS DATE), "
        + "CAST(CAST(employee_id AS STRING) AS TIME), "
        + "CAST(CAST(employee_id AS STRING) AS BOOL)\n"
        + "FROM foodmart.reserve_employee";
    sql(query).withBigQuery().ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3220">[CALCITE-3220]
   * HiveSqlDialect should transform the SQL-standard TRIM function to TRIM,
   * LTRIM or RTRIM</a>,
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3663">[CALCITE-3663]
   * Support for TRIM function in BigQuery dialect</a>, and
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3771">[CALCITE-3771]
   * Support of TRIM function for SPARK dialect and improvement in HIVE
   * Dialect</a>. */
  @Test void testHiveSparkAndBqTrim() {
    final String query = "SELECT TRIM(' str ')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT TRIM(' str ')\n"
        + "FROM foodmart.reserve_employee";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
      .ok(expected)
      .withBigQuery()
        .ok(expected);
  }

  @Test void testHiveSparkAndBqTrimWithBoth() {
    final String query = "SELECT TRIM(both ' ' from ' str ')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT TRIM(' str ')\n"
        + "FROM foodmart.reserve_employee";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
      .ok(expected)
      .withBigQuery()
        .ok(expected);
  }

  @Test void testHiveSparkAndBqTrimWithLeading() {
    final String query = "SELECT TRIM(LEADING ' ' from ' str ')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT LTRIM(' str ')\n"
        + "FROM foodmart.reserve_employee";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
      .ok(expected)
      .withBigQuery()
        .ok(expected);
  }


  @Test void testHiveSparkAndBqTrimWithTailing() {
    final String query = "SELECT TRIM(TRAILING ' ' from ' str ')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT RTRIM(' str ')\n"
        + "FROM foodmart.reserve_employee";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
      .ok(expected)
      .withBigQuery()
        .ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3663">[CALCITE-3663]
   * Support for TRIM function in BigQuery dialect</a>. */
  @Test void testBqTrimWithLeadingChar() {
    final String query = "SELECT TRIM(LEADING 'a' from 'abcd')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT LTRIM('abcd', 'a')\n"
        + "FROM foodmart.reserve_employee";
    final String expectedHS = "SELECT REGEXP_REPLACE('abcd', '^(a)*', '')\n"
        + "FROM foodmart.reserve_employee";
    sql(query)
        .withBigQuery()
        .ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3771">[CALCITE-3771]
   * Support of TRIM function for SPARK dialect and improvement in HIVE Dialect</a>. */

  @Test void testHiveAndSparkTrimWithLeadingChar() {
    final String query = "SELECT TRIM(LEADING 'a' from 'abcd')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT REGEXP_REPLACE('abcd', '^(a)*', '')\n"
        + "FROM foodmart.reserve_employee";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected);
  }

  @Test void testBqTrimWithBothChar() {
    final String query = "SELECT TRIM(both 'a' from 'abcda')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT TRIM('abcda', 'a')\n"
        + "FROM foodmart.reserve_employee";
    sql(query)
        .withBigQuery()
        .ok(expected);
  }

  @Test void testHiveAndSparkTrimWithBothChar() {
    final String query = "SELECT TRIM(both 'a' from 'abcda')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT REGEXP_REPLACE('abcda', '^(a)*|(a)*$', '')\n"
        + "FROM foodmart.reserve_employee";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected);
  }

  @Test void testHiveBqTrimWithTailingChar() {
    final String query = "SELECT TRIM(TRAILING 'a' from 'abcd')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT RTRIM('abcd', 'a')\n"
        + "FROM foodmart.reserve_employee";
    sql(query)
        .withBigQuery()
        .ok(expected);
  }

  @Test public void testTrim() {
    final String query = "SELECT TRIM(\"full_name\")\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT TRIM(full_name)\n"
        + "FROM foodmart.reserve_employee";
    final String expectedSnowFlake = "SELECT TRIM(\"full_name\")\n"
        + "FROM \"foodmart\".\"reserve_employee\"";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expected)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testTrimWithBoth() {
    final String query = "SELECT TRIM(both ' ' from \"full_name\")\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT TRIM(full_name)\n"
        + "FROM foodmart.reserve_employee";
    final String expectedSnowFlake = "SELECT TRIM(\"full_name\")\n"
        + "FROM \"foodmart\".\"reserve_employee\"";
    final String expectedMsSql = "SELECT TRIM(' ' FROM [full_name])\n"
        + "FROM [foodmart].[reserve_employee]";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expected)
        .withSnowflake()
        .ok(expectedSnowFlake)
        .withMssql()
        .ok(expectedMsSql);
  }

  @Test public void testTrimWithLeadingSpace() {
    final String query = "SELECT TRIM(LEADING ' ' from ' str ')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT LTRIM(' str ')\n"
        + "FROM foodmart.reserve_employee";
    final String expectedSnowFlake = "SELECT LTRIM(' str ')\n"
              + "FROM \"foodmart\".\"reserve_employee\"";
    final String expectedMsSql = "SELECT LTRIM(' str ')\n"
        + "FROM [foodmart].[reserve_employee]";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expected)
        .withSnowflake()
        .ok(expectedSnowFlake)
        .withMssql()
        .ok(expectedMsSql);
  }

  @Test public void testTrimWithTailingSpace() {
    final String query = "SELECT TRIM(TRAILING ' ' from ' str ')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT RTRIM(' str ')\n"
        + "FROM foodmart.reserve_employee";
    final String expectedSnowFlake = "SELECT RTRIM(' str ')\n"
        + "FROM \"foodmart\".\"reserve_employee\"";
    final String expectedMsSql = "SELECT RTRIM(' str ')\n"
        + "FROM [foodmart].[reserve_employee]";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expected)
        .withSnowflake()
        .ok(expectedSnowFlake)
        .withMssql()
        .ok(expectedMsSql);
  }

  @Test public void testTrimWithLeadingCharacter() {
    final String query = "SELECT TRIM(LEADING 'A' from \"first_name\")\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT LTRIM(first_name, 'A')\n"
        + "FROM foodmart.reserve_employee";
    final String expectedHS = "SELECT REGEXP_REPLACE(first_name, '^(A)*', '')\n"
        + "FROM foodmart.reserve_employee";
    final String expectedSnowFlake = "SELECT LTRIM(\"first_name\", 'A')\n"
        + "FROM \"foodmart\".\"reserve_employee\"";
    sql(query)
        .withHive()
        .ok(expectedHS)
        .withSpark()
        .ok(expectedHS)
        .withBigQuery()
        .ok(expected)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testTrimWithTrailingCharacter() {
    final String query = "SELECT TRIM(TRAILING 'A' from 'AABCAADCAA')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT RTRIM('AABCAADCAA', 'A')\n"
        + "FROM foodmart.reserve_employee";
    final String expectedHS = "SELECT REGEXP_REPLACE('AABCAADCAA', '(A)*$', '')\n"
        + "FROM foodmart.reserve_employee";
    final String expectedSnowFlake = "SELECT RTRIM('AABCAADCAA', 'A')\n"
        + "FROM \"foodmart\".\"reserve_employee\"";
    sql(query)
        .withHive()
        .ok(expectedHS)
        .withSpark()
        .ok(expectedHS)
        .withBigQuery()
        .ok(expected)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testTrimWithBothCharacter() {
    final String query = "SELECT TRIM(BOTH 'A' from 'AABCAADCAA')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT TRIM('AABCAADCAA', 'A')\n"
        + "FROM foodmart.reserve_employee";
    final String expectedHS = "SELECT REGEXP_REPLACE('AABCAADCAA', '^(A)*|(A)*$', '')\n"
        + "FROM foodmart.reserve_employee";
    final String expectedSnowFlake = "SELECT TRIM('AABCAADCAA', 'A')\n"
              + "FROM \"foodmart\".\"reserve_employee\"";
    sql(query)
        .withHive()
        .ok(expectedHS)
        .withSpark()
        .ok(expectedHS)
        .withBigQuery()
        .ok(expected)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testTrimWithLeadingSpecialCharacter() {
    final String query = "SELECT TRIM(LEADING 'A$@*' from 'A$@*AABCA$@*AADCAA$@*')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT LTRIM('A$@*AABCA$@*AADCAA$@*', 'A$@*')\n"
        + "FROM foodmart.reserve_employee";
    final String expectedHS =
        "SELECT REGEXP_REPLACE('A$@*AABCA$@*AADCAA$@*', '^(A\\$\\@\\*)*', '')\n"
            + "FROM foodmart.reserve_employee";
    final String expectedSnowFlake = "SELECT LTRIM('A$@*AABCA$@*AADCAA$@*', 'A$@*')\n"
        + "FROM \"foodmart\".\"reserve_employee\"";
    sql(query)
        .withHive()
        .ok(expectedHS)
        .withSpark()
        .ok(expectedHS)
        .withBigQuery()
        .ok(expected)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testTrimWithTrailingSpecialCharacter() {
    final String query = "SELECT TRIM(TRAILING '$A@*' from '$A@*AABC$@*AADCAA$A@*')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT RTRIM('$A@*AABC$@*AADCAA$A@*', '$A@*')\n"
        + "FROM foodmart.reserve_employee";
    final String expectedHS =
        "SELECT REGEXP_REPLACE('$A@*AABC$@*AADCAA$A@*', '(\\$A\\@\\*)*$', '')\n"
            + "FROM foodmart.reserve_employee";
    final String expectedSnowFlake = "SELECT RTRIM('$A@*AABC$@*AADCAA$A@*', '$A@*')\n"
        + "FROM \"foodmart\".\"reserve_employee\"";
    sql(query)
        .withHive()
        .ok(expectedHS)
        .withSpark()
        .ok(expectedHS)
        .withBigQuery()
        .ok(expected)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }


  @Test public void testTrimWithBothSpecialCharacter() {
    final String query = "SELECT TRIM(BOTH '$@*A' from '$@*AABC$@*AADCAA$@*A')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT TRIM('$@*AABC$@*AADCAA$@*A', '$@*A')\n"
        + "FROM foodmart.reserve_employee";
    final String expectedHS =
        "SELECT REGEXP_REPLACE('$@*AABC$@*AADCAA$@*A',"
            + " '^(\\$\\@\\*A)*|(\\$\\@\\*A)*$', '')\n"
            + "FROM foodmart.reserve_employee";
    final String expectedSnowFlake = "SELECT TRIM('$@*AABC$@*AADCAA$@*A', '$@*A')\n"
              + "FROM \"foodmart\".\"reserve_employee\"";
    sql(query)
        .withHive()
        .ok(expectedHS)
        .withSpark()
        .ok(expectedHS)
        .withBigQuery()
        .ok(expected)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testTrimWithFunction() {
    final String query = "SELECT TRIM(substring(\"full_name\" from 2 for 3))\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT TRIM(SUBSTR(full_name, 2, 3))\n"
        + "FROM foodmart.reserve_employee";
    final String expectedHS =
        "SELECT TRIM(SUBSTRING(full_name, 2, 3))\n"
            + "FROM foodmart.reserve_employee";
    final String expectedSpark =
        "SELECT TRIM(SUBSTRING(full_name, 2, 3))\n"
            + "FROM foodmart.reserve_employee";
    final String expectedSnowFlake = "SELECT TRIM(SUBSTR(\"full_name\", 2, 3))\n"
        + "FROM \"foodmart\".\"reserve_employee\"";

    sql(query)
        .withHive()
        .ok(expectedHS)
        .withSpark()
        .ok(expectedSpark)
        .withBigQuery()
        .ok(expected)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test void testHiveAndSparkTrimWithTailingChar() {
    final String query = "SELECT TRIM(TRAILING 'a' from 'abcd')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT REGEXP_REPLACE('abcd', '(a)*$', '')\n"
        + "FROM foodmart.reserve_employee";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected);
  }

  @Test void testBqTrimWithBothSpecialCharacter() {
    final String query = "SELECT TRIM(BOTH '$@*A' from '$@*AABC$@*AADCAA$@*A')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT TRIM('$@*AABC$@*AADCAA$@*A', '$@*A')\n"
        + "FROM foodmart.reserve_employee";
    sql(query)
      .withBigQuery()
      .ok(expected);
  }

  @Test void testHiveAndSparkTrimWithBothSpecialCharacter() {
    final String query = "SELECT TRIM(BOTH '$@*A' from '$@*AABC$@*AADCAA$@*A')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT REGEXP_REPLACE('$@*AABC$@*AADCAA$@*A',"
        + " '^(\\$\\@\\*A)*|(\\$\\@\\*A)*$', '')\n"
        + "FROM foodmart.reserve_employee";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2715">[CALCITE-2715]
   * MS SQL Server does not support character set as part of data type</a>. */
  @Test void testMssqlCharacterSet() {
    String query = "select \"hire_date\", cast(\"hire_date\" as varchar(10))\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT [hire_date], CAST([hire_date] AS VARCHAR(10))\n"
        + "FROM [foodmart].[reserve_employee]";
    sql(query).withMssql().ok(expected);
  }

  /**
   * Tests that IN can be un-parsed.
   *
   * <p>This cannot be tested using "sql", because because Calcite's SQL parser
   * replaces INs with ORs or sub-queries.
   */
  @Test void testUnparseIn1() {
    final Function<RelBuilder, RelNode> relFn = b ->
        b.scan("EMP")
            .filter(b.in(b.field("DEPTNO"), b.literal(21)))
            .build();
    final String expectedSql = "SELECT *\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "WHERE \"DEPTNO\" = 21";
    relFn(relFn).ok(expectedSql);
  }

  @Test void testUnparseIn2() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .filter(b.in(b.field("DEPTNO"), b.literal(20), b.literal(21)))
        .build();
    final String expectedSql = "SELECT *\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "WHERE \"DEPTNO\" IN (20, 21)";
    relFn(relFn).ok(expectedSql);
  }

  @Test void testUnparseInStruct1() {
    final Function<RelBuilder, RelNode> relFn = b ->
        b.scan("EMP")
            .filter(
                b.in(
                    b.call(SqlStdOperatorTable.ROW,
                        b.field("DEPTNO"), b.field("JOB")),
                    b.call(SqlStdOperatorTable.ROW, b.literal(1),
                        b.literal("PRESIDENT"))))
            .build();
    final String expectedSql = "SELECT *\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "WHERE ROW(\"DEPTNO\", \"JOB\") = ROW(1, 'PRESIDENT')";
    relFn(relFn).ok(expectedSql);
  }

  @Test void testUnparseInStruct2() {
    final Function<RelBuilder, RelNode> relFn = b ->
        b.scan("EMP")
            .filter(
                b.in(
                    b.call(SqlStdOperatorTable.ROW,
                        b.field("DEPTNO"), b.field("JOB")),
                    b.call(SqlStdOperatorTable.ROW, b.literal(1),
                        b.literal("PRESIDENT")),
                    b.call(SqlStdOperatorTable.ROW, b.literal(2),
                        b.literal("PRESIDENT"))))
            .build();
    final String expectedSql = "SELECT *\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "WHERE ROW(\"DEPTNO\", \"JOB\") IN (ROW(1, 'PRESIDENT'), ROW(2, 'PRESIDENT'))";
    relFn(relFn).ok(expectedSql);
  }

  @Test public void testScalarQueryWithBigQuery() {
    final RelBuilder builder = relBuilder();
    final RelNode scalarQueryRel = builder.
        scan("DEPT")
        .filter(builder.equals(builder.field("DEPTNO"), builder.literal(40)))
        .project(builder.field(0))
        .build();
    final RelNode root = builder
        .scan("EMP")
        .aggregate(builder.groupKey("EMPNO"),
            builder.aggregateCall(SqlStdOperatorTable.SINGLE_VALUE,
                RexSubQuery.scalar(scalarQueryRel)).as("SC_DEPTNO"),
            builder.count(builder.literal(1)).as("pid"))
        .build();
    final String expectedBigQuery = "SELECT EMPNO, (((SELECT DEPTNO\n"
        + "FROM scott.DEPT\n"
        + "WHERE DEPTNO = 40))) AS SC_DEPTNO, COUNT(1) AS pid\n"
        + "FROM scott.EMP\n"
        + "GROUP BY EMPNO";
    final String expectedSnowflake = "SELECT \"EMPNO\", (((SELECT \"DEPTNO\"\n"
        + "FROM \"scott\".\"DEPT\"\n"
        + "WHERE \"DEPTNO\" = 40))) AS \"SC_DEPTNO\", COUNT(1) AS \"pid\"\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "GROUP BY \"EMPNO\"";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()),
        isLinux(expectedBigQuery));
    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()),
        isLinux(expectedSnowflake));
  }

  @Test void testSelectQueryWithLimitClause() {
    String query = "select \"product_id\" from \"product\" limit 100 offset 10";
    final String expected = "SELECT product_id\n"
        + "FROM foodmart.product\n"
        + "LIMIT 100\nOFFSET 10";
    sql(query).withHive().ok(expected);
  }

  @Test void testPositionFunctionForHive() {
    final String query = "select position('A' IN 'ABC') from \"product\"";
    final String expected = "SELECT INSTR('ABC', 'A')\n"
        + "FROM foodmart.product";
    sql(query).withHive().ok(expected);
  }

  @Test void testPositionFunctionForBigQuery() {
    final String query = "select position('A' IN 'ABC') from \"product\"";
    final String expected = "SELECT STRPOS('ABC', 'A')\n"
        + "FROM foodmart.product";
    sql(query).withBigQuery().ok(expected);
  }

  @Test void testPositionFunctionWithSlashForBigQuery() {
    final String query = "select position('\\,' IN 'ABC') from \"product\"";
    final String expected = "SELECT STRPOS('ABC', '\\\\,')\n"
        + "FROM foodmart.product";
    sql(query).withBigQuery().ok(expected);
  }

  /** Tests that we escape single-quotes in character literals using back-slash
   * in BigQuery. The norm is to escape single-quotes with single-quotes. */
  @Test void testCharLiteralForBigQuery() {
    final String query = "select 'that''s all folks!' from \"product\"";
    final String expectedPostgresql = "SELECT 'that''s all folks!'\n"
        + "FROM \"foodmart\".\"product\"";
    final String expectedBigQuery = "SELECT 'that\\'s all folks!'\n"
        + "FROM foodmart.product";
    sql(query)
        .withPostgresql().ok(expectedPostgresql)
        .withBigQuery().ok(expectedBigQuery);
  }

  @Test void testIdentifier() {
    // Note that IGNORE is reserved in BigQuery but not in standard SQL
    final String query = "select *\n"
        + "from (\n"
        + "  select 1 as \"one\", 2 as \"tWo\", 3 as \"THREE\",\n"
        + "    4 as \"fo$ur\", 5 as \"ignore\"\n"
        + "  from \"foodmart\".\"days\") as \"my$table\"\n"
        + "where \"one\" < \"tWo\" and \"THREE\" < \"fo$ur\"";
    final String expectedBigQuery = "SELECT *\n"
        + "FROM (SELECT 1 AS one, 2 AS tWo, 3 AS THREE,"
        + " 4 AS `fo$ur`, 5 AS `ignore`\n"
        + "FROM foodmart.days) AS t\n"
        + "WHERE one < tWo AND THREE < `fo$ur`";
    final String expectedMysql = "SELECT *\n"
        + "FROM (SELECT 1 AS `one`, 2 AS `tWo`, 3 AS `THREE`,"
        + " 4 AS `fo$ur`, 5 AS `ignore`\n"
        + "FROM `foodmart`.`days`) AS `t`\n"
        + "WHERE `one` < `tWo` AND `THREE` < `fo$ur`";
    final String expectedPostgresql = "SELECT *\n"
        + "FROM (SELECT 1 AS \"one\", 2 AS \"tWo\", 3 AS \"THREE\","
        + " 4 AS \"fo$ur\", 5 AS \"ignore\"\n"
        + "FROM \"foodmart\".\"days\") AS \"t\"\n"
        + "WHERE \"one\" < \"tWo\" AND \"THREE\" < \"fo$ur\"";
    final String expectedOracle = expectedPostgresql.replace(" AS ", " ");
    sql(query)
        .withBigQuery().ok(expectedBigQuery)
        .withMysql().ok(expectedMysql)
        .withOracle().ok(expectedOracle)
        .withPostgresql().ok(expectedPostgresql);
  }

  @Test void testModFunctionForHive() {
    final String query = "select mod(11,3) from \"product\"";
    final String expected = "SELECT 11 % 3\n"
        + "FROM foodmart.product";
    sql(query).withHive().ok(expected);
  }

  @Test void testUnionOperatorForBigQuery() {
    final String query = "select mod(11,3) from \"product\"\n"
        + "UNION select 1 from \"product\"";
    final String expected = "SELECT MOD(11, 3)\n"
        + "FROM foodmart.product\n"
        + "UNION DISTINCT\n"
        + "SELECT 1\n"
        + "FROM foodmart.product";
    sql(query).withBigQuery().ok(expected);
  }

  @Test void testUnionAllOperatorForBigQuery() {
    final String query = "select mod(11,3) from \"product\"\n"
        + "UNION ALL select 1 from \"product\"";
    final String expected = "SELECT MOD(11, 3)\n"
        + "FROM foodmart.product\n"
        + "UNION ALL\n"
        + "SELECT 1\n"
        + "FROM foodmart.product";
    sql(query).withBigQuery().ok(expected);
  }

  @Test void testIntersectOperatorForBigQuery() {
    final String query = "select mod(11,3) from \"product\"\n"
        + "INTERSECT select 1 from \"product\"";
    final String expected = "SELECT MOD(11, 3)\n"
        + "FROM foodmart.product\n"
        + "INTERSECT DISTINCT\n"
        + "SELECT 1\n"
        + "FROM foodmart.product";
    sql(query).withBigQuery().ok(expected);
  }

  @Test public void testIntersectOrderBy() {
    final String query = "select * from (select \"product_id\" from \"product\"\n"
            + "INTERSECT select \"product_id\" from \"product\") t order by t.\"product_id\"";
    final String expectedBigQuery = "SELECT *\n"
            + "FROM (SELECT product_id\n"
            + "FROM foodmart.product\n"
            + "INTERSECT DISTINCT\n"
            + "SELECT product_id\n"
            + "FROM foodmart.product) AS t1\n"
            + "ORDER BY product_id IS NULL, product_id";
    sql(query).withBigQuery().ok(expectedBigQuery);
  }

  @Test public void testIntersectWithWhere() {
    final String query = "select * from (select \"product_id\" from \"product\"\n"
            + "INTERSECT select \"product_id\" from \"product\") t where t.\"product_id\"<=14";
    final String expectedBigQuery = "SELECT *\n"
            + "FROM (SELECT product_id\n"
            + "FROM foodmart.product\n"
            + "INTERSECT DISTINCT\n"
            + "SELECT product_id\n"
            + "FROM foodmart.product) AS t1\n"
            + "WHERE product_id <= 14";
    sql(query).withBigQuery().ok(expectedBigQuery);
  }

  @Test public void testIntersectWithGroupBy() {
    final String query = "select * from (select \"product_id\" from \"product\"\n"
            + "INTERSECT select \"product_id\" from \"product\") t group by  \"product_id\"";
    final String expectedBigQuery = "SELECT product_id\n"
            + "FROM foodmart.product\n"
            + "INTERSECT DISTINCT\n"
            + "SELECT product_id\n"
            + "FROM foodmart.product";
    sql(query).withBigQuery().ok(expectedBigQuery);
  }

  @Test public void testExceptOperatorForBigQuery() {
    final String query = "select mod(11,3) from \"product\"\n"
        + "EXCEPT select 1 from \"product\"";
    final String expected = "SELECT MOD(11, 3)\n"
        + "FROM foodmart.product\n"
        + "EXCEPT DISTINCT\n"
        + "SELECT 1\n"
        + "FROM foodmart.product";
    sql(query).withBigQuery().ok(expected);
  }

  @Test public void testSelectQueryWithOrderByDescAndNullsFirstShouldBeEmulated() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" desc nulls first";
    // Hive and MSSQL do not support NULLS FIRST, so need to emulate
    final String expected = "SELECT product_id\n"
        + "FROM foodmart.product\n"
        + "ORDER BY product_id IS NULL DESC, product_id DESC";
    final String expectedMssql = "SELECT [product_id]\n"
        + "FROM [foodmart].[product]\n"
        + "ORDER BY CASE WHEN [product_id] IS NULL THEN 0 ELSE 1 END, [product_id] DESC";
    sql(query)
        .withSpark()
        .ok(expected)
        .withHive()
        .ok(expected)
        .withBigQuery()
        .ok(expected)
        .withMssql()
        .ok(expectedMssql);
  }

  @Test void testSelectOrderByDescNullsFirst() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" desc nulls first";
    // Hive and MSSQL do not support NULLS FIRST, so need to emulate
    final String expected = "SELECT product_id\n"
        + "FROM foodmart.product\n"
        + "ORDER BY product_id IS NULL DESC, product_id DESC";
    final String mssqlExpected = "SELECT [product_id]\n"
        + "FROM [foodmart].[product]\n"
        + "ORDER BY CASE WHEN [product_id] IS NULL THEN 0 ELSE 1 END, [product_id] DESC";
    sql(query)
        .dialect(HiveSqlDialect.DEFAULT).ok(expected)
        .dialect(MssqlSqlDialect.DEFAULT).ok(mssqlExpected);
  }

  @Test void testSelectOrderByAscNullsLast() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" nulls last";
    // Hive and MSSQL do not support NULLS LAST, so need to emulate
    final String expected = "SELECT product_id\n"
        + "FROM foodmart.product\n"
        + "ORDER BY product_id IS NULL, product_id";
    final String mssqlExpected = "SELECT [product_id]\n"
        + "FROM [foodmart].[product]\n"
        + "ORDER BY CASE WHEN [product_id] IS NULL THEN 1 ELSE 0 END, [product_id]";
    sql(query)
        .dialect(HiveSqlDialect.DEFAULT).ok(expected)
        .dialect(MssqlSqlDialect.DEFAULT).ok(mssqlExpected);
  }

  @Test public void testSelectQueryWithOrderByAscAndNullsLastShouldBeEmulated() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" nulls last";
    // Hive and MSSQL do not support NULLS LAST, so need to emulate
    final String expected = "SELECT product_id\n"
        + "FROM foodmart.product\n"
        + "ORDER BY product_id IS NULL, product_id";
    final String expectedMssql = "SELECT [product_id]\n"
        + "FROM [foodmart].[product]\n"
        + "ORDER BY CASE WHEN [product_id] IS NULL THEN 1 ELSE 0 END, [product_id]";
    sql(query)
        .withSpark()
        .ok(expected)
        .withHive()
        .ok(expected)
        .withBigQuery()
        .ok(expected)
        .withMssql()
        .ok(expectedMssql);
  }

  @Test public void testSelectQueryWithOrderByAscNullsFirstShouldNotAddNullEmulation() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" nulls first";
    // Hive and MSSQL do not support NULLS FIRST, but nulls sort low, so no
    // need to emulate
    final String expected = "SELECT product_id\n"
        + "FROM foodmart.product\n"
        + "ORDER BY product_id";
    final String expectedMssql = "SELECT [product_id]\n"
        + "FROM [foodmart].[product]\n"
        + "ORDER BY [product_id]";
    sql(query)
        .withSpark()
        .ok(expected)
        .withHive()
        .ok(expected)
        .withBigQuery()
        .ok(expected)
        .withMssql()
        .ok(expectedMssql);
  }

  @Test void testSelectOrderByAscNullsFirst() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" nulls first";
    // Hive and MSSQL do not support NULLS FIRST, but nulls sort low, so no
    // need to emulate
    final String expected = "SELECT product_id\n"
        + "FROM foodmart.product\n"
        + "ORDER BY product_id";
    final String mssqlExpected = "SELECT [product_id]\n"
        + "FROM [foodmart].[product]\n"
        + "ORDER BY [product_id]";
    sql(query)
        .dialect(HiveSqlDialect.DEFAULT).ok(expected)
        .dialect(MssqlSqlDialect.DEFAULT).ok(mssqlExpected);
  }

  @Test public void testSelectQueryWithOrderByDescNullsLastShouldNotAddNullEmulation() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" desc nulls last";
    // Hive and MSSQL do not support NULLS LAST, but nulls sort low, so no
    // need to emulate
    final String expected = "SELECT product_id\n"
        + "FROM foodmart.product\n"
        + "ORDER BY product_id DESC";
    final String expectedMssql = "SELECT [product_id]\n"
        + "FROM [foodmart].[product]\n"
        + "ORDER BY [product_id] DESC";
    sql(query)
        .withSpark()
        .ok(expected)
        .withHive()
        .ok(expected)
        .withBigQuery()
        .ok(expected)
        .withMssql()
        .ok(expectedMssql);
  }

  @Test void testSelectOrderByDescNullsLast() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" desc nulls last";
    // Hive and MSSQL do not support NULLS LAST, but nulls sort low, so no
    // need to emulate
    final String expected = "SELECT product_id\n"
        + "FROM foodmart.product\n"
        + "ORDER BY product_id DESC";
    final String mssqlExpected = "SELECT [product_id]\n"
        + "FROM [foodmart].[product]\n"
        + "ORDER BY [product_id] DESC";
    sql(query)
        .dialect(HiveSqlDialect.DEFAULT).ok(expected)
        .dialect(MssqlSqlDialect.DEFAULT).ok(mssqlExpected);
  }

  @Test void testHiveSelectQueryWithOverDescAndNullsFirstShouldBeEmulated() {
    final String query = "SELECT row_number() over "
        + "(order by \"hire_date\" desc nulls first) FROM \"employee\"";
    final String expected = "SELECT ROW_NUMBER() "
        + "OVER (ORDER BY hire_date IS NULL DESC, hire_date DESC)\n"
        + "FROM foodmart.employee";
    sql(query).dialect(HiveSqlDialect.DEFAULT).ok(expected);
  }

  @Test void testHiveSelectQueryWithOverAscAndNullsLastShouldBeEmulated() {
    final String query = "SELECT row_number() over "
        + "(order by \"hire_date\" nulls last) FROM \"employee\"";
    final String expected = "SELECT ROW_NUMBER() OVER (ORDER BY hire_date IS NULL, hire_date)\n"
        + "FROM foodmart.employee";
    sql(query).dialect(HiveSqlDialect.DEFAULT).ok(expected);
  }

  @Test void testHiveSelectQueryWithOverAscNullsFirstShouldNotAddNullEmulation() {
    final String query = "SELECT row_number() over "
        + "(order by \"hire_date\" nulls first) FROM \"employee\"";
    final String expected = "SELECT ROW_NUMBER() OVER (ORDER BY hire_date)\n"
        + "FROM foodmart.employee";
    sql(query).dialect(HiveSqlDialect.DEFAULT).ok(expected);
  }

  @Test void testCharLengthFunctionEmulationForHiveAndBigqueryAndSpark() {
    final String query = "select char_length('xyz') from \"product\"";
    final String expected = "SELECT LENGTH('xyz')\n"
        + "FROM foodmart.product";
    final String expectedSnowFlake = "SELECT LENGTH('xyz')\n"
            + "FROM \"foodmart\".\"product\"";
    sql(query)
        .withHive()
        .ok(expected)
        .withBigQuery()
        .ok(expected)
        .withSpark()
        .ok(expected)
      .withSnowflake()
      .ok(expectedSnowFlake);
  }

  @Test public void testCharacterLengthFunctionEmulationForHiveAndBigqueryAndSpark() {
    final String query = "select character_length('xyz') from \"product\"";
    final String expected = "SELECT LENGTH('xyz')\n"
        + "FROM foodmart.product";
    final String expectedSnowFlake = "SELECT LENGTH('xyz')\n"
            + "FROM \"foodmart\".\"product\"";
    sql(query)
      .withHive()
      .ok(expected)
      .withBigQuery()
      .ok(expected)
      .withSpark()
      .ok(expected)
      .withSnowflake()
      .ok(expectedSnowFlake);
  }

  @Test void testHiveSubstringWithLength() {
    String query = "SELECT SUBSTRING('ABC', 2, 3)"
            + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT SUBSTRING('ABC', 2, 3)\n"
            + "FROM foodmart.reserve_employee";
    sql(query).withHive().ok(expected);
  }

  @Test void testHiveSubstringWithANSI() {
    String query = "SELECT SUBSTRING('ABC' FROM 2)"
            + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT SUBSTRING('ABC', 2)\n"
            + "FROM foodmart.reserve_employee";
    sql(query).withHive().ok(expected);
  }

  @Test void testHiveSubstringWithANSIAndLength() {
    String query = "SELECT SUBSTRING('ABC' FROM 2 FOR 3)"
            + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT SUBSTRING('ABC', 2, 3)\n"
            + "FROM foodmart.reserve_employee";
    sql(query).withHive().ok(expected);
  }

  @Test void testHiveSelectQueryWithOverDescNullsLastShouldNotAddNullEmulation() {
    final String query = "SELECT row_number() over "
            + "(order by \"hire_date\" desc nulls last) FROM \"employee\"";
    final String expected = "SELECT ROW_NUMBER() OVER (ORDER BY hire_date DESC)\n"
            + "FROM foodmart.employee";
    sql(query).dialect(HiveSqlDialect.DEFAULT).ok(expected);
  }

  @Test void testMysqlCastToBigint() {
    // MySQL does not allow cast to BIGINT; instead cast to SIGNED.
    final String query = "select cast(\"product_id\" as bigint) from \"product\"";
    final String expected = "SELECT CAST(`product_id` AS SIGNED)\n"
        + "FROM `foodmart`.`product`";
    sql(query).withMysql().ok(expected);
  }


  @Test void testMysqlCastToInteger() {
    // MySQL does not allow cast to INTEGER; instead cast to SIGNED.
    final String query = "select \"employee_id\",\n"
        + "  cast(\"salary_paid\" * 10000 as integer)\n"
        + "from \"salary\"";
    final String expected = "SELECT `employee_id`,"
        + " CAST(`salary_paid` * 10000 AS SIGNED)\n"
        + "FROM `foodmart`.`salary`";
    sql(query).withMysql().ok(expected);
  }

  @Test void testHiveSelectQueryWithOrderByDescAndHighNullsWithVersionGreaterThanOrEq21() {
    final HiveSqlDialect hive2_1Dialect =
        new HiveSqlDialect(HiveSqlDialect.DEFAULT_CONTEXT
            .withDatabaseMajorVersion(2)
            .withDatabaseMinorVersion(1)
            .withNullCollation(NullCollation.LOW));

    final HiveSqlDialect hive2_2_Dialect =
        new HiveSqlDialect(HiveSqlDialect.DEFAULT_CONTEXT
            .withDatabaseMajorVersion(2)
            .withDatabaseMinorVersion(2)
            .withNullCollation(NullCollation.LOW));

    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" desc nulls first";
    final String expected = "SELECT product_id\n"
        + "FROM foodmart.product\n"
        + "ORDER BY product_id DESC NULLS FIRST";
    sql(query).dialect(hive2_1Dialect).ok(expected);
    sql(query).dialect(hive2_2_Dialect).ok(expected);
  }

  @Test void testHiveSelectQueryWithOverDescAndHighNullsWithVersionGreaterThanOrEq21() {
    final HiveSqlDialect hive2_1Dialect =
        new HiveSqlDialect(SqlDialect.EMPTY_CONTEXT
            .withDatabaseMajorVersion(2)
            .withDatabaseMinorVersion(1)
            .withNullCollation(NullCollation.LOW));

    final HiveSqlDialect hive2_2_Dialect =
        new HiveSqlDialect(SqlDialect.EMPTY_CONTEXT
            .withDatabaseMajorVersion(2)
            .withDatabaseMinorVersion(2)
            .withNullCollation(NullCollation.LOW));

    final String query = "SELECT row_number() over "
        + "(order by \"hire_date\" desc nulls first) FROM \"employee\"";
    final String expected = "SELECT ROW_NUMBER() OVER (ORDER BY hire_date DESC NULLS FIRST)\n"
        + "FROM foodmart.employee";
    sql(query).dialect(hive2_1Dialect).ok(expected);
    sql(query).dialect(hive2_2_Dialect).ok(expected);
  }

  @Test void testHiveSelectQueryWithOrderByDescAndHighNullsWithVersion20() {
    final HiveSqlDialect hive2_1_0_Dialect =
        new HiveSqlDialect(HiveSqlDialect.DEFAULT_CONTEXT
            .withDatabaseMajorVersion(2)
            .withDatabaseMinorVersion(0)
            .withNullCollation(NullCollation.LOW));
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" desc nulls first";
    final String expected = "SELECT product_id\n"
        + "FROM foodmart.product\n"
        + "ORDER BY product_id IS NULL DESC, product_id DESC";
    sql(query).dialect(hive2_1_0_Dialect).ok(expected);
  }

  @Test void testHiveSelectQueryWithOverDescAndHighNullsWithVersion20() {
    final HiveSqlDialect hive2_1_0_Dialect =
        new HiveSqlDialect(SqlDialect.EMPTY_CONTEXT
            .withDatabaseMajorVersion(2)
            .withDatabaseMinorVersion(0)
            .withNullCollation(NullCollation.LOW));
    final String query = "SELECT row_number() over "
        + "(order by \"hire_date\" desc nulls first) FROM \"employee\"";
    final String expected = "SELECT ROW_NUMBER() OVER "
        + "(ORDER BY hire_date IS NULL DESC, hire_date DESC)\n"
        + "FROM foodmart.employee";
    sql(query).dialect(hive2_1_0_Dialect).ok(expected);
  }

  @Test void testJethroDataSelectQueryWithOrderByDescAndNullsFirstShouldBeEmulated() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" desc nulls first";

    final String expected = "SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "ORDER BY \"product_id\", \"product_id\" DESC";
    sql(query).dialect(jethroDataSqlDialect()).ok(expected);
  }

  @Test void testJethroDataSelectQueryWithOverDescAndNullsFirstShouldBeEmulated() {
    final String query = "SELECT row_number() over "
        + "(order by \"hire_date\" desc nulls first) FROM \"employee\"";

    final String expected = "SELECT ROW_NUMBER() OVER "
        + "(ORDER BY \"hire_date\", \"hire_date\" DESC)\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query).dialect(jethroDataSqlDialect()).ok(expected);
  }

  @Test void testMySqlSelectQueryWithOrderByDescAndNullsFirstShouldBeEmulated() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" desc nulls first";
    final String expected = "SELECT `product_id`\n"
        + "FROM `foodmart`.`product`\n"
        + "ORDER BY `product_id` IS NULL DESC, `product_id` DESC";
    sql(query).dialect(MysqlSqlDialect.DEFAULT).ok(expected);
  }

  @Test void testMySqlSelectQueryWithOverDescAndNullsFirstShouldBeEmulated() {
    final String query = "SELECT row_number() over "
        + "(order by \"hire_date\" desc nulls first) FROM \"employee\"";
    final String expected = "SELECT ROW_NUMBER() OVER "
        + "(ORDER BY `hire_date` IS NULL DESC, `hire_date` DESC)\n"
        + "FROM `foodmart`.`employee`";
    sql(query).dialect(MysqlSqlDialect.DEFAULT).ok(expected);
  }

  @Test void testMySqlSelectQueryWithOrderByAscAndNullsLastShouldBeEmulated() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" nulls last";
    final String expected = "SELECT `product_id`\n"
        + "FROM `foodmart`.`product`\n"
        + "ORDER BY `product_id` IS NULL, `product_id`";
    sql(query).dialect(MysqlSqlDialect.DEFAULT).ok(expected);
  }

  @Test void testMySqlSelectQueryWithOverAscAndNullsLastShouldBeEmulated() {
    final String query = "SELECT row_number() over "
        + "(order by \"hire_date\" nulls last) FROM \"employee\"";
    final String expected = "SELECT ROW_NUMBER() OVER "
        + "(ORDER BY `hire_date` IS NULL, `hire_date`)\n"
        + "FROM `foodmart`.`employee`";
    sql(query).dialect(MysqlSqlDialect.DEFAULT).ok(expected);
  }

  @Test void testMySqlSelectQueryWithOrderByAscNullsFirstShouldNotAddNullEmulation() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" nulls first";
    final String expected = "SELECT `product_id`\n"
        + "FROM `foodmart`.`product`\n"
        + "ORDER BY `product_id`";
    sql(query).dialect(MysqlSqlDialect.DEFAULT).ok(expected);
  }

  @Test void testMySqlSelectQueryWithOverAscNullsFirstShouldNotAddNullEmulation() {
    final String query = "SELECT row_number() "
        + "over (order by \"hire_date\" nulls first) FROM \"employee\"";
    final String expected = "SELECT ROW_NUMBER() OVER (ORDER BY `hire_date`)\n"
        + "FROM `foodmart`.`employee`";
    sql(query).dialect(MysqlSqlDialect.DEFAULT).ok(expected);
  }

  @Test void testMySqlSelectQueryWithOrderByDescNullsLastShouldNotAddNullEmulation() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" desc nulls last";
    final String expected = "SELECT `product_id`\n"
        + "FROM `foodmart`.`product`\n"
        + "ORDER BY `product_id` DESC";
    sql(query).dialect(MysqlSqlDialect.DEFAULT).ok(expected);
  }

  @Test void testMySqlSelectQueryWithOverDescNullsLastShouldNotAddNullEmulation() {
    final String query = "SELECT row_number() "
        + "over (order by \"hire_date\" desc nulls last) FROM \"employee\"";
    final String expected = "SELECT ROW_NUMBER() OVER (ORDER BY `hire_date` DESC)\n"
        + "FROM `foodmart`.`employee`";
    sql(query).dialect(MysqlSqlDialect.DEFAULT).ok(expected);
  }

  @Test void testMySqlCastToVarcharWithLessThanMaxPrecision() {
    final String query = "select cast(\"product_id\" as varchar(50)), \"product_id\" "
        + "from \"product\" ";
    final String expected = "SELECT CAST(`product_id` AS CHAR(50)), `product_id`\n"
        + "FROM `foodmart`.`product`";
    sql(query).withMysql().ok(expected);
  }

  @Test void testMySqlCastToTimestamp() {
    final String query = "select  * from \"employee\" where  \"hire_date\" - "
        + "INTERVAL '19800' SECOND(5) > cast(\"hire_date\" as TIMESTAMP) ";
    final String expected = "SELECT *\nFROM `foodmart`.`employee`"
        + "\nWHERE (`hire_date` - INTERVAL '19800' SECOND) > CAST(`hire_date` AS DATETIME)";
    sql(query).withMysql().ok(expected);
  }

  @Test void testMySqlCastToVarcharWithGreaterThanMaxPrecision() {
    final String query = "select cast(\"product_id\" as varchar(500)), \"product_id\" "
        + "from \"product\" ";
    final String expected = "SELECT CAST(`product_id` AS CHAR(255)), `product_id`\n"
        + "FROM `foodmart`.`product`";
    sql(query).withMysql().ok(expected);
  }

  @Test void testMySqlWithHighNullsSelectWithOrderByAscNullsLastAndNoEmulation() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" nulls last";
    final String expected = "SELECT `product_id`\n"
        + "FROM `foodmart`.`product`\n"
        + "ORDER BY `product_id`";
    sql(query).dialect(mySqlDialect(NullCollation.HIGH)).ok(expected);
  }

  @Test void testMySqlWithHighNullsSelectWithOverAscNullsLastAndNoEmulation() {
    final String query = "SELECT row_number() "
        + "over (order by \"hire_date\" nulls last) FROM \"employee\"";
    final String expected = "SELECT ROW_NUMBER() OVER (ORDER BY `hire_date`)\n"
        + "FROM `foodmart`.`employee`";
    sql(query).dialect(mySqlDialect(NullCollation.HIGH)).ok(expected);
  }

  @Test void testMySqlWithHighNullsSelectWithOrderByAscNullsFirstAndNullEmulation() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" nulls first";
    final String expected = "SELECT `product_id`\n"
        + "FROM `foodmart`.`product`\n"
        + "ORDER BY `product_id` IS NULL DESC, `product_id`";
    sql(query).dialect(mySqlDialect(NullCollation.HIGH)).ok(expected);
  }

  @Test void testMySqlWithHighNullsSelectWithOverAscNullsFirstAndNullEmulation() {
    final String query = "SELECT row_number() "
        + "over (order by \"hire_date\" nulls first) FROM \"employee\"";
    final String expected = "SELECT ROW_NUMBER() "
        + "OVER (ORDER BY `hire_date` IS NULL DESC, `hire_date`)\n"
        + "FROM `foodmart`.`employee`";
    sql(query).dialect(mySqlDialect(NullCollation.HIGH)).ok(expected);
  }

  @Test void testMySqlWithHighNullsSelectWithOrderByDescNullsFirstAndNoEmulation() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" desc nulls first";
    final String expected = "SELECT `product_id`\n"
        + "FROM `foodmart`.`product`\n"
        + "ORDER BY `product_id` DESC";
    sql(query).dialect(mySqlDialect(NullCollation.HIGH)).ok(expected);
  }

  @Test void testMySqlWithHighNullsSelectWithOverDescNullsFirstAndNoEmulation() {
    final String query = "SELECT row_number() "
        + "over (order by \"hire_date\" desc nulls first) FROM \"employee\"";
    final String expected = "SELECT ROW_NUMBER() OVER (ORDER BY `hire_date` DESC)\n"
        + "FROM `foodmart`.`employee`";
    sql(query).dialect(mySqlDialect(NullCollation.HIGH)).ok(expected);
  }

  @Test void testMySqlWithHighNullsSelectWithOrderByDescNullsLastAndNullEmulation() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" desc nulls last";
    final String expected = "SELECT `product_id`\n"
        + "FROM `foodmart`.`product`\n"
        + "ORDER BY `product_id` IS NULL, `product_id` DESC";
    sql(query).dialect(mySqlDialect(NullCollation.HIGH)).ok(expected);
  }

  @Test void testMySqlWithHighNullsSelectWithOverDescNullsLastAndNullEmulation() {
    final String query = "SELECT row_number() "
        + "over (order by \"hire_date\" desc nulls last) FROM \"employee\"";
    final String expected = "SELECT ROW_NUMBER() "
        + "OVER (ORDER BY `hire_date` IS NULL, `hire_date` DESC)\n"
        + "FROM `foodmart`.`employee`";
    sql(query).dialect(mySqlDialect(NullCollation.HIGH)).ok(expected);
  }

  @Test void testMySqlWithFirstNullsSelectWithOrderByDescAndNullsFirstShouldNotBeEmulated() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" desc nulls first";
    final String expected = "SELECT `product_id`\n"
        + "FROM `foodmart`.`product`\n"
        + "ORDER BY `product_id` DESC";
    sql(query).dialect(mySqlDialect(NullCollation.FIRST)).ok(expected);
  }

  @Test void testMySqlWithFirstNullsSelectWithOverDescAndNullsFirstShouldNotBeEmulated() {
    final String query = "SELECT row_number() "
        + "over (order by \"hire_date\" desc nulls first) FROM \"employee\"";
    final String expected = "SELECT ROW_NUMBER() OVER (ORDER BY `hire_date` DESC)\n"
        + "FROM `foodmart`.`employee`";
    sql(query).dialect(mySqlDialect(NullCollation.FIRST)).ok(expected);
  }

  @Test void testMySqlWithFirstNullsSelectWithOrderByAscAndNullsFirstShouldNotBeEmulated() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" nulls first";
    final String expected = "SELECT `product_id`\n"
        + "FROM `foodmart`.`product`\n"
        + "ORDER BY `product_id`";
    sql(query).dialect(mySqlDialect(NullCollation.FIRST)).ok(expected);
  }

  @Test void testMySqlWithFirstNullsSelectWithOverAscAndNullsFirstShouldNotBeEmulated() {
    final String query = "SELECT row_number() "
        + "over (order by \"hire_date\" nulls first) FROM \"employee\"";
    final String expected = "SELECT ROW_NUMBER() OVER (ORDER BY `hire_date`)\n"
        + "FROM `foodmart`.`employee`";
    sql(query).dialect(mySqlDialect(NullCollation.FIRST)).ok(expected);
  }

  @Test void testMySqlWithFirstNullsSelectWithOrderByDescAndNullsLastShouldBeEmulated() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" desc nulls last";
    final String expected = "SELECT `product_id`\n"
        + "FROM `foodmart`.`product`\n"
        + "ORDER BY `product_id` IS NULL, `product_id` DESC";
    sql(query).dialect(mySqlDialect(NullCollation.FIRST)).ok(expected);
  }

  @Test void testMySqlWithFirstNullsSelectWithOverDescAndNullsLastShouldBeEmulated() {
    final String query = "SELECT row_number() "
        + "over (order by \"hire_date\" desc nulls last) FROM \"employee\"";
    final String expected = "SELECT ROW_NUMBER() "
        + "OVER (ORDER BY `hire_date` IS NULL, `hire_date` DESC)\n"
        + "FROM `foodmart`.`employee`";
    sql(query).dialect(mySqlDialect(NullCollation.FIRST)).ok(expected);
  }

  @Test void testMySqlWithFirstNullsSelectWithOrderByAscAndNullsLastShouldBeEmulated() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" nulls last";
    final String expected = "SELECT `product_id`\n"
        + "FROM `foodmart`.`product`\n"
        + "ORDER BY `product_id` IS NULL, `product_id`";
    sql(query).dialect(mySqlDialect(NullCollation.FIRST)).ok(expected);
  }

  @Test void testMySqlWithFirstNullsSelectWithOverAscAndNullsLastShouldBeEmulated() {
    final String query = "SELECT row_number() "
        + "over (order by \"hire_date\" nulls last) FROM \"employee\"";
    final String expected = "SELECT ROW_NUMBER() "
        + "OVER (ORDER BY `hire_date` IS NULL, `hire_date`)\n"
        + "FROM `foodmart`.`employee`";
    sql(query).dialect(mySqlDialect(NullCollation.FIRST)).ok(expected);
  }

  @Test void testMySqlWithLastNullsSelectWithOrderByDescAndNullsFirstShouldBeEmulated() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" desc nulls first";
    final String expected = "SELECT `product_id`\n"
        + "FROM `foodmart`.`product`\n"
        + "ORDER BY `product_id` IS NULL DESC, `product_id` DESC";
    sql(query).dialect(mySqlDialect(NullCollation.LAST)).ok(expected);
  }

  @Test void testMySqlWithLastNullsSelectWithOverDescAndNullsFirstShouldBeEmulated() {
    final String query = "SELECT row_number() "
        + "over (order by \"hire_date\" desc nulls first) FROM \"employee\"";
    final String expected = "SELECT ROW_NUMBER() "
        + "OVER (ORDER BY `hire_date` IS NULL DESC, `hire_date` DESC)\n"
        + "FROM `foodmart`.`employee`";
    sql(query).dialect(mySqlDialect(NullCollation.LAST)).ok(expected);
  }

  @Test void testMySqlWithLastNullsSelectWithOrderByAscAndNullsFirstShouldBeEmulated() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" nulls first";
    final String expected = "SELECT `product_id`\n"
        + "FROM `foodmart`.`product`\n"
        + "ORDER BY `product_id` IS NULL DESC, `product_id`";
    sql(query).dialect(mySqlDialect(NullCollation.LAST)).ok(expected);
  }

  @Test void testMySqlWithLastNullsSelectWithOverAscAndNullsFirstShouldBeEmulated() {
    final String query = "SELECT row_number() "
        + "over (order by \"hire_date\" nulls first) FROM \"employee\"";
    final String expected = "SELECT ROW_NUMBER() "
        + "OVER (ORDER BY `hire_date` IS NULL DESC, `hire_date`)\n"
        + "FROM `foodmart`.`employee`";
    sql(query).dialect(mySqlDialect(NullCollation.LAST)).ok(expected);
  }

  @Test void testMySqlWithLastNullsSelectWithOrderByDescAndNullsLastShouldNotBeEmulated() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" desc nulls last";
    final String expected = "SELECT `product_id`\n"
        + "FROM `foodmart`.`product`\n"
        + "ORDER BY `product_id` DESC";
    sql(query).dialect(mySqlDialect(NullCollation.LAST)).ok(expected);
  }

  @Test void testMySqlWithLastNullsSelectWithOverDescAndNullsLastShouldNotBeEmulated() {
    final String query = "SELECT row_number() "
        + "over (order by \"hire_date\" desc nulls last) FROM \"employee\"";
    final String expected = "SELECT ROW_NUMBER() OVER (ORDER BY `hire_date` DESC)\n"
        + "FROM `foodmart`.`employee`";
    sql(query).dialect(mySqlDialect(NullCollation.LAST)).ok(expected);
  }

  @Test void testMySqlWithLastNullsSelectWithOrderByAscAndNullsLastShouldNotBeEmulated() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" nulls last";
    final String expected = "SELECT `product_id`\n"
        + "FROM `foodmart`.`product`\n"
        + "ORDER BY `product_id`";
    sql(query).dialect(mySqlDialect(NullCollation.LAST)).ok(expected);
  }

  @Test void testMySqlWithLastNullsSelectWithOverAscAndNullsLastShouldNotBeEmulated() {
    final String query = "SELECT row_number() over "
        + "(order by \"hire_date\" nulls last) FROM \"employee\"";
    final String expected = "SELECT ROW_NUMBER() OVER (ORDER BY `hire_date`)\n"
        + "FROM `foodmart`.`employee`";
    sql(query).dialect(mySqlDialect(NullCollation.LAST)).ok(expected);
  }

  @Test void testCastToVarchar() {
    String query = "select cast(\"product_id\" as varchar) from \"product\"";
    final String expectedClickHouse = "SELECT CAST(`product_id` AS `String`)\n"
        + "FROM `foodmart`.`product`";
    final String expectedMysql = "SELECT CAST(`product_id` AS CHAR)\n"
        + "FROM `foodmart`.`product`";
    sql(query)
        .withClickHouse()
        .ok(expectedClickHouse)
        .withMysql()
        .ok(expectedMysql);
  }

  @Test void testSelectQueryWithLimitClauseWithoutOrder() {
    String query = "select \"product_id\" from \"product\" limit 100 offset 10";
    final String expected = "SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "OFFSET 10 ROWS\n"
        + "FETCH NEXT 100 ROWS ONLY";
    final String expectedClickHouse = "SELECT `product_id`\n"
        + "FROM `foodmart`.`product`\n"
        + "LIMIT 10, 100";
    sql(query)
        .ok(expected)
        .withClickHouse()
        .ok(expectedClickHouse);

    final String expectedPresto = "SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "OFFSET 10\n"
        + "LIMIT 100";
    sql(query)
        .ok(expected)
        .withPresto()
        .ok(expectedPresto);
  }

  @Test void testSelectQueryWithLimitOffsetClause() {
    String query = "select \"product_id\" from \"product\"\n"
        + "order by \"net_weight\" asc limit 100 offset 10";
    final String expected = "SELECT \"product_id\", \"net_weight\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "ORDER BY \"net_weight\"\n"
        + "OFFSET 10 ROWS\n"
        + "FETCH NEXT 100 ROWS ONLY";
    // BigQuery uses LIMIT/OFFSET, and nulls sort low by default
    final String expectedBigQuery = "SELECT product_id, net_weight\n"
        + "FROM foodmart.product\n"
        + "ORDER BY net_weight IS NULL, net_weight\n"
        + "LIMIT 100\n"
        + "OFFSET 10";
    sql(query).ok(expected)
        .withBigQuery().ok(expectedBigQuery);
  }

  @Test void testSelectQueryWithParameters() {
    String query = "select * from \"product\" "
        + "where \"product_id\" = ? "
        + "AND ? >= \"shelf_width\"";
    final String expected = "SELECT *\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE \"product_id\" = ? "
        + "AND ? >= \"shelf_width\"";
    sql(query).ok(expected);
  }

  @Test void testSelectQueryWithFetchOffsetClause() {
    String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" offset 10 rows fetch next 100 rows only";
    final String expected = "SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "ORDER BY \"product_id\"\n"
        + "OFFSET 10 ROWS\n"
        + "FETCH NEXT 100 ROWS ONLY";
    sql(query).ok(expected);
  }

  @Test void testSelectQueryWithFetchClause() {
    String query = "select \"product_id\"\n"
        + "from \"product\"\n"
        + "order by \"product_id\" fetch next 100 rows only";
    final String expected = "SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "ORDER BY \"product_id\"\n"
        + "FETCH NEXT 100 ROWS ONLY";
    final String expectedMssql10 = "SELECT TOP (100) [product_id]\n"
        + "FROM [foodmart].[product]\n"
        + "ORDER BY CASE WHEN [product_id] IS NULL THEN 1 ELSE 0 END, [product_id]";
    final String expectedMssql = "SELECT [product_id]\n"
        + "FROM [foodmart].[product]\n"
        + "ORDER BY CASE WHEN [product_id] IS NULL THEN 1 ELSE 0 END, [product_id]\n"
        + "FETCH NEXT 100 ROWS ONLY";
    final String expectedSybase = "SELECT TOP (100) product_id\n"
        + "FROM foodmart.product\n"
        + "ORDER BY product_id";
    sql(query).ok(expected)
        .withMssql(10).ok(expectedMssql10)
        .withMssql(11).ok(expectedMssql)
        .withMssql(14).ok(expectedMssql)
        .withSybase().ok(expectedSybase);
  }

  @Test void testSelectQueryComplex() {
    String query =
        "select count(*), \"units_per_case\" from \"product\" where \"cases_per_pallet\" > 100 "
            + "group by \"product_id\", \"units_per_case\" order by \"units_per_case\" desc";
    final String expected = "SELECT COUNT(*), \"units_per_case\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE \"cases_per_pallet\" > 100\n"
        + "GROUP BY \"product_id\", \"units_per_case\"\n"
        + "ORDER BY \"units_per_case\" DESC";
    sql(query).ok(expected);
  }

  @Test void testSelectQueryWithGroup() {
    String query = "select"
        + " count(*), sum(\"employee_id\") from \"reserve_employee\" "
        + "where \"hire_date\" > '2015-01-01' "
        + "and (\"position_title\" = 'SDE' or \"position_title\" = 'SDM') "
        + "group by \"store_id\", \"position_title\"";
    final String expected = "SELECT COUNT(*), SUM(\"employee_id\")\n"
        + "FROM \"foodmart\".\"reserve_employee\"\n"
        + "WHERE \"hire_date\" > '2015-01-01' "
        + "AND (\"position_title\" = 'SDE' OR \"position_title\" = 'SDM')\n"
        + "GROUP BY \"store_id\", \"position_title\"";
    sql(query).ok(expected);
  }

  @Test void testSimpleJoin() {
    String query = "select *\n"
        + "from \"sales_fact_1997\" as s\n"
        + "join \"customer\" as c on s.\"customer_id\" = c.\"customer_id\"\n"
        + "join \"product\" as p on s.\"product_id\" = p.\"product_id\"\n"
        + "join \"product_class\" as pc\n"
        + "  on p.\"product_class_id\" = pc.\"product_class_id\"\n"
        + "where c.\"city\" = 'San Francisco'\n"
        + "and pc.\"product_department\" = 'Snacks'\n";
    final String expected = "SELECT *\n"
        + "FROM \"foodmart\".\"sales_fact_1997\"\n"
        + "INNER JOIN \"foodmart\".\"customer\" "
        + "ON \"sales_fact_1997\".\"customer_id\" = \"customer\""
        + ".\"customer_id\"\n"
        + "INNER JOIN \"foodmart\".\"product\" "
        + "ON \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
        + "INNER JOIN \"foodmart\".\"product_class\" "
        + "ON \"product\".\"product_class_id\" = \"product_class\""
        + ".\"product_class_id\"\n"
        + "WHERE \"customer\".\"city\" = 'San Francisco' AND "
        + "\"product_class\".\"product_department\" = 'Snacks'";
    sql(query).ok(expected);
  }

  @Test void testSimpleJoinUsing() {
    String query = "select *\n"
        + "from \"sales_fact_1997\" as s\n"
        + "  join \"customer\" as c using (\"customer_id\")\n"
        + "  join \"product\" as p using (\"product_id\")\n"
        + "  join \"product_class\" as pc using (\"product_class_id\")\n"
        + "where c.\"city\" = 'San Francisco'\n"
        + "and pc.\"product_department\" = 'Snacks'\n";
    final String expected = "SELECT"
        + " \"product\".\"product_class_id\","
        + " \"sales_fact_1997\".\"product_id\","
        + " \"sales_fact_1997\".\"customer_id\","
        + " \"sales_fact_1997\".\"time_id\","
        + " \"sales_fact_1997\".\"promotion_id\","
        + " \"sales_fact_1997\".\"store_id\","
        + " \"sales_fact_1997\".\"store_sales\","
        + " \"sales_fact_1997\".\"store_cost\","
        + " \"sales_fact_1997\".\"unit_sales\","
        + " \"customer\".\"account_num\","
        + " \"customer\".\"lname\","
        + " \"customer\".\"fname\","
        + " \"customer\".\"mi\","
        + " \"customer\".\"address1\","
        + " \"customer\".\"address2\","
        + " \"customer\".\"address3\","
        + " \"customer\".\"address4\","
        + " \"customer\".\"city\","
        + " \"customer\".\"state_province\","
        + " \"customer\".\"postal_code\","
        + " \"customer\".\"country\","
        + " \"customer\".\"customer_region_id\","
        + " \"customer\".\"phone1\","
        + " \"customer\".\"phone2\","
        + " \"customer\".\"birthdate\","
        + " \"customer\".\"marital_status\","
        + " \"customer\".\"yearly_income\","
        + " \"customer\".\"gender\","
        + " \"customer\".\"total_children\","
        + " \"customer\".\"num_children_at_home\","
        + " \"customer\".\"education\","
        + " \"customer\".\"date_accnt_opened\","
        + " \"customer\".\"member_card\","
        + " \"customer\".\"occupation\","
        + " \"customer\".\"houseowner\","
        + " \"customer\".\"num_cars_owned\","
        + " \"customer\".\"fullname\","
        + " \"product\".\"brand_name\","
        + " \"product\".\"product_name\","
        + " \"product\".\"SKU\","
        + " \"product\".\"SRP\","
        + " \"product\".\"gross_weight\","
        + " \"product\".\"net_weight\","
        + " \"product\".\"recyclable_package\","
        + " \"product\".\"low_fat\","
        + " \"product\".\"units_per_case\","
        + " \"product\".\"cases_per_pallet\","
        + " \"product\".\"shelf_width\","
        + " \"product\".\"shelf_height\","
        + " \"product\".\"shelf_depth\","
        + " \"product_class\".\"product_subcategory\","
        + " \"product_class\".\"product_category\","
        + " \"product_class\".\"product_department\","
        + " \"product_class\".\"product_family\"\n"
        + "FROM \"foodmart\".\"sales_fact_1997\"\n"
        + "INNER JOIN \"foodmart\".\"customer\" "
        + "ON \"sales_fact_1997\".\"customer_id\" = \"customer\""
        + ".\"customer_id\"\n"
        + "INNER JOIN \"foodmart\".\"product\" "
        + "ON \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
        + "INNER JOIN \"foodmart\".\"product_class\" "
        + "ON \"product\".\"product_class_id\" = \"product_class\""
        + ".\"product_class_id\"\n"
        + "WHERE \"customer\".\"city\" = 'San Francisco' AND "
        + "\"product_class\".\"product_department\" = 'Snacks'";
    sql(query).ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1636">[CALCITE-1636]
   * JDBC adapter generates wrong SQL for self join with sub-query</a>. */
  @Test void testSubQueryAlias() {
    String query = "select t1.\"customer_id\", t2.\"customer_id\"\n"
        + "from (select \"customer_id\" from \"sales_fact_1997\") as t1\n"
        + "inner join (select \"customer_id\" from \"sales_fact_1997\") t2\n"
        + "on t1.\"customer_id\" = t2.\"customer_id\"";
    final String expected = "SELECT *\n"
        + "FROM (SELECT sales_fact_1997.customer_id\n"
        + "FROM foodmart.sales_fact_1997 AS sales_fact_1997) AS t\n"
        + "INNER JOIN (SELECT sales_fact_19970.customer_id\n"
        + "FROM foodmart.sales_fact_1997 AS sales_fact_19970) AS t0 ON t.customer_id = t0"
        + ".customer_id";

    sql(query).withDb2().ok(expected);
  }

  @Test void testCartesianProductWithCommaSyntax() {
    String query = "select * from \"department\" , \"employee\"";
    String expected = "SELECT *\n"
        + "FROM \"foodmart\".\"department\",\n"
        + "\"foodmart\".\"employee\"";
    sql(query).ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2652">[CALCITE-2652]
   * SqlNode to SQL conversion fails if the join condition references a BOOLEAN
   * column</a>. */
  @Test void testJoinOnBoolean() {
    final String sql = "SELECT 1\n"
        + "from emps\n"
        + "join emp on (emp.deptno = emps.empno and manager)";
    final String s = sql(sql).schema(CalciteAssert.SchemaSpec.POST).exec();
    assertThat(s, notNullValue()); // sufficient that conversion did not throw
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4249">[CALCITE-4249]
   * JDBC adapter cannot translate NOT LIKE in join condition</a>. */
  @Test void testJoinOnNotLike() {
    final Function<RelBuilder, RelNode> relFn = b ->
        b.scan("EMP")
            .scan("DEPT")
            .join(JoinRelType.LEFT,
                b.and(
                    b.equals(b.field(2, 0, "DEPTNO"),
                        b.field(2, 1, "DEPTNO")),
                    b.not(
                        b.call(SqlStdOperatorTable.LIKE,
                            b.field(2, 1, "DNAME"),
                            b.literal("ACCOUNTING")))))
            .build();
    final String expectedSql = "SELECT *\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "LEFT JOIN \"scott\".\"DEPT\" "
        + "ON \"EMP\".\"DEPTNO\" = \"DEPT\".\"DEPTNO\" "
        + "AND \"DEPT\".\"DNAME\" NOT LIKE 'ACCOUNTING'";
    relFn(relFn).ok(expectedSql);
  }

  @Test void testCartesianProductWithInnerJoinSyntax() {
    String query = "select * from \"department\"\n"
        + "INNER JOIN \"employee\" ON TRUE";
    String expected = "SELECT *\n"
        + "FROM \"foodmart\".\"department\",\n"
        + "\"foodmart\".\"employee\"";
    sql(query).ok(expected);
  }

  @Test void testFullJoinOnTrueCondition() {
    String query = "select * from \"department\"\n"
        + "FULL JOIN \"employee\" ON TRUE";
    String expected = "SELECT *\n"
        + "FROM \"foodmart\".\"department\"\n"
        + "FULL JOIN \"foodmart\".\"employee\" ON TRUE";
    sql(query).ok(expected);
  }

  @Disabled
  @Test void testCaseOnSubQuery() {
    String query = "SELECT CASE WHEN v.g IN (0, 1) THEN 0 ELSE 1 END\n"
        + "FROM (SELECT * FROM \"foodmart\".\"customer\") AS c,\n"
        + "  (SELECT 0 AS g) AS v\n"
        + "GROUP BY v.g";
    final String expected = "SELECT"
        + " CASE WHEN \"t0\".\"G\" IN (0, 1) THEN 0 ELSE 1 END\n"
        + "FROM (SELECT *\nFROM \"foodmart\".\"customer\") AS \"t\",\n"
        + "(VALUES (0)) AS \"t0\" (\"G\")\n"
        + "GROUP BY \"t0\".\"G\"";
    sql(query).ok(expected);
  }

  @Test void testSimpleIn() {
    String query = "select * from \"department\" where \"department_id\" in (\n"
        + "  select \"department_id\" from \"employee\"\n"
        + "  where \"store_id\" < 150)";
    final String expected = "SELECT "
        + "\"department\".\"department_id\", \"department\""
        + ".\"department_description\"\n"
        + "FROM \"foodmart\".\"department\"\nINNER JOIN "
        + "(SELECT \"department_id\"\nFROM \"foodmart\".\"employee\"\n"
        + "WHERE \"store_id\" < 150\nGROUP BY \"department_id\") AS \"t1\" "
        + "ON \"department\".\"department_id\" = \"t1\".\"department_id\"";
    sql(query).ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1332">[CALCITE-1332]
   * DB2 should always use aliases for tables: x.y.z AS z</a>. */
  @Test void testDb2DialectJoinStar() {
    String query = "select * "
        + "from \"foodmart\".\"employee\" A "
        + "join \"foodmart\".\"department\" B\n"
        + "on A.\"department_id\" = B.\"department_id\"";
    final String expected = "SELECT *\n"
        + "FROM foodmart.employee AS employee\n"
        + "INNER JOIN foodmart.department AS department "
        + "ON employee.department_id = department.department_id";
    sql(query).withDb2().ok(expected);
  }

  @Test void testDb2DialectSelfJoinStar() {
    String query = "select * "
        + "from \"foodmart\".\"employee\" A join \"foodmart\".\"employee\" B\n"
        + "on A.\"department_id\" = B.\"department_id\"";
    final String expected = "SELECT *\n"
        + "FROM foodmart.employee AS employee\n"
        + "INNER JOIN foodmart.employee AS employee0 "
        + "ON employee.department_id = employee0.department_id";
    sql(query).withDb2().ok(expected);
  }

  @Test void testDb2DialectJoin() {
    String query = "select A.\"employee_id\", B.\"department_id\" "
        + "from \"foodmart\".\"employee\" A join \"foodmart\".\"department\" B\n"
        + "on A.\"department_id\" = B.\"department_id\"";
    final String expected = "SELECT"
        + " employee.employee_id, department.department_id\n"
        + "FROM foodmart.employee AS employee\n"
        + "INNER JOIN foodmart.department AS department "
        + "ON employee.department_id = department.department_id";
    sql(query).withDb2().ok(expected);
  }

  @Test void testDb2DialectSelfJoin() {
    String query = "select A.\"employee_id\", B.\"employee_id\" from "
        + "\"foodmart\".\"employee\" A join \"foodmart\".\"employee\" B\n"
        + "on A.\"department_id\" = B.\"department_id\"";
    final String expected = "SELECT"
        + " employee.employee_id, employee0.employee_id AS employee_id0\n"
        + "FROM foodmart.employee AS employee\n"
        + "INNER JOIN foodmart.employee AS employee0 "
        + "ON employee.department_id = employee0.department_id";
    sql(query).withDb2().ok(expected);
  }

  @Test void testDb2DialectWhere() {
    String query = "select A.\"employee_id\" from "
        + "\"foodmart\".\"employee\" A where A.\"department_id\" < 1000";
    final String expected = "SELECT employee.employee_id\n"
        + "FROM foodmart.employee AS employee\n"
        + "WHERE employee.department_id < 1000";
    sql(query).withDb2().ok(expected);
  }

  @Test void testDb2DialectJoinWhere() {
    String query = "select A.\"employee_id\", B.\"department_id\" "
        + "from \"foodmart\".\"employee\" A join \"foodmart\".\"department\" B\n"
        + "on A.\"department_id\" = B.\"department_id\" "
        + "where A.\"employee_id\" < 1000";
    final String expected = "SELECT"
        + " employee.employee_id, department.department_id\n"
        + "FROM foodmart.employee AS employee\n"
        + "INNER JOIN foodmart.department AS department "
        + "ON employee.department_id = department.department_id\n"
        + "WHERE employee.employee_id < 1000";
    sql(query).withDb2().ok(expected);
  }

  @Test void testDb2DialectSelfJoinWhere() {
    String query = "select A.\"employee_id\", B.\"employee_id\" from "
        + "\"foodmart\".\"employee\" A join \"foodmart\".\"employee\" B\n"
        + "on A.\"department_id\" = B.\"department_id\" "
        + "where B.\"employee_id\" < 2000";
    final String expected = "SELECT "
        + "employee.employee_id, employee0.employee_id AS employee_id0\n"
        + "FROM foodmart.employee AS employee\n"
        + "INNER JOIN foodmart.employee AS employee0 "
        + "ON employee.department_id = employee0.department_id\n"
        + "WHERE employee0.employee_id < 2000";
    sql(query).withDb2().ok(expected);
  }

  @Test void testDb2DialectCast() {
    String query = "select \"hire_date\", cast(\"hire_date\" as varchar(10)) "
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT reserve_employee.hire_date, "
        + "CAST(reserve_employee.hire_date AS VARCHAR(10))\n"
        + "FROM foodmart.reserve_employee AS reserve_employee";
    sql(query).withDb2().ok(expected);
  }

  @Test void testDb2DialectSelectQueryWithGroupByHaving() {
    String query = "select count(*) from \"product\" "
        + "group by \"product_class_id\", \"product_id\" "
        + "having \"product_id\"  > 10";
    final String expected = "SELECT COUNT(*)\n"
        + "FROM foodmart.product AS product\n"
        + "GROUP BY product.product_class_id, product.product_id\n"
        + "HAVING product.product_id > 10";
    sql(query).withDb2().ok(expected);
  }


  @Test void testDb2DialectSelectQueryComplex() {
    String query = "select count(*), \"units_per_case\" "
        + "from \"product\" where \"cases_per_pallet\" > 100 "
        + "group by \"product_id\", \"units_per_case\" "
        + "order by \"units_per_case\" desc";
    final String expected = "SELECT COUNT(*), product.units_per_case\n"
        + "FROM foodmart.product AS product\n"
        + "WHERE product.cases_per_pallet > 100\n"
        + "GROUP BY product.product_id, product.units_per_case\n"
        + "ORDER BY product.units_per_case DESC";
    sql(query).withDb2().ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4090">[CALCITE-4090]
   * DB2 aliasing breaks with a complex SELECT above a sub-query</a>. */
  @Test void testDb2SubQueryAlias() {
    String query = "select count(foo), \"units_per_case\"\n"
        + "from (select \"units_per_case\", \"cases_per_pallet\",\n"
        + "      \"product_id\", 1 as foo\n"
        + "  from \"product\")\n"
        + "where \"cases_per_pallet\" > 100\n"
        + "group by \"product_id\", \"units_per_case\"\n"
        + "order by \"units_per_case\" desc";
    final String expected = "SELECT COUNT(*), t.units_per_case\n"
        + "FROM (SELECT product.units_per_case, product.cases_per_pallet, "
        + "product.product_id, 1 AS FOO\n"
        + "FROM foodmart.product AS product) AS t\n"
        + "WHERE t.cases_per_pallet > 100\n"
        + "GROUP BY t.product_id, t.units_per_case\n"
        + "ORDER BY t.units_per_case DESC";
    sql(query).withDb2().ok(expected);
  }

  @Test void testDb2SubQueryFromUnion() {
    String query = "select count(foo), \"units_per_case\"\n"
        + "from (select \"units_per_case\", \"cases_per_pallet\",\n"
        + "      \"product_id\", 1 as foo\n"
        + "  from \"product\"\n"
        + "  where \"cases_per_pallet\" > 100\n"
        + "  union all\n"
        + "  select \"units_per_case\", \"cases_per_pallet\",\n"
        + "      \"product_id\", 1 as foo\n"
        + "  from \"product\"\n"
        + "  where \"cases_per_pallet\" < 100)\n"
        + "where \"cases_per_pallet\" > 100\n"
        + "group by \"product_id\", \"units_per_case\"\n"
        + "order by \"units_per_case\" desc";
    final String expected = "SELECT COUNT(*), t3.units_per_case\n"
        + "FROM (SELECT product.units_per_case, product.cases_per_pallet, "
        + "product.product_id, 1 AS FOO\n"
        + "FROM foodmart.product AS product\n"
        + "WHERE product.cases_per_pallet > 100\n"
        + "UNION ALL\n"
        + "SELECT product0.units_per_case, product0.cases_per_pallet, "
        + "product0.product_id, 1 AS FOO\n"
        + "FROM foodmart.product AS product0\n"
        + "WHERE product0.cases_per_pallet < 100) AS t3\n"
        + "WHERE t3.cases_per_pallet > 100\n"
        + "GROUP BY t3.product_id, t3.units_per_case\n"
        + "ORDER BY t3.units_per_case DESC";
    sql(query).withDb2().ok(expected);
  }

  @Test void testDb2DialectSelectQueryWithGroup() {
    String query = "select count(*), sum(\"employee_id\") "
        + "from \"reserve_employee\" "
        + "where \"hire_date\" > '2015-01-01' "
        + "and (\"position_title\" = 'SDE' or \"position_title\" = 'SDM') "
        + "group by \"store_id\", \"position_title\"";
    final String expected = "SELECT"
        + " COUNT(*), SUM(reserve_employee.employee_id)\n"
        + "FROM foodmart.reserve_employee AS reserve_employee\n"
        + "WHERE reserve_employee.hire_date > '2015-01-01' "
        + "AND (reserve_employee.position_title = 'SDE' OR "
        + "reserve_employee.position_title = 'SDM')\n"
        + "GROUP BY reserve_employee.store_id, reserve_employee.position_title";
    sql(query).withDb2().ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1372">[CALCITE-1372]
   * JDBC adapter generates SQL with wrong field names</a>. */
  @Test void testJoinPlan2() {
    final String sql = "SELECT v1.deptno, v2.deptno\n"
        + "FROM dept v1 LEFT JOIN emp v2 ON v1.deptno = v2.deptno\n"
        + "WHERE v2.job LIKE 'PRESIDENT'";
    final String expected = "SELECT \"DEPT\".\"DEPTNO\","
        + " \"EMP\".\"DEPTNO\" AS \"DEPTNO0\"\n"
        + "FROM \"SCOTT\".\"DEPT\"\n"
        + "LEFT JOIN \"SCOTT\".\"EMP\""
        + " ON \"DEPT\".\"DEPTNO\" = \"EMP\".\"DEPTNO\"\n"
        + "WHERE \"EMP\".\"JOB\" LIKE 'PRESIDENT'";
    // DB2 does not have implicit aliases, so generates explicit "AS DEPT"
    // and "AS EMP"
    final String expectedDb2 = "SELECT DEPT.DEPTNO, EMP.DEPTNO AS DEPTNO0\n"
        + "FROM SCOTT.DEPT AS DEPT\n"
        + "LEFT JOIN SCOTT.EMP AS EMP ON DEPT.DEPTNO = EMP.DEPTNO\n"
        + "WHERE EMP.JOB LIKE 'PRESIDENT'";
    sql(sql)
        .schema(CalciteAssert.SchemaSpec.JDBC_SCOTT)
        .ok(expected)
        .withDb2()
        .ok(expectedDb2);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1422">[CALCITE-1422]
   * In JDBC adapter, allow IS NULL and IS NOT NULL operators in generated SQL
   * join condition</a>. */
  @Test void testSimpleJoinConditionWithIsNullOperators() {
    String query = "select *\n"
        + "from \"foodmart\".\"sales_fact_1997\" as \"t1\"\n"
        + "inner join \"foodmart\".\"customer\" as \"t2\"\n"
        + "on \"t1\".\"customer_id\" = \"t2\".\"customer_id\" or "
        + "(\"t1\".\"customer_id\" is null "
        + "and \"t2\".\"customer_id\" is null) or\n"
        + "\"t2\".\"occupation\" is null\n"
        + "inner join \"foodmart\".\"product\" as \"t3\"\n"
        + "on \"t1\".\"product_id\" = \"t3\".\"product_id\" or "
        + "(\"t1\".\"product_id\" is not null or "
        + "\"t3\".\"product_id\" is not null)";
    // Some of the "IS NULL" and "IS NOT NULL" are reduced to TRUE or FALSE,
    // but not all.
    String expected = "SELECT *\nFROM \"foodmart\".\"sales_fact_1997\"\n"
        + "INNER JOIN \"foodmart\".\"customer\" "
        + "ON \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\""
        + " OR FALSE AND FALSE"
        + " OR \"customer\".\"occupation\" IS NULL\n"
        + "INNER JOIN \"foodmart\".\"product\" "
        + "ON \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\""
        + " OR TRUE"
        + " OR TRUE";
    // The hook prevents RelBuilder from removing "FALSE AND FALSE" and such
    try (Hook.Closeable ignore =
             Hook.REL_BUILDER_SIMPLIFY.addThread(Hook.propertyJ(false))) {
      sql(query).ok(expected);
    }
  }


  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1586">[CALCITE-1586]
   * JDBC adapter generates wrong SQL if UNION has more than two inputs</a>. */
  @Test void testThreeQueryUnion() {
    String query = "SELECT \"product_id\" FROM \"product\" "
        + " UNION ALL "
        + "SELECT \"product_id\" FROM \"sales_fact_1997\" "
        + " UNION ALL "
        + "SELECT \"product_class_id\" AS product_id FROM \"product_class\"";
    String expected = "SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "UNION ALL\n"
        + "SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"sales_fact_1997\"\n"
        + "UNION ALL\n"
        + "SELECT \"product_class_id\" AS \"PRODUCT_ID\"\n"
        + "FROM \"foodmart\".\"product_class\"";

    final RuleSet rules = RuleSets.ofList(CoreRules.UNION_MERGE);
    sql(query)
        .optimize(rules, null)
        .ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1800">[CALCITE-1800]
   * JDBC adapter fails to SELECT FROM a UNION query</a>. */
  @Test void testUnionWrappedInASelect() {
    final String query = "select sum(\n"
        + "  case when \"product_id\"=0 then \"net_weight\" else 0 end)"
        + " as net_weight\n"
        + "from (\n"
        + "  select \"product_id\", \"net_weight\"\n"
        + "  from \"product\"\n"
        + "  union all\n"
        + "  select \"product_id\", 0 as \"net_weight\"\n"
        + "  from \"sales_fact_1997\") t0";
    final String expected = "SELECT SUM(CASE WHEN \"product_id\" = 0"
        + " THEN \"net_weight\" ELSE 0 END) AS \"NET_WEIGHT\"\n"
        + "FROM (SELECT \"product_id\", \"net_weight\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "UNION ALL\n"
        + "SELECT \"product_id\", 0 AS \"net_weight\"\n"
        + "FROM \"foodmart\".\"sales_fact_1997\") AS \"t1\"";
    sql(query).ok(expected);
  }

  @Test void testLiteral() {
    checkLiteral("DATE '1978-05-02'");
    checkLiteral2("DATE '1978-5-2'", "DATE '1978-05-02'");
    checkLiteral("TIME '12:34:56'");
    checkLiteral("TIME '12:34:56.78'");
    checkLiteral2("TIME '1:4:6.080'", "TIME '01:04:06.080'");
    checkLiteral("TIMESTAMP '1978-05-02 12:34:56.78'");
    checkLiteral2("TIMESTAMP '1978-5-2 2:4:6.80'",
        "TIMESTAMP '1978-05-02 02:04:06.80'");
    checkLiteral("'I can''t explain'");
    checkLiteral("''");
    checkLiteral("TRUE");
    checkLiteral("123");
    checkLiteral("123.45");
    checkLiteral("-123.45");
    checkLiteral("INTERVAL '1-2' YEAR TO MONTH");
    checkLiteral("INTERVAL -'1-2' YEAR TO MONTH");
    checkLiteral("INTERVAL '12-11' YEAR TO MONTH");
    checkLiteral("INTERVAL '1' YEAR");
    checkLiteral("INTERVAL '1' MONTH");
    checkLiteral("INTERVAL '12' DAY");
    checkLiteral("INTERVAL -'12' DAY");
    checkLiteral2("INTERVAL '1 2' DAY TO HOUR",
        "INTERVAL '1 02' DAY TO HOUR");
    checkLiteral2("INTERVAL '1 2:10' DAY TO MINUTE",
        "INTERVAL '1 02:10' DAY TO MINUTE");
    checkLiteral2("INTERVAL '1 2:00' DAY TO MINUTE",
        "INTERVAL '1 02:00' DAY TO MINUTE");
    checkLiteral2("INTERVAL '1 2:34:56' DAY TO SECOND",
        "INTERVAL '1 02:34:56' DAY TO SECOND");
    checkLiteral2("INTERVAL '1 2:34:56.789' DAY TO SECOND",
        "INTERVAL '1 02:34:56.789' DAY TO SECOND");
    checkLiteral2("INTERVAL '1 2:34:56.78' DAY TO SECOND",
        "INTERVAL '1 02:34:56.78' DAY TO SECOND");
    checkLiteral2("INTERVAL '1 2:34:56.078' DAY TO SECOND",
        "INTERVAL '1 02:34:56.078' DAY TO SECOND");
    checkLiteral2("INTERVAL -'1 2:34:56.078' DAY TO SECOND",
        "INTERVAL -'1 02:34:56.078' DAY TO SECOND");
    checkLiteral2("INTERVAL '1 2:3:5.070' DAY TO SECOND",
        "INTERVAL '1 02:03:05.07' DAY TO SECOND");
    checkLiteral("INTERVAL '1:23' HOUR TO MINUTE");
    checkLiteral("INTERVAL '1:02' HOUR TO MINUTE");
    checkLiteral("INTERVAL -'1:02' HOUR TO MINUTE");
    checkLiteral("INTERVAL '1:23:45' HOUR TO SECOND");
    checkLiteral("INTERVAL '1:03:05' HOUR TO SECOND");
    checkLiteral("INTERVAL '1:23:45.678' HOUR TO SECOND");
    checkLiteral("INTERVAL '1:03:05.06' HOUR TO SECOND");
    checkLiteral("INTERVAL '12' MINUTE");
    checkLiteral("INTERVAL '12:34' MINUTE TO SECOND");
    checkLiteral("INTERVAL '12:34.567' MINUTE TO SECOND");
    checkLiteral("INTERVAL '12' SECOND");
    checkLiteral("INTERVAL '12.345' SECOND");
  }

  private void checkLiteral(String expression) {
    checkLiteral2(expression, expression);
  }

  private void checkLiteral2(String expression, String expected) {
    sql("VALUES " + expression)
        .withHsqldb()
        .ok("SELECT *\n"
            + "FROM (VALUES (" + expected + ")) AS t (EXPR$0)");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2625">[CALCITE-2625]
   * Removing Window Boundaries from SqlWindow of Aggregate Function which do
   * not allow Framing</a>. */
  @Test void testRowNumberFunctionForPrintingOfFrameBoundary() {
    String query = "SELECT row_number() over (order by \"hire_date\") FROM \"employee\"";
    String expected = "SELECT ROW_NUMBER() OVER (ORDER BY \"hire_date\")\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query).ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3112">[CALCITE-3112]
   * Support Window in RelToSqlConverter</a>. */
  @Test void testConvertWindowToSql() {
    String query0 = "SELECT row_number() over (order by \"hire_date\") FROM \"employee\"";
    String expected0 = "SELECT ROW_NUMBER() OVER (ORDER BY \"hire_date\") AS \"$0\"\n"
        + "FROM \"foodmart\".\"employee\"";

    String query1 = "SELECT rank() over (order by \"hire_date\") FROM \"employee\"";
    String expected1 = "SELECT RANK() OVER (ORDER BY \"hire_date\") AS \"$0\"\n"
        + "FROM \"foodmart\".\"employee\"";

    String query2 = "SELECT lead(\"employee_id\",1,'NA') over "
        + "(partition by \"hire_date\" order by \"employee_id\")\n"
        + "FROM \"employee\"";
    String expected2 = "SELECT LEAD(\"employee_id\", 1, 'NA') OVER "
        + "(PARTITION BY \"hire_date\" "
        + "ORDER BY \"employee_id\") AS \"$0\"\n"
        + "FROM \"foodmart\".\"employee\"";

    String query3 = "SELECT lag(\"employee_id\",1,'NA') over "
        + "(partition by \"hire_date\" order by \"employee_id\")\n"
        + "FROM \"employee\"";
    String expected3 = "SELECT LAG(\"employee_id\", 1, 'NA') OVER "
        + "(PARTITION BY \"hire_date\" ORDER BY \"employee_id\") AS \"$0\"\n"
        + "FROM \"foodmart\".\"employee\"";

    String query4 = "SELECT lag(\"employee_id\",1,'NA') "
        + "over (partition by \"hire_date\" order by \"employee_id\") as lag1, "
        + "lag(\"employee_id\",1,'NA') "
        + "over (partition by \"birth_date\" order by \"employee_id\") as lag2, "
        + "count(*) over (partition by \"hire_date\" order by \"employee_id\") as count1, "
        + "count(*) over (partition by \"birth_date\" order by \"employee_id\") as count2\n"
        + "FROM \"employee\"";
    String expected4 = "SELECT LAG(\"employee_id\", 1, 'NA') OVER "
        + "(PARTITION BY \"hire_date\" ORDER BY \"employee_id\") AS \"$0\", "
        + "LAG(\"employee_id\", 1, 'NA') OVER "
        + "(PARTITION BY \"birth_date\" ORDER BY \"employee_id\") AS \"$1\", "
        + "COUNT(*) OVER (PARTITION BY \"hire_date\" ORDER BY \"employee_id\" "
        + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS \"$2\", "
        + "COUNT(*) OVER (PARTITION BY \"birth_date\" ORDER BY \"employee_id\" "
        + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS \"$3\"\n"
        + "FROM \"foodmart\".\"employee\"";

    String query5 = "SELECT lag(\"employee_id\",1,'NA') "
        + "over (partition by \"hire_date\" order by \"employee_id\") as lag1, "
        + "lag(\"employee_id\",1,'NA') "
        + "over (partition by \"birth_date\" order by \"employee_id\") as lag2, "
        + "max(sum(\"employee_id\")) over (partition by \"hire_date\" order by \"employee_id\") "
        + "as count1, "
        + "max(sum(\"employee_id\")) over (partition by \"birth_date\" order by \"employee_id\") "
        + "as count2\n"
        + "FROM \"employee\" group by \"employee_id\", \"hire_date\", \"birth_date\"";
    String expected5 = "SELECT LAG(\"employee_id\", 1, 'NA') OVER "
        + "(PARTITION BY \"hire_date\" ORDER BY \"employee_id\") AS \"$0\", "
        + "LAG(\"employee_id\", 1, 'NA') OVER "
        + "(PARTITION BY \"birth_date\" ORDER BY \"employee_id\") AS \"$1\", "
        + "MAX(SUM(\"employee_id\")) OVER (PARTITION BY \"hire_date\" ORDER BY \"employee_id\" "
        + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS \"$2\", "
        + "MAX(SUM(\"employee_id\")) OVER (PARTITION BY \"birth_date\" ORDER BY \"employee_id\" "
        + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS \"$3\"\n"
        + "FROM \"foodmart\".\"employee\"\n"
        + "GROUP BY \"employee_id\", \"hire_date\", \"birth_date\"";

    String query6 = "SELECT lag(\"employee_id\",1,'NA') over "
        + "(partition by \"hire_date\" order by \"employee_id\"), \"hire_date\"\n"
        + "FROM \"employee\"\n"
        + "group by \"hire_date\", \"employee_id\"";
    String expected6 = "SELECT LAG(\"employee_id\", 1, 'NA') "
        + "OVER (PARTITION BY \"hire_date\" ORDER BY \"employee_id\"), \"hire_date\"\n"
        + "FROM \"foodmart\".\"employee\"\n"
        + "GROUP BY \"hire_date\", \"employee_id\"";
    String query7 = "SELECT "
        + "count(distinct \"employee_id\") over (order by \"hire_date\") FROM \"employee\"";
    String expected7 = "SELECT "
        + "COUNT(DISTINCT \"employee_id\") "
        + "OVER (ORDER BY \"hire_date\" RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS \"$0\""
        + "\nFROM \"foodmart\".\"employee\"";

    String query8 = "SELECT "
        + "sum(distinct \"position_id\") over (order by \"hire_date\") FROM \"employee\"";
    String expected8 =
        "SELECT CASE WHEN (COUNT(DISTINCT \"position_id\") OVER (ORDER BY \"hire_date\" "
            + "RANGE"
            + " BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) > 0 THEN COALESCE(SUM(DISTINCT "
            + "\"position_id\") OVER (ORDER BY \"hire_date\" RANGE BETWEEN UNBOUNDED "
            + "PRECEDING AND CURRENT ROW), 0) ELSE NULL END\n"
            + "FROM \"foodmart\".\"employee\"";

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ProjectToWindowRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    RuleSet rules = RuleSets.ofList(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);

    sql(query0).optimize(rules, hepPlanner).ok(expected0);
    sql(query1).optimize(rules, hepPlanner).ok(expected1);
    sql(query2).optimize(rules, hepPlanner).ok(expected2);
    sql(query3).optimize(rules, hepPlanner).ok(expected3);
    sql(query4).optimize(rules, hepPlanner).ok(expected4);
    sql(query5).optimize(rules, hepPlanner).ok(expected5);
    sql(query6).optimize(rules, hepPlanner).ok(expected6);
    sql(query7).optimize(rules, hepPlanner).ok(expected7);
    sql(query8).optimize(rules, hepPlanner).ok(expected8);
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3866">[CALCITE-3866]
   * "numeric field overflow" when running the generated SQL in PostgreSQL </a>.
   */
  @Test void testSumReturnType() {
    String query =
        "select sum(e1.\"store_sales\"), sum(e2.\"store_sales\") from \"sales_fact_dec_1998\" as "
            + "e1 , \"sales_fact_dec_1998\" as e2 where e1.\"product_id\" = e2.\"product_id\"";

    String expect = "SELECT SUM(CAST(SUM(\"store_sales\") * \"t0\".\"$f1\" AS DECIMAL"
        + "(19, 4))), SUM(CAST(\"t\".\"$f2\" * SUM(\"store_sales\") AS DECIMAL(19, 4)))\n"
        + "FROM (SELECT \"product_id\", SUM(\"store_sales\"), COUNT(*) AS \"$f2\"\n"
        + "FROM \"foodmart\".\"sales_fact_dec_1998\"\n"
        + "GROUP BY \"product_id\") AS \"t\"\n"
        + "INNER JOIN "
        + "(SELECT \"product_id\", COUNT(*) AS \"$f1\", SUM(\"store_sales\")\n"
        + "FROM \"foodmart\".\"sales_fact_dec_1998\"\n"
        + "GROUP BY \"product_id\") AS \"t0\" ON \"t\".\"product_id\" = \"t0\".\"product_id\"";

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(FilterJoinRule.class);
    builder.addRuleClass(AggregateProjectMergeRule.class);
    builder.addRuleClass(AggregateJoinTransposeRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    RuleSet rules = RuleSets.ofList(
        CoreRules.FILTER_INTO_JOIN,
        CoreRules.JOIN_CONDITION_PUSH,
        CoreRules.AGGREGATE_PROJECT_MERGE, CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED);
    sql(query).withPostgresql().optimize(rules, hepPlanner).ok(expect);
  }

  @Test void testRankFunctionForPrintingOfFrameBoundary() {
    String query = "SELECT rank() over (order by \"hire_date\") FROM \"employee\"";
    String expected = "SELECT RANK() OVER (ORDER BY \"hire_date\")\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query).ok(expected);
  }

  @Test void testLeadFunctionForPrintingOfFrameBoundary() {
    String query = "SELECT lead(\"employee_id\",1,'NA') over "
        + "(partition by \"hire_date\" order by \"employee_id\") FROM \"employee\"";
    String expected = "SELECT LEAD(\"employee_id\", 1, 'NA') OVER "
        + "(PARTITION BY \"hire_date\" ORDER BY \"employee_id\")\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query).ok(expected);
  }

  @Test void testLagFunctionForPrintingOfFrameBoundary() {
    String query = "SELECT lag(\"employee_id\",1,'NA') over "
        + "(partition by \"hire_date\" order by \"employee_id\") FROM \"employee\"";
    String expected = "SELECT LAG(\"employee_id\", 1, 'NA') OVER "
        + "(PARTITION BY \"hire_date\" ORDER BY \"employee_id\")\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query).ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3876">[CALCITE-3876]
   * RelToSqlConverter should not combine Projects when top Project contains
   * window function referencing window function from bottom Project</a>. */
  @Test void testWindowOnWindowDoesNotCombineProjects() {
    final String query = "SELECT ROW_NUMBER() OVER (ORDER BY rn)\n"
        + "FROM (SELECT *,\n"
        + "  ROW_NUMBER() OVER (ORDER BY \"product_id\") as rn\n"
        + "  FROM \"foodmart\".\"product\")";
    final String expected = "SELECT ROW_NUMBER() OVER (ORDER BY \"RN\")\n"
        + "FROM (SELECT \"product_class_id\", \"product_id\", \"brand_name\","
        + " \"product_name\", \"SKU\", \"SRP\", \"gross_weight\","
        + " \"net_weight\", \"recyclable_package\", \"low_fat\","
        + " \"units_per_case\", \"cases_per_pallet\", \"shelf_width\","
        + " \"shelf_height\", \"shelf_depth\","
        + " ROW_NUMBER() OVER (ORDER BY \"product_id\") AS \"RN\"\n"
        + "FROM \"foodmart\".\"product\") AS \"t\"";
    sql(query)
        .withPostgresql()
        .ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1798">[CALCITE-1798]
   * Generate dialect-specific SQL for FLOOR operator</a>. */
  @Test void testFloor() {
    String query = "SELECT floor(\"hire_date\" TO MINUTE) FROM \"employee\"";
    String expected = "SELECT TRUNC(hire_date, 'MI')\nFROM foodmart.employee";
    sql(query)
        .withHsqldb()
        .ok(expected);
  }

  @Test void testFloorClickHouse() {
    String query = "SELECT floor(\"hire_date\" TO MINUTE) FROM \"employee\"";
    String expected = "SELECT toStartOfMinute(`hire_date`)\nFROM `foodmart`.`employee`";
    sql(query)
        .withClickHouse()
        .ok(expected);
  }

  @Test void testFloorPostgres() {
    String query = "SELECT floor(\"hire_date\" TO MINUTE) FROM \"employee\"";
    String expected = "SELECT DATE_TRUNC('MINUTE', \"hire_date\")\nFROM \"foodmart\".\"employee\"";
    sql(query)
        .withPostgresql()
        .ok(expected);
  }

  @Test void testFloorOracle() {
    String query = "SELECT floor(\"hire_date\" TO MINUTE) FROM \"employee\"";
    String expected = "SELECT TRUNC(\"hire_date\", 'MINUTE')\nFROM \"foodmart\".\"employee\"";
    sql(query)
        .withOracle()
        .ok(expected);
  }

  @Test void testFloorPresto() {
    String query = "SELECT floor(\"hire_date\" TO MINUTE) FROM \"employee\"";
    String expected = "SELECT DATE_TRUNC('MINUTE', \"hire_date\")\nFROM \"foodmart\".\"employee\"";
    sql(query)
        .withPresto()
        .ok(expected);
  }

  @Test void testFloorMssqlWeek() {
    String query = "SELECT floor(\"hire_date\" TO WEEK) FROM \"employee\"";
    String expected = "SELECT CONVERT(DATETIME, CONVERT(VARCHAR(10), "
        + "DATEADD(day, - (6 + DATEPART(weekday, [hire_date] )) % 7, [hire_date] ), 126))\n"
        + "FROM [foodmart].[employee]";
    sql(query).withMssql()
        .ok(expected);
  }

  @Test void testFloorMssqlMonth() {
    String query = "SELECT floor(\"hire_date\" TO MONTH) FROM \"employee\"";
    String expected = "SELECT CONVERT(DATETIME, CONVERT(VARCHAR(7), [hire_date] , 126)+'-01')\n"
        + "FROM [foodmart].[employee]";
    sql(query)
        .withMssql()
        .ok(expected);
  }

  @Test void testFloorMysqlMonth() {
    String query = "SELECT floor(\"hire_date\" TO MONTH) FROM \"employee\"";
    String expected = "SELECT DATE_FORMAT(`hire_date`, '%Y-%m-01')\n"
        + "FROM `foodmart`.`employee`";
    sql(query)
        .withMysql()
        .ok(expected);
  }

  @Test void testFloorWeek() {
    final String query = "SELECT floor(\"hire_date\" TO WEEK) FROM \"employee\"";
    final String expectedClickHouse = "SELECT toMonday(`hire_date`)\n"
        + "FROM `foodmart`.`employee`";
    final String expectedMssql = "SELECT CONVERT(DATETIME, CONVERT(VARCHAR(10), "
        + "DATEADD(day, - (6 + DATEPART(weekday, [hire_date] )) % 7, [hire_date] ), 126))\n"
        + "FROM [foodmart].[employee]";
    final String expectedMysql = "SELECT STR_TO_DATE(DATE_FORMAT(`hire_date` , '%x%v-1'), "
        + "'%x%v-%w')\n"
        + "FROM `foodmart`.`employee`";
    sql(query)
        .withClickHouse()
        .ok(expectedClickHouse)
        .withMssql()
        .ok(expectedMssql)
        .withMysql()
        .ok(expectedMysql);
  }

  @Test void testUnparseSqlIntervalQualifierDb2() {
    String queryDatePlus = "select  * from \"employee\" where  \"hire_date\" + "
        + "INTERVAL '19800' SECOND(5) > TIMESTAMP '2005-10-17 00:00:00' ";
    String expectedDatePlus = "SELECT *\n"
        + "FROM foodmart.employee AS employee\n"
        + "WHERE (employee.hire_date + 19800 SECOND)"
        + " > TIMESTAMP '2005-10-17 00:00:00'";

    sql(queryDatePlus)
        .withDb2()
        .ok(expectedDatePlus);

    String queryDateMinus = "select  * from \"employee\" where  \"hire_date\" - "
        + "INTERVAL '19800' SECOND(5) > TIMESTAMP '2005-10-17 00:00:00' ";
    String expectedDateMinus = "SELECT *\n"
        + "FROM foodmart.employee AS employee\n"
        + "WHERE (employee.hire_date - 19800 SECOND)"
        + " > TIMESTAMP '2005-10-17 00:00:00'";

    sql(queryDateMinus)
        .withDb2()
        .ok(expectedDateMinus);
  }

  @Test void testUnparseSqlIntervalQualifierMySql() {
    final String sql0 = "select  * from \"employee\" where  \"hire_date\" - "
        + "INTERVAL '19800' SECOND(5) > TIMESTAMP '2005-10-17 00:00:00' ";
    final String expect0 = "SELECT *\n"
        + "FROM `foodmart`.`employee`\n"
        + "WHERE (`hire_date` - INTERVAL '19800' SECOND)"
        + " > TIMESTAMP '2005-10-17 00:00:00'";
    sql(sql0).withMysql().ok(expect0);

    final String sql1 = "select  * from \"employee\" where  \"hire_date\" + "
        + "INTERVAL '10' HOUR > TIMESTAMP '2005-10-17 00:00:00' ";
    final String expect1 = "SELECT *\n"
        + "FROM `foodmart`.`employee`\n"
        + "WHERE (`hire_date` + INTERVAL '10' HOUR)"
        + " > TIMESTAMP '2005-10-17 00:00:00'";
    sql(sql1).withMysql().ok(expect1);

    final String sql2 = "select  * from \"employee\" where  \"hire_date\" + "
        + "INTERVAL '1-2' year to month > TIMESTAMP '2005-10-17 00:00:00' ";
    final String expect2 = "SELECT *\n"
        + "FROM `foodmart`.`employee`\n"
        + "WHERE (`hire_date` + INTERVAL '1-2' YEAR_MONTH)"
        + " > TIMESTAMP '2005-10-17 00:00:00'";
    sql(sql2).withMysql().ok(expect2);

    final String sql3 = "select  * from \"employee\" "
        + "where  \"hire_date\" + INTERVAL '39:12' MINUTE TO SECOND"
        + " > TIMESTAMP '2005-10-17 00:00:00' ";
    final String expect3 = "SELECT *\n"
        + "FROM `foodmart`.`employee`\n"
        + "WHERE (`hire_date` + INTERVAL '39:12' MINUTE_SECOND)"
        + " > TIMESTAMP '2005-10-17 00:00:00'";
    sql(sql3).withMysql().ok(expect3);
  }

  @Test void testUnparseSqlIntervalQualifierMsSql() {
    String queryDatePlus = "select  * from \"employee\" where  \"hire_date\" +"
        + "INTERVAL '19800' SECOND(5) > TIMESTAMP '2005-10-17 00:00:00' ";
    String expectedDatePlus = "SELECT *\n"
        + "FROM [foodmart].[employee]\n"
        + "WHERE DATEADD(SECOND, 19800, [hire_date]) > CAST('2005-10-17 00:00:00' AS TIMESTAMP(0))";

    sql(queryDatePlus)
        .withMssql()
        .ok(expectedDatePlus);

    String queryDateMinus = "select  * from \"employee\" where  \"hire_date\" -"
        + "INTERVAL '19800' SECOND(5) > TIMESTAMP '2005-10-17 00:00:00' ";
    String expectedDateMinus = "SELECT *\n"
        + "FROM [foodmart].[employee]\n"
        + "WHERE DATEADD(SECOND, -19800, [hire_date]) > CAST('2005-10-17 00:00:00' AS TIMESTAMP(0))";

    sql(queryDateMinus)
        .withMssql()
        .ok(expectedDateMinus);

    String queryDateMinusNegate = "select  * from \"employee\" "
        + "where  \"hire_date\" -INTERVAL '-19800' SECOND(5)"
        + " > TIMESTAMP '2005-10-17 00:00:00' ";
    String expectedDateMinusNegate = "SELECT *\n"
        + "FROM [foodmart].[employee]\n"
        + "WHERE DATEADD(SECOND, 19800, [hire_date]) > CAST('2005-10-17 00:00:00' AS TIMESTAMP(0))";

    sql(queryDateMinusNegate)
        .withMssql()
        .ok(expectedDateMinusNegate);
  }

  @Test public void testUnparseTimeLiteral() {
    String queryDatePlus = "select TIME '11:25:18' "
        + "from \"employee\"";
    String expectedBQSql = "SELECT TIME '11:25:18'\n"
        + "FROM foodmart.employee";
    String expectedSql = "SELECT CAST('11:25:18' AS TIME(0))\n"
        + "FROM [foodmart].[employee]";
    sql(queryDatePlus)
        .withBigQuery()
        .ok(expectedBQSql)
        .withMssql()
        .ok(expectedSql);
  }

  @Test void testUnparseSqlIntervalQualifierBigQuery() {
    final String sql0 = "select  * from \"employee\" where  \"hire_date\" - "
        + "INTERVAL '19800' SECOND(5) > TIMESTAMP '2005-10-17 00:00:00' ";
    final String expect0 = "SELECT *\n"
        + "FROM foodmart.employee\n"
        + "WHERE TIMESTAMP_SUB(hire_date, INTERVAL 19800 SECOND)"
        + " > CAST('2005-10-17 00:00:00' AS DATETIME)";
    sql(sql0).withBigQuery().ok(expect0);

    final String sql1 = "select  * from \"employee\" where  \"hire_date\" + "
        + "INTERVAL '10' HOUR > TIMESTAMP '2005-10-17 00:00:00' ";
    final String expect1 = "SELECT *\n"
        + "FROM foodmart.employee\n"
        + "WHERE TIMESTAMP_ADD(hire_date, INTERVAL 10 HOUR)"
        + " > CAST('2005-10-17 00:00:00' AS DATETIME)";
    sql(sql1).withBigQuery().ok(expect1);

    final String sql2 = "select  * from \"employee\" where  \"hire_date\" + "
        + "INTERVAL '1 2:34:56.78' DAY TO SECOND > TIMESTAMP '2005-10-17 00:00:00' ";
    sql(sql2).withBigQuery().throws_("For input string: \"56.78\"");
  }

  @Test public void testFloorMysqlWeek() {
    String query = "SELECT floor(\"hire_date\" TO WEEK) FROM \"employee\"";
    String expected = "SELECT STR_TO_DATE(DATE_FORMAT(`hire_date` , '%x%v-1'), '%x%v-%w')\n"
        + "FROM `foodmart`.`employee`";
    sql(query)
        .withMysql()
        .ok(expected);
  }

  @Test void testFloorMonth() {
    final String query = "SELECT floor(\"hire_date\" TO MONTH) FROM \"employee\"";
    final String expectedClickHouse = "SELECT toStartOfMonth(`hire_date`)\n"
        + "FROM `foodmart`.`employee`";
    final String expectedMssql = "SELECT CONVERT(DATETIME, CONVERT(VARCHAR(7), [hire_date] , "
        + "126)+'-01')\n"
        + "FROM [foodmart].[employee]";
    final String expectedMysql = "SELECT DATE_FORMAT(`hire_date`, '%Y-%m-01')\n"
        + "FROM `foodmart`.`employee`";
    sql(query)
        .withClickHouse()
        .ok(expectedClickHouse)
        .withMssql()
        .ok(expectedMssql)
        .withMysql()
        .ok(expectedMysql);
  }

  @Test void testFloorMysqlHour() {
    String query = "SELECT floor(\"hire_date\" TO HOUR) FROM \"employee\"";
    String expected = "SELECT DATE_FORMAT(`hire_date`, '%Y-%m-%d %H:00:00')\n"
        + "FROM `foodmart`.`employee`";
    sql(query)
        .withMysql()
        .ok(expected);
  }

  @Test void testFloorMysqlMinute() {
    String query = "SELECT floor(\"hire_date\" TO MINUTE) FROM \"employee\"";
    String expected = "SELECT DATE_FORMAT(`hire_date`, '%Y-%m-%d %H:%i:00')\n"
        + "FROM `foodmart`.`employee`";
    sql(query)
        .withMysql()
        .ok(expected);
  }

  @Test void testFloorMysqlSecond() {
    String query = "SELECT floor(\"hire_date\" TO SECOND) FROM \"employee\"";
    String expected = "SELECT DATE_FORMAT(`hire_date`, '%Y-%m-%d %H:%i:%s')\n"
        + "FROM `foodmart`.`employee`";
    sql(query)
        .withMysql()
        .ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1826">[CALCITE-1826]
   * JDBC dialect-specific FLOOR fails when in GROUP BY</a>. */
  @Test void testFloorWithGroupBy() {
    final String query = "SELECT floor(\"hire_date\" TO MINUTE)\n"
        + "FROM \"employee\"\n"
        + "GROUP BY floor(\"hire_date\" TO MINUTE)";
    final String expected = "SELECT TRUNC(hire_date, 'MI')\n"
        + "FROM foodmart.employee\n"
        + "GROUP BY TRUNC(hire_date, 'MI')";
    final String expectedClickHouse = "SELECT toStartOfMinute(`hire_date`)\n"
        + "FROM `foodmart`.`employee`\n"
        + "GROUP BY toStartOfMinute(`hire_date`)";
    final String expectedOracle = "SELECT TRUNC(\"hire_date\", 'MINUTE')\n"
        + "FROM \"foodmart\".\"employee\"\n"
        + "GROUP BY TRUNC(\"hire_date\", 'MINUTE')";
    final String expectedPostgresql = "SELECT DATE_TRUNC('MINUTE', \"hire_date\")\n"
        + "FROM \"foodmart\".\"employee\"\n"
        + "GROUP BY DATE_TRUNC('MINUTE', \"hire_date\")";
    final String expectedMysql = "SELECT"
        + " DATE_FORMAT(`hire_date`, '%Y-%m-%d %H:%i:00')\n"
        + "FROM `foodmart`.`employee`\n"
        + "GROUP BY DATE_FORMAT(`hire_date`, '%Y-%m-%d %H:%i:00')";
    sql(query)
        .withHsqldb()
        .ok(expected)
        .withClickHouse()
        .ok(expectedClickHouse)
        .withOracle()
        .ok(expectedOracle)
        .withPostgresql()
        .ok(expectedPostgresql)
        .withMysql()
        .ok(expectedMysql);
  }

  @Test void testSubstring() {
    final String query = "select substring(\"brand_name\" from 2) "
        + "from \"product\"\n";
    final String expectedClickHouse = "SELECT substring(`brand_name`, 2)\n"
        + "FROM `foodmart`.`product`";
    final String expectedOracle = "SELECT SUBSTR(\"brand_name\", 2)\n"
        + "FROM \"foodmart\".\"product\"";
    final String expectedPostgresql = "SELECT SUBSTRING(\"brand_name\" FROM 2)\n"
        + "FROM \"foodmart\".\"product\"";
    final String expectedPresto = "SELECT SUBSTR(\"brand_name\", 2)\n"
        + "FROM \"foodmart\".\"product\"";
    final String expectedSnowflake = "SELECT SUBSTR(\"brand_name\", 2)\n"
            + "FROM \"foodmart\".\"product\"";
    final String expectedRedshift = expectedPostgresql;
    final String expectedMysql = "SELECT SUBSTRING(`brand_name` FROM 2)\n"
        + "FROM `foodmart`.`product`";
    final String expectedHive = "SELECT SUBSTRING(brand_name, 2)\n"
        + "FROM foodmart.product";
    final String expectedSpark = "SELECT SUBSTRING(brand_name, 2)\n"
        + "FROM foodmart.product";
    final String expectedBiqQuery = "SELECT SUBSTR(brand_name, 2)\n"
        + "FROM foodmart.product";
    sql(query)
        .withClickHouse()
        .ok(expectedClickHouse)
        .withOracle()
        .ok(expectedOracle)
        .withPostgresql()
        .ok(expectedPostgresql)
        .withPresto()
        .ok(expectedPresto)
        .withSnowflake()
        .ok(expectedSnowflake)
        .withRedshift()
        .ok(expectedRedshift)
        .withMysql()
        .ok(expectedMysql)
        .withMssql()
        // mssql does not support this syntax and so should fail
        .throws_("MSSQL SUBSTRING requires FROM and FOR arguments")
        .withHive()
        .ok(expectedHive)
        .withSpark()
        .ok(expectedSpark)
        .withBigQuery()
        .ok(expectedBiqQuery);
  }

  @Test void testSubstringWithFor() {
    final String query = "select substring(\"brand_name\" from 2 for 3) "
        + "from \"product\"\n";
    final String expectedClickHouse = "SELECT substring(`brand_name`, 2, 3)\n"
        + "FROM `foodmart`.`product`";
    final String expectedOracle = "SELECT SUBSTR(\"brand_name\", 2, 3)\n"
        + "FROM \"foodmart\".\"product\"";
    final String expectedPostgresql = "SELECT SUBSTRING(\"brand_name\" FROM 2 FOR 3)\n"
        + "FROM \"foodmart\".\"product\"";
    final String expectedPresto = "SELECT SUBSTR(\"brand_name\", 2, 3)\n"
        + "FROM \"foodmart\".\"product\"";
    final String expectedSnowflake = "SELECT SUBSTR(\"brand_name\", 2, 3)\n"
            + "FROM \"foodmart\".\"product\"";
    final String expectedRedshift = expectedPostgresql;
    final String expectedMysql = "SELECT SUBSTRING(`brand_name` FROM 2 FOR 3)\n"
        + "FROM `foodmart`.`product`";
    final String expectedMssql = "SELECT SUBSTRING([brand_name], 2, 3)\n"
        + "FROM [foodmart].[product]";
    final String expectedHive = "SELECT SUBSTRING(brand_name, 2, 3)\n"
        + "FROM foodmart.product";
    final String expectedSpark = "SELECT SUBSTRING(brand_name, 2, 3)\n"
        + "FROM foodmart.product";
    sql(query)
        .withClickHouse()
        .ok(expectedClickHouse)
        .withOracle()
        .ok(expectedOracle)
        .withPostgresql()
        .ok(expectedPostgresql)
        .withPresto()
        .ok(expectedPresto)
        .withSnowflake()
        .ok(expectedSnowflake)
        .withRedshift()
        .ok(expectedRedshift)
        .withMysql()
        .ok(expectedMysql)
        .withMssql()
        .ok(expectedMssql)
        .withSpark()
        .ok(expectedSpark)
        .withHive()
        .ok(expectedHive);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1849">[CALCITE-1849]
   * Support sub-queries (RexSubQuery) in RelToSqlConverter</a>. */
  @Test void testExistsWithExpand() {
    String query = "select \"product_name\" from \"product\" a "
        + "where exists (select count(*) "
        + "from \"sales_fact_1997\"b "
        + "where b.\"product_id\" = a.\"product_id\")";
    String expected = "SELECT \"product_name\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE EXISTS (SELECT COUNT(*)\n"
        + "FROM \"foodmart\".\"sales_fact_1997\"\n"
        + "WHERE \"product_id\" = \"product\".\"product_id\")";
    sql(query).withConfig(c -> c.withExpand(false)).ok(expected);
  }

  @Test void testNotExistsWithExpand() {
    String query = "select \"product_name\" from \"product\" a "
        + "where not exists (select count(*) "
        + "from \"sales_fact_1997\"b "
        + "where b.\"product_id\" = a.\"product_id\")";
    String expected = "SELECT \"product_name\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE NOT EXISTS (SELECT COUNT(*)\n"
        + "FROM \"foodmart\".\"sales_fact_1997\"\n"
        + "WHERE \"product_id\" = \"product\".\"product_id\")";
    sql(query).withConfig(c -> c.withExpand(false)).ok(expected);
  }

  @Test void testSubQueryInWithExpand() {
    String query = "select \"product_name\" from \"product\" a "
        + "where \"product_id\" in (select \"product_id\" "
        + "from \"sales_fact_1997\"b "
        + "where b.\"product_id\" = a.\"product_id\")";
    String expected = "SELECT \"product_name\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE \"product_id\" IN (SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"sales_fact_1997\"\n"
        + "WHERE \"product_id\" = \"product\".\"product_id\")";
    sql(query).withConfig(c -> c.withExpand(false)).ok(expected);
  }

  @Test void testSubQueryInWithExpand2() {
    String query = "select \"product_name\" from \"product\" a "
        + "where \"product_id\" in (1, 2)";
    String expected = "SELECT \"product_name\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE \"product_id\" = 1 OR \"product_id\" = 2";
    sql(query).withConfig(c -> c.withExpand(false)).ok(expected);
  }

  @Test void testSubQueryNotInWithExpand() {
    String query = "select \"product_name\" from \"product\" a "
        + "where \"product_id\" not in (select \"product_id\" "
        + "from \"sales_fact_1997\"b "
        + "where b.\"product_id\" = a.\"product_id\")";
    String expected = "SELECT \"product_name\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE \"product_id\" NOT IN (SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"sales_fact_1997\"\n"
        + "WHERE \"product_id\" = \"product\".\"product_id\")";
    sql(query).withConfig(c -> c.withExpand(false)).ok(expected);
  }

  @Test void testLike() {
    String query = "select \"product_name\" from \"product\" a "
        + "where \"product_name\" like 'abc'";
    String expected = "SELECT \"product_name\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE \"product_name\" LIKE 'abc'";
    sql(query).ok(expected);
  }

  @Test void testNotLike() {
    String query = "select \"product_name\" from \"product\" a "
        + "where \"product_name\" not like 'abc'";
    String expected = "SELECT \"product_name\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE \"product_name\" NOT LIKE 'abc'";
    sql(query).ok(expected);
  }

  @Test void testMatchRecognizePatternExpression() {
    String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "    partition by \"product_class_id\", \"brand_name\"\n"
        + "    order by \"product_class_id\" asc, \"brand_name\" desc\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > prev(up.\"net_weight\")\n"
        + "  ) mr";
    String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") MATCH_RECOGNIZE(\n"
        + "PARTITION BY \"product_class_id\", \"brand_name\"\n"
        + "ORDER BY \"product_class_id\", \"brand_name\" DESC\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "PREV(\"UP\".\"net_weight\", 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizePatternExpression2() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "    pattern (strt down+ up+$)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > prev(up.\"net_weight\")\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") MATCH_RECOGNIZE(\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" + $)\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "PREV(\"UP\".\"net_weight\", 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizePatternExpression3() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "    pattern (^strt down+ up+)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > prev(up.\"net_weight\")\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") MATCH_RECOGNIZE(\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (^ \"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "PREV(\"UP\".\"net_weight\", 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizePatternExpression4() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "    pattern (^strt down+ up+$)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > prev(up.\"net_weight\")\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") MATCH_RECOGNIZE(\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (^ \"STRT\" \"DOWN\" + \"UP\" + $)\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "PREV(\"UP\".\"net_weight\", 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizePatternExpression5() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "    pattern (strt down* up?)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > prev(up.\"net_weight\")\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") MATCH_RECOGNIZE(\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" \"DOWN\" * \"UP\" ?)\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "PREV(\"UP\".\"net_weight\", 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizePatternExpression6() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "    pattern (strt {-down-} up?)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > prev(up.\"net_weight\")\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") MATCH_RECOGNIZE(\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" {- \"DOWN\" -} \"UP\" ?)\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "PREV(\"UP\".\"net_weight\", 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizePatternExpression7() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "    pattern (strt down{2} up{3,})\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > prev(up.\"net_weight\")\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") MATCH_RECOGNIZE(\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" \"DOWN\" { 2 } \"UP\" { 3, })\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "PREV(\"UP\".\"net_weight\", 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizePatternExpression8() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "    pattern (strt down{,2} up{3,5})\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > prev(up.\"net_weight\")\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") MATCH_RECOGNIZE(\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" \"DOWN\" { , 2 } \"UP\" { 3, 5 })\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "PREV(\"UP\".\"net_weight\", 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizePatternExpression9() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "    pattern (strt {-down+-} {-up*-})\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > prev(up.\"net_weight\")\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") MATCH_RECOGNIZE(\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" {- \"DOWN\" + -} {- \"UP\" * -})\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "PREV(\"UP\".\"net_weight\", 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizePatternExpression10() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "    pattern (A B C | A C B | B A C | B C A | C A B | C B A)\n"
        + "    define\n"
        + "      A as A.\"net_weight\" < PREV(A.\"net_weight\"),\n"
        + "      B as B.\"net_weight\" > PREV(B.\"net_weight\"),\n"
        + "      C as C.\"net_weight\" < PREV(C.\"net_weight\")\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") MATCH_RECOGNIZE(\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN "
        + "(\"A\" \"B\" \"C\" | \"A\" \"C\" \"B\" | \"B\" \"A\" \"C\" "
        + "| \"B\" \"C\" \"A\" | \"C\" \"A\" \"B\" | \"C\" \"B\" \"A\")\n"
        + "DEFINE "
        + "\"A\" AS PREV(\"A\".\"net_weight\", 0) < PREV(\"A\".\"net_weight\", 1), "
        + "\"B\" AS PREV(\"B\".\"net_weight\", 0) > PREV(\"B\".\"net_weight\", 1), "
        + "\"C\" AS PREV(\"C\".\"net_weight\", 0) < PREV(\"C\".\"net_weight\", 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizePatternExpression11() {
    final String sql = "select *\n"
        + "  from (select * from \"product\") match_recognize\n"
        + "  (\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > prev(up.\"net_weight\")\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") MATCH_RECOGNIZE(\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "PREV(\"UP\".\"net_weight\", 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizePatternExpression12() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > prev(up.\"net_weight\")\n"
        + "  ) mr order by MR.\"net_weight\"";
    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") MATCH_RECOGNIZE(\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "PREV(\"UP\".\"net_weight\", 1))\n"
        + "ORDER BY \"net_weight\"";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizePatternExpression13() {
    final String sql = "select *\n"
        + "  from (\n"
        + "select *\n"
        + "from \"sales_fact_1997\" as s\n"
        + "join \"customer\" as c\n"
        + "  on s.\"customer_id\" = c.\"customer_id\"\n"
        + "join \"product\" as p\n"
        + "  on s.\"product_id\" = p.\"product_id\"\n"
        + "join \"product_class\" as pc\n"
        + "  on p.\"product_class_id\" = pc.\"product_class_id\"\n"
        + "where c.\"city\" = 'San Francisco'\n"
        + "and pc.\"product_department\" = 'Snacks'"
        + ") match_recognize\n"
        + "  (\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > prev(up.\"net_weight\")\n"
        + "  ) mr order by MR.\"net_weight\"";
    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"sales_fact_1997\"\n"
        + "INNER JOIN \"foodmart\".\"customer\" "
        + "ON \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\"\n"
        + "INNER JOIN \"foodmart\".\"product\" "
        + "ON \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
        + "INNER JOIN \"foodmart\".\"product_class\" "
        + "ON \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
        + "WHERE \"customer\".\"city\" = 'San Francisco' "
        + "AND \"product_class\".\"product_department\" = 'Snacks') "
        + "MATCH_RECOGNIZE(\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "PREV(\"UP\".\"net_weight\", 1))\n"
        + "ORDER BY \"net_weight\"";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeDefineClause() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > NEXT(up.\"net_weight\")\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") MATCH_RECOGNIZE(\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "NEXT(PREV(\"UP\".\"net_weight\", 0), 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeDefineClause2() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < FIRST(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > LAST(up.\"net_weight\")\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") MATCH_RECOGNIZE(\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "FIRST(\"DOWN\".\"net_weight\", 0), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "LAST(\"UP\".\"net_weight\", 0))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeDefineClause3() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\",1),\n"
        + "      up as up.\"net_weight\" > LAST(up.\"net_weight\" + up.\"gross_weight\")\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") MATCH_RECOGNIZE(\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "LAST(\"UP\".\"net_weight\", 0) + LAST(\"UP\".\"gross_weight\", 0))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeDefineClause4() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\",1),\n"
        + "      up as up.\"net_weight\" > "
        + "PREV(LAST(up.\"net_weight\" + up.\"gross_weight\"),3)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") MATCH_RECOGNIZE(\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "PREV(LAST(\"UP\".\"net_weight\", 0) + "
        + "LAST(\"UP\".\"gross_weight\", 0), 3))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeMeasures1() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "   measures MATCH_NUMBER() as match_num, "
        + "   CLASSIFIER() as var_match, "
        + "   STRT.\"net_weight\" as start_nw,"
        + "   LAST(DOWN.\"net_weight\") as bottom_nw,"
        + "   LAST(up.\"net_weight\") as end_nw"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > prev(up.\"net_weight\")\n"
        + "  ) mr";

    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") "
        + "MATCH_RECOGNIZE(\n"
        + "MEASURES "
        + "FINAL MATCH_NUMBER () AS \"MATCH_NUM\", "
        + "FINAL CLASSIFIER() AS \"VAR_MATCH\", "
        + "FINAL \"STRT\".\"net_weight\" AS \"START_NW\", "
        + "FINAL LAST(\"DOWN\".\"net_weight\", 0) AS \"BOTTOM_NW\", "
        + "FINAL LAST(\"UP\".\"net_weight\", 0) AS \"END_NW\"\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "PREV(\"UP\".\"net_weight\", 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeMeasures2() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "   measures STRT.\"net_weight\" as start_nw,"
        + "   FINAL LAST(DOWN.\"net_weight\") as bottom_nw,"
        + "   LAST(up.\"net_weight\") as end_nw"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > prev(up.\"net_weight\")\n"
        + "  ) mr";

    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") "
        + "MATCH_RECOGNIZE(\n"
        + "MEASURES "
        + "FINAL \"STRT\".\"net_weight\" AS \"START_NW\", "
        + "FINAL LAST(\"DOWN\".\"net_weight\", 0) AS \"BOTTOM_NW\", "
        + "FINAL LAST(\"UP\".\"net_weight\", 0) AS \"END_NW\"\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "PREV(\"UP\".\"net_weight\", 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeMeasures3() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "   measures STRT.\"net_weight\" as start_nw,"
        + "   RUNNING LAST(DOWN.\"net_weight\") as bottom_nw,"
        + "   LAST(up.\"net_weight\") as end_nw"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > prev(up.\"net_weight\")\n"
        + "  ) mr";

    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") "
        + "MATCH_RECOGNIZE(\n"
        + "MEASURES "
        + "FINAL \"STRT\".\"net_weight\" AS \"START_NW\", "
        + "FINAL (RUNNING LAST(\"DOWN\".\"net_weight\", 0)) AS \"BOTTOM_NW\", "
        + "FINAL LAST(\"UP\".\"net_weight\", 0) AS \"END_NW\"\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "PREV(\"UP\".\"net_weight\", 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeMeasures4() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "   measures STRT.\"net_weight\" as start_nw,"
        + "   FINAL COUNT(up.\"net_weight\") as up_cnt,"
        + "   FINAL COUNT(\"net_weight\") as down_cnt,"
        + "   RUNNING COUNT(\"net_weight\") as running_cnt"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > prev(up.\"net_weight\")\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") "
        + "MATCH_RECOGNIZE(\n"
        + "MEASURES "
        + "FINAL \"STRT\".\"net_weight\" AS \"START_NW\", "
        + "FINAL COUNT(\"UP\".\"net_weight\") AS \"UP_CNT\", "
        + "FINAL COUNT(\"*\".\"net_weight\") AS \"DOWN_CNT\", "
        + "FINAL (RUNNING COUNT(\"*\".\"net_weight\")) AS \"RUNNING_CNT\"\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "PREV(\"UP\".\"net_weight\", 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeMeasures5() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "   measures "
        + "   FIRST(STRT.\"net_weight\") as start_nw,"
        + "   LAST(UP.\"net_weight\") as up_cnt,"
        + "   AVG(DOWN.\"net_weight\") as down_cnt"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > prev(up.\"net_weight\")\n"
        + "  ) mr";

    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") "
        + "MATCH_RECOGNIZE(\n"
        + "MEASURES "
        + "FINAL FIRST(\"STRT\".\"net_weight\", 0) AS \"START_NW\", "
        + "FINAL LAST(\"UP\".\"net_weight\", 0) AS \"UP_CNT\", "
        + "FINAL (SUM(\"DOWN\".\"net_weight\") / "
        + "COUNT(\"DOWN\".\"net_weight\")) AS \"DOWN_CNT\"\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "PREV(\"UP\".\"net_weight\", 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeMeasures6() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "   measures "
        + "   FIRST(STRT.\"net_weight\") as start_nw,"
        + "   LAST(DOWN.\"net_weight\") as up_cnt,"
        + "   FINAL SUM(DOWN.\"net_weight\") as down_cnt"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > prev(up.\"net_weight\")\n"
        + "  ) mr";

    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") MATCH_RECOGNIZE(\n"
        + "MEASURES "
        + "FINAL FIRST(\"STRT\".\"net_weight\", 0) AS \"START_NW\", "
        + "FINAL LAST(\"DOWN\".\"net_weight\", 0) AS \"UP_CNT\", "
        + "FINAL SUM(\"DOWN\".\"net_weight\") AS \"DOWN_CNT\"\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN "
        + "(\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "PREV(\"UP\".\"net_weight\", 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeMeasures7() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "   measures "
        + "   FIRST(STRT.\"net_weight\") as start_nw,"
        + "   LAST(DOWN.\"net_weight\") as up_cnt,"
        + "   FINAL SUM(DOWN.\"net_weight\") as down_cnt"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > prev(up.\"net_weight\")\n"
        + "  ) mr order by start_nw, up_cnt";

    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") MATCH_RECOGNIZE(\n"
        + "MEASURES "
        + "FINAL FIRST(\"STRT\".\"net_weight\", 0) AS \"START_NW\", "
        + "FINAL LAST(\"DOWN\".\"net_weight\", 0) AS \"UP_CNT\", "
        + "FINAL SUM(\"DOWN\".\"net_weight\") AS \"DOWN_CNT\"\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN "
        + "(\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "PREV(\"UP\".\"net_weight\", 1))\n"
        + "ORDER BY \"START_NW\", \"UP_CNT\"";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizePatternSkip1() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "    after match skip to next row\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > NEXT(up.\"net_weight\")\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") MATCH_RECOGNIZE(\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "NEXT(PREV(\"UP\".\"net_weight\", 0), 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizePatternSkip2() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "    after match skip past last row\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > NEXT(up.\"net_weight\")\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") MATCH_RECOGNIZE(\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP PAST LAST ROW\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "NEXT(PREV(\"UP\".\"net_weight\", 0), 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizePatternSkip3() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "    after match skip to FIRST down\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > NEXT(up.\"net_weight\")\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") MATCH_RECOGNIZE(\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO FIRST \"DOWN\"\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "DEFINE \"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "NEXT(PREV(\"UP\".\"net_weight\", 0), 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizePatternSkip4() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "    after match skip to last down\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > NEXT(up.\"net_weight\")\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") MATCH_RECOGNIZE(\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO LAST \"DOWN\"\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "NEXT(PREV(\"UP\".\"net_weight\", 0), 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizePatternSkip5() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "    after match skip to down\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > NEXT(up.\"net_weight\")\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") MATCH_RECOGNIZE(\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO LAST \"DOWN\"\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "NEXT(PREV(\"UP\".\"net_weight\", 0), 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeSubset1() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "    after match skip to down\n"
        + "    pattern (strt down+ up+)\n"
        + "    subset stdn = (strt, down)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > NEXT(up.\"net_weight\")\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") MATCH_RECOGNIZE(\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO LAST \"DOWN\"\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "SUBSET \"STDN\" = (\"DOWN\", \"STRT\")\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "NEXT(PREV(\"UP\".\"net_weight\", 0), 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeSubset2() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "   measures STRT.\"net_weight\" as start_nw,"
        + "   LAST(DOWN.\"net_weight\") as bottom_nw,"
        + "   AVG(STDN.\"net_weight\") as avg_stdn"
        + "    pattern (strt down+ up+)\n"
        + "    subset stdn = (strt, down)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > prev(up.\"net_weight\")\n"
        + "  ) mr";

    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") "
        + "MATCH_RECOGNIZE(\n"
        + "MEASURES "
        + "FINAL \"STRT\".\"net_weight\" AS \"START_NW\", "
        + "FINAL LAST(\"DOWN\".\"net_weight\", 0) AS \"BOTTOM_NW\", "
        + "FINAL (SUM(\"STDN\".\"net_weight\") / "
        + "COUNT(\"STDN\".\"net_weight\")) AS \"AVG_STDN\"\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "SUBSET \"STDN\" = (\"DOWN\", \"STRT\")\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "PREV(\"UP\".\"net_weight\", 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeSubset3() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "   measures STRT.\"net_weight\" as start_nw,"
        + "   LAST(DOWN.\"net_weight\") as bottom_nw,"
        + "   SUM(STDN.\"net_weight\") as avg_stdn"
        + "    pattern (strt down+ up+)\n"
        + "    subset stdn = (strt, down)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > prev(up.\"net_weight\")\n"
        + "  ) mr";

    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") "
        + "MATCH_RECOGNIZE(\n"
        + "MEASURES "
        + "FINAL \"STRT\".\"net_weight\" AS \"START_NW\", "
        + "FINAL LAST(\"DOWN\".\"net_weight\", 0) AS \"BOTTOM_NW\", "
        + "FINAL SUM(\"STDN\".\"net_weight\") AS \"AVG_STDN\"\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "SUBSET \"STDN\" = (\"DOWN\", \"STRT\")\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "PREV(\"UP\".\"net_weight\", 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeSubset4() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "   measures STRT.\"net_weight\" as start_nw,"
        + "   LAST(DOWN.\"net_weight\") as bottom_nw,"
        + "   SUM(STDN.\"net_weight\") as avg_stdn"
        + "    pattern (strt down+ up+)\n"
        + "    subset stdn = (strt, down), stdn2 = (strt, down)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > prev(up.\"net_weight\")\n"
        + "  ) mr";

    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") "
        + "MATCH_RECOGNIZE(\n"
        + "MEASURES "
        + "FINAL \"STRT\".\"net_weight\" AS \"START_NW\", "
        + "FINAL LAST(\"DOWN\".\"net_weight\", 0) AS \"BOTTOM_NW\", "
        + "FINAL SUM(\"STDN\".\"net_weight\") AS \"AVG_STDN\"\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "SUBSET \"STDN\" = (\"DOWN\", \"STRT\"), \"STDN2\" = (\"DOWN\", \"STRT\")\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "PREV(\"UP\".\"net_weight\", 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeRowsPerMatch1() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "   measures STRT.\"net_weight\" as start_nw,"
        + "   LAST(DOWN.\"net_weight\") as bottom_nw,"
        + "   SUM(STDN.\"net_weight\") as avg_stdn"
        + "    ONE ROW PER MATCH\n"
        + "    pattern (strt down+ up+)\n"
        + "    subset stdn = (strt, down), stdn2 = (strt, down)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > prev(up.\"net_weight\")\n"
        + "  ) mr";

    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") "
        + "MATCH_RECOGNIZE(\n"
        + "MEASURES "
        + "FINAL \"STRT\".\"net_weight\" AS \"START_NW\", "
        + "FINAL LAST(\"DOWN\".\"net_weight\", 0) AS \"BOTTOM_NW\", "
        + "FINAL SUM(\"STDN\".\"net_weight\") AS \"AVG_STDN\"\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "SUBSET \"STDN\" = (\"DOWN\", \"STRT\"), \"STDN2\" = (\"DOWN\", \"STRT\")\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "PREV(\"UP\".\"net_weight\", 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeRowsPerMatch2() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "   measures STRT.\"net_weight\" as start_nw,"
        + "   LAST(DOWN.\"net_weight\") as bottom_nw,"
        + "   SUM(STDN.\"net_weight\") as avg_stdn"
        + "    ALL ROWS PER MATCH\n"
        + "    pattern (strt down+ up+)\n"
        + "    subset stdn = (strt, down), stdn2 = (strt, down)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" < PREV(down.\"net_weight\"),\n"
        + "      up as up.\"net_weight\" > prev(up.\"net_weight\")\n"
        + "  ) mr";

    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") "
        + "MATCH_RECOGNIZE(\n"
        + "MEASURES "
        + "RUNNING \"STRT\".\"net_weight\" AS \"START_NW\", "
        + "RUNNING LAST(\"DOWN\".\"net_weight\", 0) AS \"BOTTOM_NW\", "
        + "RUNNING SUM(\"STDN\".\"net_weight\") AS \"AVG_STDN\"\n"
        + "ALL ROWS PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "SUBSET \"STDN\" = (\"DOWN\", \"STRT\"), \"STDN2\" = (\"DOWN\", \"STRT\")\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) < "
        + "PREV(\"DOWN\".\"net_weight\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "PREV(\"UP\".\"net_weight\", 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeWithin() {
    final String sql = "select *\n"
        + "  from \"employee\" match_recognize\n"
        + "  (\n"
        + "   order by \"hire_date\"\n"
        + "   ALL ROWS PER MATCH\n"
        + "   pattern (strt down+ up+) within interval '3:12:22.123' hour to second\n"
        + "   define\n"
        + "     down as down.\"salary\" < PREV(down.\"salary\"),\n"
        + "     up as up.\"salary\" > prev(up.\"salary\")\n"
        + "  ) mr";

    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"employee\") "
        + "MATCH_RECOGNIZE(\n"
        + "ORDER BY \"hire_date\"\n"
        + "ALL ROWS PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" +) WITHIN INTERVAL '3:12:22.123' HOUR TO SECOND\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"salary\", 0) < "
        + "PREV(\"DOWN\".\"salary\", 1), "
        + "\"UP\" AS PREV(\"UP\".\"salary\", 0) > "
        + "PREV(\"UP\".\"salary\", 1))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeIn() {
    final String sql = "select *\n"
        + "  from \"product\" match_recognize\n"
        + "  (\n"
        + "    partition by \"product_class_id\", \"brand_name\"\n"
        + "    order by \"product_class_id\" asc, \"brand_name\" desc\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.\"net_weight\" in (0, 1),\n"
        + "      up as up.\"net_weight\" > prev(up.\"net_weight\")\n"
        + "  ) mr";

    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"product\") MATCH_RECOGNIZE(\n"
        + "PARTITION BY \"product_class_id\", \"brand_name\"\n"
        + "ORDER BY \"product_class_id\", \"brand_name\" DESC\n"
        + "ONE ROW PER MATCH\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (\"STRT\" \"DOWN\" + \"UP\" +)\n"
        + "DEFINE "
        + "\"DOWN\" AS PREV(\"DOWN\".\"net_weight\", 0) = "
        + "0 OR PREV(\"DOWN\".\"net_weight\", 0) = 1, "
        + "\"UP\" AS PREV(\"UP\".\"net_weight\", 0) > "
        + "PREV(\"UP\".\"net_weight\", 1))";
    sql(sql).ok(expected);
  }

  @Test void testValues() {
    final String sql = "select \"a\"\n"
        + "from (values (1, 'x'), (2, 'yy')) as t(\"a\", \"b\")";
    final String expectedHsqldb = "SELECT a\n"
        + "FROM (VALUES (1, 'x '),\n"
        + "(2, 'yy')) AS t (a, b)";
    final String expectedMysql = "SELECT `a`\n"
        + "FROM (SELECT 1 AS `a`, 'x ' AS `b`\n"
        + "UNION ALL\n"
        + "SELECT 2 AS `a`, 'yy' AS `b`) AS `t`";
    final String expectedPostgresql = "SELECT \"a\"\n"
        + "FROM (VALUES (1, 'x '),\n"
        + "(2, 'yy')) AS \"t\" (\"a\", \"b\")";
    final String expectedOracle = "SELECT \"a\"\n"
        + "FROM (SELECT 1 \"a\", 'x ' \"b\"\n"
        + "FROM \"DUAL\"\n"
        + "UNION ALL\n"
        + "SELECT 2 \"a\", 'yy' \"b\"\n"
        + "FROM \"DUAL\")";
    final String expectedHive = "SELECT a\n"
        + "FROM (SELECT 1 a, 'x ' b\n"
        + "UNION ALL\n"
        + "SELECT 2 a, 'yy' b)";
    final String expectedSpark = "SELECT a\n"
        + "FROM (SELECT 1 a, 'x ' b\n"
        + "UNION ALL\n"
        + "SELECT 2 a, 'yy' b)";
    final String expectedBigQuery = "SELECT a\n"
        + "FROM (SELECT 1 AS a, 'x ' AS b\n"
        + "UNION ALL\n"
        + "SELECT 2 AS a, 'yy' AS b)";
    final String expectedSnowflake = "SELECT \"a\"\n"
        + "FROM (SELECT 1 AS \"a\", 'x ' AS \"b\"\n"
        + "UNION ALL\n"
        + "SELECT 2 AS \"a\", 'yy' AS \"b\")";
    final String expectedRedshift = expectedPostgresql;
    sql(sql)
        .withHsqldb()
        .ok(expectedHsqldb)
        .withMysql()
        .ok(expectedMysql)
        .withPostgresql()
        .ok(expectedPostgresql)
        .withOracle()
        .ok(expectedOracle)
        .withHive()
        .ok(expectedHive)
        .withSpark()
        .ok(expectedSpark)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowflake)
        .withRedshift()
        .ok(expectedRedshift);
  }

  @Test void testValuesEmpty() {
    final String sql = "select *\n"
        + "from (values (1, 'a'), (2, 'bb')) as t(x, y)\n"
        + "limit 0";
    final RuleSet rules =
        RuleSets.ofList(PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE);
    final String expectedMysql = "SELECT *\n"
        + "FROM (SELECT NULL AS `X`, NULL AS `Y`) AS `t`\n"
        + "WHERE 1 = 0";
    final String expectedOracle = "SELECT NULL \"X\", NULL \"Y\"\n"
        + "FROM \"DUAL\"\n"
        + "WHERE 1 = 0";
    final String expectedPostgresql = "SELECT *\n"
        + "FROM (VALUES (NULL, NULL)) AS \"t\" (\"X\", \"Y\")\n"
        + "WHERE 1 = 0";
    sql(sql)
        .optimize(rules, null)
        .withMysql()
        .ok(expectedMysql)
        .withOracle()
        .ok(expectedOracle)
        .withPostgresql()
        .ok(expectedPostgresql);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3840">[CALCITE-3840]
   * Re-aliasing of VALUES that has column aliases produces wrong SQL in the
   * JDBC adapter</a>. */
  @Test void testValuesReAlias() {
    final RelBuilder builder = relBuilder();
    final RelNode root = builder
        .values(new String[]{ "a", "b" }, 1, "x ", 2, "yy")
        .values(new String[]{ "a", "b" }, 1, "x ", 2, "yy")
        .join(JoinRelType.FULL)
        .project(builder.field("a"))
        .build();
    final String expectedSql = "SELECT \"t\".\"a\"\n"
        + "FROM (VALUES (1, 'x '),\n"
        + "(2, 'yy')) AS \"t\" (\"a\", \"b\")\n"
        + "FULL JOIN (VALUES (1, 'x '),\n"
        + "(2, 'yy')) AS \"t0\" (\"a\", \"b\") ON TRUE";
    assertThat(toSql(root), isLinux(expectedSql));

    // Now with indentation.
    final String expectedSql2 = "SELECT \"t\".\"a\"\n"
        + "FROM (VALUES (1, 'x '),\n"
        + "        (2, 'yy')) AS \"t\" (\"a\", \"b\")\n"
        + "  FULL JOIN (VALUES (1, 'x '),\n"
        + "        (2, 'yy')) AS \"t0\" (\"a\", \"b\") ON TRUE";
    assertThat(
        toSql(root, DatabaseProduct.CALCITE.getDialect(),
            c -> c.withIndentation(2)),
        isLinux(expectedSql2));
  }

  @Test void testSelectWithoutFromEmulationForHiveAndBigQuery() {
    String query = "select 2 + 2";
    final String expected = "SELECT 2 + 2";
    sql(query)
        .withHive().ok(expected)
        .withBigQuery().ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2118">[CALCITE-2118]
   * RelToSqlConverter should only generate "*" if field names match</a>. */
  @Test void testPreserveAlias() {
    final String sql = "select \"warehouse_class_id\" as \"id\",\n"
        + " \"description\"\n"
        + "from \"warehouse_class\"";
    final String expected = ""
        + "SELECT \"warehouse_class_id\" AS \"id\", \"description\"\n"
        + "FROM \"foodmart\".\"warehouse_class\"";
    sql(sql).ok(expected);

    final String sql2 = "select \"warehouse_class_id\", \"description\"\n"
        + "from \"warehouse_class\"";
    final String expected2 = "SELECT *\n"
        + "FROM \"foodmart\".\"warehouse_class\"";
    sql(sql2).ok(expected2);
  }

  @Test void testPreservePermutation() {
    final String sql = "select \"description\", \"warehouse_class_id\"\n"
        + "from \"warehouse_class\"";
    final String expected = "SELECT \"description\", \"warehouse_class_id\"\n"
        + "FROM \"foodmart\".\"warehouse_class\"";
    sql(sql).ok(expected);
  }

  @Test void testFieldNamesWithAggregateSubQuery() {
    final String query = "select mytable.\"city\",\n"
        + "  sum(mytable.\"store_sales\") as \"my-alias\"\n"
        + "from (select c.\"city\", s.\"store_sales\"\n"
        + "  from \"sales_fact_1997\" as s\n"
        + "    join \"customer\" as c using (\"customer_id\")\n"
        + "  group by c.\"city\", s.\"store_sales\") AS mytable\n"
        + "group by mytable.\"city\"";

    final String expected = "SELECT \"t0\".\"city\","
        + " SUM(\"t0\".\"store_sales\") AS \"my-alias\"\n"
        + "FROM (SELECT \"customer\".\"city\","
        + " \"sales_fact_1997\".\"store_sales\"\n"
        + "FROM \"foodmart\".\"sales_fact_1997\"\n"
        + "INNER JOIN \"foodmart\".\"customer\""
        + " ON \"sales_fact_1997\".\"customer_id\""
        + " = \"customer\".\"customer_id\"\n"
        + "GROUP BY \"customer\".\"city\","
        + " \"sales_fact_1997\".\"store_sales\") AS \"t0\"\n"
        + "GROUP BY \"t0\".\"city\"";
    sql(query).ok(expected);
  }

  @Test void testUnparseSelectMustUseDialect() {
    final String query = "select * from \"product\"";
    final String expected = "SELECT *\n"
        + "FROM foodmart.product";

    final boolean[] callsUnparseCallOnSqlSelect = {false};
    final SqlDialect dialect = new SqlDialect(SqlDialect.EMPTY_CONTEXT) {
      @Override public void unparseCall(SqlWriter writer, SqlCall call,
          int leftPrec, int rightPrec) {
        if (call instanceof SqlSelect) {
          callsUnparseCallOnSqlSelect[0] = true;
        }
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
    };
    sql(query).dialect(dialect).ok(expected);

    assertThat("Dialect must be able to customize unparseCall() for SqlSelect",
        callsUnparseCallOnSqlSelect[0], is(true));
  }

  @Test void testCorrelate() {
    final String sql = "select d.\"department_id\", d_plusOne "
        + "from \"department\" as d, "
        + "       lateral (select d.\"department_id\" + 1 as d_plusOne"
        + "                from (values(true)))";

    final String expected = "SELECT \"$cor0\".\"department_id\", \"$cor0\".\"D_PLUSONE\"\n"
        + "FROM \"foodmart\".\"department\" AS \"$cor0\",\n"
        + "LATERAL (SELECT \"$cor0\".\"department_id\" + 1 AS \"D_PLUSONE\"\n"
        + "FROM (VALUES (TRUE)) AS \"t\" (\"EXPR$0\")) AS \"t0\"";
    sql(sql).ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3651">[CALCITE-3651]
   * NullPointerException when convert relational algebra that correlates TableFunctionScan</a>. */
  @Test void testLateralCorrelate() {
    final String query = "select * from \"product\",\n"
        + "lateral table(RAMP(\"product\".\"product_id\"))";
    final String expected = "SELECT *\n"
        + "FROM \"foodmart\".\"product\" AS \"$cor0\",\n"
        + "LATERAL (SELECT *\n"
        + "FROM TABLE(RAMP(\"$cor0\".\"product_id\"))) AS \"t\"";
    sql(query).ok(expected);
  }

  @Test void testUncollectExplicitAlias() {
    final String sql = "select did + 1\n"
        + "from unnest(select collect(\"department_id\") as deptid"
        + "            from \"department\") as t(did)";

    final String expected = "SELECT \"DEPTID\" + 1\n"
        + "FROM UNNEST (SELECT COLLECT(\"department_id\") AS \"DEPTID\"\n"
        + "FROM \"foodmart\".\"department\") AS \"t0\" (\"DEPTID\")";
    sql(sql).ok(expected);
  }

  @Test void testUncollectImplicitAlias() {
    final String sql = "select did + 1\n"
        + "from unnest(select collect(\"department_id\") "
        + "            from \"department\") as t(did)";

    final String expected = "SELECT \"col_0\" + 1\n"
        + "FROM UNNEST (SELECT COLLECT(\"department_id\")\n"
        + "FROM \"foodmart\".\"department\") AS \"t0\" (\"col_0\")";
    sql(sql).ok(expected);
  }


  @Test void testWithinGroup1() {
    final String query = "select \"product_class_id\", collect(\"net_weight\") "
        + "within group (order by \"net_weight\" desc) "
        + "from \"product\" group by \"product_class_id\"";
    final String expected = "SELECT \"product_class_id\", COLLECT(\"net_weight\") "
        + "WITHIN GROUP (ORDER BY \"net_weight\" DESC)\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_class_id\"";
    sql(query).ok(expected);
  }

  @Test void testWithinGroup2() {
    final String query = "select \"product_class_id\", collect(\"net_weight\") "
        + "within group (order by \"low_fat\", \"net_weight\" desc nulls last) "
        + "from \"product\" group by \"product_class_id\"";
    final String expected = "SELECT \"product_class_id\", COLLECT(\"net_weight\") "
        + "WITHIN GROUP (ORDER BY \"low_fat\", \"net_weight\" DESC NULLS LAST)\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_class_id\"";
    sql(query).ok(expected);
  }

  @Test void testWithinGroup3() {
    final String query = "select \"product_class_id\", collect(\"net_weight\") "
        + "within group (order by \"net_weight\" desc), "
        + "min(\"low_fat\")"
        + "from \"product\" group by \"product_class_id\"";
    final String expected = "SELECT \"product_class_id\", COLLECT(\"net_weight\") "
        + "WITHIN GROUP (ORDER BY \"net_weight\" DESC), MIN(\"low_fat\")\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_class_id\"";
    sql(query).ok(expected);
  }

  @Test void testWithinGroup4() {
    final String query = "select \"product_class_id\", collect(\"net_weight\") "
        + "within group (order by \"net_weight\" desc) filter (where \"net_weight\" > 0)"
        + "from \"product\" group by \"product_class_id\"";
    final String expected = "SELECT \"product_class_id\", COLLECT(\"net_weight\") "
        + "FILTER (WHERE \"net_weight\" > 0 IS TRUE) "
        + "WITHIN GROUP (ORDER BY \"net_weight\" DESC)\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_class_id\"";
    sql(query).ok(expected);
  }

  @Test void testJsonValueExpressionOperator() {
    String query = "select \"product_name\" format json, "
        + "\"product_name\" format json encoding utf8, "
        + "\"product_name\" format json encoding utf16, "
        + "\"product_name\" format json encoding utf32 from \"product\"";
    final String expected = "SELECT \"product_name\" FORMAT JSON, "
        + "\"product_name\" FORMAT JSON, "
        + "\"product_name\" FORMAT JSON, "
        + "\"product_name\" FORMAT JSON\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query).ok(expected);
  }

  @Test void testJsonExists() {
    String query = "select json_exists(\"product_name\", 'lax $') from \"product\"";
    final String expected = "SELECT JSON_EXISTS(\"product_name\", 'lax $')\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query).ok(expected);
  }

  @Test void testJsonPretty() {
    String query = "select json_pretty(\"product_name\") from \"product\"";
    final String expected = "SELECT JSON_PRETTY(\"product_name\")\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query).ok(expected);
  }

  @Test void testJsonValue() {
    String query = "select json_value(\"product_name\", 'lax $') from \"product\"";
    final String expected = "SELECT JSON_VALUE(\"product_name\", 'lax $')\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query).ok(expected);
  }

  @Test void testJsonQuery() {
    String query = "select json_query(\"product_name\", 'lax $') from \"product\"";
    final String expected = "SELECT JSON_QUERY(\"product_name\", 'lax $' "
        + "WITHOUT ARRAY WRAPPER NULL ON EMPTY NULL ON ERROR)\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query).ok(expected);
  }

  @Test void testJsonArray() {
    String query = "select json_array(\"product_name\", \"product_name\") from \"product\"";
    final String expected = "SELECT JSON_ARRAY(\"product_name\", \"product_name\" ABSENT ON NULL)\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query).ok(expected);
  }

  @Test void testJsonArrayAgg() {
    String query = "select json_arrayagg(\"product_name\") from \"product\"";
    final String expected = "SELECT JSON_ARRAYAGG(\"product_name\" ABSENT ON NULL)\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query).ok(expected);
  }

  @Test void testJsonObject() {
    String query = "select json_object(\"product_name\": \"product_id\") from \"product\"";
    final String expected = "SELECT "
        + "JSON_OBJECT(KEY \"product_name\" VALUE \"product_id\" NULL ON NULL)\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query).ok(expected);
  }

  @Test void testJsonObjectAgg() {
    String query = "select json_objectagg(\"product_name\": \"product_id\") from \"product\"";
    final String expected = "SELECT "
        + "JSON_OBJECTAGG(KEY \"product_name\" VALUE \"product_id\" NULL ON NULL)\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query).ok(expected);
  }

  @Test void testJsonPredicate() {
    String query = "select "
        + "\"product_name\" is json, "
        + "\"product_name\" is json value, "
        + "\"product_name\" is json object, "
        + "\"product_name\" is json array, "
        + "\"product_name\" is json scalar, "
        + "\"product_name\" is not json, "
        + "\"product_name\" is not json value, "
        + "\"product_name\" is not json object, "
        + "\"product_name\" is not json array, "
        + "\"product_name\" is not json scalar "
        + "from \"product\"";
    final String expected = "SELECT "
        + "\"product_name\" IS JSON VALUE, "
        + "\"product_name\" IS JSON VALUE, "
        + "\"product_name\" IS JSON OBJECT, "
        + "\"product_name\" IS JSON ARRAY, "
        + "\"product_name\" IS JSON SCALAR, "
        + "\"product_name\" IS NOT JSON VALUE, "
        + "\"product_name\" IS NOT JSON VALUE, "
        + "\"product_name\" IS NOT JSON OBJECT, "
        + "\"product_name\" IS NOT JSON ARRAY, "
        + "\"product_name\" IS NOT JSON SCALAR\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query).ok(expected);
  }

  @Test void testCrossJoinEmulationForSpark() {
    String query = "select * from \"employee\", \"department\"";
    final String expected = "SELECT *\n"
        + "FROM foodmart.employee\n"
        + "CROSS JOIN foodmart.department";
    sql(query).withSpark().ok(expected);
  }

  @Test void testCrossJoinEmulationForBigQuery() {
    String query = "select * from \"employee\", \"department\"";
    final String expected = "SELECT *\n"
        + "FROM foodmart.employee\n"
        + "CROSS JOIN foodmart.department";
    sql(query).withBigQuery().ok(expected);
  }

  @Test void testSubstringInSpark() {
    final String query = "select substring(\"brand_name\" from 2) "
        + "from \"product\"\n";
    final String expected = "SELECT SUBSTRING(brand_name, 2)\n"
        + "FROM foodmart.product";
    sql(query).withSpark().ok(expected);
  }

  @Test void testSubstringWithForInSpark() {
    final String query = "select substring(\"brand_name\" from 2 for 3) "
        + "from \"product\"\n";
    final String expected = "SELECT SUBSTRING(brand_name, 2, 3)\n"
        + "FROM foodmart.product";
    sql(query).withSpark().ok(expected);
  }

  @Test void testFloorInSpark() {
    final String query = "select floor(\"hire_date\" TO MINUTE) "
        + "from \"employee\"";
    final String expected = "SELECT DATE_TRUNC('MINUTE', hire_date)\n"
        + "FROM foodmart.employee";
    sql(query).withSpark().ok(expected);
  }

  @Test void testNumericFloorInSpark() {
    final String query = "select floor(\"salary\") "
        + "from \"employee\"";
    final String expected = "SELECT FLOOR(salary)\n"
        + "FROM foodmart.employee";
    sql(query).withSpark().ok(expected);
  }

  @Test void testJsonStorageSize() {
    String query = "select json_storage_size(\"product_name\") from \"product\"";
    final String expected = "SELECT JSON_STORAGE_SIZE(\"product_name\")\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query).ok(expected);
  }

  @Test void testCubeWithGroupBy() {
    final String query = "select count(*) "
        + "from \"foodmart\".\"product\" "
        + "group by cube(\"product_id\",\"product_class_id\")";
    final String expected = "SELECT COUNT(*)\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY CUBE(\"product_id\", \"product_class_id\")";
    final String expectedInSpark = "SELECT COUNT(*)\n"
        + "FROM foodmart.product\n"
        + "GROUP BY product_id, product_class_id WITH CUBE";
    final String expectedPresto = "SELECT COUNT(*)\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY CUBE(\"product_id\", \"product_class_id\")";
    sql(query)
        .ok(expected)
        .withSpark()
        .ok(expectedInSpark)
        .withPresto()
        .ok(expectedPresto);
  }

  @Test void testRollupWithGroupBy() {
    final String query = "select count(*) "
        + "from \"foodmart\".\"product\" "
        + "group by rollup(\"product_id\",\"product_class_id\")";
    final String expected = "SELECT COUNT(*)\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY ROLLUP(\"product_id\", \"product_class_id\")";
    final String expectedInSpark = "SELECT COUNT(*)\n"
        + "FROM foodmart.product\n"
        + "GROUP BY product_id, product_class_id WITH ROLLUP";
    final String expectedPresto = "SELECT COUNT(*)\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY ROLLUP(\"product_id\", \"product_class_id\")";
    sql(query)
        .ok(expected)
        .withSpark()
        .ok(expectedInSpark)
        .withPresto()
        .ok(expectedPresto);
  }

  @Test public void testCastInStringOperandOfComparison() {
    final String query = "select \"employee_id\" "
        + "from \"foodmart\".\"employee\" "
        + "where 10 = cast('10' as int) and \"birth_date\" = cast('1914-02-02' as date) or "
        + "\"hire_date\" = cast('1996-01-01 '||'00:00:00' as timestamp)";
    final String expected = "SELECT \"employee_id\"\n"
        + "FROM \"foodmart\".\"employee\"\n"
        + "WHERE 10 = '10' AND \"birth_date\" = '1914-02-02' OR \"hire_date\" = '1996-01-01 ' || "
        + "'00:00:00'";
    final String expectedBiqquery = "SELECT employee_id\n"
        + "FROM foodmart.employee\n"
        + "WHERE 10 = CAST('10' AS INT64) AND birth_date = '1914-02-02' OR hire_date = CAST"
        + "(CONCAT('1996-01-01 ', '00:00:00') AS DATETIME)";
    final String mssql = "SELECT [employee_id]\n"
        + "FROM [foodmart].[employee]\n"
        + "WHERE 10 = '10' AND [birth_date] = '1914-02-02' OR [hire_date] = CONCAT('1996-01-01 ', '00:00:00')";
    sql(query)
        .ok(expected)
        .withBigQuery()
        .ok(expectedBiqquery)
        .withMssql()
        .ok(mssql);
  }

  @Test public void testRegexSubstrFunction2Args() {
    final String query = "select regexp_substr('choco chico chipo', '.*cho*p*c*?.*')"
        + "from \"foodmart\".\"product\"";
    final String expected = "SELECT REGEXP_SUBSTR('choco chico chipo', '.*cho*p*c*?.*')\n"
        + "FROM foodmart.product";
    sql(query)
        .withBigQuery()
        .ok(expected);
  }

  @Test public void testRegexSubstrFunction3Args() {
    final String query = "select \"product_id\", regexp_substr('choco chico chipo', "
        + "'.*cho*p*c*?.*', 7)\n"
        + "from \"foodmart\".\"product\" where \"product_id\" = 1";
    final String expected = "SELECT product_id, REGEXP_SUBSTR('choco chico chipo', "
        + "'.*cho*p*c*?.*', 7)\n"
        + "FROM foodmart.product\n"
        + "WHERE product_id = 1";
    sql(query)
        .withBigQuery()
        .ok(expected);
  }

  @Test public void testRegexSubstrFunction4Args() {
    final String query = "select \"product_id\", regexp_substr('chocolate chip cookies', 'c+.{2}',"
        + " 4, 2)\n"
        + "from \"foodmart\".\"product\" where \"product_id\" in (1, 2, 3)";
    final String expected = "SELECT product_id, REGEXP_SUBSTR('chocolate chip "
        + "cookies', 'c+.{2}', 4, 2)\n"
        + "FROM foodmart.product\n"
        + "WHERE product_id = 1 OR product_id = 2 OR product_id = 3";
    sql(query)
        .withBigQuery()
        .ok(expected);
  }

  @Test public void testRegexSubstrFunction5Args() {
    final String query = "select regexp_substr('chocolate Chip cookies', 'c+.{2}',"
        + " 1, 2, 'i')\n"
        + "from \"foodmart\".\"product\" where \"product_id\" in (1, 2, 3, 4)";
    final String expected = "SELECT "
        + "REGEXP_SUBSTR('chocolate Chip cookies', '(?i)c+.{2}', 1, 2)\n"
        + "FROM foodmart.product\n"
        + "WHERE product_id = 1 OR product_id = 2 OR product_id = 3 OR product_id = 4";
    sql(query)
        .withBigQuery()
        .ok(expected);
  }

  @Test public void testRegexSubstrFunction5ArgswithBackSlash() {
    final String query = "select regexp_substr('chocolate Chip cookies','[-\\_] V[0-9]+',"
        + "1,1,'i')\n"
        + "from \"foodmart\".\"product\" where \"product_id\" in (1, 2, 3, 4)";
    final String expected = "SELECT "
        + "REGEXP_SUBSTR('chocolate Chip cookies', '(?i)[-\\\\_] V[0-9]+', 1, 1)\n"
        + "FROM foodmart.product\n"
        + "WHERE product_id = 1 OR product_id = 2 OR product_id = 3 OR product_id = 4";
    sql(query)
        .withBigQuery()
        .ok(expected);
  }

  @Test public void testTimestampFunctionRelToSql() {
    final RelBuilder builder = relBuilder();
    final RexNode currentTimestampRexNode = builder.call(SqlLibraryOperators.CURRENT_TIMESTAMP,
        builder.literal(6));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(currentTimestampRexNode, "CT"))
        .build();
    final String expectedSql = "SELECT CURRENT_TIMESTAMP(6) AS \"CT\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', "
        + "CURRENT_DATETIME()) AS DATETIME) AS CT\n"
        + "FROM scott.EMP";
    final String expectedSpark = "SELECT CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss"
        + ".ssssss') AS TIMESTAMP) CT\n"
        + "FROM scott.EMP";
    final String expectedHive = "SELECT CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss"
        + ".ssssss') AS TIMESTAMP) CT\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
    assertThat(toSql(root, DatabaseProduct.HIVE.getDialect()), isLinux(expectedHive));
  }

  @Test void testJsonType() {
    String query = "select json_type(\"product_name\") from \"product\"";
    final String expected = "SELECT "
        + "JSON_TYPE(\"product_name\")\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query).ok(expected);
  }

  @Test void testJsonDepth() {
    String query = "select json_depth(\"product_name\") from \"product\"";
    final String expected = "SELECT "
        + "JSON_DEPTH(\"product_name\")\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query).ok(expected);
  }

  @Test void testJsonLength() {
    String query = "select json_length(\"product_name\", 'lax $'), "
        + "json_length(\"product_name\") from \"product\"";
    final String expected = "SELECT JSON_LENGTH(\"product_name\", 'lax $'), "
        + "JSON_LENGTH(\"product_name\")\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query).ok(expected);
  }

  @Test void testJsonKeys() {
    String query = "select json_keys(\"product_name\", 'lax $') from \"product\"";
    final String expected = "SELECT JSON_KEYS(\"product_name\", 'lax $')\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query).ok(expected);
  }

  @Test public void testDateSubIntervalMonthFunction() {
    String query = "select \"birth_date\" - INTERVAL -'1' MONTH from \"employee\"";
    final String expectedHive = "SELECT ADD_MONTHS(birth_date, -1)\n"
        + "FROM foodmart.employee";
    final String expectedSpark = "SELECT ADD_MONTHS(birth_date, -1)\n"
        + "FROM foodmart.employee";
    final String expectedBigQuery = "SELECT DATE_SUB(birth_date, INTERVAL -1 MONTH)\n"
        + "FROM foodmart.employee";
    sql(query)
        .withHive()
        .ok(expectedHive)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSpark()
        .ok(expectedSpark);
  }

  @Test public void testDatePlusIntervalMonthFunctionWithArthOps() {
    String query = "select \"birth_date\" + -10 * INTERVAL '1' MONTH from \"employee\"";
    final String expectedHive = "SELECT ADD_MONTHS(birth_date, -10)\n"
        + "FROM foodmart.employee";
    final String expectedSpark = "SELECT ADD_MONTHS(birth_date, -10)\n"
        + "FROM foodmart.employee";
    final String expectedBigQuery = "SELECT DATE_ADD(birth_date, INTERVAL -10 MONTH)\n"
        + "FROM foodmart.employee";
    sql(query)
        .withHive()
        .ok(expectedHive)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSpark()
        .ok(expectedSpark);
  }

  @Test public void testTimestampPlusIntervalMonthFunctionWithArthOps() {
    String query = "select \"hire_date\" + -10 * INTERVAL '1' MONTH from \"employee\"";
    final String expectedBigQuery = "SELECT CAST(DATETIME_ADD(CAST(hire_date AS DATETIME), "
        + "INTERVAL "
        + "-10 MONTH) AS DATETIME)\n"
        + "FROM foodmart.employee";
    sql(query)
        .withBigQuery()
        .ok(expectedBigQuery);
  }

  @Test public void testDatePlusIntervalMonthFunctionWithCol() {
    String query = "select \"birth_date\" +  \"store_id\" * INTERVAL '10' MONTH from \"employee\"";
    final String expectedHive = "SELECT ADD_MONTHS(birth_date, store_id * 10)\n"
        + "FROM foodmart.employee";
    final String expectedSpark = "SELECT ADD_MONTHS(birth_date, store_id * 10)\n"
        + "FROM foodmart.employee";
    final String expectedBigQuery = "SELECT DATE_ADD(birth_date, INTERVAL store_id * 10 MONTH)\n"
        + "FROM foodmart.employee";
    sql(query)
        .withHive()
        .ok(expectedHive)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSpark()
        .ok(expectedSpark);
  }

  @Test public void testDatePlusIntervalMonthFunctionWithArithOp() {
    String query = "select \"birth_date\" + 10 * INTERVAL '2' MONTH from \"employee\"";
    final String expectedHive = "SELECT ADD_MONTHS(birth_date, 10 * 2)\n"
        + "FROM foodmart.employee";
    final String expectedSpark = "SELECT ADD_MONTHS(birth_date, 10 * 2)\n"
        + "FROM foodmart.employee";
    final String expectedBigQuery = "SELECT DATE_ADD(birth_date, INTERVAL 10 * 2 MONTH)\n"
        + "FROM foodmart.employee";
    sql(query)
        .withHive()
        .ok(expectedHive)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSpark()
        .ok(expectedSpark);
  }

  @Test public void testDatePlusColumnFunction() {
    String query = "select \"birth_date\" + INTERVAL '1' DAY from \"employee\"";
    final String expectedHive = "SELECT CAST(DATE_ADD(birth_date, 1) AS DATE)\n"
        + "FROM foodmart.employee";
    final String expectedSpark = "SELECT DATE_ADD(birth_date, 1)\n"
        + "FROM foodmart.employee";
    final String expectedBigQuery = "SELECT DATE_ADD(birth_date, INTERVAL 1 DAY)\n"
        + "FROM foodmart.employee";
    final String expectedSnowflake = "SELECT DATEADD(DAY, 1, \"birth_date\")\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withHive()
        .ok(expectedHive)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSpark()
        .ok(expectedSpark)
        .withSnowflake()
        .ok(expectedSnowflake);
  }

  @Test public void testDateSubColumnFunction() {
    String query = "select \"birth_date\" - INTERVAL '1' DAY from \"employee\"";
    final String expectedHive = "SELECT CAST(DATE_SUB(birth_date, 1) AS DATE)\n"
        + "FROM foodmart.employee";
    final String expectedSpark = "SELECT DATE_SUB(birth_date, 1)\n"
        + "FROM foodmart.employee";
    final String expectedBigQuery = "SELECT DATE_SUB(birth_date, INTERVAL 1 DAY)\n"
        + "FROM foodmart.employee";
    final String expectedSnowflake = "SELECT DATEADD(DAY, -1, \"birth_date\")\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withHive()
        .ok(expectedHive)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSpark()
        .ok(expectedSpark)
        .withSnowflake()
        .ok(expectedSnowflake);
  }

  @Test public void testDateValuePlusColumnFunction() {
    String query = "select DATE'2018-01-01' + INTERVAL '1' DAY from \"employee\"";
    final String expectedHive = "SELECT CAST(DATE_ADD(DATE '2018-01-01', 1) AS DATE)\n"
        + "FROM foodmart.employee";
    final String expectedSpark = "SELECT DATE_ADD(DATE '2018-01-01', 1)\n"
        + "FROM foodmart.employee";
    final String expectedBigQuery = "SELECT DATE_ADD(DATE '2018-01-01', INTERVAL 1 DAY)\n"
        + "FROM foodmart.employee";
    final String expectedSnowflake = "SELECT DATEADD(DAY, 1, DATE '2018-01-01')\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withHive()
        .ok(expectedHive)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSpark()
        .ok(expectedSpark)
        .withSnowflake()
        .ok(expectedSnowflake);
  }

  @Test public void testDateValueSubColumnFunction() {
    String query = "select DATE'2018-01-01' - INTERVAL '1' DAY from \"employee\"";
    final String expectedHive = "SELECT CAST(DATE_SUB(DATE '2018-01-01', 1) AS DATE)\n"
        + "FROM foodmart.employee";
    final String expectedSpark = "SELECT DATE_SUB(DATE '2018-01-01', 1)\n"
        + "FROM foodmart.employee";
    final String expectedBigQuery = "SELECT DATE_SUB(DATE '2018-01-01', INTERVAL 1 DAY)\n"
        + "FROM foodmart.employee";
    final String expectedSnowflake = "SELECT DATEADD(DAY, -1, DATE '2018-01-01')\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withHive()
        .ok(expectedHive)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSpark()
        .ok(expectedSpark)
        .withSnowflake()
        .ok(expectedSnowflake);
  }

  @Test public void testDateIntColumnFunction() {
    String query = "select \"birth_date\" + INTERVAL '2' day from \"employee\"";
    final String expectedHive = "SELECT CAST(DATE_ADD(birth_date, 2) AS DATE)\n"
        + "FROM foodmart.employee";
    final String expectedSpark = "SELECT DATE_ADD(birth_date, 2)\n"
        + "FROM foodmart.employee";
    final String expectedBigQuery = "SELECT DATE_ADD(birth_date, INTERVAL 2 DAY)\n"
        + "FROM foodmart.employee";
    final String expectedSnowflake = "SELECT DATEADD(DAY, 2, \"birth_date\")\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withHive()
        .ok(expectedHive)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSpark()
        .ok(expectedSpark)
        .withSnowflake()
        .ok(expectedSnowflake);
  }

  @Test public void testIntervalMinute() {
    String query = "select cast(\"birth_date\" as timestamp) + INTERVAL\n"
            + "'2' minute from \"employee\"";
    final String expectedBigQuery = "SELECT TIMESTAMP_ADD(CAST(birth_date AS "
            + "DATETIME), INTERVAL 2 MINUTE)\n"
            + "FROM foodmart.employee";
    sql(query)
            .withBigQuery()
            .ok(expectedBigQuery);
  }

  @Test public void testIntervalHour() {
    String query = "select cast(\"birth_date\" as timestamp) + INTERVAL\n"
            + "'2' hour from \"employee\"";
    final String expectedBigQuery = "SELECT TIMESTAMP_ADD(CAST(birth_date AS "
            + "DATETIME), INTERVAL 2 HOUR)\n"
            + "FROM foodmart.employee";
    sql(query)
            .withBigQuery()
            .ok(expectedBigQuery);
  }
  @Test public void testIntervalSecond() {
    String query = "select cast(\"birth_date\" as timestamp) + INTERVAL '2'\n"
            + "second from \"employee\"";
    final String expectedBigQuery = "SELECT TIMESTAMP_ADD(CAST(birth_date AS"
            + " DATETIME), INTERVAL 2 SECOND)\n"
            + "FROM foodmart.employee";
    sql(query)
            .withBigQuery()
            .ok(expectedBigQuery);
  }

  @Test public void testDateSubInterFunction() {
    String query = "select \"birth_date\" - INTERVAL '2' day from \"employee\"";
    final String expectedHive = "SELECT CAST(DATE_SUB(birth_date, 2) AS DATE)\n"
        + "FROM foodmart.employee";
    final String expectedSpark = "SELECT DATE_SUB(birth_date, 2)\n"
        + "FROM foodmart.employee";
    final String expectedBigQuery = "SELECT DATE_SUB(birth_date, INTERVAL 2 DAY)\n"
        + "FROM foodmart.employee";
    final String expectedSnowflake = "SELECT DATEADD(DAY, -2, \"birth_date\")\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withHive()
        .ok(expectedHive)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSpark()
        .ok(expectedSpark)
        .withSnowflake()
        .ok(expectedSnowflake);
  }

  @Test public void testDatePlusColumnVariFunction() {
    String query = "select \"birth_date\" + \"store_id\" * INTERVAL '1' DAY from \"employee\"";
    final String expectedHive = "SELECT CAST(DATE_ADD(birth_date, store_id) AS DATE)\n"
        + "FROM foodmart.employee";
    final String expectedSpark = "SELECT DATE_ADD(birth_date, store_id)\n"
        + "FROM foodmart.employee";
    final String expectedBigQuery = "SELECT DATE_ADD(birth_date, INTERVAL store_id DAY)\n"
        + "FROM foodmart.employee";
    final String expectedSnowflake = "SELECT (\"birth_date\" + \"store_id\")\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withHive()
        .ok(expectedHive)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSpark()
        .ok(expectedSpark)
        .withSnowflake()
        .ok(expectedSnowflake);
  }

  @Test public void testDatePlusIntervalColumnFunction() {
    String query = "select \"birth_date\" +  INTERVAL '1' DAY * \"store_id\" from \"employee\"";
    final String expectedHive = "SELECT CAST(DATE_ADD(birth_date, store_id) AS DATE)\n"
        + "FROM foodmart.employee";
    final String expectedSpark = "SELECT DATE_ADD(birth_date, store_id)\n"
        + "FROM foodmart.employee";
    final String expectedBigQuery = "SELECT DATE_ADD(birth_date, INTERVAL store_id DAY)\n"
        + "FROM foodmart.employee";
    final String expectedSnowflake = "SELECT DATEADD(DAY, '1' * \"store_id\", \"birth_date\")\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withHive()
        .ok(expectedHive)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSpark()
        .ok(expectedSpark)
        .withSnowflake()
        .ok(expectedSnowflake);
  }

  @Test public void testDatePlusIntervalIntFunction() {
    String query = "select \"birth_date\" +  INTERVAL '1' DAY * 10 from \"employee\"";
    final String expectedHive = "SELECT CAST(DATE_ADD(birth_date, 10) AS DATE)\n"
        + "FROM foodmart.employee";
    final String expectedSpark = "SELECT DATE_ADD(birth_date, 10)\n"
        + "FROM foodmart.employee";
    final String expectedBigQuery = "SELECT DATE_ADD(birth_date, INTERVAL 10 DAY)\n"
        + "FROM foodmart.employee";
    final String expectedSnowflake = "SELECT DATEADD(DAY, '1' * 10, \"birth_date\")\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withHive()
        .ok(expectedHive)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSpark()
        .ok(expectedSpark)
        .withSnowflake()
        .ok(expectedSnowflake);
  }

  @Test public void testDateSubColumnVariFunction() {
    String query = "select \"birth_date\" - \"store_id\" * INTERVAL '1' DAY from \"employee\"";
    final String expectedHive = "SELECT CAST(DATE_SUB(birth_date, store_id) AS DATE)\n"
        + "FROM foodmart.employee";
    final String expectedSpark = "SELECT DATE_SUB(birth_date, store_id)\n"
        + "FROM foodmart.employee";
    final String expectedBigQuery = "SELECT DATE_SUB(birth_date, INTERVAL store_id DAY)\n"
        + "FROM foodmart.employee";
    final String expectedSnowflake = "SELECT (\"birth_date\" - \"store_id\")\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withHive()
        .ok(expectedHive)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSpark()
        .ok(expectedSpark)
        .withSnowflake()
        .ok(expectedSnowflake);
  }

  @Test public void testDateValuePlusColumnVariFunction() {
    String query = "select DATE'2018-01-01' + \"store_id\" * INTERVAL '1' DAY from \"employee\"";
    final String expectedHive = "SELECT CAST(DATE_ADD(DATE '2018-01-01', store_id) AS DATE)\n"
        + "FROM foodmart.employee";
    final String expectedSpark = "SELECT DATE_ADD(DATE '2018-01-01', store_id)\n"
        + "FROM foodmart.employee";
    final String expectedBigQuery = "SELECT DATE_ADD(DATE '2018-01-01', INTERVAL store_id DAY)\n"
        + "FROM foodmart.employee";
    final String expectedSnowflake = "SELECT (DATE '2018-01-01' + \"store_id\")\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withHive()
        .ok(expectedHive)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSpark()
        .ok(expectedSpark)
        .withSnowflake()
        .ok(expectedSnowflake);
  }

  @Test public void testDatePlusColumnFunctionWithArithOp() {
    String query = "select \"birth_date\" + \"store_id\" *11 * INTERVAL '1' DAY from \"employee\"";
    final String expectedHive = "SELECT CAST(DATE_ADD(birth_date, store_id * 11) AS DATE)\n"
        + "FROM foodmart.employee";
    final String expectedSpark = "SELECT DATE_ADD(birth_date, store_id * 11)\n"
        + "FROM foodmart.employee";
    final String expectedBigQuery = "SELECT DATE_ADD(birth_date, INTERVAL store_id * 11 DAY)\n"
        + "FROM foodmart.employee";
    final String expectedSnowflake = "SELECT (\"birth_date\" + \"store_id\" * 11)\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withHive()
        .ok(expectedHive)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSpark()
        .ok(expectedSpark)
        .withSnowflake()
        .ok(expectedSnowflake);
  }

  @Test public void testDatePlusColumnFunctionVariWithArithOp() {
    String query = "select \"birth_date\" + \"store_id\"  * INTERVAL '11' DAY from \"employee\"";
    final String expectedHive = "SELECT CAST(DATE_ADD(birth_date, store_id * 11) AS DATE)\n"
        + "FROM foodmart.employee";
    final String expectedSpark = "SELECT DATE_ADD(birth_date, store_id * 11)\n"
        + "FROM foodmart.employee";
    final String expectedBigQuery = "SELECT DATE_ADD(birth_date, INTERVAL store_id * 11 DAY)\n"
        + "FROM foodmart.employee";
    final String expectedSnowflake = "SELECT (\"birth_date\" + \"store_id\" * 11)\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withHive()
        .ok(expectedHive)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSpark()
        .ok(expectedSpark)
        .withSnowflake()
        .ok(expectedSnowflake);
  }

  @Test public void testDateSubColumnFunctionVariWithArithOp() {
    String query = "select \"birth_date\" - \"store_id\"  * INTERVAL '11' DAY from \"employee\"";
    final String expectedHive = "SELECT CAST(DATE_SUB(birth_date, store_id * 11) AS DATE)\n"
        + "FROM foodmart.employee";
    final String expectedSpark = "SELECT DATE_SUB(birth_date, store_id * 11)\n"
        + "FROM foodmart.employee";
    final String expectedBigQuery = "SELECT DATE_SUB(birth_date, INTERVAL store_id * 11 DAY)\n"
        + "FROM foodmart.employee";
    final String expectedSnowflake = "SELECT (\"birth_date\" - \"store_id\" * 11)\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withHive()
        .ok(expectedHive)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSpark()
        .ok(expectedSpark)
        .withSnowflake()
        .ok(expectedSnowflake);
  }

  @Test public void testDatePlusIntervalDayFunctionWithArithOp() {
    String query = "select \"birth_date\" + 10 * INTERVAL '2' DAY from \"employee\"";
    final String expectedHive = "SELECT CAST(DATE_ADD(birth_date, 10 * 2) AS DATE)\n"
        + "FROM foodmart.employee";
    final String expectedSpark = "SELECT DATE_ADD(birth_date, 10 * 2)\n"
        + "FROM foodmart.employee";
    final String expectedBigQuery = "SELECT DATE_ADD(birth_date, INTERVAL 10 * 2 DAY)\n"
        + "FROM foodmart.employee";
    final String expectedSnowflake = "SELECT (\"birth_date\" + 10 * 2)\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withHive()
        .ok(expectedHive)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSpark()
        .ok(expectedSpark)
        .withSnowflake()
        .ok(expectedSnowflake);
  }

  @Test public void testIntervalDayPlusDateFunction() {
    String query = "select  INTERVAL '1' DAY + \"birth_date\" from \"employee\"";
    final String expectedHive = "SELECT CAST(DATE_ADD(birth_date, 1) AS DATE)\n"
        + "FROM foodmart.employee";
    final String expectedSpark = "SELECT DATE_ADD(birth_date, 1)\n"
        + "FROM foodmart.employee";
    final String expectedBigQuery = "SELECT DATE_ADD(birth_date, INTERVAL 1 DAY)\n"
        + "FROM foodmart.employee";
    final String expectedSnowflake = "SELECT DATEADD(DAY, 1, \"birth_date\")\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withHive()
        .ok(expectedHive)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSpark()
        .ok(expectedSpark)
        .withSnowflake()
        .ok(expectedSnowflake);
  }

  @Test public void testIntervalHourToSecond() {
    String query = "SELECT CURRENT_TIMESTAMP + INTERVAL '06:10:30' HOUR TO SECOND,"
        + "CURRENT_TIMESTAMP - INTERVAL '06:10:30' HOUR TO SECOND "
        + "FROM \"employee\"";
    final String expectedBQ = "SELECT TIMESTAMP_ADD(CURRENT_DATETIME(), INTERVAL 22230 SECOND), "
            + "TIMESTAMP_SUB(CURRENT_DATETIME(), INTERVAL 22230 SECOND)\n"
            + "FROM foodmart.employee";
    sql(query)
        .withBigQuery()
        .ok(expectedBQ);
  }

  @Test public void truncateFunctionEmulationForBigQuery() {
    String query = "select truncate(2.30259, 3) from \"employee\"";
    final String expectedBigQuery = "SELECT TRUNC(2.30259, 3)\n"
        + "FROM foodmart.employee";
    sql(query)
        .withBigQuery().ok(expectedBigQuery);
  }

  @Test public void truncateFunctionWithSingleOperandEmulationForBigQuery() {
    String query = "select truncate(2.30259) from \"employee\"";
    final String expectedBigQuery = "SELECT TRUNC(2.30259)\n"
        + "FROM foodmart.employee";
    sql(query)
        .withBigQuery().ok(expectedBigQuery);
  }

  @Test public void extractFunctionEmulation() {
    String query = "select extract(year from \"hire_date\") from \"employee\"";
    final String expectedHive = "SELECT YEAR(hire_date)\n"
        + "FROM foodmart.employee";
    final String expectedSpark = "SELECT YEAR(hire_date)\n"
        + "FROM foodmart.employee";
    final String expectedBigQuery = "SELECT EXTRACT(YEAR FROM hire_date)\n"
        + "FROM foodmart.employee";
    final String expectedMsSql = "SELECT YEAR([hire_date])\n"
        + "FROM [foodmart].[employee]";
    sql(query)
        .withHive()
        .ok(expectedHive)
        .withSpark()
        .ok(expectedSpark)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withMssql()
        .ok(expectedMsSql);
  }

  @Test public void extractMinuteFunctionEmulation() {
    String query = "select extract(minute from \"hire_date\") from \"employee\"";
    final String expectedBigQuery = "SELECT EXTRACT(MINUTE FROM hire_date)\n"
        + "FROM foodmart.employee";
    final String expectedMsSql = "SELECT DATEPART(MINUTE, [hire_date])\n"
        + "FROM [foodmart].[employee]";
    sql(query)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withMssql()
        .ok(expectedMsSql);
  }

  @Test public void extractSecondFunctionEmulation() {
    String query = "select extract(second from \"hire_date\") from \"employee\"";
    final String expectedBigQuery = "SELECT EXTRACT(SECOND FROM hire_date)\n"
        + "FROM foodmart.employee";
    final String expectedMsSql = "SELECT DATEPART(SECOND, [hire_date])\n"
        + "FROM [foodmart].[employee]";
    sql(query)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withMssql()
        .ok(expectedMsSql);
  }

  @Test public void selectWithoutFromEmulationForHiveAndSparkAndBigquery() {
    String query = "select 2 + 2";
    final String expected = "SELECT 2 + 2";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expected);
  }

  @Test public void currentTimestampFunctionForHiveAndSparkAndBigquery() {
    String query = "select current_timestamp";
    final String expectedHiveQuery = "SELECT CURRENT_TIMESTAMP `CURRENT_TIMESTAMP`";
    final String expectedSparkQuery = "SELECT CURRENT_TIMESTAMP `CURRENT_TIMESTAMP`";
    final String expectedBigQuery = "SELECT CURRENT_DATETIME() AS CURRENT_TIMESTAMP";

    sql(query)
        .withHiveIdentifierQuoteString()
        .ok(expectedHiveQuery)
        .withSparkIdentifierQuoteString()
        .ok(expectedSparkQuery)
        .withBigQuery()
        .ok(expectedBigQuery);
  }

  @Test public void concatFunctionEmulationForHiveAndSparkAndBigQuery() {
    String query = "select 'foo' || 'bar' from \"employee\"";
    final String expected = "SELECT CONCAT('foo', 'bar')\n"
        + "FROM foodmart.employee";
    final String mssql = "SELECT CONCAT('foo', 'bar')\n"
            + "FROM [foodmart].[employee]";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expected)
        .withMssql()
        .ok(mssql);
  }

  @Test void testJsonRemove() {
    String query = "select json_remove(\"product_name\", '$[0]') from \"product\"";
    final String expected = "SELECT JSON_REMOVE(\"product_name\", '$[0]')\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query).ok(expected);
  }
/*
  @Test void testUnionAllWithNoOperandsUsingOracleDialect() {
    String query = "select A.\"department_id\" "
        + "from \"foodmart\".\"employee\" A "
        + " where A.\"department_id\" = ( select min( A.\"department_id\") from \"foodmart\""
        + ".\"department\" B where 1=2 )";
    final String expected = "SELECT \"employee\".\"department_id\"\n"
        + "FROM \"foodmart\".\"employee\"\n"
        + "INNER JOIN (SELECT \"t1\".\"department_id\" \"department_id0\", MIN(\"t1\""
        + ".\"department_id\") \"EXPR$0\"\n"
        + "FROM (SELECT NULL \"department_id\", NULL \"department_description\"\nFROM "
        + "\"DUAL\"\nWHERE 1 = 0) \"t\",\n"
        + "(SELECT \"department_id\"\nFROM \"foodmart\".\"employee\"\nGROUP BY \"department_id\")"
        + " \"t1\"\n"
        + "GROUP BY \"t1\".\"department_id\") \"t3\" ON \"employee\".\"department_id\" = \"t3\""
        + ".\"department_id0\""
        + " AND \"employee\".\"department_id\" = \"t3\".\"EXPR$0\"";
    sql(query).withOracle().ok(expected);
  }*/

  /*@Test void testUnionAllWithNoOperands() {
    String query = "select A.\"department_id\" "
        + "from \"foodmart\".\"employee\" A "
        + " where A.\"department_id\" = ( select min( A.\"department_id\") from \"foodmart\""
        + ".\"department\" B where 1=2 )";
    final String expected = "SELECT \"employee\".\"department_id\"\n"
        + "FROM \"foodmart\".\"employee\"\n"
        + "INNER JOIN (SELECT \"t1\".\"department_id\" AS \"department_id0\","
        + " MIN(\"t1\".\"department_id\") AS \"EXPR$0\"\n"
        + "FROM (SELECT *\nFROM (VALUES (NULL, NULL))"
        + " AS \"t\" (\"department_id\", \"department_description\")"
        + "\nWHERE 1 = 0) AS \"t\","
        + "\n(SELECT \"department_id\"\nFROM \"foodmart\".\"employee\""
        + "\nGROUP BY \"department_id\") AS \"t1\""
        + "\nGROUP BY \"t1\".\"department_id\") AS \"t3\" "
        + "ON \"employee\".\"department_id\" = \"t3\".\"department_id0\""
        + " AND \"employee\".\"department_id\" = \"t3\".\"EXPR$0\"";
    sql(query).ok(expected);
  }*/

  @Test void testSmallintOracle() {
    String query = "SELECT CAST(\"department_id\" AS SMALLINT) FROM \"employee\"";
    String expected = "SELECT CAST(\"department_id\" AS NUMBER(5))\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withOracle()
        .ok(expected);
  }

  @Test void testBigintOracle() {
    String query = "SELECT CAST(\"department_id\" AS BIGINT) FROM \"employee\"";
    String expected = "SELECT CAST(\"department_id\" AS NUMBER(19))\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withOracle()
        .ok(expected);
  }

  @Test void testDoubleOracle() {
    String query = "SELECT CAST(\"department_id\" AS DOUBLE) FROM \"employee\"";
    String expected = "SELECT CAST(\"department_id\" AS DOUBLE PRECISION)\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withOracle()
        .ok(expected);
  }

  @Test void testDateLiteralOracle() {
    String query = "SELECT DATE '1978-05-02' FROM \"employee\"";
    String expected = "SELECT TO_DATE('1978-05-02', 'YYYY-MM-DD')\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withOracle()
        .ok(expected);
  }

  @Test void testTimestampLiteralOracle() {
    String query = "SELECT TIMESTAMP '1978-05-02 12:34:56.78' FROM \"employee\"";
    String expected = "SELECT TO_TIMESTAMP('1978-05-02 12:34:56.78',"
        + " 'YYYY-MM-DD HH24:MI:SS.FF')\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withOracle()
        .ok(expected);
  }

  @Test void testTimeLiteralOracle() {
    String query = "SELECT TIME '12:34:56.78' FROM \"employee\"";
    String expected = "SELECT TO_TIME('12:34:56.78', 'HH24:MI:SS.FF')\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withOracle()
        .ok(expected);
  }


  @Test public void testSelectWithGroupByOnColumnNotPresentInProjection() {
    String query = "select \"t1\".\"department_id\" from\n"
        + "\"foodmart\".\"employee\" as \"t1\" inner join \"foodmart\".\"department\" as \"t2\"\n"
        + "on \"t1\".\"department_id\" = \"t2\".\"department_id\"\n"
        + "group by \"t2\".\"department_id\", \"t1\".\"department_id\"";
    final String expected = "SELECT t0.department_id\n"
        + "FROM (SELECT department.department_id AS department_id0, employee.department_id\n"
        + "FROM foodmart.employee\n"
        + "INNER JOIN foodmart.department ON employee.department_id = department.department_id\n"
        + "GROUP BY department_id0, employee.department_id) AS t0";
    sql(query).withBigQuery().ok(expected);
  }

  @Test void testSupportsDataType() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataType booleanDataType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    final RelDataType integerDataType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    final SqlDialect oracleDialect = SqlDialect.DatabaseProduct.ORACLE.getDialect();
    assertFalse(oracleDialect.supportsDataType(booleanDataType));
    assertTrue(oracleDialect.supportsDataType(integerDataType));
    final SqlDialect postgresqlDialect = SqlDialect.DatabaseProduct.POSTGRESQL.getDialect();
    assertTrue(postgresqlDialect.supportsDataType(booleanDataType));
    assertTrue(postgresqlDialect.supportsDataType(integerDataType));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4150">[CALCITE-4150]
   * JDBC adapter throws UnsupportedOperationException when generating SQL
   * for untyped NULL literal</a>. */
  @Test void testSelectRawNull() {
    final String query = "SELECT NULL FROM \"product\"";
    final String expected = "SELECT NULL\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query).ok(expected);
  }

  @Test void testSelectRawNullWithAlias() {
    final String query = "SELECT NULL AS DUMMY FROM \"product\"";
    final String expected = "SELECT NULL AS \"DUMMY\"\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query).ok(expected);
  }

  @Test void testSelectNullWithCast() {
    final String query = "SELECT CAST(NULL AS INT)";
    final String expected = "SELECT *\n"
        + "FROM (VALUES (NULL)) AS \"t\" (\"EXPR$0\")";
    sql(query).ok(expected);
    // validate
    sql(expected).exec();
  }

  @Test void testSelectNullWithCount() {
    final String query = "SELECT COUNT(CAST(NULL AS INT))";
    final String expected = "SELECT COUNT(\"$f0\")\n"
        + "FROM (VALUES (NULL)) AS \"t\" (\"$f0\")";
    sql(query).ok(expected);
    // validate
    sql(expected).exec();
  }

  @Test void testSelectNullWithGroupByNull() {
    final String query = "SELECT COUNT(CAST(NULL AS INT))\n"
        + "FROM (VALUES  (0))AS \"t\"\n"
        + "GROUP BY CAST(NULL AS VARCHAR CHARACTER SET \"ISO-8859-1\")";
    final String expected = "SELECT COUNT(\"$f1\")\n"
        + "FROM (VALUES (NULL, NULL)) AS \"t\" (\"$f0\", \"$f1\")\n"
        + "GROUP BY \"$f0\"";
    sql(query).ok(expected);
    // validate
    sql(expected).exec();
  }

  @Test void testSelectNullWithGroupByVar() {
    final String query = "SELECT COUNT(CAST(NULL AS INT))\n"
        + "FROM \"account\" AS \"t\"\n"
        + "GROUP BY \"account_type\"";
    final String expected = "SELECT COUNT(CAST(NULL AS INTEGER))\n"
        + "FROM \"foodmart\".\"account\"\n"
        + "GROUP BY \"account_type\"";
    sql(query).ok(expected);
    // validate
    sql(expected).exec();
  }

  @Test void testSelectNullWithInsert() {
    final String query = "insert into\n"
        + "\"account\"(\"account_id\",\"account_parent\",\"account_type\",\"account_rollup\")\n"
        + "select 1, cast(NULL AS INT), cast(123 as varchar), cast(123 as varchar)";
    final String expected = "INSERT INTO \"foodmart\".\"account\" ("
        + "\"account_id\", \"account_parent\", \"account_description\", "
        + "\"account_type\", \"account_rollup\", \"Custom_Members\")\n"
        + "(SELECT \"EXPR$0\" AS \"account_id\","
        + " \"EXPR$1\" AS \"account_parent\","
        + " CAST(NULL AS VARCHAR(30) CHARACTER SET \"ISO-8859-1\") "
        + "AS \"account_description\","
        + " \"EXPR$2\" AS \"account_type\", "
        + "\"EXPR$3\" AS \"account_rollup\","
        + " CAST(NULL AS VARCHAR(255) CHARACTER SET \"ISO-8859-1\") "
        + "AS \"Custom_Members\"\n"
        + "FROM (VALUES (1, NULL, '123', '123')) "
        + "AS \"t\" (\"EXPR$0\", \"EXPR$1\", \"EXPR$2\", \"EXPR$3\"))";
    sql(query).ok(expected);
    // validate
    sql(expected).exec();
  }

  @Test void testSelectNullWithInsertFromJoin() {
    final String query = "insert into\n"
        + "\"account\"(\"account_id\",\"account_parent\",\n"
        + "\"account_type\",\"account_rollup\")\n"
        + "select \"product\".\"product_id\",\n"
        + "cast(NULL AS INT),\n"
        + "cast(\"product\".\"product_id\" as varchar),\n"
        + "cast(\"sales_fact_1997\".\"store_id\" as varchar)\n"
        + "from \"product\"\n"
        + "inner join \"sales_fact_1997\"\n"
        + "on \"product\".\"product_id\" = \"sales_fact_1997\".\"product_id\"";
    final String expected = "INSERT INTO \"foodmart\".\"account\" "
        + "(\"account_id\", \"account_parent\", \"account_description\", "
        + "\"account_type\", \"account_rollup\", \"Custom_Members\")\n"
        + "(SELECT \"product\".\"product_id\" AS \"account_id\", "
        + "CAST(NULL AS INTEGER) AS \"account_parent\", CAST(NULL AS VARCHAR"
        + "(30) CHARACTER SET \"ISO-8859-1\") AS \"account_description\", "
        + "CAST(\"product\".\"product_id\" AS VARCHAR CHARACTER SET "
        + "\"ISO-8859-1\") AS \"account_type\", "
        + "CAST(\"sales_fact_1997\".\"store_id\" AS VARCHAR CHARACTER SET \"ISO-8859-1\") AS "
        + "\"account_rollup\", "
        + "CAST(NULL AS VARCHAR(255) CHARACTER SET \"ISO-8859-1\") AS \"Custom_Members\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "INNER JOIN \"foodmart\".\"sales_fact_1997\" "
        + "ON \"product\".\"product_id\" = \"sales_fact_1997\".\"product_id\")";
    sql(query).ok(expected);
    // validate
    sql(expected).exec();
  }

  @Test void testCastDecimalOverflow() {
    final String query =
        "SELECT CAST('11111111111111111111111111111111.111111' AS DECIMAL(38,6)) AS \"num\" from \"product\"";
    final String expected =
        "SELECT CAST('11111111111111111111111111111111.111111' AS DECIMAL(19, 6)) AS \"num\"\n"
            + "FROM \"foodmart\".\"product\"";
    sql(query).ok(expected);

    final String query2 =
        "SELECT CAST(1111111 AS DECIMAL(5,2)) AS \"num\" from \"product\"";
    final String expected2 =
        "SELECT CAST(1111111 AS DECIMAL(5, 2)) AS \"num\"\nFROM \"foodmart\".\"product\"";
    sql(query2).ok(expected2);
  }

  @Test void testCastInStringIntegerComparison() {
    final String query = "select \"employee_id\" "
        + "from \"foodmart\".\"employee\" "
        + "where 10 = cast('10' as int) and \"birth_date\" = cast('1914-02-02' as date) or "
        + "\"hire_date\" = cast('1996-01-01 '||'00:00:00' as timestamp)";
    final String expected = "SELECT \"employee_id\"\n"
        + "FROM \"foodmart\".\"employee\"\n"
        + "WHERE 10 = '10' AND \"birth_date\" = '1914-02-02' OR \"hire_date\" = '1996-01-01 ' || "
        + "'00:00:00'";
    final String expectedBiqquery = "SELECT employee_id\n"
        + "FROM foodmart.employee\n"
        + "WHERE 10 = CAST('10' AS INT64) AND birth_date = '1914-02-02' OR hire_date = "
        + "CAST(CONCAT('1996-01-01 ', '00:00:00') AS DATETIME)";
    sql(query)
        .ok(expected)
        .withBigQuery()
        .ok(expectedBiqquery);
  }

  @Test void testDialectQuoteStringLiteral() {
    dialects().forEach((dialect, databaseProduct) -> {
      assertThat(dialect.quoteStringLiteral(""), is("''"));
      assertThat(dialect.quoteStringLiteral("can't run"),
          databaseProduct == DatabaseProduct.BIG_QUERY
              ? is("'can\\'t run'")
              : is("'can''t run'"));

      assertThat(dialect.unquoteStringLiteral("''"), is(""));
      if (databaseProduct == DatabaseProduct.BIG_QUERY) {
        assertThat(dialect.unquoteStringLiteral("'can\\'t run'"),
            is("can't run"));
      } else {
        assertThat(dialect.unquoteStringLiteral("'can't run'"),
            is("can't run"));
      }
    });
  }

  @Test public void testToNumberFunctionHandlingHexaToInt() {
    String query = "select TO_NUMBER('03ea02653f6938ba','XXXXXXXXXXXXXXXX')";
    final String expected = "SELECT CAST(CONV('03ea02653f6938ba', 16, 10) AS BIGINT)";
    final String expectedBigQuery = "SELECT CAST(CONCAT('0x', '03ea02653f6938ba') AS INT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER('03ea02653f6938ba', 'XXXXXXXXXXXXXXXX')";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingFloatingPoint() {
    String query = "select TO_NUMBER('-1.7892','9.9999')";
    final String expected = "SELECT CAST('-1.7892' AS FLOAT)";
    final String expectedBigQuery = "SELECT CAST('-1.7892' AS FLOAT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER('-1.7892', 38, 4)";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionWithColumns() {
    String query = "SELECT TO_NUMBER(\"first_name\", '000') FROM \"foodmart\""
        + ".\"employee\"";
    final String expectedBigQuery = "SELECT CAST(first_name AS INT64)\n"
        + "FROM foodmart.employee";
    sql(query)
        .withBigQuery()
        .ok(expectedBigQuery);
  }

  @Test public void testOver() {
    String query = "SELECT distinct \"product_id\", MAX(\"product_id\") \n"
        + "OVER(PARTITION BY \"product_id\") AS abc\n"
        + "FROM \"product\"";
    final String expected = "SELECT product_id, MAX(product_id) OVER "
        + "(PARTITION BY product_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ABC\n"
        + "FROM foodmart.product\n"
        + "GROUP BY product_id, MAX(product_id) OVER (PARTITION BY product_id "
        + "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)";
    final String expectedBQ = "SELECT product_id, ABC\n"
        + "FROM (SELECT product_id, MAX(product_id) OVER "
        + "(PARTITION BY product_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ABC\n"
        + "FROM foodmart.product) AS t\n"
        + "GROUP BY product_id, ABC";
    final String expectedSnowFlake = "SELECT \"product_id\", MAX(\"product_id\") OVER "
        + "(PARTITION BY \"product_id\" ORDER BY \"product_id\" ROWS "
        + "BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS \"ABC\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_id\", MAX(\"product_id\") OVER (PARTITION BY \"product_id\" "
        + "ORDER BY \"product_id\" ROWS BETWEEN UNBOUNDED PRECEDING AND "
        + "UNBOUNDED FOLLOWING)";
    final String mssql = "SELECT [product_id], MAX([product_id]) OVER (PARTITION "
        + "BY [product_id] ORDER BY [product_id] ROWS BETWEEN UNBOUNDED PRECEDING AND "
        + "UNBOUNDED FOLLOWING) AS [ABC]\n"
        + "FROM [foodmart].[product]\n"
        + "GROUP BY [product_id], MAX([product_id]) OVER (PARTITION BY [product_id] "
        + "ORDER BY [product_id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)";
    sql(query)
      .withHive()
      .ok(expected)
      .withSpark()
      .ok(expected)
      .withBigQuery()
      .ok(expectedBQ)
      .withSnowflake()
      .ok(expectedSnowFlake)
      .withMssql()
      .ok(mssql);
  }

  @Test public void testNtileFunction() {
    String query = "SELECT ntile(2)\n"
        + "OVER(order BY \"product_id\") AS abc\n"
        + "FROM \"product\"";
    final String expectedBQ = "SELECT NTILE(2) OVER (ORDER BY product_id IS NULL, product_id) "
        + "AS ABC\n"
        + "FROM foodmart.product";
    sql(query)
      .withBigQuery()
      .ok(expectedBQ);
  }

  @Test public void testCountWithWindowFunction() {
    String query = "Select count(*) over() from \"product\"";
    String expected = "SELECT COUNT(*) OVER (RANGE BETWEEN UNBOUNDED PRECEDING "
        + "AND UNBOUNDED FOLLOWING)\n"
        + "FROM foodmart.product";
    String expectedBQ = "SELECT COUNT(*) OVER (RANGE BETWEEN UNBOUNDED PRECEDING "
        + "AND UNBOUNDED FOLLOWING)\n"
        + "FROM foodmart.product";
    final String expectedSnowFlake = "SELECT COUNT(*) OVER (ORDER BY 0 "
        + "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)\n"
        + "FROM \"foodmart\".\"product\"";
    final String mssql = "SELECT COUNT(*) OVER ()\n"
        + "FROM [foodmart].[product]";
    sql(query)
      .withHive()
      .ok(expected)
      .withSpark()
      .ok(expected)
      .withBigQuery()
      .ok(expectedBQ)
      .withSnowflake()
      .ok(expectedSnowFlake)
      .withMssql()
      .ok(mssql);
  }

  @Test public void testOrderByInWindowFunction() {
    String query = "select \"first_name\", COUNT(\"department_id\") as "
        + "\"department_id_number\", ROW_NUMBER() OVER (ORDER BY "
        + "\"department_id\" ASC), SUM(\"department_id\") OVER "
        + "(ORDER BY \"department_id\" ASC) \n"
        + "from \"foodmart\".\"employee\" \n"
        + "GROUP by \"first_name\", \"department_id\"";
    final String expected = "SELECT first_name, department_id_number, ROW_NUMBER() "
        + "OVER (ORDER BY department_id IS NULL, department_id), SUM(department_id) "
        + "OVER (ORDER BY department_id IS NULL, department_id "
        + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)\n"
        + "FROM (SELECT first_name, department_id, COUNT(*) department_id_number\n"
        + "FROM foodmart.employee\n"
        + "GROUP BY first_name, department_id) t0";
    final String expectedBQ = "SELECT first_name, department_id_number, "
        + "ROW_NUMBER() OVER (ORDER BY department_id IS NULL, department_id), SUM(department_id) "
        + "OVER (ORDER BY department_id IS NULL, department_id "
        + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)\n"
        + "FROM (SELECT first_name, department_id, COUNT(*) AS department_id_number\n"
        + "FROM foodmart.employee\n"
        + "GROUP BY first_name, department_id) AS t0";
    final String expectedSnowFlake = "SELECT \"first_name\", \"department_id_number\", "
        + "ROW_NUMBER() OVER (ORDER BY \"department_id\"), SUM(\"department_id\") "
        + "OVER (ORDER BY \"department_id\" RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)\n"
        + "FROM (SELECT \"first_name\", \"department_id\", COUNT(*) AS \"department_id_number\"\n"
        + "FROM \"foodmart\".\"employee\"\n"
        + "GROUP BY \"first_name\", \"department_id\") AS \"t0\"";
    final String mssql = "SELECT [first_name], [department_id_number], ROW_NUMBER()"
        + " OVER (ORDER BY CASE WHEN [department_id] IS NULL THEN 1 ELSE 0 END,"
        + " [department_id]), SUM([department_id]) OVER (ORDER BY CASE WHEN [department_id] IS NULL"
        + " THEN 1 ELSE 0 END, [department_id] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)\n"
        + "FROM (SELECT [first_name], [department_id], COUNT(*) AS [department_id_number]\n"
        + "FROM [foodmart].[employee]\n"
        + "GROUP BY [first_name], [department_id]) AS [t0]";
    sql(query)
      .withHive()
      .ok(expected)
      .withSpark()
      .ok(expected)
      .withBigQuery()
      .ok(expectedBQ)
      .withSnowflake()
      .ok(expectedSnowFlake)
      .withMssql()
      .ok(mssql);
  }

  @Test public void testToNumberFunctionHandlingFloatingPointWithD() {
    String query = "select TO_NUMBER('1.789','9D999')";
    final String expected = "SELECT CAST('1.789' AS FLOAT)";
    final String expectedBigQuery = "SELECT CAST('1.789' AS FLOAT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER('1.789', 38, 3)";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingWithSingleFloatingPoint() {
    String query = "select TO_NUMBER('1.789')";
    final String expected = "SELECT CAST('1.789' AS FLOAT)";
    final String expectedBigQuery = "SELECT CAST('1.789' AS FLOAT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER('1.789', 38, 3)";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingWithComma() {
    String query = "SELECT TO_NUMBER ('1,789', '9,999')";
    final String expected = "SELECT CAST('1789' AS BIGINT)";
    final String expectedBigQuery = "SELECT CAST('1789' AS INT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER('1,789', '9,999')";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingWithCurrency() {
    String query = "SELECT TO_NUMBER ('$1789', '$9999')";
    final String expected = "SELECT CAST('1789' AS BIGINT)";
    final String expectedBigQuery = "SELECT CAST('1789' AS INT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER('$1789', '$9999')";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingWithCurrencyAndL() {
    String query = "SELECT TO_NUMBER ('$1789', 'L9999')";
    final String expected = "SELECT CAST('1789' AS BIGINT)";
    final String expectedBigQuery = "SELECT CAST('1789' AS INT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER('$1789', '$9999')";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingWithMinus() {
    String query = "SELECT TO_NUMBER ('-12334', 'S99999')";
    final String expected = "SELECT CAST('-12334' AS BIGINT)";
    final String expectedBigQuery = "SELECT CAST('-12334' AS INT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER('-12334', 'S99999')";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingWithMinusLast() {
    String query = "SELECT TO_NUMBER ('12334-', '99999S')";
    final String expected = "SELECT CAST('-12334' AS BIGINT)";
    final String expectedBigQuery = "SELECT CAST('-12334' AS INT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER('12334-', '99999S')";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingWithE() {
    String query = "SELECT TO_NUMBER ('12E3', '99EEEE')";
    final String expected = "SELECT CAST('12E3' AS DECIMAL(19, 0))";
    final String expectedBigQuery = "SELECT CAST('12E3' AS NUMERIC)";
    final String expectedSnowFlake = "SELECT TO_NUMBER('12E3', '99EEEE')";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingWithCurrencyName() {
    String query = "SELECT TO_NUMBER('dollar1234','L9999','NLS_CURRENCY=''dollar''')";
    final String expected = "SELECT CAST('1234' AS BIGINT)";
    final String expectedBigQuery = "SELECT CAST('1234' AS INT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER('1234')";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingWithCurrencyNameFloat() {
    String query = "SELECT TO_NUMBER('dollar12.34','L99D99','NLS_CURRENCY=''dollar''')";
    final String expected = "SELECT CAST('12.34' AS FLOAT)";
    final String expectedBigQuery = "SELECT CAST('12.34' AS FLOAT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER('12.34', 38, 2)";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingWithCurrencyNameNull() {
    String query = "SELECT TO_NUMBER('dollar12.34','L99D99',null)";
    final String expected = "SELECT CAST(NULL AS INT)";
    final String expectedBigQuery = "SELECT CAST(NULL AS INT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER(NULL)";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingWithCurrencyNameMinus() {
    String query = "SELECT TO_NUMBER('-dollar1234','L9999','NLS_CURRENCY=''dollar''')";
    final String expected = "SELECT CAST('-1234' AS BIGINT)";
    final String expectedBigQuery = "SELECT CAST('-1234' AS INT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER('-1234')";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingWithG() {
    String query = "SELECT TO_NUMBER ('1,2345', '9G9999')";
    final String expected = "SELECT CAST('12345' AS BIGINT)";
    final String expectedBigQuery = "SELECT CAST('12345' AS INT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER('1,2345', '9G9999')";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingWithU() {
    String query = "SELECT TO_NUMBER ('$1234', 'U9999')";
    final String expected = "SELECT CAST('1234' AS BIGINT)";
    final String expectedBigQuery = "SELECT CAST('1234' AS INT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER('$1234', '$9999')";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingWithPR() {
    String query = "SELECT TO_NUMBER (' 123 ', '999PR')";
    final String expected = "SELECT CAST('123' AS BIGINT)";
    final String expectedBigQuery = "SELECT CAST('123' AS INT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER('123')";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingWithMI() {
    String query = "SELECT TO_NUMBER ('1234-', '9999MI')";
    final String expected = "SELECT CAST('-1234' AS BIGINT)";
    final String expectedBigQuery = "SELECT CAST('-1234' AS INT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER('1234-', '9999MI')";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingWithMIDecimal() {
    String query = "SELECT TO_NUMBER ('1.234-', '9.999MI')";
    final String expected = "SELECT CAST('-1.234' AS FLOAT)";
    final String expectedBigQuery = "SELECT CAST('-1.234' AS FLOAT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER('-1.234', 38, 3)";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingWithZero() {
    String query = "select TO_NUMBER('01234','09999')";
    final String expected = "SELECT CAST('01234' AS BIGINT)";
    final String expectedBigQuery = "SELECT CAST('01234' AS INT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER('01234', '09999')";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingWithB() {
    String query = "select TO_NUMBER('1234','B9999')";
    final String expected = "SELECT CAST('1234' AS BIGINT)";
    final String expectedBigQuery = "SELECT CAST('1234' AS INT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER('1234', 'B9999')";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingWithC() {
    String query = "select TO_NUMBER('USD1234','C9999')";
    final String expected = "SELECT CAST('1234' AS BIGINT)";
    final String expectedBigQuery = "SELECT CAST('1234' AS INT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER('1234')";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandling() {
    final String query = "SELECT TO_NUMBER ('1234', '9999')";
    final String expected = "SELECT CAST('1234' AS BIGINT)";
    final String expectedBigQuery = "SELECT CAST('1234' AS INT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER('1234', '9999')";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingSingleArgumentInt() {
    final String query = "SELECT TO_NUMBER ('1234')";
    final String expected = "SELECT CAST('1234' AS BIGINT)";
    final String expectedBigQuery = "SELECT CAST('1234' AS INT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER('1234')";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingSingleArgumentFloat() {
    final String query = "SELECT TO_NUMBER ('-1.234')";
    final String expected = "SELECT CAST('-1.234' AS FLOAT)";
    final String expectedBigQuery = "SELECT CAST('-1.234' AS FLOAT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER('-1.234', 38, 3)";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingNull() {
    final String query = "SELECT TO_NUMBER ('-1.234',null)";
    final String expected = "SELECT CAST(NULL AS INT)";
    final String expectedBigQuery = "SELECT CAST(NULL AS INT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER(NULL)";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingNullOperand() {
    final String query = "SELECT TO_NUMBER (null)";
    final String expected = "SELECT CAST(NULL AS INT)";
    final String expectedBigQuery = "SELECT CAST(NULL AS INT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER(NULL)";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingSecoNull() {
    final String query = "SELECT TO_NUMBER(null,'9D99')";
    final String expected = "SELECT CAST(NULL AS INT)";
    final String expectedBigQuery = "SELECT CAST(NULL AS INT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER(NULL)";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingFunctionAsArgument() {
    final String query = "SELECT TO_NUMBER(SUBSTRING('12345',2))";
    final String expected = "SELECT CAST(SUBSTRING('12345', 2) AS BIGINT)";
    final String expectedSpark = "SELECT CAST(SUBSTRING('12345', 2) AS BIGINT)";
    final String expectedBigQuery = "SELECT CAST(SUBSTR('12345', 2) AS INT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER(SUBSTR('12345', 2))";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expectedSpark)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingWithNullArgument() {
    final String query = "SELECT TO_NUMBER (null)";
    final String expected = "SELECT CAST(NULL AS INT)";
    final String expectedBigQuery = "SELECT CAST(NULL AS INT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER(NULL)";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingCaseWhenThen() {
    final String query = "select case when TO_NUMBER('12.77') is not null then "
            + "'is_numeric' else 'is not numeric' end";
    final String expected = "SELECT CASE WHEN CAST('12.77' AS FLOAT) IS NOT NULL THEN "
            + "'is_numeric    ' ELSE 'is not numeric' END";
    final String expectedBigQuery = "SELECT CASE WHEN CAST('12.77' AS FLOAT64) IS NOT NULL THEN "
            + "'is_numeric    ' ELSE 'is not numeric' END";
    final String expectedSnowFlake = "SELECT CASE WHEN TO_NUMBER('12.77', 38, 2) IS NOT NULL THEN"
            + " 'is_numeric    ' ELSE 'is not numeric' END";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testToNumberFunctionHandlingWithGDS() {
    String query = "SELECT TO_NUMBER ('12,454.8-', '99G999D9S')";
    final String expected = "SELECT CAST('-12454.8' AS FLOAT)";
    final String expectedBigQuery = "SELECT CAST('-12454.8' AS FLOAT64)";
    final String expectedSnowFlake = "SELECT TO_NUMBER('-12454.8', 38, 1)";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withSnowflake()
        .ok(expectedSnowFlake)
        .withMssql()
        .ok(expected);
  }

  @Test public void testAscii() {
    String query = "SELECT ASCII ('ABC')";
    final String expected = "SELECT ASCII('ABC')";
    final String expectedBigQuery = "SELECT TO_CODE_POINTS('ABC') [OFFSET(0)]";
    sql(query)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected);
  }

  @Test public void testAsciiMethodArgument() {
    String query = "SELECT ASCII (SUBSTRING('ABC',1,1))";
    final String expected = "SELECT ASCII(SUBSTRING('ABC', 1, 1))";
    final String expectedSpark = "SELECT ASCII(SUBSTRING('ABC', 1, 1))";
    final String expectedBigQuery = "SELECT TO_CODE_POINTS(SUBSTR('ABC', 1, 1)) [OFFSET(0)]";
    sql(query)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expectedSpark);
  }

  @Test public void testAsciiColumnArgument() {
    final String query = "select ASCII(\"product_name\") from \"product\" ";
    final String bigQueryExpected = "SELECT TO_CODE_POINTS(product_name) [OFFSET(0)]\n"
        + "FROM foodmart.product";
    final String hiveExpected = "SELECT ASCII(product_name)\n"
        + "FROM foodmart.product";
    sql(query)
        .withBigQuery()
        .ok(bigQueryExpected)
        .withHive()
        .ok(hiveExpected);
  }

  @Test public void testNullIfFunctionRelToSql() {
    final RelBuilder builder = relBuilder();
    final RexNode nullifRexNode = builder.call(SqlStdOperatorTable.NULLIF,
        builder.scan("EMP").field(0), builder.literal(20));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(nullifRexNode, "NI"))
        .build();
    final String expectedSql = "SELECT NULLIF(\"EMPNO\", 20) AS \"NI\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT NULLIF(EMPNO, 20) AS NI\n"
        + "FROM scott.EMP";
    final String expectedSpark = "SELECT NULLIF(EMPNO, 20) NI\n"
        + "FROM scott.EMP";
    final String expectedHive = "SELECT IF(EMPNO = 20, NULL, EMPNO) NI\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
    assertThat(toSql(root, DatabaseProduct.HIVE.getDialect()), isLinux(expectedHive));
  }

  @Test public void testCurrentUser() {
    String query = "select CURRENT_USER";
    final String expectedSql = "SELECT CURRENT_USER() CURRENT_USER";
    final String expectedSqlBQ = "SELECT SESSION_USER() AS CURRENT_USER";
    sql(query)
        .withHive()
        .ok(expectedSql)
        .withBigQuery()
        .ok(expectedSqlBQ);
  }

  @Test public void testCurrentUserWithAlias() {
    String query = "select CURRENT_USER myuser from \"product\" where \"product_id\" = 1";
    final String expectedSql = "SELECT CURRENT_USER() MYUSER\n"
        + "FROM foodmart.product\n"
        + "WHERE product_id = 1";
    final String expected = "SELECT SESSION_USER() AS MYUSER\n"
        + "FROM foodmart.product\n"
        + "WHERE product_id = 1";
    sql(query)
        .withHive()
        .ok(expectedSql)
        .withBigQuery()
        .ok(expected);
  }
  @Test void testSelectCountStar() {
    final String query = "select count(*) from \"product\"";
    final String expected = "SELECT COUNT(*)\n"
        + "FROM \"foodmart\".\"product\"";
    Sql sql = sql(query);
    sql.ok(expected);
  }

  @Test void testRowValueExpression() {
    String sql = "insert into \"DEPT\"\n"
        + "values ROW(1,'Fred', 'San Francisco'),\n"
        + "  ROW(2, 'Eric', 'Washington')";
    final String expectedDefault = "INSERT INTO \"SCOTT\".\"DEPT\""
        + " (\"DEPTNO\", \"DNAME\", \"LOC\")\n"
        + "VALUES (1, 'Fred', 'San Francisco'),\n"
        + "(2, 'Eric', 'Washington')";
    final String expectedDefaultX = "INSERT INTO \"SCOTT\".\"DEPT\""
        + " (\"DEPTNO\", \"DNAME\", \"LOC\")\n"
        + "SELECT 1, 'Fred', 'San Francisco'\n"
        + "FROM (VALUES (0)) AS \"t\" (\"ZERO\")\n"
        + "UNION ALL\n"
        + "SELECT 2, 'Eric', 'Washington'\n"
        + "FROM (VALUES (0)) AS \"t\" (\"ZERO\")";
    final String expectedHive = "INSERT INTO SCOTT.DEPT (DEPTNO, DNAME, LOC)\n"
        + "VALUES (1, 'Fred', 'San Francisco'),\n"
        + "(2, 'Eric', 'Washington')";
    final String expectedHiveX = "INSERT INTO SCOTT.DEPT (DEPTNO, DNAME, LOC)\n"
        + "SELECT 1, 'Fred', 'San Francisco'\n"
        + "UNION ALL\n"
        + "SELECT 2, 'Eric', 'Washington'";
    final String expectedMysql = "INSERT INTO `SCOTT`.`DEPT`"
        + " (`DEPTNO`, `DNAME`, `LOC`)\n"
        + "VALUES (1, 'Fred', 'San Francisco'),\n"
        + "(2, 'Eric', 'Washington')";
    final String expectedMysqlX = "INSERT INTO `SCOTT`.`DEPT`"
        + " (`DEPTNO`, `DNAME`, `LOC`)\nSELECT 1, 'Fred', 'San Francisco'\n"
        + "UNION ALL\n"
        + "SELECT 2, 'Eric', 'Washington'";
    final String expectedOracle = "INSERT INTO \"SCOTT\".\"DEPT\""
        + " (\"DEPTNO\", \"DNAME\", \"LOC\")\n"
        + "VALUES (1, 'Fred', 'San Francisco'),\n"
        + "(2, 'Eric', 'Washington')";
    final String expectedOracleX = "INSERT INTO \"SCOTT\".\"DEPT\""
        + " (\"DEPTNO\", \"DNAME\", \"LOC\")\n"
        + "SELECT 1, 'Fred', 'San Francisco'\n"
        + "FROM \"DUAL\"\n"
        + "UNION ALL\n"
        + "SELECT 2, 'Eric', 'Washington'\n"
        + "FROM \"DUAL\"";
    final String expectedMssql = "INSERT INTO [SCOTT].[DEPT]"
        + " ([DEPTNO], [DNAME], [LOC])\n"
        + "VALUES (1, 'Fred', 'San Francisco'),\n"
        + "(2, 'Eric', 'Washington')";
    final String expectedMssqlX = "INSERT INTO [SCOTT].[DEPT]"
        + " ([DEPTNO], [DNAME], [LOC])\n"
        + "SELECT 1, 'Fred', 'San Francisco'\n"
        + "UNION ALL\n"
        + "SELECT 2, 'Eric', 'Washington'";
    final String expectedCalcite = "INSERT INTO \"SCOTT\".\"DEPT\""
        + " (\"DEPTNO\", \"DNAME\", \"LOC\")\n"
        + "VALUES (1, 'Fred', 'San Francisco'),\n"
        + "(2, 'Eric', 'Washington')";
    final String expectedCalciteX = "INSERT INTO \"SCOTT\".\"DEPT\""
        + " (\"DEPTNO\", \"DNAME\", \"LOC\")\n"
        + "SELECT 1, 'Fred', 'San Francisco'\n"
        + "FROM (VALUES (0)) AS \"t\" (\"ZERO\")\n"
        + "UNION ALL\n"
        + "SELECT 2, 'Eric', 'Washington'\n"
        + "FROM (VALUES (0)) AS \"t\" (\"ZERO\")";
    sql(sql)
        .schema(CalciteAssert.SchemaSpec.JDBC_SCOTT)
        .ok(expectedDefault)
        .withHive().ok(expectedHive)
        .withMysql().ok(expectedMysql)
        .withOracle().ok(expectedOracle)
        .withMssql().ok(expectedMssql)
        .withCalcite().ok(expectedCalcite)
        .withConfig(c ->
            c.withRelBuilderConfigTransform(b ->
                b.withSimplifyValues(false)))
        .withCalcite().ok(expectedDefaultX)
        .withHive().ok(expectedHiveX)
        .withMysql().ok(expectedMysqlX)
        .withOracle().ok(expectedOracleX)
        .withMssql().ok(expectedMssqlX)
        .withCalcite().ok(expectedCalciteX);
  }

  @Test void testInsertValuesWithDynamicParams() {
    final String sql = "insert into \"DEPT\" values (?,?,?), (?,?,?)";
    final String expected = ""
        + "INSERT INTO \"SCOTT\".\"DEPT\" (\"DEPTNO\", \"DNAME\", \"LOC\")\n"
        + "SELECT ?, ?, ?\n"
        + "FROM (VALUES (0)) AS \"t\" (\"ZERO\")\n"
        + "UNION ALL\n"
        + "SELECT ?, ?, ?\n"
        + "FROM (VALUES (0)) AS \"t\" (\"ZERO\")";
    sql(sql)
        .schema(CalciteAssert.SchemaSpec.JDBC_SCOTT)
        .ok(expected);
  }

  @Test void testInsertValuesWithExplicitColumnsAndDynamicParams() {
    final String sql = ""
        + "insert into \"DEPT\" (\"DEPTNO\", \"DNAME\", \"LOC\")\n"
        + "values (?,?,?), (?,?,?)";
    final String expected = ""
        + "INSERT INTO \"SCOTT\".\"DEPT\" (\"DEPTNO\", \"DNAME\", \"LOC\")\n"
        + "SELECT ?, ?, ?\n"
        + "FROM (VALUES (0)) AS \"t\" (\"ZERO\")\n"
        + "UNION ALL\n"
        + "SELECT ?, ?, ?\n"
        + "FROM (VALUES (0)) AS \"t\" (\"ZERO\")";
    sql(sql)
        .schema(CalciteAssert.SchemaSpec.JDBC_SCOTT)
        .ok(expected);
  }

  @Test void testTableFunctionScan() {
    final String query = "SELECT *\n"
        + "FROM TABLE(DEDUP(CURSOR(select \"product_id\", \"product_name\"\n"
        + "from \"product\"), CURSOR(select  \"employee_id\", \"full_name\"\n"
        + "from \"employee\"), 'NAME'))";

    final String expected = "SELECT *\n"
        + "FROM TABLE(DEDUP(CURSOR ((SELECT \"product_id\", \"product_name\"\n"
        + "FROM \"foodmart\".\"product\")), CURSOR ((SELECT \"employee_id\", \"full_name\"\n"
        + "FROM \"foodmart\".\"employee\")), 'NAME'))";
    sql(query).ok(expected);

    final String query2 = "select * from table(ramp(3))";
    sql(query2).ok("SELECT *\n"
        + "FROM TABLE(RAMP(3))");
  }

  @Test void testTableFunctionScanWithComplexQuery() {
    final String query = "SELECT *\n"
        + "FROM TABLE(DEDUP(CURSOR(select \"product_id\", \"product_name\"\n"
        + "from \"product\"\n"
        + "where \"net_weight\" > 100 and \"product_name\" = 'Hello World')\n"
        + ",CURSOR(select  \"employee_id\", \"full_name\"\n"
        + "from \"employee\"\n"
        + "group by \"employee_id\", \"full_name\"), 'NAME'))";

    final String expected = "SELECT *\n"
        + "FROM TABLE(DEDUP(CURSOR ((SELECT \"product_id\", \"product_name\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE \"net_weight\" > 100 AND \"product_name\" = 'Hello World')), "
        + "CURSOR ((SELECT \"employee_id\", \"full_name\"\n"
        + "FROM \"foodmart\".\"employee\"\n"
        + "GROUP BY \"employee_id\", \"full_name\")), 'NAME'))";
    sql(query).ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3593">[CALCITE-3593]
   * RelToSqlConverter changes target of ambiguous HAVING clause with a Project
   * on Filter on Aggregate</a>. */


  /*@Test void testBigQueryHaving() {
    final String sql = ""
        + "SELECT \"DEPTNO\" - 10 \"DEPT\"\n"
        + "FROM \"EMP\"\n"
        + "GROUP BY \"DEPTNO\"\n"
        + "HAVING \"DEPTNO\" > 0";
    final String expected = ""
        + "SELECT DEPTNO - 10 AS DEPTNO\n"
        + "FROM (SELECT DEPTNO\n"
        + "FROM SCOTT.EMP\n"
        + "GROUP BY DEPTNO\n"
        + "HAVING DEPTNO > 0) AS t1";

    // Parse the input SQL with PostgreSQL dialect,
    // in which "isHavingAlias" is false.
    final SqlParser.Config parserConfig =
        PostgresqlSqlDialect.DEFAULT.configureParser(SqlParser.config());

    // Convert rel node to SQL with BigQuery dialect,
    // in which "isHavingAlias" is true.
    sql(sql)
        .parserConfig(parserConfig)
        .schema(CalciteAssert.SchemaSpec.JDBC_SCOTT)
        .withBigQuery()
        .ok(expected);
  }
*/


  @Test public void testCastToTimestamp() {
    String query = "SELECT cast(\"birth_date\" as TIMESTAMP) "
        + "FROM \"foodmart\".\"employee\"";
    final String expected = "SELECT CAST(birth_date AS TIMESTAMP)\n"
        + "FROM foodmart.employee";
    final String expectedBigQuery = "SELECT CAST(birth_date AS DATETIME)\n"
        + "FROM foodmart.employee";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery);
  }

  @Test public void testCastToTimestampWithPrecision() {
    String query = "SELECT cast(\"birth_date\" as TIMESTAMP(3)) "
        + "FROM \"foodmart\".\"employee\"";
    final String expectedHive = "SELECT CAST(DATE_FORMAT(CAST(birth_date AS TIMESTAMP), "
        + "'yyyy-MM-dd HH:mm:ss.sss') AS TIMESTAMP)\n"
        + "FROM foodmart.employee";
    final String expectedSpark = expectedHive;
    final String expectedBigQuery = "SELECT CAST(FORMAT_TIMESTAMP('%F %H:%M:%E3S', CAST"
        + "(birth_date AS DATETIME)) AS DATETIME)\n"
        + "FROM foodmart.employee";
    sql(query)
        .withHive()
        .ok(expectedHive)
        .withSpark()
        .ok(expectedSpark)
        .withBigQuery()
        .ok(expectedBigQuery);
  }

  @Test public void testCastToTime() {
    String query = "SELECT cast(\"hire_date\" as TIME) "
        + "FROM \"foodmart\".\"employee\"";
    final String expected = "SELECT SPLIT(DATE_FORMAT(hire_date, 'yyyy-MM-dd HH:mm:ss'), ' ')[1]\n"
        + "FROM foodmart.employee";
    final String expectedBigQuery = "SELECT CAST(hire_date AS TIME)\n"
        + "FROM foodmart.employee";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery);
  }

  @Test public void testCastToTimeWithPrecision() {
    String query = "SELECT cast(\"hire_date\" as TIME(5)) "
        + "FROM \"foodmart\".\"employee\"";
    final String expectedHive = "SELECT SPLIT(DATE_FORMAT(hire_date, 'yyyy-MM-dd HH:mm:ss.sss'), "
        + "' ')[1]\n"
        + "FROM foodmart.employee";
    final String expectedSpark = expectedHive;
    final String expectedBigQuery = "SELECT CAST(FORMAT_TIME('%H:%M:%E3S', CAST(hire_date AS TIME))"
        + " AS TIME)\n"
        + "FROM foodmart.employee";
    sql(query)
        .withHive()
        .ok(expectedHive)
        .withSpark()
        .ok(expectedSpark)
        .withBigQuery()
        .ok(expectedBigQuery);
  }

  @Test public void testCastToTimeWithPrecisionWithStringInput() {
    String query = "SELECT cast('12:00'||':05' as TIME(5)) "
        + "FROM \"foodmart\".\"employee\"";
    final String expectedHive = "SELECT CONCAT('12:00', ':05')\n"
        + "FROM foodmart.employee";
    final String expectedSpark = expectedHive;
    final String expectedBigQuery = "SELECT CAST(FORMAT_TIME('%H:%M:%E3S', CAST(CONCAT('12:00', "
        + "':05') AS TIME)) AS TIME)\n"
        + "FROM foodmart.employee";
    final String mssql = "SELECT CAST(CONCAT('12:00', ':05') AS TIME(3))\n"
            + "FROM [foodmart].[employee]";
    sql(query)
        .withHive()
        .ok(expectedHive)
        .withSpark()
        .ok(expectedSpark)
        .withBigQuery()
        .ok(expectedBigQuery)
        .withMssql()
        .ok(mssql);
  }

  @Test public void testCastToTimeWithPrecisionWithStringLiteral() {
    String query = "SELECT cast('12:00:05' as TIME(3)) "
        + "FROM \"foodmart\".\"employee\"";
    final String expectedHive = "SELECT '12:00:05'\n"
        + "FROM foodmart.employee";
    final String expectedSpark = expectedHive;
    final String expectedBigQuery = "SELECT TIME '12:00:05.000'\n"
        + "FROM foodmart.employee";
    sql(query)
        .withHive()
        .ok(expectedHive)
        .withSpark()
        .ok(expectedSpark)
        .withBigQuery()
        .ok(expectedBigQuery);
  }

  @Test public void testFormatDateRelToSql() {
    final RelBuilder builder = relBuilder();
    final RexNode formatDateRexNode = builder.call(SqlLibraryOperators.FORMAT_DATE,
        builder.literal("YYYY-MM-DD"), builder.scan("EMP").field(4));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(formatDateRexNode, "FD"))
        .build();
    final String expectedSql = "SELECT FORMAT_DATE('YYYY-MM-DD', \"HIREDATE\") AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT FORMAT_DATE('%F', HIREDATE) AS FD\n"
        + "FROM scott.EMP";
    final String expectedHive = "SELECT DATE_FORMAT(HIREDATE, 'yyyy-MM-dd') FD\n"
        + "FROM scott.EMP";
    final String expectedSnowFlake = "SELECT TO_VARCHAR(\"HIREDATE\", 'YYYY-MM-DD') AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedSpark = expectedHive;
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.HIVE.getDialect()), isLinux(expectedHive));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedSnowFlake));
  }

  @Test public void testDOMAndDOY() {
    final RelBuilder builder = relBuilder();
    final RexNode dayOfMonthRexNode = builder.call(SqlLibraryOperators.FORMAT_DATE,
            builder.literal("W"), builder.scan("EMP").field(4));
    final RexNode dayOfYearRexNode = builder.call(SqlLibraryOperators.FORMAT_DATE,
            builder.literal("WW"), builder.scan("EMP").field(4));

    final RelNode domRoot = builder
            .scan("EMP")
            .project(builder.alias(dayOfMonthRexNode, "FD"))
            .build();
    final RelNode doyRoot = builder
            .scan("EMP")
            .project(builder.alias(dayOfYearRexNode, "FD"))
            .build();

    final String expectedDOMBiqQuery = "SELECT CAST(CEIL(EXTRACT(DAY "
            + "FROM HIREDATE) / 7) AS STRING) AS FD\n"
            + "FROM scott.EMP";
    final String expectedDOYBiqQuery = "SELECT CAST(CEIL(EXTRACT(DAYOFYEAR "
            + "FROM HIREDATE) / 7) AS STRING) AS FD\n"
            + "FROM scott.EMP";

    assertThat(toSql(doyRoot, DatabaseProduct.BIG_QUERY.getDialect()),
            isLinux(expectedDOYBiqQuery));
    assertThat(toSql(domRoot, DatabaseProduct.BIG_QUERY.getDialect()),
            isLinux(expectedDOMBiqQuery));
  }

  @Test public void testFormatTimestampRelToSql() {
    final RelBuilder builder = relBuilder();
    final RexNode formatTimestampRexNode = builder.call(SqlLibraryOperators.FORMAT_TIMESTAMP,
        builder.literal("YYYY-MM-DD HH:MI:SS.S(5)"), builder.scan("EMP").field(4));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(formatTimestampRexNode, "FD"))
        .build();
    final String expectedSql = "SELECT FORMAT_TIMESTAMP('YYYY-MM-DD HH:MI:SS.S(5)', \"HIREDATE\") "
        + "AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT FORMAT_TIMESTAMP('%F %I:%M:%E5S', HIREDATE) AS FD\n"
        + "FROM scott.EMP";
    final String expectedHive = "SELECT DATE_FORMAT(HIREDATE, 'yyyy-MM-dd hh:mm:ss.sssss') FD\n"
        + "FROM scott.EMP";
    final String expectedSpark = expectedHive;
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.HIVE.getDialect()), isLinux(expectedHive));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
  }

  @Test public void testFormatTimestampFormatsRelToSql() {
    final RelBuilder builder = relBuilder();
    final RexNode formatTimestampRexNode2 = builder.call(SqlLibraryOperators.FORMAT_TIMESTAMP,
        builder.literal("HH24MI"), builder.scan("EMP").field(4));
    final RexNode formatTimestampRexNode3 = builder.call(SqlLibraryOperators.FORMAT_TIMESTAMP,
        builder.literal("HH24MISS"), builder.scan("EMP").field(4));
    final RexNode formatTimestampRexNode4 = builder.call(SqlLibraryOperators.FORMAT_TIMESTAMP,
        builder.literal("YYYYMMDDHH24MISS"), builder.scan("EMP").field(4));
    final RexNode formatTimestampRexNode5 = builder.call(SqlLibraryOperators.FORMAT_TIMESTAMP,
        builder.literal("YYYYMMDDHHMISS"), builder.scan("EMP").field(4));
    final RexNode formatTimestampRexNode6 = builder.call(SqlLibraryOperators.FORMAT_TIMESTAMP,
        builder.literal("YYYYMMDDHH24MI"), builder.scan("EMP").field(4));
    final RexNode formatTimestampRexNode7 = builder.call(SqlLibraryOperators.FORMAT_TIMESTAMP,
        builder.literal("YYYYMMDDHH24"), builder.scan("EMP").field(4));
    final RexNode formatTimestampRexNode8 = builder.call(SqlLibraryOperators.FORMAT_TIMESTAMP,
        builder.literal("MS"), builder.scan("EMP").field(4));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(formatTimestampRexNode2, "FD2"),
            builder.alias(formatTimestampRexNode3, "FD3"),
            builder.alias(formatTimestampRexNode4, "FD4"),
            builder.alias(formatTimestampRexNode5, "FD5"),
            builder.alias(formatTimestampRexNode6, "FD6"),
            builder.alias(formatTimestampRexNode7, "FD7"),
            builder.alias(formatTimestampRexNode8, "FD8"))
        .build();
    final String expectedSql = "SELECT FORMAT_TIMESTAMP('HH24MI', \"HIREDATE\") AS \"FD2\", "
        + "FORMAT_TIMESTAMP('HH24MISS', \"HIREDATE\") AS \"FD3\", "
        + "FORMAT_TIMESTAMP('YYYYMMDDHH24MISS', \"HIREDATE\") AS \"FD4\", "
        + "FORMAT_TIMESTAMP('YYYYMMDDHHMISS', \"HIREDATE\") AS \"FD5\", FORMAT_TIMESTAMP"
        + "('YYYYMMDDHH24MI', \"HIREDATE\") AS \"FD6\", FORMAT_TIMESTAMP('YYYYMMDDHH24', "
        + "\"HIREDATE\") AS \"FD7\", FORMAT_TIMESTAMP('MS', \"HIREDATE\") AS \"FD8\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT FORMAT_TIMESTAMP('%H%M', HIREDATE) AS FD2, "
        + "FORMAT_TIMESTAMP('%H%M%S', HIREDATE) AS FD3, FORMAT_TIMESTAMP('%Y%m%d%H%M%S', "
        + "HIREDATE) AS FD4, FORMAT_TIMESTAMP('%Y%m%d%I%M%S', HIREDATE) AS FD5, FORMAT_TIMESTAMP"
        + "('%Y%m%d%H%M', HIREDATE) AS FD6, FORMAT_TIMESTAMP('%Y%m%d%H', HIREDATE) AS FD7, "
        + "FORMAT_TIMESTAMP('%E', HIREDATE) AS FD8\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testFormatTimeRelToSql() {
    final RelBuilder builder = relBuilder();
    final RexNode formatTimeRexNode = builder.call(SqlLibraryOperators.FORMAT_TIME,
        builder.literal("HH:MI:SS"), builder.scan("EMP").field(4));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(formatTimeRexNode, "FD"))
        .build();
    final String expectedSql = "SELECT FORMAT_TIME('HH:MI:SS', \"HIREDATE\") AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT FORMAT_TIME('%I:%M:%S', HIREDATE) AS FD\n"
        + "FROM scott.EMP";
    final String expectedHive = "SELECT DATE_FORMAT(HIREDATE, 'hh:mm:ss') FD\n"
        + "FROM scott.EMP";
    final String expectedSpark = expectedHive;
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.HIVE.getDialect()), isLinux(expectedHive));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
  }

  @Test public void testStrToDateRelToSql() {
    final RelBuilder builder = relBuilder();
    final RexNode strToDateNode1 = builder.call(SqlLibraryOperators.STR_TO_DATE,
        builder.literal("20181106"), builder.literal("YYYYMMDD"));
    final RexNode strToDateNode2 = builder.call(SqlLibraryOperators.STR_TO_DATE,
        builder.literal("2018/11/06"), builder.literal("YYYY/MM/DD"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(strToDateNode1, "date1"), builder.alias(strToDateNode2, "date2"))
        .build();
    final String expectedSql = "SELECT STR_TO_DATE('20181106', 'YYYYMMDD') AS \"date1\", "
        + "STR_TO_DATE('2018/11/06', 'YYYY/MM/DD') AS \"date2\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT PARSE_DATE('%Y%m%d', '20181106') AS date1, "
        + "PARSE_DATE('%Y/%m/%d', '2018/11/06') AS date2\n"
        + "FROM scott.EMP";
    final String expectedHive = "SELECT CAST(FROM_UNIXTIME("
        + "UNIX_TIMESTAMP('20181106', 'yyyyMMdd'), 'yyyy-MM-dd') AS DATE) date1, "
        + "CAST(FROM_UNIXTIME(UNIX_TIMESTAMP('2018/11/06', 'yyyy/MM/dd'), 'yyyy-MM-dd') AS DATE) date2\n"
        + "FROM scott.EMP";
    final String expectedSpark = expectedHive;
    final String expectedSnowflake =
        "SELECT TO_DATE('20181106', 'YYYYMMDD') AS \"date1\", "
        + "TO_DATE('2018/11/06', 'YYYY/MM/DD') AS \"date2\"\n"
        + "FROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.HIVE.getDialect()), isLinux(expectedHive));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedSnowflake));
  }

  @Test public void testFormatDatetimeRelToSql() {
    final RelBuilder builder = relBuilder();
    final RexNode formatDateNode1 = builder.call(SqlLibraryOperators.FORMAT_DATETIME,
            builder.literal("DDMMYY"), builder.literal("2008-12-25 15:30:00"));
    final RexNode formatDateNode2 = builder.call(SqlLibraryOperators.FORMAT_DATETIME,
            builder.literal("YY/MM/DD"), builder.literal("2012-12-25 12:50:10"));
    final RexNode formatDateNode3 = builder.call(SqlLibraryOperators.FORMAT_DATETIME,
        builder.literal("YY-MM-01"), builder.literal("2012-12-25 12:50:10"));
    final RexNode formatDateNode4 = builder.call(SqlLibraryOperators.FORMAT_DATETIME,
        builder.literal("YY-MM-DD 00:00:00"), builder.literal("2012-12-25 12:50:10"));
    final RelNode root = builder
            .scan("EMP")
            .project(builder.alias(formatDateNode1, "date1"),
                    builder.alias(formatDateNode2, "date2"),
                    builder.alias(formatDateNode3, "date3"),
                    builder.alias(formatDateNode4, "date4"))
            .build();
    final String expectedSql = "SELECT FORMAT_DATETIME('DDMMYY', '2008-12-25 15:30:00') AS "
            + "\"date1\", FORMAT_DATETIME('YY/MM/DD', '2012-12-25 12:50:10') AS \"date2\", "
            + "FORMAT_DATETIME('YY-MM-01', '2012-12-25 12:50:10') AS \"date3\", FORMAT_DATETIME"
            + "('YY-MM-DD 00:00:00', '2012-12-25 12:50:10') AS \"date4\"\n"
            + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT FORMAT_DATETIME('%d%m%y', '2008-12-25 15:30:00') "
            + "AS date1, FORMAT_DATETIME('%y/%m/%d', '2012-12-25 12:50:10') AS date2,"
            + " FORMAT_DATETIME('%y-%m-01', '2012-12-25 12:50:10') AS date3,"
            + " FORMAT_DATETIME('%y-%m-%d 00:00:00', '2012-12-25 12:50:10') AS date4\n"
            + "FROM scott.EMP";
    final String expectedSpark = "SELECT DATE_FORMAT('2008-12-25 15:30:00', 'ddMMyy') date1, "
            + "DATE_FORMAT('2012-12-25 12:50:10', 'yy/MM/dd') date2,"
            + " DATE_FORMAT('2012-12-25 12:50:10', 'yy-MM-01') date3,"
            + " DATE_FORMAT('2012-12-25 12:50:10', 'yy-MM-dd 00:00:00') date4\n"
            + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
  }

  @Test public void testParseTimestampFunctionFormat() {
    final RelBuilder builder = relBuilder();
    final RexNode parseTSNode1 = builder.call(SqlLibraryOperators.PARSE_TIMESTAMP,
        builder.literal("YYYY-MM-dd HH24:MI:SS"), builder.literal("2009-03-20 12:25:50"));
    final RexNode parseTSNode2 = builder.call(SqlLibraryOperators.PARSE_TIMESTAMP,
        builder.literal("MI dd-YYYY-MM SS HH24"), builder.literal("25 20-2009-03 50 12"));
    final RexNode parseTSNode3 = builder.call(SqlLibraryOperators.PARSE_TIMESTAMP,
        builder.literal("yyyy@MM@dd@hh@mm@ss"), builder.literal("20200903020211"));
    final RexNode parseTSNode4 = builder.call(SqlLibraryOperators.PARSE_TIMESTAMP,
        builder.literal("yyyy@MM@dd@HH@mm@ss"), builder.literal("20200903210211"));
    final RexNode parseTSNode5 = builder.call(SqlLibraryOperators.PARSE_TIMESTAMP,
        builder.literal("HH@mm@ss"), builder.literal("215313"));
    final RexNode parseTSNode6 = builder.call(SqlLibraryOperators.PARSE_TIMESTAMP,
        builder.literal("MM@dd@yy"), builder.literal("090415"));
    final RexNode parseTSNode7 = builder.call(SqlLibraryOperators.PARSE_TIMESTAMP,
        builder.literal("MM@dd@yy"), builder.literal("Jun1215"));
    final RexNode parseTSNode8 = builder.call(SqlLibraryOperators.PARSE_TIMESTAMP,
        builder.literal("yyyy@MM@dd@HH"), builder.literal("2015061221"));
    final RexNode parseTSNode9 = builder.call(SqlLibraryOperators.PARSE_TIMESTAMP,
        builder.literal("yyyy@dd@mm"), builder.literal("20150653"));
    final RexNode parseTSNode10 = builder.call(SqlLibraryOperators.PARSE_TIMESTAMP,
        builder.literal("yyyy@mm@dd"), builder.literal("20155308"));
    final RexNode parseTSNode11 = builder.call(SqlLibraryOperators.PARSE_TIMESTAMP,
        builder.literal("YYYY-MM-dd@HH:mm:ss"), builder.literal("2009-03-2021:25:50"));
    final RexNode parseTSNode12 = builder.call(SqlLibraryOperators.PARSE_TIMESTAMP,
        builder.literal("YYYY-MM-dd@hh:mm:ss"), builder.literal("2009-03-2007:25:50"));
    final RexNode parseTSNode13 = builder.call(SqlLibraryOperators.PARSE_TIMESTAMP,
        builder.literal("YYYY-MM-dd@hh:mm:ss z"), builder.literal("2009-03-20 12:25:50.222"));
    final RexNode parseTSNode14 = builder.call(SqlLibraryOperators.PARSE_TIMESTAMP,
        builder.literal("YYYY-MM-dd'T'hh:mm:ss"), builder.literal("2012-05-09T04:12:12"));
    final RexNode parseTSNode15 = builder.call(SqlLibraryOperators.PARSE_TIMESTAMP,
        builder.literal("yyyy- MM-dd  HH: -mm:ss"), builder.literal("2015- 09-11  09: -07:23"));
    final RexNode parseTSNode16 = builder.call(SqlLibraryOperators.PARSE_TIMESTAMP,
        builder.literal("yyyy- MM-dd@HH: -mm:ss"), builder.literal("2015- 09-1109: -07:23"));
    final RexNode parseTSNode17 = builder.call(SqlLibraryOperators.PARSE_TIMESTAMP,
        builder.literal("yyyy-MM-dd-HH:mm:ss.S(3)@ZZ"), builder.literal("2015-09-11-09:07:23"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(parseTSNode1, "date1"), builder.alias(parseTSNode2, "date2"),
            builder.alias(parseTSNode3, "timestamp1"), builder.alias(parseTSNode4, "timestamp2"),
            builder.alias(parseTSNode5, "time1"), builder.alias(parseTSNode6, "date1"),
            builder.alias(parseTSNode7, "date2"), builder.alias(parseTSNode8, "date3"),
            builder.alias(parseTSNode9, "date5"),
            builder.alias(parseTSNode10, "date6"), builder.alias(parseTSNode11, "timestamp3"),
            builder.alias(parseTSNode12, "timestamp4"), builder.alias(parseTSNode13, "timestamp5"),
            builder.alias(parseTSNode14, "timestamp6"), builder.alias(parseTSNode15, "timestamp7"),
            builder.alias(parseTSNode16, "timestamp8"), builder.alias(parseTSNode17, "timestamp9"))
        .build();
    final String expectedSql =
        "SELECT PARSE_TIMESTAMP('YYYY-MM-dd HH24:MI:SS', '2009-03-20 12:25:50') AS \"date1\","
            + " PARSE_TIMESTAMP('MI dd-YYYY-MM SS HH24', '25 20-2009-03 50 12') AS \"date2\","
            + " PARSE_TIMESTAMP('yyyy@MM@dd@hh@mm@ss', '20200903020211') AS \"timestamp1\","
            + " PARSE_TIMESTAMP('yyyy@MM@dd@HH@mm@ss', '20200903210211') AS \"timestamp2\","
            + " PARSE_TIMESTAMP('HH@mm@ss', '215313') AS \"time1\", "
            + "PARSE_TIMESTAMP('MM@dd@yy', '090415') AS \"date10\", "
            + "PARSE_TIMESTAMP('MM@dd@yy', 'Jun1215') AS \"date20\", "
            + "PARSE_TIMESTAMP('yyyy@MM@dd@HH', '2015061221') AS \"date3\", "
            + "PARSE_TIMESTAMP('yyyy@dd@mm', '20150653') AS \"date5\", "
            + "PARSE_TIMESTAMP('yyyy@mm@dd', '20155308') AS \"date6\", "
            + "PARSE_TIMESTAMP('YYYY-MM-dd@HH:mm:ss', '2009-03-2021:25:50') AS \"timestamp3\", "
            + "PARSE_TIMESTAMP('YYYY-MM-dd@hh:mm:ss', '2009-03-2007:25:50') AS \"timestamp4\", "
            + "PARSE_TIMESTAMP('YYYY-MM-dd@hh:mm:ss z', '2009-03-20 12:25:50.222') AS \"timestamp5\", "
            + "PARSE_TIMESTAMP('YYYY-MM-dd''T''hh:mm:ss', '2012-05-09T04:12:12') AS \"timestamp6\""
            + ", PARSE_TIMESTAMP('yyyy- MM-dd  HH: -mm:ss', '2015- 09-11  09: -07:23') AS \"timestamp7\""
            + ", PARSE_TIMESTAMP('yyyy- MM-dd@HH: -mm:ss', '2015- 09-1109: -07:23') AS \"timestamp8\""
            + ", PARSE_TIMESTAMP('yyyy-MM-dd-HH:mm:ss.S(3)@ZZ', '2015-09-11-09:07:23') AS \"timestamp9\"\n"
            + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery =
        "SELECT PARSE_DATETIME('%F %H:%M:%S', '2009-03-20 12:25:50') AS date1,"
            + " PARSE_DATETIME('%M %d-%Y-%m %S %H', '25 20-2009-03 50 12') AS date2,"
            + " PARSE_DATETIME('%Y%m%d%I%m%S', '20200903020211') AS timestamp1,"
            + " PARSE_DATETIME('%Y%m%d%I%m%S', '20200903210211') AS timestamp2,"
            + " PARSE_DATETIME('%I%m%S', '215313') AS time1,"
            + " PARSE_DATETIME('%m%d%y', '090415') AS date10,"
            + " PARSE_DATETIME('%m%d%y', 'Jun1215') AS date20,"
            + " PARSE_DATETIME('%Y%m%d%I', '2015061221') AS date3,"
            + " PARSE_DATETIME('%Y%d%m', '20150653') AS date5,"
            + " PARSE_DATETIME('%Y%m%d', '20155308') AS date6,"
            + " PARSE_DATETIME('%F%I:%m:%S', '2009-03-2021:25:50') AS timestamp3,"
            + " PARSE_DATETIME('%F%I:%m:%S', '2009-03-2007:25:50') AS timestamp4, "
            + "PARSE_DATETIME('%F%I:%m:%S %Z', '2009-03-20 12:25:50.222') AS timestamp5, "
            + "PARSE_DATETIME('%FT%I:%m:%S', '2012-05-09T04:12:12') AS timestamp6,"
            + " PARSE_DATETIME('%Y- %m-%d  %I: -%m:%S', '2015- 09-11  09: -07:23') AS timestamp7,"
            + " PARSE_DATETIME('%Y- %m-%d%I: -%m:%S', '2015- 09-1109: -07:23') AS timestamp8,"
            + " PARSE_DATETIME('%F-%I:%m:%E3S%Ez', '2015-09-11-09:07:23') AS timestamp9\n"
            + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testToTimestampFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode parseTSNode1 = builder.call(SqlLibraryOperators.TO_TIMESTAMP,
        builder.literal("2009-03-20 12:25:50"), builder.literal("yyyy-MM-dd HH24:MI:SS"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(parseTSNode1, "timestamp_value"))
        .build();
    final String expectedSql =
        "SELECT TO_TIMESTAMP('2009-03-20 12:25:50', 'yyyy-MM-dd HH24:MI:SS') AS "
            + "\"timestamp_value\"\nFROM \"scott\".\"EMP\"";
    final String expectedBiqQuery =
        "SELECT PARSE_DATETIME('%F %H:%M:%S', '2009-03-20 12:25:50') AS timestamp_value\n"
            + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void toTimestampFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode parseTSNode1 = builder.call(SqlLibraryOperators.TO_TIMESTAMP,
        builder.literal("Jan 15, 1989, 11:00:06 AM"), builder.literal("MMM dd, YYYY,HH:MI:SS AM"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(parseTSNode1, "timestamp_value"))
        .build();
    final String expectedSql =
        "SELECT TO_TIMESTAMP('Jan 15, 1989, 11:00:06 AM', 'MMM dd, YYYY,HH:MI:SS AM') AS "
        + "\"timestamp_value\"\nFROM \"scott\".\"EMP\"";
    final String expectedSF =
        "SELECT TO_TIMESTAMP('Jan 15, 1989, 11:00:06 AM' , 'MON DD, YYYY,HH:MI:SS AM') AS "
        + "\"timestamp_value\"\nFROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedSF));
  }

  @Test public void datediffFunctionWithTwoOperands() {
    final RelBuilder builder = relBuilder();
    final RexNode parseTSNode1 = builder.call(SqlLibraryOperators.DATE_DIFF,
        builder.literal("1994-07-21"), builder.literal("1993-07-21"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(parseTSNode1, "date_diff_value"))
        .build();
    final String expectedSql =
        "SELECT DATE_DIFF('1994-07-21', '1993-07-21') AS \"date_diff_value\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBQ =
        "SELECT DATE_DIFF('1994-07-21', '1993-07-21') AS date_diff_value\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQ));
  }

  @Test public void datediffFunctionWithThreeOperands() {
    final RelBuilder builder = relBuilder();
    final RexNode parseTSNode1 = builder.call(SqlLibraryOperators.DATE_DIFF,
        builder.literal("1994-07-21"), builder.literal("1993-07-21"), builder.literal("Month"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(parseTSNode1, "date_diff_value"))
        .build();
    final String expectedSql =
        "SELECT DATE_DIFF('1994-07-21', '1993-07-21', 'Month') AS \"date_diff_value\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBQ =
        "SELECT DATE_DIFF('1994-07-21', '1993-07-21', Month) AS date_diff_value\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQ));
  }

  @Test public void testToDateFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode parseTSNode1 = builder.call(SqlLibraryOperators.TO_DATE,
        builder.literal("2009/03/20"), builder.literal("yyyy/MM/dd"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(parseTSNode1, "date_value"))
        .build();
    final String expectedSql =
        "SELECT TO_DATE('2009/03/20', 'yyyy/MM/dd') AS \"date_value\"\n"
            + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery =
        "SELECT DATE(PARSE_DATETIME('%Y/%m/%d', '2009/03/20')) AS date_value\n"
            + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  /** Fluid interface to run tests. */
  static class Sql {
    private final SchemaPlus schema;
    private final String sql;
    private final SqlDialect dialect;
    private final Function<RelBuilder, RelNode> relFn;
    private final List<Function<RelNode, RelNode>> transforms;
    private final SqlParser.Config parserConfig;
    private final UnaryOperator<SqlToRelConverter.Config> config;

    Sql(CalciteAssert.SchemaSpec schemaSpec, String sql, SqlDialect dialect,
        SqlParser.Config parserConfig,
        UnaryOperator<SqlToRelConverter.Config> config,
        Function<RelBuilder, RelNode> relFn,
        List<Function<RelNode, RelNode>> transforms) {
      final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
      this.schema = CalciteAssert.addSchema(rootSchema, schemaSpec);
      this.sql = sql;
      this.dialect = dialect;
      this.relFn = relFn;
      this.transforms = ImmutableList.copyOf(transforms);
      this.parserConfig = parserConfig;
      this.config = config;
    }

    Sql(SchemaPlus schema, String sql, SqlDialect dialect,
        SqlParser.Config parserConfig,
        UnaryOperator<SqlToRelConverter.Config> config,
        Function<RelBuilder, RelNode> relFn,
        List<Function<RelNode, RelNode>> transforms) {
      this.schema = schema;
      this.sql = sql;
      this.dialect = dialect;
      this.relFn = relFn;
      this.transforms = ImmutableList.copyOf(transforms);
      this.parserConfig = parserConfig;
      this.config = config;
    }

    Sql dialect(SqlDialect dialect) {
      return new Sql(schema, sql, dialect, parserConfig, config, relFn,
          transforms);
    }

    Sql relFn(Function<RelBuilder, RelNode> relFn) {
      return new Sql(schema, sql, dialect, parserConfig, config, relFn,
          transforms);
    }

    Sql withCalcite() {
      return dialect(SqlDialect.DatabaseProduct.CALCITE.getDialect());
    }

    Sql withClickHouse() {
      return dialect(SqlDialect.DatabaseProduct.CLICKHOUSE.getDialect());
    }

    Sql withDb2() {
      return dialect(SqlDialect.DatabaseProduct.DB2.getDialect());
    }

    Sql withHive() {
      return dialect(SqlDialect.DatabaseProduct.HIVE.getDialect());
    }

    Sql withHive2() {
      return dialect(
          new HiveSqlDialect(HiveSqlDialect.DEFAULT_CONTEXT
              .withDatabaseMajorVersion(2)
              .withDatabaseMinorVersion(1)
              .withNullCollation(NullCollation.LOW)));
    }


    Sql withHsqldb() {
      return dialect(SqlDialect.DatabaseProduct.HSQLDB.getDialect());
    }

    Sql withMssql() {
      return withMssql(14); // MSSQL 2008 = 10.0, 2012 = 11.0, 2017 = 14.0
    }

    Sql withMssql(int majorVersion) {
      final SqlDialect mssqlDialect = DatabaseProduct.MSSQL.getDialect();
      return dialect(
          new MssqlSqlDialect(MssqlSqlDialect.DEFAULT_CONTEXT
              .withDatabaseMajorVersion(majorVersion)
              .withIdentifierQuoteString(mssqlDialect.quoteIdentifier("")
                  .substring(0, 1))
              .withNullCollation(mssqlDialect.getNullCollation())));
    }

    Sql withMysql() {
      return dialect(SqlDialect.DatabaseProduct.MYSQL.getDialect());
    }

    Sql withMysql8() {
      final SqlDialect mysqlDialect = DatabaseProduct.MYSQL.getDialect();
      return dialect(
          new SqlDialect(MysqlSqlDialect.DEFAULT_CONTEXT
              .withDatabaseMajorVersion(8)
              .withIdentifierQuoteString(mysqlDialect.quoteIdentifier("")
                  .substring(0, 1))
              .withNullCollation(mysqlDialect.getNullCollation())));
    }

    Sql withOracle() {
      return dialect(SqlDialect.DatabaseProduct.ORACLE.getDialect());
    }

    Sql withPostgresql() {
      return dialect(SqlDialect.DatabaseProduct.POSTGRESQL.getDialect());
    }

    Sql withPresto() {
      return dialect(DatabaseProduct.PRESTO.getDialect());
    }

    Sql withRedshift() {
      return dialect(DatabaseProduct.REDSHIFT.getDialect());
    }

    Sql withSnowflake() {
      return dialect(DatabaseProduct.SNOWFLAKE.getDialect());
    }

    Sql withSybase() {
      return dialect(DatabaseProduct.SYBASE.getDialect());
    }

    Sql withVertica() {
      return dialect(SqlDialect.DatabaseProduct.VERTICA.getDialect());
    }

    Sql withBigQuery() {
      return dialect(SqlDialect.DatabaseProduct.BIG_QUERY.getDialect());
    }

    Sql withSpark() {
      return dialect(DatabaseProduct.SPARK.getDialect());
    }

    Sql withHiveIdentifierQuoteString() {
      final HiveSqlDialect hiveSqlDialect =
          new HiveSqlDialect((SqlDialect.EMPTY_CONTEXT)
              .withDatabaseProduct(DatabaseProduct.HIVE)
              .withIdentifierQuoteString("`"));
      return dialect(hiveSqlDialect);
    }

    Sql withSparkIdentifierQuoteString() {
      final SparkSqlDialect sparkSqlDialect =
          new SparkSqlDialect((SqlDialect.EMPTY_CONTEXT)
              .withDatabaseProduct(DatabaseProduct.SPARK)
              .withIdentifierQuoteString("`"));
      return dialect(sparkSqlDialect);
    }

    Sql withPostgresqlModifiedTypeSystem() {
      // Postgresql dialect with max length for varchar set to 256
      final PostgresqlSqlDialect postgresqlSqlDialect =
          new PostgresqlSqlDialect(PostgresqlSqlDialect.DEFAULT_CONTEXT
              .withDataTypeSystem(new RelDataTypeSystemImpl() {
                @Override public int getMaxPrecision(SqlTypeName typeName) {
                  switch (typeName) {
                  case VARCHAR:
                    return 256;
                  default:
                    return super.getMaxPrecision(typeName);
                  }
                }
              }));
      return dialect(postgresqlSqlDialect);
    }

    Sql withOracleModifiedTypeSystem() {
      // Oracle dialect with max length for varchar set to 512
      final OracleSqlDialect oracleSqlDialect =
          new OracleSqlDialect(OracleSqlDialect.DEFAULT_CONTEXT
              .withDataTypeSystem(new RelDataTypeSystemImpl() {
                @Override public int getMaxPrecision(SqlTypeName typeName) {
                  switch (typeName) {
                  case VARCHAR:
                    return 512;
                  default:
                    return super.getMaxPrecision(typeName);
                  }
                }
              }));
      return dialect(oracleSqlDialect);
    }

    Sql parserConfig(SqlParser.Config parserConfig) {
      return new Sql(schema, sql, dialect, parserConfig, config, relFn,
          transforms);
    }

    Sql withConfig(UnaryOperator<SqlToRelConverter.Config> config) {
      return new Sql(schema, sql, dialect, parserConfig, config, relFn,
          transforms);
    }

    Sql optimize(final RuleSet ruleSet, final RelOptPlanner relOptPlanner) {
      return new Sql(schema, sql, dialect, parserConfig, config, relFn,
          FlatLists.append(transforms, r -> {
            Program program = Programs.of(ruleSet);
            final RelOptPlanner p =
                Util.first(relOptPlanner,
                    new HepPlanner(
                        new HepProgramBuilder().addRuleClass(RelOptRule.class)
                            .build()));
            return program.run(p, r, r.getTraitSet(),
                ImmutableList.of(), ImmutableList.of());
          }));
    }

    Sql ok(String expectedQuery) {
      assertThat(exec(), isLinux(expectedQuery));
      return this;
    }

    Sql throws_(String errorMessage) {
      try {
        final String s = exec();
        throw new AssertionError("Expected exception with message `"
            + errorMessage + "` but nothing was thrown; got " + s);
      } catch (Exception e) {
        assertThat(e.getMessage(), is(errorMessage));
        return this;
      }
    }

    String exec() {
      try {
        RelNode rel;
        if (relFn != null) {
          rel = relFn.apply(relBuilder());
        } else {
          final SqlToRelConverter.Config config = this.config.apply(SqlToRelConverter.config()
              .withTrimUnusedFields(false));
          final Planner planner =
              getPlanner(null, parserConfig, schema, config);
          SqlNode parse = planner.parse(sql);
          SqlNode validate = planner.validate(parse);
          rel = planner.rel(validate).rel;
        }
        for (Function<RelNode, RelNode> transform : transforms) {
          rel = transform.apply(rel);
        }
        return toSql(rel, dialect);
      } catch (Exception e) {
        throw TestUtil.rethrow(e);
      }
    }

    public Sql schema(CalciteAssert.SchemaSpec schemaSpec) {
      return new Sql(schemaSpec, sql, dialect, parserConfig, config, relFn,
          transforms);
    }
  }

  @Test public void testIsNotTrueWithEqualCondition() {
    final String query = "select \"product_name\" from \"product\" where "
        + "\"product_name\" = 'Hello World' is not true";
    final String bigQueryExpected = "SELECT product_name\n"
        + "FROM foodmart.product\n"
        + "WHERE product_name <> 'Hello World'";
    sql(query)
        .withBigQuery()
        .ok(bigQueryExpected);
  }

  @Test public void testCoalseceWithCast() {
    final String query = "Select coalesce(cast('2099-12-31 00:00:00.123' as TIMESTAMP),\n"
            + "cast('2010-12-31 01:00:00.123' as TIMESTAMP))";
    final String expectedHive = "SELECT TIMESTAMP '2099-12-31 00:00:00'";
    final String expectedSpark = "SELECT TIMESTAMP '2099-12-31 00:00:00'";
    final String bigQueryExpected = "SELECT CAST('2099-12-31 00:00:00' AS DATETIME)";
    sql(query)
            .withHive()
            .ok(expectedHive)
            .withSpark()
            .ok(expectedSpark)
            .withBigQuery()
            .ok(bigQueryExpected);
  }

  @Test public void testCoalseceWithLiteral() {
    final String query = "Select coalesce('abc','xyz')";
    final String expectedHive = "SELECT 'abc'";
    final String expectedSpark = "SELECT 'abc'";
    final String bigQueryExpected = "SELECT 'abc'";
    sql(query)
            .withHive()
            .ok(expectedHive)
            .withSpark()
            .ok(expectedSpark)
            .withBigQuery()
            .ok(bigQueryExpected);
  }
  @Test public void testCoalseceWithNull() {
    final String query = "Select coalesce(null, 'abc')";
    final String expectedHive = "SELECT 'abc'";
    final String expectedSpark = "SELECT 'abc'";
    final String bigQueryExpected = "SELECT 'abc'";
    sql(query)
            .withHive()
            .ok(expectedHive)
            .withSpark()
            .ok(expectedSpark)
            .withBigQuery()
            .ok(bigQueryExpected);
  }

  @Test public void testLog10Function() {
    final String query = "SELECT LOG10(2) as dd";
    final String expectedSnowFlake = "SELECT LOG(10, 2) AS \"DD\"";
    sql(query)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testLog10ForOne() {
    final String query = "SELECT LOG10(1) as dd";
    final String expectedSnowFlake = "SELECT 0 AS \"DD\"";
    sql(query)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testLog10ForColumn() {
    final String query = "SELECT LOG10(\"product_id\") as dd from \"product\"";
    final String expectedSnowFlake = "SELECT LOG(10, \"product_id\") AS \"DD\"\n"
                      + "FROM \"foodmart\".\"product\"";
    sql(query)
        .withSnowflake()
        .ok(expectedSnowFlake);
  }

  @Test public void testDivideIntegerSnowflake() {
    final RelBuilder builder = relBuilder();
    final RexNode intdivideRexNode = builder.call(SqlStdOperatorTable.DIVIDE_INTEGER,
            builder.scan("EMP").field(0), builder.scan("EMP").field(3));
    final RelNode root = builder
            .scan("EMP")
            .project(builder.alias(intdivideRexNode, "a"))
            .build();
    final String expectedSql = "SELECT \"EMPNO\" /INT \"MGR\" AS \"a\"\n"
            + "FROM \"scott\".\"EMP\"";
    final String expectedSF = "SELECT FLOOR(\"EMPNO\" / \"MGR\") AS \"a\"\n"
            + "FROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedSF));
  }

  @Test public void testRoundFunctionWithColumnPlaceHandling() {
    final String query = "SELECT ROUND(123.41445, \"product_id\") AS \"a\"\n"
            + "FROM \"foodmart\".\"product\"";
    final String expectedBq = "SELECT ROUND(123.41445, product_id) AS a\nFROM foodmart.product";
    final String expected = "SELECT ROUND(123.41445, product_id) a\n"
            + "FROM foodmart.product";
    final String expectedSnowFlake = "SELECT TO_DECIMAL(ROUND(123.41445, "
            + "CASE WHEN \"product_id\" > 38 THEN 38 WHEN \"product_id\" < -12 "
            + "THEN -12 ELSE \"product_id\" END) ,38, 4) AS \"a\"\n"
            + "FROM \"foodmart\".\"product\"";
    final String expectedMssql = "SELECT ROUND(123.41445, [product_id]) AS [a]\n"
            + "FROM [foodmart].[product]";
    sql(query)
            .withBigQuery()
            .ok(expectedBq)
            .withHive()
            .ok(expected)
            .withSpark()
            .ok(expected)
            .withSnowflake()
            .ok(expectedSnowFlake)
            .withMssql()
            .ok(expectedMssql);
  }

  @Test public void testRoundFunctionWithOneParameter() {
    final String query = "SELECT ROUND(123.41445) AS \"a\"\n"
            + "FROM \"foodmart\".\"product\"";
    final String expectedMssql = "SELECT ROUND(123.41445, 0) AS [a]\n"
            + "FROM [foodmart].[product]";
    sql(query)
            .withMssql()
            .ok(expectedMssql);
  }

  @Test public void testTruncateFunctionWithColumnPlaceHandling() {
    String query = "select truncate(2.30259, \"employee_id\") from \"employee\"";
    final String expectedBigQuery = "SELECT TRUNC(2.30259, employee_id)\n"
            + "FROM foodmart.employee";
    final String expectedSnowFlake = "SELECT TRUNCATE(2.30259, CASE WHEN \"employee_id\" > 38"
            + " THEN 38 WHEN \"employee_id\" < -12 THEN -12 ELSE \"employee_id\" END)\n"
            + "FROM \"foodmart\".\"employee\"";
    final String expectedMssql = "SELECT ROUND(2.30259, [employee_id])"
            + "\nFROM [foodmart].[employee]";
    sql(query)
            .withBigQuery()
            .ok(expectedBigQuery)
            .withSnowflake()
            .ok(expectedSnowFlake)
            .withMssql()
            .ok(expectedMssql);
  }

  @Test public void testTruncateFunctionWithOneParameter() {
    String query = "select truncate(2.30259) from \"employee\"";
    final String expectedMssql = "SELECT ROUND(2.30259, 0)"
            + "\nFROM [foodmart].[employee]";
    sql(query)
            .withMssql()
            .ok(expectedMssql);
  }

  @Test public void testWindowFunctionWithOrderByWithoutcolumn() {
    String query = "Select count(*) over() from \"employee\"";
    final String expectedSnowflake = "SELECT COUNT(*) OVER (ORDER BY 0 ROWS BETWEEN UNBOUNDED "
            + "PRECEDING AND UNBOUNDED FOLLOWING)\n"
            + "FROM \"foodmart\".\"employee\"";
    final String mssql = "SELECT COUNT(*) OVER ()\n"
            + "FROM [foodmart].[employee]";
    sql(query)
            .withSnowflake()
            .ok(expectedSnowflake)
            .withMssql()
            .ok(mssql);
  }

  @Test public void testWindowFunctionWithOrderByWithcolumn() {
    String query = "select count(\"employee_id\") over () as a from \"employee\"";
    final String expectedSnowflake = "SELECT COUNT(\"employee_id\") OVER (ORDER BY \"employee_id\" "
            + "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS \"A\"\n"
            + "FROM \"foodmart\".\"employee\"";
    sql(query)
            .withSnowflake()
            .ok(expectedSnowflake);
  }

  @Test public void testRoundFunction() {
    final String query = "SELECT ROUND(123.41445, \"product_id\") AS \"a\"\n"
            + "FROM \"foodmart\".\"product\"";
    final String expectedSnowFlake = "SELECT TO_DECIMAL(ROUND(123.41445, CASE "
            + "WHEN \"product_id\" > 38 THEN 38 WHEN \"product_id\" < -12 THEN -12 "
            + "ELSE \"product_id\" END) ,38, 4) AS \"a\"\n"
            + "FROM \"foodmart\".\"product\"";
    sql(query)
            .withSnowflake()
            .ok(expectedSnowFlake);
  }

  @Test public void testRandomFunction() {
    String query = "select rand_integer(1,3) from \"employee\"";
    final String expectedSnowFlake = "SELECT UNIFORM(1, 3, RANDOM())\n"
            + "FROM \"foodmart\".\"employee\"";
    final String expectedHive = "SELECT FLOOR(RAND() * (3 - 1 + 1)) + 1\n"
            + "FROM foodmart.employee";
    final String expectedBQ = "SELECT FLOOR(RAND() * (3 - 1 + 1)) + 1\n"
            + "FROM foodmart.employee";
    final String expectedSpark = "SELECT FLOOR(RAND() * (3 - 1 + 1)) + 1\n"
            + "FROM foodmart.employee";
    sql(query)
            .withHive()
            .ok(expectedHive)
            .withSpark()
            .ok(expectedSpark)
            .withBigQuery()
            .ok(expectedBQ)
            .withSnowflake()
            .ok(expectedSnowFlake);
  }

  @Test public void testCaseExprForE4() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RexNode condition = builder.call(SqlLibraryOperators.FORMAT_DATE,
            builder.literal("E4"), builder.field("HIREDATE"));
    final RelNode root = relBuilder().scan("EMP").filter(condition).build();
    final String expectedSF = "SELECT *\n"
            + "FROM \"scott\".\"EMP\"\n"
            + "WHERE CASE WHEN TO_VARCHAR(\"HIREDATE\", 'DY') = 'Sun' "
            + "THEN 'Sunday' WHEN TO_VARCHAR(\"HIREDATE\", 'DY') = 'Mon' "
            + "THEN 'Monday' WHEN TO_VARCHAR(\"HIREDATE\", 'DY') = 'Tue' "
            + "THEN 'Tuesday' WHEN TO_VARCHAR(\"HIREDATE\", 'DY') = 'Wed' "
            + "THEN 'Wednesday' WHEN TO_VARCHAR(\"HIREDATE\", 'DY') = 'Thu' "
            + "THEN 'Thursday' WHEN TO_VARCHAR(\"HIREDATE\", 'DY') = 'Fri' "
            + "THEN 'Friday' WHEN TO_VARCHAR(\"HIREDATE\", 'DY') = 'Sat' "
            + "THEN 'Saturday' END";
    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedSF));
  }

  @Test public void testCaseExprForEEEE() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RexNode condition = builder.call(SqlLibraryOperators.FORMAT_DATE,
            builder.literal("EEEE"), builder.field("HIREDATE"));
    final RelNode root = relBuilder().scan("EMP").filter(condition).build();
    final String expectedSF = "SELECT *\n"
            + "FROM \"scott\".\"EMP\"\n"
            + "WHERE CASE WHEN TO_VARCHAR(\"HIREDATE\", 'DY') = 'Sun' "
            + "THEN 'Sunday' WHEN TO_VARCHAR(\"HIREDATE\", 'DY') = 'Mon' "
            + "THEN 'Monday' WHEN TO_VARCHAR(\"HIREDATE\", 'DY') = 'Tue' "
            + "THEN 'Tuesday' WHEN TO_VARCHAR(\"HIREDATE\", 'DY') = 'Wed' "
            + "THEN 'Wednesday' WHEN TO_VARCHAR(\"HIREDATE\", 'DY') = 'Thu' "
            + "THEN 'Thursday' WHEN TO_VARCHAR(\"HIREDATE\", 'DY') = 'Fri' "
            + "THEN 'Friday' WHEN TO_VARCHAR(\"HIREDATE\", 'DY') = 'Sat' "
            + "THEN 'Saturday' END";
    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedSF));
  }

  @Test public void testCaseExprForE3() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RexNode condition = builder.call(SqlLibraryOperators.FORMAT_DATE,
            builder.literal("E3"), builder.field("HIREDATE"));
    final RelNode root = relBuilder().scan("EMP").filter(condition).build();
    final String expectedSF = "SELECT *\n"
            + "FROM \"scott\".\"EMP\"\n"
            + "WHERE TO_VARCHAR(\"HIREDATE\", 'DY')";
    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedSF));
  }

  @Test public void testCaseExprForEEE() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RexNode condition = builder.call(SqlLibraryOperators.FORMAT_DATE,
            builder.literal("EEE"), builder.field("HIREDATE"));
    final RelNode root = relBuilder().scan("EMP").filter(condition).build();
    final String expectedSF = "SELECT *\n"
            + "FROM \"scott\".\"EMP\"\n"
            + "WHERE TO_VARCHAR(\"HIREDATE\", 'DY')";
    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedSF));
  }

  @Test public void octetLength() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RexNode condition = builder.call(SqlLibraryOperators.OCTET_LENGTH,
            builder.field("ENAME"));
    final RelNode root = relBuilder().scan("EMP").filter(condition).build();

    final String expectedBQ = "SELECT *\n"
            + "FROM scott.EMP\n"
            + "WHERE OCTET_LENGTH(ENAME)";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQ));
  }

  @Test public void octetLengthWithLiteral() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RexNode condition = builder.call(SqlLibraryOperators.OCTET_LENGTH,
            builder.literal("ENAME"));
    final RelNode root = relBuilder().scan("EMP").filter(condition).build();

    final String expectedBQ = "SELECT *\n"
            + "FROM scott.EMP\n"
            + "WHERE OCTET_LENGTH('ENAME')";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQ));
  }

  @Test public void testInt2Shr() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RexNode condition = builder.call(SqlLibraryOperators.INT2SHR,
            builder.literal(3), builder.literal(1), builder.literal(6));
    final RelNode root = relBuilder().scan("EMP").filter(condition).build();

    final String expectedBQ = "SELECT *\n"
            + "FROM scott.EMP\n"
            + "WHERE (3 & 6 ) >> 1";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQ));
  }

  @Test public void testInt8Xor() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RexNode condition = builder.call(SqlLibraryOperators.BITWISE_XOR,
            builder.literal(3), builder.literal(6));
    final RelNode root = relBuilder().scan("EMP").filter(condition).build();

    final String expectedBQ = "SELECT *\n"
            + "FROM scott.EMP\n"
            + "WHERE 3 ^ 6";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQ));
  }

  @Test public void testInt2Shl() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RexNode condition = builder.call(SqlLibraryOperators.INT2SHL,
            builder.literal(3), builder.literal(1), builder.literal(6));
    final RelNode root = relBuilder().scan("EMP").filter(condition).build();

    final String expectedBQ = "SELECT *\n"
            + "FROM scott.EMP\n"
            + "WHERE (3 & 6 ) << 1";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQ));
  }

  @Test public void testInt2And() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RexNode condition = builder.call(SqlLibraryOperators.BITWISE_AND,
            builder.literal(3), builder.literal(6));
    final RelNode root = relBuilder().scan("EMP").filter(condition).build();

    final String expectedBQ = "SELECT *\n"
            + "FROM scott.EMP\n"
            + "WHERE 3 & 6";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQ));
  }

  @Test public void testInt1Or() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RexNode condition = builder.call(SqlLibraryOperators.BITWISE_OR,
            builder.literal(3), builder.literal(6));
    final RelNode root = relBuilder().scan("EMP").filter(condition).build();

    final String expectedBQ = "SELECT *\n"
            + "FROM scott.EMP\n"
            + "WHERE 3 | 6";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQ));
  }

  @Test public void testCot() {
    final String query = "SELECT COT(0.12)";

    final String expectedBQ = "SELECT 1 / TAN(0.12)";
    sql(query)
            .withBigQuery()
            .ok(expectedBQ);
  }

  @Test public void testTimestampLiteral() {
    final String query = "SELECT Timestamp '1993-07-21 10:10:10'";
    final String expectedBQ = "SELECT CAST('1993-07-21 10:10:10' AS DATETIME)";
    sql(query)
        .withBigQuery()
        .ok(expectedBQ);
  }

  @Test public void testCaseForLnFunction() {
    final String query = "SELECT LN(\"product_id\") as dd from \"product\"";
    final String expectedMssql = "SELECT LOG([product_id]) AS [DD]"
            + "\nFROM [foodmart].[product]";
    sql(query)
            .withMssql()
            .ok(expectedMssql);
  }

  @Test public void testCaseForCeilToCeilingMSSQL() {
    final String query = "SELECT CEIL(12345) FROM \"product\"";
    final String expected = "SELECT CEILING(12345)\n"
            + "FROM [foodmart].[product]";
    sql(query)
      .withMssql()
      .ok(expected);
  }

  @Test public void testLastDayMSSQL() {
    final String query = "SELECT LAST_DAY(DATE '2009-12-20')";
    final String expected = "SELECT EOMONTH('2009-12-20')";
    sql(query)
            .withMssql()
            .ok(expected);
  }

  @Test public void testCurrentDate() {
    String query =
        "select CURRENT_DATE from \"product\" where \"product_id\" < 10";
    final String expected = "SELECT CAST(GETDATE() AS DATE) AS [CURRENT_DATE]\n"
        + "FROM [foodmart].[product]\n"
        + "WHERE [product_id] < 10";
    sql(query).withMssql().ok(expected);
  }

  @Test public void testCurrentTime() {
    String query =
        "select CURRENT_TIME from \"product\" where \"product_id\" < 10";
    final String expected = "SELECT CAST(GETDATE() AS TIME) AS [CURRENT_TIME]\n"
        + "FROM [foodmart].[product]\n"
        + "WHERE [product_id] < 10";
    sql(query).withMssql().ok(expected);
  }

  @Test public void testCurrentTimestamp() {
    String query =
        "select CURRENT_TIMESTAMP from \"product\" where \"product_id\" < 10";
    final String expected = "SELECT GETDATE() AS [CURRENT_TIMESTAMP]\n"
        + "FROM [foodmart].[product]\n"
        + "WHERE [product_id] < 10";
    sql(query).withMssql().ok(expected);
  }

  @Test public void testDayOfMonth() {
    String query = "select DAYOFMONTH( DATE '2008-08-29')";
    final String expectedMssql = "SELECT DAY('2008-08-29')";
    final String expectedBQ = "SELECT EXTRACT(DAY FROM DATE '2008-08-29')";

    sql(query)
      .withMssql()
      .ok(expectedMssql)
      .withBigQuery()
      .ok(expectedBQ);
  }

  @Test public void testExtractDecade() {
    String query = "SELECT EXTRACT(DECADE FROM DATE '2008-08-29')";
    final String expectedBQ = "SELECT CAST(SUBSTR(CAST("
            + "EXTRACT(YEAR FROM DATE '2008-08-29') AS STRING), 0, 3) AS INTEGER)";

    sql(query)
            .withBigQuery()
            .ok(expectedBQ);
  }

  @Test public void testExtractCentury() {
    String query = "SELECT EXTRACT(CENTURY FROM DATE '2008-08-29')";
    final String expectedBQ = "SELECT CAST(CEIL(EXTRACT(YEAR FROM DATE '2008-08-29') / 100) "
            + "AS INTEGER)";

    sql(query)
            .withBigQuery()
            .ok(expectedBQ);
  }

  @Test public void testExtractDOY() {
    String query = "SELECT EXTRACT(DOY FROM DATE '2008-08-29')";
    final String expectedBQ = "SELECT EXTRACT(DAYOFYEAR FROM DATE '2008-08-29')";

    sql(query)
            .withBigQuery()
            .ok(expectedBQ);
  }

  @Test public void testExtractDOW() {
    String query = "SELECT EXTRACT(DOW FROM DATE '2008-08-29')";
    final String expectedBQ = "SELECT EXTRACT(DAYOFWEEK FROM DATE '2008-08-29')";

    sql(query)
            .withBigQuery()
            .ok(expectedBQ);
  }

  @Test public void testExtractEpoch() {
    String query = "SELECT EXTRACT(EPOCH FROM DATE '2008-08-29')";
    final String expectedBQ = "SELECT UNIX_SECONDS(DATE '2008-08-29')";

    sql(query)
            .withBigQuery()
            .ok(expectedBQ);
  }

  @Test public void testExtractMillennium() {
    String query = "SELECT EXTRACT(MILLENNIUM FROM DATE '2008-08-29')";
    final String expectedBQ = "SELECT CAST(SUBSTR(CAST("
            + "EXTRACT(YEAR FROM DATE '2008-08-29') AS STRING), 0, 1) AS INTEGER)";

    sql(query)
            .withBigQuery()
            .ok(expectedBQ);
  }

  @Test public void testSecFromMidnightFormatTimestamp() {
    final RelBuilder builder = relBuilder();
    final RexNode formatTimestampRexNode = builder.call(SqlLibraryOperators.FORMAT_TIMESTAMP,
        builder.literal("SEC_FROM_MIDNIGHT"), builder.scan("EMP").field(4));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(formatTimestampRexNode, "FD"))
        .build();
    final String expectedSql = "SELECT FORMAT_TIMESTAMP('SEC_FROM_MIDNIGHT', \"HIREDATE\") AS"
        + " \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT CAST(DATE_DIFF(HIREDATE, CAST(CAST(HIREDATE AS DATE) "
        + "AS DATETIME), SECOND) AS STRING) AS FD\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testGetQuarterFromDate() {
    final RelBuilder builder = relBuilder();
    final RexNode formatDateRexNode = builder.call(SqlLibraryOperators.FORMAT_DATE,
        builder.literal("QUARTER"), builder.scan("EMP").field(4));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(formatDateRexNode, "FD"))
        .build();

    final String expectedBiqQuery = "SELECT FORMAT_DATE('%Q', HIREDATE) AS FD\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }


  @Test public void testExtractDay() {
    String query = "SELECT EXTRACT(DAY FROM CURRENT_DATE), EXTRACT(DAY FROM CURRENT_TIMESTAMP)";
    final String expectedSFSql = "SELECT DAY(CURRENT_DATE), DAY(CURRENT_TIMESTAMP)";
    final String expectedBQSql = "SELECT EXTRACT(DAY FROM CURRENT_DATE), "
        + "EXTRACT(DAY FROM CURRENT_DATETIME())";
    final String expectedMsSql = "SELECT DAY(CAST(GETDATE() AS DATE)), DAY(GETDATE())";

    sql(query)
        .withSnowflake()
        .ok(expectedSFSql)
        .withBigQuery()
        .ok(expectedBQSql)
        .withMssql()
        .ok(expectedMsSql);
  }

  @Test public void testExtractMonth() {
    String query = "SELECT EXTRACT(MONTH FROM CURRENT_DATE), EXTRACT(MONTH FROM CURRENT_TIMESTAMP)";
    final String expectedSFSql = "SELECT MONTH(CURRENT_DATE), MONTH(CURRENT_TIMESTAMP)";
    final String expectedBQSql = "SELECT EXTRACT(MONTH FROM CURRENT_DATE), "
        + "EXTRACT(MONTH FROM CURRENT_DATETIME())";
    final String expectedMsSql = "SELECT MONTH(CAST(GETDATE() AS DATE)), MONTH(GETDATE())";

    sql(query)
        .withSnowflake()
        .ok(expectedSFSql)
        .withBigQuery()
        .ok(expectedBQSql)
        .withMssql()
        .ok(expectedMsSql);
  }

  @Test public void testExtractYear() {
    String query = "SELECT EXTRACT(YEAR FROM CURRENT_DATE), EXTRACT(YEAR FROM CURRENT_TIMESTAMP)";
    final String expectedSFSql = "SELECT YEAR(CURRENT_DATE), YEAR(CURRENT_TIMESTAMP)";
    final String expectedBQSql = "SELECT EXTRACT(YEAR FROM CURRENT_DATE), "
        + "EXTRACT(YEAR FROM CURRENT_DATETIME())";
    final String expectedMsSql = "SELECT YEAR(CAST(GETDATE() AS DATE)), YEAR(GETDATE())";

    sql(query)
        .withSnowflake()
        .ok(expectedSFSql)
        .withBigQuery()
        .ok(expectedBQSql)
        .withMssql()
        .ok(expectedMsSql);
  }

  @Test public void testIntervalMultiplyWithInteger() {
    String query = "select \"hire_date\" + 10 * INTERVAL '00:01:00' HOUR "
        + "TO SECOND from \"employee\"";
    final String expectedBQSql = "SELECT TIMESTAMP_ADD(hire_date, INTERVAL 10 * 60 SECOND)\n"
        + "FROM foodmart.employee";

    sql(query)
        .withBigQuery()
        .ok(expectedBQSql);
  }

  @Test public void testDateUnderscoreSeparator() {
    final RelBuilder builder = relBuilder();
    final RexNode formatTimestampRexNode = builder.call(SqlLibraryOperators.FORMAT_TIMESTAMP,
        builder.literal("YYYYMMDD_HH24MISS"), builder.scan("EMP").field(4));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(formatTimestampRexNode, "FD"))
        .build();
    final String expectedBiqQuery = "SELECT FORMAT_TIMESTAMP('%Y%m%d_%H%M%S', HIREDATE) AS FD\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testParseDatetime() {
    final RelBuilder builder = relBuilder();
    final RexNode parseDatetimeRexNode = builder.call(SqlLibraryOperators.PARSE_TIMESTAMP,
        builder.literal("YYYYMMDD_HH24MISS"), builder.scan("EMP").field(4));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(parseDatetimeRexNode, "FD"))
        .build();
    final String expectedBiqQuery = "SELECT PARSE_DATETIME('%Y%m%d_%H%M%S', HIREDATE) AS FD\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testUnixFunctions() {
    final RelBuilder builder = relBuilder();
    final RexNode unixSecondsRexNode = builder.call(SqlLibraryOperators.UNIX_SECONDS,
        builder.scan("EMP").field(4));
    final RexNode unixMicrosRexNode = builder.call(SqlLibraryOperators.UNIX_MICROS,
        builder.scan("EMP").field(4));
    final RexNode unixMillisRexNode = builder.call(SqlLibraryOperators.UNIX_MILLIS,
        builder.scan("EMP").field(4));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(unixSecondsRexNode, "US"),
            builder.alias(unixMicrosRexNode,  "UM"),
            builder.alias(unixMillisRexNode, "UMI"))
        .build();
    final String expectedBiqQuery = "SELECT UNIX_SECONDS(CAST(HIREDATE AS TIMESTAMP)) AS US, "
        + "UNIX_MICROS(CAST(HIREDATE AS TIMESTAMP)) AS UM, UNIX_MILLIS(CAST(HIREDATE AS TIMESTAMP)) "
        + "AS UMI\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testTimestampFunctions() {
    final RelBuilder builder = relBuilder();
    final RexNode unixSecondsRexNode = builder.call(SqlLibraryOperators.TIMESTAMP_SECONDS,
        builder.scan("EMP").field(4));
    final RexNode unixMicrosRexNode = builder.call(SqlLibraryOperators.TIMESTAMP_MICROS,
        builder.scan("EMP").field(4));
    final RexNode unixMillisRexNode = builder.call(SqlLibraryOperators.TIMESTAMP_MILLIS,
        builder.scan("EMP").field(4));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(unixSecondsRexNode, "TS"),
            builder.alias(unixMicrosRexNode, "TM"),
            builder.alias(unixMillisRexNode, "TMI"))
        .build();
    final String expectedBiqQuery = "SELECT CAST(TIMESTAMP_SECONDS(HIREDATE) AS DATETIME) AS TS, "
        + "CAST(TIMESTAMP_MICROS(HIREDATE) AS DATETIME) AS TM, CAST(TIMESTAMP_MILLIS(HIREDATE) AS "
        + "DATETIME) AS TMI\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testFormatTimestamp() {
    final RelBuilder builder = relBuilder();
    final RexNode formatTimestampRexNode = builder.call(SqlLibraryOperators.FORMAT_TIMESTAMP,
        builder.literal("EEEE"),
        builder.cast(builder.literal("1999-07-01 15:00:00-08:00"), SqlTypeName.TIMESTAMP));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(formatTimestampRexNode, "FT"))
        .build();
    final String expectedBiqQuery =
        "SELECT FORMAT_TIMESTAMP('%A', CAST('1999-07-01 15:00:00-08:00' AS TIMESTAMP)) AS FT\n"
            + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testGroupingFunction() {
    String query = "SELECT \"first_name\",\"last_name\", "
        + "grouping(\"first_name\")+ grouping(\"last_name\") "
        + "from \"foodmart\".\"employee\" group by \"first_name\",\"last_name\"";
    final String expectedBQSql = "SELECT first_name, last_name, CASE WHEN first_name IS NULL THEN"
        + " 1 ELSE 0 END + CASE WHEN last_name IS NULL THEN 1 ELSE 0 END\n"
        + "FROM foodmart.employee\n"
        + "GROUP BY first_name, last_name";

    sql(query)
      .withBigQuery()
      .ok(expectedBQSql);
  }

  @Test public void testDateMinus() {
    String query = "SELECT \"birth_date\" - \"birth_date\" from \"foodmart\".\"employee\"";
    final String expectedBQSql = "SELECT DATE_DIFF(birth_date, birth_date, DAY)\n"
        + "FROM foodmart.employee";

    sql(query)
      .withBigQuery()
      .ok(expectedBQSql);
  }

  @Test public void testhashbucket() {
    final RelBuilder builder = relBuilder();
    final RexNode formatDateRexNode = builder.call(SqlLibraryOperators.HASHBUCKET,
        builder.call(SqlLibraryOperators.HASHROW, builder.scan("EMP").field(0)));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(formatDateRexNode, "FD"))
        .build();
    final String expectedSql = "SELECT HASHBUCKET(HASHROW(\"EMPNO\")) AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT FARM_FINGERPRINT(EMPNO) AS FD\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testdatetrunc() {
    final RelBuilder builder = relBuilder();
    final RexNode trunc = builder.call(SqlLibraryOperators.TRUNC,
        builder.literal("2008-19-12"), builder.literal("DAY"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(trunc, "FD"))
        .build();
    final String expectedSql = "SELECT TRUNC('2008-19-12', 'DAY') AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT DATE_TRUNC('2008-19-12', DAY) AS FD\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testhashrow() {
    final RelBuilder builder = relBuilder();
    final RexNode hashrow = builder.call(SqlLibraryOperators.HASHROW,
        builder.scan("EMP").field(1));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(hashrow, "FD"))
        .build();
    final String expectedSql = "SELECT HASHROW(\"ENAME\") AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT FARM_FINGERPRINT(ENAME) AS FD\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }


  RelNode createLogicalValueRel(RexNode col1, RexNode col2) {
    final RelBuilder builder = relBuilder();
    RelDataTypeField field = new RelDataTypeFieldImpl("ZERO", 0,
        builder.getTypeFactory().createSqlType(SqlTypeName.INTEGER));
    List<RelDataTypeField> fieldList = new ArrayList<>();
    fieldList.add(field);
    RelRecordType type = new RelRecordType(fieldList);
    builder.values(
        ImmutableList.of(
            ImmutableList.of(
                builder.getRexBuilder().makeZeroLiteral(
                    builder.getTypeFactory().createSqlType(SqlTypeName.INTEGER))
            )), type);
    builder.project(col1, col2);
    return builder.build();
  }

  @Test public void testMultipleUnionWithLogicalValue() {
    final RelBuilder builder = relBuilder();
    builder.push(
        createLogicalValueRel(builder.alias(builder.literal("ALA"), "col1"),
            builder.alias(builder.literal("AmericaAnchorage"), "col2")));
    builder.push(
        createLogicalValueRel(builder.alias(builder.literal("ALAW"), "col1"),
            builder.alias(builder.literal("USAleutian"), "col2")));
    builder.union(true);
    builder.push(
        createLogicalValueRel(builder.alias(builder.literal("AST"), "col1"),
            builder.alias(builder.literal("AmericaHalifax"), "col2")));
    builder.union(true);

    final RelNode root = builder.build();
    final String expectedHive = "SELECT 'ALA' col1, 'AmericaAnchorage' col2\n"
        + "UNION ALL\n"
        + "SELECT 'ALAW' col1, 'USAleutian' col2\n"
        + "UNION ALL\n"
        + "SELECT 'AST' col1, 'AmericaHalifax' col2";
    final String expectedBigQuery = "SELECT 'ALA' AS col1, 'AmericaAnchorage' AS col2\n"
        + "UNION ALL\n"
        + "SELECT 'ALAW' AS col1, 'USAleutian' AS col2\n"
        + "UNION ALL\n"
        + "SELECT 'AST' AS col1, 'AmericaHalifax' AS col2";
    relFn(b -> root)
        .withHive2().ok(expectedHive)
        .withBigQuery().ok(expectedBigQuery);
  }

  @Test public void testRowid() {
    final RelBuilder builder = relBuilder();
    final RexNode rowidRexNode = builder.call(SqlLibraryOperators.ROWID);
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(rowidRexNode, "FD"))
        .build();
    final String expectedSql = "SELECT ROWID() AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT GENERATE_UUID() AS FD\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testEscapeFunction() {
    String query =
        "SELECT '\\\\PWFSNFS01EFS\\imagenowcifs\\debitmemo' AS DM_SENDFILE_PATH1";
    final String expectedBQSql =
        "SELECT '\\\\\\\\PWFSNFS01EFS\\\\imagenowcifs\\\\debitmemo' AS "
            + "DM_SENDFILE_PATH1";

    sql(query)
        .withBigQuery()
        .ok(expectedBQSql);
  }

  @Test public void testTimeAdd() {
    final RelBuilder builder = relBuilder();

    final RexNode createRexNode = builder.call(SqlLibraryOperators.TIME_ADD,
        builder.literal("00:00:00"),
        builder.call(SqlLibraryOperators.INTERVAL_SECONDS, builder.literal(10000)));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(createRexNode, "FD"))
        .build();
    final String expectedBiqQuery = "SELECT TIME_ADD('00:00:00', INTERVAL 10000 SECOND) AS FD\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }
  @Test public void testIntervalSeconds() {
    final RelBuilder builder = relBuilder();

    final RexNode createRexNode = builder.call
        (SqlLibraryOperators.INTERVAL_SECONDS, builder.literal(10000));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(createRexNode, "FD"))
        .build();
    final String expectedBiqQuery = "SELECT INTERVAL 10000 SECOND AS FD\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test void testUnicodeCharacters() {
    final String query = "SELECT 'ð', '°C' FROM \"product\"";
    final String expected = "SELECT '\\u00f0', '\\u00b0C'\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query).ok(expected);
  }

  @Test public void testPlusForTimeAdd() {
    final RelBuilder builder = relBuilder();

    final RexNode createRexNode = builder.call(SqlStdOperatorTable.PLUS,
        builder.cast(builder.literal("12:15:07"), SqlTypeName.TIME),
        builder.getRexBuilder().makeIntervalLiteral(new BigDecimal(1000),
            new SqlIntervalQualifier(MICROSECOND, null, SqlParserPos.ZERO)));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(createRexNode, "FD"))
        .build();
    final String expectedBiqQuery = "SELECT TIME_ADD(TIME '12:15:07', INTERVAL 1 MICROSECOND) "
        + "AS FD\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testMinusForTimeSub() {
    final RelBuilder builder = relBuilder();

    final RexNode createRexNode = builder.call(SqlStdOperatorTable.MINUS,
        builder.cast(builder.literal("12:15:07"), SqlTypeName.TIME),
        builder.getRexBuilder().makeIntervalLiteral(new BigDecimal(1000),
            new SqlIntervalQualifier(MICROSECOND, null, SqlParserPos.ZERO)));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(createRexNode, "FD"))
        .build();
    final String expectedBiqQuery = "SELECT TIME_SUB(TIME '12:15:07', INTERVAL 1 MICROSECOND) "
        + "AS FD\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testPlusForTimestampAdd() {
    final RelBuilder builder = relBuilder();

    final RexNode createRexNode = builder.call(SqlStdOperatorTable.PLUS,
        builder.cast(builder.literal("1999-07-01 15:00:00-08:00"), SqlTypeName.TIMESTAMP),
        builder.getRexBuilder().makeIntervalLiteral(new BigDecimal(1000),
            new SqlIntervalQualifier(MICROSECOND, null, SqlParserPos.ZERO)));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(createRexNode, "FD"))
        .build();
    final String expectedBiqQuery =
        "SELECT TIMESTAMP_ADD(CAST('1999-07-01 15:00:00-08:00' AS DATETIME), "
            + "INTERVAL 1 MICROSECOND) AS FD\n"
            + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testPlusForTimestampSub() {
    final RelBuilder builder = relBuilder();

    final RexNode createRexNode = builder.call(SqlStdOperatorTable.MINUS,
        builder.cast(builder.literal("1999-07-01 15:00:00-08:00"), SqlTypeName.TIMESTAMP),
        builder.getRexBuilder().makeIntervalLiteral(new BigDecimal(1000),
            new SqlIntervalQualifier(MICROSECOND, null, SqlParserPos.ZERO)));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(createRexNode, "FD"))
        .build();
    final String expectedBiqQuery =
        "SELECT TIMESTAMP_SUB(CAST('1999-07-01 15:00:00-08:00' AS DATETIME), "
            + "INTERVAL 1 MICROSECOND) AS FD\n"
            + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testPlusForDateAdd() {
    final RelBuilder builder = relBuilder();

    final RexNode createRexNode = builder.call(SqlStdOperatorTable.PLUS,
        builder.cast(builder.literal("1999-07-01"), SqlTypeName.DATE),
        builder.getRexBuilder().makeIntervalLiteral(new BigDecimal(86400000),
            new SqlIntervalQualifier(DAY, 6, DAY,
                -1, SqlParserPos.ZERO)));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(createRexNode, "FD"))
        .build();
    final String expectedBiqQuery = "SELECT DATE_ADD(DATE '1999-07-01', INTERVAL 1 DAY) AS FD\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testPlusForDateSub() {
    final RelBuilder builder = relBuilder();

    final RexNode createRexNode = builder.call(SqlStdOperatorTable.MINUS,
        builder.cast(builder.literal("1999-07-01"), SqlTypeName.DATE),
        builder.getRexBuilder().makeIntervalLiteral(new BigDecimal(86400000),
            new SqlIntervalQualifier(DAY, 6, DAY,
                -1, SqlParserPos.ZERO)));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(createRexNode, "FD"))
        .build();
    final String expectedBiqQuery = "SELECT DATE_SUB(DATE '1999-07-01', INTERVAL 1 DAY) AS FD\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testWhenTableNameAndColumnNameIsSame() {
    String query =
        "select \"test\" from \"foodmart\".\"test\"";
    final String expectedBQSql =
        "SELECT test.test\n"
            + "FROM foodmart.test AS test";
    sqlTest(query)
        .withBigQuery()
        .ok(expectedBQSql);
  }
}
