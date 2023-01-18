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
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
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
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialect.DatabaseProduct;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.dialect.JethroDataSqlDialect;
import org.apache.calcite.sql.dialect.MssqlSqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
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
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.calcite.test.Matchers.isLinux;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static java.util.Objects.requireNonNull;

/**
 * Tests for {@link RelToSqlConverter}.
 */
class RelToSqlConverterTest {

  private Sql fixture() {
    return new Sql(CalciteAssert.SchemaSpec.JDBC_FOODMART, "?",
        CalciteSqlDialect.DEFAULT, SqlParser.Config.DEFAULT, ImmutableSet.of(),
        UnaryOperator.identity(), null, ImmutableList.of(), RelDataTypeSystem.DEFAULT);
  }

  /** Initiates a test case with a given SQL query. */
  private Sql sql(String sql) {
    return fixture().withSql(sql);
  }

  /** Initiates a test case with a given {@link RelNode} supplier. */
  private Sql relFn(Function<RelBuilder, RelNode> relFn) {
    return fixture()
        .schema(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL)
        .relFn(relFn);
  }

  private static Planner getPlanner(List<RelTraitDef> traitDefs,
      SqlParser.Config parserConfig, SchemaPlus schema,
      SqlToRelConverter.Config sqlToRelConf, Collection<SqlLibrary> librarySet,
      RelDataTypeSystem typeSystem, Program... programs) {
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(parserConfig)
        .defaultSchema(schema)
        .traitDefs(traitDefs)
        .sqlToRelConverterConfig(sqlToRelConf)
        .programs(programs)
        .operatorTable(MockSqlOperatorTable.standard()
            .plus(librarySet)
            .extend())
        .typeSystem(typeSystem)
        .build();
    return Frameworks.getPlanner(config);
  }

  private static JethroDataSqlDialect jethroDataSqlDialect() {
    SqlDialect.Context dummyContext = SqlDialect.EMPTY_CONTEXT
        .withDatabaseProduct(DatabaseProduct.JETHRO)
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
        .put(DatabaseProduct.BIG_QUERY.getDialect(), DatabaseProduct.BIG_QUERY)
        .put(DatabaseProduct.CALCITE.getDialect(), DatabaseProduct.CALCITE)
        .put(DatabaseProduct.DB2.getDialect(), DatabaseProduct.DB2)
        .put(DatabaseProduct.EXASOL.getDialect(), DatabaseProduct.EXASOL)
        .put(DatabaseProduct.HIVE.getDialect(), DatabaseProduct.HIVE)
        .put(jethroDataSqlDialect(), DatabaseProduct.JETHRO)
        .put(DatabaseProduct.MSSQL.getDialect(), DatabaseProduct.MSSQL)
        .put(DatabaseProduct.MYSQL.getDialect(), DatabaseProduct.MYSQL)
        .put(mySqlDialect(NullCollation.HIGH), DatabaseProduct.MYSQL)
        .put(DatabaseProduct.ORACLE.getDialect(), DatabaseProduct.ORACLE)
        .put(DatabaseProduct.POSTGRESQL.getDialect(), DatabaseProduct.POSTGRESQL)
        .put(DatabaseProduct.PRESTO.getDialect(), DatabaseProduct.PRESTO)
        .build();
  }

  /** Creates a RelBuilder. */
  private static RelBuilder relBuilder() {
    return RelBuilder.create(RelBuilderTest.config().build());
  }

  /** Converts a relational expression to SQL. */
  private String toSql(RelNode root) {
    return toSql(root, DatabaseProduct.CALCITE.getDialect());
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

  @Test void testGroupByBooleanLiteral() {
    String query = "select avg(\"salary\") from \"employee\" group by true";
    String expectedRedshift = "SELECT AVG(\"employee\".\"salary\")\n"
        + "FROM \"foodmart\".\"employee\",\n"
        + "(SELECT TRUE AS \"$f0\") AS \"t\"\nGROUP BY \"t\".\"$f0\"";
    String expectedInformix = "SELECT AVG(employee.salary)\nFROM foodmart.employee,"
        + "\n(SELECT TRUE AS $f0) AS t\nGROUP BY t.$f0";
    sql(query)
        .withRedshift().ok(expectedRedshift)
        .withInformix().ok(expectedInformix);
  }

  @Test void testGroupByDateLiteral() {
    String query = "select avg(\"salary\") from \"employee\" group by DATE '2022-01-01'";
    String expectedRedshift = "SELECT AVG(\"employee\".\"salary\")\n"
        + "FROM \"foodmart\".\"employee\",\n"
        + "(SELECT DATE '2022-01-01' AS \"$f0\") AS \"t\"\nGROUP BY \"t\".\"$f0\"";
    String expectedInformix = "SELECT AVG(employee.salary)\nFROM foodmart.employee,"
        + "\n(SELECT DATE '2022-01-01' AS $f0) AS t\nGROUP BY t.$f0";
    sql(query)
        .withRedshift().ok(expectedRedshift)
        .withInformix().ok(expectedInformix);
  }

  @Test void testSimpleSelectStarFromProductTable() {
    String query = "select * from \"product\"";
    String expected = "SELECT *\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query).ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4901">[CALCITE-4901]
   * JDBC adapter incorrectly adds ORDER BY columns to the SELECT list</a>. */
  @Test void testOrderByNotInSelectList() {
    // Before 4901 was fixed, the generated query would have "product_id" in its
    // SELECT clause.
    String query = "select count(1) as c\n"
        + "from \"foodmart\".\"product\"\n"
        + "group by \"product_id\"\n"
        + "order by \"product_id\" desc";
    final String expected = "SELECT COUNT(*) AS \"C\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_id\"\n"
        + "ORDER BY \"product_id\" DESC";
    sql(query).ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4321">[CALCITE-4321]
   * JDBC adapter omits FILTER (WHERE ...) expressions when generating SQL</a>
   * and
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5270">[CALCITE-5270]
   * JDBC adapter should not generate FILTER (WHERE) in Firebolt dialect</a>. */
  @Test void testAggregateFilterWhere() {
    String query = "select\n"
        + "  sum(\"shelf_width\") filter (where \"net_weight\" > 0),\n"
        + "  sum(\"shelf_width\")\n"
        + "from \"foodmart\".\"product\"\n"
        + "where \"product_id\" > 0\n"
        + "group by \"product_id\"";
    final String expectedDefault = "SELECT"
        + " SUM(\"shelf_width\") FILTER (WHERE \"net_weight\" > 0 IS TRUE),"
        + " SUM(\"shelf_width\")\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE \"product_id\" > 0\n"
        + "GROUP BY \"product_id\"";
    final String expectedBigQuery = "SELECT"
        + " SUM(CASE WHEN net_weight > 0 IS TRUE"
        + " THEN shelf_width ELSE NULL END), "
        + "SUM(shelf_width)\n"
        + "FROM foodmart.product\n"
        + "WHERE product_id > 0\n"
        + "GROUP BY product_id";
    final String expectedFirebolt = "SELECT"
        + " SUM(CASE WHEN \"net_weight\" > 0 IS TRUE"
        + " THEN \"shelf_width\" ELSE NULL END), "
        + "SUM(\"shelf_width\")\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE \"product_id\" > 0\n"
        + "GROUP BY \"product_id\"";
    sql(query).ok(expectedDefault)
        .withBigQuery().ok(expectedBigQuery)
        .withFirebolt().ok(expectedFirebolt);
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4449">[CALCITE-4449]
   * Calcite generates incorrect SQL for Sarg 'x IS NULL OR x NOT IN
   * (1, 2)'</a>. */
  @Test void testSelectWhereNotIn() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .filter(
            b.or(b.isNull(b.field("COMM")),
                b.not(b.in(b.field("COMM"), b.literal(1), b.literal(2)))))
        .build();
    final String expected = "SELECT *\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "WHERE \"COMM\" IS NULL OR \"COMM\" NOT IN (1, 2)";
    relFn(relFn).ok(expected);
  }

  @Test void testSelectWhereNotEquals() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .filter(
            b.or(b.isNull(b.field("COMM")),
                b.not(b.in(b.field("COMM"), b.literal(1)))))
        .build();
    final String expected = "SELECT *\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "WHERE \"COMM\" IS NULL OR \"COMM\" <> 1";
    relFn(relFn).ok(expected);
  }

  @Test void testSelectWhereIn() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .filter(b.in(b.field("COMM"), b.literal(1), b.literal(2)))
        .build();
    final String expected = "SELECT *\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "WHERE \"COMM\" IN (1, 2)";
    relFn(relFn).ok(expected);
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
    final String expectedMysql = "SELECT COUNT(*)\n"
        + "FROM `foodmart`.`product`";
    final String expectedPresto = "SELECT COUNT(*)\n"
        + "FROM \"foodmart\".\"product\"";
    sql(sql0)
        .ok(expected)
        .withMysql().ok(expectedMysql)
        .withPresto().ok(expectedPresto);
    sql(sql1)
        .ok(expected)
        .withMysql().ok(expectedMysql)
        .withPresto().ok(expectedPresto);
  }

  @Test void testSelectQueryWithGroupByEmpty2() {
    final String query = "select 42 as c from \"product\" group by ()";
    final String expected = "SELECT 42 AS \"C\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY ()";
    final String expectedMysql = "SELECT 42 AS `C`\n"
        + "FROM `foodmart`.`product`\n"
        + "GROUP BY ()";
    final String expectedPresto = "SELECT 42 AS \"C\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY ()";
    sql(query)
        .ok(expected)
        .withMysql().ok(expectedMysql)
        .withPresto().ok(expectedPresto);
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
        .withPostgresql().ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4665">[CALCITE-4665]
   * Allow Aggregate.groupSet to contain columns not in any of the
   * groupSets</a>. Generate a redundant grouping set and a HAVING clause to
   * filter it out. */
  @Test void testGroupSuperset() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .aggregate(
            b.groupKey(ImmutableBitSet.of(0, 1, 2),
                ImmutableList.of(ImmutableBitSet.of(0, 1), ImmutableBitSet.of(0))),
            b.count(false, "C"),
            b.sum(false, "S", b.field("SAL")))
        .filter(b.equals(b.field("JOB"), b.literal("DEVELOP")))
        .project(b.field("JOB"))
        .build();
    final String expectedSql = "SELECT \"JOB\"\n"
        + "FROM (SELECT \"EMPNO\", \"ENAME\", \"JOB\", COUNT(*) AS \"C\","
        + " SUM(\"SAL\") AS \"S\"\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "GROUP BY GROUPING SETS((\"EMPNO\", \"ENAME\", \"JOB\"),"
        + " (\"EMPNO\", \"ENAME\"), \"EMPNO\")\n"
        + "HAVING GROUPING(\"EMPNO\", \"ENAME\", \"JOB\") <> 0"
        + " AND \"JOB\" = 'DEVELOP') AS \"t\"";
    relFn(relFn).ok(expectedSql);
  }

  /** As {@link #testGroupSuperset()},
   * but HAVING has one standalone condition. */
  @Test void testGroupSuperset2() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .aggregate(
            b.groupKey(ImmutableBitSet.of(0, 1, 2),
                ImmutableList.of(ImmutableBitSet.of(0, 1), ImmutableBitSet.of(0))),
            b.count(false, "C"),
            b.sum(false, "S", b.field("SAL")))
        .filter(
            b.call(SqlStdOperatorTable.GREATER_THAN, b.field("C"),
                b.literal(10)))
        .filter(b.equals(b.field("JOB"), b.literal("DEVELOP")))
        .project(b.field("JOB"))
        .build();
    final String expectedSql = "SELECT \"JOB\"\n"
        + "FROM (SELECT *\n"
        + "FROM (SELECT \"EMPNO\", \"ENAME\", \"JOB\", COUNT(*) AS \"C\","
        + " SUM(\"SAL\") AS \"S\"\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "GROUP BY GROUPING SETS((\"EMPNO\", \"ENAME\", \"JOB\"),"
        + " (\"EMPNO\", \"ENAME\"), \"EMPNO\")\n"
        + "HAVING GROUPING(\"EMPNO\", \"ENAME\", \"JOB\") <> 0"
        + " AND \"C\" > 10) AS \"t\") "
        + "AS \"t0\"\n"
        + "WHERE \"JOB\" = 'DEVELOP'";
    relFn(relFn).ok(expectedSql);
  }

  /** As {@link #testGroupSuperset()},
   * but HAVING has one OR condition and the result can add appropriate
   * parentheses. Also there is an empty grouping set. */
  @Test void testGroupSuperset3() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .aggregate(
            b.groupKey(ImmutableBitSet.of(0, 1, 2),
                ImmutableList.of(ImmutableBitSet.of(0, 1),
                    ImmutableBitSet.of(0),
                    ImmutableBitSet.of())),
            b.count(false, "C"),
            b.sum(false, "S", b.field("SAL")))
        .filter(
            b.or(
                b.greaterThan(b.field("C"), b.literal(10)),
                b.lessThan(b.field("S"), b.literal(3000))))
        .filter(b.equals(b.field("JOB"), b.literal("DEVELOP")))
        .project(b.field("JOB"))
        .build();
    final String expectedSql = "SELECT \"JOB\"\n"
        + "FROM (SELECT *\n"
        + "FROM (SELECT \"EMPNO\", \"ENAME\", \"JOB\", COUNT(*) AS \"C\","
        + " SUM(\"SAL\") AS \"S\"\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "GROUP BY GROUPING SETS((\"EMPNO\", \"ENAME\", \"JOB\"),"
        + " (\"EMPNO\", \"ENAME\"), \"EMPNO\", ())\n"
        + "HAVING GROUPING(\"EMPNO\", \"ENAME\", \"JOB\") <> 0"
        + " AND (\"C\" > 10 OR \"S\" < 3000)) AS \"t\") "
        + "AS \"t0\"\n"
        + "WHERE \"JOB\" = 'DEVELOP'";
    relFn(relFn).ok(expectedSql);
  }

  /** As {@link #testGroupSuperset()}, but with no Filter between the Aggregate
   * and the Project. */
  @Test void testGroupSuperset4() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .aggregate(
            b.groupKey(ImmutableBitSet.of(0, 1, 2),
                ImmutableList.of(ImmutableBitSet.of(0, 1), ImmutableBitSet.of(0))),
            b.count(false, "C"),
            b.sum(false, "S", b.field("SAL")))
        .project(b.field("JOB"))
        .build();
    final String expectedSql = "SELECT \"JOB\"\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "GROUP BY GROUPING SETS((\"EMPNO\", \"ENAME\", \"JOB\"),"
        + " (\"EMPNO\", \"ENAME\"), \"EMPNO\")\n"
        + "HAVING GROUPING(\"EMPNO\", \"ENAME\", \"JOB\") <> 0";
    relFn(relFn).ok(expectedSql);
  }

  /** As {@link #testGroupSuperset()}, but with no Filter between the Aggregate
   * and the Sort. */
  @Test void testGroupSuperset5() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .aggregate(
            b.groupKey(ImmutableBitSet.of(0, 1, 2),
                ImmutableList.of(ImmutableBitSet.of(0, 1), ImmutableBitSet.of(0))),
            b.count(false, "C"),
            b.sum(false, "S", b.field("SAL")))
        .sort(b.field("C"))
        .build();
    final String expectedSql = "SELECT \"EMPNO\", \"ENAME\", \"JOB\","
        + " COUNT(*) AS \"C\", SUM(\"SAL\") AS \"S\"\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "GROUP BY GROUPING SETS((\"EMPNO\", \"ENAME\", \"JOB\"),"
        + " (\"EMPNO\", \"ENAME\"), \"EMPNO\")\n"
        + "HAVING GROUPING(\"EMPNO\", \"ENAME\", \"JOB\") <> 0\n"
        + "ORDER BY COUNT(*)";
    relFn(relFn).ok(expectedSql);
  }

  /** As {@link #testGroupSuperset()}, but with Filter condition and Where condition. */
  @Test void testGroupSuperset6() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .aggregate(
            b.groupKey(ImmutableBitSet.of(0, 1, 2),
                ImmutableList.of(ImmutableBitSet.of(0, 1),
                    ImmutableBitSet.of(0),
                    ImmutableBitSet.of())),
            b.count(false, "C"),
            b.sum(false, "S", b.field("SAL")))
        .filter(
            b.lessThan(
                b.call(SqlStdOperatorTable.GROUP_ID, b.field("EMPNO")),
                b.literal(1)))
        .filter(b.equals(b.field("JOB"), b.literal("DEVELOP")))
        .project(b.field("JOB"))
        .build();
    final String expectedSql = "SELECT \"JOB\"\n"
        + "FROM (SELECT *\n"
        + "FROM (SELECT \"EMPNO\", \"ENAME\", \"JOB\", COUNT(*) AS \"C\", SUM(\"SAL\") AS \"S\"\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "GROUP BY GROUPING SETS((\"EMPNO\", \"ENAME\", \"JOB\"),"
        + " (\"EMPNO\", \"ENAME\"), \"EMPNO\", ())\n"
        + "HAVING GROUPING(\"EMPNO\", \"ENAME\", \"JOB\") <> 0"
        + " AND GROUP_ID(\"EMPNO\") < 1) AS \"t\") "
        + "AS \"t0\"\n"
        + "WHERE \"JOB\" = 'DEVELOP'";
    relFn(relFn).ok(expectedSql);
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
    final String expectedMysql = "SELECT `product_class_id`, `brand_name`\n"
        + "FROM `foodmart`.`product`\n"
        + "GROUP BY `product_class_id`, `brand_name` WITH ROLLUP";
    final String expectedMysql8 = "SELECT `product_class_id`, `brand_name`\n"
        + "FROM `foodmart`.`product`\n"
        + "GROUP BY ROLLUP(`product_class_id`, `brand_name`)\n"
        + "ORDER BY `product_class_id` NULLS LAST, `brand_name` NULLS LAST";
    sql(query)
        .ok(expected)
        .withMysql().ok(expectedMysql)
        .withMysql8().ok(expectedMysql8);
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
    final String expectedMysql = "SELECT `product_class_id`, `brand_name`\n"
        + "FROM `foodmart`.`product`\n"
        + "GROUP BY `brand_name`, `product_class_id` WITH ROLLUP";
    sql(query)
        .ok(expected)
        .withMysql().ok(expectedMysql);
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
        .withMysql().ok(expectedMysql);

    // Equivalent sub-query that uses SELECT DISTINCT
    final String query2 = "select sum(\"product_id\")\n"
        + "from (select distinct \"product_class_id\", \"product_id\"\n"
        + "    from \"product\") as t";
    sql(query2)
        .ok(expected)
        .withMysql().ok(expectedMysql);
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
        + "ORDER BY \"product_class_id\", COUNT(*)";
    final String expectedMysql = "SELECT `product_class_id`, COUNT(*) AS `C`\n"
        + "FROM `foodmart`.`product`\n"
        + "GROUP BY `product_class_id` WITH ROLLUP\n"
        + "ORDER BY `product_class_id` IS NULL, `product_class_id`,"
        + " COUNT(*) IS NULL, COUNT(*)";
    final String expectedPresto = "SELECT \"product_class_id\", COUNT(*) AS \"C\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY ROLLUP(\"product_class_id\")\n"
        + "ORDER BY \"product_class_id\" IS NULL, \"product_class_id\", "
        + "COUNT(*) IS NULL, COUNT(*)";
    sql(query)
        .ok(expected)
        .withMysql().ok(expectedMysql)
        .withPresto().ok(expectedPresto);
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
    final String expectedMysql = "SELECT `product_class_id`, COUNT(*) AS `C`\n"
        + "FROM `foodmart`.`product`\n"
        + "GROUP BY `product_class_id` WITH ROLLUP";
    final String expectedPresto = "SELECT \"product_class_id\", COUNT(*) AS \"C\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY ROLLUP(\"product_class_id\")";
    sql(query)
        .ok(expected)
        .withMysql().ok(expectedMysql)
        .withPresto().ok(expectedPresto);
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
        + "ORDER BY \"product_class_id\", \"brand_name\", COUNT(*)";
    final String expectedMysql = "SELECT `product_class_id`, `brand_name`,"
        + " COUNT(*) AS `C`\n"
        + "FROM `foodmart`.`product`\n"
        + "GROUP BY `product_class_id`, `brand_name` WITH ROLLUP\n"
        + "ORDER BY `product_class_id` IS NULL, `product_class_id`,"
        + " `brand_name` IS NULL, `brand_name`,"
        + " COUNT(*) IS NULL, COUNT(*)";
    sql(query)
        .ok(expected)
        .withMysql().ok(expectedMysql);
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
    final String expectedMysql = "SELECT `product_class_id`, COUNT(*) AS `C`\n"
        + "FROM `foodmart`.`product`\n"
        + "GROUP BY `product_class_id` WITH ROLLUP\n"
        + "LIMIT 5";
    final String expectedPresto = "SELECT \"product_class_id\", COUNT(*) AS \"C\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY ROLLUP(\"product_class_id\")\n"
        + "LIMIT 5";
    sql(query)
        .ok(expected)
        .withMysql().ok(expectedMysql)
        .withPresto().ok(expectedPresto);
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

  @Test void testCastDecimal1() {
    final String query = "select -0.0000000123\n"
        + " from \"expense_fact\"";
    final String expected = "SELECT -1.23E-8\n"
        + "FROM \"foodmart\".\"expense_fact\"";
    sql(query).ok(expected);
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4706">[CALCITE-4706]
   * JDBC adapter generates casts exceeding Redshift's data types bounds</a>.
   */
  @Test void testCastDecimalBigPrecision() {
    final String query = "select cast(\"product_id\" as decimal(60,2)) "
        + "from \"product\" ";
    final String expectedRedshift = "SELECT CAST(\"product_id\" AS DECIMAL(38, 2))\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query)
        .withTypeSystem(new RelDataTypeSystemImpl() {
          @Override public int getMaxNumericPrecision() {
            // Ensures that parsed decimal will not be truncated during SQL to Rel transformation
            // The default type system sets precision to 19 so it is not sufficient to test
            // this change.
            return 100;
          }
        })
        .withRedshift()
        .ok(expectedRedshift);
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4706">[CALCITE-4706]
   * JDBC adapter generates casts exceeding Redshift's data types bounds</a>.
   */
  @Test void testCastDecimalBigScale() {
    final String query = "select cast(\"product_id\" as decimal(2,90)) "
        + "from \"product\" ";
    final String expectedRedshift = "SELECT CAST(\"product_id\" AS DECIMAL(2, 37))\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query)
        .withRedshift()
        .ok(expectedRedshift);
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4706">[CALCITE-4706]
   * JDBC adapter generates casts exceeding Redshift's data types bounds</a>.
   */
  @Test void testCastLongChar() {
    final String query = "select cast(\"product_id\" as char(9999999)) "
        + "from \"product\" ";
    final String expectedRedshift = "SELECT CAST(\"product_id\" AS CHAR(4096))\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query)
        .withRedshift()
        .ok(expectedRedshift);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2713">[CALCITE-2713]
   * JDBC adapter may generate casts on PostgreSQL for VARCHAR type exceeding
   * max length</a>. */
  @Test void testCastLongVarchar1() {
    final String query = "select cast(\"store_id\" as VARCHAR(10485761))\n"
        + " from \"expense_fact\"";
    final String expectedPostgresql = "SELECT CAST(\"store_id\" AS VARCHAR(256))\n"
        + "FROM \"foodmart\".\"expense_fact\"";
    final String expectedOracle = "SELECT CAST(\"store_id\" AS VARCHAR(512))\n"
        + "FROM \"foodmart\".\"expense_fact\"";
    final String expectedRedshift = "SELECT CAST(\"store_id\" AS VARCHAR(65535))\n"
        + "FROM \"foodmart\".\"expense_fact\"";
    sql(query)
        .withPostgresqlModifiedTypeSystem()
        .ok(expectedPostgresql)
        .withOracleModifiedTypeSystem()
        .ok(expectedOracle)
        .withRedshift()
        .ok(expectedRedshift);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2713">[CALCITE-2713]
   * JDBC adapter may generate casts on PostgreSQL for VARCHAR type exceeding
   * max length</a>. */
  @Test void testCastLongVarchar2() {
    final String query = "select cast(\"store_id\" as VARCHAR(175))\n"
        + " from \"expense_fact\"";
    final String expectedPostgresql = "SELECT CAST(\"store_id\" AS VARCHAR(175))\n"
        + "FROM \"foodmart\".\"expense_fact\"";
    sql(query)
        .withPostgresqlModifiedTypeSystem()
        .ok(expectedPostgresql);

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
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .aggregate(b.groupKey(),
            b.aggregateCall(SqlStdOperatorTable.SUM0, b.field(3))
                .as("s"))
        .build();
    final String expectedMysql = "SELECT COALESCE(SUM(`MGR`), 0) AS `s`\n"
        + "FROM `scott`.`EMP`";
    final String expectedPostgresql = "SELECT COALESCE(SUM(\"MGR\"), 0) AS \"s\"\n"
        + "FROM \"scott\".\"EMP\"";
    relFn(relFn)
        .withPostgresql().ok(expectedPostgresql)
        .withMysql().ok(expectedMysql);
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
        .withPostgresql().ok(expectedPostgresql);
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
    final SqlDialect dialect = DatabaseProduct.CALCITE.getDialect();
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4491">[CALCITE-4491]
   * Aggregation of window function produces invalid SQL for PostgreSQL</a>. */
  @Test void testAggregatedWindowFunction() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .project(b.field("SAL"))
        .project(
            b.aggregateCall(SqlStdOperatorTable.RANK)
                .over()
                .orderBy(b.field("SAL"))
                .rowsUnbounded()
                .allowPartial(true)
                .nullWhenCountZero(false)
                .as("rank"))
        .as("t")
        .aggregate(b.groupKey(),
            b.count(b.field("t", "rank")).distinct().as("c"))
        .filter(
            b.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                b.field("c"), b.literal(10)))
        .build();

    // PostgreSQL does not not support nested aggregations
    final String expectedPostgresql =
        "SELECT COUNT(DISTINCT \"rank\") AS \"c\"\n"
        + "FROM (SELECT RANK() OVER (ORDER BY \"SAL\") AS \"rank\"\n"
        + "FROM \"scott\".\"EMP\") AS \"t\"\n"
        + "HAVING COUNT(DISTINCT \"rank\") >= 10";
    relFn(relFn).withPostgresql().ok(expectedPostgresql);

    // Oracle does support nested aggregations
    final String expectedOracle =
        "SELECT COUNT(DISTINCT RANK() OVER (ORDER BY \"SAL\")) \"c\"\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "HAVING COUNT(DISTINCT RANK() OVER (ORDER BY \"SAL\")) >= 10";
    relFn(relFn).withOracle().ok(expectedOracle);
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
              builder.field("EMPNO"),
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5395">[CALCITE-5395]
   * RelToSql converter fails when SELECT * is under a semi-join node</a>. */
  @Test void testUnionUnderSemiJoinNode() {
    final RelBuilder builder = relBuilder();
    final RelNode base = builder
        .scan("EMP")
        .scan("EMP")
        .union(true)
        .build();
    final RelNode root = builder
        .push(base)
        .scan("DEPT")
        .join(
            JoinRelType.SEMI, builder.equals(
                builder.field(2, 1, "DEPTNO"),
                builder.field(2, 0, "DEPTNO")))
        .project(builder.field("DEPTNO"))
        .build();
    final String expectedSql = "SELECT \"DEPTNO\"\n"
        + "FROM (SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "UNION ALL\n"
        + "SELECT *\n"
        + "FROM \"scott\".\"EMP\")\n"
        + "WHERE EXISTS (SELECT 1\n"
        + "FROM \"scott\".\"DEPT\"\n"
        + "WHERE \"t\".\"DEPTNO\" = \"DEPT\".\"DEPTNO\")) AS \"t\"";
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
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5394">[CALCITE-5394]
   * RelToSql converter fails when semi-join is under a join node</a>. */
  @Test void testSemiJoinUnderJoin() {
    final RelBuilder builder = relBuilder();
    final RelNode base = builder
        .scan("EMP")
        .scan("EMP")
        .join(
            JoinRelType.SEMI, builder.equals(
                builder.field(2, 0, "EMPNO"),
                builder.field(2, 1, "EMPNO")))
        .build();
    final RelNode root = builder
        .scan("DEPT")
        .push(base)
        .join(
            JoinRelType.INNER, builder.equals(
                builder.field(2, 1, "DEPTNO"),
                builder.field(2, 0, "DEPTNO")))
        .project(builder.field("DEPTNO"))
        .build();
    final String expectedSql = "SELECT \"DEPT\".\"DEPTNO\"\n"
        + "FROM \"scott\".\"DEPT\"\n"
        + "INNER JOIN (SELECT *\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "WHERE EXISTS (SELECT 1\n"
        + "FROM \"scott\".\"EMP\" AS \"EMP0\"\n"
        + "WHERE \"EMP\".\"EMPNO\" = \"EMP0\".\"EMPNO\")) AS \"t\" ON \"DEPT\".\"DEPTNO\" = \"t\""
        + ".\"DEPTNO\"";
    assertThat(toSql(root), isLinux(expectedSql));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2792">[CALCITE-2792]
   * StackOverflowError while evaluating filter with large number of OR
   * conditions</a>. */
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
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4716">[CALCITE-4716]
   * ClassCastException converting SARG in RelNode to SQL</a>. */
  @Test void testSargConversion() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .filter(
            b.or(
              b.and(b.greaterThanOrEqual(b.field("EMPNO"), b.literal(10)),
                b.lessThan(b.field("EMPNO"), b.literal(12))),
              b.and(b.greaterThanOrEqual(b.field("EMPNO"), b.literal(6)),
                b.lessThan(b.field("EMPNO"), b.literal(8)))))
        .build();
    final RuleSet rules = RuleSets.ofList(CoreRules.FILTER_TO_CALC);
    final String expected = "SELECT *\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "WHERE \"EMPNO\" >= 6 AND \"EMPNO\" < 8 OR \"EMPNO\" >= 10 AND \"EMPNO\" < 12";
    relFn(relFn).optimize(rules, null).ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4632">[CALCITE-4632]
   * Find the least restrictive datatype for SARG</a>. */
  @Test void testLeastRestrictiveTypeForSargMakeIn() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .filter(
            b.or(b.isNull(b.field("COMM")),
                  b.in(
                  b.field("COMM"),
                  b.literal(new BigDecimal("1.0")), b.literal(new BigDecimal("20000.0")))))
        .build();

    final String expected = "SELECT *\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "WHERE \"COMM\" IS NULL OR \"COMM\" IN (1.0, 20000.0)";
    relFn(relFn).ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4632">[CALCITE-4632]
   * Find the least restrictive datatype for SARG</a>. */
  @Test void testLeastRestrictiveTypeForSargMakeBetween() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .filter(
            b.or(b.isNull(b.field("COMM")),
                b.between(
                    b.field("COMM"),
                    b.literal(
                        new BigDecimal("1.0")), b.literal(new BigDecimal("20000.0")))))
        .build();

    final String expected = "SELECT *\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "WHERE \"COMM\" IS NULL OR \"COMM\" >= 1.0 AND \"COMM\" <= 20000.0";

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
    final String expectedMysql = "SELECT SUM(`net_weight1`) AS `net_weight_converted`\n"
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
    final String expectedExasol = expectedBigQuery;
    sql(query)
        .withBigQuery().ok(expectedBigQuery)
        .withExasol().ok(expectedExasol)
        .withHive().ok(expectedHive)
        .withMysql().ok(expectedMysql)
        .withOracle().ok(expectedOracle)
        .withPostgresql().ok(expectedPostgresql)
        .withSpark().ok(expectedSpark)
        .withVertica().ok(expectedVertica);
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3207">[CALCITE-3207]
   * Fail to convert Join RelNode with like condition to sql statement</a>.
   */
  @Test void testJoinWithLikeConditionRel2Sql() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .scan("DEPT")
        .join(JoinRelType.LEFT,
            b.and(
                b.equals(b.field(2, 0, "DEPTNO"),
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
        + " MIN(\"sales_fact_1997\".\"store_id\") AS \"EXPR$1\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "INNER JOIN \"foodmart\".\"sales_fact_1997\" ON \"product\".\"product_id\" = \"sales_fact_1997\".\"product_id\"\n"
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
            builder.lessThan(builder.field("emps.count"), builder.literal(2)));

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
        + "GROUP BY `DEPTNO`\n"
        + "HAVING `emps.count` < 2) AS `t1`";
    final String expectedPostgresql = "SELECT \"DEPTNO\" AS \"emps.deptno\"\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "GROUP BY \"DEPTNO\"\n"
        + "HAVING COUNT(*) < 2";
    final String expectedBigQuery = "SELECT D2 AS `emps.deptno`\n"
        + "FROM (SELECT DEPTNO AS D2, COUNT(*) AS `emps.count`\n"
        + "FROM scott.EMP\n"
        + "GROUP BY DEPTNO\n"
        + "HAVING `emps.count` < 2) AS t1";
    relFn(b -> root)
        .withBigQuery().ok(expectedBigQuery)
        .withMysql().ok(expectedMysql)
        .withPostgresql().ok(expectedPostgresql);
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
        .withBigQuery().ok(expectedBigQuery)
        .withPostgresql().ok(expectedPostgresql)
        .withMysql().ok(expectedMysql);
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
    final String expected = "SELECT \"product_id\"\n"
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
    final String expected = "SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "ORDER BY \"net_weight\", \"gross_weight\"";
    sql(query).ok(expected);
  }

  @Test void testSelectQueryWithAscDescOrderByClause() {
    String query = "select \"product_id\" from \"product\" "
        + "order by \"net_weight\" asc, \"gross_weight\" desc, \"low_fat\"";
    final String expected = "SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "ORDER BY \"net_weight\", \"gross_weight\" DESC, \"low_fat\"";
    sql(query).ok(expected);
  }

  /** A dialect that doesn't treat integer literals in the ORDER BY as field
   * references. */
  private SqlDialect nonOrdinalDialect() {
    return new SqlDialect(SqlDialect.EMPTY_CONTEXT) {
      @Override public SqlConformance getConformance() {
        return SqlConformanceEnum.STRICT_99;
      }
    };
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5044">[CALCITE-5044]
   * JDBC adapter generates integer literal in ORDER BY, which some dialects
   * wrongly interpret as a reference to a field</a>. */
  @Test void testRewriteOrderByWithNumericConstants() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .project(b.literal(1), b.field(1), b.field(2), b.literal("23"),
            b.alias(b.literal(12), "col1"), b.literal(34))
        .sort(
            RelCollations.of(
                ImmutableList.of(
                    new RelFieldCollation(0), new RelFieldCollation(3), new RelFieldCollation(4),
                    new RelFieldCollation(1),
                    new RelFieldCollation(5, Direction.DESCENDING, NullDirection.LAST))))
        .project(b.field(2), b.field(1))
        .build();
    // Default dialect rewrite numeric constant keys to string literal in the order-by.
    // case1: numeric constant - rewrite it.
    // case2: string constant - no need rewrite it.
    // case3: wrap alias to numeric constant - rewrite it.
    // case4: wrap collation's info to numeric constant - rewrite it.
    relFn(relFn)
        .ok("SELECT \"JOB\", \"ENAME\"\n"
            + "FROM \"scott\".\"EMP\"\n"
            + "ORDER BY '1', '23', '12', \"ENAME\", '34' DESC NULLS LAST")
        .dialect(nonOrdinalDialect())
        .ok("SELECT JOB, ENAME\n"
            + "FROM scott.EMP\n"
            + "ORDER BY 1, '23', 12, ENAME, 34 DESC NULLS LAST");
  }

  @Test void testNoNeedRewriteOrderByConstantsForOver() {
    final String query = "select row_number() over "
        + "(order by 1 nulls last) from \"employee\"";
    // Default dialect keep numeric constant keys in the over of order-by.
    sql(query).ok("SELECT ROW_NUMBER() OVER (ORDER BY 1)\n"
        + "FROM \"foodmart\".\"employee\"");
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
        + "ORDER BY 1";
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
        + "ORDER BY 2";
    final String expectedMysql = "SELECT `net_weight` AS `product_id`,"
        + " `product_id` AS `product_id0`\n"
        + "FROM `foodmart`.`product`\n"
        + "ORDER BY `product_id` IS NULL, 2";
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
            + "CAST(CAST(employee_id AS STRING) AS TIMESTAMP), "
            + "CAST(CAST(employee_id AS STRING) AS FLOAT64), "
            + "CAST(CAST(employee_id AS STRING) AS NUMERIC), "
            + "CAST(CAST(employee_id AS STRING) AS DATE), "
            + "CAST(CAST(employee_id AS STRING) AS TIME), "
            + "CAST(CAST(employee_id AS STRING) AS BOOL)\n"
            + "FROM foodmart.reserve_employee";
    sql(query).withBigQuery().ok(expected);
  }

  @Test void testBigQueryTimeTruncFunctions() {
    String timestampTrunc = "select timestamp_trunc(timestamp '2012-02-03 15:30:00', month)\n"
        + "from \"foodmart\".\"product\"\n";
    final String expectedTimestampTrunc =
        "SELECT TIMESTAMP_TRUNC(TIMESTAMP '2012-02-03 15:30:00', MONTH)\n"
        + "FROM \"foodmart\".\"product\"";
    sql(timestampTrunc).withLibrary(SqlLibrary.BIG_QUERY).ok(expectedTimestampTrunc);

    String timeTrunc = "select time_trunc(time '15:30:00', minute)\n"
        + "from \"foodmart\".\"product\"\n";
    final String expectedTimeTrunc = "SELECT TIME_TRUNC(TIME '15:30:00', MINUTE)\n"
        + "FROM \"foodmart\".\"product\"";
    sql(timeTrunc).withLibrary(SqlLibrary.BIG_QUERY).ok(expectedTimeTrunc);
  }

  @Test void testBigQueryDatetimeFormatFunctions() {
    final String formatTime = "select format_time('%H', time '12:45:30')\n"
        + "from \"foodmart\".\"product\"\n";
    final String formatDate = "select format_date('%b-%d-%Y', date '2012-02-03')\n"
        + "from \"foodmart\".\"product\"\n";
    final String formatTimestamp =
        "select format_timestamp('%b-%d-%Y', TIMESTAMP '2012-02-03 12:30:40')\n"
            + "from \"foodmart\".\"product\"\n";
    final String formatDatetime = "select format_datetime('%R', timestamp '2012-02-03 12:34:34')\n"
        + "from \"foodmart\".\"product\"\n";

    final String expectedBqFormatTime =
        "SELECT FORMAT_TIME('%H', TIME '12:45:30')\nFROM foodmart.product";
    final String expectedBqFormatDate =
        "SELECT FORMAT_DATE('%b-%d-%Y', DATE '2012-02-03')\nFROM foodmart.product";
    final String expectedBqFormatTimestamp =
        "SELECT FORMAT_TIMESTAMP('%b-%d-%Y', TIMESTAMP '2012-02-03 12:30:40')\nFROM foodmart"
            + ".product";
    final String expectedBqFormatDatetime =
        "SELECT FORMAT_DATETIME('%H:%M', TIMESTAMP '2012-02-03 12:34:34')\nFROM foodmart.product";
    sql(formatTime).withBigQuery().withLibrary(SqlLibrary.BIG_QUERY).ok(expectedBqFormatTime);
    sql(formatDate).withBigQuery().withLibrary(SqlLibrary.BIG_QUERY).ok(expectedBqFormatDate);
    sql(formatTimestamp).withBigQuery().withLibrary(SqlLibrary.BIG_QUERY)
        .ok(expectedBqFormatTimestamp);
    sql(formatDatetime).withBigQuery().withLibrary(SqlLibrary.BIG_QUERY)
        .ok(expectedBqFormatDatetime);

    final String expectedMySqlFormatTime = "SELECT DATE_FORMAT(TIME '12:45:30', '%k')\nFROM "
        + "`foodmart`.`product`";
    final String expectedMySqlFormatDate = "SELECT DATE_FORMAT(DATE '2012-02-03', '%b-%d-%Y')"
        + "\nFROM `foodmart`.`product`";
    final String expectedMySqlFormatTimestamp = "SELECT DATE_FORMAT(TIMESTAMP '2012-02-03 "
        + "12:30:40', '%b-%d-%Y')\nFROM `foodmart`.`product`";
    final String expectedMySqlFormatDatetime = "SELECT DATE_FORMAT(TIMESTAMP '2012-02-03 "
        + "12:34:34', '%k:%i')\nFROM `foodmart`.`product`";
    sql(formatTime).withMysql().withLibrary(SqlLibrary.BIG_QUERY).ok(expectedMySqlFormatTime);
    sql(formatDate).withMysql().withLibrary(SqlLibrary.BIG_QUERY).ok(expectedMySqlFormatDate);
    sql(formatTimestamp).withMysql().withLibrary(SqlLibrary.BIG_QUERY)
        .ok(expectedMySqlFormatTimestamp);
    sql(formatDatetime).withMysql().withLibrary(SqlLibrary.BIG_QUERY)
        .ok(expectedMySqlFormatDatetime);
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
        .withBigQuery().ok(expected)
        .withHive().ok(expected)
        .withSpark().ok(expected);
  }

  @Test void testHiveSparkAndBqTrimWithBoth() {
    final String query = "SELECT TRIM(both ' ' from ' str ')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT TRIM(' str ')\n"
        + "FROM foodmart.reserve_employee";
    sql(query)
        .withBigQuery().ok(expected)
        .withHive().ok(expected)
        .withSpark().ok(expected);
  }

  @Test void testHiveSparkAndBqTrimWithLeading() {
    final String query = "SELECT TRIM(LEADING ' ' from ' str ')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT LTRIM(' str ')\n"
        + "FROM foodmart.reserve_employee";
    sql(query)
        .withBigQuery().ok(expected)
        .withHive().ok(expected)
        .withSpark().ok(expected);
  }

  @Test void testHiveSparkAndBqTrimWithTailing() {
    final String query = "SELECT TRIM(TRAILING ' ' from ' str ')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT RTRIM(' str ')\n"
        + "FROM foodmart.reserve_employee";
    sql(query)
        .withBigQuery().ok(expected)
        .withHive().ok(expected)
        .withSpark().ok(expected);
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
        .withBigQuery().ok(expected);
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
        .withHive().ok(expected)
        .withSpark().ok(expected);
  }

  @Test void testBqTrimWithBothChar() {
    final String query = "SELECT TRIM(both 'a' from 'abcda')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT TRIM('abcda', 'a')\n"
        + "FROM foodmart.reserve_employee";
    sql(query)
        .withBigQuery().ok(expected);
  }

  @Test void testHiveAndSparkTrimWithBothChar() {
    final String query = "SELECT TRIM(both 'a' from 'abcda')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT REGEXP_REPLACE('abcda', '^(a)*|(a)*$', '')\n"
        + "FROM foodmart.reserve_employee";
    sql(query)
        .withHive().ok(expected)
        .withSpark().ok(expected);
  }

  @Test void testHiveBqTrimWithTailingChar() {
    final String query = "SELECT TRIM(TRAILING 'a' from 'abcd')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT RTRIM('abcd', 'a')\n"
        + "FROM foodmart.reserve_employee";
    sql(query)
        .withBigQuery().ok(expected);
  }

  @Test void testHiveAndSparkTrimWithTailingChar() {
    final String query = "SELECT TRIM(TRAILING 'a' from 'abcd')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT REGEXP_REPLACE('abcd', '(a)*$', '')\n"
        + "FROM foodmart.reserve_employee";
    sql(query)
        .withHive().ok(expected)
        .withSpark().ok(expected);
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
        .withHive().ok(expected)
        .withSpark().ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2715">[CALCITE-2715]
   * MS SQL Server does not support character set as part of data type</a>
   * and
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4690">[CALCITE-4690]
   * Error when executing query with CHARACTER SET in Redshift</a>. */
  @Test void testCharacterSet() {
    String query = "select \"hire_date\", cast(\"hire_date\" as varchar(10))\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expectedMssql = "SELECT [hire_date],"
        + " CAST([hire_date] AS VARCHAR(10))\n"
        + "FROM [foodmart].[reserve_employee]";
    final String expectedRedshift = "SELECT \"hire_date\","
        + " CAST(\"hire_date\" AS VARCHAR(10))\n"
        + "FROM \"foodmart\".\"reserve_employee\"";
    final String expectedExasol = "SELECT hire_date,"
        + " CAST(hire_date AS VARCHAR(10))\n"
        + "FROM foodmart.reserve_employee";
    sql(query)
        .withExasol().ok(expectedExasol)
        .withMssql().ok(expectedMssql)
        .withRedshift().ok(expectedRedshift);
  }

  @Test void testExasolCastToTimestamp() {
    final String query = "select  * from \"employee\" where  \"hire_date\" - "
        + "INTERVAL '19800' SECOND(5) > cast(\"hire_date\" as TIMESTAMP(0))";
    final String expected = "SELECT *\n"
        + "FROM foodmart.employee\n"
        + "WHERE (hire_date - INTERVAL '19800' SECOND(5))"
        + " > CAST(hire_date AS TIMESTAMP)";
    sql(query).withExasol().ok(expected);
  }

  /**
   * Tests that IN can be un-parsed.
   *
   * <p>This cannot be tested using "sql", because because Calcite's SQL parser
   * replaces INs with ORs or sub-queries.
   */
  @Test void testUnparseIn1() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
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
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
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
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4876">[CALCITE-4876]
   * Converting RelNode to SQL with CalciteSqlDialect gets wrong result
   * while EnumerableIntersect is followed by EnumerableLimit</a>.
   */
  @Test void testUnparseIntersectWithLimit() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("DEPT")
        .project(b.field("DEPTNO"))
        .scan("EMP")
        .project(b.field("DEPTNO"))
        .intersect(true)
        .limit(1, 3)
        .build();
    final String expectedSql = "SELECT *\n"
        + "FROM (SELECT \"DEPTNO\"\n"
        + "FROM \"scott\".\"DEPT\"\n"
        + "INTERSECT ALL\n"
        + "SELECT \"DEPTNO\"\n"
        + "FROM \"scott\".\"EMP\")\n"
        + "OFFSET 1 ROWS\n"
        + "FETCH NEXT 3 ROWS ONLY";
    relFn(relFn).ok(expectedSql);
  }

  @Test void testSelectQueryWithLimitClause() {
    String query = "select \"product_id\" from \"product\" limit 100 offset 10";
    final String expected = "SELECT product_id\n"
        + "FROM foodmart.product\n"
        + "LIMIT 100\n"
        + "OFFSET 10";
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
        + "    4 as \"fo$ur\", 5 as \"ignore\", 6 as \"si`x\"\n"
        + "  from \"foodmart\".\"days\") as \"my$table\"\n"
        + "where \"one\" < \"tWo\" and \"THREE\" < \"fo$ur\"";
    final String expectedBigQuery = "SELECT *\n"
        + "FROM (SELECT 1 AS one, 2 AS tWo, 3 AS THREE,"
        + " 4 AS `fo$ur`, 5 AS `ignore`, 6 AS `si\\`x`\n"
        + "FROM foodmart.days) AS t\n"
        + "WHERE one < tWo AND THREE < `fo$ur`";
    final String expectedMysql =  "SELECT *\n"
        + "FROM (SELECT 1 AS `one`, 2 AS `tWo`, 3 AS `THREE`,"
        + " 4 AS `fo$ur`, 5 AS `ignore`, 6 AS `si``x`\n"
        + "FROM `foodmart`.`days`) AS `t`\n"
        + "WHERE `one` < `tWo` AND `THREE` < `fo$ur`";
    final String expectedPostgresql = "SELECT *\n"
        + "FROM (SELECT 1 AS \"one\", 2 AS \"tWo\", 3 AS \"THREE\","
        + " 4 AS \"fo$ur\", 5 AS \"ignore\", 6 AS \"si`x\"\n"
        + "FROM \"foodmart\".\"days\") AS \"t\"\n"
        + "WHERE \"one\" < \"tWo\" AND \"THREE\" < \"fo$ur\"";
    final String expectedOracle = expectedPostgresql.replace(" AS ", " ");
    final String expectedExasol = "SELECT *\n"
        + "FROM (SELECT 1 AS one, 2 AS tWo, 3 AS THREE,"
        + " 4 AS \"fo$ur\", 5 AS \"ignore\", 6 AS \"si`x\"\n"
        + "FROM foodmart.days) AS t\n"
        + "WHERE one < tWo AND THREE < \"fo$ur\"";
    sql(query)
        .withBigQuery().ok(expectedBigQuery)
        .withMysql().ok(expectedMysql)
        .withOracle().ok(expectedOracle)
        .withPostgresql().ok(expectedPostgresql)
        .withExasol().ok(expectedExasol);
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

  @Test void testExceptOperatorForBigQuery() {
    final String query = "select mod(11,3) from \"product\"\n"
        + "EXCEPT select 1 from \"product\"";
    final String expected = "SELECT MOD(11, 3)\n"
        + "FROM foodmart.product\n"
        + "EXCEPT DISTINCT\n"
        + "SELECT 1\n"
        + "FROM foodmart.product";
    sql(query).withBigQuery().ok(expected);
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

  @Test void testHiveSubstring() {
    String query = "SELECT SUBSTRING('ABC', 2)"
            + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT SUBSTRING('ABC', 2)\n"
            + "FROM foodmart.reserve_employee";
    sql(query).withHive().ok(expected);
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
    final String expected = "SELECT *\n"
        + "FROM `foodmart`.`employee`\n"
        + "WHERE (`hire_date` - INTERVAL '19800' SECOND)"
        + " > CAST(`hire_date` AS DATETIME)";
    sql(query).withMysql().ok(expected);
  }

  @Test void testMySqlCastToVarcharWithGreaterThanMaxPrecision() {
    final String query = "select cast(\"product_id\" as varchar(500)), \"product_id\" "
        + "from \"product\" ";
    final String expected = "SELECT CAST(`product_id` AS CHAR(255)), `product_id`\n"
        + "FROM `foodmart`.`product`";
    sql(query).withMysql().ok(expected);
  }

  @Test void testMySqlUnparseListAggCall() {
    final String query = "select\n"
        + "listagg(distinct \"product_name\", ',') within group(order by \"cases_per_pallet\"),\n"
        + "listagg(\"product_name\", ',') within group(order by \"cases_per_pallet\"),\n"
        + "listagg(distinct \"product_name\") within group(order by \"cases_per_pallet\" desc),\n"
        + "listagg(distinct \"product_name\", ',') within group(order by \"cases_per_pallet\"),\n"
        + "listagg(\"product_name\"),\n"
        + "listagg(\"product_name\", ',')\n"
        + "from \"product\"\n"
        + "group by \"product_id\"\n";
    final String expected = "SELECT GROUP_CONCAT(DISTINCT `product_name` "
        + "ORDER BY `cases_per_pallet` IS NULL, `cases_per_pallet` SEPARATOR ','), "
        + "GROUP_CONCAT(`product_name` "
        + "ORDER BY `cases_per_pallet` IS NULL, `cases_per_pallet` SEPARATOR ','), "
        + "GROUP_CONCAT(DISTINCT `product_name` "
        + "ORDER BY `cases_per_pallet` IS NULL DESC, `cases_per_pallet` DESC), "
        + "GROUP_CONCAT(DISTINCT `product_name` "
        + "ORDER BY `cases_per_pallet` IS NULL, `cases_per_pallet` SEPARATOR ','), "
        + "GROUP_CONCAT(`product_name`), GROUP_CONCAT(`product_name` SEPARATOR ',')\n"
        + "FROM `foodmart`.`product`\n"
        + "GROUP BY `product_id`";
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
        .withClickHouse().ok(expectedClickHouse)
        .withMysql().ok(expectedMysql);
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
        .withClickHouse().ok(expectedClickHouse);

    final String expectedPresto = "SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "OFFSET 10\n"
        + "LIMIT 100";
    sql(query)
        .ok(expected)
        .withPresto().ok(expectedPresto);
  }

  @Test void testSelectQueryWithLimitOffsetClause() {
    String query = "select \"product_id\" from \"product\"\n"
        + "order by \"net_weight\" asc limit 100 offset 10";
    final String expected = "SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "ORDER BY \"net_weight\"\n"
        + "OFFSET 10 ROWS\n"
        + "FETCH NEXT 100 ROWS ONLY";
    // BigQuery uses LIMIT/OFFSET, and nulls sort low by default
    final String expectedBigQuery = "SELECT product_id\n"
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
    final String expectedMssql = "SELECT TOP (100) [product_id]\n"
        + "FROM [foodmart].[product]\n"
        + "ORDER BY CASE WHEN [product_id] IS NULL THEN 1 ELSE 0 END, [product_id]";
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
        + "FROM foodmart.sales_fact_1997 AS sales_fact_19970) AS t0 ON t.customer_id = t0.customer_id";

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
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
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

  @Test void testCaseOnSubQuery() {
    String query = "SELECT CASE WHEN v.g IN (0, 1) THEN 0 ELSE 1 END\n"
        + "FROM (SELECT * FROM \"foodmart\".\"customer\") AS c,\n"
        + "  (SELECT 0 AS g) AS v\n"
        + "GROUP BY v.g";
    final String expected = "SELECT"
        + " CASE WHEN \"t0\".\"G\" IN (0, 1) THEN 0 ELSE 1 END\n"
        + "FROM (SELECT *\n"
        + "FROM \"foodmart\".\"customer\") AS \"t\",\n"
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
        + "FROM \"foodmart\".\"department\"\n"
        + "INNER JOIN "
        + "(SELECT \"department_id\"\n"
        + "FROM \"foodmart\".\"employee\"\n"
        + "WHERE \"store_id\" < 150\n"
        + "GROUP BY \"department_id\") AS \"t1\" "
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
        .withDb2().ok(expectedDb2);
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
    String expected = "SELECT *\n"
        + "FROM \"foodmart\".\"sales_fact_1997\"\n"
        + "INNER JOIN \"foodmart\".\"customer\" "
        + "ON \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\""
        + " OR \"sales_fact_1997\".\"customer_id\" IS NULL"
        + " AND \"customer\".\"customer_id\" IS NULL"
        + " OR \"customer\".\"occupation\" IS NULL\n"
        + "INNER JOIN \"foodmart\".\"product\" "
        + "ON \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\""
        + " OR \"sales_fact_1997\".\"product_id\" IS NOT NULL"
        + " OR \"product\".\"product_id\" IS NOT NULL";
    // The hook prevents RelBuilder from removing "FALSE AND FALSE" and such
    try (Hook.Closeable ignore =
             Hook.REL_BUILDER_SIMPLIFY.addThread(Hook.propertyJ(false))) {
      sql(query).ok(expected);
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4610">[CALCITE-4610]
   * Join on range causes AssertionError in RelToSqlConverter</a>. */
  @Test void testJoinOnRange() {
    final String sql = "SELECT d.deptno, e.deptno\n"
        + "FROM dept d\n"
        + "LEFT JOIN emp e\n"
        + " ON d.deptno = e.deptno\n"
        + " AND d.deptno < 15\n"
        + " AND d.deptno > 10\n"
        + "WHERE e.job LIKE 'PRESIDENT'";
    final String expected = "SELECT \"DEPT\".\"DEPTNO\","
        + " \"EMP\".\"DEPTNO\" AS \"DEPTNO0\"\n"
        + "FROM \"SCOTT\".\"DEPT\"\n"
        + "LEFT JOIN \"SCOTT\".\"EMP\" "
        + "ON \"DEPT\".\"DEPTNO\" = \"EMP\".\"DEPTNO\" "
        + "AND (\"DEPT\".\"DEPTNO\" > 10"
        + " AND \"DEPT\".\"DEPTNO\" < 15)\n"
        + "WHERE \"EMP\".\"JOB\" LIKE 'PRESIDENT'";
    sql(sql)
        .schema(CalciteAssert.SchemaSpec.JDBC_SCOTT)
        .ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4620">[CALCITE-4620]
   * Join on CASE causes AssertionError in RelToSqlConverter</a>. */
  @Test void testJoinOnCase() {
    final String sql = "SELECT d.deptno, e.deptno\n"
        + "FROM dept AS d LEFT JOIN emp AS e\n"
        + " ON CASE WHEN e.job = 'PRESIDENT' THEN true ELSE d.deptno = 10 END\n"
        + "WHERE e.job LIKE 'PRESIDENT'";
    final String expected = "SELECT \"DEPT\".\"DEPTNO\","
        + " \"EMP\".\"DEPTNO\" AS \"DEPTNO0\"\n"
        + "FROM \"SCOTT\".\"DEPT\"\n"
        + "LEFT JOIN \"SCOTT\".\"EMP\""
        + " ON CASE WHEN \"EMP\".\"JOB\" = 'PRESIDENT' THEN TRUE"
        + " ELSE CAST(\"DEPT\".\"DEPTNO\" AS INTEGER) = 10 END\n"
        + "WHERE \"EMP\".\"JOB\" LIKE 'PRESIDENT'";
    sql(sql)
        .schema(CalciteAssert.SchemaSpec.JDBC_SCOTT)
        .ok(expected);
  }

  @Test void testWhereCase() {
    final String sql = "SELECT d.deptno, e.deptno\n"
        + "FROM dept AS d LEFT JOIN emp AS e ON d.deptno = e.deptno\n"
        + "WHERE CASE WHEN e.job = 'PRESIDENT' THEN true\n"
        + "      ELSE d.deptno = 10 END\n";
    final String expected = "SELECT \"DEPT\".\"DEPTNO\","
        + " \"EMP\".\"DEPTNO\" AS \"DEPTNO0\"\n"
        + "FROM \"SCOTT\".\"DEPT\"\n"
        + "LEFT JOIN \"SCOTT\".\"EMP\""
        + " ON \"DEPT\".\"DEPTNO\" = \"EMP\".\"DEPTNO\"\n"
        + "WHERE CASE WHEN \"EMP\".\"JOB\" = 'PRESIDENT' THEN TRUE"
        + " ELSE CAST(\"DEPT\".\"DEPTNO\" AS INTEGER) = 10 END";
    sql(sql)
        .schema(CalciteAssert.SchemaSpec.JDBC_SCOTT)
        .ok(expected);
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


  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5013">[CALCITE-5013]
   * Unparse SqlSetOperator should be retained parentheses
   * when the operand has limit or offset</a>. */
  @Test void testSetOpRetainParentheses() {
    // Parentheses will be discarded, because semantics not be affected.
    final String discardedParenthesesQuery = "SELECT \"product_id\" FROM \"product\""
        + "UNION ALL\n"
        + "(SELECT \"product_id\" FROM \"product\" WHERE \"product_id\" > 10)\n"
        + "INTERSECT ALL\n"
        + "(SELECT \"product_id\" FROM \"product\" )";
    final String discardedParenthesesRes = "SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "UNION ALL\n"
        + "SELECT *\n"
        + "FROM (SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE \"product_id\" > 10\n"
        + "INTERSECT ALL\n"
        + "SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"product\")";
    sql(discardedParenthesesQuery).ok(discardedParenthesesRes);

    // Parentheses will be retained because sub-query has LIMIT or OFFSET.
    // If parentheses are discarded the semantics of parsing will be affected.
    final String allSetOpQuery = "SELECT \"product_id\" FROM \"product\""
        + "UNION ALL\n"
        + "(SELECT \"product_id\" FROM \"product\" LIMIT 10)\n"
        + "INTERSECT ALL\n"
        + "(SELECT \"product_id\" FROM \"product\" OFFSET 10)\n"
        + "EXCEPT ALL\n"
        + "(SELECT \"product_id\" FROM \"product\" LIMIT 5 OFFSET 5)";
    final String allSetOpRes = "SELECT *\n"
        + "FROM (SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "UNION ALL\n"
        + "SELECT *\n"
        + "FROM ((SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "FETCH NEXT 10 ROWS ONLY)\n"
        + "INTERSECT ALL\n"
        + "(SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "OFFSET 10 ROWS)))\n"
        + "EXCEPT ALL\n"
        + "(SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "OFFSET 5 ROWS\n"
        + "FETCH NEXT 5 ROWS ONLY)";
    sql(allSetOpQuery).ok(allSetOpRes);

    // After the config is enabled, order by will be retained, so parentheses are required.
    final String retainOrderQuery = "SELECT \"product_id\" FROM \"product\""
        + "UNION ALL\n"
        + "(SELECT \"product_id\" FROM \"product\" ORDER BY \"product_id\")";
    final String retainOrderResult = "SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "UNION ALL\n"
        + "(SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "ORDER BY \"product_id\")";
    sql(retainOrderQuery).withConfig(c -> c.withRemoveSortInSubQuery(false)).ok(retainOrderResult);

    // Parentheses are required to keep ORDER and LIMIT on the sub-query.
    final String retainLimitQuery = "SELECT \"product_id\" FROM \"product\""
        + "UNION ALL\n"
        + "(SELECT \"product_id\" FROM \"product\" ORDER BY \"product_id\" LIMIT 2)";
    final String retainLimitResult = "SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "UNION ALL\n"
        + "(SELECT \"product_id\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "ORDER BY \"product_id\"\n"
        + "FETCH NEXT 2 ROWS ONLY)";
    sql(retainLimitQuery).ok(retainLimitResult);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4674">[CALCITE-4674]
   * Excess quotes in generated SQL when STAR is a column alias</a>. */
  @Test void testAliasOnStarNoExcessQuotes() {
    final String query = "select \"customer_id\" as \"*\" from \"customer\"";
    final String expected = "SELECT \"customer_id\" AS \"*\"\n"
        + "FROM \"foodmart\".\"customer\"";
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
    String expectedHsqldb = "SELECT *\n"
        + "FROM (VALUES (" + expected + ")) AS t (EXPR$0)";
    sql("VALUES " + expression)
        .withHsqldb().ok(expectedHsqldb);
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
            + "max(sum(\"employee_id\")) over (partition by \"hire_date\" order by \"employee_id\") as count1, "
            + "max(sum(\"employee_id\")) over (partition by \"birth_date\" order by \"employee_id\") as count2\n"
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
        + "COUNT(DISTINCT \"employee_id\") OVER (ORDER BY \"hire_date\""
        + " RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS \"$0\"\n"
        + "FROM \"foodmart\".\"employee\"";

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

    String expect = "SELECT SUM(CAST(\"t\".\"EXPR$0\" * \"t0\".\"$f1\" AS DECIMAL"
        + "(19, 4))), SUM(CAST(\"t\".\"$f2\" * \"t0\".\"EXPR$1\" AS DECIMAL(19, 4)))\n"
        + "FROM (SELECT \"product_id\", SUM(\"store_sales\") AS \"EXPR$0\", COUNT(*) AS \"$f2\"\n"
        + "FROM \"foodmart\".\"sales_fact_dec_1998\"\n"
        + "GROUP BY \"product_id\") AS \"t\"\n"
        + "INNER JOIN "
        + "(SELECT \"product_id\", COUNT(*) AS \"$f1\", SUM(\"store_sales\") AS \"EXPR$1\"\n"
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

  @Test void testMultiplicationNotAliasedToStar() {
    final String sql = "select s.\"customer_id\", sum(s.\"store_sales\" * s.\"store_cost\")"
        + "from \"sales_fact_1997\" as s\n"
        + "join \"customer\" as c\n"
        + "  on s.\"customer_id\" = c.\"customer_id\"\n"
        + "group by s.\"customer_id\"";
    final String expected = "SELECT \"t\".\"customer_id\", SUM(\"t\".\"$f1\")\n"
        + "FROM (SELECT \"customer_id\", \"store_sales\" * \"store_cost\" AS \"$f1\"\n"
        + "FROM \"foodmart\".\"sales_fact_1997\") AS \"t\"\n"
        + "INNER JOIN (SELECT \"customer_id\"\n"
        + "FROM \"foodmart\".\"customer\") AS \"t0\" ON \"t\".\"customer_id\" = \"t0\".\"customer_id\"\n"
        + "GROUP BY \"t\".\"customer_id\"";
    RuleSet rules = RuleSets.ofList(CoreRules.PROJECT_JOIN_TRANSPOSE);
    sql(sql).optimize(rules, null).ok(expected);
  }

  @Test void testMultiplicationRetainsExplicitAlias() {
    final String sql = "select s.\"customer_id\", s.\"store_sales\" * s.\"store_cost\" as \"total\""
        + "from \"sales_fact_1997\" as s\n"
        + "join \"customer\" as c\n"
        + "  on s.\"customer_id\" = c.\"customer_id\"\n";
    final String expected = "SELECT \"t\".\"customer_id\", \"t\".\"total\"\n"
        + "FROM (SELECT \"customer_id\", \"store_sales\" * \"store_cost\" AS \"total\"\n"
        + "FROM \"foodmart\".\"sales_fact_1997\") AS \"t\"\n"
        + "INNER JOIN (SELECT \"customer_id\"\n"
        + "FROM \"foodmart\".\"customer\") AS \"t0\" ON \"t\".\"customer_id\" = \"t0\""
        + ".\"customer_id\"";
    RuleSet rules = RuleSets.ofList(CoreRules.PROJECT_JOIN_TRANSPOSE);
    sql(sql).optimize(rules, null).ok(expected);
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
        .withPostgresql().ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1798">[CALCITE-1798]
   * Generate dialect-specific SQL for FLOOR operator</a>. */
  @Test void testFloor() {
    String query = "SELECT floor(\"hire_date\" TO MINUTE) FROM \"employee\"";
    String expectedClickHouse = "SELECT toStartOfMinute(`hire_date`)\n"
        + "FROM `foodmart`.`employee`";
    String expectedHsqldb = "SELECT TRUNC(hire_date, 'MI')\n"
        + "FROM foodmart.employee";
    String expectedOracle = "SELECT TRUNC(\"hire_date\", 'MINUTE')\n"
        + "FROM \"foodmart\".\"employee\"";
    String expectedPostgresql = "SELECT DATE_TRUNC('MINUTE', \"hire_date\")\n"
        + "FROM \"foodmart\".\"employee\"";
    String expectedPresto = "SELECT DATE_TRUNC('MINUTE', \"hire_date\")\n"
        + "FROM \"foodmart\".\"employee\"";
    String expectedFirebolt = expectedPostgresql;
    sql(query)
        .withClickHouse().ok(expectedClickHouse)
        .withFirebolt().ok(expectedFirebolt)
        .withHsqldb().ok(expectedHsqldb)
        .withOracle().ok(expectedOracle)
        .withPostgresql().ok(expectedPostgresql)
        .withPresto().ok(expectedPresto);
  }

  @Test void testFloorMssqlWeek() {
    String query = "SELECT floor(\"hire_date\" TO WEEK) FROM \"employee\"";
    String expected = "SELECT CONVERT(DATETIME, CONVERT(VARCHAR(10), "
        + "DATEADD(day, - (6 + DATEPART(weekday, [hire_date] )) % 7, [hire_date] ), 126))\n"
        + "FROM [foodmart].[employee]";
    sql(query)
        .withMssql().ok(expected);
  }

  @Test void testFetchMssql() {
    String query = "SELECT * FROM \"employee\" LIMIT 1";
    String expected = "SELECT TOP (1) *\nFROM [foodmart].[employee]";
    sql(query)
        .withMssql().ok(expected);
  }

  @Test void testFetchOffset() {
    String query = "SELECT * FROM \"employee\" LIMIT 1 OFFSET 1";
    String expectedMssql = "SELECT *\nFROM [foodmart].[employee]\nOFFSET 1 ROWS\n"
        + "FETCH NEXT 1 ROWS ONLY";
    String expectedSybase = "SELECT TOP (1) START AT 1 *\nFROM foodmart.employee";
    sql(query)
        .withMssql().ok(expectedMssql)
        .withSybase().ok(expectedSybase);
  }

  @Test void testFloorMssqlMonth() {
    String query = "SELECT floor(\"hire_date\" TO MONTH) FROM \"employee\"";
    String expected = "SELECT CONVERT(DATETIME, CONVERT(VARCHAR(7), [hire_date] , 126)+'-01')\n"
        + "FROM [foodmart].[employee]";
    sql(query)
        .withMssql().ok(expected);
  }

  @Test void testFloorMysqlMonth() {
    String query = "SELECT floor(\"hire_date\" TO MONTH) FROM \"employee\"";
    String expected = "SELECT DATE_FORMAT(`hire_date`, '%Y-%m-01')\n"
        + "FROM `foodmart`.`employee`";
    sql(query)
        .withMysql().ok(expected);
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
        .withClickHouse().ok(expectedClickHouse)
        .withMssql().ok(expectedMssql)
        .withMysql().ok(expectedMysql);
  }

  @Test void testUnparseSqlIntervalQualifierDb2() {
    String queryDatePlus = "select  * from \"employee\" where  \"hire_date\" + "
        + "INTERVAL '19800' SECOND(5) > TIMESTAMP '2005-10-17 00:00:00' ";
    String expectedDatePlus = "SELECT *\n"
        + "FROM foodmart.employee AS employee\n"
        + "WHERE (employee.hire_date + 19800 SECOND)"
        + " > TIMESTAMP '2005-10-17 00:00:00'";

    sql(queryDatePlus)
        .withDb2().ok(expectedDatePlus);

    String queryDateMinus = "select  * from \"employee\" where  \"hire_date\" - "
        + "INTERVAL '19800' SECOND(5) > TIMESTAMP '2005-10-17 00:00:00' ";
    String expectedDateMinus = "SELECT *\n"
        + "FROM foodmart.employee AS employee\n"
        + "WHERE (employee.hire_date - 19800 SECOND)"
        + " > TIMESTAMP '2005-10-17 00:00:00'";

    sql(queryDateMinus)
        .withDb2().ok(expectedDateMinus);
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
        + "WHERE DATEADD(SECOND, 19800, [hire_date]) > '2005-10-17 00:00:00'";

    sql(queryDatePlus)
        .withMssql().ok(expectedDatePlus);

    String queryDateMinus = "select  * from \"employee\" where  \"hire_date\" -"
        + "INTERVAL '19800' SECOND(5) > TIMESTAMP '2005-10-17 00:00:00' ";
    String expectedDateMinus = "SELECT *\n"
        + "FROM [foodmart].[employee]\n"
        + "WHERE DATEADD(SECOND, -19800, [hire_date]) > '2005-10-17 00:00:00'";

    sql(queryDateMinus)
        .withMssql().ok(expectedDateMinus);

    String queryDateMinusNegate = "select  * from \"employee\" "
        + "where  \"hire_date\" -INTERVAL '-19800' SECOND(5)"
        + " > TIMESTAMP '2005-10-17 00:00:00' ";
    String expectedDateMinusNegate = "SELECT *\n"
        + "FROM [foodmart].[employee]\n"
        + "WHERE DATEADD(SECOND, 19800, [hire_date]) > '2005-10-17 00:00:00'";

    sql(queryDateMinusNegate)
        .withMssql().ok(expectedDateMinusNegate);
  }

  @Test void testUnparseSqlIntervalQualifierBigQuery() {
    final String sql0 = "select  * from \"employee\" where  \"hire_date\" - "
            + "INTERVAL '19800' SECOND(5) > TIMESTAMP '2005-10-17 00:00:00' ";
    final String expect0 = "SELECT *\n"
            + "FROM foodmart.employee\n"
            + "WHERE (hire_date - INTERVAL 19800 SECOND)"
            + " > TIMESTAMP '2005-10-17 00:00:00'";
    sql(sql0).withBigQuery().ok(expect0);

    final String sql1 = "select  * from \"employee\" where  \"hire_date\" + "
            + "INTERVAL '10' HOUR > TIMESTAMP '2005-10-17 00:00:00' ";
    final String expect1 = "SELECT *\n"
            + "FROM foodmart.employee\n"
            + "WHERE (hire_date + INTERVAL 10 HOUR)"
            + " > TIMESTAMP '2005-10-17 00:00:00'";
    sql(sql1).withBigQuery().ok(expect1);

    final String sql2 = "select  * from \"employee\" where  \"hire_date\" + "
            + "INTERVAL '1 2:34:56.78' DAY TO SECOND > TIMESTAMP '2005-10-17 00:00:00' ";
    sql(sql2).withBigQuery().throws_("Only INT64 is supported as the interval value for BigQuery.");
  }

  @Test void testUnparseSqlIntervalQualifierFirebolt() {
    final String sql0 = "select  * from \"employee\" where  \"hire_date\" - "
        + "INTERVAL '19800' SECOND(5) > TIMESTAMP '2005-10-17 00:00:00' ";
    final String expect0 = "SELECT *\n"
        + "FROM \"foodmart\".\"employee\"\n"
        + "WHERE (\"hire_date\" - INTERVAL '19800 SECOND ')"
        + " > TIMESTAMP '2005-10-17 00:00:00'";
    sql(sql0).withFirebolt().ok(expect0);

    final String sql1 = "select  * from \"employee\" where  \"hire_date\" + "
        + "INTERVAL '10' HOUR > TIMESTAMP '2005-10-17 00:00:00' ";
    final String expect1 = "SELECT *\n"
        + "FROM \"foodmart\".\"employee\"\n"
        + "WHERE (\"hire_date\" + INTERVAL '10 HOUR ')"
        + " > TIMESTAMP '2005-10-17 00:00:00'";
    sql(sql1).withFirebolt().ok(expect1);

    final String sql2 = "select  * from \"employee\" where  \"hire_date\" + "
        + "INTERVAL '1 2:34:56.78' DAY TO SECOND > TIMESTAMP '2005-10-17 00:00:00' ";
    sql(sql2).withFirebolt().throws_("Only INT64 is supported as the interval value for Firebolt.");
  }

  @Test void testFloorMysqlWeek() {
    String query = "SELECT floor(\"hire_date\" TO WEEK) FROM \"employee\"";
    String expected = "SELECT STR_TO_DATE(DATE_FORMAT(`hire_date` , '%x%v-1'), '%x%v-%w')\n"
        + "FROM `foodmart`.`employee`";
    sql(query)
        .withMysql().ok(expected);
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
        .withClickHouse().ok(expectedClickHouse)
        .withMssql().ok(expectedMssql)
        .withMysql().ok(expectedMysql);
  }

  @Test void testFloorMysqlHour() {
    String query = "SELECT floor(\"hire_date\" TO HOUR) FROM \"employee\"";
    String expected = "SELECT DATE_FORMAT(`hire_date`, '%Y-%m-%d %H:00:00')\n"
        + "FROM `foodmart`.`employee`";
    sql(query)
        .withMysql().ok(expected);
  }

  @Test void testFloorMysqlMinute() {
    String query = "SELECT floor(\"hire_date\" TO MINUTE) FROM \"employee\"";
    String expected = "SELECT DATE_FORMAT(`hire_date`, '%Y-%m-%d %H:%i:00')\n"
        + "FROM `foodmart`.`employee`";
    sql(query)
        .withMysql().ok(expected);
  }

  @Test void testFloorMysqlSecond() {
    String query = "SELECT floor(\"hire_date\" TO SECOND) FROM \"employee\"";
    String expected = "SELECT DATE_FORMAT(`hire_date`, '%Y-%m-%d %H:%i:%s')\n"
        + "FROM `foodmart`.`employee`";
    sql(query)
        .withMysql().ok(expected);
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
    final String expectedFirebolt = expectedPostgresql;
    sql(query)
        .withClickHouse().ok(expectedClickHouse)
        .withFirebolt().ok(expectedFirebolt)
        .withHsqldb().ok(expected)
        .withMysql().ok(expectedMysql)
        .withOracle().ok(expectedOracle)
        .withPostgresql().ok(expectedPostgresql);
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
    final String expectedSnowflake = expectedPostgresql;
    final String expectedRedshift = expectedPostgresql;
    final String expectedFirebolt = expectedPresto;
    final String expectedMysql = "SELECT SUBSTRING(`brand_name` FROM 2)\n"
        + "FROM `foodmart`.`product`";
    sql(query)
        .withClickHouse().ok(expectedClickHouse)
        .withFirebolt().ok(expectedFirebolt)
        .withMssql()
        // mssql does not support this syntax and so should fail
        .throws_("MSSQL SUBSTRING requires FROM and FOR arguments")
        .withMysql().ok(expectedMysql)
        .withOracle().ok(expectedOracle)
        .withPostgresql().ok(expectedPostgresql)
        .withPresto().ok(expectedPresto)
        .withRedshift().ok(expectedRedshift)
        .withSnowflake().ok(expectedSnowflake);
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
    final String expectedSnowflake = expectedPostgresql;
    final String expectedRedshift = expectedPostgresql;
    final String expectedFirebolt = expectedPresto;
    final String expectedMysql = "SELECT SUBSTRING(`brand_name` FROM 2 FOR 3)\n"
        + "FROM `foodmart`.`product`";
    final String expectedMssql = "SELECT SUBSTRING([brand_name], 2, 3)\n"
        + "FROM [foodmart].[product]";
    sql(query)
        .withClickHouse().ok(expectedClickHouse)
        .withFirebolt().ok(expectedFirebolt)
        .withMysql().ok(expectedMysql)
        .withMssql().ok(expectedMssql)
        .withOracle().ok(expectedOracle)
        .withPostgresql().ok(expectedPostgresql)
        .withPresto().ok(expectedPresto)
        .withRedshift().ok(expectedRedshift)
        .withSnowflake().ok(expectedSnowflake);
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

  @Test void testIlike() {
    String query = "select \"product_name\" from \"product\" a "
        + "where \"product_name\" ilike 'abC'";
    String expected = "SELECT \"product_name\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE \"product_name\" ILIKE 'abC'";
    sql(query).withLibrary(SqlLibrary.POSTGRESQL).ok(expected);
  }

  @Test void testRlike() {
    String query = "select \"product_name\" from \"product\" a "
        + "where \"product_name\" rlike '.+@.+\\\\..+'";
    String expectedSpark = "SELECT \"product_name\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE \"product_name\" RLIKE '.+@.+\\\\..+'";
    String expectedHive = "SELECT \"product_name\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE \"product_name\" RLIKE '.+@.+\\\\..+'";
    sql(query)
        .withLibrary(SqlLibrary.SPARK).ok(expectedSpark)
        .withLibrary(SqlLibrary.HIVE).ok(expectedHive);
  }

  @Test void testNotRlike() {
    String query = "select \"product_name\" from \"product\" a "
        + "where \"product_name\" not rlike '.+@.+\\\\..+'";
    String expected = "SELECT \"product_name\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE \"product_name\" NOT RLIKE '.+@.+\\\\..+'";
    sql(query).withLibrary(SqlLibrary.SPARK).ok(expected);
  }

  @Test void testNotIlike() {
    String query = "select \"product_name\" from \"product\" a "
        + "where \"product_name\" not ilike 'abC'";
    String expected = "SELECT \"product_name\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE \"product_name\" NOT ILIKE 'abC'";
    sql(query).withLibrary(SqlLibrary.POSTGRESQL).ok(expected);
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
        + "FINAL MATCH_NUMBER() AS \"MATCH_NUM\", "
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
    final String expectedClickHouse = "SELECT `a`\n"
        + "FROM (SELECT 1 AS `a`, 'x ' AS `b`\n"
        + "UNION ALL\n"
        + "SELECT 2 AS `a`, 'yy' AS `b`)"; // almost the same as MySQL
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
    final String expectedBigQuery = "SELECT a\n"
        + "FROM (SELECT 1 AS a, 'x ' AS b\n"
        + "UNION ALL\n"
        + "SELECT 2 AS a, 'yy' AS b)";
    final String expectedFirebolt = expectedPostgresql;
    final String expectedSnowflake = expectedPostgresql;
    final String expectedRedshift = "SELECT \"a\"\n"
        + "FROM (SELECT 1 AS \"a\", 'x ' AS \"b\"\n"
        + "UNION ALL\nSELECT 2 AS \"a\", 'yy' AS \"b\")";
    sql(sql)
        .withClickHouse().ok(expectedClickHouse)
        .withFirebolt().ok(expectedFirebolt)
        .withBigQuery().ok(expectedBigQuery)
        .withHive().ok(expectedHive)
        .withHsqldb().ok(expectedHsqldb)
        .withMysql().ok(expectedMysql)
        .withOracle().ok(expectedOracle)
        .withPostgresql().ok(expectedPostgresql)
        .withRedshift().ok(expectedRedshift)
        .withSnowflake().ok(expectedSnowflake);
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5179">[CALCITE-5179]
   * In RelToSqlConverter, AssertionError for values with more than two items
   * when SqlDialect#supportsAliasedValues is false</a>.
   */
  @Test void testThreeValues() {
    final String sql = "select * from (values (1), (2), (3)) as t(\"a\")\n";
    sql(sql)
        .withRedshift().ok("SELECT *\n"
            + "FROM (SELECT 1 AS \"a\"\n"
            + "UNION ALL\n"
            + "SELECT 2 AS \"a\"\n"
            + "UNION ALL\n"
            + "SELECT 3 AS \"a\")");
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
    final String expectedClickHouse = expectedMysql;
    sql(sql)
        .optimize(rules, null)
        .withClickHouse().ok(expectedClickHouse)
        .withMysql().ok(expectedMysql)
        .withOracle().ok(expectedOracle)
        .withPostgresql().ok(expectedPostgresql);
  }

  /** Tests SELECT without FROM clause; effectively the same as a VALUES
   * query.
   *
   * <p>Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4724">[CALCITE-4724]
   * In JDBC adapter for ClickHouse, implement Values by generating SELECT
   * without FROM</a>. */
  @Test void testSelectWithoutFrom() {
    final String query = "select 2 + 2";
    final String expectedBigQuery = "SELECT 2 + 2";
    final String expectedClickHouse = expectedBigQuery;
    final String expectedHive = expectedBigQuery;
    final String expectedMysql = expectedBigQuery;
    final String expectedPostgresql = "SELECT 2 + 2\n"
        + "FROM (VALUES (0)) AS \"t\" (\"ZERO\")";
    sql(query)
        .withBigQuery().ok(expectedBigQuery)
        .withClickHouse().ok(expectedClickHouse)
        .withHive().ok(expectedHive)
        .withMysql().ok(expectedMysql)
        .withPostgresql().ok(expectedPostgresql);
  }

  @Test void testSelectOne() {
    final String query = "select 1";
    final String expectedBigQuery = "SELECT 1";
    final String expectedClickHouse = expectedBigQuery;
    final String expectedHive = expectedBigQuery;
    final String expectedMysql = expectedBigQuery;
    final String expectedPostgresql = "SELECT *\n"
        + "FROM (VALUES (1)) AS \"t\" (\"EXPR$0\")";
    sql(query)
        .withBigQuery().ok(expectedBigQuery)
        .withClickHouse().ok(expectedClickHouse)
        .withHive().ok(expectedHive)
        .withMysql().ok(expectedMysql)
        .withPostgresql().ok(expectedPostgresql);
  }

  /** As {@link #testValuesEmpty()} but with extra {@code SUBSTRING}. Before
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4524">[CALCITE-4524]
   * Make some fields non-nullable</a> was fixed, this case would fail with
   * {@code java.lang.IndexOutOfBoundsException}. */
  @Test void testValuesEmpty2() {
    final String sql0 = "select *\n"
        + "from (values (1, 'a'), (2, 'bb')) as t(x, y)\n"
        + "limit 0";
    final String sql = "SELECT SUBSTRING(y, 1, 1) FROM (" + sql0 + ") t";
    final RuleSet rules =
        RuleSets.ofList(PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE);
    final String expected = "SELECT SUBSTRING(`Y` FROM 1 FOR 1)\n"
        + "FROM (SELECT NULL AS `X`, NULL AS `Y`) AS `t`\n"
        + "WHERE 1 = 0";
    sql(sql).optimize(rules, null).withMysql().ok(expected);
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

  @Test void testTableScanHints() {
    final RelBuilder builder = relBuilder();
    builder.getCluster().setHintStrategies(HintStrategyTable.builder()
        .hintStrategy("PLACEHOLDERS", HintPredicates.TABLE_SCAN)
        .build());
    final RelNode root = builder
        .scan("orders")
        .hints(RelHint.builder("PLACEHOLDERS")
            .hintOption("a", "b")
            .build())
        .project(builder.field("PRODUCT"))
        .build();

    final String expectedSql = "SELECT \"PRODUCT\"\n"
        + "FROM \"scott\".\"orders\"";
    assertThat(
        toSql(root, DatabaseProduct.CALCITE.getDialect()),
        isLinux(expectedSql));
    final String expectedSql2 = "SELECT PRODUCT\n"
        + "FROM scott.orders\n"
        + "/*+ PLACEHOLDERS(a = 'b') */";
    assertThat(
        toSql(root, new AnsiSqlDialect(SqlDialect.EMPTY_CONTEXT)),
        isLinux(expectedSql2));
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
        + "FROM (SELECT \"department_id\", \"department_description\", \"department_id\" + 1 AS \"$f2\"\n"
        + "FROM \"foodmart\".\"department\") AS \"$cor0\",\n"
        + "LATERAL (SELECT \"$cor0\".\"$f2\" AS \"D_PLUSONE\"\n"
        + "FROM (VALUES (TRUE)) AS \"t\" (\"EXPR$0\")) AS \"t1\"";
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4485">[CALCITE-4485]
   * JDBC adapter generates invalid SQL when one of the joins is {@code INNER
   * JOIN ... ON TRUE}</a>. */
  @Test void testCommaCrossJoin() {
    final Function<RelBuilder, RelNode> relFn = b ->
        b.scan("tpch", "customer")
            .aggregate(b.groupKey(b.field("nation_name")),
                b.count().as("cnt1"))
            .project(b.field("nation_name"), b.field("cnt1"))
            .as("cust")
            .scan("tpch", "lineitem")
            .aggregate(b.groupKey(),
                b.count().as("cnt2"))
            .project(b.field("cnt2"))
            .as("lineitem")
            .join(JoinRelType.INNER)
            .scan("tpch", "part")
            .join(JoinRelType.LEFT,
                b.equals(b.field(2, "cust", "nation_name"),
                    b.field(2, "part", "p_brand")))
            .project(b.field("cust", "nation_name"),
                b.alias(
                    b.call(SqlStdOperatorTable.MINUS,
                        b.field("cnt1"),
                        b.field("cnt2")),
                    "f1"))
            .build();

    // For documentation purposes, here is the query that was generated before
    // [CALCITE-4485] was fixed.
    final String previousPostgresql = ""
        + "SELECT \"t\".\"nation_name\", \"t\".\"cnt1\" - \"t0\".\"cnt2\" AS \"f1\"\n"
        + "FROM (SELECT \"nation_name\", COUNT(*) AS \"cnt1\"\n"
        + "FROM \"tpch\".\"customer\"\n"
        + "GROUP BY \"nation_name\") AS \"t\",\n"
        + "(SELECT COUNT(*) AS \"cnt2\"\n"
        + "FROM \"tpch\".\"lineitem\") AS \"t0\"\n"
        + "LEFT JOIN \"tpch\".\"part\" ON \"t\".\"nation_name\" = \"part\".\"p_brand\"";
    final String expectedPostgresql = ""
        + "SELECT \"t\".\"nation_name\", \"t\".\"cnt1\" - \"t0\".\"cnt2\" AS \"f1\"\n"
        + "FROM (SELECT \"nation_name\", COUNT(*) AS \"cnt1\"\n"
        + "FROM \"tpch\".\"customer\"\n"
        + "GROUP BY \"nation_name\") AS \"t\"\n"
        + "CROSS JOIN (SELECT COUNT(*) AS \"cnt2\"\n"
        + "FROM \"tpch\".\"lineitem\") AS \"t0\"\n"
        + "LEFT JOIN \"tpch\".\"part\" ON \"t\".\"nation_name\" = \"part\".\"p_brand\"";
    relFn(relFn)
        .schema(CalciteAssert.SchemaSpec.TPCH)
        .withPostgresql().ok(expectedPostgresql);
  }

  /** A cartesian product is unparsed as a CROSS JOIN on Spark,
   * comma join on other DBs.
   *
   * @see SqlDialect#emulateJoinTypeForCrossJoin()
   */
  @Test void testCrossJoinEmulation() {
    final String expectedSpark = "SELECT *\n"
        + "FROM foodmart.employee\n"
        + "CROSS JOIN foodmart.department";
    final String expectedMysql = "SELECT *\n"
        + "FROM `foodmart`.`employee`,\n"
        + "`foodmart`.`department`";
    Consumer<String> fn = sql ->
        sql(sql)
            .withSpark().ok(expectedSpark)
            .withMysql().ok(expectedMysql);
    fn.accept("select * from \"employee\", \"department\"");
    fn.accept("select * from \"employee\" cross join \"department\"");
    fn.accept("select * from \"employee\" join \"department\" on true");
  }

  /** Similar to {@link #testCommaCrossJoin()} (but uses SQL)
   * and {@link #testCrossJoinEmulation()} (but is 3 way). We generate a comma
   * join if the only joins are {@code CROSS JOIN} or
   * {@code INNER JOIN ... ON TRUE}, and if we're not on Spark. */
  @Test void testCommaCrossJoin3way() {
    String sql = "select *\n"
        + "from \"store\" as s\n"
        + "inner join \"employee\" as e on true\n"
        + "cross join \"department\" as d";
    final String expectedMysql = "SELECT *\n"
        + "FROM `foodmart`.`store`,\n"
        + "`foodmart`.`employee`,\n"
        + "`foodmart`.`department`";
    final String expectedSpark = "SELECT *\n"
        + "FROM foodmart.store\n"
        + "CROSS JOIN foodmart.employee\n"
        + "CROSS JOIN foodmart.department";
    sql(sql)
        .withMysql().ok(expectedMysql)
        .withSpark().ok(expectedSpark);
  }

  /** As {@link #testCommaCrossJoin3way()}, but shows that if there is a
   * {@code LEFT JOIN} in the FROM clause, we can't use comma-join. */
  @Test void testLeftJoinPreventsCommaJoin() {
    String sql = "select *\n"
        + "from \"store\" as s\n"
        + "left join \"employee\" as e on true\n"
        + "cross join \"department\" as d";
    final String expectedMysql = "SELECT *\n"
        + "FROM `foodmart`.`store`\n"
        + "LEFT JOIN `foodmart`.`employee` ON TRUE\n"
        + "CROSS JOIN `foodmart`.`department`";
    sql(sql).withMysql().ok(expectedMysql);
  }

  /** As {@link #testLeftJoinPreventsCommaJoin()}, but the non-cross-join
   * occurs later in the FROM clause. */
  @Test void testRightJoinPreventsCommaJoin() {
    String sql = "select *\n"
        + "from \"store\" as s\n"
        + "cross join \"employee\" as e\n"
        + "right join \"department\" as d on true";
    final String expectedMysql = "SELECT *\n"
        + "FROM `foodmart`.`store`\n"
        + "CROSS JOIN `foodmart`.`employee`\n"
        + "RIGHT JOIN `foodmart`.`department` ON TRUE";
    sql(sql).withMysql().ok(expectedMysql);
  }

  /** As {@link #testLeftJoinPreventsCommaJoin()}, but the impediment is a
   * {@code JOIN} whose condition is not {@code TRUE}. */
  @Test void testOnConditionPreventsCommaJoin() {
    String sql = "select *\n"
        + "from \"store\" as s\n"
        + "join \"employee\" as e on s.\"store_id\" = e.\"store_id\"\n"
        + "cross join \"department\" as d";
    final String expectedMysql = "SELECT *\n"
        + "FROM `foodmart`.`store`\n"
        + "INNER JOIN `foodmart`.`employee`"
        + " ON `store`.`store_id` = `employee`.`store_id`\n"
        + "CROSS JOIN `foodmart`.`department`";
    sql(sql).withMysql().ok(expectedMysql);
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
        .withPresto().ok(expectedPresto)
        .withSpark().ok(expectedInSpark);
  }

  @Test void testRollupWithGroupBy() {
    final String query = "select count(*) "
        + "from \"foodmart\".\"product\" "
        + "group by rollup(\"product_id\",\"product_class_id\")";
    final String expected = "SELECT COUNT(*)\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY ROLLUP(\"product_id\", \"product_class_id\")";
    final String expectedSpark = "SELECT COUNT(*)\n"
        + "FROM foodmart.product\n"
        + "GROUP BY product_id, product_class_id WITH ROLLUP";
    final String expectedPresto = "SELECT COUNT(*)\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY ROLLUP(\"product_id\", \"product_class_id\")";
    sql(query)
        .ok(expected)
        .withPresto().ok(expectedPresto)
        .withSpark().ok(expectedSpark);
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

  @Test void testJsonRemove() {
    String query = "select json_remove(\"product_name\", '$[0]') from \"product\"";
    final String expected = "SELECT JSON_REMOVE(\"product_name\", '$[0]')\n"
           + "FROM \"foodmart\".\"product\"";
    sql(query).ok(expected);
  }

  @Test public void testJsonInsert() {
    String query0 = "select json_insert(\"product_name\", '$', 10) from \"product\"";
    String query1 = "select json_insert(cast(null as varchar), '$', 10, '$', null, '$',"
        + " '\n\t\n') from \"product\"";
    final String expected0 = "SELECT JSON_INSERT(\"product_name\", '$', 10)\n"
        + "FROM \"foodmart\".\"product\"";
    final String expected1 = "SELECT JSON_INSERT(NULL, '$', 10, '$', NULL, '$', "
        + "u&'\\000a\\0009\\000a')\nFROM \"foodmart\".\"product\"";
    sql(query0).ok(expected0);
    sql(query1).ok(expected1);
  }

  @Test public void testJsonReplace() {
    String query = "select json_replace(\"product_name\", '$', 10) from \"product\"";
    String query1 = "select json_replace(cast(null as varchar), '$', 10, '$', null, '$',"
        + " '\n\t\n') from \"product\"";
    final String expected = "SELECT JSON_REPLACE(\"product_name\", '$', 10)\n"
        + "FROM \"foodmart\".\"product\"";
    final String expected1 = "SELECT JSON_REPLACE(NULL, '$', 10, '$', NULL, '$', "
        + "u&'\\000a\\0009\\000a')\nFROM \"foodmart\".\"product\"";
    sql(query).ok(expected);
    sql(query1).ok(expected1);
  }

  @Test public void testJsonSet() {
    String query = "select json_set(\"product_name\", '$', 10) from \"product\"";
    String query1 = "select json_set(cast(null as varchar), '$', 10, '$', null, '$',"
        + " '\n\t\n') from \"product\"";
    final String expected = "SELECT JSON_SET(\"product_name\", '$', 10)\n"
        + "FROM \"foodmart\".\"product\"";
    final String expected1 = "SELECT JSON_SET(NULL, '$', 10, '$', NULL, '$', "
        + "u&'\\000a\\0009\\000a')\nFROM \"foodmart\".\"product\"";
    sql(query).ok(expected);
    sql(query1).ok(expected1);
  }

  @Test void testUnionAllWithNoOperandsUsingOracleDialect() {
    String query = "select A.\"department_id\" "
        + "from \"foodmart\".\"employee\" A "
        + " where A.\"department_id\" = ( select min( A.\"department_id\") from \"foodmart\".\"department\" B where 1=2 )";
    final String expected = "SELECT \"employee\".\"department_id\"\n"
        + "FROM \"foodmart\".\"employee\"\n"
        + "INNER JOIN (SELECT \"t1\".\"department_id\" \"department_id0\", MIN(\"t1\".\"department_id\") \"EXPR$0\"\n"
        + "FROM (SELECT NULL \"department_id\", NULL \"department_description\"\n"
        + "FROM \"DUAL\"\n"
        + "WHERE 1 = 0) \"t\",\n"
        + "(SELECT \"department_id\"\n"
        + "FROM \"foodmart\".\"employee\"\n"
        + "GROUP BY \"department_id\") \"t1\"\n"
        + "GROUP BY \"t1\".\"department_id\"\n"
        + "HAVING \"t1\".\"department_id\" = MIN(\"t1\".\"department_id\")) \"t4\" ON \"employee\".\"department_id\" = \"t4\".\"department_id0\"";
    sql(query).withOracle().ok(expected);
  }

  @Test void testUnionAllWithNoOperands() {
    String query = "select A.\"department_id\" "
        + "from \"foodmart\".\"employee\" A "
        + " where A.\"department_id\" = ( select min( A.\"department_id\") from \"foodmart\".\"department\" B where 1=2 )";
    final String expected = "SELECT \"employee\".\"department_id\"\n"
        + "FROM \"foodmart\".\"employee\"\n"
        + "INNER JOIN (SELECT \"t1\".\"department_id\" AS \"department_id0\", MIN(\"t1\".\"department_id\") AS \"EXPR$0\"\n"
        + "FROM (SELECT *\n"
        + "FROM (VALUES (NULL, NULL)) AS \"t\" (\"department_id\", \"department_description\")\n"
        + "WHERE 1 = 0) AS \"t\",\n"
        + "(SELECT \"department_id\"\n"
        + "FROM \"foodmart\".\"employee\"\n"
        + "GROUP BY \"department_id\") AS \"t1\"\n"
        + "GROUP BY \"t1\".\"department_id\"\n"
        + "HAVING \"t1\".\"department_id\" = MIN(\"t1\".\"department_id\")) AS \"t4\" ON \"employee\".\"department_id\" = \"t4\".\"department_id0\"";
    sql(query).ok(expected);
  }

  @Test void testSmallintOracle() {
    String query = "SELECT CAST(\"department_id\" AS SMALLINT) FROM \"employee\"";
    String expected = "SELECT CAST(\"department_id\" AS NUMBER(5))\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withOracle().ok(expected);
  }

  @Test void testBigintOracle() {
    String query = "SELECT CAST(\"department_id\" AS BIGINT) FROM \"employee\"";
    String expected = "SELECT CAST(\"department_id\" AS NUMBER(19))\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withOracle().ok(expected);
  }

  @Test void testDoubleOracle() {
    String query = "SELECT CAST(\"department_id\" AS DOUBLE) FROM \"employee\"";
    String expected = "SELECT CAST(\"department_id\" AS DOUBLE PRECISION)\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withOracle().ok(expected);
  }

  @Test void testRedshiftCastToTinyint() {
    String query = "SELECT CAST(\"department_id\" AS tinyint) FROM \"employee\"";
    String expected = "SELECT CAST(\"department_id\" AS \"int2\")\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withRedshift().ok(expected);
  }

  @Test void testRedshiftCastToDouble() {
    String query = "SELECT CAST(\"department_id\" AS double) FROM \"employee\"";
    String expected = "SELECT CAST(\"department_id\" AS \"float8\")\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withRedshift().ok(expected);
  }

  @Test void testDateLiteralOracle() {
    String query = "SELECT DATE '1978-05-02' FROM \"employee\"";
    String expected = "SELECT TO_DATE('1978-05-02', 'YYYY-MM-DD')\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withOracle().ok(expected);
  }

  @Test void testTimestampLiteralOracle() {
    String query = "SELECT TIMESTAMP '1978-05-02 12:34:56.78' FROM \"employee\"";
    String expected = "SELECT TO_TIMESTAMP('1978-05-02 12:34:56.78',"
        + " 'YYYY-MM-DD HH24:MI:SS.FF')\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withOracle().ok(expected);
  }

  @Test void testTimeLiteralOracle() {
    String query = "SELECT TIME '12:34:56.78' FROM \"employee\"";
    String expected = "SELECT TO_TIME('12:34:56.78', 'HH24:MI:SS.FF')\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withOracle().ok(expected);
  }

  @Test void testSupportsDataType() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataType booleanDataType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    final RelDataType integerDataType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    final SqlDialect oracleDialect = DatabaseProduct.ORACLE.getDialect();
    assertFalse(oracleDialect.supportsDataType(booleanDataType));
    assertTrue(oracleDialect.supportsDataType(integerDataType));
    final SqlDialect postgresqlDialect = DatabaseProduct.POSTGRESQL.getDialect();
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
        + "SELECT \"EXPR$0\" AS \"account_id\","
        + " \"EXPR$1\" AS \"account_parent\","
        + " CAST(NULL AS VARCHAR(30) CHARACTER SET \"ISO-8859-1\") "
        + "AS \"account_description\","
        + " \"EXPR$2\" AS \"account_type\","
        + " \"EXPR$3\" AS \"account_rollup\","
        + " CAST(NULL AS VARCHAR(255) CHARACTER SET \"ISO-8859-1\") "
        + "AS \"Custom_Members\"\n"
        + "FROM (VALUES (1, NULL, '123', '123')) "
        + "AS \"t\" (\"EXPR$0\", \"EXPR$1\", \"EXPR$2\", \"EXPR$3\")";
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
        + "SELECT \"product\".\"product_id\" AS \"account_id\", "
        + "CAST(NULL AS INTEGER) AS \"account_parent\", CAST(NULL AS VARCHAR"
        + "(30) CHARACTER SET \"ISO-8859-1\") AS \"account_description\", "
        + "CAST(\"product\".\"product_id\" AS VARCHAR CHARACTER SET "
        + "\"ISO-8859-1\") AS \"account_type\", "
        + "CAST(\"sales_fact_1997\".\"store_id\" AS VARCHAR CHARACTER SET \"ISO-8859-1\") AS "
        + "\"account_rollup\", "
        + "CAST(NULL AS VARCHAR(255) CHARACTER SET \"ISO-8859-1\") AS \"Custom_Members\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "INNER JOIN \"foodmart\".\"sales_fact_1997\" "
        + "ON \"product\".\"product_id\" = \"sales_fact_1997\".\"product_id\"";
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
    final String expected2 = "SELECT CAST(1111111 AS DECIMAL(5, 2)) AS \"num\"\n"
        + "FROM \"foodmart\".\"product\"";
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
        + "CAST('1996-01-01 ' || '00:00:00' AS TIMESTAMP)";
    sql(query)
        .ok(expected)
        .withBigQuery().ok(expectedBiqquery);
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

  @Test void testSelectCountStar() {
    final String query = "select count(*) from \"product\"";
    final String expected = "SELECT COUNT(*)\n"
            + "FROM \"foodmart\".\"product\"";
    Sql sql = sql(query);
    sql.ok(expected);
  }

  @Test void testSelectApproxCountDistinct() {
    final String query = "select approx_count_distinct(\"product_id\") from \"product\"";
    final String expectedExact = "SELECT COUNT(DISTINCT \"product_id\")\n"
        + "FROM \"foodmart\".\"product\"";
    final String expectedApprox = "SELECT APPROX_COUNT_DISTINCT(product_id)\n"
        + "FROM foodmart.product";
    final String expectedApproxQuota = "SELECT APPROX_COUNT_DISTINCT(\"product_id\")\n"
        + "FROM \"foodmart\".\"product\"";
    final String expectedPrestoSql = "SELECT APPROX_DISTINCT(\"product_id\")\n"
        + "FROM \"foodmart\".\"product\"";
    sql(query).ok(expectedExact)
        .withHive().ok(expectedApprox)
        .withSpark().ok(expectedApprox)
        .withBigQuery().ok(expectedApprox)
        .withOracle().ok(expectedApproxQuota)
        .withSnowflake().ok(expectedApproxQuota)
        .withPresto().ok(expectedPrestoSql);
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
        + " (`DEPTNO`, `DNAME`, `LOC`)\n"
        + "SELECT 1, 'Fred', 'San Francisco'\n"
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
        + "FROM (VALUES (0)) AS [t] ([ZERO])\n"
        + "UNION ALL\n"
        + "SELECT 2, 'Eric', 'Washington'\n"
        + "FROM (VALUES (0)) AS [t] ([ZERO])";
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5265">[CALCITE-5265]
   * JDBC adapter sometimes adds unnecessary parentheses around SELECT in INSERT</a>. */
  @Test void testInsertSelect() {
    final String sql = "insert into \"DEPT\" select * from \"DEPT\"";
    final String expected = ""
        + "INSERT INTO \"SCOTT\".\"DEPT\" (\"DEPTNO\", \"DNAME\", \"LOC\")\n"
        + "SELECT *\n"
        + "FROM \"SCOTT\".\"DEPT\"";
    sql(sql)
        .schema(CalciteAssert.SchemaSpec.JDBC_SCOTT)
        .ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5265">[CALCITE-5265]
   * JDBC adapter sometimes adds unnecessary parentheses around SELECT in INSERT</a>. */
  @Test void testInsertUnionThenIntersect() {
    final String sql = ""
        + "insert into \"DEPT\"\n"
        + "(select * from \"DEPT\" union select * from \"DEPT\")\n"
        + "intersect select * from \"DEPT\"";
    final String expected = ""
        + "INSERT INTO \"SCOTT\".\"DEPT\" (\"DEPTNO\", \"DNAME\", \"LOC\")\n"
        + "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM \"SCOTT\".\"DEPT\"\n"
        + "UNION\n"
        + "SELECT *\n"
        + "FROM \"SCOTT\".\"DEPT\")\n"
        + "INTERSECT\n"
        + "SELECT *\n"
        + "FROM \"SCOTT\".\"DEPT\"";
    sql(sql)
        .schema(CalciteAssert.SchemaSpec.JDBC_SCOTT)
        .ok(expected);
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
  @Test void testBigQueryHaving() {
    final String sql = ""
        + "SELECT \"DEPTNO\" - 10 \"DEPTNO\"\n"
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
        .withBigQuery().ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4740">[CALCITE-4740]
   * JDBC adapter generates incorrect HAVING clause in BigQuery dialect</a>. */
  @Test void testBigQueryHavingWithoutGeneratedAlias() {
    final String sql = ""
        + "SELECT \"DEPTNO\", COUNT(DISTINCT \"EMPNO\")\n"
        + "FROM \"EMP\"\n"
        + "GROUP BY \"DEPTNO\"\n"
        + "HAVING COUNT(DISTINCT \"EMPNO\") > 0\n"
        + "ORDER BY COUNT(DISTINCT \"EMPNO\") DESC";
    final String expected = ""
        + "SELECT DEPTNO, COUNT(DISTINCT EMPNO)\n"
        + "FROM SCOTT.EMP\n"
        + "GROUP BY DEPTNO\n"
        + "HAVING COUNT(DISTINCT EMPNO) > 0\n"
        + "ORDER BY COUNT(DISTINCT EMPNO) IS NULL DESC, COUNT(DISTINCT EMPNO) DESC";

    // Convert rel node to SQL with BigQuery dialect,
    // in which "isHavingAlias" is true.
    sql(sql)
        .schema(CalciteAssert.SchemaSpec.JDBC_SCOTT)
        .withBigQuery().ok(expected);
  }

  /** Fluid interface to run tests. */
  static class Sql {
    private final CalciteAssert.SchemaSpec schemaSpec;
    private final String sql;
    private final SqlDialect dialect;
    private final Set<SqlLibrary> librarySet;
    private final @Nullable Function<RelBuilder, RelNode> relFn;
    private final List<Function<RelNode, RelNode>> transforms;
    private final SqlParser.Config parserConfig;
    private final UnaryOperator<SqlToRelConverter.Config> config;
    private final RelDataTypeSystem typeSystem;

    Sql(CalciteAssert.SchemaSpec schemaSpec, String sql, SqlDialect dialect,
        SqlParser.Config parserConfig, Set<SqlLibrary> librarySet,
        UnaryOperator<SqlToRelConverter.Config> config,
        @Nullable Function<RelBuilder, RelNode> relFn,
        List<Function<RelNode, RelNode>> transforms,
        RelDataTypeSystem typeSystem) {
      this.schemaSpec = schemaSpec;
      this.sql = sql;
      this.dialect = dialect;
      this.librarySet = librarySet;
      this.relFn = relFn;
      this.transforms = ImmutableList.copyOf(transforms);
      this.parserConfig = parserConfig;
      this.config = config;
      this.typeSystem = requireNonNull(typeSystem, "typeSystem");
    }

    Sql withSql(String sql) {
      return new Sql(schemaSpec, sql, dialect, parserConfig, librarySet, config,
          relFn, transforms, typeSystem);
    }

    Sql dialect(SqlDialect dialect) {
      return new Sql(schemaSpec, sql, dialect, parserConfig, librarySet, config,
          relFn, transforms, typeSystem);
    }

    Sql relFn(Function<RelBuilder, RelNode> relFn) {
      return new Sql(schemaSpec, sql, dialect, parserConfig, librarySet, config,
          relFn, transforms, typeSystem);
    }

    Sql withCalcite() {
      return dialect(DatabaseProduct.CALCITE.getDialect());
    }

    Sql withClickHouse() {
      return dialect(DatabaseProduct.CLICKHOUSE.getDialect());
    }

    Sql withDb2() {
      return dialect(DatabaseProduct.DB2.getDialect());
    }

    Sql withExasol() {
      return dialect(DatabaseProduct.EXASOL.getDialect());
    }

    Sql withFirebolt() {
      return dialect(DatabaseProduct.FIREBOLT.getDialect());
    }

    Sql withHive() {
      return dialect(DatabaseProduct.HIVE.getDialect());
    }

    Sql withHsqldb() {
      return dialect(DatabaseProduct.HSQLDB.getDialect());
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
      return dialect(DatabaseProduct.MYSQL.getDialect());
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
      return dialect(DatabaseProduct.ORACLE.getDialect());
    }

    Sql withPostgresql() {
      return dialect(DatabaseProduct.POSTGRESQL.getDialect());
    }

    Sql withPresto() {
      return dialect(DatabaseProduct.PRESTO.getDialect());
    }

    Sql withRedshift() {
      return dialect(DatabaseProduct.REDSHIFT.getDialect());
    }

    Sql withInformix() {
      return dialect(DatabaseProduct.INFORMIX.getDialect());
    }

    Sql withSnowflake() {
      return dialect(DatabaseProduct.SNOWFLAKE.getDialect());
    }

    Sql withSybase() {
      return dialect(DatabaseProduct.SYBASE.getDialect());
    }

    Sql withVertica() {
      return dialect(DatabaseProduct.VERTICA.getDialect());
    }

    Sql withBigQuery() {
      return dialect(DatabaseProduct.BIG_QUERY.getDialect());
    }

    Sql withSpark() {
      return dialect(DatabaseProduct.SPARK.getDialect());
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
      return new Sql(schemaSpec, sql, dialect, parserConfig, librarySet, config,
          relFn, transforms, typeSystem);
    }

    Sql withConfig(UnaryOperator<SqlToRelConverter.Config> config) {
      return new Sql(schemaSpec, sql, dialect, parserConfig, librarySet, config,
          relFn, transforms, typeSystem);
    }

    Sql withTypeSystem(RelDataTypeSystem typeSystem) {
      return new Sql(schemaSpec, sql, dialect, parserConfig, librarySet, config,
          relFn, transforms, typeSystem);
    }

    final Sql withLibrary(SqlLibrary library) {
      return withLibrarySet(ImmutableSet.of(library));
    }

    Sql withLibrarySet(Iterable<? extends SqlLibrary> librarySet) {
      return new Sql(schemaSpec, sql, dialect, parserConfig,
          ImmutableSet.copyOf(librarySet), config, relFn, transforms, typeSystem);
    }

    Sql optimize(final RuleSet ruleSet,
        final @Nullable RelOptPlanner relOptPlanner) {
      final List<Function<RelNode, RelNode>> transforms =
          FlatLists.append(this.transforms, r -> {
            Program program = Programs.of(ruleSet);
            final RelOptPlanner p =
                Util.first(relOptPlanner,
                    new HepPlanner(
                        new HepProgramBuilder().addRuleClass(RelOptRule.class)
                            .build()));
            return program.run(p, r, r.getTraitSet(),
                ImmutableList.of(), ImmutableList.of());
          });
      return new Sql(schemaSpec, sql, dialect, parserConfig, librarySet, config,
          relFn, transforms, typeSystem);
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
        final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        final SchemaPlus defaultSchema =
            CalciteAssert.addSchema(rootSchema, schemaSpec);
        RelNode rel;
        if (relFn != null) {
          final FrameworkConfig frameworkConfig = RelBuilderTest.config()
              .defaultSchema(defaultSchema)
              .build();
          final RelBuilder relBuilder = RelBuilder.create(frameworkConfig);
          rel = relFn.apply(relBuilder);
        } else {
          final SqlToRelConverter.Config config = this.config.apply(SqlToRelConverter.config()
              .withTrimUnusedFields(false));
          final Planner planner =
              getPlanner(null, parserConfig, defaultSchema, config, librarySet, typeSystem);
          SqlNode parse = planner.parse(sql);
          SqlNode validate = planner.validate(parse);
          rel = planner.rel(validate).project();
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
      return new Sql(schemaSpec, sql, dialect, parserConfig, librarySet, config,
          relFn, transforms, typeSystem);
    }
  }
}
