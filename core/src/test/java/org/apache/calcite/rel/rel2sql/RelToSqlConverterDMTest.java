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

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.plan.CTEDefinationTrait;
import org.apache.calcite.plan.CTEScopeTrait;
import org.apache.calcite.plan.GroupByWithQualifyHavingRankRelTrait;
import org.apache.calcite.plan.QualifyRelTrait;
import org.apache.calcite.plan.QualifyRelTraitDef;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.ViewChildProjectRelTrait;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.logical.ToLogicalConverter;
import org.apache.calcite.rel.rules.AggregateJoinTransposeRule;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterExtractInnerJoinRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.JoinToCorrelateRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDateTimeFormat;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialect.Context;
import org.apache.calcite.sql.SqlDialect.DatabaseProduct;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.dialect.JethroDataSqlDialect;
import org.apache.calcite.sql.dialect.MssqlSqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.fun.SqlAddMonths;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.TeradataStrtokSplitToTableFunction;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.BasicSqlTypeWithFormat;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql2rel.RelDecorrelator;
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
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.TimestampString;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.DayOfWeek;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static org.apache.calcite.avatica.util.TimeUnit.DAY;
import static org.apache.calcite.avatica.util.TimeUnit.HOUR;
import static org.apache.calcite.avatica.util.TimeUnit.MICROSECOND;
import static org.apache.calcite.avatica.util.TimeUnit.MINUTE;
import static org.apache.calcite.avatica.util.TimeUnit.MONTH;
import static org.apache.calcite.avatica.util.TimeUnit.SECOND;
import static org.apache.calcite.avatica.util.TimeUnit.WEEK;
import static org.apache.calcite.avatica.util.TimeUnit.YEAR;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.BITNOT;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.CURRENT_TIMESTAMP;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.CURRENT_TIMESTAMP_WITH_TIME_ZONE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.DATEFROMPARTS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.DATE_ADD;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.DATE_MOD;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.DAYNUMBER_OF_CALENDAR;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.DAYOCCURRENCE_OF_MONTH;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.FALSE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.GENERATE_ARRAY;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.GETDATE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.MAKE_DATE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.MONTHNUMBER_OF_YEAR;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.PERIOD_CONSTRUCTOR;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.PERIOD_INTERSECT;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.QUARTERNUMBER_OF_YEAR;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_OFFSET;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TRUE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.USING;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.WEEKNUMBER_OF_CALENDAR;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.WEEKNUMBER_OF_YEAR;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.YEARNUMBER_OF_CALENDAR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.COLUMN_LIST;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_DATE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.REGEXP_SUBSTR;
import static org.apache.calcite.test.Matchers.isLinux;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link RelToSqlConverter}.
 */
class RelToSqlConverterDMTest {

  /** Initiates a test case with a given SQL query. */
  private RelToSqlConverterTest.Sql sql(String sql) {
    return new RelToSqlConverterTest.Sql(CalciteAssert.SchemaSpec.JDBC_FOODMART, sql,
        CalciteSqlDialect.DEFAULT, SqlParser.Config.DEFAULT, ImmutableSet.of(),
        UnaryOperator.identity(), null, ImmutableList.of());
  }

  private RelToSqlConverterTest.Sql sqlTest(String sql) {
    return new RelToSqlConverterTest.Sql(CalciteAssert.SchemaSpec.FOODMART_TEST, sql,
        CalciteSqlDialect.DEFAULT, SqlParser.Config.DEFAULT, ImmutableSet.of(),
        UnaryOperator.identity(), null, ImmutableList.of());
  }

  public static Frameworks.ConfigBuilder salesConfig() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(
            CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.SALESSCHEMA))
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.ofRules(Programs.RULE_SET));
  }

  /** Initiates a test case with a given {@link RelNode} supplier. */
  private RelToSqlConverterTest.Sql relFn(Function<RelBuilder, RelNode> relFn) {
    return sql("?").relFn(relFn);
  }

  private static Planner getPlanner(List<RelTraitDef> traitDefs,
      SqlParser.Config parserConfig, SchemaPlus schema,
      SqlToRelConverter.Config sqlToRelConf, Collection<SqlLibrary> librarySet,
      Program... programs) {
    final MockSqlOperatorTable operatorTable =
        new MockSqlOperatorTable(
            SqlOperatorTables.chain(SqlStdOperatorTable.instance(),
                SqlLibraryOperatorTableFactory.INSTANCE
                    .getOperatorTable(librarySet)));
    MockSqlOperatorTable.of(operatorTable);
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
        .put(DatabaseProduct.BIG_QUERY.getDialect(),
            DatabaseProduct.BIG_QUERY)
        .put(DatabaseProduct.CALCITE.getDialect(),
            DatabaseProduct.CALCITE)
        .put(DatabaseProduct.DB2.getDialect(),
            DatabaseProduct.DB2)
        .put(DatabaseProduct.HIVE.getDialect(),
            DatabaseProduct.HIVE)
        .put(jethroDataSqlDialect(),
            DatabaseProduct.JETHRO)
        .put(DatabaseProduct.MSSQL.getDialect(),
            DatabaseProduct.MSSQL)
        .put(DatabaseProduct.MYSQL.getDialect(),
            DatabaseProduct.MYSQL)
        .put(mySqlDialect(NullCollation.HIGH),
            DatabaseProduct.MYSQL)
        .put(DatabaseProduct.ORACLE.getDialect(),
            DatabaseProduct.ORACLE)
        .put(DatabaseProduct.POSTGRESQL.getDialect(),
            DatabaseProduct.POSTGRESQL)
        .put(DatabaseProduct.PRESTO.getDialect(),
            DatabaseProduct.PRESTO)
        .build();
  }

  /** Creates a RelBuilder. */
  private static RelBuilder relBuilder() {
    return RelBuilder.create(RelBuilderTest.config().build());
  }

  private static RelBuilder foodmartRelBuilder() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    FrameworkConfig foodmartConfig = RelBuilderTest.config()
        .defaultSchema(CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.JDBC_FOODMART))
        .build();
    return RelBuilder.create(foodmartConfig);
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
    final SqlNode sqlNode = toSqlNode(root, dialect);
    return sqlNode.toSqlString(c -> transform.apply(c.withDialect(dialect)))
        .getSql();
  }

  /** Converts a relational expression to SQL Node. */
  private static SqlNode toSqlNode(RelNode root, SqlDialect dialect) {
    final RelToSqlConverter converter = new RelToSqlConverter(dialect);
    return converter.visitRoot(root).asStatement();
  }

  @Test public void testSimpleSelectWithOrderByAliasAsc() {
    final String query = "select sku+1 as a from \"product\" order by a";
    final String bigQueryExpected = "SELECT SKU + 1 AS A\nFROM foodmart.product\n"
        + "ORDER BY A NULLS LAST";
    final String hiveExpected = "SELECT SKU + 1 A\nFROM foodmart.product\n"
        + "ORDER BY A IS NULL, A";
    final String sparkExpected = "SELECT SKU + 1 A\nFROM foodmart.product\n"
        + "ORDER BY A NULLS LAST";
    sql(query)
        .withBigQuery()
        .ok(bigQueryExpected)
        .withHive()
        .ok(hiveExpected)
        .withSpark()
        .ok(sparkExpected);
  }

  @Test public void testSimpleSelectWithOrderByAliasDesc() {
    final String query = "select sku+1 as a from \"product\" order by a desc";
    final String bigQueryExpected = "SELECT SKU + 1 AS A\nFROM foodmart.product\n"
        + "ORDER BY A DESC NULLS FIRST";
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

  @Test void testAggregateFilterWhereToSqlFromProductTable1() {
    String query = "select *\n"
        + "from \"foodmart\".\"product\"\n"
        + "group by \"product_class_id\", \"product_id\", \"brand_name\", \"product_name\", \"SKU\", \"SRP\", \"gross_weight\", \"net_weight\", \"recyclable_package\", \"low_fat\", \"units_per_case\", \"cases_per_pallet\", \"shelf_width\", \"shelf_height\", \"shelf_depth\"";
    final String expected = "SELECT *\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"product_class_id\", \"product_id\", \"brand_name\", \"product_name\", \"SKU\", \"SRP\", \"gross_weight\", \"net_weight\", \"recyclable_package\", \"low_fat\", \"units_per_case\", \"cases_per_pallet\", \"shelf_width\", \"shelf_height\", \"shelf_depth\"";
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
    final String expectedBigQuery = "SELECT COUNT(*)\n"
        + "FROM foodmart.product\n"
        + "GROUP BY product_class_id, product_id";
    sql(query)
        .ok(expected)
        .withBigQuery()
        .ok(expectedBigQuery);
  }

  @Test void testSelectQueryWithGroupByAndComplexGroupingItemInSelect() {
    String query = "select 'abc' || case when \"product_id\" = 1 then 'a' else 'b' end,"
        + "case when \"product_id\" = 1 then 'a' else 'b' end as grp "
        + "from \"product\" "
        + "group by case when \"product_id\" = 1 then 'a' else 'b' end";
    String expected = "SELECT 'abc' || GRP, GRP\n"
        + "FROM (SELECT CASE WHEN product_id = 1 THEN 'a' ELSE 'b' END AS GRP\n"
        + "FROM foodmart.product\n"
        + "GROUP BY GRP) AS t0";
    sql(query).withBigQuery().ok(expected);
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
    final String expectedMySql = "SELECT *\n"
        + "FROM (SELECT `product_class_id`, `brand_name`\n"
        + "FROM `foodmart`.`product`\n"
        + "GROUP BY `product_class_id`, `brand_name` WITH ROLLUP) AS `t0`\n"
        + "ORDER BY `brand_name`, `product_class_id`";
    final String expectedHive = "SELECT *\n"
        + "FROM (SELECT product_class_id, brand_name\n"
        + "FROM foodmart.product\n"
        + "GROUP BY product_class_id, brand_name WITH ROLLUP) t0\n"
        + "ORDER BY brand_name, product_class_id";
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
        + "GROUP BY a, B";
    sql(query)
        .withBigQuery()
        .ok(bigQueryExpected);
  }

  @Test public void testSimpleSelectWithGroupByAliasAndAggregate() {
    final String query = "select 'literal' as \"a\", sku + 1 as \"b\", sum(\"product_id\") from"
        + " \"product\" group by sku + 1, 'literal'";
    final String bigQueryExpected = "SELECT 'literal' AS a, SKU + 1 AS b, SUM(product_id)\n"
        + "FROM foodmart.product\n"
        + "GROUP BY b, a";
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
        + "GROUP BY d, B";
    final String expectedSpark = "SELECT '1' a, SKU + 1 B, '1' d\n"
        + "FROM foodmart.product\n"
        + "GROUP BY d, B";
    sql(query)
        .withHive()
        .ok(expectedSql)
        .withSpark()
        .ok(expectedSpark)
        .withBigQuery()
        .ok(bigQueryExpected);
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
        + "ORDER BY \"product_class_id\", 2";
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

  @Test public void testNestedCaseClauseInAggregateFunction() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RexNode innerWhenClauseRex =
        builder.call(
            EQUALS, builder.call(
            SqlStdOperatorTable.COALESCE, builder.field(
        "DEPTNO"), builder.literal(0)), builder.literal(4));
    final RexNode innerCaseRex =
        builder.call(SqlStdOperatorTable.CASE, innerWhenClauseRex, builder.call(TRUE),
        builder.call(FALSE));
    final RexNode outerCaseRex =
        builder.call(SqlStdOperatorTable.CASE, innerCaseRex, builder.field("DEPTNO"),
        builder.literal(100));
    final RelNode root = builder
        .scan("EMP")
        .aggregate(
            builder.groupKey(), builder.aggregateCall(SqlStdOperatorTable.MAX,
            outerCaseRex).as("val"))
        .build();

    final String expectedSql = "SELECT MAX(CASE WHEN CASE WHEN COALESCE(\"DEPTNO\", 0) = 4 "
        + "THEN TRUE() ELSE FALSE() END THEN \"DEPTNO\" ELSE 100 END) AS \"val\"\nFROM "
        + "\"scott\".\"EMP\"";
    final String expectedBigQuery = "SELECT MAX(CASE WHEN CASE WHEN COALESCE(DEPTNO, 0) = 4 THEN "
        + "TRUE ELSE FALSE END THEN DEPTNO ELSE 100 END) AS val\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBigQuery));
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
    final String expected = "SELECT -0.0000000123\n"
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
        + "FROM (SELECT RANK() OVER (ORDER BY hire_date NULLS LAST) rnk\n"
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
        + "FROM (SELECT CASE WHEN (RANK() OVER (ORDER BY hire_date NULLS LAST)) = 1 "
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
    final String expectedSpark = "SELECT *\n"
        + "FROM (SELECT CASE WHEN CAST(salary AS DECIMAL(14, 4)) = 20 THEN MAX(salary) OVER "
        + "(PARTITION BY position_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) "
        + "ELSE NULL END rnk\n"
        + "FROM foodmart.employee) t\n"
        + "GROUP BY rnk";
    final String expectedBigQuery = "SELECT *\n"
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
    final String expectedSpark = "SELECT *\n"
        + "FROM (SELECT CASE WHEN (ROW_NUMBER() OVER (PARTITION BY hire_date)) = 1 THEN 100 ELSE "
        + "200 END rnk\n"
        + "FROM foodmart.employee) t\n"
        + "GROUP BY rnk";
    final String expectedBigQuery = "SELECT *\n"
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

  @Test public void testTableFunctionScanWithUnnest() {
    final RelBuilder builder = relBuilder();
    String[] array = {"abc", "bcd", "fdc"};
    RelNode root =
            builder.functionScan(
                    SqlStdOperatorTable.UNNEST, 0, builder.makeArrayLiteral(
                            Arrays.asList(array))).project(builder.field(0)).build();
    final SqlDialect dialect = DatabaseProduct.BIG_QUERY.getDialect();
    final String expectedSql = "SELECT *\nFROM UNNEST(ARRAY['abc', 'bcd', 'fdc'])\nAS EXPR$0";
    assertThat(toSql(root, dialect), isLinux(expectedSql));
  }

  @Test public void testUnpivotWithIncludeNullsAsTrueOnSalesTable() {
    final RelBuilder builder =  RelBuilder.create(salesConfig().build());
    RelNode root = builder
        .scan("sales")
        .unpivot(true, ImmutableList.of("monthly_sales"), //value_column(measureList)
            ImmutableList.of("month"), //unpivot_column(axisList)
            Pair.zip(
                Arrays.asList(ImmutableList.of(builder.literal("jan")), //column_alias
                    ImmutableList.of(builder.literal("feb")),
                    ImmutableList.of(builder.literal("march"))),
                Arrays.asList(ImmutableList.of(builder.field("jansales")), //column_list
                    ImmutableList.of(builder.field("febsales")),
                    ImmutableList.of(builder.field("marsales")))))
        .build();
    final SqlDialect dialect = DatabaseProduct.BIG_QUERY.getDialect();
    final String expectedSql = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM SALESSCHEMA.sales) UNPIVOT INCLUDE NULLS (monthly_sales FOR month IN (jansales "
        + "AS 'jan', febsales AS 'feb', marsales AS 'march'))";
    assertThat(toSql(root, dialect), isLinux(expectedSql));
  }

  @Test public void testUnpivotWithIncludeNullsAsFalseOnSalesTable() {
    final RelBuilder builder =  RelBuilder.create(salesConfig().build());
    RelNode root = builder
        .scan("sales")
        .unpivot(false, ImmutableList.of("monthly_sales"), //value_column(measureList)
            ImmutableList.of("month"), //unpivot_column(axisList)
            Pair.zip(
                Arrays.asList(ImmutableList.of(builder.literal("jan")), //column_alias
                    ImmutableList.of(builder.literal("feb")),
                    ImmutableList.of(builder.literal("march"))),
                Arrays.asList(ImmutableList.of(builder.field("jansales")), //column_list
                    ImmutableList.of(builder.field("febsales")),
                    ImmutableList.of(builder.field("marsales")))))
        .build();
    final SqlDialect dialect = DatabaseProduct.BIG_QUERY.getDialect();
    final String expectedSql = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM SALESSCHEMA.sales) UNPIVOT EXCLUDE NULLS (monthly_sales FOR month IN (jansales "
        + "AS 'jan', febsales AS 'feb', marsales AS 'march'))";
    assertThat(toSql(root, dialect), isLinux(expectedSql));
  }

  @Test public void testUnpivotWithIncludeNullsAsTrueWithMeasureColumnList() {
    final RelBuilder builder =  RelBuilder.create(salesConfig().build());
    RelNode root = builder
        .scan("sales")
        .unpivot(
            true, ImmutableList.of("monthly_sales",
                "monthly_expense"), //value_column(measureList)
            ImmutableList.of("month"), //unpivot_column(axisList)
            Pair.zip(
                Arrays.asList(ImmutableList.of(builder.literal("jan")), //column_alias
                    ImmutableList.of(builder.literal("feb")),
                    ImmutableList.of(builder.literal("march"))),
                Arrays.asList(
                    ImmutableList.of(builder.field("jansales"),
                        builder.field("janexpense")), //column_list
                    ImmutableList.of(builder.field("febsales"), builder.field("febexpense")),
                    ImmutableList.of(builder.field("marsales"), builder.field("marexpense")))))
        .build();
    final SqlDialect dialect = DatabaseProduct.BIG_QUERY.getDialect();
    final String expectedSql = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM SALESSCHEMA.sales) UNPIVOT INCLUDE NULLS ((monthly_sales, monthly_expense) FOR "
        + "month IN ((jansales, janexpense) AS 'jan', (febsales, febexpense) AS 'feb', "
        + "(marsales, marexpense) AS 'march'))";
    assertThat(toSql(root, dialect), isLinux(expectedSql));
  }

  @Test public void testUnpivotWithIncludeNullsAsFalseWithMeasureColumnList() {
    final RelBuilder builder =  RelBuilder.create(salesConfig().build());
    RelNode root = builder
        .scan("sales")
        .unpivot(
            false, ImmutableList.of("monthly_sales",
                "monthly_expense"), //value_column(measureList)
            ImmutableList.of("month"), //unpivot_column(axisList)
            Pair.zip(
                Arrays.asList(ImmutableList.of(builder.literal("jan")), //column_alias
                    ImmutableList.of(builder.literal("feb")),
                    ImmutableList.of(builder.literal("march"))),
                Arrays.asList(
                    ImmutableList.of(builder.field("jansales"),
                        builder.field("janexpense")), //column_list
                    ImmutableList.of(builder.field("febsales"), builder.field("febexpense")),
                    ImmutableList.of(builder.field("marsales"), builder.field("marexpense")))))
        .build();
    final SqlDialect dialect = DatabaseProduct.BIG_QUERY.getDialect();
    final String expectedSql = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM SALESSCHEMA.sales) UNPIVOT EXCLUDE NULLS ((monthly_sales, monthly_expense) FOR "
        + "month IN ((jansales, janexpense) AS 'jan', (febsales, febexpense) AS 'feb', "
        + "(marsales, marexpense) AS 'march'))";
    assertThat(toSql(root, dialect), isLinux(expectedSql));
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
    final String expectedSpark = "SELECT product_id + 1, " + alias + "\n"
        + "FROM (SELECT product_id, SUM(gross_weight) " + alias + "\n"
        + "FROM foodmart.product\n"
        + "GROUP BY product_id\n"
        + "HAVING " + alias + " < 200) t1";
    sql(query)
        .withPostgresql().ok(expectedPostgresql)
        .withMysql().ok(expectedMysql)
        .withSpark().ok(expectedSpark)
        .withBigQuery().ok(expectedBigQuery);
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
    final String expectedSpark = "SELECT TRIM(' ' FROM ' str ')\nFROM foodmart"
        + ".reserve_employee";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
      .ok(expectedSpark)
      .withBigQuery()
        .ok(expected);
  }

  @Test void testHiveSparkAndBqTrimWithBoth() {
    final String query = "SELECT TRIM(both ' ' from ' str ')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT TRIM(' str ')\n"
        + "FROM foodmart.reserve_employee";
    final String expectedSpark = "SELECT TRIM(' ' FROM ' str ')\nFROM foodmart.reserve_employee";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
      .ok(expectedSpark)
      .withBigQuery()
        .ok(expected);
  }

  @Test void testHiveSparkAndBqTrimWithLeading() {
    final String query = "SELECT TRIM(LEADING ' ' from ' str ')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT LTRIM(' str ')\n"
        + "FROM foodmart.reserve_employee";
    final String expectedSpark = "SELECT TRIM(LEADING ' ' FROM ' str ')\nFROM foodmart"
        + ".reserve_employee";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
      .ok(expectedSpark)
      .withBigQuery()
        .ok(expected);
  }


  @Test void testHiveSparkAndBqTrimWithTailing() {
    final String query = "SELECT TRIM(TRAILING ' ' from ' str ')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT RTRIM(' str ')\n"
        + "FROM foodmart.reserve_employee";
    final String expectedSpark = "SELECT TRIM(TRAILING ' ' FROM ' str ')\nFROM foodmart"
        + ".reserve_employee";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
      .ok(expectedSpark)
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

  @Test void jsonValueWithAccessFields() {
    final String query = "SELECT JSON_VALUE('{\"fruits\": [\"apple\","
        + " \"banana\"]}', '$.fruits[0]')";

    final String expected = "SELECT JSON_VALUE('{\"fruits\": [\"apple\","
        + " \"banana\"]}', '$.fruits[0]')";
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
    final String expectedSpark = "SELECT TRIM(LEADING 'a' FROM 'abcd')\nFROM foodmart"
        + ".reserve_employee";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expectedSpark);
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
    final String expectedSpark = "SELECT TRIM('a' FROM 'abcda')\n"
        + "FROM foodmart.reserve_employee";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expectedSpark);
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
    final String expectedSpark = "SELECT TRIM(' ' FROM full_name)\nFROM foodmart"
        + ".reserve_employee";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expectedSpark)
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
    final String expectedSpark = "SELECT TRIM(' ' FROM full_name)\n"
        + "FROM foodmart.reserve_employee";
    final String expectedSnowFlake = "SELECT TRIM(\"full_name\")\n"
        + "FROM \"foodmart\".\"reserve_employee\"";
    final String expectedMsSql = "SELECT TRIM(' ' FROM [full_name])\n"
        + "FROM [foodmart].[reserve_employee]";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expectedSpark)
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
    final String expectedSpark = "SELECT TRIM(LEADING ' ' FROM ' str ')\nFROM foodmart"
        + ".reserve_employee";
    final String expectedSnowFlake = "SELECT LTRIM(' str ')\n"
              + "FROM \"foodmart\".\"reserve_employee\"";
    final String expectedMsSql = "SELECT LTRIM(' str ')\n"
        + "FROM [foodmart].[reserve_employee]";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expectedSpark)
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
    final String expectedSpark = "SELECT TRIM(TRAILING ' ' FROM ' str ')"
        + "\nFROM foodmart.reserve_employee";
    final String expectedSnowFlake = "SELECT RTRIM(' str ')\n"
        + "FROM \"foodmart\".\"reserve_employee\"";
    final String expectedMsSql = "SELECT RTRIM(' str ')\n"
        + "FROM [foodmart].[reserve_employee]";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expectedSpark)
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
    final String expectedSpark = "SELECT TRIM(LEADING 'A' FROM first_name)\nFROM foodmart"
        + ".reserve_employee";
    final String expectedHS = "SELECT REGEXP_REPLACE(first_name, '^(A)*', '')\n"
        + "FROM foodmart.reserve_employee";
    final String expectedSnowFlake = "SELECT LTRIM(\"first_name\", 'A')\n"
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

  @Test public void testTrimWithColumnsAsOperands() {
    final String query = "SELECT TRIM(LEADING \"first_name\" from \"full_name\")\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT LTRIM(full_name, first_name)\n"
        + "FROM foodmart.reserve_employee";

    sql(query)
        .withBigQuery()
        .ok(expected);
  }

  @Test public void testTrimWithTrailingCharacter() {
    final String query = "SELECT TRIM(TRAILING 'A' from 'AABCAADCAA')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT RTRIM('AABCAADCAA', 'A')\n"
        + "FROM foodmart.reserve_employee";
    final String expectedSpark = "SELECT TRIM(TRAILING 'A' FROM 'AABCAADCAA')\nFROM foodmart"
        + ".reserve_employee";
    final String expectedHS = "SELECT REGEXP_REPLACE('AABCAADCAA', '(A)*$', '')\n"
        + "FROM foodmart.reserve_employee";
    final String expectedSnowFlake = "SELECT RTRIM('AABCAADCAA', 'A')\n"
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

  @Test public void testTrimWithBothCharacter() {
    final String query = "SELECT TRIM(BOTH 'A' from 'AABCAADCAA')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT TRIM('AABCAADCAA', 'A')\n"
        + "FROM foodmart.reserve_employee";
    final String expectedSpark = "SELECT TRIM('A' FROM 'AABCAADCAA')\nFROM foodmart"
        + ".reserve_employee";
    final String expectedHS = "SELECT REGEXP_REPLACE('AABCAADCAA', '^(A)*|(A)*$', '')\n"
        + "FROM foodmart.reserve_employee";
    final String expectedSnowFlake = "SELECT TRIM('AABCAADCAA', 'A')\n"
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

  @Test public void testTrimWithLeadingSpecialCharacter() {
    final String query = "SELECT TRIM(LEADING 'A$@*' from 'A$@*AABCA$@*AADCAA$@*')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT LTRIM('A$@*AABCA$@*AADCAA$@*', 'A$@*')\n"
        + "FROM foodmart.reserve_employee";
    final String expectedHS =
        "SELECT REGEXP_REPLACE('A$@*AABCA$@*AADCAA$@*', '^(A\\$\\@\\*)*', '')\n"
            + "FROM foodmart.reserve_employee";
    final String expectedSpark = "SELECT TRIM(LEADING 'A$@*' FROM 'A$@*AABCA$@*AADCAA$@*')\nFROM"
        + " foodmart.reserve_employee";
    final String expectedSnowFlake = "SELECT LTRIM('A$@*AABCA$@*AADCAA$@*', 'A$@*')\n"
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

  @Test public void testTrimWithTrailingSpecialCharacter() {
    final String query = "SELECT TRIM(TRAILING '$A@*' from '$A@*AABC$@*AADCAA$A@*')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT RTRIM('$A@*AABC$@*AADCAA$A@*', '$A@*')\n"
        + "FROM foodmart.reserve_employee";
    final String expectedHS =
        "SELECT REGEXP_REPLACE('$A@*AABC$@*AADCAA$A@*', '(\\$A\\@\\*)*$', '')\n"
            + "FROM foodmart.reserve_employee";
    final String expectedSpark = "SELECT TRIM(TRAILING '$A@*' FROM '$A@*AABC$@*AADCAA$A@*')\n"
            + "FROM foodmart.reserve_employee";
    final String expectedSnowFlake = "SELECT RTRIM('$A@*AABC$@*AADCAA$A@*', '$A@*')\n"
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


  @Test public void testTrimWithBothSpecialCharacter() {
    final String query = "SELECT TRIM(BOTH '$@*A' from '$@*AABC$@*AADCAA$@*A')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT TRIM('$@*AABC$@*AADCAA$@*A', '$@*A')\n"
        + "FROM foodmart.reserve_employee";
    final String expectedHS =
        "SELECT REGEXP_REPLACE('$@*AABC$@*AADCAA$@*A',"
            + " '^(\\$\\@\\*A)*|(\\$\\@\\*A)*$', '')\n"
            + "FROM foodmart.reserve_employee";
    final String expectedSpark = "SELECT TRIM('$@*A' FROM '$@*AABC$@*AADCAA$@*A')\nFROM "
        + "foodmart.reserve_employee";
    final String expectedSnowFlake = "SELECT TRIM('$@*AABC$@*AADCAA$@*A', '$@*A')\n"
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

  @Test public void testTrimWithFunction() {
    final String query = "SELECT TRIM(substring(\"full_name\" from 2 for 3))\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT TRIM(SUBSTR(full_name, 2, 3))\n"
        + "FROM foodmart.reserve_employee";
    final String expectedHS =
        "SELECT TRIM(SUBSTRING(full_name, 2, 3))\n"
            + "FROM foodmart.reserve_employee";
    final String expectedSpark = "SELECT TRIM(' ' FROM SUBSTRING(full_name, 2, 3))\n"
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
    final String expectedSpark = "SELECT TRIM(TRAILING 'a' FROM 'abcd')\n"
        + "FROM foodmart.reserve_employee";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expectedSpark);
  }

  @Test void testHiveAndSparkTrimWithBothSpecialCharacter() {
    final String query = "SELECT TRIM(BOTH '$@*A' from '$@*AABC$@*AADCAA$@*A')\n"
        + "from \"foodmart\".\"reserve_employee\"";
    final String expected = "SELECT REGEXP_REPLACE('$@*AABC$@*AADCAA$@*A',"
        + " '^(\\$\\@\\*A)*|(\\$\\@\\*A)*$', '')\n"
        + "FROM foodmart.reserve_employee";
    final String expectedSpark = "SELECT TRIM('$@*A' FROM '$@*AABC$@*AADCAA$@*A')\n"
        + "FROM foodmart.reserve_employee";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expectedSpark);
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
    final String expectedBigQuery = "SELECT EMPNO, (SELECT DEPTNO\n"
        + "FROM scott.DEPT\n"
        + "WHERE DEPTNO = 40) AS SC_DEPTNO, COUNT(1) AS pid\n"
        + "FROM scott.EMP\n"
        + "GROUP BY EMPNO";
    final String expectedSnowflake = "SELECT \"EMPNO\", (SELECT \"DEPTNO\"\n"
        + "FROM \"scott\".\"DEPT\"\n"
        + "WHERE \"DEPTNO\" = 40) AS \"SC_DEPTNO\", COUNT(1) AS \"pid\"\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "GROUP BY \"EMPNO\"";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()),
        isLinux(expectedBigQuery));
    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()),
        isLinux(expectedSnowflake));
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

  @Test void testModFunction() {
    final String query = "select mod(11,3) from \"product\"";
    final String expected = "SELECT 11 % 3\n"
        + "FROM foodmart.product";
    final String expectedSpark = "SELECT MOD(11, 3)\n"
        + "FROM foodmart.product";
    sql(query).withSpark().ok(expectedSpark);
    sql(query).withHive().ok(expected);
  }

  @Test void testModFunctionWithNumericLiterals() {
    final String query = "select mod(11.9, 3), MOD(2, 4),"
        + "MOD(3, 4.5), MOD(\"product_id\", 4.5)"
        + " from \"product\"";
    final String expected = "SELECT MOD(CAST(11.9 AS NUMERIC), 3), "
        + "MOD(2, 4), MOD(3, CAST(4.5 AS NUMERIC)), "
        + "MOD(product_id, CAST(4.5 AS NUMERIC))\n"
        + "FROM foodmart.product";
    sql(query).withBigQuery().ok(expected);
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
            + "ORDER BY product_id NULLS LAST";
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
    final String expectedBQ = "SELECT product_id\n"
        + "FROM foodmart.product\n"
        + "ORDER BY product_id DESC NULLS FIRST";
    final String expectedSpark = "SELECT product_id\n"
        + "FROM foodmart.product\n"
        + "ORDER BY product_id DESC NULLS FIRST";
    final String expectedMssql = "SELECT [product_id]\n"
        + "FROM [foodmart].[product]\n"
        + "ORDER BY CASE WHEN [product_id] IS NULL THEN 0 ELSE 1 END, [product_id] DESC";
    sql(query)
        .withSpark()
        .ok(expectedSpark)
        .withHive()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBQ)
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
    final String expectedBQ = "SELECT product_id\n"
        + "FROM foodmart.product\n"
        + "ORDER BY product_id NULLS LAST";
    final String expectedSpark = "SELECT product_id\nFROM foodmart.product\n"
        + "ORDER BY product_id NULLS LAST";
    final String expectedMssql = "SELECT [product_id]\n"
        + "FROM [foodmart].[product]\n"
        + "ORDER BY CASE WHEN [product_id] IS NULL THEN 1 ELSE 0 END, [product_id]";
    sql(query)
        .withSpark()
        .ok(expectedSpark)
        .withHive()
        .ok(expected)
        .withBigQuery()
        .ok(expectedBQ)
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

  @Test void testMySqlSelectQueryWithOrderByDescAndNullsFirstShouldBeEmulated() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" desc nulls first";
    final String expected = "SELECT `product_id`\n"
        + "FROM `foodmart`.`product`\n"
        + "ORDER BY `product_id` IS NULL DESC, `product_id` DESC";
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

  @Test void testMySqlSelectQueryWithOrderByAscNullsFirstShouldNotAddNullEmulation() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" nulls first";
    final String expected = "SELECT `product_id`\n"
        + "FROM `foodmart`.`product`\n"
        + "ORDER BY `product_id`";
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

  @Test void testMySqlWithHighNullsSelectWithOrderByAscNullsLastAndNoEmulation() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" nulls last";
    final String expected = "SELECT `product_id`\n"
        + "FROM `foodmart`.`product`\n"
        + "ORDER BY `product_id`";
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

  @Test void testMySqlWithHighNullsSelectWithOrderByDescNullsFirstAndNoEmulation() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" desc nulls first";
    final String expected = "SELECT `product_id`\n"
        + "FROM `foodmart`.`product`\n"
        + "ORDER BY `product_id` DESC";
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

  @Test void testMySqlWithFirstNullsSelectWithOrderByDescAndNullsFirstShouldNotBeEmulated() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" desc nulls first";
    final String expected = "SELECT `product_id`\n"
        + "FROM `foodmart`.`product`\n"
        + "ORDER BY `product_id` DESC";
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

  @Test void testMySqlWithFirstNullsSelectWithOrderByDescAndNullsLastShouldBeEmulated() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" desc nulls last";
    final String expected = "SELECT `product_id`\n"
        + "FROM `foodmart`.`product`\n"
        + "ORDER BY `product_id` IS NULL, `product_id` DESC";
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

  @Test void testMySqlWithLastNullsSelectWithOrderByDescAndNullsFirstShouldBeEmulated() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" desc nulls first";
    final String expected = "SELECT `product_id`\n"
        + "FROM `foodmart`.`product`\n"
        + "ORDER BY `product_id` IS NULL DESC, `product_id` DESC";
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

  @Test void testMySqlWithLastNullsSelectWithOrderByDescAndNullsLastShouldNotBeEmulated() {
    final String query = "select \"product_id\" from \"product\"\n"
        + "order by \"product_id\" desc nulls last";
    final String expected = "SELECT `product_id`\n"
        + "FROM `foodmart`.`product`\n"
        + "ORDER BY `product_id` DESC";
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

  @Test void testCastToVarcharForSpark() {
    String query = "select cast(\"product_id\" as varchar), "
        + "cast(\"product_id\" as varchar(10)) from \"product\"";
    final String expectedSparkSql = "SELECT CAST(product_id AS STRING), "
        + "CAST(product_id AS VARCHAR(10))\n"
        + "FROM foodmart.product";
    sql(query)
        .withSpark()
        .ok(expectedSparkSql);
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
    final String expected = "SELECT \"product_id\"\n"
            + "FROM (SELECT \"product_id\", \"net_weight\"\n"
            + "FROM \"foodmart\".\"product\"\n"
            + "ORDER BY \"net_weight\"\n"
            + "OFFSET 10 ROWS\n"
            + "FETCH NEXT 100 ROWS ONLY) AS \"t0\"";
    // BigQuery uses LIMIT/OFFSET, and nulls sort low by default
    final String expectedBigQuery = "SELECT product_id\n"
            + "FROM (SELECT product_id, net_weight\n"
            + "FROM foodmart.product\n"
            + "ORDER BY net_weight NULLS LAST\n"
            + "LIMIT 100\n"
            + "OFFSET 10) AS t0";
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
    RuleSet rules =
        RuleSets.ofList(CoreRules.FILTER_INTO_JOIN,
        CoreRules.JOIN_CONDITION_PUSH,
        CoreRules.AGGREGATE_PROJECT_MERGE, CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED);
    sql(query).withPostgresql().optimize(rules, hepPlanner).ok(expect);
  }

  @Test void testDistinctWithGroupByAndAlias() {
    String query =
        "SELECT distinct \"product_id\", SUM(\"store_sales\"), COUNT(*) AS \"$f2\" "
            + "FROM \"foodmart\".\"sales_fact_dec_1998\" "
            + "GROUP BY \"product_id\"";

    String expect =
        "SELECT \"product_id\", SUM(\"store_sales\"), COUNT(*) AS \"$f2\""
            + "\nFROM \"foodmart\".\"sales_fact_dec_1998\""
            + "\nGROUP BY \"product_id\"";

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(FilterJoinRule.class);
    builder.addRuleClass(AggregateProjectMergeRule.class);
    builder.addRuleClass(AggregateJoinTransposeRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    RuleSet rules =
        RuleSets.ofList(CoreRules.FILTER_INTO_JOIN,
        CoreRules.JOIN_CONDITION_PUSH,
        CoreRules.AGGREGATE_PROJECT_MERGE, CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED);
    sql(query).withPostgresql().optimize(rules, hepPlanner).ok(expect);
  }

  @Test void testselectAllFieldsWithGroupByAllFieldsInSameSequence() {
    String query =
        "SELECT \"product_id\", \"time_id\", \"customer_id\", \"promotion_id\", \"store_id\", \"store_sales\", \"store_cost\", \"unit_sales\""
            + "FROM \"foodmart\".\"sales_fact_dec_1998\" "
            + "GROUP BY \"product_id\", \"time_id\", \"customer_id\", \"promotion_id\", \"store_id\", \"store_sales\", \"store_cost\", \"unit_sales\"";

    String expect =
        "SELECT *"
            + "\nFROM \"foodmart\".\"sales_fact_dec_1998\""
            + "\nGROUP BY \"product_id\", \"time_id\", \"customer_id\", \"promotion_id\", \"store_id\", \"store_sales\", \"store_cost\", \"unit_sales\"";

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(FilterJoinRule.class);
    builder.addRuleClass(AggregateProjectMergeRule.class);
    builder.addRuleClass(AggregateJoinTransposeRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    RuleSet rules =
        RuleSets.ofList(CoreRules.FILTER_INTO_JOIN,
        CoreRules.JOIN_CONDITION_PUSH,
        CoreRules.AGGREGATE_PROJECT_MERGE,
        CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED);
    sql(query).withPostgresql().optimize(rules, hepPlanner).ok(expect);
  }

  @Test void testselectAllFieldsWithGroupByAllFieldsInDifferentSequence() {
    String query =
        "SELECT \"promotion_id\", \"store_id\", \"store_sales\", \"store_cost\", \"unit_sales\", \"product_id\", \"time_id\", \"customer_id\""
            + "FROM \"foodmart\".\"sales_fact_dec_1998\" "
            + "GROUP BY \"product_id\", \"time_id\", \"customer_id\", \"promotion_id\", \"store_id\", \"store_sales\", \"store_cost\", \"unit_sales\"";

    String expect =
        "SELECT \"promotion_id\", \"store_id\", \"store_sales\", \"store_cost\", \"unit_sales\", \"product_id\", \"time_id\", \"customer_id\""
            + "\nFROM \"foodmart\".\"sales_fact_dec_1998\""
            + "\nGROUP BY \"product_id\", \"time_id\", \"customer_id\", \"promotion_id\", \"store_id\", \"store_sales\", \"store_cost\", \"unit_sales\"";

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(FilterJoinRule.class);
    builder.addRuleClass(AggregateProjectMergeRule.class);
    builder.addRuleClass(AggregateJoinTransposeRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    RuleSet rules =
        RuleSets.ofList(CoreRules.FILTER_INTO_JOIN,
        CoreRules.JOIN_CONDITION_PUSH,
        CoreRules.AGGREGATE_PROJECT_MERGE,
        CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED);
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

  @Test public void testArrayFunction() {
    final RelBuilder builder = relBuilder();

    RexNode arrayNode =
        builder.call(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
            builder.literal(0), builder.literal(1), builder.literal(2));

    RelNode unCollectNode = builder
        .push(LogicalValues.createOneRow(builder.getCluster()))
        .project(builder.alias(arrayNode, "EXPR$"))
        .uncollect(new ArrayList<String>(Collections.singleton("element")), true)
        .build();
    RelNode projectNode = builder
        .push(unCollectNode)
        .filter(
            builder.call(SqlLibraryOperators.BETWEEN, builder.field(1),
                builder.literal(0), builder.literal(1)))
        .project(builder.field(0))
        .build();
    final RelNode root = builder
        .scan("EMP")
        .project(RexSubQuery.array(projectNode), builder.field(0))
        .build();
    final String expectedSnowflake = "SELECT ARRAY (SELECT element\n"
        + "FROM UNNEST(ARRAY[0, 1, 2]) AS element WITH OFFSET AS ORDINALITY\n"
        + "WHERE ORDINALITY BETWEEN 0 AND 1) AS `$f0`, EMPNO\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedSnowflake));
  }

  @Test public void testArrayLengthFunction() {
    final RelBuilder builder = relBuilder();
    RexNode array =
        builder.call(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
            builder.literal(0), builder.literal(1), builder.literal(2));
    RexNode arrayLengthCall =
        builder.call(SqlLibraryOperators.POSTGRES_ARRAY_LENGTH, array, builder.literal(1));
    RelNode root = builder
        .push(LogicalValues.createOneRow(builder.getCluster()))
        .project(arrayLengthCall)
        .build();
    final String expectedPostgres = "SELECT ARRAY_LENGTH(ARRAY[0, 1, 2], 1) AS \"$f0\"";
    assertThat(toSql(root, DatabaseProduct.POSTGRESQL.getDialect()), isLinux(expectedPostgres));
  }

  @Test public void testArrayStartIndexFunction() {
    final RelBuilder builder = relBuilder();
    RexNode array =
        builder.call(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
            builder.literal(0), builder.literal(1), builder.literal(2));
    RexNode arrayStartIndexCall =
        builder.call(SqlLibraryOperators.ARRAY_START_INDEX, array);
    RelNode root = builder
        .push(LogicalValues.createOneRow(builder.getCluster()))
        .project(arrayStartIndexCall)
        .build();
    final String expectedPostgres = "SELECT ARRAY_START_INDEX(ARRAY[0, 1, 2]) AS \"$f0\"";
    assertThat(toSql(root, DatabaseProduct.POSTGRESQL.getDialect()), isLinux(expectedPostgres));
  }

  @Test public void testHostFunction() {
    final RelBuilder builder = relBuilder();
    RexNode hostCall = builder.call(SqlLibraryOperators.HOST, builder.literal("127.0.0.1"));
    RelNode root = builder
        .push(LogicalValues.createOneRow(builder.getCluster()))
        .project(builder.alias(hostCall, "EXPR$0"))
        .build();
    final String expectedPostgres = "SELECT HOST('127.0.0.1')";
    final String expectedBq = "SELECT NET.HOST('127.0.0.1')";
    assertThat(toSql(root, DatabaseProduct.POSTGRESQL.getDialect()), isLinux(expectedPostgres));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBq));
  }

  @Test public void testPostgresQuoteFunctions() {
    final RelBuilder builder = relBuilder();
    RexNode quoteIdentCall = builder.call(SqlLibraryOperators.QUOTE_IDENT, builder.literal("Foo bar"));
    RexNode quoteLiteralCall = builder.call(SqlLibraryOperators.QUOTE_LITERAL, builder.literal(42.5));
    RelNode root = builder
        .push(LogicalValues.createOneRow(builder.getCluster()))
        .project(quoteIdentCall, quoteLiteralCall)
        .build();
    final String expectedPostgres = "SELECT QUOTE_IDENT('Foo bar') AS \"$f0\", "
        + "QUOTE_LITERAL(42.5) AS \"$f1\"";
    assertThat(toSql(root, DatabaseProduct.POSTGRESQL.getDialect()), isLinux(expectedPostgres));
  }

  @Test public void testArraySlice() {
    final RelBuilder builder = relBuilder();

    RexNode arraySliceNode =
        builder.call(SqlLibraryOperators.ARRAY_SLICE, builder.literal(null), builder.literal(1), builder.literal(2));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(arraySliceNode, "arraySlice"))
        .build();
    final String expectedSnowflake = "SELECT ARRAY_SLICE(NULL, 1, 2) AS \"arraySlice\"\n"
        + "FROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedSnowflake));
  }

  @Test void testLagFunctionForPrintingitgOfFrameBoundary() {
    String query = "SELECT lag(\"employee_id\",1,'NA') over "
        + "(partition by \"hire_date\" order by \"employee_id\") FROM \"employee\"";
    String expected = "SELECT LAG(\"employee_id\", 1, 'NA') OVER "
        + "(PARTITION BY \"hire_date\" ORDER BY \"employee_id\")\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query).ok(expected);
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
        + "WHERE DATETIME_SUB(hire_date, INTERVAL 19800 SECOND)"
        + " > CAST('2005-10-17 00:00:00' AS DATETIME)";
    sql(sql0).withBigQuery().ok(expect0);

    final String sql1 = "select  * \n"
        + "from \"employee\" "
        + "where  \"hire_date\" + INTERVAL '10' HOUR > TIMESTAMP '2005-10-17 00:00:00' ";
    final String expect1 = "SELECT *\n"
        + "FROM foodmart.employee\n"
        + "WHERE DATETIME_ADD(hire_date, INTERVAL 10 HOUR) > CAST('2005-10-17 00:00:00' AS DATETIME)";
    sql(sql1).withBigQuery().ok(expect1);

    final String sql2 = "select  * from \"employee\" where  \"hire_date\" + "
        + "INTERVAL '1 2:34:56.78' DAY TO SECOND > TIMESTAMP '2005-10-17 00:00:00' ";
    sql(sql2).withBigQuery().throws_("For input string: \"56.78\"");
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
    final String expectedClickHouse = "SELECT SUBSTRING(`brand_name`, 2)\n"
        + "FROM `foodmart`.`product`";
    final String expectedOracle = "SELECT SUBSTR(\"brand_name\", 2)\n"
        + "FROM \"foodmart\".\"product\"";
    final String expectedPostgresql = "SELECT SUBSTRING(\"brand_name\", 2)\n"
        + "FROM \"foodmart\".\"product\"";
    final String expectedPresto = "SELECT SUBSTR(\"brand_name\", 2)\n"
        + "FROM \"foodmart\".\"product\"";
    final String expectedSnowflake = "SELECT SUBSTR(\"brand_name\", 2)\n"
            + "FROM \"foodmart\".\"product\"";
    final String expectedRedshift = expectedPostgresql;
    final String expectedMysql = "SELECT SUBSTRING(`brand_name`, 2)\n"
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
    final String expectedClickHouse = "SELECT SUBSTRING(`brand_name`, 2, 3)\n"
        + "FROM `foodmart`.`product`";
    final String expectedOracle = "SELECT SUBSTR(\"brand_name\", 2, 3)\n"
        + "FROM \"foodmart\".\"product\"";
    final String expectedPostgresql = "SELECT SUBSTRING(\"brand_name\", 2, 3)\n"
        + "FROM \"foodmart\".\"product\"";
    final String expectedPresto = "SELECT SUBSTR(\"brand_name\", 2, 3)\n"
        + "FROM \"foodmart\".\"product\"";
    final String expectedSnowflake = "SELECT SUBSTR(\"brand_name\", 2, 3)\n"
            + "FROM \"foodmart\".\"product\"";
    final String expectedRedshift = expectedPostgresql;
    final String expectedMysql = "SELECT SUBSTRING(`brand_name`, 2, 3)\n"
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


  @Test void testExistsCorrelation() {
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

  @Test void testNotExistsCorrelation() {
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

  @Test void testIlike() {
    String query = "select \"product_name\" from \"product\" a "
        + "where \"product_name\" ilike 'abC'";
    String expected = "SELECT \"product_name\"\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE \"product_name\" ILIKE 'abC'";
    sql(query).withLibrary(SqlLibrary.SNOWFLAKE).ok(expected);
  }

  @Test void testNotIlike() {
    final RelBuilder builder = relBuilder();
    RelNode root =
        builder.scan("EMP")
            .filter(
                builder.call(SqlLibraryOperators.NOT_ILIKE,
                    builder.field("ENAME"),
                    builder.literal("a%b%c")))
            .build();
    String expected = "SELECT *\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "WHERE \"ENAME\" NOT ILIKE 'a%b%c'";
    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expected));
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
        + "FROM (SELECT 1 AS \"a\", 'x ' AS \"b\"\n"
        + "UNION ALL\n"
        + "SELECT 2 AS \"a\", 'yy' AS \"b\") AS \"t\"";
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
//    final String expectedRedshift = expectedPostgresql;
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
        .ok(expectedSnowflake);
//        .withRedshift()
//        .ok(expectedRedshift);
  }

  @Test void testCorrelate() {
    final String sql = "select d.\"department_id\", d_plusOne "
        + "from \"department\" as d, "
        + "       lateral (select d.\"department_id\" + 1 as d_plusOne"
        + "                from (values(true)))";

    final String expected = "SELECT \"t\".\"department_id\", \"t1\".\"D_PLUSONE\"\n"
            + "FROM (SELECT \"department_id\", \"department_description\", \"department_id\" + 1 AS \"$f2\"\n"
            + "FROM \"foodmart\".\"department\") AS \"t\",\n"
            + "LATERAL (SELECT \"t\".\"$f2\" AS \"D_PLUSONE\"\n"
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
        + "FROM \"foodmart\".\"product\",\n"
        + "LATERAL (SELECT *\n"
        + "FROM TABLE(RAMP(\"product\".\"product_id\"))) AS \"t\"";
    sql(query).ok(expected);
  }

  @Test void testUncollectExplicitAlias() {
    final String sql = "select did + 1\n"
        + "from unnest(select collect(\"department_id\") as deptid"
        + "            from \"department\") as t(did)";

    final String expected = "SELECT \"DEPTID\" + 1\n"
        + "FROM UNNEST(COLLECT(\"department_id\") AS \"DEPTID\") AS \"t0\" (\"DEPTID\")";
    sql(sql).ok(expected);
  }

  @Test void testUncollectImplicitAlias() {
    final String sql = "select did + 1\n"
        + "from unnest(select collect(\"department_id\") "
        + "            from \"department\") as t(did)";

    final String expected = "SELECT \"col_0\" + 1\n"
        + "FROM UNNEST(COLLECT(\"department_id\")) AS \"t0\" (\"col_0\")";
    sql(sql).ok(expected);
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
        + "INNER JOIN foodmart.department ON TRUE";
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
        + "GROUP BY CUBE(product_id, product_class_id)";
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
        + "GROUP BY ROLLUP(product_id, product_class_id)";
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
        + "WHERE 10 = CAST('10' AS INT64) AND birth_date = CAST('1914-02-02' AS DATE) OR "
        + "hire_date = CAST('1996-01-01 ' || '00:00:00' AS DATETIME)";
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
        + "REGEXP_SUBSTR('chocolate Chip cookies', '(?i)[-\\_] V[0-9]+', 1, 1)\n"
        + "FROM foodmart.product\n"
        + "WHERE product_id = 1 OR product_id = 2 OR product_id = 3 OR product_id = 4";
    sql(query)
        .withBigQuery()
        .ok(expected);
  }

  @Test public void testTimestampFunctionRelToSql() {
    final RelBuilder builder = relBuilder();
    RelDataType relDataType =
        builder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP);
    final RexNode currentTimestampRexNode =
        builder.getRexBuilder().makeCall(relDataType,
            CURRENT_TIMESTAMP, Collections.singletonList(builder.literal(6)));
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
        + ".SSSSSS') AS TIMESTAMP) CT\nFROM scott.EMP";
    final String expectedHive = "SELECT CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss"
        + ".ssssss') AS TIMESTAMP) CT\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
    assertThat(toSql(root, DatabaseProduct.HIVE.getDialect()), isLinux(expectedHive));
  }

  @Test public void testGetDateFunctionRelToSql() {
    final RelBuilder builder = relBuilder();
    final RexNode getDateRexNode = builder.call(GETDATE);
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(getDateRexNode, "CURRENTDATETIME"))
        .build();
    final String expectedSpark = "SELECT GETDATE() CURRENTDATETIME\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
  }

  @Test public void testConcatFunctionWithMultipleArgumentsRelToSql() {
    final RelBuilder builder = relBuilder();
    final RexNode concatRexNode =
        builder.call(SqlLibraryOperators.CONCAT_FUNCTION, builder.literal("foo"),
                builder.literal("bar"), builder.literal("\\.com"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(concatRexNode, "CR"))
        .build();
    final String expectedSql = "SELECT CONCAT('foo', 'bar', '\\.com') AS \"CR\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT CONCAT('foo', 'bar', '\\\\.com') AS CR"
        + "\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testConcat2FunctionWithBooleanArgument() {
    final RelBuilder builder = relBuilder();
    final RexNode concatRexNode =
        builder.call(SqlLibraryOperators.CONCAT2, builder.literal("foo"),
            builder.literal("bar"), builder.literal("true"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(concatRexNode, "CR"))
        .build();
    final String expectedSql = "SELECT CONCAT('foo', 'bar', 'true') AS \"CR\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedPostgresSql = "SELECT CONCAT('foo', 'bar') AS \"CR\"\n"
        + "FROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.POSTGRESQL.getDialect()), isLinux(expectedPostgresSql));
  }

  @Test public void testDateFromPartsFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode dateFromPartsNode =
        builder.call(DATEFROMPARTS, builder.literal(2024),
            builder.literal(12), builder.literal(25));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(dateFromPartsNode, "DATEFROMPARTS"))
        .build();
    final String expectedMSSQL = "SELECT DATEFROMPARTS(2024, 12, 25) AS [DATEFROMPARTS]\n"
        + "FROM [scott].[EMP]";
    assertThat(toSql(root, DatabaseProduct.MSSQL.getDialect()), isLinux(expectedMSSQL));
  }

  @Test public void testMakeDateFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode makeDateNode =
        builder.call(MAKE_DATE, builder.literal(2024),
            builder.literal(12), builder.literal(25));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(makeDateNode, "MAKEDATE"))
        .build();
    final String expectedSpark = "SELECT MAKE_DATE(2024, 12, 25) MAKEDATE\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
  }

  @Test public void testDayFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode dayNode =
        builder.call(SqlLibraryOperators.DAY, builder.call(CURRENT_TIMESTAMP));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(dayNode, "day"))
        .build();
    final String expectedSpark = "SELECT DAY(CURRENT_TIMESTAMP) day\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
  }

  @Test public void testDateTimeDiffFunctionRelToSql() {
    final RelBuilder builder = relBuilder();
    final RexNode dateTimeDiffRexNode =
        builder.call(SqlLibraryOperators.DATETIME_DIFF,
                builder.call(CURRENT_DATE),
        builder.call(CURRENT_DATE), builder.literal(HOUR));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(dateTimeDiffRexNode, "HOURS"))
        .build();
    final String expectedSql = "SELECT DATETIME_DIFF(CURRENT_DATE, CURRENT_DATE, HOUR) AS "
        + "\"HOURS\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT DATETIME_DIFF(CURRENT_DATE, CURRENT_DATE, HOUR) AS "
        + "HOURS\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testDateDiffFunctionRelToSql() {
    final RelBuilder builder = relBuilder();
    final RexNode dateDiffRexNode =
        builder.call(SqlLibraryOperators.DATE_DIFF,
                builder.call(SqlStdOperatorTable.CURRENT_TIMESTAMP),
        builder.call(SqlStdOperatorTable.CURRENT_TIMESTAMP),
                builder.literal(HOUR));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(dateDiffRexNode, "HOURS"))
        .build();
    final String expectedSql = "SELECT DATE_DIFF(CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, HOUR) "
        + "AS \"HOURS\""
        + "\nFROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT DATE_DIFF(CURRENT_DATETIME(), CURRENT_DATETIME(), HOUR)"
        + " AS HOURS"
        + "\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testTimestampDiffFunctionRelToSql() {
    final RelBuilder builder = relBuilder();
    final RexNode timestampDiffRexNode =
        builder.call(SqlLibraryOperators.TIMESTAMP_DIFF, builder.call(SqlStdOperatorTable.CURRENT_TIMESTAMP),
        builder.call(SqlStdOperatorTable.CURRENT_TIMESTAMP), builder.literal(HOUR));
    final RelNode root = builder.scan("EMP")
        .project(builder.alias(timestampDiffRexNode, "HOURS")).build();
    final String expectedSql = "SELECT TIMESTAMP_DIFF(CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, HOUR)"
        + " AS \"HOURS\""
        + "\nFROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT TIMESTAMP_DIFF(CURRENT_DATETIME(), CURRENT_DATETIME(), "
        + "HOUR) AS HOURS"
        + "\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testRegexpInstr() {
    final RelBuilder builder = relBuilder();
    final RexNode regexpInstrWithTwoArgs =
        builder.call(SqlLibraryOperators.REGEXP_INSTR, builder.literal("Hello, Hello, World!"),
                builder.literal("Hello"));
    final RexNode regexpInstrWithThreeArgs =
        builder.call(SqlLibraryOperators.REGEXP_INSTR, builder.literal("Hello, Hello, World!"),
                builder.literal("Hello"), builder.literal(2));
    final RexNode regexpInstrWithFourArgs =
        builder.call(SqlLibraryOperators.REGEXP_INSTR, builder.literal("Hello, Hello, World!"),
                builder.literal("Hello"), builder.literal(2), builder.literal(1));
    final RexNode regexpInstrWithFiveArgs =
        builder.call(SqlLibraryOperators.REGEXP_INSTR, builder.literal("Hello, Hello, World!"),
                builder.literal("Hello"), builder.literal(2), builder.literal(1), builder.literal(1));
    final RelNode root = builder.scan("EMP")
        .project(builder.alias(regexpInstrWithTwoArgs, "position1"),
            builder.alias(regexpInstrWithThreeArgs, "position2"),
            builder.alias(regexpInstrWithFourArgs, "position3"),
            builder.alias(regexpInstrWithFiveArgs, "position4")).build();
    final String expectedSql = "SELECT REGEXP_INSTR('Hello, Hello, World!', 'Hello') "
        + "AS \"position1\", "
        + "REGEXP_INSTR('Hello, Hello, World!', 'Hello', 2) AS \"position2\", "
        + "REGEXP_INSTR('Hello, Hello, World!', 'Hello', 2, 1) AS \"position3\", "
        + "REGEXP_INSTR('Hello, Hello, World!', 'Hello', 2, 1, 1) AS \"position4\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT REGEXP_INSTR('Hello, Hello, World!', 'Hello') "
        + "AS position1, "
        + "REGEXP_INSTR('Hello, Hello, World!', 'Hello', 2) AS position2, "
        + "REGEXP_INSTR('Hello, Hello, World!', 'Hello', 2, 1) AS position3, "
        + "REGEXP_INSTR('Hello, Hello, World!', 'Hello', 2, 1, 1) AS position4\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
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
    final String expectedSpark = "SELECT ADD_MONTHS(birth_date, -1)\nFROM foodmart.employee";
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
    final String expectedSpark = "SELECT ADD_MONTHS(birth_date, -10)\nFROM foodmart"
        + ".employee";
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

  @Test public void testTimestampMinusIntervalDayToSecond() {
    String query = "select \"hire_date\" - (10 * INTERVAL '20 10:10:10' DAY TO SECOND) from \"employee\"";
    final String expectedBigQuery = "SELECT hire_date - 10 * INTERVAL '20 10:10:10' DAY TO SECOND\n"
        + "FROM foodmart.employee";
    sql(query)
        .withBigQuery()
        .ok(expectedBigQuery);
  }

  @Test public void testTimestampPlusIntervalMonthFunctionWithArthOps() {
    String query = "select \"hire_date\" + -10 * INTERVAL '1' MONTH from \"employee\"";
    final String expectedBigQuery = "SELECT DATETIME_ADD(hire_date, "
        + "INTERVAL "
        + "-10 MONTH)\n"
        + "FROM foodmart.employee";
    sql(query)
        .withBigQuery()
        .ok(expectedBigQuery);
  }

  @Test public void testDatePlusIntervalMonthFunctionWithCol() {
    String query = "select \"birth_date\" +  \"store_id\" * INTERVAL '10' MONTH from \"employee\"";
    final String expectedHive = "SELECT ADD_MONTHS(birth_date, store_id * 10)\n"
        + "FROM foodmart.employee";
    final String expectedSpark = "SELECT ADD_MONTHS(birth_date, store_id * 10)\nFROM "
        + "foodmart.employee";
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
    final String expectedSpark = "SELECT ADD_MONTHS(birth_date, 10 * 2)\nFROM foodmart"
        + ".employee";
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
    final String expectedSpark = "SELECT birth_date + 1\nFROM foodmart.employee";
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
    final String expectedSpark = "SELECT birth_date - 1\nFROM foodmart.employee";
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
    final String expectedSpark = "SELECT DATE '2018-01-01' + 1\nFROM foodmart"
        + ".employee";
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
    final String expectedSpark = "SELECT DATE '2018-01-01' - 1\n"
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
    final String expectedSpark = "SELECT birth_date + 2\nFROM foodmart.employee";
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
    final String expectedBigQuery = "SELECT "
        + "DATETIME_ADD(CAST(birth_date AS DATETIME), INTERVAL 2 MINUTE)\n"
            + "FROM foodmart.employee";
    sql(query)
            .withBigQuery()
            .ok(expectedBigQuery);
  }

  @Test public void testIntervalHour() {
    String query = "select cast(\"birth_date\" as timestamp) + INTERVAL\n"
            + "'2' hour from \"employee\"";
    final String expectedBigQuery = "SELECT "
        + "DATETIME_ADD(CAST(birth_date AS DATETIME), INTERVAL 2 HOUR)\n"
            + "FROM foodmart.employee";
    sql(query)
            .withBigQuery()
            .ok(expectedBigQuery);
  }
  @Test public void testIntervalSecond() {
    String query = "select cast(\"birth_date\" as timestamp) + INTERVAL '2'\n"
            + "second from \"employee\"";
    final String expectedBigQuery = "SELECT "
        + "DATETIME_ADD(CAST(birth_date AS DATETIME), INTERVAL 2 SECOND)\n"
        + "FROM foodmart.employee";
    sql(query)
            .withBigQuery()
            .ok(expectedBigQuery);
  }

  @Test public void testDateSubInterFunction() {
    String query = "select \"birth_date\" - INTERVAL '2' day from \"employee\"";
    final String expectedHive = "SELECT CAST(DATE_SUB(birth_date, 2) AS DATE)\n"
        + "FROM foodmart.employee";
    final String expectedSpark = "SELECT birth_date - 2"
        + "\nFROM foodmart.employee";
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
    final String expectedSpark = "SELECT birth_date + store_id"
        + "\nFROM foodmart.employee";
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
    final String expectedSpark = "SELECT birth_date + store_id\nFROM foodmart"
        + ".employee";
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
    final String expectedSpark = "SELECT birth_date + 10\n"
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
    final String expectedSpark = "SELECT birth_date - store_id"
        + "\nFROM foodmart.employee";
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
    final String expectedSpark = "SELECT DATE '2018-01-01' + store_id\nFROM "
        + "foodmart.employee";
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
    final String expectedSpark = "SELECT birth_date + store_id * 11\nFROM "
        +  "foodmart.employee";
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
    final String expectedSpark = "SELECT birth_date + store_id * 11\nFROM "
        + "foodmart.employee";
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
    final String expectedSpark = "SELECT birth_date - store_id * 11\nFROM "
        + "foodmart.employee";
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
    final String expectedSpark = "SELECT birth_date + 10 * 2\n"
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
    final String expectedSpark = "SELECT birth_date + 1\n"
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
    final String expectedBQ = "SELECT CURRENT_DATETIME() + INTERVAL 22230 SECOND, "
        + "TIMESTAMP_SUB(CURRENT_DATETIME(), INTERVAL 22230 SECOND)\n"
            + "FROM foodmart.employee";
    sql(query)
        .withBigQuery()
        .ok(expectedBQ);
  }

  @Test public void testUnparseMinusCallWithReturnTypeOfTimestampWithZoneToTimestampSub() {
    final RelBuilder relBuilder = relBuilder();
    final RexBuilder rexBuilder = relBuilder.getRexBuilder();

    final RexLiteral literalTimestampLTZ =
        rexBuilder.makeTimestampWithLocalTimeZoneLiteral(
            new TimestampString(2022, 2, 18, 8, 23, 45), 0);

    final RexLiteral intervalLiteral =
        rexBuilder.makeIntervalLiteral(new BigDecimal(1000),
                new SqlIntervalQualifier(MICROSECOND, null, SqlParserPos.ZERO));

    final RexNode minusCall =
        relBuilder.call(SqlStdOperatorTable.MINUS, literalTimestampLTZ, intervalLiteral);

    final RelNode root = relBuilder
                            .values(new String[] {"c"}, 1)
                            .project(minusCall)
                            .build();

    final String expectedBigQuery = "SELECT TIMESTAMP_SUB(TIMESTAMP '2022-02-18 08:23:45'"
        + ", INTERVAL 1 MICROSECOND) AS `$f0`";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBigQuery));
  }

  @Test public void testUnparsePlusCallWithReturnTypeOfTimestampWithZoneToTimestampAdd() {
    final RelBuilder relBuilder = relBuilder();
    final RexBuilder rexBuilder = relBuilder.getRexBuilder();

    final RexLiteral literalTimestampLTZ =
        rexBuilder.makeTimestampWithLocalTimeZoneLiteral(
            new TimestampString(2022, 2, 18, 8, 23, 45), 0);

    final RexLiteral intervalLiteral =
        rexBuilder.makeIntervalLiteral(new BigDecimal(1000),
                new SqlIntervalQualifier(MICROSECOND, null, SqlParserPos.ZERO));

    final RexNode plusCall =
        relBuilder.call(PLUS, literalTimestampLTZ, intervalLiteral);

    final RelNode root = relBuilder
        .values(new String[] {"c"}, 1)
        .project(plusCall)
        .build();

    final String expectedBigQuery = "SELECT DATETIME_ADD(TIMESTAMP '2022-02-18 08:23:45', "
        + "INTERVAL 1 MICROSECOND) AS `$f0`";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBigQuery));
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
    final String expectedSpark = "SELECT EXTRACT(YEAR FROM hire_date)\n"
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
    final String expectedBigQuery = "SELECT CURRENT_DATETIME() AS `CURRENT_TIMESTAMP`";

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
    final String expectedHive = "SELECT CONCAT('foo', 'bar')\n"
        + "FROM foodmart.employee";
    final String mssql = "SELECT CONCAT('foo', 'bar')\n"
            + "FROM [foodmart].[employee]";
    final String expected = "SELECT 'foo' || 'bar'\n"
        + "FROM foodmart.employee";
    sql(query)
        .withHive()
        .ok(expectedHive)
        .withSpark()
        .ok(expected)
        .withBigQuery()
        .ok(expected)
        .withMssql()
        .ok(mssql);
  }

  @Test public void testTruncate() {
    final RelBuilder builder = relBuilder();
    final RexNode trunc =
        builder.call(SqlStdOperatorTable.TRUNCATE, builder.literal(1234.56), builder.literal(1));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(trunc, "FD"))
        .build();
    final String expectedPostgresSql = "SELECT TRUNC(1234.56, 1) AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.POSTGRESQL.getDialect()), isLinux(expectedPostgresSql));
  }

  @Test public void testUidAndPgBackEndId() {
    final RelBuilder builder = relBuilder();
    final RexNode oracleUID = builder.call(SqlLibraryOperators.UID);
    final RexNode pgBackendId = builder.call(SqlLibraryOperators.PG_BACKEND_PID);
    final RelNode root = builder
        .scan("EMP")
        .project(oracleUID, pgBackendId)
        .build();
    final String expectedBqQuery = "SELECT UID() AS \"$f0\", PG_BACKEND_PID() AS "
        + "\"$f1\"\n"
        + "FROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedBqQuery));
  }

  @Test public void testAgeFunction() {
    final RelBuilder builder = relBuilder();
    final RexLiteral timestampLiteral1 =
        builder.getRexBuilder().makeTimestampLiteral(
            new TimestampString(2022, 2, 18, 8, 23, 45), 0);
    final RexLiteral timestampLiteral2 =
        builder.getRexBuilder().makeTimestampLiteral(
            new TimestampString(2023, 4, 18, 8, 23, 45), 0);
    final RexNode ageNode =
        builder.call(SqlLibraryOperators.AGE, timestampLiteral1, timestampLiteral2);
    final RelNode root = builder
        .scan("EMP")
        .project(ageNode)
        .build();
    final String expectedPostgresSql = "SELECT AGE(TIMESTAMP '2022-02-18 08:23:45', TIMESTAMP "
        + "'2023-04-18 08:23:45') AS \"$f0\"\n"
        + "FROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.POSTGRESQL.getDialect()), isLinux(expectedPostgresSql));
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


  @Test void testDecimalInBQ() {
    String query = "SELECT CAST(\"department_id\" AS DECIMAL(19,0)) FROM \"employee\"";
    String expected = "SELECT CAST(department_id AS NUMERIC)\n"
        + "FROM foodmart.employee";
    sql(query)
        .withBigQuery()
        .ok(expected);
  }

  @Test void testDecimalWithMaxPrecisionInBQ() {
    String query = "SELECT CAST(\"department_id\" AS DECIMAL(38,10)) FROM \"employee\"";
    String expected = "SELECT CAST(department_id AS BIGNUMERIC)\n"
        + "FROM foodmart.employee";
    sql(query)
        .withBigQuery()
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
    final SqlDialect oracleDialect = DatabaseProduct.ORACLE.getDialect();
    assertFalse(oracleDialect.supportsDataType(booleanDataType));
    assertTrue(oracleDialect.supportsDataType(integerDataType));
    final SqlDialect postgresqlDialect = DatabaseProduct.POSTGRESQL.getDialect();
    assertTrue(postgresqlDialect.supportsDataType(booleanDataType));
    assertTrue(postgresqlDialect.supportsDataType(integerDataType));
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
        + "WHERE 10 = CAST('10' AS INT64) AND birth_date = CAST('1914-02-02' AS DATE) OR "
        + "hire_date = CAST('1996-01-01 ' || '00:00:00' AS DATETIME)";
    sql(query)
        .ok(expected)
        .withBigQuery()
        .ok(expectedBiqquery);
  }

  @Test public void testToNumberFunctionHandlingHexaToInt() {
    String query = "select TO_NUMBER('03ea02653f6938ba','XXXXXXXXXXXXXXXX')";
    final String expected = "SELECT CAST(CONV('03ea02653f6938ba', 16, 10) AS BIGINT)";
    final String expectedBigQuery = "SELECT CAST('0x' || '03ea02653f6938ba' AS INT64)";
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
    final String expectedBQ = "SELECT *\n"
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
    final String expectedSpark = "SELECT *\n"
        + "FROM (SELECT product_id, MAX(product_id) OVER (PARTITION BY product_id RANGE BETWEEN "
        + "UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ABC\n"
        + "FROM foodmart.product) t\n"
        + "GROUP BY product_id, ABC";
    sql(query)
      .withHive()
      .ok(expected)
      .withSpark()
      .ok(expectedSpark)
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
    final String expected = "SELECT first_name, COUNT(*) department_id_number, ROW_NUMBER() "
        + "OVER (ORDER BY department_id IS NULL, department_id), SUM(department_id) "
        + "OVER (ORDER BY department_id IS NULL, department_id "
        + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)\n"
        + "FROM foodmart.employee\n"
        + "GROUP BY first_name, department_id";
    final String expectedSpark = "SELECT first_name, COUNT(*) department_id_number, ROW_NUMBER() "
        + "OVER (ORDER BY department_id NULLS LAST), SUM(department_id) "
        + "OVER (ORDER BY department_id NULLS LAST "
        + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)\n"
        + "FROM foodmart.employee\n"
        + "GROUP BY first_name, department_id";
    final String expectedBQ = "SELECT first_name, COUNT(*) AS department_id_number, ROW_NUMBER() "
        + "OVER (ORDER BY department_id IS NULL, department_id), SUM(department_id) "
        + "OVER (ORDER BY department_id IS NULL, department_id "
        + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)\n"
        + "FROM foodmart.employee\n"
        + "GROUP BY first_name, department_id";
    final String expectedSnowFlake = "SELECT \"first_name\", COUNT(*) AS \"department_id_number\", "
        + "ROW_NUMBER() OVER (ORDER BY \"department_id\"), SUM(\"department_id\") "
        + "OVER (ORDER BY \"department_id\" RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)\n"
        + "FROM \"foodmart\".\"employee\"\n"
        + "GROUP BY \"first_name\", \"department_id\"";
    final String mssql = "SELECT [first_name], COUNT(*) AS [department_id_number], ROW_NUMBER()"
        + " OVER (ORDER BY CASE WHEN [department_id] IS NULL THEN 1 ELSE 0 END,"
        + " [department_id]), SUM([department_id]) OVER (ORDER BY CASE WHEN [department_id] IS NULL"
        + " THEN 1 ELSE 0 END, [department_id] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)\n"
        + "FROM [foodmart].[employee]\n"
        + "GROUP BY [first_name], [department_id]";
    sql(query)
      .withHive()
      .ok(expected)
      .withSpark()
      .ok(expectedSpark)
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
    final String expectedBigQuery = "SELECT ASCII('ABC')";
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
    final String expectedBigQuery = "SELECT ASCII(SUBSTR('ABC', 1, 1))";
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
    final String bigQueryExpected = "SELECT ASCII(product_name)\n"
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
    final RexNode nullifRexNode =
        builder.call(SqlStdOperatorTable.NULLIF, builder.scan("EMP").field(0), builder.literal(20));
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
    final String expectedSpark = "SELECT CAST(DATE_FORMAT(CAST(birth_date AS TIMESTAMP), "
        + "'yyyy-MM-dd HH:mm:ss.SSS') AS TIMESTAMP)\nFROM foodmart.employee";
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
    final String expectedSpark = "SELECT CAST('1970-01-01 ' || DATE_FORMAT(hire_date, 'HH:mm:ss') "
        + "AS TIMESTAMP)\nFROM foodmart.employee";
    final String expectedBigQuery = "SELECT CAST(hire_date AS TIME)\n"
        + "FROM foodmart.employee";
    sql(query)
        .withHive()
        .ok(expected)
        .withSpark()
        .ok(expectedSpark)
        .withBigQuery()
        .ok(expectedBigQuery);
  }

  @Test public void testCastToTimeWithPrecision() {
    String query = "SELECT cast(\"hire_date\" as TIME(5)) "
        + "FROM \"foodmart\".\"employee\"";
    final String expectedHive = "SELECT SPLIT(DATE_FORMAT(hire_date, 'yyyy-MM-dd HH:mm:ss.sss'), "
        + "' ')[1]\n"
        + "FROM foodmart.employee";
    final String expectedSpark = "SELECT CAST('1970-01-01 ' || DATE_FORMAT(hire_date, 'HH:mm:ss"
        + ".SSS') AS TIMESTAMP)\nFROM foodmart.employee";
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
    final String expectedSpark = "SELECT CAST('1970-01-01 ' || "
        + "DATE_FORMAT('12:00' || ':05', 'HH:mm:ss.SSS') AS TIMESTAMP)\nFROM foodmart.employee";
    final String expectedBigQuery = "SELECT CAST(FORMAT_TIME('%H:%M:%E3S', CAST('12:00' || ':05' "
        + "AS TIME)) AS TIME)\n"
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
    final String expectedSpark = "SELECT TIMESTAMP '1970-01-01 12:00:05.000'\n"
        + "FROM foodmart.employee";
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

  @Test public void testCastToTimeWithPrecisionWithTimeZoneStringLiteral() {
    String query = "SELECT cast('12:00:05+08:30' as TIME(3)) "
        + "FROM \"foodmart\".\"employee\"";
    final String expectedSpark =  "SELECT CAST('1970-01-01 ' || "
        + "DATE_FORMAT('12:00:05+08:30', 'HH:mm:ss.SSS') AS TIMESTAMP)\nFROM foodmart.employee";
    sql(query)
        .withSpark()
        .ok(expectedSpark);
  }

  @Test public void testFormatDateRelToSql() {
    final RelBuilder builder = relBuilder();
    final RexNode formatDateRexNode =
        builder.call(SqlLibraryOperators.FORMAT_DATE, builder.literal("YYYY-MM-DD"), builder.scan("EMP").field(4));
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

  @Test public void testUnparseOfDateFromUnixDateWithFloorFunctionAsOperand() {
    final RelBuilder builder = relBuilder();
    builder.scan("EMP");
    final RexNode epochSeconds =
        builder.cast(builder.literal("'20091223'"), SqlTypeName.INTEGER);
    final RexNode epochDays =
        builder.call(SqlStdOperatorTable.FLOOR,
                builder.call(SqlStdOperatorTable.DIVIDE, epochSeconds, builder.literal(86400)));
    final RexNode dateFromUnixDate =
        builder.call(SqlLibraryOperators.DATE_FROM_UNIX_DATE, epochDays);
    final RelNode root = builder
        .project(builder.alias(dateFromUnixDate, "unix_date"))
        .build();
    final String expectedSql = "SELECT DATE_FROM_UNIX_DATE(FLOOR(CAST('''20091223''' AS INTEGER) "
        + "/ 86400)) AS \"unix_date\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT DATE_FROM_UNIX_DATE(CAST(FLOOR(CAST"
        + "('\\'20091223\\'' AS INT64) / 86400) AS INTEGER)) AS unix_date\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testDateFunction() {
    final RelBuilder builder = relBuilder();
    RexNode dateRex0 =
        builder.call(SqlLibraryOperators.DATE, builder.literal("1970-02-02 01:02:03"));
    RexNode dateRex1 =
        builder.call(SqlLibraryOperators.DATE, builder.literal("1970-02-02"));
    RexNode dateRex2 =
        builder.call(SqlLibraryOperators.DATE, builder.cast(builder.literal("1970-02-02"), SqlTypeName.DATE));
    RexNode dateRex3 =
        builder.call(SqlLibraryOperators.DATE,
                builder.cast(builder.literal("1970-02-02 01:02:03"), SqlTypeName.TIMESTAMP));

    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(dateRex0, "date0"), builder.alias(dateRex1, "date1"),
            builder.alias(dateRex2, "date2"), builder.alias(dateRex3, "date3"))
        .build();

    final String expectedBigQuery = "SELECT DATE('1970-02-02 01:02:03') AS date0, "
        + "DATE('1970-02-02') AS date1, DATE(DATE '1970-02-02') AS date2, "
        + "DATE(CAST('1970-02-02 01:02:03' AS DATETIME)) AS date3\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()),
        isLinux(expectedBigQuery));
  }

  @Test public void testTimestampFunction() {
    final RelBuilder builder = relBuilder();
    RexNode timestampRex0 =
        builder.call(SqlLibraryOperators.TIMESTAMP, builder.literal("1970-02-02"));
    RexNode timestampRex1 =
        builder.call(SqlLibraryOperators.TIMESTAMP, builder.literal("1970-02-02 01:02:03"));
    RexNode timestampRex2 =
        builder.call(SqlLibraryOperators.TIMESTAMP, builder.cast(builder.literal("1970-02-02"), SqlTypeName.DATE));
    RexNode timestampRex3 =
        builder.call(SqlLibraryOperators.TIMESTAMP,
                builder.cast(builder.literal("1970-02-02 01:02:03"), SqlTypeName.TIMESTAMP));

    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(timestampRex0, "timestamp0"),
            builder.alias(timestampRex1, "timestamp1"),
            builder.alias(timestampRex2, "timestamp2"),
            builder.alias(timestampRex3, "timestamp3"))
        .build();

    final String expectedBigQuery = "SELECT TIMESTAMP('1970-02-02') AS timestamp0, "
        + "TIMESTAMP('1970-02-02 01:02:03') AS timestamp1, "
        + "TIMESTAMP(DATE '1970-02-02') AS timestamp2, "
        + "TIMESTAMP(CAST('1970-02-02 01:02:03' AS DATETIME)) AS timestamp3\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()),
        isLinux(expectedBigQuery));
  }

  @Test public void testDOMAndDOY() {
    final RelBuilder builder = relBuilder();
    final RexNode dayOfMonthRexNode =
            builder.call(SqlLibraryOperators.FORMAT_DATE, builder.literal("W"), builder.scan("EMP").field(4));
    final RexNode dayOfYearRexNode =
            builder.call(SqlLibraryOperators.FORMAT_DATE, builder.literal("WW"), builder.scan("EMP").field(4));

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

  @Test public void testYYYYWW() {
    final RelBuilder builder = relBuilder();
    final RexNode dayOfYearWithYYYYRexNode =
        builder.call(SqlLibraryOperators.FORMAT_DATE, builder.literal("YYYY-WW"), builder.scan("EMP").field(4));

    final RelNode doyRoot = builder
        .scan("EMP")
        .project(builder.alias(dayOfYearWithYYYYRexNode, "FD"))
        .build();

    final String expectedDOYBiqQuery = "SELECT FORMAT_DATE('%Y-%W', HIREDATE) AS FD\n"
        + "FROM scott.EMP";

    assertThat(toSql(doyRoot, DatabaseProduct.BIG_QUERY.getDialect()),
        isLinux(expectedDOYBiqQuery));
  }

  @Test public void testFormatTimestampRelToSql() {
    final RelBuilder builder = relBuilder();
    final RexNode formatTimestampRexNode =
        builder.call(SqlLibraryOperators.FORMAT_TIMESTAMP, builder.literal("YYYY-MM-DD HH:MI:SS.S(5)"),
                builder.scan("EMP").field(4));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(formatTimestampRexNode, "FD"))
        .build();
    final String expectedSql = "SELECT FORMAT_TIMESTAMP('YYYY-MM-DD HH:MI:SS.S(5)', \"HIREDATE\") "
        + "AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedSpark = "SELECT DATE_FORMAT(HIREDATE, 'yyyy-MM-dd hh:mm:ss.SSSSS') FD\n"
        + "FROM scott.EMP";
    final String expectedBiqQuery = "SELECT FORMAT_TIMESTAMP('%F %I:%M:%E5S', HIREDATE) AS FD\n"
        + "FROM scott.EMP";
    final String expectedHive = "SELECT DATE_FORMAT(HIREDATE, 'yyyy-MM-dd hh:mm:ss.sssss') FD\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.HIVE.getDialect()), isLinux(expectedHive));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
  }

  @Test public void testFormatTimestampFormatsRelToSql() {
    final RelBuilder builder = relBuilder();
    final RexNode formatTimestampRexNode2 =
        builder.call(SqlLibraryOperators.FORMAT_TIMESTAMP, builder.literal("HH24MI"),
                builder.scan("EMP").field(4));
    final RexNode formatTimestampRexNode3 =
        builder.call(SqlLibraryOperators.FORMAT_TIMESTAMP, builder.literal("HH24MISS"),
                builder.scan("EMP").field(4));
    final RexNode formatTimestampRexNode4 =
        builder.call(SqlLibraryOperators.FORMAT_TIMESTAMP, builder.literal("YYYYMMDDHH24MISS"),
                builder.scan("EMP").field(4));
    final RexNode formatTimestampRexNode5 =
        builder.call(SqlLibraryOperators.FORMAT_TIMESTAMP, builder.literal("YYYYMMDDHHMISS"),
                builder.scan("EMP").field(4));
    final RexNode formatTimestampRexNode6 =
        builder.call(SqlLibraryOperators.FORMAT_TIMESTAMP, builder.literal("YYYYMMDDHH24MI"),
                builder.scan("EMP").field(4));
    final RexNode formatTimestampRexNode7 =
        builder.call(SqlLibraryOperators.FORMAT_TIMESTAMP, builder.literal("YYYYMMDDHH24"),
                builder.scan("EMP").field(4));
    final RexNode formatTimestampRexNode8 =
        builder.call(SqlLibraryOperators.FORMAT_TIMESTAMP, builder.literal("MS"),
                builder.scan("EMP").field(4));
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
    final RexNode formatTimeRexNode =
        builder.call(SqlLibraryOperators.FORMAT_TIME, builder.literal("HH:MI:SS"), builder.scan("EMP").field(4));
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
    final RexNode strToDateNode1 =
        builder.call(SqlLibraryOperators.STR_TO_DATE, builder.literal("20181106"), builder.literal("YYYYMMDD"));
    final RexNode strToDateNode2 =
        builder.call(SqlLibraryOperators.STR_TO_DATE, builder.literal("2018/11/06"), builder.literal("YYYY/MM/DD"));
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
    final String expectedSpark = "SELECT TO_DATE('20181106', 'yyyyMMdd') date1, "
        + "TO_DATE('2018/11/06', 'yyyy/MM/dd') date2\nFROM scott.EMP";
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
    final RexNode formatDateNode1 =
            builder.call(SqlLibraryOperators.FORMAT_DATETIME, builder.literal("DDMMYY"),
                    builder.literal("2008-12-25 15:30:00"));
    final RexNode formatDateNode2 =
            builder.call(SqlLibraryOperators.FORMAT_DATETIME, builder.literal("YY/MM/DD"),
                    builder.literal("2012-12-25 12:50:10"));
    final RexNode formatDateNode3 =
        builder.call(SqlLibraryOperators.FORMAT_DATETIME, builder.literal("YY-MM-01"),
                builder.literal("2012-12-25 12:50:10"));
    final RexNode formatDateNode4 =
        builder.call(SqlLibraryOperators.FORMAT_DATETIME, builder.literal("YY-MM-DD 00:00:00"),
                builder.literal("2012-12-25 12:50:10"));
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

  @Test public void testConvertTimezoneFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode convertTimezoneNode =
        builder.call(SqlLibraryOperators.CONVERT_TIMEZONE_SF, builder.literal("America/Los_Angeles"),
                builder.literal("2008-08-21 07:23:54"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(convertTimezoneNode, "time"))
        .build();
    final String expectedSF =
        "SELECT CONVERT_TIMEZONE_SF('America/Los_Angeles', '2008-08-21 07:23:54') AS \"time\"\nFROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedSF));
  }

  @Test public void testParseTimestampWithTimezoneFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode parseTSNode =
        builder.call(SqlLibraryOperators.PARSE_TIMESTAMP_WITH_TIMEZONE,
        builder.literal("%c%z"), builder.call(SqlLibraryOperators.FORMAT_TIMESTAMP,
            builder.literal("%c%z"),
            builder.cast(builder.literal("2008-08-21 07:23:54"), SqlTypeName.TIMESTAMP),
            builder.literal("America/Los_Angeles")));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(parseTSNode, "timestamp"))
        .build();
    final String expectedBigQuery =
        "SELECT PARSE_TIMESTAMP('%c%z', FORMAT_TIMESTAMP('%c%z', CAST('2008-08-21 07:23:54' AS "
            + "DATETIME), 'America/Los_Angeles')) AS timestamp\n"
            + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBigQuery));
  }

  @Test public void testTimeWithTimezoneFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode formatTimestampRexNode =
        builder.call(SqlLibraryOperators.FORMAT_TIMESTAMP, builder.literal("%c%z"),
                builder.call(CURRENT_TIMESTAMP));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(formatTimestampRexNode, "FD2"))
        .build();
    final String expectedBigQuery = "SELECT FORMAT_TIMESTAMP('%c%z', CURRENT_DATETIME()) AS FD2\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBigQuery));
  }

  @Test public void testParseTimestampFunctionFormat() {
    final RelBuilder builder = relBuilder();
    final RexNode parseTSNode1 =
        builder.call(SqlLibraryOperators.PARSE_TIMESTAMP, builder.literal("YYYY-MM-dd HH24:MI:SS"),
                builder.literal("2009-03-20 12:25:50"));
    final RexNode parseTSNode2 =
        builder.call(SqlLibraryOperators.PARSE_TIMESTAMP, builder.literal("MI dd-YYYY-MM SS HH24"),
                builder.literal("25 20-2009-03 50 12"));
    final RexNode parseTSNode3 =
        builder.call(SqlLibraryOperators.PARSE_TIMESTAMP, builder.literal("yyyy@MM@dd@hh@mm@ss"),
                builder.literal("20200903020211"));
    final RexNode parseTSNode4 =
        builder.call(SqlLibraryOperators.PARSE_TIMESTAMP, builder.literal("yyyy@MM@dd@HH@mm@ss"),
                builder.literal("20200903210211"));
    final RexNode parseTSNode5 =
        builder.call(SqlLibraryOperators.PARSE_TIMESTAMP, builder.literal("HH@mm@ss"), builder.literal("215313"));
    final RexNode parseTSNode6 =
        builder.call(SqlLibraryOperators.PARSE_TIMESTAMP, builder.literal("MM@dd@yy"), builder.literal("090415"));
    final RexNode parseTSNode7 =
        builder.call(SqlLibraryOperators.PARSE_TIMESTAMP, builder.literal("MM@dd@yy"), builder.literal("Jun1215"));
    final RexNode parseTSNode8 =
        builder.call(SqlLibraryOperators.PARSE_TIMESTAMP, builder.literal("yyyy@MM@dd@HH"),
                builder.literal("2015061221"));
    final RexNode parseTSNode9 =
        builder.call(SqlLibraryOperators.PARSE_TIMESTAMP, builder.literal("yyyy@dd@mm"), builder.literal("20150653"));
    final RexNode parseTSNode10 =
        builder.call(SqlLibraryOperators.PARSE_TIMESTAMP, builder.literal("yyyy@mm@dd"), builder.literal("20155308"));
    final RexNode parseTSNode11 =
        builder.call(SqlLibraryOperators.PARSE_TIMESTAMP, builder.literal("YYYY-MM-dd@HH:mm:ss"),
                builder.literal("2009-03-2021:25:50"));
    final RexNode parseTSNode12 =
        builder.call(SqlLibraryOperators.PARSE_TIMESTAMP, builder.literal("YYYY-MM-dd@hh:mm:ss"),
                builder.literal("2009-03-2007:25:50"));
    final RexNode parseTSNode13 =
        builder.call(SqlLibraryOperators.PARSE_TIMESTAMP, builder.literal("YYYY-MM-dd@hh:mm:ss z"),
                builder.literal("2009-03-20 12:25:50.222"));
    final RexNode parseTSNode14 =
        builder.call(SqlLibraryOperators.PARSE_TIMESTAMP, builder.literal("YYYY-MM-dd'T'hh:mm:ss"),
                builder.literal("2012-05-09T04:12:12"));
    final RexNode parseTSNode15 =
        builder.call(SqlLibraryOperators.PARSE_TIMESTAMP, builder.literal("yyyy- MM-dd  HH: -mm:ss"),
                builder.literal("2015- 09-11  09: -07:23"));
    final RexNode parseTSNode16 =
        builder.call(SqlLibraryOperators.PARSE_TIMESTAMP, builder.literal("yyyy- MM-dd@HH: -mm:ss"),
                builder.literal("2015- 09-1109: -07:23"));
    final RexNode parseTSNode17 =
        builder.call(SqlLibraryOperators.PARSE_TIMESTAMP, builder.literal("yyyy-MM-dd-HH:mm:ss.S(3)@ZZ"),
                builder.literal("2015-09-11-09:07:23"));
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
    final RexNode parseTSNode1 =
        builder.call(SqlLibraryOperators.TO_TIMESTAMP, builder.literal("2009-03-20 12:25:50"),
                builder.literal("yyyy-MM-dd HH24:MI:SS"));
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
    final RexNode parseTSNode1 =
        builder.call(SqlLibraryOperators.TO_TIMESTAMP, builder.literal("Jan 15, 1989, 11:00:06 AM"),
                builder.literal("MMM dd, YYYY,HH:MI:SS AM"));
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
    final RexNode parseTSNode1 =
        builder.call(SqlLibraryOperators.DATE_DIFF, builder.literal("1994-07-21"), builder.literal("1993-07-21"));
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
    final RexNode parseTSNode1 =
        builder.call(SqlLibraryOperators.DATE_DIFF, builder.literal("1994-07-21"),
                builder.literal("1993-07-21"), builder.literal(MONTH));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(parseTSNode1, "date_diff_value"))
        .build();
    final String expectedSql =
        "SELECT DATE_DIFF('1994-07-21', '1993-07-21', MONTH) AS \"date_diff_value\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBQ =
        "SELECT DATE_DIFF('1994-07-21', '1993-07-21', MONTH) AS date_diff_value\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQ));
  }

  @Test public void testToDateFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode parseTSNode1 =
        builder.call(SqlLibraryOperators.TO_DATE, builder.literal("2009/03/20"),
                builder.literal("yyyy/MM/dd"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(parseTSNode1, "date_value"))
        .build();
    final String expectedSql =
        "SELECT TO_DATE('2009/03/20', 'yyyy/MM/dd') AS \"date_value\"\n"
            + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
  }

  @Test public void testToDateFunctionWithAMInFormat() {
    final RelBuilder builder = relBuilder();
    final RexNode toDateNode =
        builder.call(SqlLibraryOperators.TO_DATE, builder.literal("January 15, 1989, 11:00 A.M."),
        builder.literal("MMMM DD, YYYY, HH: MI A.M."));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(toDateNode, "date_value"))
        .build();
    final String expectedSparkQuery =
        "SELECT TO_DATE('JANUARY 15, 1989, 11:00 AM', 'MMMM dd, yyyy, hh: mm a') date_value\n"
            + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testToDateFunctionWithPMInFormat() {
    final RelBuilder builder = relBuilder();
    final RexNode toDateNode =
        builder.call(SqlLibraryOperators.TO_DATE, builder.literal("January 15, 1989, 11:00 P.M."),
        builder.literal("MMMM DD, YYYY, HH: MI P.M."));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(toDateNode, "date_value"))
        .build();
    final String expectedSparkQuery =
        "SELECT TO_DATE('JANUARY 15, 1989, 11:00 PM', 'MMMM dd, yyyy, hh: mm a') date_value\n"
            + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
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
    final RexNode intdivideRexNode =
            builder.call(SqlStdOperatorTable.DIVIDE_INTEGER, builder.scan("EMP").field(0),
                    builder.scan("EMP").field(3));
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
    final String expectedSparkSql = "SELECT UDF_ROUND(123.41445, product_id) a\n"
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
            .ok(expectedSparkSql)
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
    final String expectedSparkSql = "SELECT ROUND(123.41445) a\n"
            + "FROM foodmart.product";
    sql(query)
            .withMssql()
            .ok(expectedMssql)
            .withSpark()
            .ok(expectedSparkSql);
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

  @Test public void testRandFunction() {
    String query = "select rand() from \"employee\"";
    final String expectedSpark = "SELECT RANDOM()\n"
        + "FROM \"foodmart\".\"employee\"";
    sql(query)
        .withPostgresql()
        .ok(expectedSpark);
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
    final RexNode condition =
            builder.call(SqlLibraryOperators.FORMAT_DATE, builder.literal("E4"), builder.field("HIREDATE"));
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
    final RexNode condition =
            builder.call(SqlLibraryOperators.FORMAT_DATE, builder.literal("EEEE"), builder.field("HIREDATE"));
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
    final RexNode condition =
            builder.call(SqlLibraryOperators.FORMAT_DATE, builder.literal("E3"), builder.field("HIREDATE"));
    final RelNode root = relBuilder().scan("EMP").filter(condition).build();
    final String expectedSF = "SELECT *\n"
            + "FROM \"scott\".\"EMP\"\n"
            + "WHERE TO_VARCHAR(\"HIREDATE\", 'DY')";
    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedSF));
  }

  @Test public void testCaseExprForEEE() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RexNode condition =
            builder.call(SqlLibraryOperators.FORMAT_DATE, builder.literal("EEE"), builder.field("HIREDATE"));
    final RelNode root = relBuilder().scan("EMP").filter(condition).build();
    final String expectedSF = "SELECT *\n"
            + "FROM \"scott\".\"EMP\"\n"
            + "WHERE TO_VARCHAR(\"HIREDATE\", 'DY')";
    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedSF));
  }

  @Test public void octetLength() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RexNode condition =
            builder.call(SqlStdOperatorTable.OCTET_LENGTH, builder.field("ENAME"));
    final RelNode root = relBuilder().scan("EMP").filter(condition).build();

    final String expectedBQ = "SELECT *\n"
            + "FROM scott.EMP\n"
            + "WHERE OCTET_LENGTH(ENAME)";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQ));
  }

  @Test public void octetLengthWithLiteral() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RexNode condition =
            builder.call(SqlStdOperatorTable.OCTET_LENGTH, builder.literal("ENAME"));
    final RelNode root = relBuilder().scan("EMP").filter(condition).build();

    final String expectedBQ = "SELECT *\n"
            + "FROM scott.EMP\n"
            + "WHERE OCTET_LENGTH('ENAME')";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQ));
  }

  @Test public void testInt2Shr() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RexNode condition =
            builder.call(SqlLibraryOperators.INT2SHR, builder.literal(3), builder.literal(1), builder.literal(6));
    final RelNode root = relBuilder().scan("EMP").filter(condition).build();

    final String expectedBQ = "SELECT *\n"
            + "FROM scott.EMP\n"
            + "WHERE (3 & 6) >> 1";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQ));
  }

  @Test public void testInt8Xor() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RexNode condition =
            builder.call(SqlLibraryOperators.BITWISE_XOR, builder.literal(3), builder.literal(6));
    final RelNode root = relBuilder().scan("EMP").filter(condition).build();

    final String expectedBQ = "SELECT *\n"
            + "FROM scott.EMP\n"
            + "WHERE (3 ^ 6)";
    final String expectedSpark = "SELECT *\n"
            + "FROM scott.EMP\n"
            + "WHERE 3 ^ 6";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQ));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
  }

  @Test public void testInt2Shl() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RexNode condition =
            builder.call(SqlLibraryOperators.INT2SHL, builder.literal(3), builder.literal(1), builder.literal(6));
    final RelNode root = relBuilder().scan("EMP").filter(condition).build();

    final String expectedBQ = "SELECT *\n"
            + "FROM scott.EMP\n"
            + "WHERE (3 & 6) << 1";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQ));
  }

  @Test public void testInt2And() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RexNode condition =
            builder.call(SqlLibraryOperators.BITWISE_AND, builder.literal(3), builder.literal(6));
    final RelNode root = relBuilder().scan("EMP").filter(condition).build();

    final String expectedBQ = "SELECT *\n"
            + "FROM scott.EMP\n"
            + "WHERE (3 & 6)";
    final String expectedSpark = "SELECT *\n"
            + "FROM scott.EMP\n"
            + "WHERE 3 & 6";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQ));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
  }

  @Test public void testInt1Or() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RexNode condition =
            builder.call(SqlLibraryOperators.BITWISE_OR, builder.literal(3), builder.literal(6));
    final RelNode root = relBuilder().scan("EMP").filter(condition).build();

    final String expectedBQ = "SELECT *\n"
            + "FROM scott.EMP\n"
            + "WHERE (3 | 6)";
    final String expectedSpark = "SELECT *\n"
            + "FROM scott.EMP\n"
            + "WHERE 3 | 6";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQ));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
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

  @Test public void testExtractHour() {
    String query = "SELECT HOUR(TIMESTAMP '1999-06-23 10:30:47')";
    final String expectedBQ = "SELECT EXTRACT(HOUR FROM CAST('1999-06-23 10:30:47' AS DATETIME))";

    sql(query)
        .withBigQuery()
        .ok(expectedBQ);
  }

  @Test public void testExtractMinute() {
    String query = "SELECT MINUTE(TIMESTAMP '1999-06-23 10:30:47')";
    final String expectedBQ = "SELECT EXTRACT(MINUTE FROM CAST('1999-06-23 10:30:47' AS DATETIME))";

    sql(query)
        .withBigQuery()
        .ok(expectedBQ);
  }

  @Test public void testExtractSecond() {
    String query = "SELECT SECOND(TIMESTAMP '1999-06-23 10:30:47')";
    final String expectedBQ = "SELECT EXTRACT(SECOND FROM CAST('1999-06-23 10:30:47' AS DATETIME))";

    sql(query)
        .withBigQuery()
        .ok(expectedBQ);
  }

  @Test public void testExtractMicrosecond() {
    String query = "SELECT MICROSECOND(TIMESTAMP '1999-06-23 10:30:47.123')";
    final String expectedBQ = "SELECT EXTRACT(MICROSECOND FROM "
        + "CAST('1999-06-23 10:30:47.123' AS DATETIME))";

    sql(query)
        .withBigQuery()
        .ok(expectedBQ);
  }

  @Test public void testExtractEpoch() {
    String query = "SELECT EXTRACT(EPOCH FROM DATE '2008-08-29')";
    final String expectedBQ = "SELECT UNIX_SECONDS(CAST(DATE '2008-08-29' AS TIMESTAMP))";

    sql(query)
        .withBigQuery()
        .ok(expectedBQ);
  }

  @Test public void testExtractEpochWithDifferentOperands() {
    String query = "SELECT EXTRACT(EPOCH FROM \"birth_date\"), "
        + "EXTRACT(EPOCH FROM TIMESTAMP '2018-01-01 00:00:00'), "
        + "EXTRACT(EPOCH FROM TIMESTAMP'2018-01-01 12:12:12'), "
        + "EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)\n"
        + "FROM  \"employee\"";
    final String expectedBQ = "SELECT UNIX_SECONDS(CAST(birth_date AS TIMESTAMP)), "
        + "UNIX_SECONDS(CAST('2018-01-01 00:00:00' AS TIMESTAMP)), "
        + "UNIX_SECONDS(CAST('2018-01-01 12:12:12' AS TIMESTAMP)), "
        + "UNIX_SECONDS(CURRENT_TIMESTAMP())\n"
        + "FROM foodmart.employee";

    sql(query)
            .withBigQuery()
            .ok(expectedBQ);
  }

  @Test public void testExtractEpochWithMinusOperandBetweenCurrentTimestamp() {
    final RelBuilder builder = relBuilder();
    final RexNode extractEpochRexNode =
        builder.call(
            SqlStdOperatorTable.EXTRACT, builder.literal(TimeUnitRange.EPOCH), builder.call(SqlStdOperatorTable.MINUS,
            builder.call(SqlStdOperatorTable.CURRENT_TIMESTAMP),
            builder.call(SqlStdOperatorTable.CURRENT_TIMESTAMP)));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(extractEpochRexNode, "EE"))
        .build();
    final String expectedSql = "SELECT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - CURRENT_TIMESTAMP) "
        + "AS \"EE\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT UNIX_SECONDS(CURRENT_TIMESTAMP())  - UNIX_SECONDS"
        + "(CURRENT_TIMESTAMP()) AS EE\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testExtractEpochWithCurrentDate() {
    final RelBuilder builder = relBuilder();
    final RexNode extractEpochRexNode =
        builder.call(SqlStdOperatorTable.EXTRACT, builder.literal(TimeUnitRange.EPOCH),
                builder.call(CURRENT_DATE));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(extractEpochRexNode, "EE"))
        .build();
    final String expectedSql = "SELECT EXTRACT(EPOCH FROM CURRENT_DATE) AS \"EE\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT UNIX_SECONDS() AS EE\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testExtractEpochWithTimestamp() {
    final RelBuilder builder = relBuilder();
    final RexNode extractEpochRexNode =
        builder.call(SqlStdOperatorTable.EXTRACT, builder.literal(TimeUnitRange.EPOCH),
        builder.cast(builder.call(CURRENT_DATE), SqlTypeName.TIMESTAMP));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(extractEpochRexNode, "EE"))
        .build();
    final String expectedSql = "SELECT EXTRACT(EPOCH FROM CAST(CURRENT_DATE AS TIMESTAMP(0))) "
        + "AS \"EE\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT UNIX_SECONDS(CAST(CURRENT_DATE AS TIMESTAMP)) AS EE\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testExtractIsoweekWithCurrentDate() {
    final RelBuilder builder = relBuilder();
    final RexNode extractIsoweekRexNode =
        builder.call(SqlStdOperatorTable.EXTRACT, builder.literal(TimeUnitRange.ISOWEEK),
            builder.call(SqlStdOperatorTable.CURRENT_TIMESTAMP));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(extractIsoweekRexNode, "isoweek"))
        .build();

    final String expectedBiqQuery = "SELECT EXTRACT(ISOWEEK FROM CURRENT_DATETIME()) AS isoweek\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
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
    final RexNode formatTimestampRexNode =
        builder.call(SqlLibraryOperators.FORMAT_TIMESTAMP, builder.literal("SEC_FROM_MIDNIGHT"),
                builder.scan("EMP").field(4));
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
    final RexNode formatDateRexNode =
        builder.call(SqlLibraryOperators.FORMAT_DATE, builder.literal("QUARTER"), builder.scan("EMP").field(4));
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
    final String expectedBQSql = "SELECT hire_date + 10 * INTERVAL 60 SECOND\n"
        + "FROM foodmart.employee";

    sql(query)
        .withBigQuery()
        .ok(expectedBQSql);
  }

  @Test public void testDateUnderscoreSeparator() {
    final RelBuilder builder = relBuilder();
    final RexNode formatTimestampRexNode =
        builder.call(SqlLibraryOperators.FORMAT_TIMESTAMP, builder.literal("YYYYMMDD_HH24MISS"),
                builder.scan("EMP").field(4));
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
    final RexNode parseDatetimeRexNode =
        builder.call(SqlLibraryOperators.PARSE_TIMESTAMP, builder.literal("YYYYMMDD_HH24MISS"),
                builder.scan("EMP").field(4));
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
    final RexNode unixSecondsRexNode =
        builder.call(SqlLibraryOperators.UNIX_SECONDS, builder.scan("EMP").field(4));
    final RexNode unixMicrosRexNode =
        builder.call(SqlLibraryOperators.UNIX_MICROS, builder.scan("EMP").field(4));
    final RexNode unixMillisRexNode =
        builder.call(SqlLibraryOperators.UNIX_MILLIS, builder.scan("EMP").field(4));
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
    final RexNode unixSecondsRexNode =
        builder.call(SqlLibraryOperators.TIMESTAMP_SECONDS, builder.scan("EMP").field(4));
    final RexNode unixMicrosRexNode =
        builder.call(SqlLibraryOperators.TIMESTAMP_MICROS, builder.scan("EMP").field(4));
    final RexNode unixMillisRexNode =
        builder.call(SqlLibraryOperators.TIMESTAMP_MILLIS, builder.scan("EMP").field(4));
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

  @Test public void testTimestampFunctionsWithTwoOperands() {
    final RelBuilder builder = relBuilder();
    final RexNode unixSecondsRexNode =
        builder.call(SqlLibraryOperators.TIMESTAMP_SECONDS, builder.scan("EMP").field(4), builder.literal(true));
    final RexNode unixMicrosRexNode =
        builder.call(SqlLibraryOperators.TIMESTAMP_MICROS, builder.scan("EMP").field(4), builder.literal(true));
    final RexNode unixMillisRexNode =
        builder.call(SqlLibraryOperators.TIMESTAMP_MILLIS, builder.scan("EMP").field(4), builder.literal(true));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(unixSecondsRexNode, "TS"),
            builder.alias(unixMicrosRexNode, "TM"),
            builder.alias(unixMillisRexNode, "TMI"))
        .build();
    final String expectedBiqQuery = "SELECT TIMESTAMP_SECONDS(HIREDATE) AS TS, "
        + "TIMESTAMP_MICROS(HIREDATE) AS TM, "
        + "TIMESTAMP_MILLIS(HIREDATE) AS TMI\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testFormatTimestamp() {
    final RelBuilder builder = relBuilder();
    final RexNode formatTimestampRexNode =
        builder.call(SqlLibraryOperators.FORMAT_TIMESTAMP, builder.literal("EEEE"),
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
    final String expectedBQSql = "SELECT first_name, last_name, GROUPING(first_name) + GROUPING(last_name)\n"
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
    final RexNode formatDateRexNode =
        builder.call(SqlLibraryOperators.HASHBUCKET,
                builder.call(SqlLibraryOperators.HASHROW, builder.scan("EMP").field(0)));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(formatDateRexNode, "FD"))
        .build();
    final String expectedSql = "SELECT HASHBUCKET(HASHROW(\"EMPNO\")) AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT FARM_FINGERPRINT(CAST(EMPNO AS STRING)) AS FD\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testdatetrunc() {
    final RelBuilder builder = relBuilder();
    final RexNode trunc =
        builder.call(SqlLibraryOperators.TRUNC, builder.literal("2008-09-12"), builder.literal("DAY"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(trunc, "FD"))
        .build();
    final String expectedSql = "SELECT TRUNC('2008-09-12', 'DAY') AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT DATE_TRUNC('2008-09-12', DAY) AS FD\n"
        + "FROM scott.EMP";
    final String expectedSparkQuery = "SELECT CAST(DATE_TRUNC('DAY', '2008-09-12') AS DATE) FD\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }
  @Test public void testdatetruncWithYear() {
    final RelBuilder builder = relBuilder();
    final RexNode trunc =
        builder.call(SqlLibraryOperators.TRUNC, builder.literal("2008-09-12"), builder.literal("YEAR"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(trunc, "FD"))
        .build();
    final String expectedSql = "SELECT TRUNC('2008-09-12', 'YEAR') AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT DATE_TRUNC('2008-09-12', YEAR) AS FD\n"
        + "FROM scott.EMP";
    final String expectedSparkQuery = "SELECT TRUNC('2008-09-12', 'YEAR') FD\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testdatetruncWithQuarter() {
    final RelBuilder builder = relBuilder();
    final RexNode trunc =
        builder.call(SqlLibraryOperators.TRUNC, builder.literal("2008-09-12"), builder.literal("QUARTER"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(trunc, "FD"))
        .build();
    final String expectedSql = "SELECT TRUNC('2008-09-12', 'QUARTER') AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT DATE_TRUNC('2008-09-12', QUARTER) AS FD\n"
        + "FROM scott.EMP";
    final String expectedSparkQuery = "SELECT TRUNC('2008-09-12', 'QUARTER') FD\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testdatetruncWithMonth() {
    final RelBuilder builder = relBuilder();
    final RexNode trunc =
        builder.call(SqlLibraryOperators.TRUNC, builder.literal("2008-09-12"), builder.literal("MONTH"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(trunc, "FD"))
        .build();
    final String expectedSql = "SELECT TRUNC('2008-09-12', 'MONTH') AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT DATE_TRUNC('2008-09-12', MONTH) AS FD\n"
        + "FROM scott.EMP";
    final String expectedSparkQuery = "SELECT TRUNC('2008-09-12', 'MONTH') FD\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testdatetruncWithWeek() {
    final RelBuilder builder = relBuilder();
    final RexNode trunc =
        builder.call(SqlLibraryOperators.TRUNC, builder.literal("2008-09-12"), builder.literal("WEEK"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(trunc, "FD"))
        .build();
    final String expectedSql = "SELECT TRUNC('2008-09-12', 'WEEK') AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT DATE_TRUNC('2008-09-12', WEEK) AS FD\n"
        + "FROM scott.EMP";
    final String expectedSparkQuery = "SELECT TRUNC('2008-09-12', 'WEEK') FD\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testDateTimeTruncWithYear() {
    final RelBuilder builder = relBuilder();
    final RexNode trunc =
        builder.call(
                SqlLibraryOperators.TRUNC, builder.cast(builder.literal("2017-02-14 20:38:40"),
                        SqlTypeName.TIMESTAMP),
        builder.literal("YEAR"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(trunc, "FD"))
        .build();
    final String expectedSql = "SELECT TRUNC(TIMESTAMP '2017-02-14 20:38:40', 'YEAR') AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT DATETIME_TRUNC(CAST('2017-02-14 20:38:40' AS DATETIME),"
        + " YEAR) AS FD\nFROM scott.EMP";
    final String expectedSparkQuery = "SELECT TRUNC(TIMESTAMP '2017-02-14 20:38:40', 'YEAR') FD\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testDateTimeTruncWithMonth() {
    final RelBuilder builder = relBuilder();
    final RexNode trunc =
        builder.call(
                SqlLibraryOperators.TRUNC, builder.cast(builder.literal("2017-02-14 20:38:40"),
                        SqlTypeName.TIMESTAMP),
        builder.literal("MONTH"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(trunc, "FD"))
        .build();
    final String expectedSql = "SELECT TRUNC(TIMESTAMP '2017-02-14 20:38:40', 'MONTH') AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT DATETIME_TRUNC(CAST('2017-02-14 20:38:40' AS DATETIME),"
        + " MONTH) AS FD\nFROM scott.EMP";
    final String expectedSparkQuery = "SELECT TRUNC(TIMESTAMP '2017-02-14 20:38:40', 'MONTH') "
        + "FD\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testDateTimeTruncWithQuarter() {
    final RelBuilder builder = relBuilder();
    final RexNode trunc =
        builder.call(
                SqlLibraryOperators.TRUNC, builder.cast(builder.literal("2017-02-14 20:38:40"),
                        SqlTypeName.TIMESTAMP),
        builder.literal("QUARTER"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(trunc, "FD"))
        .build();
    final String expectedSql = "SELECT TRUNC(TIMESTAMP '2017-02-14 20:38:40', 'QUARTER') AS \"FD\""
        + "\nFROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT DATETIME_TRUNC(CAST('2017-02-14 20:38:40' AS DATETIME),"
        + " QUARTER) AS FD\nFROM scott.EMP";
    final String expectedSparkQuery = "SELECT TRUNC(TIMESTAMP '2017-02-14 20:38:40', 'QUARTER') "
        + "FD\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testDateTimeTruncWithWeek() {
    final RelBuilder builder = relBuilder();
    final RexNode trunc =
        builder.call(
            SqlLibraryOperators.TRUNC, builder.cast(builder.literal("2017-02-14 20:38:40"),
                        SqlTypeName.TIMESTAMP),
        builder.literal("WEEK"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(trunc, "FD"))
        .build();
    final String expectedSql = "SELECT TRUNC(TIMESTAMP '2017-02-14 20:38:40', 'WEEK') AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT DATETIME_TRUNC(CAST('2017-02-14 20:38:40' AS DATETIME),"
        + " WEEK) AS FD\nFROM scott.EMP";
    final String expectedSparkQuery = "SELECT TRUNC(TIMESTAMP '2017-02-14 20:38:40', 'WEEK') FD\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testDateTimeTruncWithDay() {
    final RelBuilder builder = relBuilder();
    final RexNode trunc =
        builder.call(
                SqlLibraryOperators.TRUNC, builder.cast(builder.literal("2017-02-14 20:38:40"),
                        SqlTypeName.TIMESTAMP),
        builder.literal("DAY"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(trunc, "FD"))
        .build();
    final String expectedSql = "SELECT TRUNC(TIMESTAMP '2017-02-14 20:38:40', 'DAY') AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT DATETIME_TRUNC(CAST('2017-02-14 20:38:40' AS DATETIME),"
        + " DAY) AS FD\nFROM scott.EMP";
    final String expectedSparkQuery = "SELECT CAST(DATE_TRUNC('DAY', TIMESTAMP '2017-02-14 "
        + "20:38:40') AS DATE) FD\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testDateTimeTruncWithHour() {
    final RelBuilder builder = relBuilder();
    final RexNode trunc =
        builder.call(
            SqlLibraryOperators.TRUNC, builder.cast(builder.literal("2017-02-14 20:38:40"),
                        SqlTypeName.TIMESTAMP),
        builder.literal("HOUR"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(trunc, "FD"))
        .build();
    final String expectedSql = "SELECT TRUNC(TIMESTAMP '2017-02-14 20:38:40', 'HOUR') AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT DATETIME_TRUNC(CAST('2017-02-14 20:38:40' AS DATETIME),"
        + " HOUR) AS FD\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testDateTimeTruncWithMinute() {
    final RelBuilder builder = relBuilder();
    final RexNode trunc =
        builder.call(
            SqlLibraryOperators.TRUNC, builder.cast(builder.literal("2017-02-14 20:38:40"),
                        SqlTypeName.TIMESTAMP),
        builder.literal("MINUTE"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(trunc, "FD"))
        .build();
    final String expectedSql = "SELECT TRUNC(TIMESTAMP '2017-02-14 20:38:40', 'MINUTE') AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT DATETIME_TRUNC(CAST('2017-02-14 20:38:40' AS DATETIME),"
        + " MINUTE) AS FD\nFROM scott.EMP";
    final String expectedSparkQuery = "SELECT DATE_TRUNC('MINUTE', TIMESTAMP '2017-02-14 "
        + "20:38:40') FD\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testDateTimeTruncWithSecond() {
    final RelBuilder builder = relBuilder();
    final RexNode trunc =
        builder.call(
            SqlLibraryOperators.TRUNC, builder.cast(builder.literal("2017-02-14 20:38:40"),
                        SqlTypeName.TIMESTAMP),
        builder.literal("SECOND"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(trunc, "FD"))
        .build();
    final String expectedSql = "SELECT TRUNC(TIMESTAMP '2017-02-14 20:38:40', 'SECOND') AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT DATETIME_TRUNC(CAST('2017-02-14 20:38:40' AS DATETIME),"
        + " SECOND) AS FD\nFROM scott.EMP";
    final String expectedSparkQuery = "SELECT DATE_TRUNC('SECOND', TIMESTAMP '2017-02-14 "
        + "20:38:40') FD\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testDateTimeTruncWithMilliSecond() {
    final RelBuilder builder = relBuilder();
    final RexNode trunc =
        builder.call(
            SqlLibraryOperators.TRUNC, builder.cast(builder.literal("2017-02-14 20:38:40"),
                        SqlTypeName.TIMESTAMP),
        builder.literal("MILLISECOND"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(trunc, "FD"))
        .build();
    final String expectedSql = "SELECT TRUNC(TIMESTAMP '2017-02-14 20:38:40', 'MILLISECOND')"
        + " AS \"FD\"\nFROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT DATETIME_TRUNC(CAST('2017-02-14 20:38:40' AS DATETIME),"
        + " MILLISECOND) AS FD\nFROM scott.EMP";
    final String expectedSparkQuery = "SELECT DATE_TRUNC('MILLISECOND', TIMESTAMP '2017-02-14 "
        + "20:38:40') FD\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testDateTimeTruncWithMicroSecond() {
    final RelBuilder builder = relBuilder();
    final RexNode trunc =
        builder.call(
            SqlLibraryOperators.TRUNC, builder.cast(builder.literal("2017-02-14 20:38:40"),
                        SqlTypeName.TIMESTAMP),
        builder.literal("MICROSECOND"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(trunc, "FD"))
        .build();
    final String expectedSql = "SELECT TRUNC(TIMESTAMP '2017-02-14 20:38:40', 'MICROSECOND')"
        + " AS \"FD\"\nFROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT DATETIME_TRUNC(CAST('2017-02-14 20:38:40' AS DATETIME),"
        + " MICROSECOND) AS FD\nFROM scott.EMP";
    final String expectedSparkQuery = "SELECT DATE_TRUNC('MICROSECOND', TIMESTAMP '2017-02-14 "
        + "20:38:40') FD\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testTimeTruncWithHour() {
    final RelBuilder builder = relBuilder();
    final RexNode trunc =
        builder.call(SqlLibraryOperators.TRUNC, builder.literal("20:48:18"), builder.literal("HOUR"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(trunc, "FD"))
        .build();
    final String expectedSql = "SELECT TRUNC('20:48:18', 'HOUR') AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT TIME_TRUNC('20:48:18', HOUR) AS FD\n"
        + "FROM scott.EMP";
    final String expectedSparkQuery = "SELECT DATE_TRUNC('HOUR', '20:48:18') FD\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test void testUnparseSqlIntervalQualifierForDayAndHour() {
    String queryDatePlus = "select INTERVAL -'200' DAY(6) , INTERVAL '10' HOUR(2)";
    String expectedPostgres = "SELECT INTERVAL '-200' DAY, INTERVAL '10' HOUR";

    sql(queryDatePlus)
        .withPostgresql()
        .ok(expectedPostgres);
  }

  @Test void testUnparseSqlIntervalQualifierForSecAndMin() {
    String queryDatePlus = "select INTERVAL '19800' SECOND(5) , INTERVAL '100' MINUTE(3)";
    String expectedPostgres = "SELECT INTERVAL '19800' SECOND(5), INTERVAL '100' MINUTE";

    sql(queryDatePlus)
        .withPostgresql()
        .ok(expectedPostgres);
  }

  @Test public void testCastToClobForPostgres() {
    RelBuilder relBuilder = relBuilder().scan("EMP");
    final RexNode clobNode =
        relBuilder.cast(relBuilder.literal("a123"), SqlTypeName.CLOB);
    RelNode root = relBuilder
        .project(clobNode)
        .build();
    final String expectedDB2Sql = "SELECT CAST('a123' AS TEXT) AS \"$f0\"\n"
        + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.POSTGRESQL.getDialect()), isLinux(expectedDB2Sql));
  }

  @Test public void testOraToPostgresBitAnd() {
    RelBuilder relBuilder = relBuilder().scan("EMP");
    final RexNode literalTimestamp =
        relBuilder.call(SqlLibraryOperators.BITWISE_AND, relBuilder.literal(3), relBuilder.literal(2));
    RelNode root = relBuilder
        .project(literalTimestamp)
        .build();
    final String expectedOracleSql = "SELECT BITWISE_AND(3, 2) \"$f0\"\nFROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.ORACLE.getDialect()), isLinux(expectedOracleSql));
  }

  @Test public void testFromClauseInPg() {
    final RelBuilder builder = relBuilder();
    final RexNode currenDateNode = builder.call(CURRENT_DATE);
    final RelNode root = builder
        .push(LogicalValues.createOneRow(builder.getCluster()))
        .project(currenDateNode, builder.alias(builder.literal(1), "one"))
        .build();
    final String expectedPgQuery =
        "SELECT CURRENT_DATE AS \"$f0\", 1 AS \"one\"";
    assertThat(toSql(root, DatabaseProduct.POSTGRESQL.getDialect()), isLinux(expectedPgQuery));
  }

  @Test public void testForEmptyClobFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode toClobRex = builder.call(SqlLibraryOperators.EMPTY_CLOB);
    final RelNode root = builder
        .scan("EMP")
        .project(toClobRex)
        .build();
    final String expectedOracleQuery = "SELECT EMPTY_CLOB() \"$f0\"\nFROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.ORACLE.getDialect()), isLinux(expectedOracleQuery));
  }

  @Test public void testTimeTruncWithMinute() {
    final RelBuilder builder = relBuilder();
    final RexNode trunc =
        builder.call(SqlLibraryOperators.TRUNC, builder.literal("20:48:18"), builder.literal("MINUTE"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(trunc, "FD"))
        .build();
    final String expectedSql = "SELECT TRUNC('20:48:18', 'MINUTE') AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT TIME_TRUNC('20:48:18', MINUTE) AS FD\n"
        + "FROM scott.EMP";
    final String expectedSparkQuery = "SELECT DATE_TRUNC('MINUTE', '20:48:18') FD\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testTimeTruncWithSecond() {
    final RelBuilder builder = relBuilder();
    final RexNode trunc =
        builder.call(SqlLibraryOperators.TRUNC, builder.literal("20:48:18"), builder.literal("SECOND"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(trunc, "FD"))
        .build();
    final String expectedSql = "SELECT TRUNC('20:48:18', 'SECOND') AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT TIME_TRUNC('20:48:18', SECOND) AS FD\n"
        + "FROM scott.EMP";
    final String expectedSparkQuery = "SELECT DATE_TRUNC('SECOND', '20:48:18') FD\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testTimeTruncWithMiliSecond() {
    final RelBuilder builder = relBuilder();
    final RexNode trunc =
        builder.call(SqlLibraryOperators.TRUNC, builder.literal("20:48:18"), builder.literal("MILLISECOND"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(trunc, "FD"))
        .build();
    final String expectedSql = "SELECT TRUNC('20:48:18', 'MILLISECOND') AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT TIME_TRUNC('20:48:18', MILLISECOND) AS FD\n"
        + "FROM scott.EMP";
    final String expectedSparkQuery = "SELECT DATE_TRUNC('MILLISECOND', '20:48:18') FD\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testTimeTruncWithMicroSecond() {
    final RelBuilder builder = relBuilder();
    final RexNode trunc =
        builder.call(SqlLibraryOperators.TRUNC, builder.literal("20:48:18"), builder.literal("MICROSECOND"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(trunc, "FD"))
        .build();
    final String expectedSql = "SELECT TRUNC('20:48:18', 'MICROSECOND') AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT TIME_TRUNC('20:48:18', MICROSECOND) AS FD\n"
        + "FROM scott.EMP";
    final String expectedSparkQuery = "SELECT DATE_TRUNC('MICROSECOND', '20:48:18') FD\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testhashrow() {
    final RelBuilder builder = relBuilder();
    final RexNode hashrow =
        builder.call(SqlLibraryOperators.HASHROW, builder.scan("EMP").field(1));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(hashrow, "FD"))
        .build();
    final String expectedSql = "SELECT HASHROW(\"ENAME\") AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT FARM_FINGERPRINT(CAST(ENAME AS STRING)) AS FD\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testhashrowWithoutOperand() {
    final RelBuilder builder = relBuilder();
    final RexNode hashrow =
        builder.call(SqlLibraryOperators.HASHROW);
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(hashrow, "FD"))
        .build();
    final String expectedTDSql = "SELECT HASHROW() AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedTDSql));
  }

  @Test public void testSnowflakeHashFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode hashNode =
        builder.call(SqlLibraryOperators.HASH, builder.scan("EMP").field(1));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(hashNode, "FD"))
        .build();
    final String expectedSFSql = "SELECT HASH(\"ENAME\") AS \"FD\"\n"
        + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedSFSql));
  }

  @Test public void testSnowflakeSha2Function() {
    final RelBuilder builder = relBuilder();
    final RexNode sha2Node =
        builder.call(SqlLibraryOperators.SHA2, builder.scan("EMP").field(1), builder.literal(256));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(sha2Node, "hashing"))
        .build();
    final String expectedSFSql = "SELECT SHA2(\"ENAME\", 256) AS \"hashing\"\n"
        + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedSFSql));
  }

  @Test public void testMsSqlHashBytesFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode hashByteNode =
        builder.call(SqlLibraryOperators.HASHBYTES,
            builder.literal("SHA2_256"),
            builder.scan("EMP").field(1));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(hashByteNode, "HashedValue"))
        .build();
    final String expectedSFSql = "SELECT HASHBYTES('SHA2_256', [ENAME]) AS [HashedValue]"
        + "\nFROM [scott].[EMP]";

    assertThat(toSql(root, DatabaseProduct.MSSQL.getDialect()), isLinux(expectedSFSql));
  }

  @Test public void testIsNumericFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode isNumericNode =
        builder.call(SqlLibraryOperators.ISNUMERIC,
            builder.literal(123.45));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(isNumericNode, "isNumericValue"))
        .build();
    final String expectedSql = "SELECT ISNUMERIC(123.45) AS [isNumericValue]\n"
        + "FROM [scott].[EMP]";

    assertThat(toSql(root, DatabaseProduct.MSSQL.getDialect()), isLinux(expectedSql));
  }

  @Test public void testCurrentDatabaseFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode currentDatabase =
        builder.call(SqlLibraryOperators.CURRENT_DATABASE);
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(currentDatabase, "currentDatabase"))
        .build();
    final String expectedSql = "SELECT CURRENT_DATABASE() AS \"currentDatabase\""
        + "\nFROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.POSTGRESQL.getDialect()), isLinux(expectedSql));
  }

  @Test public void testProjectIdFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode projectId =
        builder.call(SqlLibraryOperators.PROJECT_ID);
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(projectId, "projectId"))
        .build();
    final String expectedSql = "SELECT @@PROJECT_ID AS projectId"
        + "\nFROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedSql));
  }

  @Test public void testBigQuerySha256Function() {
    final RelBuilder builder = relBuilder();
    final RexNode sha256Node =
        builder.call(SqlLibraryOperators.SHA256, builder.scan("EMP").field(1));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(sha256Node, "hashing"))
        .build();
    final String expectedBQSql = "SELECT SHA256(ENAME) AS hashing\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQSql));
  }


  RelNode createLogicalValueRel(RexNode col1, RexNode col2) {
    final RelBuilder builder = relBuilder();
    RelDataTypeField field =
        new RelDataTypeFieldImpl("ZERO", 0, builder.getTypeFactory().createSqlType(SqlTypeName.INTEGER));
    List<RelDataTypeField> fieldList = new ArrayList<>();
    fieldList.add(field);
    RelRecordType type = new RelRecordType(fieldList);
    builder.values(
        ImmutableList.of(
            ImmutableList.of(
                builder.getRexBuilder().makeZeroLiteral(
                    builder.getTypeFactory().createSqlType(SqlTypeName.INTEGER)))), type);
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

    final RexNode createRexNode =
        builder.call(SqlLibraryOperators.TIME_ADD, builder.literal("00:00:00"),
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

    final RexNode createRexNode =
        builder.call(PLUS, builder.cast(builder.literal("12:15:07"), SqlTypeName.TIME),
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

    final RexNode createRexNode =
        builder.call(SqlStdOperatorTable.MINUS, builder.cast(builder.literal("12:15:07"), SqlTypeName.TIME),
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

    final RexNode createRexNode =
        builder.call(
            PLUS, builder.cast(builder.literal("1999-07-01 15:00:00-08:00"),
                        SqlTypeName.TIMESTAMP),
        builder.getRexBuilder().makeIntervalLiteral(new BigDecimal(1000),
            new SqlIntervalQualifier(MICROSECOND, null, SqlParserPos.ZERO)));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(createRexNode, "FD"))
        .build();
    final String expectedBiqQuery =
        "SELECT DATETIME_ADD(CAST('1999-07-01 15:00:00-08:00' AS DATETIME), INTERVAL 1 MICROSECOND) AS FD\n"
            + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testPlusForTimestampSub() {
    final RelBuilder builder = relBuilder();

    final RexNode createRexNode =
        builder.call(
            SqlStdOperatorTable.MINUS, builder.cast(builder.literal("1999-07-01 15:00:00-08:00"),
                        SqlTypeName.TIMESTAMP),
        builder.getRexBuilder().makeIntervalLiteral(new BigDecimal(1000),
            new SqlIntervalQualifier(MICROSECOND, null, SqlParserPos.ZERO)));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(createRexNode, "FD"))
        .build();
    final String expectedBiqQuery =
        "SELECT DATETIME_SUB(CAST('1999-07-01 15:00:00-08:00' AS DATETIME), "
            + "INTERVAL 1 MICROSECOND) AS FD\n"
            + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testPlusForDateAdd() {
    final RelBuilder builder = relBuilder();

    final RexNode createRexNode =
        builder.call(PLUS, builder.cast(builder.literal("1999-07-01"), SqlTypeName.DATE),
        builder.getRexBuilder().makeIntervalLiteral(new BigDecimal(86400000),
            new SqlIntervalQualifier(DAY, 6, DAY,
                -1, SqlParserPos.ZERO)));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(createRexNode, "FD"))
        .build();
    final String expectedBiqQuery = "SELECT DATE_ADD(DATE '1999-07-01', INTERVAL 1 DAY) AS FD\n"
        + "FROM scott.EMP";
    final String expectedSparkQuery = "SELECT DATE '1999-07-01' + 1 FD\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testPlusForDateAddForWeek() {
    final RelBuilder builder = relBuilder();

    final RexNode createRexNode =
        builder.call(PLUS, builder.cast(builder.literal("1999-07-01"), SqlTypeName.DATE),
        builder.getRexBuilder().makeIntervalLiteral(new BigDecimal(604800000),
            new SqlIntervalQualifier(WEEK, 7, WEEK,
                -1, SqlParserPos.ZERO)));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(createRexNode, "FD"))
        .build();
    final String expectedBiqQuery = "SELECT DATE_ADD(DATE '1999-07-01', INTERVAL 1 WEEK) AS FD\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testPlusForDateSub() {
    final RelBuilder builder = relBuilder();

    final RexNode createRexNode =
        builder.call(SqlStdOperatorTable.MINUS, builder.cast(builder.literal("1999-07-01"), SqlTypeName.DATE),
        builder.getRexBuilder().makeIntervalLiteral(new BigDecimal(86400000),
            new SqlIntervalQualifier(DAY, 6, DAY,
                -1, SqlParserPos.ZERO)));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(createRexNode, "FD"))
        .build();
    final String expectedBiqQuery = "SELECT DATE_SUB(DATE '1999-07-01', INTERVAL 1 DAY) AS FD\n"
        + "FROM scott.EMP";
    final String expectedSparkQuery = "SELECT DATE '1999-07-01' - 1 FD\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
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

  @Test public void testTimeOfDayFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode formatTimestampRexNode2 =
          builder.call(SqlLibraryOperators.FORMAT_TIMESTAMP, builder.literal("TIMEOFDAY"),
                  builder.call(CURRENT_TIMESTAMP));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(formatTimestampRexNode2, "FD2"))
        .build();
    final String expectedSql = "SELECT FORMAT_TIMESTAMP('TIMEOFDAY', CURRENT_TIMESTAMP) AS "
        + "\"FD2\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT FORMAT_TIMESTAMP('%c', CURRENT_DATETIME()) AS FD2\n"
        + "FROM scott.EMP";
    final String expSprk = "SELECT DATE_FORMAT(CURRENT_TIMESTAMP, 'EE MMM dd HH:mm:ss yyyy zz') "
        + "FD2\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expSprk));
  }

  @Test void testConversionOfFilterWithCrossJoinToFilterWithInnerJoin() {
    String query =
        "select *\n"
            + " from \"foodmart\".\"employee\" as \"e\", \"foodmart\".\"department\" as \"d\"\n"
            + " where \"e\".\"department_id\" = \"d\".\"department_id\" "
            + "and \"e\".\"employee_id\" > 2";

    String expect = "SELECT *\n"
        + "FROM foodmart.employee\n"
        + "INNER JOIN foodmart.department ON employee.department_id = department.department_id\n"
        + "WHERE employee.employee_id > 2";

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(FilterExtractInnerJoinRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    RuleSet rules = RuleSets.ofList(CoreRules.FILTER_EXTRACT_INNER_JOIN_RULE);
    sql(query).withBigQuery().optimize(rules, hepPlanner).ok(expect);
  }

  @Test void testConversionOfFilterWithCrossJoinAndFilterHasBetweenOperator() {
    final RelBuilder builder = foodmartRelBuilder();
    RelNode leftTable = builder.scan("employee").build();
    RelNode rightTable = builder.scan("reserve_employee").build();
    RelNode join = builder.push(leftTable).push(rightTable).join(JoinRelType.INNER).build();
    RelNode filteredRel = builder.push(join)
        .filter(
            builder.call(SqlLibraryOperators.BETWEEN, builder.field(11),
            builder.call(SqlStdOperatorTable.DIVIDE, builder.field(11),
                builder.literal(100)), builder.field(28)))
        .build();

    // Projection
    RelNode relNode = builder.push(filteredRel)
        .project(builder.field(1))
        .build();

    // Applying rel optimization
    Collection<RelOptRule> rules = new ArrayList<>();
    rules.add((FilterExtractInnerJoinRule.Config.DEFAULT).toRule());
    HepProgram hepProgram = new HepProgramBuilder().addRuleCollection(rules).build();
    HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(relNode);
    RelNode optimizedRel = hepPlanner.findBestExp();

    final String expectedSql = "SELECT employee.full_name\n"
        + "FROM foodmart.employee\n"
        + "INNER JOIN foodmart.reserve_employee ON TRUE\n"
        + "WHERE employee.salary BETWEEN employee.salary / 100 AND reserve_employee.salary";

    assertThat(toSql(optimizedRel, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedSql));
  }

  @Test void testConversionOfFilterWithCrossJoinToFilterWithInnerJoinWithOneConditionInFilter() {
    String query =
        "select *\n"
            + " from \"foodmart\".\"employee\" as \"e\", \"foodmart\".\"department\" as \"d\"\n"
            + " where \"e\".\"department_id\" = \"d\".\"department_id\"";

    String expect = "SELECT *\n"
        + "FROM foodmart.employee\n"
        + "INNER JOIN foodmart.department ON employee.department_id = department.department_id";

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(FilterExtractInnerJoinRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    RuleSet rules = RuleSets.ofList(CoreRules.FILTER_EXTRACT_INNER_JOIN_RULE);
    sql(query).withBigQuery().optimize(rules, hepPlanner).ok(expect);
  }

  @Test void testConversionOfFilterWithThreeCrossJoinToFilterWithInnerJoin() {
    String query = "select *\n"
        + " from \"foodmart\".\"employee\" as \"e\", \"foodmart\".\"department\" as \"d\", \n"
        + " \"foodmart\".\"reserve_employee\" as \"re\"\n"
        + " where \"e\".\"department_id\" = \"d\".\"department_id\" and \"e\".\"employee_id\" > 2\n"
        + " and \"re\".\"employee_id\" > \"e\".\"employee_id\"\n"
        + " and \"e\".\"department_id\" > 5";

    String expect = "SELECT *\n"
        + "FROM foodmart.employee\n"
        + "INNER JOIN foodmart.department ON employee.department_id = department.department_id\n"
        + "INNER JOIN foodmart.reserve_employee "
        + "ON employee.employee_id < reserve_employee.employee_id\n"
        + "WHERE employee.employee_id > 2 AND employee.department_id > 5";

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(FilterExtractInnerJoinRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    RuleSet rules = RuleSets.ofList(CoreRules.FILTER_EXTRACT_INNER_JOIN_RULE);
    sql(query).withBigQuery().optimize(rules, hepPlanner).ok(expect);
  }

  @Test void testConversionOfFilterWithCompositeConditionWithThreeCrossJoinToFilterWithInnerJoin() {
    String query = "select *\n"
        + " from \"foodmart\".\"employee\" as \"e\", \"foodmart\".\"department\" as \"d\", \n"
        + " \"foodmart\".\"reserve_employee\" as \"re\"\n"
        + " where (\"e\".\"department_id\" = \"d\".\"department_id\"\n"
        + " or \"re\".\"employee_id\" = \"e\".\"employee_id\")\n"
        + " and \"re\".\"employee_id\" = \"d\".\"department_id\"\n";

    String expect = "SELECT *\n"
        + "FROM foodmart.employee\n"
        + "INNER JOIN foodmart.department ON TRUE\n"
        +  "INNER JOIN foodmart.reserve_employee ON (employee.department_id = department"
        +  ".department_id OR employee.employee_id = reserve_employee.employee_id) AND department"
        +  ".department_id = reserve_employee.employee_id";

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(FilterExtractInnerJoinRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    RuleSet rules = RuleSets.ofList(CoreRules.FILTER_EXTRACT_INNER_JOIN_RULE);
    sql(query).withBigQuery().optimize(rules, hepPlanner).ok(expect);
  }

  @Test void testConversionOfFilterWithOrConditionToFilterWithInnerJoin() {
    String query = "select *\n"
        + " from \"foodmart\".\"employee\" as \"e\", \"foodmart\".\"reserve_employee\" as \"re\"\n"
        + " where (\"re\".\"department_id\" = \"e\".\"employee_id\")\n"
        + " or \"re\".\"employee_id\" = \"e\".\"department_id\"\n";

    String expect = "SELECT *\n"
        + "FROM foodmart.employee\nINNER JOIN foodmart.reserve_employee"
        + " ON employee.employee_id = reserve_employee.department_id"
        + " OR employee.department_id = reserve_employee.employee_id";

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(FilterExtractInnerJoinRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    RuleSet rules = RuleSets.ofList(CoreRules.FILTER_EXTRACT_INNER_JOIN_RULE);
    sql(query).withBigQuery().optimize(rules, hepPlanner).ok(expect);
  }

  //WHERE t1.c1 = t2.c1 AND t2.c2 = t3.c2 AND (t1.c3 = t3.c3 OR t1.c4 = t2.c4)
  @Test void testFilterWithParenthesizedConditionsWithThreeCrossJoinToFilterWithInnerJoin() {
    String query = "select *\n"
        + " from \"foodmart\".\"employee\" as \"e\", \"foodmart\".\"department\" as \"d\", \n"
        + " \"foodmart\".\"reserve_employee\" as \"re\"\n"
        + " where \"e\".\"department_id\" = \"d\".\"department_id\"\n"
        + " and \"re\".\"employee_id\" = \"d\".\"department_id\"\n"
        + " and (\"re\".\"department_id\" < \"d\".\"department_id\"\n"
        + " or \"d\".\"department_id\" = \"re\".\"department_id\")\n";

    String expect = "SELECT *\n"
        + "FROM foodmart.employee\n"
        + "INNER JOIN foodmart.department ON employee.department_id = department.department_id\n"
        + "INNER JOIN foodmart.reserve_employee ON department.department_id = reserve_employee"
        +  ".employee_id AND (department.department_id > reserve_employee.department_id OR "
        +   "department.department_id = reserve_employee.department_id)";

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(FilterExtractInnerJoinRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    RuleSet rules = RuleSets.ofList(CoreRules.FILTER_EXTRACT_INNER_JOIN_RULE);
    sql(query).withBigQuery().optimize(rules, hepPlanner).ok(expect);
  }

  @Test void translateCastOfTimestampWithLocalTimeToTimestampInBq() {
    final RelBuilder relBuilder = relBuilder();

    final RexNode castTimestampTimeZoneCall =
        relBuilder.cast(relBuilder.call(SqlStdOperatorTable.CURRENT_TIMESTAMP),
            SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);

    final RelNode root = relBuilder
        .values(new String[] {"c"}, 1)
        .project(castTimestampTimeZoneCall)
        .build();

    final String expectedBigQuery =
        "SELECT CAST(CURRENT_DATETIME() AS TIMESTAMP_WITH_LOCAL_TIME_ZONE) AS `$f0`";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBigQuery));
  }


  @Test public void testParseDateTimeFormat() {
    final RelBuilder builder = relBuilder();
    final RexNode parseDateNode =
        builder.call(SqlLibraryOperators.PARSE_DATE, builder.literal("YYYYMMDD"), builder.literal("99991231"));
    final RexNode parseTimeNode =
        builder.call(SqlLibraryOperators.PARSE_TIME, builder.literal("HH24MISS"), builder.literal("122333"));
    final RelNode root = builder.scan("EMP").
        project(builder.alias(parseDateNode, "date1"),
            builder.alias(parseTimeNode, "time1"))
        .build();

    final String expectedSql = "SELECT PARSE_DATE('YYYYMMDD', '99991231') AS \"date1\", "
        + "PARSE_TIME('HH24MISS', '122333') AS \"time1\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT PARSE_DATE('%Y%m%d', '99991231') AS date1, "
        + "PARSE_TIME('%H%M%S', '122333') AS time1\n"
        + "FROM scott.EMP";
    final String expectedSparkQuery = "SELECT PARSE_DATE('YYYYMMDD', '99991231') date1, "
        + "PARSE_TIME('HH24MISS', '122333') time1\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testPositionOperator() {
    final RelBuilder builder = relBuilder();

    final RexNode parseTrimNode =
        builder.call(SqlStdOperatorTable.POSITION, builder.literal("a"),
        builder.literal("Name"));
    final RelNode root = builder.scan("EMP").
        project(builder.alias(parseTrimNode, "t"))
        .build();

    final String expectedSql = "SELECT POSITION('a' IN 'Name') AS \"t\"\n"
        + "FROM \"scott\".\"EMP\"";

    final String expectedSparkQuery = "SELECT POSITION('a' IN 'Name') t\nFROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testBigQueryErrorOperator() {
    final RelBuilder builder = relBuilder();

    final SqlFunction errorOperator =
        new SqlFunction("ERROR",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_2000,
            null,
            OperandTypes.STRING_STRING,
            SqlFunctionCategory.SYSTEM);

    final RexNode parseTrimNode =
        builder.call(errorOperator, builder.literal("Error Message!"));
    final RelNode root = builder.scan("EMP").
        project(builder.alias(parseTrimNode, "t"))
        .build();

    final String expectedSql = "SELECT ERROR('Error Message!') AS \"t\"\n"
        + "FROM \"scott\".\"EMP\"";

    final String expectedSparkQuery = "SELECT RAISE_ERROR('Error Message!') t\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testTrue() {
    final RelBuilder builder = relBuilder();
    final RexNode trueRexNode = builder.call(TRUE);
    final RelNode root = builder.scan("EMP")
        .project(builder.alias(trueRexNode, "dm"))
        .build();
    final String expectedSql = "SELECT TRUE() AS \"dm\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT TRUE  AS dm\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testTimeFromParts() {
    final RelBuilder builder = relBuilder();
    final RexNode timeFromParts =
        builder.call(SqlLibraryOperators.TIME_FROM_PARTS, builder.literal(3),
        builder.literal(15), builder.literal(30));
    final RelNode root = builder.scan("EMP")
        .project(builder.alias(timeFromParts, "time_diff")).build();

    final String expectedSql = "SELECT TIME_FROM_PARTS(3, 15, 30) AS \"time_diff\"\nFROM "
        + "\"scott\".\"EMP\"";

    assertThat(toSql(root), isLinux(expectedSql));
  }

  @Test public void testJsonCheckFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode jsonCheckNode =
        builder.call(SqlLibraryOperators.JSON_CHECK, builder.literal("{\"name\": \"Bob\", \"age\": \"thirty\"}"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(jsonCheckNode, "json_data"))
        .build();
    final String expectedTeradataQuery = "SELECT JSON_CHECK('{\"name\": \"Bob\", \"age\": "
        + "\"thirty\"}') AS \"json_data\"\n"
        + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.TERADATA.getDialect()), isLinux(expectedTeradataQuery));
  }

  @Test public void testJsonExtractFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode jsonCheckNode =
        builder.call(SqlLibraryOperators.JSON_EXTRACT, builder.literal("{\"name\": \"Bob\", \"age\": \"thirty\"}"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(jsonCheckNode, "json_data"))
        .build();
    final String expectedBqQuery = "SELECT "
        + "JSON_EXTRACT('{\"name\": \"Bob\", \"age\": \"thirty\"}') AS json_data\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBqQuery));
  }

  @Test public void testToCharWithSingleOperand() {
    final RelBuilder builder = relBuilder();
    final RexNode toCharNode =
        builder.call(SqlLibraryOperators.TO_CHAR, builder.literal(123.56));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(toCharNode, "Numeric format"))
        .build();
    final String expectedTDSql = "SELECT TO_CHAR(123.56) AS \"Numeric format\"\n"
        + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.TERADATA.getDialect()), isLinux(expectedTDSql));
  }



  @Test public void testFalse() {
    final RelBuilder builder = relBuilder();
    final RexNode falseRexNode = builder.call(FALSE);
    final RelNode root = builder.scan("EMP")
        .project(builder.alias(falseRexNode, "dm"))
        .build();
    final String expectedSql = "SELECT FALSE() AS \"dm\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT FALSE  AS dm\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test void testFilterWithInnerJoinGivingAssertionError() {
    String query = "SELECT * FROM  \n"
        + "\"foodmart\".\"employee\" E1\n"
        + "INNER JOIN\n"
        + "\"foodmart\".\"employee\" E2\n"
        + "ON CASE WHEN E1.\"first_name\" = '' THEN E1.\"first_name\" <> 'abc' "
        + "ELSE UPPER(E1.\"first_name\") = UPPER(E2.\"first_name\") END AND "
        + "CASE WHEN E1.\"first_name\" = '' THEN E1.\"first_name\" <> 'abc' "
        + "ELSE INITCAP(E1.\"first_name\") = INITCAP(E2.\"first_name\") END";
    String expect = "SELECT *\n"
        + "FROM foodmart.employee\n"
        + "INNER JOIN foodmart.employee AS employee0 ON CASE WHEN employee.first_name = '' THEN employee.first_name <> 'abc' ELSE UPPER(employee.first_name) = UPPER(employee0.first_name) END AND CASE WHEN employee.first_name = '' THEN employee.first_name <> 'abc' ELSE INITCAP(employee.first_name) = INITCAP(employee0.first_name) END";

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(FilterExtractInnerJoinRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    RuleSet rules = RuleSets.ofList(CoreRules.FILTER_EXTRACT_INNER_JOIN_RULE);
    sql(query).withBigQuery().optimize(rules, hepPlanner).ok(expect);
  }

  @Test public void testSubQueryWithFunctionCallInGroupByClause() {
    final RelBuilder builder = relBuilder();
    builder.scan("EMP");
    final RexNode lengthFunctionCall =
        builder.call(SqlStdOperatorTable.CHAR_LENGTH, builder.field(1));
    final RelNode subQueryInClause = builder
        .project(builder.alias(lengthFunctionCall, "A2301"))
        .aggregate(builder.groupKey(builder.field(0)))
        .filter(
            builder.call(EQUALS,
            builder.call(SqlStdOperatorTable.CHARACTER_LENGTH,
                builder.literal("TEST")), builder.literal(2)))
        .project(Arrays.asList(builder.field(0)), Arrays.asList("a2301"), true)
        .build();

    builder.scan("EMP");
    final RelNode root = builder
        .filter(RexSubQuery.in(subQueryInClause, ImmutableList.of(builder.field(0))))
        .project(builder.field(0)).build();

    final String expectedSql = "SELECT \"EMPNO\"\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "WHERE \"EMPNO\" IN (SELECT CHAR_LENGTH(\"ENAME\") AS \"a2301\"\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "GROUP BY CHAR_LENGTH(\"ENAME\")\n"
        + "HAVING CHARACTER_LENGTH('TEST') = 2)";

    final String expectedBiqQuery = "SELECT EMPNO\n"
        + "FROM scott.EMP\n"
        + "WHERE EMPNO IN (SELECT A2301 AS a2301\n"
        + "FROM (SELECT LENGTH(ENAME) AS A2301\n"
        + "FROM scott.EMP\n"
        + "GROUP BY A2301\n"
        + "HAVING LENGTH('TEST') = 2) AS t1)";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testSubQueryWithFunctionCallInGroupByAndAggregateInHavingClause() {
    final RelBuilder builder = relBuilder();
    builder.scan("EMP");
    final RexNode lengthFunctionCall =
        builder.call(SqlStdOperatorTable.CHAR_LENGTH, builder.field(1));
    final RelNode subQueryInClause = builder
        .project(builder.alias(lengthFunctionCall, "A2301"), builder.field("EMPNO"))
        .aggregate(builder.groupKey(builder.field(0)),
            builder.countStar("EXPR$1354574361"))
        .filter(
            builder.call(EQUALS,
                builder.field("EXPR$1354574361"), builder.literal(2)))
        .project(Arrays.asList(builder.field(0)), Arrays.asList("a2301"), true)
        .build();

    builder.scan("EMP");
    final RelNode root = builder
        .filter(RexSubQuery.in(subQueryInClause, ImmutableList.of(builder.field(0))))
        .project(builder.field(0)).build();

    final String expectedSql = "SELECT \"EMPNO\"\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "WHERE \"EMPNO\" IN (SELECT CHAR_LENGTH(\"ENAME\") AS \"a2301\"\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "GROUP BY CHAR_LENGTH(\"ENAME\")\n"
        + "HAVING COUNT(*) = 2)";

    final String expectedBiqQuery = "SELECT EMPNO\n"
        + "FROM scott.EMP\n"
        + "WHERE EMPNO IN (SELECT A2301 AS a2301\n"
        + "FROM (SELECT LENGTH(ENAME) AS A2301\n"
        + "FROM scott.EMP\n"
        + "GROUP BY A2301\n"
        + "HAVING COUNT(*) = 2) AS t1)";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }


  @Test public void dayOccurenceOfMonth() {
    final RelBuilder builder = relBuilder();
    final RexNode dayOccurenceOfMonth =
        builder.call(DAYOCCURRENCE_OF_MONTH, builder.call(CURRENT_DATE));
    final RelNode root = builder.scan("EMP")
        .project(dayOccurenceOfMonth)
        .build();
    final String expectedSql = "SELECT DAYOCCURRENCE_OF_MONTH(CURRENT_DATE) AS \"$f0\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedSpark = "SELECT CEIL(EXTRACT(DAY FROM CURRENT_DATE) / 7) $f0\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
  }

  @Test public void testDateTimeNumberOfYear() {
    final RelBuilder builder = relBuilder();
    final RexNode weekNumberOfYearCall =
        builder.call(WEEKNUMBER_OF_YEAR, builder.call(CURRENT_DATE));
    final RexNode monthNumberOfYearCall =
        builder.call(MONTHNUMBER_OF_YEAR, builder.call(CURRENT_TIMESTAMP));
    final RexNode quarterNumberOfYearCall =
        builder.call(QUARTERNUMBER_OF_YEAR, builder.call(CURRENT_TIMESTAMP));
    final RelNode root = builder.scan("EMP")
        .project(weekNumberOfYearCall,
            monthNumberOfYearCall,
            quarterNumberOfYearCall)
        .build();
    final String expectedSql = "SELECT WEEKNUMBER_OF_YEAR(CURRENT_DATE) AS \"$f0\", "
        + "MONTHNUMBER_OF_YEAR(CURRENT_TIMESTAMP) AS \"$f1\", "
        + "QUARTERNUMBER_OF_YEAR(CURRENT_TIMESTAMP) AS \"$f2\""
        + "\nFROM \"scott\".\"EMP\"";
    final String expectedSpark = "SELECT EXTRACT(WEEK FROM CURRENT_DATE) $f0, "
        + "EXTRACT(MONTH FROM CURRENT_TIMESTAMP) $f1, "
        + "EXTRACT(QUARTER FROM CURRENT_TIMESTAMP) $f2\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
  }

  @Test public void testXNumberOfCalendar() {
    final RelBuilder builder = relBuilder();
    final RexNode dayNumberOfCalendarCall =
        builder.call(DAYNUMBER_OF_CALENDAR, builder.call(CURRENT_TIMESTAMP));
    final RexNode weekNumberOfCalendarCall =
        builder.call(WEEKNUMBER_OF_CALENDAR, builder.call(CURRENT_TIMESTAMP));
    final RexNode yearNumberOfCalendarCall =
        builder.call(YEARNUMBER_OF_CALENDAR, builder.call(CURRENT_TIMESTAMP));
    final RelNode root = builder.scan("EMP")
        .project(dayNumberOfCalendarCall,
            weekNumberOfCalendarCall,
            yearNumberOfCalendarCall)
        .build();
    final String expectedSql = "SELECT DAYNUMBER_OF_CALENDAR(CURRENT_TIMESTAMP) AS \"$f0\", "
        + "WEEKNUMBER_OF_CALENDAR(CURRENT_TIMESTAMP) AS \"$f1\", "
        + "YEARNUMBER_OF_CALENDAR(CURRENT_TIMESTAMP) AS \"$f2\""
        + "\nFROM \"scott\".\"EMP\"";
    final String expectedSpark = "SELECT DATEDIFF(CURRENT_TIMESTAMP, DATE '1899-12-31') $f0,"
        + " FLOOR((DATEDIFF(CURRENT_TIMESTAMP, DATE '1900-01-01') + 1) / 7) $f1,"
        + " EXTRACT(YEAR FROM CURRENT_TIMESTAMP) $f2"
        + "\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
  }

  @Test public void testForAddingMonths() {
    final RelBuilder builder = relBuilder();

    final RexNode createRexNode =
        builder.call(PLUS, builder.cast(builder.literal("1999-07-01"), SqlTypeName.DATE),
        builder.getRexBuilder().makeIntervalLiteral(new BigDecimal(10),
            new SqlIntervalQualifier(MONTH, 6, MONTH,
                -1, SqlParserPos.ZERO)));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(createRexNode, "FD"))
        .build();
    final String expectedSparkQuery = "SELECT DATE '1999-07-01' + INTERVAL '10' MONTH FD"
        + "\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testForSparkCurrentTime() {
    String query = "SELECT CURRENT_TIME(2) > '08:00:00', "
        + "CAST(\"hire_date\" AS TIME(4)) = '00:00:00'"
        + "FROM \"foodmart\".\"employee\"";
    final String expectedSpark = "SELECT CAST('1970-01-01 ' || DATE_FORMAT(CURRENT_TIMESTAMP, "
        + "'HH:mm:ss.SS') AS TIMESTAMP) > TIMESTAMP '1970-01-01 08:00:00.00', "
        + "CAST('1970-01-01 ' || DATE_FORMAT(hire_date, 'HH:mm:ss.SSS') AS TIMESTAMP) = "
        + "TIMESTAMP '1970-01-01 00:00:00.000'\nFROM foodmart.employee";
    sql(query)
        .withSpark()
        .ok(expectedSpark);
  }

  @Test public void testForSparkExtract() {
    String query = "SELECT "
        + "EXTRACT(YEAR FROM \"hire_date\") AS hire_date "
        + "FROM \"foodmart\".\"employee\"";
    final String expectedSpark = "SELECT EXTRACT(YEAR FROM hire_date) HIRE_DATE"
        + "\nFROM foodmart.employee";
    sql(query)
        .withSpark()
        .ok(expectedSpark);
  }

  @Test public void testForHashrowWithMultipleArguments() {
    final RelBuilder builder = relBuilder();
    final RexNode hashrow =
        builder.call(SqlLibraryOperators.HASHROW, builder.literal("employee"), builder.scan("EMP").field(1),
        builder.literal("dm"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(hashrow, "HASHCODE"))
        .build();

    final String expectedBiqQuery = "SELECT FARM_FINGERPRINT(CONCAT('employee', ENAME, 'dm')) AS "
        + "HASHCODE\nFROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testForPI() {
    final RelBuilder builder = relBuilder();
    final RexNode piNode = builder.call(SqlStdOperatorTable.PI);
    final RelNode root = builder.scan("EMP")
        .project(builder.alias(piNode, "t"))
        .build();

    final String expectedSpark = "SELECT PI() t\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
  }

  @Test public void testSessionUser() {
    String query = "select SESSION_USER";
    final String expectedSparkSql = "SELECT CURRENT_USER SESSION_USER";
    sql(query)
        .withSpark()
        .ok(expectedSparkSql);
  }

  @Test public void testSafeCast() {
    final RelBuilder builder = relBuilder();
    RelDataType type = builder.getCluster().getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
    final RexNode safeCastNode =
        builder.getRexBuilder().makeAbstractCast(type, builder.literal(1234), true);
    final RelNode root = builder
        .scan("EMP")
        .project(safeCastNode)
        .build();
    final String expectedBqSql = "SELECT '1234' AS `$f0`\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBqSql));
  }

  @Test public void testIsRealFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode toReal =
        builder.call(SqlLibraryOperators.IS_REAL, builder.literal(123.12));

    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(toReal, "Result"))
        .build();

    final String expectedSql = "SELECT IS_REAL(123.12) AS \"Result\"\n"
        + "FROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
  }


  @Test public void testTruncWithTimestamp() {
    final RelBuilder builder = relBuilder();
    final RexNode trunc =
        builder.call(
            SqlLibraryOperators.TRUNC, builder.cast(builder.literal("2017-02-14 20:38:40"),
                        SqlTypeName.TIMESTAMP),
        builder.literal("DAY"));
    final RelNode root = builder
        .scan("EMP")
        .project(trunc)
        .build();
    final String expectedSparkSql = "SELECT CAST(DATE_TRUNC('DAY', TIMESTAMP '2017-02-14 "
        + "20:38:40') AS DATE) $f0\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkSql));
  }

  @Test public void testFormatFunctionCastAsInteger() {
    final RelBuilder builder = relBuilder();
    final RexNode formatIntegerCastRexNode =
        builder.cast(
            builder.call(SqlLibraryOperators.FORMAT,
        builder.literal("'%.4f'"), builder.scan("EMP").field(5)), SqlTypeName.INTEGER);
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(formatIntegerCastRexNode, "FORMATCALL"))
        .build();
    final String expectedSql = "SELECT CAST(FORMAT('''%.4f''', \"SAL\") AS INTEGER) AS "
        + "\"FORMATCALL\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT CAST(CAST(FORMAT('\\'%.4f\\'', SAL) AS FLOAT64) AS "
        + "INTEGER) AS FORMATCALL\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testCastAsIntegerForStringLiteral() {
    final RelBuilder builder = relBuilder();
    final RexNode formatIntegerCastRexNode =
        builder.cast(builder.literal("45.67"), SqlTypeName.INTEGER);
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(formatIntegerCastRexNode, "c1"))
        .build();
    final String expectedSql = "SELECT CAST('45.67' AS INTEGER) AS \"c1\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT CAST(CAST('45.67' AS FLOAT64) AS INTEGER) AS c1\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testForToChar() {
    final RelBuilder builder = relBuilder();

    final RexNode toCharWithDate =
        builder.call(SqlLibraryOperators.TO_CHAR, builder.getRexBuilder().makeDateLiteral(new DateString("1970-01-01")),
        builder.literal("MM-DD-YYYY HH24:MI:SS"));
    final RexNode toCharWithNumber =
        builder.call(SqlLibraryOperators.TO_CHAR, builder.literal(1000), builder.literal("9999"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(toCharWithDate, "FD"), toCharWithNumber)
        .build();
    final String expectedSparkQuery = "SELECT "
        + "DATE_FORMAT(DATE '1970-01-01', 'MM-dd-yyyy HH:mm:ss') FD, TO_CHAR(1000, '9999') $f1"
        + "\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testForToCharWithJulian() {
    final RelBuilder builder = relBuilder();

    final RexNode toCharWithDate =
        builder.call(SqlLibraryOperators.TO_CHAR,
        builder.getRexBuilder().makeDateLiteral(new DateString("1970-01-01")),
        builder.literal("J"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(toCharWithDate, "FD"))
        .build();
    final String expectedOracleQuery = "SELECT TO_CHAR(TO_DATE('1970-01-01', 'YYYY-MM-DD'), 'J') "
        + "\"FD\"\nFROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.ORACLE.getDialect()), isLinux(expectedOracleQuery));
  }

  @Test public void testQualify() {
    final RelNode root = createRelNodeWithQualifyStatement();
    final String expectedSparkQuery = "SELECT HIREDATE\n"
        + "FROM scott.EMP\n"
        + "QUALIFY (MAX(EMPNO) OVER (ORDER BY HIREDATE IS NULL, HIREDATE RANGE BETWEEN UNBOUNDED "
        + "PRECEDING AND UNBOUNDED FOLLOWING)) = 1";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testQualifyWithReferenceFromSelect() {
    RelBuilder builder = relBuilder().scan("EMP");
    RexNode aggregateFunRexNode = builder.call(SqlStdOperatorTable.COUNT, builder.field(0));
    RelDataType type = aggregateFunRexNode.getType();
    final RexNode analyticalFunCall =
        builder.getRexBuilder().makeOver(type, SqlStdOperatorTable.COUNT,
            ImmutableList.of(), ImmutableList.of(), ImmutableList.of(),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            true, true, false, false, false);
    final RelNode root = builder
        .project(builder.alias(analyticalFunCall, "CNT"))
        .filter(builder.equals(builder.field("CNT"), builder.literal(1)))
        .build();
    final String expectedBQSql = "SELECT COUNT(*) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS CNT\n"
        + "FROM scott.EMP\n"
        + "QUALIFY (COUNT(*) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQSql));
  }

  @Test public void testQueryWithTwoFilters() {
    RelBuilder builder = relBuilder().scan("EMP");
    RexNode aggregateFunRexNode = builder.call(SqlStdOperatorTable.COUNT, builder.field(0));
    RelDataType type = aggregateFunRexNode.getType();
    final RexNode analyticalFunCall =
        builder.getRexBuilder().makeOver(type, SqlStdOperatorTable.COUNT,
            ImmutableList.of(), ImmutableList.of(), ImmutableList.of(),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            true, true, false, false, false);
    final RelNode root = builder
        .filter(builder.greaterThanOrEqual(builder.field("EMPNO"), builder.literal(20)))
        .project(builder.field("EMPNO"), builder.alias(analyticalFunCall, "CNT"))
        .filter(builder.lessThanOrEqual(builder.field("EMPNO"), builder.literal(50)))
        .build();
    final String expectedBQSql = "SELECT *\n"
        + "FROM (SELECT EMPNO, COUNT(*) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "
        + "CNT\n"
        + "FROM scott.EMP\n"
        + "WHERE EMPNO >= 20) AS t0\n"
        + "WHERE EMPNO <= 50";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQSql));
  }

  @Test public void testQualifyForSqlSelectCall() {
    final RelNode root = createRelNodeWithQualifyStatement();
    SqlCall call = (SqlCall) toSqlNode(root, DatabaseProduct.BIG_QUERY.getDialect());
    List<SqlNode> operands = call.getOperandList();
    String  generatedSql =
            String.valueOf(
                call.getOperator().createCall(
            null, SqlParserPos.ZERO, operands).toSqlString(DatabaseProduct.SPARK.getDialect()));
    String expectedSql = "SELECT HIREDATE\n"
        + "FROM scott.EMP\n"
        + "QUALIFY (MAX(EMPNO) OVER (ORDER BY HIREDATE IS NULL, HIREDATE RANGE BETWEEN UNBOUNDED "
        + "PRECEDING AND UNBOUNDED FOLLOWING)) = 1";
    assertThat(generatedSql, isLinux(expectedSql));
  }

  @Test void testForSparkRound() {
    final String query = "select round(123.41445, 2)";
    final String expected = "SELECT ROUND(123.41445, 2)";
    sql(query).withSpark().ok(expected);
  }

  @Test public void testStandardHash() {
    final RelBuilder builder = relBuilder();
    final RexNode stdHash =
        builder.call(SqlLibraryOperators.STANDARD_HASH, builder.scan("EMP").field("ENAME"));
    final RelNode root = builder
        .scan("EMP")
        .project(stdHash)
        .build();
    final String expectedOracleSql = "SELECT STANDARD_HASH(\"ENAME\")"
        + " \"$f0\"\nFROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.ORACLE.getDialect()), isLinux(expectedOracleSql));
  }

  @Test public void testBigQuerySha512Function() {
    final RelBuilder builder = relBuilder();
    final RexNode sha512Node =
        builder.call(SqlLibraryOperators.SHA512, builder.scan("EMP").field(1));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(sha512Node, "hashing"))
        .build();
    final String expectedBQSql = "SELECT SHA512(ENAME) AS hashing\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQSql));
  }

  @Test public void testRoundFunctionWithColumn() {
    final String query = "SELECT round(\"gross_weight\", \"product_id\") AS \"a\"\n"
        + "FROM \"foodmart\".\"product\"";
    final String expectedSparkSql = "SELECT UDF_ROUND(gross_weight, product_id) a\n"
        + "FROM foodmart.product";
    sql(query)
        .withSpark()
        .ok(expectedSparkSql);
  }

  @Test public void testForBitAndNotFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode bitPart =
        builder.call(SqlLibraryOperators.BITANDNOT, builder.literal(3), builder.literal(2));

    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(bitPart, "Result"))
        .build();

    final String expectedDB2Query = "SELECT BITANDNOT(3, 2) AS Result\nFROM scott.EMP AS EMP";

    assertThat(toSql(root, DatabaseProduct.DB2.getDialect()), isLinux(expectedDB2Query));
  }


  @Test public void testRoundFunctionWithColumnAndLiteral() {
    final String query = "SELECT round(\"gross_weight\", 2) AS \"a\"\n"
        + "FROM \"foodmart\".\"product\"";
    final String expectedSparkSql = "SELECT ROUND(gross_weight, 2) a\n"
        + "FROM foodmart.product";
    sql(query)
        .withSpark()
        .ok(expectedSparkSql);
  }

  @Test public void testRoundFunctionWithOnlyColumn() {
    final String query = "SELECT round(\"gross_weight\") AS \"a\"\n"
        + "FROM \"foodmart\".\"product\"";
    final String expectedSparkSql = "SELECT ROUND(gross_weight) a\n"
        + "FROM foodmart.product";
    sql(query)
        .withSpark()
        .ok(expectedSparkSql);
  }

  @Test public void testSortByOrdinalForSpark() {
    final String query = "SELECT \"product_id\",\"gross_weight\" from \"product\"\n"
        + "order by 2";
    final String expectedSparkSql = "SELECT product_id, gross_weight\n"
        + "FROM foodmart.product\n"
        + "ORDER BY gross_weight NULLS LAST";
    sql(query)
        .withSpark()
        .ok(expectedSparkSql);
  }

  @Test public void newLineInLiteral() {
    final String query = "SELECT 'netezza\n to bq'";
    final String expectedBQSql = "SELECT 'netezza\\n to bq'";
    sql(query)
        .withBigQuery()
        .ok(expectedBQSql);
  }

  @Test public void newLineInWhereClauseLiteral() {
    final String query = "SELECT *\n"
        + "FROM \"foodmart\".\"employee\"\n"
        + "WHERE \"first_name\" ='Maya\n Gutierrez'";
    final String expectedBQSql = "SELECT *\n"
        + "FROM foodmart.employee\n"
        + "WHERE first_name = 'Maya\\n Gutierrez'";
    sql(query)
        .withBigQuery()
        .ok(expectedBQSql);
  }

  @Test public void literalWithBackslashesInSelectWithAlias() {
    final String query = "SELECT 'No IBL' AS \"FIRST_NM\","
        + " 'US\\' AS \"AB\", 'Y' AS \"IBL_FG\", 'IBL' AS "
        + "\"PRSN_ORG_ROLE_CD\"";
    final String expectedBQSql = "SELECT 'No IBL' AS FIRST_NM,"
        + " 'US\\\\' AS AB, 'Y' AS IBL_FG,"
        + " 'IBL' AS PRSN_ORG_ROLE_CD";
    sql(query)
        .withBigQuery()
        .ok(expectedBQSql);
  }

  @Test public void literalWithBackslashesInSelectList() {
    final String query = "SELECT \"first_name\", '', '', '', '', '', '\\'\n"
        + "  FROM \"foodmart\".\"employee\"";
    final String expectedBQSql = "SELECT first_name, '', '', '', '', '', '\\\\'\n"
        + "FROM foodmart.employee";
    sql(query)
        .withBigQuery()
        .ok(expectedBQSql);
  }

  @Test public void testToDateFunctionWithFormatYYYYDDMM() {
    final RelBuilder builder = relBuilder();
    final RexNode toDateRexNode =
        builder.call(SqlLibraryOperators.TO_DATE, builder.literal("20092003"), builder.literal("YYYYDDMM"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(toDateRexNode, "date_value"))
        .build();
    final String expectedSpark =
        "SELECT TO_DATE('20092003', 'yyyyddMM') date_value\n"
            + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
  }

  @Test public void testToCharFunctionWithYYYFormat() {
    final RelBuilder builder = relBuilder();
    final RexNode toCharNode =
        builder.call(SqlLibraryOperators.TO_CHAR, builder.call(CURRENT_TIMESTAMP), builder.literal("DD/MM/YYY"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(toCharNode, "three_year_format"))
        .build();
    final String expectedSpark =
        "SELECT TO_CHAR(CURRENT_TIMESTAMP, 'DD/MM/YYY') \"three_year_format\"\n"
        + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.ORACLE.getDialect()), isLinux(expectedSpark));
  }

  @Test public void testModOperationOnDateField() {
    final RelBuilder builder = relBuilder();
    final RexNode modRex =
        builder.call(DATE_MOD, builder.call(CURRENT_DATE),
        builder.literal(2));
    final RelNode root = builder.scan("EMP")
        .project(builder.alias(modRex, "current_date"))
        .build();
    final String expectedSql = "SELECT "
        + "MOD((EXTRACT(YEAR FROM CURRENT_DATE) - 1900) * 10000 + EXTRACT(MONTH FROM CURRENT_DATE)  * 100 + "
        + "EXTRACT(DAY FROM CURRENT_DATE) , 2) current_date\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSql));
  }

  @Test public void testCurrentDatePlusIntervalDayHour() {
    final RelBuilder builder = relBuilder();

    final RexNode createRexNode =
        builder.call(
            PLUS, builder.call(CURRENT_DATE), builder.call(PLUS,
            builder.getRexBuilder().makeIntervalLiteral(new BigDecimal(86400000),
                new SqlIntervalQualifier(DAY, 6, DAY,
                    -1, SqlParserPos.ZERO)),
            builder.getRexBuilder().makeIntervalLiteral(new BigDecimal(3600000),
                new SqlIntervalQualifier(HOUR, 1, HOUR,
                    -1, SqlParserPos.ZERO))));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(createRexNode, "FD"))
        .build();
    final String expectedBigQuery = "SELECT CURRENT_DATE + (INTERVAL 1 DAY + INTERVAL 1 HOUR) AS FD"
        + "\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBigQuery));
  }

  @Test public void testCurrentDatePlusIntervalHourMin() {
    final RelBuilder builder = relBuilder();

    final RexNode createRexNode =
        builder.call(
            PLUS, builder.call(CURRENT_DATE), builder.call(PLUS,
            builder.getRexBuilder().makeIntervalLiteral(new BigDecimal(3600000),
                new SqlIntervalQualifier(HOUR, 1, HOUR,
                    -1, SqlParserPos.ZERO)),
            builder.getRexBuilder().makeIntervalLiteral(new BigDecimal(60000),
                new SqlIntervalQualifier(MINUTE, 1, MINUTE,
                    -1, SqlParserPos.ZERO))));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(createRexNode, "FD"))
        .build();
    final String expectedBigQuery = "SELECT CURRENT_DATE + (INTERVAL 1 HOUR + INTERVAL 1 MINUTE) "
        + "AS FD"
        + "\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBigQuery));
  }

  @Test public void testCurrentDatePlusIntervalHourSec() {
    final RelBuilder builder = relBuilder();

    final RexNode createRexNode =
        builder.call(
            PLUS, builder.call(CURRENT_DATE), builder.call(PLUS,
            builder.getRexBuilder().makeIntervalLiteral(new BigDecimal(3600000),
                new SqlIntervalQualifier(HOUR, 1, HOUR,
                    -1, SqlParserPos.ZERO)),
            builder.getRexBuilder().makeIntervalLiteral(new BigDecimal(1000),
                new SqlIntervalQualifier(SECOND, 1, SECOND,
                    -1, SqlParserPos.ZERO))));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(createRexNode, "FD"))
        .build();
    final String expectedBigQuery = "SELECT CURRENT_DATE + (INTERVAL 1 HOUR + INTERVAL 1 SECOND) "
        + "AS FD"
        + "\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBigQuery));
  }

  @Test public void testCurrentDatePlusIntervalYearMonth() {
    final RelBuilder builder = relBuilder();

    final RexNode createRexNode =
        builder.call(
            PLUS, builder.call(CURRENT_DATE), builder.call(PLUS,
            builder.getRexBuilder().makeIntervalLiteral(new BigDecimal(12),
                new SqlIntervalQualifier(YEAR, 1, YEAR,
                    -1, SqlParserPos.ZERO)),
            builder.getRexBuilder().makeIntervalLiteral(new BigDecimal(1),
                new SqlIntervalQualifier(MONTH, 1, MONTH,
                    -1, SqlParserPos.ZERO))));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(createRexNode, "FD"))
        .build();
    final String expectedBigQuery = "SELECT CURRENT_DATE + (INTERVAL 1 YEAR + INTERVAL 1 MONTH) "
        + "AS FD"
        + "\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBigQuery));
  }

  // Unparsing "ABC" IN(UNNEST(ARRAY("ABC", "XYZ"))) --> "ABC" IN UNNEST(ARRAY["ABC", "XYZ"])
  @Test public void inUnnestSqlNode() {
    final RelBuilder builder = relBuilder();
    RexNode arrayRex =
        builder.call(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, builder.literal("ABC"), builder.literal("XYZ"));
    RexNode unnestRex = builder.call(SqlStdOperatorTable.UNNEST, arrayRex);
    final RexNode createRexNode =
        builder.call(IN, builder.literal("ABC"), unnestRex);
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(createRexNode, "array_contains"))
        .build();
    final String expectedBiqQuery = "SELECT 'ABC' IN UNNEST(ARRAY['ABC', 'XYZ']) "
        + "AS array_contains\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void rowNumberOverFunctionAsWhereClauseInJoin() {
    String query = " select \"A\".\"product_id\"\n"
        + "    from (select \"product_id\", ROW_NUMBER() OVER (ORDER BY \"product_id\") AS RNK from \"product\") A\n"
        + "    cross join \"sales_fact_1997\"\n"
        + "    where \"RNK\" =1 \n"
        + "    group by \"A\".\"product_id\"\n";
    final String expectedBQ = "SELECT t.product_id\n"
        + "FROM (SELECT product_id, ROW_NUMBER() OVER (ORDER BY product_id IS NULL, product_id) AS "
        + "RNK\n"
        + "FROM foodmart.product) AS t\n"
        + "INNER JOIN foodmart.sales_fact_1997 ON TRUE\n"
        + "WHERE t.RNK = 1\n"
        + "GROUP BY t.product_id";
    sql(query)
        .withBigQuery()
        .ok(expectedBQ);
  }

  @Test public void testForRegexpSimilarFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode regexpSimilar =
        builder.call(SqlLibraryOperators.REGEXP_SIMILAR, builder.literal("12-12-2000"),
                builder.literal("^\\d\\d-\\w{2}-\\d{4}$"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(regexpSimilar, "A"))
        .build();

    final String expectedBiqQuery = "SELECT IF(REGEXP_CONTAINS('12-12-2000' , "
        + "r'^\\d\\d-\\w{2}-\\d{4}$'), 1, 0) AS A\n"
        + "FROM scott.EMP";

    final String expectedSparkSql = "SELECT IF('12-12-2000' rlike r'^\\d\\d-\\w{2}-\\d{4}$', 1, 0)"
        + " A\nFROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkSql));
  }

  @Test public void testForRegexpSimilarFunctionWithThirdArgumentAsI() {
    final RelBuilder builder = relBuilder();
    final RexNode regexpSimilar =
        builder.call(SqlLibraryOperators.REGEXP_SIMILAR, builder.literal("Mike BIrd"), builder.literal("MikE B(i|y)RD"),
        builder.literal("i"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(regexpSimilar, "A"))
        .build();

    final String expectedBiqQuery = "SELECT IF(REGEXP_CONTAINS('Mike BIrd' , "
        + "r'^(?i)MikE B(i|y)RD$'), 1, 0) AS A\n"
        + "FROM scott.EMP";

    final String expectedSparkSql = "SELECT IF('Mike BIrd' rlike r'(?i)MikE B(i|y)RD', 1, 0)"
        + " A\nFROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkSql));
  }

  @Test public void testForRegexpSimilarFunctionWithThirdArgumentAsX() {
    final RelBuilder builder = relBuilder();
    final RexNode regexpSimilar =
        builder.call(SqlLibraryOperators.REGEXP_SIMILAR, builder.literal("Mike"),
                builder.literal("M i k e"), builder.literal("x"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(regexpSimilar, "A"))
        .build();

    final String expectedBiqQuery = "SELECT IF(REGEXP_CONTAINS('Mike' , r'Mike'), 1, 0) AS A\n"
        + "FROM scott.EMP";

    final String expectedSparkSql = "SELECT IF('Mike' rlike r'(?x)M i k e', 1, 0)"
        + " A\nFROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }


  @Test public void testForRegexpSimilarFunctionWithThirdArgumentAsC() {
    final RelBuilder builder = relBuilder();
    final RexNode regexpSimilar =
        builder.call(SqlLibraryOperators.REGEXP_SIMILAR, builder.literal("Mike Bird"), builder.literal("Mike B(i|y)RD"),
        builder.literal("c"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(regexpSimilar, "A"))
        .build();

    final String expectedBiqQuery = "SELECT IF(REGEXP_CONTAINS('Mike Bird' , "
        + "r'Mike B(i|y)RD'), 1, 0) AS A\n"
        + "FROM scott.EMP";

    final String expectedSparkSql = "SELECT IF('Mike Bird' rlike r'Mike B(i|y)RD', 1, 0)"
        + " A\nFROM scott.EMP";

    final String expectedSnowflake = "SELECT IF(REGEXP_LIKE('Mike Bird', 'Mike B(i|y)RD', 'c'), "
        + "1, 0) AS \"A\"\n"
        + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkSql));
    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedSnowflake));
  }

  @Test public void testForRegexpSimilarFunctionWithThirdArgumentAsN() {
    final RelBuilder builder = relBuilder();
    final RexNode regexpSimilar =
        builder.call(
            SqlLibraryOperators.REGEXP_SIMILAR, builder.literal("abcd\n"
            + "e"), builder.literal(".*e"), builder.literal("n"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(regexpSimilar, "A"))
        .build();

    final String expectedSparkSql = "SELECT IF('abcd\n"
        + "e' rlike r'.*e', 1, 0)"
        + " A\nFROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkSql));
  }

  @Test public void testForRegexpLikeFunctionWithThirdArgumentAsI() {
    final RelBuilder builder = relBuilder();
    final RexNode regexplike =
        builder.call(SqlLibraryOperators.REGEXP_LIKE, builder.literal("Mike Bird"), builder.literal("Mike B(i|y)RD"),
        builder.literal("i"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(regexplike, "A"))
        .build();

    final String expectedBqSql = "SELECT REGEXP_CONTAINS('Mike Bird' , "
        + "r'^(?i)Mike B(i|y)RD$') AS A\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBqSql));
  }

  @Test public void testForPatindexFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode patindex =
        builder.call(SqlLibraryOperators.PATINDEX, builder.literal("%abc%"), builder.literal("abcdef"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(patindex, "A"))
        .build();

    final String expectedSql = "SELECT PATINDEX('%abc%', "
        + "'abcdef') A\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSql));
  }

  @Test public void testForRegexpSimilarFunctionWithThirdArgumentAsM() {
    final RelBuilder builder = relBuilder();
    final RexNode regexpSimilar =
        builder.call(
            SqlLibraryOperators.REGEXP_SIMILAR, builder.literal("MikeBira\n"
            + "aaa\n"
            + "bb\n"
            + "MikeBird"), builder.literal("^MikeBird$"), builder.literal("m"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(regexpSimilar, "A"))
        .build();

    final String expectedSparkSql = "SELECT IF('MikeBira\n"
        + "aaa\n"
        + "bb\n"
        + "MikeBird' rlike r'(?m)^MikeBird$', 1, 0)"
        + " A\nFROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkSql));
  }

  @Test public void testColumnListInWhereEquals() {
    final RelBuilder builder = relBuilder();
    final RelNode scalarQueryRel = builder.
        scan("EMP")
        .filter(builder.equals(builder.field("EMPNO"), builder.literal("100")))
        .project(
            builder.call(
            SqlStdOperatorTable.COLUMN_LIST, builder.field("EMPNO"), builder.field("HIREDATE")))
        .build();
    final RelNode root = builder
        .scan("EMP")
        .filter(
            builder.equals(
                builder.call(
                    SqlStdOperatorTable.COLUMN_LIST, builder.field("EMPNO"), builder.field("HIREDATE")),
            RexSubQuery.scalar(scalarQueryRel)))
        .build();

    final String expectedBigQuery = "SELECT *\n"
        + "FROM scott.EMP\n"
        + "WHERE (EMPNO, HIREDATE) = (SELECT (EMPNO, HIREDATE) AS `$f0`\n"
        + "FROM scott.EMP\n"
        + "WHERE EMPNO = '100')";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()),
        isLinux(expectedBigQuery));
  }


  @Test public void testNextDayFunctionWithDate() {
    final RelBuilder builder = relBuilder();
    final RexNode nextDayRexNode =
        builder.call(SqlLibraryOperators.NEXT_DAY, builder.literal("2023-02-22"),
                builder.literal(DayOfWeek.TUESDAY.name()));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(nextDayRexNode, "next_day"))
        .build();
    final String expectedSpark =
        "SELECT NEXT_DAY('2023-02-22', 'TUESDAY') next_day\n"
            + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
  }

  @Test public void testNextDayFunctionWithCurrentDate() {
    final RelBuilder builder = relBuilder();
    final RexNode nextDayRexNode =
        builder.call(SqlLibraryOperators.NEXT_DAY, builder.call(CURRENT_DATE),
                builder.literal(DayOfWeek.TUESDAY.name()));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(nextDayRexNode, "next_day"))
        .build();
    final String expectedSpark =
        "SELECT NEXT_DAY(CURRENT_DATE, 'TUESDAY') next_day\n"
            + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
  }

  @Test public void testNextDayFunctionWithTimestamp() {
    final RelBuilder builder = relBuilder();
    final RexNode nextDayRexNode =
        builder.call(SqlLibraryOperators.NEXT_DAY, builder.literal("2023-02-22 10:00:00"),
                builder.literal(DayOfWeek.TUESDAY.name()));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(nextDayRexNode, "next_day"))
        .build();
    final String expectedSpark =
        "SELECT NEXT_DAY('2023-02-22 10:00:00', 'TUESDAY') next_day\n"
            + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
  }

  @Test public void testNextDayFunctionWithCurrentTimestamp() {
    final RelBuilder builder = relBuilder();
    final RexNode nextDayRexNode =
        builder.call(SqlLibraryOperators.NEXT_DAY, builder.call(CURRENT_TIMESTAMP),
                builder.literal(DayOfWeek.TUESDAY.name()));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(nextDayRexNode, "next_day"))
        .build();
    final String expectedSpark =
        "SELECT NEXT_DAY(CURRENT_TIMESTAMP, 'TUESDAY') next_day\n"
            + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
  }

  @Test public void testNextDayFunctionWithSunday() {
    final RelBuilder builder = relBuilder();
    final RexNode nextDayRexNode =
        builder.call(SqlLibraryOperators.NEXT_DAY, builder.literal("2023-02-22"),
                builder.literal(DayOfWeek.SUNDAY.name()));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(nextDayRexNode, "next_day"))
        .build();
    final String expectedSpark =
        "SELECT NEXT_DAY('2023-02-22', 'SUNDAY') next_day\n"
            + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
  }

  @Test public void testNextDayFunctionWithMonday() {
    final RelBuilder builder = relBuilder();
    final RexNode nextDayRexNode =
        builder.call(SqlLibraryOperators.NEXT_DAY, builder.call(CURRENT_DATE),
                builder.literal(DayOfWeek.MONDAY.name()));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(nextDayRexNode, "next_day"))
        .build();
    final String expectedSpark =
        "SELECT NEXT_DAY(CURRENT_DATE, 'MONDAY') next_day\n"
            + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
  }

  @Test public void testNextDayFunctionWithWednesday() {
    final RelBuilder builder = relBuilder();
    final RexNode nextDayRexNode =
        builder.call(SqlLibraryOperators.NEXT_DAY, builder.literal("2023-02-23"),
                builder.literal(DayOfWeek.WEDNESDAY.name()));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(nextDayRexNode, "next_day"))
        .build();
    final String expectedSpark =
        "SELECT NEXT_DAY('2023-02-23', 'WEDNESDAY') next_day\n"
            + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
  }

  @Test public void testNextDayFunctionWithThursday() {
    final RelBuilder builder = relBuilder();
    final RexNode nextDayRexNode =
        builder.call(SqlLibraryOperators.NEXT_DAY, builder.call(CURRENT_DATE),
                builder.literal(DayOfWeek.THURSDAY.name()));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(nextDayRexNode, "next_day"))
        .build();
    final String expectedSpark =
        "SELECT NEXT_DAY(CURRENT_DATE, 'THURSDAY') next_day\n"
            + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
  }

  @Test public void testNextDayFunctionWithFriday() {
    final RelBuilder builder = relBuilder();
    final RexNode nextDayRexNode =
        builder.call(SqlLibraryOperators.NEXT_DAY, builder.call(CURRENT_DATE),
                builder.literal(DayOfWeek.FRIDAY.name()));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(nextDayRexNode, "next_day"))
        .build();
    final String expectedSpark =
        "SELECT NEXT_DAY(CURRENT_DATE, 'FRIDAY') next_day\n"
            + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
  }

  @Test public void testNextDayFunctionWithSaturday() {
    final RelBuilder builder = relBuilder();
    final RexNode nextDayRexNode =
        builder.call(SqlLibraryOperators.NEXT_DAY, builder.call(CURRENT_DATE),
                builder.literal(DayOfWeek.SATURDAY.name()));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(nextDayRexNode, "next_day"))
        .build();
    final String expectedSpark =
        "SELECT NEXT_DAY(CURRENT_DATE, 'SATURDAY') next_day\n"
            + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
  }

  @Test public void testStringAggFuncWithCollation() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RelBuilder.AggCall aggCall =
        builder.aggregateCall(SqlLibraryOperators.STRING_AGG, builder.field("ENAME"),
        builder.literal(";  ")).sort(builder.field("ENAME"), builder.field("HIREDATE"));
    final RelNode rel = builder
        .aggregate(relBuilder().groupKey(), aggCall)
        .build();

    final String expectedBigQuery = "SELECT STRING_AGG(ENAME, ';  ' ORDER BY ENAME IS NULL, ENAME,"
        + " HIREDATE IS NULL, HIREDATE) AS `$f0`\n"
        + "FROM scott.EMP";

    assertThat(toSql(rel, DatabaseProduct.BIG_QUERY.getDialect()),
        isLinux(expectedBigQuery));
  }


  @Test public void testCoalesceFunctionWithIntegerAndStringArgument() {
    final RelBuilder builder = relBuilder();

    final RexNode formatIntegerRexNode =
        builder.call(SqlLibraryOperators.FORMAT,
            builder.literal("'%11d'"), builder.scan("EMP").field(0));
    final RexNode formatCoalesceRexNode =
        builder.call(SqlStdOperatorTable.COALESCE,
            formatIntegerRexNode, builder.scan("EMP").field(1));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(formatCoalesceRexNode, "Name"))
        .build();

    final String expectedSparkQuery = "SELECT "
        + "COALESCE(STRING(EMPNO), ENAME) Name"
        + "\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testFormatFunction() {
    final RelBuilder builder = relBuilder();

    final RexNode formatIntegerRexNode =
        builder.call(SqlLibraryOperators.POSTGRESQL_FORMAT,
            builder.literal("Hello, %s"), builder.literal("John"));

    final RelNode root = builder
        .scan("EMP")
        .project(formatIntegerRexNode)
        .build();

    final String expectedPostgresQuery = "SELECT FORMAT('Hello, %s', 'John') AS \"$f0\"\n"
        + "FROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.POSTGRESQL.getDialect()), isLinux(expectedPostgresQuery));
  }

  @Test public void testCoalesceFunctionWithDecimalAndStringArgument() {
    final RelBuilder builder = relBuilder();

    final RexNode formatFloatRexNode =
        builder.call(SqlLibraryOperators.FORMAT,
            builder.literal("'%10.4f'"), builder.scan("EMP").field(5));
    final RexNode formatCoalesceRexNode =
        builder.call(SqlStdOperatorTable.COALESCE,
            formatFloatRexNode, builder.scan("EMP").field(1));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(formatCoalesceRexNode, "Name"))
        .build();

    final String expectedSparkQuery = "SELECT "
        + "COALESCE(STRING(SAL), ENAME) Name"
        + "\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testLiteralWithoutAliasInSelectForGroupBy() {
    final String query = "select 'testliteral' from"
        + " \"product\" group by 'testliteral'";
    final String expectedSql = "SELECT 'testliteral'\n"
        + "FROM foodmart.product\n"
        + "GROUP BY 'testliteral'";
    final String bigQueryExpected = "SELECT 'testliteral'\n"
        + "FROM foodmart.product\n"
        + "GROUP BY 1";
    final String expectedSpark = "SELECT 'testliteral'\n"
        + "FROM foodmart.product\n"
        + "GROUP BY 1";
    sql(query)
        .withHive()
        .ok(expectedSql)
        .withSpark()
        .ok(expectedSpark)
        .withBigQuery()
        .ok(bigQueryExpected);
  }

  @Test public void testBetween() {
    final RelBuilder builder = relBuilder();
    final RelNode root = builder
        .scan("EMP")
        .filter(
            builder.call(SqlLibraryOperators.BETWEEN,
                builder.field("EMPNO"), builder.literal(1), builder.literal(3)))
        .build();
    final String expectedBigQuery = "SELECT *\n"
        + "FROM scott.EMP\n"
        + "WHERE EMPNO BETWEEN 1 AND 3";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()),
        isLinux(expectedBigQuery));
  }

  @Test public void testNotBetween() {
    final RelBuilder builder = relBuilder();
    final RelNode root = builder
        .scan("EMP")
        .filter(
            builder.call(SqlLibraryOperators.NOT_BETWEEN,
                builder.field("EMPNO"), builder.literal(1), builder.literal(3)))
        .build();
    final String expectedBigQuery = "SELECT *\n"
        + "FROM scott.EMP\n"
        + "WHERE EMPNO NOT BETWEEN 1 AND 3";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()),
        isLinux(expectedBigQuery));
  }

  @Test void testBracesJoinConditionInClause() {
    RelBuilder builder = foodmartRelBuilder();
    builder = builder.scan("foodmart", "product");
    final RelNode root = builder
        .scan("foodmart", "sales_fact_1997")
        .join(
            JoinRelType.INNER, builder.call(IN,
              builder.field(2, 0, "product_id"),
              builder.field(2, 1, "product_id")))
        .project(builder.field("store_id"))
        .build();

    String expectedBigQuery = "SELECT sales_fact_1997.store_id\n"
        + "FROM foodmart.product\n"
        + "INNER JOIN foodmart.sales_fact_1997 ON product.product_id IN (sales_fact_1997.product_id)";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()),
        isLinux(expectedBigQuery));
  }

  @Test void testJoinWithUsingClause() {
    RelBuilder builder = foodmartRelBuilder();
    builder = builder.scan("foodmart", "product");
    final RelNode root = builder
        .scan("foodmart", "sales_fact_1997")
        .join(
            JoinRelType.INNER, builder.call(
                USING, builder.call(EQUALS,
                    builder.field(2, 0, "product_id"),
                    builder.field(2, 1, "product_id")))
        )
        .project(builder.field("store_id"))
        .build();

    String expectedBigQuery = "SELECT sales_fact_1997.store_id\n"
        + "FROM foodmart.product\n"
        + "INNER JOIN foodmart.sales_fact_1997 USING (product_id)";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()),
        isLinux(expectedBigQuery));
  }

  @Test public void testSnowflakeDateTrunc() {
    final RelBuilder builder = relBuilder();
    final RexNode dateTrunc =
        builder.call(SqlLibraryOperators.SNOWFLAKE_DATE_TRUNC, builder.literal("DAY"),
        builder.call(CURRENT_DATE));
    final RelNode root = builder
        .scan("EMP")
        .project(dateTrunc)
        .build();
    final String expectedSnowflakeSql = "SELECT DATE_TRUNC('DAY', CURRENT_DATE) AS \"$f0\"\n"
        + "FROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedSnowflakeSql));
  }

  @Test public void testBQDateTrunc() {
    final RelBuilder builder = relBuilder();
    final RexNode dateTrunc =
        builder.call(SqlLibraryOperators.DATE_TRUNC, builder.call(CURRENT_DATE),
        builder.literal(DAY));
    final RelNode root = builder
        .scan("EMP")
        .project(dateTrunc)
        .build();
    final String expectedBqSql = "SELECT DATE_TRUNC(CURRENT_DATE, DAY) AS `$f0`\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBqSql));
  }


  @Test public void testBracesForScalarSubQuery() {
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
                RexSubQuery.scalar(scalarQueryRel)).as("t"),
            builder.count(builder.literal(1)).as("pid"))
        .build();
    final String expectedBigQuery = "SELECT EMPNO, (SELECT DEPTNO\n"
        + "FROM scott.DEPT\n"
        + "WHERE DEPTNO = 40) AS t, COUNT(1) AS pid\n"
        + "FROM scott.EMP\n"
        + "GROUP BY EMPNO";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()),
        isLinux(expectedBigQuery));
  }

  @Test public void testSortByOrdinal() {
    RelBuilder builder = relBuilder();
    final RelNode root = builder
        .scan("EMP")
        .sort(builder.ordinal(0))
        .build();
    final String expectedBQSql = "SELECT *\n"
        + "FROM scott.EMP\n"
        + "ORDER BY 1 NULLS LAST";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQSql));
  }

  @Test public void testSortByOrdinalWithExprForBigQuery() {
    RelBuilder builder = relBuilder();
    final RexNode nextDayRexNode =
        builder.call(SqlLibraryOperators.NEXT_DAY, builder.call(CURRENT_DATE),
                builder.literal(DayOfWeek.SATURDAY.name()));
    RelNode root = builder
        .scan("EMP")
        .project(nextDayRexNode)
        .sort(builder.ordinal(0))
        .build();
    final String expectedBQSql =
        "SELECT NEXT_DAY(CURRENT_DATE, 'SATURDAY') AS `$f0`\n"
        + "FROM scott.EMP\n"
        + "ORDER BY 1 NULLS LAST";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQSql));
  }

  @Test public void testSubstr4() {
    RelBuilder builder = relBuilder().scan("EMP");
    final RexNode substr4Call =
        builder.call(SqlLibraryOperators.SUBSTR4, builder.field(0), builder.literal(1));
    RelNode root = builder
        .project(substr4Call)
        .build();
    final String expectedOracleSql = "SELECT SUBSTR4(\"EMPNO\", 1) \"$f0\"\nFROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.ORACLE.getDialect()), isLinux(expectedOracleSql));
  }

  @Test public void testToChar() {
    final RelBuilder builder = relBuilder();

    final RexNode toCharNode =
        builder.call(SqlLibraryOperators.TO_CHAR, builder.call(SqlStdOperatorTable.CURRENT_TIMESTAMP),
        builder.literal("MM-DD-YYYY HH24:MI:SS"));
    final RexNode toCharWithNumber =
        builder.call(SqlLibraryOperators.TO_CHAR, builder.literal(1000), builder.literal("9999"));
    final RelNode root = builder
        .scan("EMP")
        .project(toCharNode, toCharWithNumber)
        .build();
    final String expectedSparkQuery = "SELECT TO_CHAR(CURRENT_TIMESTAMP, 'MM-DD-YYYY HH24:MI:SS')"
        + " \"$f0\", TO_CHAR(1000, '9999') \"$f1\"\n"
        + "FROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.ORACLE.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testToCharInSpark() {
    final RelBuilder builder = relBuilder();

    final RexNode toCharNodeMonthFormat =
        builder.call(SqlLibraryOperators.TO_CHAR, builder.call(SqlStdOperatorTable.CURRENT_TIMESTAMP),
        builder.literal(SqlDateTimeFormat.MONTH_NAME.value));
    final RexNode toCharNodeHourFormat =
        builder.call(SqlLibraryOperators.TO_CHAR, builder.call(SqlStdOperatorTable.CURRENT_TIMESTAMP),
        builder.literal(SqlDateTimeFormat.HOUR_OF_DAY_12.value));
    final RelNode root = builder
        .scan("EMP")
        .project(toCharNodeMonthFormat, builder.alias(toCharNodeHourFormat, "hour"))
        .build();
    final String expectedSparkQuery = "SELECT DATE_FORMAT(CURRENT_TIMESTAMP, 'MMMM') $f0, "
        + "DATE_FORMAT(CURRENT_TIMESTAMP, 'hh') hour\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testIsoFormats() {
    final RelBuilder builder = relBuilder();

    final RexNode toCharNodeIsoFormat =
        builder.call(SqlLibraryOperators.FORMAT_DATE,
            builder.literal(SqlDateTimeFormat.FOURDIGITISOYEAR.value + "@" + SqlDateTimeFormat.ISOWEEK.value),
            builder.call(CURRENT_DATE));
    final RelNode root = builder
        .scan("EMP")
        .project(toCharNodeIsoFormat)
        .build();
    final String expectedBQQuery = "SELECT FORMAT_DATE('%G%V', CURRENT_DATE) AS `$f0`\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQQuery));
  }

  @Test public void testToDateforOracle() {
    RelBuilder builder = relBuilder().scan("EMP");
    final RexNode oracleToDateCall =
        builder.call(SqlLibraryOperators.ORACLE_TO_DATE, builder.call(CURRENT_DATE));
    RelNode root = builder
        .project(oracleToDateCall)
        .build();
    final String expectedOracleSql = "SELECT TO_DATE(CURRENT_DATE) \"$f0\"\nFROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.ORACLE.getDialect()), isLinux(expectedOracleSql));
  }

  @Test public void testMONDateFormatforOracle() {
    RelBuilder builder = relBuilder().scan("EMP");
    final RexNode oracleToDateCall =
        builder.call(SqlLibraryOperators.PARSE_DATETIME, builder.literal("DDMON-YYYY"), builder.literal("23FEB-2021"));
    RelNode root = builder
        .project(oracleToDateCall)
        .build();
    final String expectedBQSql = "SELECT PARSE_DATETIME('%d%b-%Y', '23FEB-2021') AS `$f0`"
        + "\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQSql));
  }

  @Test public void testTranslateWithLiteralParameter() {
    RelBuilder builder = relBuilder().scan("EMP");
    final RexNode rexNode =
        builder.call(SqlLibraryOperators.TRANSLATE, builder.literal("scott"), builder.literal("t"),
                builder.literal("a"));
    RelNode root = builder
        .project(rexNode)
        .build();
    final String expectedBQSql = "SELECT TRANSLATE('scott', 't', 'a') AS `$f0`"
        + "\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQSql));
  }

  @Test public void testTranslateWithNumberParameter() {
    RelBuilder builder = relBuilder().scan("EMP");
    final RexNode rexNode =
        builder.call(SqlLibraryOperators.TRANSLATE, builder.literal("12.345.6789~10~"), builder.literal("~."),
        builder.literal(""));
    RelNode root = builder
        .project(rexNode)
        .build();
    final String expectedBQSql = "SELECT TRANSLATE('12.345.6789~10~', '~.', '') AS `$f0`"
        + "\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQSql));
  }

  @Test public void testForLikeSomeOperator() {
    final RelBuilder builder = relBuilder();
    final RelNode subQuery = builder
        .scan("EMP")
        .project(builder.field(0), builder.field(2))
        .build();
    final RexNode anyCondition =
        RexSubQuery.some(
            subQuery, ImmutableList.of(builder.literal(1),
                builder.literal(2)),
        SqlLibraryOperators.SOME_LIKE);
    final RelNode root = builder
        .scan("EMP")
        .filter(anyCondition)
        .project(builder.field(1))
        .build();

    final String expectedSql = "SELECT \"ENAME\""
        + "\nFROM \"scott\".\"EMP\""
        + "\nWHERE (1, 2) ILIKE SOME (SELECT \"EMPNO\", \"JOB\""
        + "\nFROM \"scott\".\"EMP\")";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
  }

  @Test public void testRowsInOverClauseWhenUnboudedPrecedingAndFollowing() {
    RelBuilder builder = relBuilder().scan("EMP");
    RexNode aggregateFunRexNode = builder.call(SqlStdOperatorTable.MAX, builder.field(0));
    RelDataType type = aggregateFunRexNode.getType();
    RexFieldCollation orderKeys =
        new RexFieldCollation(builder.field("HIREDATE"),
        ImmutableSet.of());
    final RexNode analyticalFunCall =
        builder.getRexBuilder().makeOver(type, SqlStdOperatorTable.MAX,
        ImmutableList.of(builder.field(0)), ImmutableList.of(), ImmutableList.of(orderKeys),
        RexWindowBounds.UNBOUNDED_PRECEDING,
        RexWindowBounds.UNBOUNDED_FOLLOWING,
        true, true, false, false, false);
    RelNode root = builder
        .project(analyticalFunCall)
        .build();
    final String expectedOracleSql = "SELECT MAX(\"EMPNO\") OVER (ORDER BY \"HIREDATE\" "
        + "RANGE BETWEEN "
        + "UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) \"$f0\"\n"
        + "FROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.ORACLE.getDialect()), isLinux(expectedOracleSql));
  }

  @Test public void testOracleTrunc() {
    RelBuilder builder = relBuilder().scan("EMP");
    final RexNode dateTruncNode =
        builder.call(SqlLibraryOperators.TRUNC_ORACLE, builder.call(CURRENT_TIMESTAMP),
        builder.literal("YYYY"));
    RelNode root = builder
        .project(dateTruncNode)
        .build();
    final String expectedOracleSql =
        "SELECT TRUNC(CURRENT_TIMESTAMP, 'YYYY') \"$f0\"\n"
         + "FROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.ORACLE.getDialect()), isLinux(expectedOracleSql));
  }

  @Test public void testForTruncTimestampFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode truncTimestampNode =
        builder.call(SqlLibraryOperators.TRUNC_TIMESTAMP, builder.call(CURRENT_TIMESTAMP),
        builder.literal("Year"));
    final RelNode root = builder
        .scan("EMP")
        .project(truncTimestampNode)
        .build();
    final String expectedDB2Sql = "SELECT TRUNC_TIMESTAMP(CURRENT_TIMESTAMP, 'Year') AS $f0\nFROM"
        + " scott.EMP AS EMP";

    assertThat(toSql(root, DatabaseProduct.DB2.getDialect()), isLinux(expectedDB2Sql));
  }

  @Test public void testAddMonths() {
    RelBuilder relBuilder = relBuilder().scan("EMP");
    RexBuilder rexBuilder = relBuilder.getRexBuilder();
    final RexLiteral intervalLiteral =
        rexBuilder.makeIntervalLiteral(BigDecimal.valueOf(-2),
                new SqlIntervalQualifier(MONTH, null, SqlParserPos.ZERO));
    final RexNode oracleAddMonthsCall =
        relBuilder.call(new SqlAddMonths(true),
                relBuilder.call(SqlStdOperatorTable.CURRENT_TIMESTAMP), intervalLiteral);
    RelNode root = relBuilder
        .project(oracleAddMonthsCall)
        .build();
    final String expectedOracleSql = "SELECT "
        + "ADD_MONTHS(CURRENT_TIMESTAMP, INTERVAL -'2' MONTH) \"$f0\""
        + "\nFROM \"scott\".\"EMP\"";

    final String expectedBQSql = "SELECT "
        + "DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -2 MONTH) AS `$f0`"
        + "\nFROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.ORACLE.getDialect()), isLinux(expectedOracleSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQSql));
  }

  @Test public void testCurrentTimestampWithTimeZone() {
    final RelBuilder builder = relBuilder().scan("EMP");
    RelDataType relDataType =
        builder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP_WITH_TIME_ZONE);
    final RexNode currentTimestampRexNode =
        builder.getRexBuilder().makeCall(
            relDataType, CURRENT_TIMESTAMP_WITH_TIME_ZONE,
            Collections.singletonList(builder.literal(6)));
    RelNode root = builder
        .project(currentTimestampRexNode)
        .build();

    final String expectedBQSql = "SELECT CURRENT_TIMESTAMP() AS `$f0`\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQSql));
  }

  @Test public void testCurrentTimestampWithLocalTimeZone() {
    final RelBuilder builder = relBuilder().scan("EMP");
    RelDataType relDataType =
        builder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    final RexNode currentTimestampRexNode =
        builder.getRexBuilder().makeCall(relDataType,
            SqlLibraryOperators.CURRENT_TIMESTAMP_WITH_LOCAL_TIME_ZONE,
            Collections.singletonList(builder.literal(6)));
    RelNode root = builder
        .project(currentTimestampRexNode)
        .build();

    final String expectedBQSql = "SELECT CURRENT_TIMESTAMP() AS `$f0`\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQSql));
  }

  @Test public void testMonthsBetween() {
    RelBuilder builder = relBuilder().scan("EMP");
    final RexNode dateTruncNode =
        builder.call(SqlLibraryOperators.MONTHS_BETWEEN, builder.call(CURRENT_TIMESTAMP),
        builder.call(CURRENT_TIMESTAMP));
    RelNode root = builder
        .project(dateTruncNode)
        .build();
    final String expectedOracleSql =
        "SELECT MONTHS_BETWEEN(CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) \"$f0\"\n"
            + "FROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.ORACLE.getDialect()), isLinux(expectedOracleSql));
  }

  @Test public void testArithmeticOnTimestamp() {
    RelBuilder relBuilder = relBuilder().scan("EMP");
    RexBuilder rexBuilder = relBuilder.getRexBuilder();
    final RexLiteral intervalLiteral =
        rexBuilder.makeIntervalLiteral(BigDecimal.valueOf(2), new SqlIntervalQualifier(MONTH, null, SqlParserPos.ZERO));
    final RexNode oracleMinusTimestampCall =
        relBuilder.call(SqlStdOperatorTable.MINUS, relBuilder.call(SqlStdOperatorTable.CURRENT_TIMESTAMP),
                intervalLiteral);
    RelNode root = relBuilder
        .project(oracleMinusTimestampCall)
        .build();

    final String expectedBQSql = "SELECT DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 2 MONTH) AS "
        + "`$f0`\nFROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQSql));
  }

  @Test public void testConvertUDFFunctionCreation() {
    RelBuilder builder = relBuilder();
    RexLiteral textToConvert = builder.literal("Texto en Español");
    RexLiteral targetCharset = builder.literal("US7ASCII");
    RexLiteral sourceCharset = builder.literal("WE8ISO8859P1");
    final RexNode udfFunction =
        builder.call(
            SqlLibraryOperators.createUDFSqlFunction("convert_string_udf",
                ReturnTypes.VARCHAR_2000),
            textToConvert, targetCharset, sourceCharset);
    RelNode root = builder
        .scan("EMP")
        .project(udfFunction)
        .build();
    final String expectedSql = "SELECT "
        + "CONVERT_STRING_UDF('Texto en Español', 'US7ASCII', 'WE8ISO8859P1') $f0"
        + "\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSql));
  }

  @Test public void testCastWithFormat() {
    RelBuilder builder = relBuilder().scan("EMP");
    final RexBuilder rexBuilder = builder.getRexBuilder();
    RexLiteral format = builder.literal("9999.9999");
    final RelDataType varcharRelType = builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
    final RelDataType type =
        BasicSqlTypeWithFormat.from(RelDataTypeSystem.DEFAULT, (BasicSqlType) varcharRelType,
        format.getValueAs(String.class));
    final RexNode castCall = rexBuilder.makeCast(type, builder.literal(1234), false);
    RelNode root = builder
        .project(castCall)
        .build();
    final String expectedBQSql = "SELECT CAST(1234 AS STRING FORMAT '9999.9999') AS `$f0`\n"
                                     + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQSql));
  }

  @Test public void testTryCast() {
    RelBuilder builder = relBuilder().scan("EMP");
    final RexBuilder rexBuilder = builder.getRexBuilder();
    final RelDataType varcharRelType = builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
    final RelDataType decimalType = builder.getTypeFactory().createSqlType(SqlTypeName.DECIMAL, 5, 2);
    final RexNode castCall =
        rexBuilder.makeCast(varcharRelType, builder.field(0), false, true);
    final RexNode castLiteral =
        rexBuilder.makeAbstractCast(decimalType, builder.literal(123.456), true);
    RelNode root = builder
        .project(castCall, castLiteral)
        .build();
    final String expectedSql = "SELECT TRY_CAST(EMPNO AS STRING) $f0, CAST(123.456 AS DECIMAL(5, 2)) $f1\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSql));
  }

  @Test public void testOracleToTimestamp() {
    RelBuilder builder = relBuilder().scan("EMP");
    final RexNode toTimestampNode =
        builder.call(SqlLibraryOperators.ORACLE_TO_TIMESTAMP, builder.literal("January 15, 1989, 11:00:06 AM"),
        builder.literal("MONTH DD, YYYY, hh:mi:ss AM"));
    final RexNode toTimestampNodeWithOnlyLiteral =
        builder.call(SqlLibraryOperators.ORACLE_TO_TIMESTAMP,
        builder.literal("04-JAN-2001"));
    RelNode root = builder
        .project(toTimestampNode, toTimestampNodeWithOnlyLiteral)
        .build();
    final String expectedOracleSql = "SELECT TO_TIMESTAMP('January 15, 1989, 11:00:06 AM', 'MONTH"
        + " DD, YYYY, hh:mi:ss AM') \"$f0\", TO_TIMESTAMP('04-JAN-2001') \"$f1\"\n"
        + "FROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.ORACLE.getDialect()), isLinux(expectedOracleSql));
  }

  @Test public void testOracleLastDay() {
    RelBuilder relBuilder = relBuilder().scan("EMP");
    final RexNode literalTimestamp = relBuilder.call(SqlStdOperatorTable.CURRENT_TIMESTAMP);
    RexNode lastDayNode = relBuilder.call(SqlLibraryOperators.LAST_DAY, literalTimestamp);
    RelNode root = relBuilder
        .project(lastDayNode)
        .build();
    final String expectedOracleSql = "SELECT LAST_DAY(CURRENT_TIMESTAMP) \"$f0\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedDB2Sql = "SELECT LAST_DAY(CURRENT_TIMESTAMP) AS $f0\n"
        + "FROM scott.EMP AS EMP";

    assertThat(toSql(root, DatabaseProduct.ORACLE.getDialect()), isLinux(expectedOracleSql));
    assertThat(toSql(root, DatabaseProduct.DB2.getDialect()), isLinux(expectedDB2Sql));
  }

  @Test public void testSnowflakeLastDay() {
    RelBuilder relBuilder = relBuilder().scan("EMP");
    RexNode lastDayNode =
        relBuilder.call(SqlLibraryOperators.SNOWFLAKE_LAST_DAY, relBuilder.literal("13-JAN-1999"));
    RexNode lastDayWithDatePartNode =
        relBuilder.call(SqlLibraryOperators.SNOWFLAKE_LAST_DAY, relBuilder.literal("13-JAN-1999"),
        relBuilder.literal("YEAR"));

    RelNode root = relBuilder
        .project(lastDayWithDatePartNode, lastDayNode)
        .build();
    final String expectedSnowflakeSql = "SELECT LAST_DAY('13-JAN-1999', 'YEAR') AS \"$f0\", "
        + "LAST_DAY('13-JAN-1999') AS \"$f1\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBQSql = "SELECT LAST_DAY('13-JAN-1999', YEAR) AS `$f0`, "
        + "LAST_DAY('13-JAN-1999') AS `$f1`\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedSnowflakeSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQSql));
  }
  @Test public void testOracleRoundFunction() {
    RelBuilder relBuilder = relBuilder().scan("EMP");
    final RexNode literalTimestamp = relBuilder.call(SqlStdOperatorTable.CURRENT_TIMESTAMP);
    final RexNode formatNode = relBuilder.literal("DAY");
    RexNode roundNode =
        relBuilder.call(SqlLibraryOperators.ORACLE_ROUND, literalTimestamp,
        formatNode);
    RelNode root = relBuilder
        .project(roundNode)
        .build();
    final String expectedOracleSql = "SELECT ROUND(CURRENT_TIMESTAMP, 'DAY') \"$f0\"\n"
        + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.ORACLE.getDialect()), isLinux(expectedOracleSql));
  }

  @Test public void testOracleToNumber() {
    RelBuilder relBuilder = relBuilder().scan("EMP");
    RexNode toNumberNode =
        relBuilder.call(SqlLibraryOperators.ORACLE_TO_NUMBER, relBuilder.literal("1.789"),
        relBuilder.literal("9D999"));
    RelNode root = relBuilder
        .project(toNumberNode)
        .build();
    final String expectedOracleSql = "SELECT TO_NUMBER('1.789', '9D999') \"$f0\"\n"
        + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.ORACLE.getDialect()), isLinux(expectedOracleSql));
  }

  @Test public void testOracleNextDayFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode nextDayRexNode =
        builder.call(SqlLibraryOperators.ORACLE_NEXT_DAY, builder.call(CURRENT_DATE),
                builder.literal(DayOfWeek.SATURDAY.name()));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(nextDayRexNode, "next_day"))
        .build();
    final String expectedOracle = "SELECT ORACLE_NEXT_DAY(CURRENT_DATE, 'SATURDAY') \"next_day\"\n"
        + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.ORACLE.getDialect()), isLinux(expectedOracle));
  }

  @Test public void testForGetBitFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode getBitRexNode =
        builder.call(SqlLibraryOperators.GETBIT, builder.literal(8), builder.literal(3));
    final RelNode root = builder
        .values(new String[]{""}, 1)
        .project(builder.alias(getBitRexNode, "aa"))
        .build();

    final String expectedBQ = "SELECT (8 >> 3 & 1) AS aa";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQ));
  }

  @Test public void testGetBitFunctionWithNullArgument() {
    final RelBuilder builder = relBuilder();
    final RexNode getBitRexNode =
        builder.call(SqlLibraryOperators.GETBIT, builder.literal(8), builder.literal(null));
    final RelNode root = builder
        .values(new String[]{""}, 1)
        .project(builder.alias(getBitRexNode, "aa"))
        .build();

    final String expectedBQ = "SELECT (8 >> NULL & 1) AS aa";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQ));
  }

  @Test public void testGetBitFunctionWithColumnValue() {
    final RelBuilder builder = relBuilder();
    final RexNode getBitRexNode =
        builder.call(SqlLibraryOperators.GETBIT, builder.literal(8),
        builder.scan("EMP").field(0));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(getBitRexNode, "aa"))
        .build();

    final String expectedBQ = "SELECT (8 >> EMPNO & 1) AS aa\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQ));
  }
  @Test public void testShiftLeft() {
    final RelBuilder builder = relBuilder();
    final RexNode shiftLeftRexNode =
        builder.call(SqlLibraryOperators.SHIFTLEFT, builder.literal(3), builder.literal(2));
    final RelNode root = builder
        .values(new String[] {""}, 1)
        .project(builder.alias(shiftLeftRexNode, "FD"))
        .build();
    final String expectedBigQuery = "SELECT (3 << 2) AS FD";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBigQuery));
  }

  @Test public void testShiftLeftWithNullInSecondArgument() {
    final RelBuilder builder = relBuilder();
    final RexNode shiftLeftRexNode =
        builder.call(SqlLibraryOperators.SHIFTLEFT, builder.literal(3), builder.literal(null));
    final RelNode root = builder
        .values(new String[] {""}, 1)
        .project(builder.alias(shiftLeftRexNode, "FD"))
        .build();
    final String expectedBigQuery = "SELECT (3 << NULL) AS FD";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBigQuery));
  }

  @Test public void testBitNot() {
    final RelBuilder builder = relBuilder();
    final RexNode bitNotRexNode = builder.call(BITNOT, builder.literal(10));
    final RelNode root = builder
            .values(new String[]{""}, 1)
            .project(builder.alias(bitNotRexNode, "bit_not"))
            .build();
    final String expectedBigQuery = "SELECT ~ (10) AS bit_not";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBigQuery));
  }

  @Test public void testBitNotForDb2() {
    final RelBuilder builder = relBuilder();
    final RexNode bitNotRexNode = builder.call(BITNOT, builder.literal(123.45));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(bitNotRexNode, "bit_not"))
        .build();
    final String expectedDB2Sql = "SELECT BITNOT(123.45) AS bit_not\n"
        + "FROM scott.EMP AS EMP";
    assertThat(toSql(root, DatabaseProduct.DB2.getDialect()), isLinux(expectedDB2Sql));
  }

  @Test public void testBitNotWithTableColumn() {
    final RelBuilder builder = relBuilder();
    final RexNode bitNotRexNode = builder.call(BITNOT, builder.scan("EMP").field(5));
    final RelNode root = builder
            .scan("EMP")
            .project(builder.alias(bitNotRexNode, "bit_not"))
            .build();
    final String expectedSparkQuery = "SELECT ~ (SAL) AS bit_not\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedSparkQuery));
  }

  @Test public void testShiftRight() {
    final RelBuilder builder = relBuilder();
    final RexNode shiftRightRexNode =
        builder.call(SqlLibraryOperators.SHIFTRIGHT, builder.literal(3), builder.literal(2));
    final RelNode root = builder
        .values(new String[] {""}, 1)
        .project(builder.alias(shiftRightRexNode, "FD"))
        .build();
    final String expectedBigQuery = "SELECT (3 >> 2) AS FD";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBigQuery));
  }

  @Test public void testShiftRightWithNegativeValueInSecondArgument() {
    final RelBuilder builder = relBuilder();
    final RexNode shiftRightRexNode =
        builder.call(SqlLibraryOperators.SHIFTRIGHT, builder.literal(3),
                builder.call(SqlStdOperatorTable.UNARY_MINUS, builder.literal(1)));
    final RelNode root = builder
        .values(new String[] {""}, 1)
        .project(builder.alias(shiftRightRexNode, "a"))
        .build();
    final String expectedBigQuery = "SELECT (3 << 1) AS a";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBigQuery));
  }

  @Test public void testShiftLeftWithNegativeValueInSecondArgument() {
    final RelBuilder builder = relBuilder();
    final RexNode shiftLeftRexNode =
        builder.call(SqlLibraryOperators.SHIFTLEFT, builder.literal(3),
                builder.call(SqlStdOperatorTable.UNARY_MINUS, builder.literal(1)));
    final RelNode root = builder
        .values(new String[] {""}, 1)
        .project(builder.alias(shiftLeftRexNode, "a"))
        .build();
    final String expectedBigQuery = "SELECT (3 >> 1) AS a";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBigQuery));
  }

  @Test public void testTryToDateFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode tryToDateNode0 =
        builder.call(SqlLibraryOperators.TRY_TO_DATE, builder.literal("2013-12-05 01:02:03"),
                builder.literal("YYYY-MM-DD HH24:MI:SS"));
    final RexNode tryToDateNode1 =
        builder.call(SqlLibraryOperators.TRY_TO_DATE, builder.literal("2013-12-05"),
                builder.literal("YYYY-MM-DD"));
    final RexNode tryToDateNode2 =
        builder.call(SqlLibraryOperators.TRY_TO_DATE, builder.literal("invalid"));
    final RelNode root = builder
        .scan("EMP")
        .project(
            builder.alias(tryToDateNode0, "date_value0"),
            builder.alias(tryToDateNode1, "date_value1"),
            builder.alias(tryToDateNode2, "date_value2"))
        .build();
    final String expectedSql =
        "SELECT TRY_TO_DATE('2013-12-05 01:02:03', 'YYYY-MM-DD HH24:MI:SS') AS "
            + "\"date_value0\", TRY_TO_DATE('2013-12-05', 'YYYY-MM-DD') AS \"date_value1\", "
            + "TRY_TO_DATE('invalid') AS \"date_value2\"\n"
            + "FROM \"scott\".\"EMP\"";
    final String snowflakeSql =
        "SELECT TRY_TO_DATE('2013-12-05 01:02:03', 'YYYY-MM-DD HH24:MI:SS') AS "
            + "\"date_value0\", TRY_TO_DATE('2013-12-05', 'YYYY-MM-DD') AS \"date_value1\", "
            + "TRY_TO_DATE('invalid') AS \"date_value2\"\n"
            + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(snowflakeSql));
  }

  @Test public void testTryToTimestampFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode tryToTimestampNode =
        builder.call(SqlLibraryOperators.TRY_TO_TIMESTAMP, builder.literal("2013-12-05 01:02:03"),
                builder.literal("YYYY-MM-DD HH24:MI:SS"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(tryToTimestampNode, "timestamp_value"))
        .build();
    final String expectedSql =
        "SELECT TRY_TO_TIMESTAMP('2013-12-05 01:02:03', 'YYYY-MM-DD HH24:MI:SS') AS "
            + "\"timestamp_value\"\nFROM \"scott\".\"EMP\"";
    final String snowflakeSql =
        "SELECT TRY_TO_TIMESTAMP('2013-12-05 01:02:03', 'YYYY-MM-DD HH24:MI:SS') AS "
            + "\"timestamp_value\"\nFROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(snowflakeSql));
  }

  @Test public void testTryToTimeFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode tryToTimeNode =
        builder.call(SqlLibraryOperators.TRY_TO_TIME, builder.literal("01:02:03"));
    final RexNode tryToTimeNodeWithFormat =
        builder.call(SqlLibraryOperators.TRY_TO_TIME, builder.literal("01:02:03"), builder.literal("HH24:MI:SS"));
    final RexNode tryToTimeNodeWithInvalidFormat =
        builder.call(SqlLibraryOperators.TRY_TO_TIME, builder.literal("invalid"), builder.literal("HH24:MI:SS"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(tryToTimeNode, "time_value"),
            tryToTimeNodeWithFormat, tryToTimeNodeWithInvalidFormat)
        .build();

    final String snowflakeSql =
        "SELECT TRY_TO_TIME('01:02:03') AS \"time_value\", TRY_TO_TIME('01:02:03', "
           +  "'HH24:MI:SS') AS \"$f1\", TRY_TO_TIME('invalid', 'HH24:MI:SS') AS \"$f2\"\n"
           + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(snowflakeSql));
  }

  @Test public void testCountSetWithLiteralParameter() {
    RelBuilder builder = relBuilder();
    final RexNode bitCountRexNode =
        builder.call(SqlLibraryOperators.BIT_COUNT, builder.literal(7));
    RelNode root = builder.values(new String[]{""}, 1)
        .project(builder.alias(bitCountRexNode, "number"))
        .build();
    final String expectedBQSql = "SELECT BIT_COUNT(7) AS number";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQSql));
  }

  @Test public void testCountSetWithFieldParameter() {
    RelBuilder builder = relBuilder().scan("EMP");
    final RexNode bitCountRexNode =
        builder.call(SqlLibraryOperators.BIT_COUNT, builder.field(0));
    RelNode root = builder
        .project(builder.alias(bitCountRexNode, "emp_no"))
        .build();
    final String expectedBQSql = "SELECT BIT_COUNT(EMPNO) AS emp_no"
        + "\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQSql));
  }

  @Test public void testForToJsonStringFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode toJsonStr =
            builder.call(SqlLibraryOperators.TO_JSON_STRING, builder.scan("EMP").field(5));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(toJsonStr, "value"))
        .build();

    final String expectedBiqQuery = "SELECT TO_JSON_STRING(SAL) AS value\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testForToJsonFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode toJson =
        builder.call(SqlLibraryOperators.TO_JSON, builder.scan("EMP").field(5));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(toJson, "value"))
        .build();

    final String expectedBiqQuery = "SELECT TO_JSON(SAL) AS value\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testForJsonAggFunction() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RelBuilder.AggCall aggCall =
        builder.aggregateCall(SqlLibraryOperators.JSON_AGG,
            builder.field(0), builder.field(1));
    final RelNode rel = builder
        .aggregate(relBuilder().groupKey(), aggCall)
        .build();
    final String expectedTDQuery = "SELECT JSON_AGG(\"EMPNO\", \"ENAME\") AS \"$f0\"\n"
        + "FROM \"scott\".\"EMP\"";
    assertThat(toSql(rel, DatabaseProduct.TERADATA.getDialect()), isLinux(expectedTDQuery));
  }

  @Test public void testForJsonComposeFunction() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RexNode jsonCompose =
        builder.call(SqlLibraryOperators.JSON_COMPOSE,
            builder.field(0), builder.field(1));
    final RelNode root = builder
        .project(builder.alias(jsonCompose, "value"))
        .build();

    final String expectedTDQuery = "SELECT JSON_COMPOSE(\"EMPNO\", \"ENAME\") AS \"value\"\n"
        + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.TERADATA.getDialect()), isLinux(expectedTDQuery));
  }

  @Test void testBloatedProjects() {
    final RelBuilder builder = relBuilder();

    RexNode rex = builder.literal(2);
    RexNode rex2 = builder.literal(20);
    builder.scan("EMP")
        .project(
            getExtendedRexList(builder.peek(), builder.alias(rex, "f9"),
            builder.alias(rex2, "f10"),
                builder.alias(makeCaseCall(builder, 0, 0), "f11"),
            builder.alias(makeCaseCall(builder, 0, 1), "f12"),
            builder.alias(makeCaseCall(builder, 0, 2), "f13"),
            builder.alias(makeCaseCall(builder, 0, 3), "f14"),
            builder.alias(makeCaseCall(builder, 0, 4), "f15"),
            builder.alias(makeCaseCall(builder, 0, 5), "f16"),
            builder.alias(makeCaseCall(builder, 0, 6), "f17"),
            builder.alias(makeCaseCall(builder, 0, 7), "f18"),
            builder.alias(makeCaseCall(builder, 0, 8), "f19"),
            builder.alias(makeCaseCall(builder, 0, 9), "f20"),
            builder.alias(makeCaseCall(builder, 0, 10), "f21")));

    builder.project(
        getExtendedRexList(builder.peek(),
        builder.alias(
            builder.getRexBuilder().makeCall(SqlStdOperatorTable.CASE,
            builder.equals(builder.field(0), builder.literal(0)),
            builder.field(10), rex2), "f111"),
            builder.alias(makeCaseCall(builder, 10, 11), "f112"),
            builder.alias(makeCaseCall(builder, 11, 12), "f113"),
            builder.alias(makeCaseCall(builder, 12, 13), "f114"),
            builder.alias(makeCaseCall(builder, 13, 14), "f115"),
            builder.alias(makeCaseCall(builder, 14, 15), "f116"),
            builder.alias(makeCaseCall(builder, 15, 16), "f117"),
            builder.alias(makeCaseCall(builder, 16, 17), "f118"),
            builder.alias(makeCaseCall(builder, 17, 18), "f119"),
            builder.alias(makeCaseCall(builder, 18, 19), "f120"),
            builder.alias(makeCaseCall(builder, 19, 20), "f121")));

    builder.project(
        getExtendedRexList(builder.peek(),
        builder.alias(
            builder.getRexBuilder().makeCall(SqlStdOperatorTable.CASE,
            builder.equals(builder.field(0), builder.literal(0)),
            builder.field(10), rex2), "f111"),
        makeCaseCall(builder, 11, 121),
        makeCaseCall(builder, 12, 123),
        makeCaseCall(builder, 13, 113),
        makeCaseCall(builder, 14, 142),
        makeCaseCall(builder, 15, 115),
        makeCaseCall(builder, 16, 126),
        makeCaseCall(builder, 17, 1237),
        makeCaseCall(builder, 18, 1228),
        makeCaseCall(builder, 19, 119),
        makeCaseCall(builder, 20, 1192),
        makeCaseCall(builder, 21, 1193),
        makeCaseCall(builder, 23, 1194),
        makeCaseCall(builder, 24, 1195),
        makeCaseCall(builder, 25, 1194),
        makeCaseCall(builder, 26, 1196),
        makeCaseCall(builder, 27, 1179),
        makeCaseCall(builder, 28, 11923),
        makeCaseCall(builder, 29, 11239),
        makeCaseCall(builder, 30, 11419),
        makeCaseCall(builder, 31, 2000)));

    final RelNode root = builder.build();

    assert root instanceof Project && root.getInput(0) instanceof Project;

    final String expectedSql = "SELECT \"EMPNO\", \"ENAME\", \"JOB\", \"MGR\", \"HIREDATE\", "
        + "\"SAL\", \"COMM\", \"DEPTNO\", \"f9\", \"f10\", \"f11\", \"f12\", \"f13\", \"f14\", "
        + "\"f15\", \"f16\", \"f17\", \"f18\", \"f19\", \"f20\", \"f21\", \"f111\", \"f112\", "
        + "\"f113\", \"f114\", \"f115\", \"f116\", \"f117\", \"f118\", \"f119\", \"f120\", "
        + "\"f121\", CASE WHEN \"EMPNO\" = 0 THEN \"f11\" ELSE 20 END AS \"f1110\", "
        + "CASE WHEN \"f12\" = 121 THEN 121 ELSE 1210 END AS \"$f33\", "
        + "CASE WHEN \"f13\" = 123 THEN 123 ELSE 1230 END AS \"$f34\", "
        + "CASE WHEN \"f14\" = 113 THEN 113 ELSE 1130 END AS \"$f35\", "
        + "CASE WHEN \"f15\" = 142 THEN 142 ELSE 1420 END AS \"$f36\", "
        + "CASE WHEN \"f16\" = 115 THEN 115 ELSE 1150 END AS \"$f37\", "
        + "CASE WHEN \"f17\" = 126 THEN 126 ELSE 1260 END AS \"$f38\", "
        + "CASE WHEN \"f18\" = 1237 THEN 1237 ELSE 12370 END AS \"$f39\", "
        + "CASE WHEN \"f19\" = 1228 THEN 1228 ELSE 12280 END AS \"$f40\", "
        + "CASE WHEN \"f20\" = 119 THEN 119 ELSE 1190 END AS \"$f41\", "
        + "CASE WHEN \"f21\" = 1192 THEN 1192 ELSE 11920 END AS \"$f42\", "
        + "CASE WHEN \"f111\" = 1193 THEN 1193 ELSE 11930 END AS \"$f43\", "
        + "CASE WHEN \"f113\" = 1194 THEN 1194 ELSE 11940 END AS \"$f44\", "
        + "CASE WHEN \"f114\" = 1195 THEN 1195 ELSE 11950 END AS \"$f45\", "
        + "CASE WHEN \"f115\" = 1194 THEN 1194 ELSE 11940 END AS \"$f46\", "
        + "CASE WHEN \"f116\" = 1196 THEN 1196 ELSE 11960 END AS \"$f47\", "
        + "CASE WHEN \"f117\" = 1179 THEN 1179 ELSE 11790 END AS \"$f48\", "
        + "CASE WHEN \"f118\" = 11923 THEN 11923 ELSE 119230 END AS \"$f49\", "
        + "CASE WHEN \"f119\" = 11239 THEN 11239 ELSE 112390 END AS \"$f50\", "
        + "CASE WHEN \"f120\" = 11419 THEN 11419 ELSE 114190 END AS \"$f51\", "
        + "CASE WHEN \"f121\" = 2000 THEN 2000 ELSE 20000 END AS \"$f52\""
        + "\nFROM (SELECT \"EMPNO\", \"ENAME\", \"JOB\", \"MGR\", \"HIREDATE\", \"SAL\", \"COMM\","
        + " \"DEPTNO\", 2 AS \"f9\", 20 AS \"f10\", 0 AS \"f11\", "
        + "CASE WHEN \"EMPNO\" = 1 THEN 1 ELSE 10 END AS \"f12\", "
        + "CASE WHEN \"EMPNO\" = 2 THEN 2 ELSE 20 END AS \"f13\", "
        + "CASE WHEN \"EMPNO\" = 3 THEN 3 ELSE 30 END AS \"f14\", "
        + "CASE WHEN \"EMPNO\" = 4 THEN 4 ELSE 40 END AS \"f15\", "
        + "CASE WHEN \"EMPNO\" = 5 THEN 5 ELSE 50 END AS \"f16\", "
        + "CASE WHEN \"EMPNO\" = 6 THEN 6 ELSE 60 END AS \"f17\", "
        + "CASE WHEN \"EMPNO\" = 7 THEN 7 ELSE 70 END AS \"f18\", "
        + "CASE WHEN \"EMPNO\" = 8 THEN 8 ELSE 80 END AS \"f19\", "
        + "CASE WHEN \"EMPNO\" = 9 THEN 9 ELSE 90 END AS \"f20\", "
        + "CASE WHEN \"EMPNO\" = 10 THEN 10 ELSE 100 END AS \"f21\", "
        + "CASE WHEN \"EMPNO\" = 0 THEN 0 ELSE 20 END AS \"f111\", 110 AS \"f112\", "
        + "CASE WHEN CASE WHEN \"EMPNO\" = 1 THEN 1 ELSE 10 END = 12 "
        + "THEN 12 ELSE 120 END AS \"f113\", "
        + "CASE WHEN CASE WHEN \"EMPNO\" = 2 THEN 2 ELSE 20 END = 13 "
        + "THEN 13 ELSE 130 END AS \"f114\", "
        + "CASE WHEN CASE WHEN \"EMPNO\" = 3 THEN 3 ELSE 30 END = 14 "
        + "THEN 14 ELSE 140 END AS \"f115\", "
        + "CASE WHEN CASE WHEN \"EMPNO\" = 4 THEN 4 ELSE 40 END = 15 "
        + "THEN 15 ELSE 150 END AS \"f116\", "
        + "CASE WHEN CASE WHEN \"EMPNO\" = 5 THEN 5 ELSE 50 END = 16 "
        + "THEN 16 ELSE 160 END AS \"f117\", "
        + "CASE WHEN CASE WHEN \"EMPNO\" = 6 THEN 6 ELSE 60 END = 17 "
        + "THEN 17 ELSE 170 END AS \"f118\", "
        + "CASE WHEN CASE WHEN \"EMPNO\" = 7 THEN 7 ELSE 70 END = 18 "
        + "THEN 18 ELSE 180 END AS \"f119\", "
        + "CASE WHEN CASE WHEN \"EMPNO\" = 8 THEN 8 ELSE 80 END = 19 "
        + "THEN 19 ELSE 190 END AS \"f120\", "
        + "CASE WHEN CASE WHEN \"EMPNO\" = 9 THEN 9 ELSE 90 END = 20 "
        + "THEN 20 ELSE 200 END AS \"f121\""
        + "\nFROM \"scott\".\"EMP\") AS \"t\"";
    assertThat(toSqlWithBloat(root, 101), isLinux(expectedSql));
  }

  @Test public void testFunctionsWithRegexOperands() {
    final RelBuilder builder = relBuilder();
    final RexNode regexpSimilarRex =
        builder.call(SqlLibraryOperators.REGEXP_SIMILAR, builder.literal("12-12-2000"),
                builder.literal("^\\d\\d-\\w{2}-\\d{4}$"));
    final RexNode regexpExtractRex =
        builder.call(SqlLibraryOperators.REGEXP_EXTRACT, builder.literal("Calcite"),
                builder.literal("\\."), builder.literal("DM."));
    final RexNode regexpReplaceRex =
        builder.call(SqlLibraryOperators.REGEXP_REPLACE, builder.literal("Calcite"),
                builder.literal("\\."), builder.literal("DM."));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(regexpSimilarRex, "regexpLike"),
            builder.alias(regexpExtractRex, "regexpExtract"),
            builder.alias(regexpReplaceRex, "regexpReplace"))
        .build();

    final String expectedBiqQuery = "SELECT "
        + "IF(REGEXP_CONTAINS('12-12-2000' , r'^\\d\\d-\\w{2}-\\d{4}$'), 1, 0) AS regexpLike, "
        + "REGEXP_EXTRACT('Calcite', '\\.', 'DM.') AS regexpExtract, "
        + "REGEXP_REPLACE('Calcite', '\\.', 'DM.') AS regexpReplace\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testStringLiteralsWithInvalidEscapeSequences() {
    final RelBuilder builder = relBuilder();
    final RexNode literal1 = builder.literal("Datam\\etica");
    final RexNode literal2 = builder.literal("Sh\\\\irin");
    final RexNode literal3 = builder.literal("Peg\\\\\\gy");
    final RexNode literal4 = builder.literal("Mich\\\\\\\\ael");
    final RexNode literal5 = builder.literal("Pa\\\\\\\\\\ula");
    final RelNode root = builder
        .scan("EMP")
        .project(literal1, literal2, literal3, literal4, literal5)
        .build();

    final String expectedBiqQuery = "SELECT 'Datam\\\\etica' AS `$f0`, "
        + "'Sh\\\\\\\\irin' AS `$f1`, "
        + "'Peg\\\\\\\\\\\\gy' AS `$f2`, "
        + "'Mich\\\\\\\\\\\\\\\\ael' AS `$f3`, "
        + "'Pa\\\\\\\\\\\\\\\\\\\\ula' AS `$f4`\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testForDaysBetweenFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode dayPart =
        builder.call(SqlLibraryOperators.DAYS_BETWEEN, builder.literal("2012-03-03"), builder.literal("2012-05-03"));

    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(dayPart, "daysBetween"))
        .build();

    final String expectedQuery = "SELECT DAYS_BETWEEN('2012-03-03', '2012-05-03') AS "
        + "daysBetween\nFROM scott.EMP AS EMP";

    assertThat(toSql(root, DatabaseProduct.DB2.getDialect()), isLinux(expectedQuery));
  }

  @Test public void testStringLiteralsWithValidEscapeSequences() {
    final RelBuilder builder = relBuilder();
    final RexNode literal1 = builder.literal("Wal\ter");
    final RexNode literal2 = builder.literal("Dia\na");
    final RexNode literal3 = builder.literal("Mo\\\rgan");
    final RexNode literal4 = builder.literal("Re\\\\\becca");
    final RexNode literal5 = builder.literal("Shi\\\\\\rin");
    final RelNode root = builder
        .scan("EMP")
        .project(literal1, literal2, literal3, literal4, literal5)
        .build();

    final String expectedBiqQuery = "SELECT 'Wal\\ter' AS `$f0`, "
        + "'Dia\\na' AS `$f1`, "
        + "'Mo\\\\\\rgan' AS `$f2`, "
        + "'Re\\\\\\\\\\becca' AS `$f3`, "
        + "'Shi\\\\\\\\\\\\rin' AS `$f4`\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test void testLiteralAfterGroupBy() {
    String query = "SELECT D.\"department_id\",MIN(E.\"salary\") MINSAL, COUNT(E.\"salary\") "
        + "SALCOUNT, 'INSIDE CTE1'\n"
        + "FROM \"employee\" E \n"
        + "FULL JOIN \"department\" D ON E.\"department_id\" = D.\"department_id\" \n"
        + "GROUP BY D.\"department_id\"  \n"
        + "HAVING MIN(E.\"salary\") < 1000";
    final String expected = "SELECT department.department_id, MIN(employee.salary) AS MINSAL, "
        + "COUNT(employee.salary) AS SALCOUNT, 'INSIDE CTE1'\n"
        + "FROM foodmart.employee\n"
        + "FULL JOIN foodmart.department ON employee.department_id = department.department_id\n"
        + "GROUP BY department.department_id\n"
        + "HAVING MINSAL < 1000";

    sql(query)
        .schema(CalciteAssert.SchemaSpec.JDBC_FOODMART)
        .withBigQuery().ok(expected);
  }

  @Test void testNonAggregateExpressionInOrderBy() {
    String query = "SELECT EXTRACT(DAY FROM \"birth_date\") \n"
        + "FROM \"employee\" \n"
        + "GROUP BY EXTRACT(DAY FROM \"birth_date\") \n"
        + "ORDER BY EXTRACT(DAY FROM \"birth_date\")";
    final String expected = "SELECT EXTRACT(DAY FROM birth_date)\n"
        + "FROM foodmart.employee\n"
        + "GROUP BY EXTRACT(DAY FROM birth_date)\n"
        + "ORDER BY 1 NULLS LAST";

    sql(query)
        .schema(CalciteAssert.SchemaSpec.JDBC_FOODMART)
        .withBigQuery().ok(expected);
  }

  @Test public void testForAddDaysFunction() {
    RelBuilder relBuilder = relBuilder().scan("EMP");
    final RexLiteral literalValue = relBuilder.literal(1);
    final RexNode addDays =
        relBuilder.call(SqlLibraryOperators.ADD_DAYS, relBuilder.call(CURRENT_DATE), literalValue);
    RelNode root = relBuilder
        .project(addDays)
        .build();
    final String expectedDb2Sql = "SELECT ADD_DAYS(CURRENT_DATE, 1) AS $f0\n"
        + "FROM scott.EMP AS EMP";

    assertThat(toSql(root, DatabaseProduct.DB2.getDialect()), isLinux(expectedDb2Sql));
  }

  @Test public void testDb2Trunc() {
    RelBuilder builder = relBuilder().scan("EMP");
    final RexNode dateTruncNode =
        builder.call(SqlLibraryOperators.DB2_TRUNC, builder.call(CURRENT_TIMESTAMP),
        builder.literal("YEAR"));
    RelNode root = builder
        .project(dateTruncNode)
        .build();
    final String expectedDb2Sql = "SELECT TRUNC(CURRENT_TIMESTAMP, 'YEAR') AS $f0\n"
            + "FROM scott.EMP AS EMP";
    assertThat(toSql(root, DatabaseProduct.DB2.getDialect()), isLinux(expectedDb2Sql));
  }

  @Test void testAggregateExpressionInOrderBy() {
    String query = "SELECT EXTRACT(DAY FROM \"birth_date\") \n"
        + "FROM \"employee\" \n"
        + "GROUP BY EXTRACT(DAY FROM \"birth_date\") \n"
        + "ORDER BY SUM(\"salary\")";
    final String expected = "SELECT EXTRACT(DAY FROM birth_date)\n"
        + "FROM foodmart.employee\n"
        + "GROUP BY EXTRACT(DAY FROM birth_date)\n"
        + "ORDER BY SUM(salary) NULLS LAST";

    sql(query)
        .schema(CalciteAssert.SchemaSpec.JDBC_FOODMART)
        .withBigQuery().ok(expected);
  }

  @Test void testBQCastToDecimal() {
    final String query = "select \"employee_id\",\n"
        + "  cast(\"salary_paid\" as DECIMAL)\n"
        + "from \"salary\"";
    final String expected = "SELECT employee_id, CAST(salary_paid AS NUMERIC)\n"
        + "FROM foodmart.salary";
    sql(query).withBigQuery().ok(expected);
  }

  @Test void testBQCastToDecimalForLiteral() {
    final String query = "select \"employee_id\",\n"
        + " cast('1234.67' as DECIMAL(10,1)), cast(1234.6 as DECIMAL),\n"
        + " cast('1234.67' as DECIMAL(10,4))\n"
        + "from \"salary\"";
    final String expected = "SELECT employee_id, "
        + "ROUND(CAST(1234.67 AS NUMERIC), 1), ROUND(CAST"
        + "(1234.6 AS NUMERIC), 0), 1234.67\n"
        + "FROM foodmart.salary";
    sql(query).withBigQuery().ok(expected);
  }

  @Test public void testMsSqlDateNameFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode isNumericNode =
        builder.call(SqlLibraryOperators.DATENAME,
            builder.literal(DAY),
            builder.call(GETDATE));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(isNumericNode, "datename"))
        .build();
    final String expectedSFSql = "SELECT DATENAME(DAY, GETDATE) AS [datename]\n"
        + "FROM [scott].[EMP]";

    assertThat(toSql(root, DatabaseProduct.MSSQL.getDialect()), isLinux(expectedSFSql));
  }

  @Test public void testQuoteInStringLiterals() {
    final RelBuilder builder = relBuilder();
    final RexNode literal = builder.literal("Datam\"etica");
    final RelNode root = builder
        .scan("EMP")
        .project(literal)
        .build();

    final String expectedBiqQuery = "SELECT 'Datam\"etica' AS `$f0`\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testSplitToTable() {
    final RelBuilder builder = relBuilder();
    final RelNode root = builder
        .functionScan(SqlLibraryOperators.SPLIT_TO_TABLE, 0,
            builder.literal("a,b,c"), builder.literal(","))
        .project(builder.field(2))
        .build();

    final String expectedBiqQuery = "SELECT \"VALUE\"\n"
        + "FROM TABLE(SPLIT_TO_TABLE('a,b,c', ','))";

    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testMsSqlStringSplit() {
    final RelBuilder builder = relBuilder();
    final RelNode root = builder
        .functionScan(SqlLibraryOperators.MSSQL_STRING_SPLIT, 0,
            builder.literal("a-b-c"), builder.literal("-"))
        .build();

    final String expectedQuery = "SELECT *\n"
        + "FROM TABLE(MSSQL_STRING_SPLIT('a-b-c', '-'))";

    assertThat(toSql(root, DatabaseProduct.MSSQL.getDialect()), isLinux(expectedQuery));
  }

  @Test public void testStrtokSplitToTable() {
    final RelBuilder builder = relBuilder();
    final Map<String, RelDataType> columnDefinition = new HashMap<>();
    final RelDataType intType =
        builder.getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER);
    final RelDataType varcharType =
        builder.getCluster().getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
    columnDefinition.put("OUTKEY", intType);
    columnDefinition.put("TOKENNUM", intType);
    columnDefinition.put("TOKEN", varcharType);
    final RelNode root = builder
        .functionScan(new TeradataStrtokSplitToTableFunction(columnDefinition), 0,
            builder.literal(1), builder.literal("a,b,c"), builder.literal(","))
        .project(builder.field(2))
        .build();

    final String expectedTDQuery = "SELECT \"TOKEN\"\n"
        + "FROM TABLE(STRTOK_SPLIT_TO_TABLE(1, 'a,b,c', ',') "
        + "RETURNS(OUTKEY INTEGER, TOKENNUM INTEGER, TOKEN VARCHAR))";

    assertThat(toSql(root, DatabaseProduct.TERADATA.getDialect()), isLinux(expectedTDQuery));
  }

  @Test public void testSimpleStrtokFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode strtokNode =
        builder.call(SqlLibraryOperators.STRTOK, builder.literal("TERADATA-BIGQUERY-SPARK-ORACLE"),
                builder.literal("-"), builder.literal(2));
    final RelNode root = builder
        .values(new String[]{""}, 1)
        .project(builder.alias(strtokNode, "aa"))
        .build();

    final String expectedBiqQuery = "SELECT REGEXP_EXTRACT_ALL('TERADATA-BIGQUERY-SPARK-ORACLE' ,"
        + " r'[^-]+') [OFFSET ( 1 ) ] AS aa";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testSimpleStrtokFunctionWithMultipleDelimiters() {
    final RelBuilder builder = relBuilder();
    final RexNode strtokNode =
        builder.call(SqlLibraryOperators.STRTOK, builder.literal("TERADATA BIGQUERY-SPARK/ORACLE"),
                builder.literal(" -/"), builder.literal(2));
    final RelNode root = builder
        .values(new String[]{""}, 1)
        .project(builder.alias(strtokNode, "aa"))
        .build();

    final String expectedBiqQuery = "SELECT REGEXP_EXTRACT_ALL('TERADATA BIGQUERY-SPARK/ORACLE' ,"
        + " r'[^ -/]+') [OFFSET ( 1 ) ] AS aa";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testComposeFunction() {
    final RelBuilder builder = relBuilder();
    RexNode unistrNode = builder.call(SqlLibraryOperators.UNISTR, builder.literal("\\0308"));
    final RelNode root = builder
        .scan("EMP")
        .project(
            builder.call(SqlLibraryOperators.COMPOSE,
                builder.call(SqlStdOperatorTable.CONCAT,
                    builder.literal("abcd"), unistrNode)))
        .build();
    final String expectedOracleSql = "SELECT COMPOSE('abcd' || UNISTR('\\0308')) \"$f0\"\n"
        + "FROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.ORACLE.getDialect()), isLinux(expectedOracleSql));
  }

  @Test public void testBigQueryUnicodeEscapeSequence() {
    final RelBuilder builder = relBuilder();
    String unicodeEncodedChar = String.valueOf((char) 0x0308);
    final RelNode root = builder
        .scan("EMP")
        .project(builder.getRexBuilder()
            .makeCharLiteral(new NlsString(unicodeEncodedChar, "UTF-8", null)))
        .build();
    final String expectedBqSql = "SELECT '\\u0308' AS `$f0`\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBqSql));
  }

  @Test void testSpecialCharacterInSpark() {
    final String query = "select 'café' "
        + "from \"employee\"";
    final String expected = "SELECT 'café'\n"
        + "FROM foodmart.employee";
    sql(query).withSpark().ok(expected);
  }

  @Test public void testSimpleStrtokFunctionWithSecondOpernadAsNull() {
    final RelBuilder builder = relBuilder();
    final RexNode strtokNode =
        builder.call(SqlLibraryOperators.STRTOK, builder.literal("TERADATA BIGQUERY-SPARK/ORACLE"),
                builder.literal(null), builder.literal(2));
    final RelNode root = builder
        .values(new String[]{""}, 1)
        .project(builder.alias(strtokNode, "aa"))
        .build();

    final String expectedBiqQuery = "SELECT REGEXP_EXTRACT_ALL('TERADATA BIGQUERY-SPARK/ORACLE' , "
        + "NULL) [OFFSET ( 1 ) ] AS aa";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testStrtokWithIndexFunctionAsThirdArgument() {
    final RelBuilder builder = relBuilder();
    final RexNode positionRexNode =
        builder.call(SqlStdOperatorTable.POSITION, builder.literal("B"), builder.literal("ABC"));
    final RexNode strtokRexNode =
        builder.call(SqlLibraryOperators.STRTOK, builder.literal("TERADATA BIGQUERY SPARK ORACLE"),
                builder.literal(" "), positionRexNode);
    final RelNode root = builder
        .values(new String[]{""}, 1)
        .project(builder.alias(strtokRexNode, "aa"))
        .build();

    final String expectedBiqQuery = "SELECT REGEXP_EXTRACT_ALL('TERADATA BIGQUERY SPARK ORACLE' , "
        + "r'[^ ]+') [OFFSET ( STRPOS('ABC', 'B') -1 ) ] AS aa";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testStrtokWithCastFunctionAsThirdArgument() {
    final RelBuilder builder = relBuilder();
    final RexNode lengthFunRexNode =
        builder.call(SqlStdOperatorTable.CHAR_LENGTH, builder.literal("dm-R"));
    final RexNode formatIntegerCastRexNode =
        builder.cast(lengthFunRexNode, SqlTypeName.INTEGER);
    final RexNode strtokRexNode =
        builder.call(SqlLibraryOperators.STRTOK, builder.literal("TERADATA-BIGQUERY-SPARK-ORACLE"),
                builder.literal("-"),
        formatIntegerCastRexNode);
    final RelNode root = builder
        .values(new String[]{""}, 1)
        .project(builder.alias(strtokRexNode, "aa"))
        .build();

    final String expectedBiqQuery = "SELECT REGEXP_EXTRACT_ALL('TERADATA-BIGQUERY-SPARK-ORACLE' , "
        + "r'[^-]+') [OFFSET ( LENGTH('dm-R') -1 ) ] AS aa";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testBQRangeLiteral() {
    final RelBuilder builder = relBuilder();
    final RexNode upperRange =
        builder.getRexBuilder().makeDateLiteral(new DateString("2000-12-12"));
    final RexNode lowerRange =
        builder.getRexBuilder().makeDateLiteral(new DateString("2002-12-12"));
    final RexNode rangeLiteralNode =
        builder.call(SqlLibraryOperators.RANGE_LITERAL, upperRange, lowerRange);
    final RelNode root = builder
        .scan("EMP")
        .project(rangeLiteralNode)
        .build();

    final String expectedBiqQuery = "SELECT RANGE<DATE> '[2000-12-12, 2002-12-12)' AS `$f0`\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testOracleLength() {
    RelBuilder relBuilder = relBuilder().scan("EMP");
    final RexNode lengthNode =
        relBuilder.call(SqlLibraryOperators.LENGTH, relBuilder.literal("abcd"));
    RelNode root = relBuilder
        .project(lengthNode)
        .build();
    final String expectedOracleSql = "SELECT LENGTH('abcd') \"$f0\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedPostgresSql = "SELECT LENGTH('abcd') AS \"$f0\"\n"
        + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.ORACLE.getDialect()), isLinux(expectedOracleSql));
    assertThat(toSql(root, DatabaseProduct.POSTGRESQL.getDialect()), isLinux(expectedPostgresSql));
  }

  @Test public void testPostgresCurrentTimestampTZ() {
    RelBuilder relBuilder = relBuilder().scan("EMP");
    RelDataType relDataType =
        relBuilder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP_WITH_TIME_ZONE);
    List<RexNode> operandList = new ArrayList<>();
    operandList.add(relBuilder.literal(9));
    final RexNode literalTimestamp =
        relBuilder.getRexBuilder().makeCall(relDataType, CURRENT_TIMESTAMP_WITH_TIME_ZONE, operandList);
    RelNode root = relBuilder
        .project(literalTimestamp)
        .build();
    final String expectedDB2Sql = "SELECT CURRENT_TIMESTAMP (6) AS \"$f0\"\n"
        + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.POSTGRESQL.getDialect()), isLinux(expectedDB2Sql));
  }

  @Test void testTeraDataCastAsInt() {
    String query = "SELECT CAST(45.12 AS Integer) ";
    final String expected = "SELECT 45";
    sql(query).withBigQuery().ok(expected);
  }

  private RexNode makeCaseCall(RelBuilder builder, int index, int number) {
    RexNode rex = builder.literal(number);
    RexNode rex2 = builder.literal(number * 10);
    return builder.getRexBuilder().makeCall(SqlStdOperatorTable.CASE,
        builder.equals(builder.field(index), builder.literal(number)), rex, rex2);
  }

  private List<RexNode> getExtendedRexList(RelNode relNode, RexNode... rexNodes) {
    List<RexNode> fields = new ArrayList<>();
    for (RelDataTypeField field : relNode.getRowType().getFieldList()) {
      fields.add(
          relNode.getCluster().getRexBuilder().makeInputRef(field.getType(), field.getIndex()));
    }
    Collections.addAll(fields, rexNodes);
    return fields;
  }

  private String toSqlWithBloat(RelNode root, int bloat) {
    SqlDialect dialect = DatabaseProduct.CALCITE.getDialect();
    UnaryOperator<SqlWriterConfig> transform = c ->
        c.withAlwaysUseParentheses(false)
            .withSelectListItemsOnSeparateLines(false)
            .withUpdateSetListNewline(false)
            .withIndentation(0);
    final RelToSqlConverter converter = new RelToSqlConverter(dialect, bloat);
    final SqlNode sqlNode = converter.visitRoot(root).asStatement();
    return sqlNode.toSqlString(c -> transform.apply(c.withDialect(dialect)))
        .getSql();
  }

  @Test public void testStrTimeRelToSql() {
    final RelBuilder builder = relBuilder();
    final RexNode strToDateNode =
        builder.call(SqlLibraryOperators.TIME, builder.cast(builder.literal("11:15:00"), SqlTypeName.TIME));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(strToDateNode, "date1"))
        .build();
    final String expectedSql = "SELECT TIME(TIME '11:15:00') AS \"date1\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT TIME(TIME '11:15:00') AS date1\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  /* this is giving class cast exception SqlIdentifier to SqlBasicCall
  when case clause is used in Aggregate*/
  @Test public void testCaseClauseInAggregate() {
    final String query = "SELECT sum(case when \"employee_id\" = 100 then 1 else 0 end)\n"
        + "FROM \"foodmart\".\"employee\"";
    final String expected = "SELECT SUM(CASE WHEN employee_id = 100 THEN 1 ELSE 0 END)\n"
        + "FROM foodmart.employee";
    sql(query)
        .schema(CalciteAssert.SchemaSpec.JDBC_FOODMART)
        .withBigQuery().ok(expected);
  }

  @Test public void testLogFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode logRexNode =
        builder.call(SqlLibraryOperators.LOG, builder.literal(3), builder.literal(2));
    final RelNode root = builder
        .values(new String[] {""}, 1)
        .project(builder.alias(logRexNode, "value"))
        .build();
    final String expectedSFQuery = "SELECT LOG(3, 2) AS \"value\"";
    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedSFQuery));
  }

  @Test public void testPercentileCont() {
    final String query = "SELECT\n"
        + " PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY \"product_id\")\n"
        + "FROM \"product\"";
    final String expectedSql = "SELECT PERCENTILE_CONT(0.25) WITHIN GROUP "
        + "(ORDER BY \"product_id\")\n"
        + "FROM \"foodmart\".\"product\"";

    sql(query)
        .ok(expectedSql);

  }

  @Test void testPercentileContWithGroupBy() {
    final String query = "SELECT \"shelf_width\",\n"
        + " PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY \"product_id\")\n"
        + "FROM \"product\"\n"
        + "GROUP BY \"shelf_width\"";
    final String expectedSql = "SELECT \"shelf_width\", PERCENTILE_CONT(0.25) WITHIN GROUP "
        + "(ORDER BY \"product_id\")\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "GROUP BY \"shelf_width\"";
    sql(query)
        .ok(expectedSql);
  }

  @Test void testHashAgg() {
    final RelBuilder builder = relBuilder().scan("EMP");
    RelBuilder.AggCall hashAggCall =
        builder.aggregateCall(SqlLibraryOperators.HASH_AGG, builder.field(1));
    final RelNode root = builder
        .aggregate(builder.groupKey(), hashAggCall.as("hash"))
        .build();
    final String expectedSnowflakeSql = "SELECT HASH_AGG(\"ENAME\") AS \"hash\"\n"
        + "FROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedSnowflakeSql));
  }

  @Test public void testForBlobFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode toClobRex = builder.call(SqlLibraryOperators.EMPTY_BLOB);
    final RelNode root = builder
        .scan("EMP")
        .project(toClobRex)
        .build();
    final String expectedOracleQuery = "SELECT EMPTY_BLOB() \"$f0\"\nFROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.ORACLE.getDialect()), isLinux(expectedOracleQuery));
  }

  @Test void testCollectListAndConcatWsFunction() {
    final RelBuilder builder = relBuilder().scan("EMP");
    RelBuilder.AggCall collectLIstAggCall =
        builder.aggregateCall(SqlLibraryOperators.COLLECT_LIST, builder.field(1));
    final RelNode root = builder
        .aggregate(builder.groupKey(), collectLIstAggCall.as("collect_list"))
        .project(builder.call(SqlLibraryOperators.CONCAT_WS_SPARK, builder.literal(";"), builder.field(0)))
        .build();
    final String expectedSparkSql = "SELECT CONCAT_WS(';', COLLECT_LIST(ENAME)) $f0\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkSql));
  }

  @Test void testBitXor() {
    final RelBuilder builder = relBuilder().scan("EMP");
    RelBuilder.AggCall xorCall =
        builder.aggregateCall(SqlLibraryOperators.BIT_XOR, builder.field("EMPNO"));
    final RelNode root = builder
        .aggregate(builder.groupKey(), xorCall.as("hash"))
        .build();
    final String expectedBQSql = "SELECT BIT_XOR(EMPNO) AS `hash`\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQSql));
  }

  @Test void testCorrelatedScalarQueryInSelectList() {
    RelBuilder builder = foodmartRelBuilder();
    builder.scan("employee");
    CorrelationId correlationId = builder.getCluster().createCorrel();
    RelDataType relDataType = builder.peek().getRowType();
    RexNode correlVariable = builder.getRexBuilder().makeCorrel(relDataType, correlationId);
    int departmentIdIndex = builder.field("department_id").getIndex();
    RexNode correlatedScalarSubQuery = RexSubQuery.scalar(builder
        .scan("department")
        .filter(builder
            .equals(
                builder.field("department_id"),
                builder.getRexBuilder().makeFieldAccess(correlVariable, departmentIdIndex)))
        .project(builder.field("department_id"))
        .build());
    RelNode root = builder
        .project(
            ImmutableSet.of(builder.field("employee_id"), correlatedScalarSubQuery),
            ImmutableSet.of("emp_id", "dept_id"),
            false,
            ImmutableSet.of(correlationId))
        .build();
    final String expectedSql = "SELECT employee_id AS emp_id, (SELECT department_id\n"
        + "FROM foodmart.department\n"
        + "WHERE department_id = employee.department_id) AS dept_id\n"
        + "FROM foodmart.employee";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedSql));
  }

  @Test public void testUnparsingOfPercentileCont() {
    final RelBuilder builder = relBuilder();
    builder.push(builder.scan("EMP").build());

    final List<RexNode> percentileContRex =
        ImmutableList.of(builder.field("DEPTNO"), builder.literal("0.5"));
    final RelDataType decimalType =
        builder.getTypeFactory().createSqlType(SqlTypeName.DECIMAL);
    List<RexNode> partitionKeyRexNodes =
        ImmutableList.of(
            builder.field("EMPNO"), builder.field(
        "DEPTNO"));
    final RexNode overRex =
        builder.getRexBuilder().makeOver(decimalType, SqlStdOperatorTable.PERCENTILE_CONT,
        percentileContRex, partitionKeyRexNodes, ImmutableList.of(),
        RexWindowBounds.UNBOUNDED_PRECEDING, RexWindowBounds.UNBOUNDED_FOLLOWING,
        false, true, false, false, false);

    builder.build();
    final RelNode root = builder
        .scan("EMP")
        .project(builder.field(0), overRex)
        .aggregate(builder.groupKey(builder.field(0), builder.field(1)))
        .build();
    final String expectedSql = "SELECT \"EMPNO\", PERCENTILE_CONT(\"DEPTNO\", '0.5') OVER"
        + " (PARTITION BY \"EMPNO\", \"DEPTNO\") AS \"$f1\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT EMPNO, PERCENTILE_CONT(DEPTNO, '0.5') OVER (PARTITION"
        + " BY EMPNO, DEPTNO) AS `$f1`\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testForRegexpReplaceFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode regexpReplaceRex =
        builder.call(SqlLibraryOperators.REGEXP_REPLACE, builder.literal("CalCITE"), builder.literal("al"),
            builder.literal("AL"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(regexpReplaceRex, "regexpReplace"))
        .build();

    final String expectedSparkSql = "SELECT REGEXP_REPLACE('CalCITE', 'al', 'AL') regexpReplace\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSparkSql));
  }

  @Test public void testSplitPartFunction() {
    final RelBuilder builder = relBuilder();
    RexNode splitPart =
        builder.call(SqlLibraryOperators.SPLIT_PART, builder.literal("123@Domain|Example"),
                builder.literal("@"), builder.literal(2));

    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(splitPart, "Result"))
        .build();
    final String expectedSnowFlakeQuery = "SELECT SPLIT_PART('123@Domain|Example', '@', 2) AS "
        + "\"Result\"\nFROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()),
        isLinux(expectedSnowFlakeQuery));

  }

  @Test public void testSplitFunction() {
    final RelBuilder builder = relBuilder();
    RexNode split =
        builder.call(SqlLibraryOperators.SPLIT, builder.literal("123@Domain|Example"), builder.literal("@"));

    RexNode splitAccess = builder.call(SAFE_OFFSET, split, builder.literal(2));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(splitAccess, "Result"))
        .build();

    final String expectedBigQuery = "SELECT SPLIT('123@Domain|Example', '@')[SAFE_OFFSET(2)] "
        + "AS Result\nFROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()),
        isLinux(expectedBigQuery));
  }

  @Test public void testIsDateFunction() {
    final RelBuilder builder = relBuilder();
    RexNode isDateRexNode = builder.call(SqlLibraryOperators.ISDATE, builder.literal("2024-12-09"));

    final RelNode root =
        builder.scan("EMP").project(builder.alias(isDateRexNode, "Result")).build();

    final String expectedSynapseSql = "SELECT ISDATE('2024-12-09') "
        + "AS [Result]\nFROM [scott].[EMP]";

    assertThat(toSql(root, DatabaseProduct.MSSQL.getDialect()), isLinux(expectedSynapseSql));
  }

  @Test public void testReplicateFunction() {
    final RelBuilder builder = relBuilder();
    RexNode replicate =
        builder.call(SqlLibraryOperators.REPLICATE, builder.literal("b"), builder.literal(1));

    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(replicate, "Result"))
        .build();

    final String expectedSynapseQuery = "SELECT REPLICATE('b', 1) "
        + "AS [Result]\nFROM [scott].[EMP]";

    assertThat(toSql(root, DatabaseProduct.MSSQL.getDialect()),
        isLinux(expectedSynapseQuery));
  }

  @Test public void testToCurrentTimestampFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode parseTSNode1 =
        builder.call(SqlLibraryOperators.TO_TIMESTAMP, builder.literal("2009-03-20 12:25:50.123456"),
        builder.literal("yyyy-MM-dd HH24:MI:MS.sssss"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(parseTSNode1, "timestamp_value"))
        .build();
    final String expectedSql =
        "SELECT TO_TIMESTAMP('2009-03-20 12:25:50.123456', 'yyyy-MM-dd HH24:MI:MS.sssss') AS "
            + "\"timestamp_value\"\nFROM \"scott\".\"EMP\"";
    final String expectedBiqQuery =
        "SELECT PARSE_DATETIME('%F %H:%M:%E*S', '2009-03-20 12:25:50.123456') AS timestamp_value\n"
            + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testRegexpCount() {
    final RelBuilder builder = relBuilder();
    final RexNode regexpCountRexNode =
        builder.call(SqlLibraryOperators.REGEXP_COUNT, builder.literal("foo1 foo foo40 foo"), builder.literal("foo"));
    final RelNode root = builder
        .values(new String[] {""}, 1)
        .project(builder.alias(regexpCountRexNode, "value"))
        .build();
    final String expectedSFQuery = "SELECT REGEXP_COUNT('foo1 foo foo40 foo', 'foo') AS \"value\"";
    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedSFQuery));
  }

  @Test public void testRegexpSplitToArray() {
    final RelBuilder builder = relBuilder();
    final RexNode regexpCountRexNode =
        builder.call(SqlLibraryOperators.REGEXP_SPLIT_TO_ARRAY,
            builder.literal("foo1 foo foo40 foo"),
            builder.literal("foo"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(regexpCountRexNode, "value"))
        .build();
    final String expectedOracleQuery = "SELECT REGEXP_SPLIT_TO_ARRAY('foo1 foo foo40 foo', 'foo') "
        + "AS \"value\"\n"
        + "FROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.POSTGRESQL.getDialect()), isLinux(expectedOracleQuery));
  }

  @Test public void testMONInUppercase() {
    final RelBuilder builder = relBuilder();
    final RexNode monthInUppercase =
        builder.call(SqlLibraryOperators.FORMAT_DATE, builder.literal("MONU"), builder.scan("EMP").field(4));

    final RelNode doyRoot = builder
        .scan("EMP")
        .project(builder.alias(monthInUppercase, "month"))
        .build();

    final String expectedMONBiqQuery = "SELECT FORMAT_DATE('%^b', HIREDATE) AS month\n"
        + "FROM scott.EMP";

    assertThat(toSql(doyRoot, DatabaseProduct.BIG_QUERY.getDialect()),
        isLinux(expectedMONBiqQuery));
  }

  @Test public void testToHexFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode toHexFunction =
        builder.call(SqlLibraryOperators.TO_HEX, builder.call(SqlLibraryOperators.MD5, builder.literal("snowflake")));

    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(toHexFunction, "md5_hashed"))
        .build();
    final String expectedSql = "SELECT TO_HEX(MD5('snowflake')) AS \"md5_hashed\"\n"
        + "FROM \"scott\".\"EMP\"";
    final String expectedBiqQuery = "SELECT TO_HEX(MD5('snowflake')) AS md5_hashed\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testJsonObjectFunction() {
    final RelBuilder builder = relBuilder();
    Map<String, String> obj = new HashMap<>();
    obj.put("Name", "John");
    obj.put("Surname", "Mark");
    obj.put("Age", "30");
    List<RexNode> operands = new ArrayList<>();
    for (Map.Entry<String, String> m : obj.entrySet()) {
      operands.add(builder.literal(m.getKey()));
      operands.add(builder.literal(m.getValue()));
    }
    final RexNode jsonNode = builder.call(SqlLibraryOperators.JSON_OBJECT, operands);
    final RelNode root = builder
        .scan("EMP")
        .project(jsonNode)
        .build();

    final String expectedBiqQuery = "SELECT JSON_OBJECT('Surname', 'Mark', 'Age', '30', "
        + "'Name', 'John') AS `$f0`\nFROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()),
        isLinux(expectedBiqQuery));
  }

  @Test public void testParseJsonFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode parseJsonNode =
        builder.call(SqlLibraryOperators.PARSE_JSON, builder.literal("NULL"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(parseJsonNode, "null_value"))
        .build();
    final String expectedBigquery = "SELECT PARSE_JSON('NULL') AS null_value\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBigquery));
  }

  @Test public void testParseJsonSfFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode parseJsonSfNode =
        builder.call(SqlLibraryOperators.PARSE_JSON_SF, builder.literal("{\"PI\":3.14}"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(parseJsonSfNode, "parsed_val"))
        .build();
    final String expectedBigquery = "SELECT PARSE_JSON_SF('{\"PI\":3.14}') AS \"parsed_val\"\n"
        + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedBigquery));
  }

  @Test public void testTryParseJsonFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode tryParseJsonNode =
        builder.call(SqlLibraryOperators.TRY_PARSE_JSON, builder.literal("{\"PI\":3.14}"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(tryParseJsonNode, "parsed_val"))
        .build();
    final String expectedBigquery = "SELECT TRY_PARSE_JSON('{\"PI\":3.14}') AS \"parsed_val\"\n"
        + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedBigquery));
  }

  @Test public void testArrayContainsSfFunction() {
    final RelBuilder builder = relBuilder();
    RexNode valueExpression = builder.literal("A");
    RexNode arrayNode =
        builder.call(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, builder.literal("A"), builder.literal("B"));
    final RexNode arrayContainsSfNode =
        builder.call(SqlLibraryOperators.ARRAY_CONTAINS_SF, valueExpression, arrayNode);
    final RelNode root = builder
        .scan("EMP")
        .project(arrayContainsSfNode)
        .build();
    final String expectedBigquery = "SELECT ARRAY_CONTAINS('A', ARRAY['A', 'B']) AS \"$f0\""
        + "\nFROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedBigquery));
  }

  @Test public void testParseIpFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode parseIpNode1 =
        builder.call(SqlLibraryOperators.PARSE_IP, builder.literal("192.168.242.188"),
            builder.literal("INET"));
    final RexNode parseIpNode2 =
        builder.call(SqlLibraryOperators.PARSE_IP, builder.literal("192.168.242.188"),
            builder.literal("INET"), builder.literal(1));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(parseIpNode1, "twoArgs"), builder.alias(parseIpNode2, "threeArgs"))
        .build();
    final String expectedBigquery = "SELECT PARSE_IP('192.168.242.188', 'INET') AS twoArgs, "
        + "PARSE_IP('192.168.242.188', 'INET', 1) AS threeArgs\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBigquery));
  }

  @Test public void testQuantileFunction() {
    final RelBuilder builder = relBuilder();
    RexNode finalRexforQuantile = createRexForQuantile(builder);
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(finalRexforQuantile, "quantile"))
        .build();

    final String expectedBiqQuery = "SELECT CAST(FLOOR(((RANK() OVER (ORDER BY 23)) - 1) * 5 "
        + "/ (COUNT(*) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))) AS INT64)"
        + " AS quantile\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testQuantileFunctionWithQualify() {
    final RelBuilder builder = relBuilder();
    RexNode finalRexforQuantile = createRexForQuantile(builder);
    final RelNode root = builder
        .scan("EMP")
        .filter(
            builder.call(SqlLibraryOperators.NOT_BETWEEN,
                builder.field("EMPNO"), builder.literal(1), builder.literal(3)))
        .project(builder.field("DEPTNO"), builder.alias(finalRexforQuantile, "quantile"))
        .filter(
            builder.call(SqlStdOperatorTable.EQUALS,
                builder.field("quantile"), builder.literal(1)))
        .project(builder.field("DEPTNO"))
        .build();

    final String expectedBiqQuery = "SELECT DEPTNO\n"
        + "FROM scott.EMP\n"
        + "WHERE EMPNO NOT BETWEEN 1 AND 3\n"
        + "QUALIFY CAST(FLOOR(((RANK() OVER (ORDER BY 23)) - 1) * 5 / (COUNT(*) OVER (RANGE BETWEEN "
        + "UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))) AS INT64) = 1";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  private RexNode createRexForQuantile(RelBuilder builder) {
    List<RexFieldCollation> windowOrderCollation = new ArrayList<>();
    final RelDataType rankRelDataType =
        builder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
    windowOrderCollation.add(
        new RexFieldCollation(builder.literal(23),
        Collections.singleton(SqlKind.NULLS_FIRST)));

    final RexNode windowRexNode =
        builder.getRexBuilder().makeOver(rankRelDataType, SqlStdOperatorTable.RANK, ImmutableList.of(),
                ImmutableList.of(), ImmutableList.copyOf(windowOrderCollation),
                RexWindowBounds.UNBOUNDED_PRECEDING, RexWindowBounds.UNBOUNDED_FOLLOWING, true,
                true, false, false, false);

    RexNode minusRexNode =
        builder.call(SqlStdOperatorTable.MINUS, windowRexNode, builder.literal(1));
    RexNode multiplicationRex =
        builder.call(SqlStdOperatorTable.MULTIPLY, minusRexNode, builder.literal(5));

    final RexNode windowRexNodeOfCount =
        builder.getRexBuilder().makeOver(rankRelDataType, SqlStdOperatorTable.COUNT, ImmutableList.of(),
                ImmutableList.of(), ImmutableList.of(), RexWindowBounds.UNBOUNDED_PRECEDING,
                RexWindowBounds.UNBOUNDED_FOLLOWING, true, true, false, false, false);
    return builder.call(SqlStdOperatorTable.DIVIDE_INTEGER, multiplicationRex,
        windowRexNodeOfCount);
  }

  @Test void testArrayAgg() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RelBuilder.AggCall aggCall =
        builder.aggregateCall(SqlLibraryOperators.ARRAY_AGG, builder.field("ENAME")).sort(builder.field("ENAME"));
    final RelNode rel = builder
        .aggregate(relBuilder().groupKey(), aggCall)
        .build();
    final String expectedBigQuery = "SELECT ARRAY_AGG(ENAME ORDER BY ENAME IS NULL, ENAME)"
        + " AS `$f0`\n"
        + "FROM scott.EMP";
    assertThat(toSql(rel, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBigQuery));
  }

  @Test void testDateTimeFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode dateTimeNode =
        builder.call(SqlLibraryOperators.DATETIME, builder.literal("2008-08-21 07:23:54"),
                builder.literal("US/Mountain"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(dateTimeNode, "converted_value"))
        .build();
    final String expectedBQ =
        "SELECT DATETIME('2008-08-21 07:23:54', 'US/Mountain') AS converted_value\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQ));
  }

  @Test public void testZEROIFNULL() {
    final RelBuilder builder = relBuilder();
    final RexNode zeroIfNullRexNode =
        builder.call(SqlLibraryOperators.ZEROIFNULL, builder.literal(5));
    final RelNode root = builder
        .scan("EMP")
        .project(zeroIfNullRexNode)
        .build();
    final String expectedSFQuery = "SELECT ZEROIFNULL(5) AS \"$f0\"\nFROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedSFQuery));
  }

  @Test public void testWidthBucketFunction() {
    final RelBuilder builder = relBuilder();
    RelNode root = builder
        .scan("EMP")
        .project(
            builder.call(SqlLibraryOperators.WIDTH_BUCKET, builder.field("SAL"),
                builder.literal(30000), builder.literal(100000), builder.literal(5)))
        .build();
    final String expectedTeradataQuery = "SELECT WIDTH_BUCKET(\"SAL\", 30000, 100000, 5) AS \"$f0\"\n"
        + "FROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.TERADATA.getDialect()), isLinux(expectedTeradataQuery));
  }

  @Test public void testOracleNChrFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode nchrCall = builder.call(SqlLibraryOperators.NCHR, builder.literal(5));
    final RelNode root = builder
        .scan("EMP")
        .project(nchrCall)
        .build();
    final String expectedOracleSql = "SELECT NCHR(5) \"$f0\"\nFROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.ORACLE.getDialect()), isLinux(expectedOracleSql));
  }

  @Test public void testSqlServerReplaceFunction() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RexNode replaceCall =
        builder.call(SqlLibraryOperators.CASE_INSENSTIVE_REPLACE,
            builder.field("ENAME"), builder.literal("A"), builder.literal("aa"));
    final RelNode root = builder.project(replaceCall).build();
    final String expectedSql = "SELECT REPLACE(\"ENAME\", 'A', 'aa') AS \"$f0\""
        + "\nFROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
  }

  @Disabled
  @Test void testInnerAndLeftJoinWithBooleanColumnEqualityConditionInWhereClause() {
    String query = "select \"first_name\" \n"
        + "from \"employee\" as \"emp\" , \"department\" as \"dept\" LEFT JOIN "
        + " \"product\" as \"p\" ON \"p\".\"product_id\" = \"dept\".\"department_id\""
        + " where \"p\".\"low_fat\" = true AND \"emp\".\"employee_id\" = 1";
    final String expected = "SELECT employee.first_name\n"
        + "FROM foodmart.employee\n"
        + "INNER JOIN foodmart.department ON TRUE\n"
        + "LEFT JOIN foodmart.product ON department.department_id = product.product_id\n"
        + "WHERE product.low_fat AND employee.employee_id = 1";
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(FilterExtractInnerJoinRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    RuleSet rules = RuleSets.ofList(CoreRules.FILTER_EXTRACT_INNER_JOIN_RULE);
    sql(query).withBigQuery().optimize(rules, hepPlanner).ok(expected);
  }

  @Test void testFilterExtractRuleWithDynamicVariable() {
    String query = "SELECT \"first_name\" \n"
        + "FROM \"employee\" AS \"emp\" "
        + ", \"department\" AS \"dept\" "
        + ", \"product\" AS \"p\" "
        + "WHERE \"p\".\"product_id\" = (?) "
        + "AND \"emp\".\"department_id\" = \"dept\".\"department_id\"";
    final String expected = "SELECT employee.first_name\n"
        + "FROM foodmart.employee\n"
        + "INNER JOIN foodmart.department ON employee.department_id = department.department_id\n"
        + "INNER JOIN foodmart.product ON TRUE\n"
        + "WHERE product.product_id = ?";
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(FilterExtractInnerJoinRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    RuleSet rules = RuleSets.ofList(CoreRules.FILTER_EXTRACT_INNER_JOIN_RULE);
    sql(query).withBigQuery().optimize(rules, hepPlanner).ok(expected);
  }

  @Test void testCorrelatedQueryHavingCorrelatedVariableLookedUpInWrongTable() {
    RelBuilder builder = foodmartRelBuilder();
    RelNode subQueryForCorrelatedVariableLookUp = builder.scan("employee")
        .project(builder.field("employee_id"),  builder.field("department_id"))
        .build();
    builder.push(subQueryForCorrelatedVariableLookUp);
    CorrelationId correlationId = builder.getCluster().createCorrel();
    RelDataType relDataType = builder.peek().getRowType();
    RexNode correlVariable = builder.getRexBuilder().makeCorrel(relDataType, correlationId);
    int departmentIdIndex = builder.field("department_id").getIndex();
    builder.build();

    //outer query Rel building
    builder.scan("employee");
    RelNode whereClauseSubQuery = builder
        .scan("department")
        .filter(builder
            .equals(
                builder.field("department_id"),
                builder.getRexBuilder().makeFieldAccess(correlVariable, departmentIdIndex)))
        .project(builder.field("department_id"))
        .build();
    RelNode root = builder
        .filter(
            ImmutableSet.of(correlationId), builder.call(SqlStdOperatorTable.NOT,
            RexSubQuery.exists(whereClauseSubQuery)))
        .project(builder.field("department_id"))
        .build();
    final String expectedSql = "SELECT \"department_id\"\n"
        + "FROM \"foodmart\".\"employee\"\n"
        + "WHERE NOT EXISTS (SELECT \"department_id\"\n"
        + "FROM \"foodmart\".\"department\"\nWHERE \"department_id\" = \"employee\".\"department_id\")";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
  }

  @Test public void testDateAddWithMilliSecondsInterval() {
    final RelBuilder builder = relBuilder();
    final RexNode intervalMillisecondsRex =
        builder.getRexBuilder().makeIntervalLiteral(new BigDecimal("70000000"),
        new SqlIntervalQualifier(SECOND, SECOND, SqlParserPos.ZERO));
    final RexNode divideIntervalRex =
        builder.call(SqlStdOperatorTable.DIVIDE, intervalMillisecondsRex,
        builder.literal(1000));
    final RexNode dateAddRexWithAlias =
        builder.alias(
            builder.call(DATE_ADD, builder.cast(builder.call(CURRENT_DATE), SqlTypeName.TIMESTAMP),
            divideIntervalRex), "add_interval_millis");
    final RelNode root = builder
        .scan("EMP")
        .project(dateAddRexWithAlias)
        .build();
    final String expectedBQSql = "SELECT DATETIME_ADD(CAST(CURRENT_DATE AS DATETIME), "
        + "INTERVAL CAST(70000000 / 1000 "
        + "AS INT64) SECOND) AS add_interval_millis\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQSql));
  }

  @Test void testOrConditionRedundantBracketAddition() {
    final RelBuilder builder = relBuilder();
    final RelNode root = builder
        .scan("EMP")
        .filter(
            builder.or(
                builder.equals(builder.field("EMPNO"), builder.literal(1)),
                builder.equals(builder.field("DEPTNO"), builder.literal(2)),
                builder.equals(builder.field("SAL"), builder.literal(1999)),
                builder.equals(builder.field("COMM"), builder.literal(500)),
                builder.equals(builder.field("EMPNO"), builder.literal(8)),
                builder.equals(builder.field("DEPTNO"), builder.literal(3))))
        .build();
    final String expectedSql = "SELECT *\n"
        + "FROM \"scott\".\"EMP\"\n"
        + "WHERE \"EMPNO\" IN (1, 8) OR \"DEPTNO\" IN (2, 3) OR \"SAL\" = 1999 OR \"COMM\" = 500";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
  }

  @Test public void testDatetimeAddWithMilliSecondsIntervalAndCurrentTimestamp() {
    final RelBuilder builder = relBuilder();
    final RexNode intervalMillisecondsRex =
        builder.getRexBuilder().makeIntervalLiteral(new BigDecimal("70000000"),
            new SqlIntervalQualifier(SECOND, SECOND, SqlParserPos.ZERO));
    final RexNode divideIntervalRex =
        builder.call(SqlStdOperatorTable.DIVIDE, intervalMillisecondsRex,
        builder.literal(1000));
    RelDataType relDataType =
        builder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP_WITH_TIME_ZONE);
    List<RexNode> operandList = new ArrayList<>(Collections.singletonList(builder.literal(0)));
    RexNode currentTimestampNode =
        builder.getRexBuilder().makeCall(relDataType, CURRENT_TIMESTAMP_WITH_TIME_ZONE,
            operandList);
    final RexNode datetimeAddRex =
        builder.call(PLUS, currentTimestampNode,
            divideIntervalRex);
    final RelNode root = builder
        .scan("EMP")
        .project(datetimeAddRex)
        .build();
    final String expectedBQSql = "SELECT DATETIME_ADD(CURRENT_TIMESTAMP(), INTERVAL CAST(70000000 "
        + "/ 1000 AS INT64) SECOND) AS `$f0`\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQSql));
  }

  @Test public void testRegexpExtractAllFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode regexpExtractNode =
        builder.call(SqlLibraryOperators.REGEXP_EXTRACT_ALL, builder.literal("TERADATA-BIGQUERY-SPARK-ORACLE"),
                builder.literal("[^-]+"));
    final RexNode arrayAccess = builder.call(SAFE_OFFSET, regexpExtractNode, builder.literal(2));
    final RelNode root = builder
        .values(new String[]{""}, 1)
        .project(builder.alias(arrayAccess, "ss"))
        .build();

    final String expectedBiqQuery = "SELECT "
        + "REGEXP_EXTRACT_ALL('TERADATA-BIGQUERY-SPARK-ORACLE', '[^-]+')[SAFE_OFFSET(2)] AS ss";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testEditDistanceFunctionWithTwoArgs() {
    final RelBuilder builder = relBuilder();
    final RexNode editDistanceRex =
        builder.call(SqlLibraryOperators.EDIT_DISTANCE, builder.literal("abc"), builder.literal("xyz"));
    final RelNode root = builder
        .scan("EMP")
        .project(editDistanceRex)
        .build();
    final String expectedBQQuery = "SELECT EDIT_DISTANCE('abc', 'xyz') AS `$f0`"
        + "\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQQuery));
  }

  @Test public void testEditDistanceFunctionWithSixArgs() {
    final RelBuilder builder = relBuilder();
    final RexNode editDistanceRex =
        builder.call(SqlLibraryOperators.EDIT_DISTANCE, builder.literal("PHONE"),
            builder.literal("FONE"),
            builder.literal(1),
            builder.literal(0),
            builder.literal(1),
            builder.literal(0));
    final RelNode root = builder
        .scan("EMP")
        .project(editDistanceRex)
        .build();
    final String expectedBQQuery = "SELECT EDIT_DISTANCE('PHONE', 'FONE', 1, 0, 1, 0) AS `$f0`"
        + "\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQQuery));
  }

  @Test public void testEditDistanceFunctionWithThreeArgs() {
    final RelBuilder builder = relBuilder();
    final RexNode editDistanceRex =
        builder.call(SqlLibraryOperators.EDIT_DISTANCE, builder.literal("abc"), builder.literal("xyz"),
        builder.literal(2));
    final RelNode root = builder
        .scan("EMP")
        .project(editDistanceRex)
        .build();
    final String expectedBqQuery = "SELECT EDIT_DISTANCE('abc', 'xyz', max_distance => 2) AS `$f0`"
        + "\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBqQuery));
  }

  @Test public void testCTEWithTraits() {
    final RelBuilder builder = foodmartRelBuilder();

    final RelNode rundate = builder.scan("employee")
        .project(builder.field("first_name"), builder.field("last_name"),
            builder.field("birth_date"))
        .aggregate(builder.groupKey(0, 1, 2))
        .build();

    // add CTE definition trait
    final CTEDefinationTrait cteTrait = new CTEDefinationTrait(true, "RUNDATE");
    final RelTraitSet cteRelTraitSet = rundate.getTraitSet().plus(cteTrait);
    final RelNode cteRelNodeWithRelTrait = rundate.copy(cteRelTraitSet, rundate.getInputs());

    final RelNode innerSelect = builder
        .push(cteRelNodeWithRelTrait)
        .project(
            builder.alias(
                builder.field("first_name"),
                "FNAME")).build();

    // add CTE Scope trait
    final CTEScopeTrait cteScopeTrait = new CTEScopeTrait(true);
    final RelTraitSet cteScopeRelTraitSet = innerSelect.getTraitSet().plus(cteScopeTrait);
    final RelNode cteScopeRelNodeWithRelTrait =
        innerSelect.copy(cteScopeRelTraitSet, innerSelect.getInputs());

    final String actualSql =
        toSql(cteScopeRelNodeWithRelTrait, DatabaseProduct.BIG_QUERY.getDialect());

    final String expectedSql = "WITH RUNDATE AS (SELECT first_name, last_name, birth_date\nFROM "
                                + "foodmart.employee\nGROUP BY first_name, last_name, birth_date)"
                                + " (SELECT first_name AS FNAME\nFROM RUNDATE)";
    assertThat(actualSql, isLinux(expectedSql));
  }

  @Test public void testGenerateUUID() {
    final RelBuilder builder = relBuilder();
    final RexNode generateUUID = builder.call(SqlLibraryOperators.GENERATE_UUID);
    final RelNode root = builder
        .scan("EMP")
        .project(generateUUID)
        .build();
    final String expectedBqQuery = "SELECT GENERATE_UUID() AS `$f0`"
        + "\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBqQuery));
  }

  @Test public void testDatetimeTrunc() {
    final RelBuilder builder = relBuilder();
    final RexNode trunc =
        builder.call(SqlLibraryOperators.DATETIME_TRUNC, builder.call(SqlStdOperatorTable.CURRENT_TIMESTAMP),
                builder.literal(DAY));
    final RelNode root = builder
        .scan("EMP")
        .project(trunc)
        .build();
    final String expectedBQSql = "SELECT DATETIME_TRUNC(CURRENT_DATETIME(), DAY)"
        + " AS `$f0`\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBQSql));
  }

  @Test public void testSnowflakeTrunc() {
    final RelBuilder builder = relBuilder();
    final RexNode trunc =
        builder.call(
            SqlLibraryOperators.SNOWFLAKE_TRUNC, builder.cast(builder.literal("12323.3434"),
                SqlTypeName.DECIMAL));
    final RelNode root = builder
        .scan("EMP")
        .project(trunc)
        .build();
    final String expectedSnowflakeSql = "SELECT TRUNC(12323.3434) AS \"$f0\"\nFROM \"scott\""
        + ".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedSnowflakeSql));
  }

  @Test public void testTimestampAdd() {
    final RelBuilder builder = relBuilder();
    final RexNode timeDayPart = builder.literal(DAY);
    final RexNode timeWeekPart = builder.literal(WEEK);
    final RexNode diffNode = builder.literal(-1);
    final RexNode toTimestampNode =
        builder.call(SqlLibraryOperators.TO_TIMESTAMP, builder.literal("2023-10-20"), builder.literal("yyyy-MM-dd"));
    final RexNode timestampNode =
        builder.call(SqlLibraryOperators.DATE_TRUNC, timeWeekPart, toTimestampNode);
    final RexNode timestampaddRex =
        builder.call(SqlLibraryOperators.TIMESTAMPADD_DATABRICKS, timeDayPart, diffNode, timestampNode);
    final RelNode root = builder
        .scan("EMP")
        .project(timestampaddRex)
        .build();
    final String expectedBqQuery = "SELECT TIMESTAMPADD(DAY, -1, DATE_TRUNC(WEEK, "
        + "TO_TIMESTAMP('2023-10-20', 'YYYY-MM-DD'))) AS `$f0`"
        + "\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBqQuery));
  }

  @Test public void testDiv0() {
    final RelBuilder builder = relBuilder();
    RexNode div0Rex =
        builder.call(SqlLibraryOperators.DIV0, builder.literal(120), builder.literal(0));

    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(div0Rex, "Result"))
        .build();
    final String expectedSnowFlakeQuery =
        "SELECT DIV0(120, 0) AS \"Result\"\nFROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()),
        isLinux(expectedSnowFlakeQuery));
  }

  @Test public void testForRegexpReplaceWithReplaceString() {
    final RelBuilder builder = relBuilder();
    final RexNode regexpReplaceRex =
        builder.call(
            SqlLibraryOperators.createUDFSqlFunction("REGEXP_REPLACE_UDF", ReturnTypes.VARCHAR_NULLABLE),
            builder.literal("Calcite"), builder.literal("te"),
                builder.literal("me"), builder.literal(1), builder.literal(0),
            builder.literal("i"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(regexpReplaceRex, "regexpReplace"))
        .build();

    final String expectedBiqQuery = "SELECT "
        + "udf_schema.REGEXP_REPLACE_UDF('Calcite', 'te', 'me', 1, 0, 'i') AS regexpReplace\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testForRegexpReplaceWithReplaceStringAsEmpty() {
    final RelBuilder builder = relBuilder();
    final RexNode regexpReplaceRex =
        builder.call(
            SqlLibraryOperators.createUDFSqlFunction("REGEXP_REPLACE_UDF", ReturnTypes.VARCHAR_NULLABLE),
            builder.literal("Calcite"), builder.literal("ac"),
                builder.literal(""), builder.literal(1), builder.literal(0),
            builder.literal("i"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(regexpReplaceRex, "regexpReplace"))
        .build();

    final String expectedBiqQuery = "SELECT "
        + "udf_schema.REGEXP_REPLACE_UDF('Calcite', 'ac', '', 1, 0, 'i') AS regexpReplace\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testForImplicitCastingForDateColumn() {
    final String query = "select \"employee_id\" "
        + "from \"foodmart\".\"employee\" "
        + "where \"birth_date\" = '0'";
    final String expected = "SELECT \"employee_id\"\n"
        + "FROM \"foodmart\".\"employee\"\n"
        + "WHERE \"birth_date\" = '0'";
    final String expectedBiqquery = "SELECT employee_id\n"
        + "FROM foodmart.employee\n"
        + "WHERE birth_date = CAST('0' AS DATE)";
    sql(query)
        .ok(expected)
        .withBigQuery()
        .ok(expectedBiqquery);
  }

  @Test public void testForImplicitCastingForDateTimeColumn() {
    final String query = "select \"employee_id\" "
        + "from \"foodmart\".\"employee\" "
        + "where \"hire_date\" = '0'";
    final String expected = "SELECT \"employee_id\"\n"
        + "FROM \"foodmart\".\"employee\"\n"
        + "WHERE \"hire_date\" = '0'";
    final String expectedBiqquery = "SELECT employee_id\n"
        + "FROM foodmart.employee\n"
        + "WHERE hire_date = CAST('0' AS DATETIME)";
    sql(query)
        .ok(expected)
        .withBigQuery()
        .ok(expectedBiqquery);
  }

  @Test public void testPostgresUnicodeString() {
    final String query = "select 'éléphant', 'é'";
    final String expected = "SELECT *\n"
        + "FROM (VALUES ('\\u00e9l\\u00e9phant', '\\u00e9'))"
        + " AS \"t\" (\"EXPR$0\", \"EXPR$1\")";
    final String expectedPgSql = "SELECT 'éléphant', 'é'";
    sql(query)
        .ok(expected)
        .withPostgresql()
        .ok(expectedPgSql);
  }

  @Test public void testArrayConcatAndArray() {
    final RelBuilder builder = relBuilder();
    final RelDataType stringValue = builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
    final RelDataType arrayRelDataType = builder.getTypeFactory().createArrayType(stringValue, -1);
    final RexNode arrayConcatRex =
        builder.call(
            SqlLibraryOperators.ARRAY_CONCAT, builder.getRexBuilder().makeCall(arrayRelDataType,
                        SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, new ArrayList<>()),
        builder.call(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
            builder.literal("A"), builder.literal("B")));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(arrayConcatRex, "ARRY_CONCAT"))
        .build();

    final String expectedBiqQuery = "SELECT "
        + "ARRAY_CONCAT(ARRAY[], ARRAY['A', 'B']) AS ARRY_CONCAT\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testFromTimezoneFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode fromTimezoneNode =
            builder.call(SqlLibraryOperators.FROM_TZ, builder.literal("2008-08-21 07:23:54"),
                    builder.literal("America/Los_Angeles"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(fromTimezoneNode, "Datetime"))
        .build();
    final String expectedBqQuery =
        "SELECT FROM_TZ('2008-08-21 07:23:54', 'America/Los_Angeles') AS Datetime\nFROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBqQuery));
  }

  @Test public void testForRegexpContainsWithString() {
    final RelBuilder builder = relBuilder();
    final RexNode regexpContainsRex =
        builder.call(SqlLibraryOperators.REGEXP_CONTAINS, builder.literal("Calcite"),
                builder.literal("^[0-9]*$"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(regexpContainsRex, "regexpContains0"))
        .build();

    final String expectedBiqQuery = "SELECT "
        + "REGEXP_CONTAINS('Calcite', r'^[0-9]*$') AS regexpContains0\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testToClobFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode toClobRex =
            builder.call(SqlLibraryOperators.TO_CLOB, builder.literal("^XYZ\\$"));
    final RelNode root = builder
            .scan("EMP")
            .project(toClobRex)
            .build();

    final String expectedBiqQuery = "SELECT "
            + "TO_CLOB('^XYZ\\\\$') AS `$f0`\n"
            + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  private RelNode createRelNodeWithQualifyStatement() {
    RelBuilder builder = relBuilder().scan("EMP");
    RexNode aggregateFunRexNode = builder.call(SqlStdOperatorTable.MAX, builder.field(0));
    RelDataType type = aggregateFunRexNode.getType();
    RexFieldCollation orderKeys =
        new RexFieldCollation(builder.field("HIREDATE"),
        ImmutableSet.of());
    final RexNode analyticalFunCall =
        builder.getRexBuilder().makeOver(type, SqlStdOperatorTable.MAX,
        ImmutableList.of(builder.field(0)), ImmutableList.of(), ImmutableList.of(orderKeys),
        RexWindowBounds.UNBOUNDED_PRECEDING,
        RexWindowBounds.UNBOUNDED_FOLLOWING,
        true, true, false, false, false);
    final RexNode equalsNode =
        builder.getRexBuilder().makeCall(EQUALS,
            new RexInputRef(1, analyticalFunCall.getType()), builder.literal(1));
    return builder
        .project(builder.field("HIREDATE"), builder.alias(analyticalFunCall, "EXPR$"))
        .filter(equalsNode)
        .project(builder.field("HIREDATE"))
        .build();
  }

  @Test public void testForXMLElementFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode xmlElementRex =
        builder.call(SqlLibraryOperators.XMLELEMENT, builder.literal("EMPLOYEE_NAME"), builder.scan("EMP").field(1));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(xmlElementRex, "xmlElement"))
        .build();

    final String expectedOracleQuery = "SELECT "
        + "XMLELEMENT('EMPLOYEE_NAME', \"ENAME\") \"xmlElement\"\n"
        + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.ORACLE.getDialect()), isLinux(expectedOracleQuery));
  }

  @Test public void testOracleTableFunctions() {
    final RelBuilder builder = relBuilder();
    final RelNode inStringRel = builder
        .functionScan(SqlLibraryOperators.IN_STRING, 0,
            builder.literal("abc"))
        .build();
    final RelNode inNumberRel = builder
        .functionScan(SqlLibraryOperators.IN_NUMBER, 0,
            builder.literal(2))
        .build();

    final String inStringSql = "SELECT *\n"
        + "FROM TABLE(IN_STRING('abc'))";

    final String inNumberSql = "SELECT *\n"
        + "FROM TABLE(IN_NUMBER(2))";

    assertThat(toSql(inStringRel, DatabaseProduct.ORACLE.getDialect()), isLinux(inStringSql));
    assertThat(toSql(inNumberRel, DatabaseProduct.ORACLE.getDialect()), isLinux(inNumberSql));
  }

  @Test public void testPeriodValueFunctions() {
    final RelBuilder builder = relBuilder();
    final RexNode date1 = builder.literal(new DateString("2000-01-01"));
    final RexNode date2 = builder.literal(new DateString("2000-10-01"));
    final RexNode periodConstructor = builder.call(PERIOD_CONSTRUCTOR, date1, date2);
    RelNode root = builder.scan("EMP").project(periodConstructor).build();
    final String teradataPeriod = "SELECT PERIOD(DATE '2000-01-01', DATE '2000-10-01') AS \"$f0\""
        + "\nFROM \"scott\".\"EMP\"";
    final String bigQueryPeriod = "SELECT RANGE(DATE '2000-01-01', DATE '2000-10-01') AS `$f0`"
        + "\nFROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.TERADATA.getDialect()), isLinux(teradataPeriod));
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(bigQueryPeriod));
  }

  @Test public void testPeriodIntersectFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode date = builder.literal(new DateString("2000-01-01"));
    final RexNode periodConstructor1 = builder.call(PERIOD_CONSTRUCTOR, date, date);
    final RexNode periodConstructor2 =
        builder.call(PERIOD_CONSTRUCTOR, date, builder.call(CURRENT_DATE));
    final RexNode periodIntersect =
        builder.call(PERIOD_INTERSECT, periodConstructor1, periodConstructor2);
    RelNode root = builder.scan("EMP").project(periodIntersect).build();
    final String teradataPeriod =
        "SELECT RANGE_INTERSECT(RANGE(DATE '2000-01-01', DATE '2000-01-01'), "
            + "RANGE(DATE '2000-01-01', CURRENT_DATE)) AS `$f0`"
            + "\nFROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(teradataPeriod));
  }

  @Test public void testRangeArrayFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode date = builder.literal(new DateString("2000-01-01"));
    final RexNode periodConstructor =
        builder.call(PERIOD_CONSTRUCTOR, date, builder.call(CURRENT_DATE));
    RexNode intervalDay =
        builder.getRexBuilder().makeIntervalLiteral(BigDecimal.valueOf(2),
            new SqlIntervalQualifier(MONTH, null, SqlParserPos.ZERO));
    final RexNode generateRangeCall =
        builder.call(SqlLibraryOperators.GENERATE_RANGE_ARRAY, periodConstructor, intervalDay,
            builder.literal(false));
    RelNode root = builder.scan("EMP").project(generateRangeCall).build();
    final String teradataPeriod =
        "SELECT GENERATE_RANGE_ARRAY(RANGE(DATE '2000-01-01', CURRENT_DATE), "
            + "INTERVAL 2 MONTH, FALSE) AS `$f0`\n"
            + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(teradataPeriod));
  }

  @Test public void testGenerateArrayFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode generateArrayCall =
        builder.call(GENERATE_ARRAY, builder.literal(1), builder.literal(5));
    RelNode root = builder.push(LogicalValues.createOneRow(builder.getCluster()))
        .project(generateArrayCall)
        .build();
    final String bigqueryGenerateArray = "SELECT GENERATE_ARRAY(1, 5) AS `$f0`";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(bigqueryGenerateArray));
  }

  /*NEXT VALUE is a SqlSequenceValueOperator which works on sequence generator.
  As of now, we don't have any sequence generator present in calcite, nor do we have the complete
  implementation to create one. It can be implemented later on using SqlKind.CREATE_SEQUENCE
  For now test for NEXT_VALUE has been added using the literal "EMP_SEQ" as an argument.*/
  @Test public void testNextValueAndCurrentValueFunction() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RexNode nextValueRex =
            builder.call(SqlStdOperatorTable.NEXT_VALUE, builder.literal("EMP_SEQ"));
    final RexNode currentValueRex =
        builder.call(SqlStdOperatorTable.CURRENT_VALUE, builder.literal("EMP_SEQ"));

    final RelNode root = builder
        .project(nextValueRex, currentValueRex)
        .build();

    final String expectedSql = "SELECT NEXT VALUE FOR 'EMP_SEQ' AS \"$f0\", CURRENT VALUE FOR"
        +  " 'EMP_SEQ' AS \"$f1\"\n"
        +  "FROM \"scott\".\"EMP\"";
    final String expectedPG = "SELECT NEXTVAL('EMP_SEQ') AS \"$f0\", "
        + "CURRVAL('EMP_SEQ') AS \"$f1\"\n"
        + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root), isLinux(expectedSql));
    assertThat(toSql(root, DatabaseProduct.POSTGRESQL.getDialect()), isLinux(expectedPG));
  }

  /**
   * The creation of RelNode of this test case mirrors the rel building process in the MIG side
   * for the query below. However, it's important to note that this approach may not perfectly
   * align with how Calcite would generate the plan for the same.
   *
   * SELECT q.product_id from (SELECT product.product_id FROM foodmart.product JOIN
   * foodmart.employee ON product.product_id =
   * (SELECT department_id FROM foodmart.department WHERE employee.employee_id = department_id)) q
   * where exists (select * from foodmart.department where department.department_id = q.product_id);
   *
   * Test case for
   * <a href= https://datametica.atlassian.net/browse/RSFB-2814>[RSFB-2814] Nullpointer in
   * correlated EXISTS query</a>
   */
  @Test void testCorrelatedOuterExistsHavingCorrelatedSubqueryInJoinCondition() {
    final RelBuilder builder = foodmartRelBuilder();

    //building the derived table/sub query table
    RelNode leftTable = toLogical(builder.scan("product").build(), builder);
    RelNode rightTable = toLogical(builder.scan("employee").build(), builder);
    //building correlated sub query in join condition
    final Holder<RexCorrelVariable> v = Holder.of(null);
    RelNode correlatedJoinSubqueryRel = builder.push(rightTable)
        .variable(v)
        .push(toLogical(builder.scan("department").build(), builder))
        .filter(builder.call(EQUALS, builder.field(0), builder.field(v.get(), "employee_id")))
        .project(builder.alias(builder.field(0), "a123"))
        .build();

    //creating LogicalCorrelate using right table and correlated sub query since there is a
    // correlation
    final ImmutableBitSet.Builder requiredColumns = ImmutableBitSet.builder();
    requiredColumns.set(0);
    LogicalCorrelate correlate =
        LogicalCorrelate.create(rightTable, correlatedJoinSubqueryRel, v.get().id,
        requiredColumns.build(), JoinRelType.LEFT);

    builder.clear();
    builder.push(correlatedJoinSubqueryRel);
    builder.push(correlate);

    builder.clear();
    builder.push(leftTable);
    builder.push(correlate);
    int joinSubqueryProjectionItemIndex = correlate.getRowType().getFieldCount() - 1;
    //join Condition is created using field references of product_id and department_id in query
    // of join condition
    RexNode joinCondition =
        builder.call(
            EQUALS, builder.field(1), builder.field(2, 1,
        joinSubqueryProjectionItemIndex));
    RelNode joinRel = builder.join(JoinRelType.INNER, joinCondition).build();

    RelNode innerSubqueryRel = builder.push(joinRel)
        .project(builder.field(1))
        .build();

    /**
     * starting to build main query which contains correlated Exists
     * creating a correlated reference to innerSubqueryRel's field to be used in EXISTS filter
     * condition
     */

    CorrelationId correlationId = builder.getCluster().createCorrel();
    RelDataType relDataType = innerSubqueryRel.getRowType();
    RexNode correlVariable = builder.getRexBuilder().makeCorrel(relDataType, correlationId);

    RelNode whereClauseSubQuery = builder
        .push(toLogical(builder.scan("department").build(), builder))
        .filter(builder
            .equals(
                builder.field(0),
                builder.getRexBuilder().makeFieldAccess(correlVariable, 0)))
        .build();

    RelNode finalRel = builder.push(innerSubqueryRel)
        .filter(ImmutableSet.of(correlationId), RexSubQuery.exists(whereClauseSubQuery))
        .project(builder.field(0))
        .build();

    /**
     * Rel Before Optimization
     *
     * LogicalFilter(condition=[EXISTS({
     * LogicalFilter(condition=[=($0, $cor1.product_id)])
     *   LogicalTableScan(table=[[foodmart, department]])
     * })], variablesSet=[[$cor1]])
     *   LogicalProject(product_id=[$1])
     *     LogicalJoin(condition=[=($1, $32)], joinType=[inner])
     *       LogicalTableScan(table=[[foodmart, product]])
     *       LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0}])
     *         LogicalTableScan(table=[[foodmart, employee]])
     *         LogicalProject(a123=[$0])
     *           LogicalFilter(condition=[=($0, $cor0.employee_id)])
     *             LogicalTableScan(table=[[foodmart, department]])
     */

    //applying certain optimization rules
    Collection<RelOptRule> rules = new ArrayList<>();
    rules.add((FilterProjectTransposeRule.Config.DEFAULT).toRule());
    rules.add((JoinToCorrelateRule.Config.DEFAULT).toRule());
    HepProgram hepProgram = new HepProgramBuilder().addRuleCollection(rules).build();
    HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(finalRel);
    RelNode optimizedRel = hepPlanner.findBestExp();
    RelNode decorrelatedRel = RelDecorrelator.decorrelateQuery(optimizedRel, builder);

    final String expectedSql = "SELECT \"t0\".\"product_id\"\n"
        + "FROM (SELECT *\nFROM \"foodmart\".\"product\"\n"
        + "WHERE EXISTS (SELECT *\n"
        + "FROM \"foodmart\".\"department\"\n"
        + "WHERE \"department_id\" = \"product\".\"product_id\")) AS \"t0\"\n"
        + "INNER JOIN (SELECT \"employee\".\"employee_id\", \"employee\".\"full_name\", \"employee\".\"first_name\", \"employee\".\"last_name\", \"employee\".\"position_id\", \"employee\".\"position_title\", \"employee\".\"store_id\", \"employee\".\"department_id\", \"employee\".\"birth_date\", \"employee\".\"hire_date\", \"employee\".\"end_date\", \"employee\".\"salary\", \"employee\".\"supervisor_id\", \"employee\".\"education_level\", \"employee\".\"marital_status\", \"employee\".\"gender\", \"employee\".\"management_role\", CAST(\"t1\".\"a123\" AS INTEGER) AS \"a123\", CAST(\"t1\".\"department_id\" AS INTEGER) AS \"department_id0\"\n"
        + "FROM \"foodmart\".\"employee\"\n"
        + "INNER JOIN (SELECT \"department_id\" AS \"a123\", \"department_id\"\n"
        + "FROM \"foodmart\".\"department\") AS \"t1\" ON \"employee\".\"employee_id\" = \"t1\".\"department_id\") AS \"t2\" ON \"t0\".\"product_id\" = \"t2\".\"a123\"";
    assertThat(toSql(decorrelatedRel, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
  }

  private static RelNode toLogical(RelNode rel, RelBuilder builder) {
    return rel.accept(new ToLogicalConverter(builder));
  }

  @Test public void testSubQueryRemoveRuleForSubqueryWithInClause() {
    final RelBuilder builder = relBuilder();
    final RelNode subQueryInClause = builder.scan("DEPT")
        .project(builder.field("DEPTNO"), builder.field("DNAME"))
        .build();

    final RelNode root = builder.scan("EMP")
        .scan("DEPT")
        .join(JoinRelType.INNER,
            builder.and(
                builder.call(EQUALS,
                    builder.field(2, 0, "DEPTNO"),
                    builder.field(2, 1, "DEPTNO")),
                RexSubQuery.in(
                    subQueryInClause, ImmutableList.of(builder.field(0),
                        builder.field(1)))))
        .build();

    final String expected = "SELECT EMP.EMPNO, EMP.ENAME, EMP.JOB, EMP.MGR, EMP.HIREDATE, EMP.SAL, "
        + "EMP.COMM, EMP.DEPTNO, DEPT.DEPTNO AS DEPTNO0, DEPT.DNAME, DEPT.LOC\n"
        + "FROM scott.EMP\n"
        + "INNER JOIN (scott.DEPT INNER JOIN (SELECT DEPTNO, DNAME\n"
        + "FROM scott.DEPT) AS t ON TRUE) ON EMP.DEPTNO = DEPT.DEPTNO "
        + "AND EMP.EMPNO = t.DEPTNO AND EMP.ENAME = t.DNAME";

    HepProgram hepProgram = new HepProgramBuilder()
        .addRuleInstance(SubQueryRemoveRule.Config.JOIN.toRule())
        .build();
    HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(root);
    RelNode optimizedRel = hepPlanner.findBestExp();

    assertThat(toSql(optimizedRel, DatabaseProduct.BIG_QUERY.getDialect()),
        isLinux(expected));
  }

  @Test public void testOracleFirstDay() {
    RelBuilder relBuilder = relBuilder().scan("EMP");
    final RexNode literalTimestamp = relBuilder.call(SqlStdOperatorTable.CURRENT_TIMESTAMP);
    RexNode firstDayNode = relBuilder.call(SqlLibraryOperators.FIRST_DAY, literalTimestamp);
    RelNode root = relBuilder
        .project(firstDayNode)
        .build();
    final String expectedDB2Sql = "SELECT FIRST_DAY(CURRENT_TIMESTAMP) AS $f0\n"
        + "FROM scott.EMP AS EMP";

    assertThat(toSql(root, DatabaseProduct.DB2.getDialect()), isLinux(expectedDB2Sql));
  }

  @Test public void testBQUnaryOperators() {
    final RelBuilder builder = relBuilder().scan("EMP");

    RexNode isNullNode = builder.call(SqlStdOperatorTable.IS_NULL, builder.field(0));
    RexNode greaterThanNode =
        builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field(0), builder.literal(10));

    final RexNode andNode = builder.call(SqlStdOperatorTable.AND, isNullNode, greaterThanNode);
    final LogicalProject projectionNode =
        LogicalProject.create(builder.build(), ImmutableList.of(),
            Lists.newArrayList(builder.call(SqlStdOperatorTable.IS_FALSE, andNode),
                builder.call(SqlStdOperatorTable.IS_NOT_FALSE, greaterThanNode)),
            ImmutableList.of("a", "b"),
            ImmutableSet.of());
    final RelNode root = builder
        .push(projectionNode)
        .build();
    final String expectedBiqQuery = "SELECT (EMPNO IS NULL AND EMPNO > 10) IS FALSE AS a, "
        + "(EMPNO > 10) IS NOT FALSE AS b\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testSparkUnaryOperators() {
    final RelBuilder builder = relBuilder().scan("EMP");
    RexNode inClauseNode =
        builder.call(SqlStdOperatorTable.IN, builder.field(0),
            builder.literal(10), builder.literal(20));
    RexNode likeNode =
        builder.call(SqlStdOperatorTable.LIKE, builder.field(1),
            builder.literal("abC"));
    RexNode conditionNode =
        RexUtil.composeConjunction(builder.getRexBuilder(),
            Arrays.asList(builder.call(SqlStdOperatorTable.IS_NOT_FALSE, inClauseNode),
            builder.call(SqlStdOperatorTable.IS_NOT_FALSE, likeNode)));
    RelNode filterNode =
        LogicalFilter.create(builder.build(), conditionNode);

    final RelNode root = builder
        .push(filterNode)
        .build();
    final String expectedBiqQuery = "SELECT *\n"
        + "FROM scott.EMP\n"
        + "WHERE (EMPNO IN (10, 20)) IS NOT FALSE AND (ENAME LIKE 'abC') IS NOT FALSE";
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test void testForRegressionInterceptFunction() {
    final RelBuilder builder = relBuilder().scan("EMP");
    final RelBuilder.AggCall aggCall =
        builder.aggregateCall(SqlLibraryOperators.REGR_INTERCEPT, builder.literal(12),
            builder.literal(25));
    final RelNode rel = builder
        .aggregate(relBuilder().groupKey(), aggCall)
        .build();
    final String expectedBigQuery = "SELECT REGR_INTERCEPT(12, 25) AS \"$f0\"\n"
        + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(rel, DatabaseProduct.TERADATA.getDialect()), isLinux(expectedBigQuery));
  }

  @Test public void testUserDefinedType() {
    RelBuilder builder = relBuilder();
    RelDataTypeFactory typeFactory = builder.getTypeFactory();
    RelDataType udt = createUserDefinedType(typeFactory);
    RexNode udtConstructor =
        builder.call(COLUMN_LIST,
            builder.literal("street_name"),
            builder.literal("city_name"),
            builder.literal(10),
            builder.literal("state"));
    RelNode root = builder.scan("EMP")
        .project(
            builder.alias(
                builder.getRexBuilder().makeAbstractCast(udt, udtConstructor), "address"),
            builder.field("EMPNO"))
        .filter(builder.equals(builder.field("EMPNO"), builder.literal(1)))
        .project(builder.getRexBuilder()
            .makeFieldAccess(builder.field(0), "STREET", true))
        .build();

    final String expectedPostgres = "SELECT (\"address\").\"STREET\" AS \"$f0\"\n"
        + "FROM (SELECT CAST(ROW ('street_name', 'city_name', 10, 'state') AS \"ADDRESS\") AS \"address\", \"EMPNO\"\n"
        + "FROM \"scott\".\"EMP\") AS \"t\"\nWHERE \"EMPNO\" = 1";

    assertThat(toSql(root, DatabaseProduct.POSTGRESQL.getDialect()), isLinux(expectedPostgres));
  }

  private static RelDataType createUserDefinedType(RelDataTypeFactory typeFactory) {
    final RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR, 20);
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    return typeFactory.createStructuredTypeWithName(
        Arrays.asList(varcharType, varcharType, intType, varcharType),
        Arrays.asList("STREET", "CITY", "ZIP", "STATE"),
        Collections.singletonList("ADDRESS"));
  }

  @Test public void testParseDateFunctionWithConcat() {
    final RelBuilder builder = relBuilder();
    final RexNode formatRexNode =
        builder.call(SqlLibraryOperators.FORMAT, builder.literal("%11d"), builder.literal(200802));
    final RexNode concatRexNode =
        builder.call(SqlStdOperatorTable.CONCAT, builder.literal("01"), formatRexNode);
    final RexNode toDateNode =
        builder.call(SqlLibraryOperators.PARSE_DATE, builder.literal("MMYYYYDD"), concatRexNode);
    RelNode root = builder
        .scan("EMP")
        .project(builder.alias(toDateNode, "date_value"))
        .build();
    final String expectedSql =
        "SELECT PARSE_DATE('%m%Y%d', '01' || 200802) AS date_value"
            + "\nFROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedSql));
  }

  @Test public void filterMergerWithQualifyReltrait() {
    final RelBuilder builder = relBuilder();

    final RelNode rundate = builder.scan("DEPT")
        .project(builder.field("DNAME"), builder.field("DEPTNO"))
        .filter(
            builder.call(EQUALS,
                builder.field("DNAME"), builder.literal("ABC")))
        .filter(
            builder.call(SqlStdOperatorTable.GREATER_THAN,
                builder.field("DEPTNO"), builder.literal(2)))
        .build();

    final QualifyRelTrait qualifyTrait = new QualifyRelTrait("QUALIFY");
    final RelTraitSet qualifyTraitSet = rundate.getTraitSet().plus(qualifyTrait);
    final RelNode qualifyRelNodeWithRel = rundate.copy(qualifyTraitSet, rundate.getInputs());

    QualifyRelTrait relTrait =
        qualifyRelNodeWithRel.getTraitSet().getTrait(QualifyRelTraitDef.instance);
    RelNode optimizedRel = null;
    if (relTrait.getClauseName().equalsIgnoreCase("QUALIFY")) {
      Collection<RelOptRule> rules = new ArrayList<>();
      rules.add((FilterMergeRule.Config.DEFAULT).toRule());
      HepProgram hepProgram = new HepProgramBuilder().addRuleCollection(rules).build();
      HepPlanner hepPlanner = new HepPlanner(hepProgram);
      hepPlanner.setRoot(qualifyRelNodeWithRel);
      optimizedRel = hepPlanner.findBestExp();
    }
    final String actualSql =
        toSql(optimizedRel, DatabaseProduct.BIG_QUERY.getDialect());

    final String expectedSql = "SELECT *\nFROM (SELECT DNAME, DEPTNO\nFROM scott.DEPT) AS "
        + "t\nWHERE DNAME = 'ABC' AND DEPTNO > 2";
    assertThat(actualSql, isLinux(expectedSql));
  }

  @Test public void viewWithProjectRelTrait() {
    final RelBuilder builder = relBuilder();

    final RelNode rundate = builder.scan("DEPT")
        .project(builder.field("DNAME"), builder.field("DEPTNO"))
        .build();

    final ViewChildProjectRelTrait projectViewTrait = new ViewChildProjectRelTrait(true, false, false);
    final RelTraitSet projectTraitSet = rundate.getTraitSet().plus(projectViewTrait);
    final RelNode qualifyRelNodeWithRel = rundate.copy(projectTraitSet, rundate.getInputs());

    final String actualSql =
        toSql(qualifyRelNodeWithRel, DatabaseProduct.BIG_QUERY.getDialect());

    final String expectedSql = "SELECT DNAME, DEPTNO\nFROM scott.DEPT";
    assertThat(actualSql, isLinux(expectedSql));
  }

  @Test public void testDDMMYYYYHH24AndYYMMDDHH24MISSFormat() {
    final RelBuilder builder = relBuilder();
    final RexNode parseTSNode1 =
            builder.call(SqlLibraryOperators.PARSE_TIMESTAMP_WITH_TIMEZONE,
                    builder.literal("DDMMYYYYHH24"), builder.literal("2015-09-11-09:07:23"));
    final RexNode parseTSNode2 =
            builder.call(SqlLibraryOperators.PARSE_TIMESTAMP_WITH_TIMEZONE,
                    builder.literal("YYMMDDHH24MISS"), builder.literal("2015-09-11-09:07:23"));
    final RelNode root = builder
            .scan("EMP")
            .project(builder.alias(parseTSNode1, "ddmmyyyyhh24"), builder.alias(parseTSNode2, "yymmddhh24miss"))

            .build();
    final String expectedBiqQuery =
            "SELECT PARSE_TIMESTAMP('%d%m%Y%H', '2015-09-11-09:07:23') AS ddmmyyyyhh24, PARSE_TIMESTAMP('%y%m%d%H%M%S', '2015-09-11-09:07:23') AS yymmddhh24miss\n"
                    + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testExtract2() {
    final RelBuilder builder = relBuilder();
    final RexNode extractIsoweekRexNode =
            builder.call(SqlLibraryOperators.EXTRACT2, builder.literal(TimeUnitRange.YEAR),
                    builder.call(SqlStdOperatorTable.CURRENT_TIMESTAMP));
    final RelNode root = builder
            .scan("EMP")
            .project(builder.alias(extractIsoweekRexNode, "year"))
            .build();

    final String expectedBiqQuery = "SELECT CAST(EXTRACT(YEAR FROM CURRENT_DATETIME()) AS FLOAT64) AS year\n"
                                           + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testPower1() {
    final RelBuilder builder = relBuilder();
    final RexNode extractIsoweekRexNode =
        builder.call(SqlLibraryOperators.POWER1, builder.literal(10), builder.literal(2));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(extractIsoweekRexNode, "POWER_RESULT"))
        .build();

    final String expectedBiqQuery = "SELECT POWER1(10, 2) AS POWER_RESULT\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testDbName() {
    final RelBuilder builder = relBuilder();
    final RexNode dbNameRexNode = builder.call(SqlLibraryOperators.DB_NAME);
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(dbNameRexNode, "db_name_alias"))
        .build();

    final String expectedMsSqlQuery = "SELECT DB_NAME() AS [db_name_alias]\n"
        + "FROM [scott].[EMP]";
    assertThat(toSql(root, DatabaseProduct.MSSQL.getDialect()), isLinux(expectedMsSqlQuery));
  }

  @Test public void testEnDashSpecialChar() {
    RelBuilder relBuilder = relBuilder().scan("EMP");
    final RexNode endashLiteral = relBuilder.literal("–");
    RelNode root = relBuilder
        .project(endashLiteral)
        .build();
    final String expectedBigQuerySql = "SELECT _UTF-16LE'–' AS `$f0`\n"
        + "FROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBigQuerySql));
  }

  @Test public void testRegexSubstrFunction() {
    RelBuilder relBuilder = relBuilder().scan("EMP");
    final RexNode substrCall =
        relBuilder.call(REGEXP_SUBSTR, relBuilder.literal("choco chico chipo"),
        relBuilder.literal(".*cho*p*c*?.*"));
    RelNode root = relBuilder
        .project(substrCall)
        .build();
    final String expectedQuery = "SELECT REGEXP_SUBSTR('choco chico chipo', '.*cho*p*c*?.*') "
        + "\"$f0\"\n"
        + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.ORACLE.getDialect()), isLinux(expectedQuery));
  }

  @Test public void testFloorSpark() {
    final RelBuilder builder = relBuilder();
    final RexNode floor =
        builder.call(SqlLibraryOperators.FLOOR, builder.literal(541.7));
    final RelNode root = builder.scan("EMP")
        .project(builder.alias(floor, "floor")).build();

    final String expectedSql = "SELECT FLOOR(541.7) floor\nFROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSql));
  }

  @Test public void testCeilingSpark() {
    final RelBuilder builder = relBuilder();
    final RexNode ceiling =
        builder.call(SqlLibraryOperators.CEILING, builder.literal(541.7));
    final RelNode root = builder.scan("EMP")
        .project(builder.alias(ceiling, "ceiling")).build();

    final String expectedSql = "SELECT CEILING(541.7) ceiling\nFROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSql));
  }

  @Test public void testGroupByWithQualifyHavingRankFunction() {
    final RelBuilder builder = foodmartRelBuilder();
    final RelDataType rankRelDataType =
        builder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);

    RelNode relNode = builder
        .scan("employee")
        .project(
            builder.field("department_id"),
            builder.field("employee_id"),
            builder.field("salary"))
        .aggregate(builder.groupKey(0, 1, 2))
        .build();

    // add GroupByWithQualifyHavingRANK Trait
    final GroupByWithQualifyHavingRankRelTrait qualifyTrait =
        new GroupByWithQualifyHavingRankRelTrait("RANK");
    final RelTraitSet qualifyTRelTraitSet = relNode.getTraitSet().plus(qualifyTrait);
    final RelNode qualifyRelNodeWithRelTrait =
        relNode.copy(qualifyTRelTraitSet, relNode.getInputs());

    RelNode finalRex = builder.push(qualifyRelNodeWithRelTrait)
        .filter(
            builder.equals(
                builder.getRexBuilder().makeOver(rankRelDataType,
                    SqlStdOperatorTable.RANK, ImmutableList.of(),
                    ImmutableList.of(builder.field("department_id")),
                    ImmutableList.of(
                        new RexFieldCollation(builder.field("salary"),
                            Collections.singleton(SqlKind.NULLS_LAST))),
                    RexWindowBounds.UNBOUNDED_PRECEDING, RexWindowBounds.UNBOUNDED_FOLLOWING, true,
                    true, false, false, false),
                builder.literal(1)))
        .project(
            builder.field("department_id"),
            builder.field("employee_id"))
        .build();

    final String expectedBiqQuery = "SELECT department_id, employee_id\n"
        +
        "FROM foodmart.employee\n"
        +
        "GROUP BY department_id, employee_id, salary\n"
        +
        "QUALIFY (RANK() OVER (PARTITION BY department_id ORDER BY salary IS NULL, salary)) = 1";
    assertThat(toSql(finalRex, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testWithRegrAvgx() {
    RelBuilder relBuilder = relBuilder().scan("EMP");
    final RexNode regrAVGCall = relBuilder
        .call(SqlLibraryOperators.REGR_AVGX,
            relBuilder.literal(122),
            relBuilder.literal(2));
    RelNode root = relBuilder
        .project(regrAVGCall)
        .build();
    final String expectedBigQuerySql = "SELECT REGR_AVGX(122, 2) AS \"$f0\"\n"
        + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedBigQuerySql));
  }

  @Test public void testWithRegrAvgy() {
    RelBuilder relBuilder = relBuilder().scan("EMP");
    final RexNode regrAVGCall = relBuilder
        .call(SqlLibraryOperators.REGR_AVGY,
            relBuilder.literal(122),
            relBuilder.literal(2));
    RelNode root = relBuilder
        .project(regrAVGCall)
        .build();
    final String expectedBigQuerySql = "SELECT REGR_AVGY(122, 2) AS \"$f0\"\n"
        + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedBigQuerySql));
  }

  @Test public void testWithRegrIntercept() {
    RelBuilder relBuilder = relBuilder().scan("EMP");
    final RexNode regrAVGCall = relBuilder
        .call(SqlLibraryOperators.REGR_INTERCEPT,
            relBuilder.literal(122),
            relBuilder.literal(2));
    RelNode root = relBuilder
        .project(regrAVGCall)
        .build();
    final String expectedBigQuerySql = "SELECT REGR_INTERCEPT(122, 2) AS \"$f0\"\n"
        + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedBigQuerySql));
  }

  @Test public void testWithOracleRatioToReport() {
    RelBuilder builder = relBuilder().scan("EMP");
    RexNode ratioToReport =
        builder.call(SqlLibraryOperators.RATIO_TO_REPORT, builder.field(0));
    final RexNode overCall = builder.getRexBuilder()
        .makeOver(ratioToReport.getType(), SqlLibraryOperators.RATIO_TO_REPORT,
            ImmutableList.of(), ImmutableList.of(), ImmutableList.of(),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            true, true, false, false, false);
    RelNode root = builder
        .project(overCall)
        .build();
    final String expactedOracleSql =
        "SELECT RATIO_TO_REPORT() OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \"$f0\"\n"
            + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.ORACLE.getDialect()), isLinux(expactedOracleSql));
  }

  @Test void testCaseExpressionInParseTimestamp() {
    final RelBuilder builder = relBuilder().scan("DEPT");
    final RexNode firstCondition =
        builder.call(SqlStdOperatorTable.LESS_THAN,
            builder.field("DEPTNO"), builder.literal(2));
    final RexNode secondCondition =
        builder.call(SqlStdOperatorTable.GREATER_THAN,
            builder.field("DEPTNO"), builder.literal(2));
    final RexNode elseRexNode = builder.literal("YYYY-MM-DD HH24");
    final RexNode caseOperandRexNode =
        builder.call(SqlStdOperatorTable.CASE, firstCondition,
            builder.literal("YYYY-MM-DD HH24:MI:SS"),
            secondCondition, builder.literal("YYYY-MM-DD HH24:MI"), elseRexNode);
    final RexNode parseTSNode1 =
        builder.call(SqlLibraryOperators.PARSE_TIMESTAMP_WITH_TIMEZONE,
            builder.literal("2024-07-02 13:45:30"), caseOperandRexNode);
    final RelNode root = builder
        .scan("DEPT")
        .project(builder.alias(parseTSNode1, "timestamp_value"))
        .build();

    final String expectedBiqQuery =
        "SELECT PARSE_TIMESTAMP('2024-07-02 13:45:30', "
            + "CASE WHEN DEPTNO < 2 THEN 'YYYY-MM-DD HH24:MI:SS' "
            + "WHEN DEPTNO > 2 THEN 'YYYY-MM-DD HH24:MI' "
            + "ELSE 'YYYY-MM-DD HH24' END) AS timestamp_value"
            + "\nFROM scott.DEPT";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testToTimestamp2Function() {
    final RelBuilder builder = relBuilder();
    final RexNode parseTSNode1 =
        builder.call(SqlLibraryOperators.TO_TIMESTAMP2, builder.literal("2009-03-20 12:25:50"),
            builder.literal("yyyy-MM-dd HH24:MI:SS"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(parseTSNode1, "timestamp_value"))
        .build();
    final String expectedSql =
        "SELECT TO_TIMESTAMP2('2009-03-20 12:25:50', 'yyyy-MM-dd HH24:MI:SS') AS "
            + "\"timestamp_value\"\nFROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.POSTGRESQL.getDialect()), isLinux(expectedSql));
  }

  @Test public void testJsonQueryFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode jsonQueryNode =
        builder.call(SqlLibraryOperators.JSON_QUERY,
            builder.literal("{\"class\": {\"students\": [{\"name\": \"Jane\"}]}}"),
            builder.literal("\\$.class"));
    final RelNode root = builder
        .scan("EMP")
        .project(jsonQueryNode)
        .build();
    final String expectedBigquery = "SELECT JSON_QUERY('{\"class\": {\"students\":"
        + " [{\"name\": \"Jane\"}]}}', '\\\\$.class') AS `$f0`\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBigquery));
  }

  @Test public void testTeradataJsonExtractFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode jsonCheckNode =
        builder.call(SqlLibraryOperators.JSONEXTRACT, builder.literal("{\"name\": \"Bob\",\"Jane\"}"),
        builder.literal("$.name"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(jsonCheckNode, "json_data"))
        .build();
    final String expectedTeraDataQuery = "SELECT JSONEXTRACT('{\"name\": \"Bob\",\"Jane\"}', '$"
        + ".name') AS \"json_data\"\n"
        + "FROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.TERADATA.getDialect()), isLinux(expectedTeraDataQuery));
  }

  @Test public void testFloorFunctionForSnowflake() {
    final RelBuilder builder = relBuilder();
    final RexNode parseTSNode1 =
        builder.call(SqlLibraryOperators.SF_FLOOR, builder.literal("2009.34343"));
    final RelNode root = builder
        .scan("EMP")
        .project(parseTSNode1)
        .build();
    final String expectedSql =
        "SELECT FLOOR('2009.34343') AS "
            + "\"$f0\"\nFROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedSql));
  }

  @Test public void testIntervalYearMonthFunction() {
    String query = "select \"birth_date\" - INTERVAL -'3-9' YEAR TO MONTH from \"employee\"";

    final String expectedSpark = "SELECT birth_date - (INTERVAL '-3' YEAR + INTERVAL '-9' MONTH)"
        + "\nFROM foodmart.employee";
    sql(query)
        .withSpark()
        .ok(expectedSpark);
  }

  @Test public void testToLocalTimestampFunction() {
    final RelBuilder builder = relBuilder();
    RelDataType relDataType =
        builder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP);
    final RexNode currentTimestampRexNode =
        builder.getRexBuilder().makeCall(relDataType,
            CURRENT_TIMESTAMP, Collections.singletonList(builder.literal(8)));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(currentTimestampRexNode, "CT"))
        .build();
    final String expectedSpark = "SELECT CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss"
        + ".SSSSSSSS') AS TIMESTAMP) CT\nFROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
  }

  @Test public void testMakeInterval() {
    final RelBuilder builder = relBuilder();
    final RexNode makeIntervalRexNode =
        builder.call(SqlLibraryOperators.MAKE_INTERVAL,
            builder.literal(1), builder.literal(2), builder.literal(3),
            builder.literal(4), builder.literal(5), builder.literal(6));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(makeIntervalRexNode, "interval_example"))
        .build();
    final String expectedBigQuery = "SELECT MAKE_INTERVAL(1, 2, 3, 4, 5, 6) AS interval_example\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBigQuery));
  }

  @Test public void testDateTruncFunction() {
    final RelBuilder builder = relBuilder();
    RexNode timeUnitRex = builder.literal(TimeUnit.DAY);
    final RexNode dateTrunc =
        builder.call(SqlLibraryOperators.DATETRUNC, timeUnitRex, builder.call(CURRENT_TIMESTAMP));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(dateTrunc, "day"))
        .build();
    final String expectedSql = "SELECT DATETRUNC(DAY, GETDATE()) AS [day]"
        + "\nFROM [scott].[EMP]";

    assertThat(toSql(root, DatabaseProduct.MSSQL.getDialect()), isLinux(expectedSql));
  }

  @Test public void testRowCountFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode rex = builder.call(SqlLibraryOperators.ROW_COUNT);
    final RelNode root = builder
        .scan("EMP")
        .project(rex)
        .build();
    final String expectedQuery = "SELECT ROW_COUNT() AS \"$f0\"\nFROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedQuery));
  }

  @Test public void testCurrentJobIdFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode rex =
        builder.literal(
            builder.call(SqlLibraryOperators.CURRENT_JOB_ID).toString().replace("()",
            ""));
    final RelNode root = builder
        .scan("EMP")
        .project(rex)
        .build();
    final String expectedQuery = "SELECT 'CURRENT_JOB_ID' AS \"$f0\"\nFROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedQuery));
  }

  @Test public void testSQLERRMFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode rex =
        builder.literal(
            builder.call(SqlLibraryOperators.GENERATE_SQLERRM).toString().replace("()",
            ""));
    final RelNode root = builder
        .scan("EMP")
        .project(rex)
        .build();
    final String expectedQuery = "SELECT 'SQLERRM' AS \"$f0\"\nFROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedQuery));
  }

  @Test public void testSQLERRCFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode rex =
        builder.literal(
            builder.call(SqlLibraryOperators.GENERATE_SQLERRC).toString().replace("()",
            ""));
    final RelNode root = builder
        .scan("EMP")
        .project(rex)
        .build();
    final String expectedQuery = "SELECT 'SQLERRC' AS \"$f0\"\nFROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedQuery));
  }

  @Test public void testSQLERRSTFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode rex =
        builder.literal(
            builder.call(SqlLibraryOperators.GENERATE_SQLERRST).toString().replace("()",
            ""));
    final RelNode root = builder
        .scan("EMP")
        .project(rex)
        .build();
    final String expectedQuery = "SELECT 'SQLERRST' AS \"$f0\"\nFROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedQuery));
  }

  @Test public void testJsonExtractArrayFunction() {
    String jsonString = "{\"numbers\": [1, 2, 3]}";
    String jsonPath = "$.numbers";
    final RelBuilder builder = relBuilder();
    final RexNode parseTSNode1 =
        builder.call(SqlLibraryOperators.JSON_EXTRACT_ARRAY, builder.literal(jsonString), builder.literal(jsonPath));
    final RelNode root = builder
        .scan("EMP")
        .project(parseTSNode1)
        .build();
    final String expectedSql =
        "SELECT JSON_EXTRACT_ARRAY('{\"numbers\": [1, 2, 3]}', '$.numbers') AS "
            + "\"$f0\"\nFROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
  }

  @Test public void testUUIDSpark() {
    final RelBuilder builder = relBuilder();
    final RexNode uuid = builder.call(SqlLibraryOperators.UUID);
    final RelNode root = builder
        .scan("EMP")
        .project(uuid)
        .build();
    final String expectedSpark = "SELECT UUID() $f0"
        + "\nFROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.SPARK.getDialect()), isLinux(expectedSpark));
  }

  /**
   * The creation of RelNode of this test case mirrors the rel building process in the MIG
   * side for the query below. This approach may not perfectly align with how Calcite would
   * generate the plan for the same.
   * Test case for - NullPointerException: variable $cor0 is not found
   * <a href= https://datametica.atlassian.net/browse/RGS-51>[RSFB-51] </a>
   * */
  @Test void testCorrelateWithInnerJoin() {
    final RelBuilder builder = foodmartRelBuilder();
    RelNode leftTable = builder.scan("product").build();
    RelNode rightTable = builder.scan("employee").build();
    RelNode join = builder.push(leftTable).push(rightTable).join(JoinRelType.INNER).build();

    // Create correlated EXISTS sub query
    CorrelationId correlationId = builder.getCluster().createCorrel();
    RexNode correlVar = builder.getRexBuilder().makeCorrel(leftTable.getRowType(), correlationId);
    RelNode existsSubquery = builder.scan("product")
        .filter(
            builder.equals(builder.field(1),
            builder.getRexBuilder().makeFieldAccess(correlVar, 1)))
        .project(builder.alias(builder.literal("X"), "EXPR$40249"))
        .build();

    // Filter with EXISTS
    RelNode filteredRel = builder.push(join)
        .filter(ImmutableSet.of(correlationId), RexSubQuery.exists(existsSubquery))
        .build();

    // Projection
    RelNode relNode = builder.push(filteredRel)
        .project(builder.field(1))
        .build();

    /** REL BEFORE OPTIMIZATION
     * LogicalProject(product_id=[$1])
     *   LogicalFilter(condition=[EXISTS({
     * LogicalProject($f0=['X'])
     *   LogicalFilter(condition=[=($1, $cor0.product_id)])
     *     JdbcTableScan(table=[[foodmart, product]])
     * })], variablesSet=[[$cor0]])
     *     LogicalJoin(condition=[true], joinType=[inner])
     *       JdbcTableScan(table=[[foodmart, product]])
     *       JdbcTableScan(table=[[foodmart, employee]])
     **/

    // Applying rel optimization
    Collection<RelOptRule> rules = new ArrayList<>();
    rules.add((FilterExtractInnerJoinRule.Config.DEFAULT).toRule());
    HepProgram hepProgram = new HepProgramBuilder().addRuleCollection(rules).build();
    HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(relNode);
    RelNode optimizedRel = hepPlanner.findBestExp();
    RelNode decorrelatedRel = RelDecorrelator.decorrelateQuery(optimizedRel, builder);

    final String expectedSql = "SELECT \"product\".\"product_id\"\n"
        + "FROM \"foodmart\".\"product\",\n"
        + "\"foodmart\".\"employee\"\n"
        + "WHERE EXISTS (SELECT 'X'\n"
        + "FROM \"foodmart\".\"product\"\n"
        + "WHERE \"product_id\" = \"product\".\"product_id\")";

    assertThat(toSql(decorrelatedRel, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
  }

  @Test public void testObjectSchemaName() {
    final RelBuilder builder = relBuilder();
    final RexNode dbNameRexNode = builder.call(SqlLibraryOperators.OBJECT_SCHEMA_NAME, builder.literal(12345));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(dbNameRexNode, "schema_name_alias"))
        .build();

    final String expectedMsSqlQuery = "SELECT OBJECT_SCHEMA_NAME(12345) AS [schema_name_alias]\n"
        + "FROM [scott].[EMP]";
    assertThat(toSql(root, DatabaseProduct.MSSQL.getDialect()), isLinux(expectedMsSqlQuery));
  }

  @Test public void testNvl2Function() {
    final RelBuilder builder = relBuilder();
    final RexNode nvl2Call =
        builder.call(SqlLibraryOperators.NVL2, builder.literal(null), builder.literal(0),
            builder.literal(1));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(nvl2Call, "bool_check"))
        .build();

    final String expectedMsSqlQuery = "SELECT NVL2(NULL, 0, 1) \"bool_check\"\n"
        + "FROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.ORACLE.getDialect()), isLinux(expectedMsSqlQuery));
  }

  @Test public void testCollateFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode collateRexNode =
        builder.call(SqlLibraryOperators.COLLATE, builder.literal("John"),
            builder.literal("en-ci"));
    final RelNode root = builder
        .scan("EMP")
        .project(collateRexNode)
        .build();
    final String expectedMsSqlQuery = "SELECT COLLATE('John', 'en-ci') AS \"$f0\"\n"
        + "FROM \"scott\".\"EMP\"";
    assertThat(toSql(root, DatabaseProduct.SNOWFLAKE.getDialect()), isLinux(expectedMsSqlQuery));
  }

  @Test public void testHashBytesFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode rexNode =
        builder.call(SqlLibraryOperators.HASHBYTES, builder.literal("SHA1"), builder.literal("dfdd76d7vb"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(rexNode, "HashValue"))
        .build();

    final String expectedMsSqlQuery = "SELECT HASHBYTES('SHA1', 'dfdd76d7vb') AS [HashValue]\n"
        + "FROM [scott].[EMP]";
    assertThat(toSql(root, DatabaseProduct.MSSQL.getDialect()), isLinux(expectedMsSqlQuery));
  }

  @Test public void testProjectWithCastAndCastOperandUsedInGroupBy() {
    final RelBuilder builder = foodmartRelBuilder();
    builder.scan("employee");
    RexNode literalRex = builder.alias(builder.literal(10), "EXPR$123");
    RexNode functionRex =
        builder.alias(
            builder.call(SqlStdOperatorTable.CONCAT, builder.field("employee_id"),
            builder.field("department_id")), "EXPR$456");

    RelNode relNode = builder
        .project(literalRex, functionRex)
        .aggregate(builder.groupKey(0, 1))
        .project(
            builder.alias(
                builder.cast(
                    builder.cast(builder.field(0), SqlTypeName.DECIMAL,
                        38, 0), SqlTypeName.INTEGER), "EXPR$123"),
            builder.alias(
                builder.cast(builder.field(1),
            SqlTypeName.VARCHAR, 10), "EXPR$456"))
        .build();

    final String expectedBiqQuery = "SELECT CAST(CAST(10 AS NUMERIC) AS INT64), "
        + "CAST(employee_id || department_id AS STRING)\n"
        + "FROM foodmart.employee\nGROUP BY 1, 2";

    assertThat(toSql(relNode, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedBiqQuery));
  }

  @Test public void testErrorMessageFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode errorMessage =
        builder.call(SqlLibraryOperators.ERROR_MESSAGE);
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(errorMessage, "errorMessage"))
        .build();
    final String expectedSql = "SELECT ERROR_MESSAGE() AS [errorMessage]"
        + "\nFROM [scott].[EMP]";

    assertThat(toSql(root, DatabaseProduct.MSSQL.getDialect()), isLinux(expectedSql));
  }

  @Test public void testErrorMessageIdFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode errorMessageId =
        builder.call(SqlLibraryOperators.ERROR_MESSAGE_ID);
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(errorMessageId, "errorMessageId"))
        .build();
    final String expectedSql = "SELECT @@ERROR.MESSAGE AS errorMessageId"
        + "\nFROM scott.EMP";

    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedSql));
  }

  @Test public void testObjectIdFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode objectId =
        builder.call(SqlLibraryOperators.OBJECT_ID, builder.literal("#schema1.table1"));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(objectId, "objectId"))
        .build();
    final String expectedSql = "SELECT OBJECT_ID('#schema1.table1') AS [objectId]"
        + "\nFROM [scott].[EMP]";

    assertThat(toSql(root, DatabaseProduct.MSSQL.getDialect()), isLinux(expectedSql));
  }

  @Test public void testMsSqlFormatFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode formatIntegerPaddingRexNode =
        builder.call(SqlLibraryOperators.MSSQL_FORMAT,
            builder.literal("1234"), builder.literal("00000"));

    final RelNode root = builder
        .scan("EMP")
        .project(formatIntegerPaddingRexNode)
        .build();

    final String expectedPostgresQuery = "SELECT FORMAT('1234', '00000') AS [$f0]"
        + "\nFROM [scott].[EMP]";
    assertThat(toSql(root, DatabaseProduct.MSSQL.getDialect()), isLinux(expectedPostgresQuery));
  }

  @Test public void testSTRFunction() {
    final RelBuilder builder = relBuilder();
    final RexNode strNode =
        builder.call(SqlLibraryOperators.STR, builder.literal(-123.45), builder.literal(8),
            builder.literal(1));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(strNode, "Result"))
        .build();

    final String expectedMsSqlQuery = "SELECT STR(-123.45, 8, 1) AS [Result]\n"
        + "FROM [scott].[EMP]";
    assertThat(toSql(root, DatabaseProduct.MSSQL.getDialect()), isLinux(expectedMsSqlQuery));
  }

  @Test public void testDateAddWithMinusInterval() {
    final RelBuilder builder = relBuilder();
    RexBuilder rexBuilder = builder.getRexBuilder();
    RelDataTypeFactory typeFactory = builder.getTypeFactory();

    RexNode concatYear =
        builder.call(SqlStdOperatorTable.CONCAT,
        builder.literal("19"),
        builder.literal("25"));

    RexNode castYear =
        rexBuilder.makeAbstractCast(typeFactory.createSqlType(SqlTypeName.INTEGER),
        concatYear);

    RexNode baseDate =
        builder.call(SqlLibraryOperators.DATE,
        castYear,
        builder.literal(1),
        builder.literal(1));


    RexNode castDay =
        rexBuilder.makeAbstractCast(typeFactory.createSqlType(SqlTypeName.INTEGER),
        builder.literal("087"));

    RexNode intervalExpr =
        builder.call(SqlStdOperatorTable.MINUS,
        castDay,
        builder.literal(1));

    RexNode dateAdd =
        builder.call(SqlLibraryOperators.DATE_ADD,
        baseDate,
        intervalExpr);

    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(dateAdd, "JULIANDATE"))
        .build();

    final String expectedSql = "SELECT "
        + "DATE_ADD(DATE(CAST('19' || '25' AS INT64), 1, 1), INTERVAL (87 - 1) DAY) AS JULIANDATE\n"
        + "FROM scott.EMP";
    assertThat(toSql(root, DatabaseProduct.BIG_QUERY.getDialect()), isLinux(expectedSql));
  }

  @Test public void testHashamp() {
    final RelBuilder builder = relBuilder();
    final RexNode formatDateRexNode = builder.call(SqlLibraryOperators.HASHAMP, builder.scan("EMP").field(0));
    final RelNode root = builder
        .scan("EMP")
        .project(builder.alias(formatDateRexNode, "FD"))
        .build();
    final String expectedSql = "SELECT HASHAMP(\"EMPNO\") AS \"FD\"\nFROM \"scott\".\"EMP\"";

    assertThat(toSql(root, DatabaseProduct.CALCITE.getDialect()), isLinux(expectedSql));
  }
}
