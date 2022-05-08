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

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.schema.impl.TableFunctionImpl;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.impl.ViewTableMacro;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.test.schemata.hr.HrSchema;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunner;
import org.apache.calcite.tools.RelRunners;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Smalls;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.apache.calcite.test.Matchers.hasHints;
import static org.apache.calcite.test.Matchers.hasTree;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit test for {@link RelBuilder}.
 *
 * <p>Tasks:</p>
 * <ol>
 *   <li>Add RelBuilder.scan(List&lt;String&gt;)</li>
 *   <li>Add RelBuilder.scan(Table)</li>
 *   <li>Test that {@link RelBuilder#filter} does not create a filter if the
 *   predicates optimize to true</li>
 *   <li>Test that {@link RelBuilder#filter} DOES create a filter if the
 *   predicates optimize to false. (Creating an empty Values seems too
 *   devious.)</li>
 *   <li>Test that {@link RelBuilder#scan} throws good error if table not
 *   found</li>
 *   <li>Test that {@link RelBuilder#scan} obeys case-sensitivity</li>
 *   <li>Test that {@link RelBuilder#join(JoinRelType, String...)} obeys
 *   case-sensitivity</li>
 *   <li>Test RelBuilder with alternative factories</li>
 *   <li>Test that {@link RelBuilder#field(String)} obeys case-sensitivity</li>
 *   <li>Test case-insensitive unique field names</li>
 *   <li>Test that an alias created using
 *      {@link RelBuilder#alias(RexNode, String)} is removed if not a top-level
 *      project</li>
 *   <li>{@link RelBuilder#aggregate} with grouping sets</li>
 *   <li>Add call to create {@link TableFunctionScan}</li>
 *   <li>Add call to create {@link Window}</li>
 *   <li>Add call to create {@link TableModify}</li>
 *   <li>Add call to create {@link Exchange}</li>
 *   <li>Add call to create {@link Correlate}</li>
 *   <li>Add call to create {@link AggregateCall} with filter</li>
 * </ol>
 */
public class RelBuilderTest {
  /** Creates a config based on the "scott" schema. */
  public static Frameworks.ConfigBuilder config() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(
            CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL))
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  /** Creates a config builder that will contain a view, "MYVIEW", and also
   * the SCOTT JDBC schema, whose tables implement
   * {@link org.apache.calcite.schema.TranslatableTable}. */
  static Frameworks.ConfigBuilder expandingConfig(Connection connection)
      throws SQLException {
    final CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    final SchemaPlus root = calciteConnection.getRootSchema();
    CalciteAssert.SchemaSpec spec = CalciteAssert.SchemaSpec.SCOTT;
    CalciteAssert.addSchema(root, spec);
    final String viewSql =
        String.format(Locale.ROOT, "select * from \"%s\".\"%s\" where 1=1",
            spec.schemaName, "EMP");

    // create view
    ViewTableMacro macro = ViewTable.viewMacro(root, viewSql,
        Collections.singletonList("test"), Arrays.asList("test", "view"), false);

    // register view (in root schema)
    root.add("MYVIEW", macro);

    return Frameworks.newConfigBuilder().defaultSchema(root);
  }

  /** Creates a RelBuilder with default config. */
  static RelBuilder createBuilder() {
    return createBuilder(c -> c);
  }

  /** Creates a RelBuilder with transformed config. */
  static RelBuilder createBuilder(UnaryOperator<RelBuilder.Config> transform) {
    final Frameworks.ConfigBuilder configBuilder = config();
    configBuilder.context(
        Contexts.of(transform.apply(RelBuilder.Config.DEFAULT)));
    return RelBuilder.create(configBuilder.build());
  }

  @Test void testScan() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    final RelNode root =
        RelBuilder.create(config().build())
            .scan("EMP")
            .build();
    assertThat(root,
        hasTree("LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  @Test void testScanQualifiedTable() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM "scott"."emp"
    final RelNode root =
        RelBuilder.create(config().build())
            .scan("scott", "EMP")
            .build();
    assertThat(root,
        hasTree("LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  @Test void testScanInvalidTable() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM zzz
    try {
      final RelNode root =
          RelBuilder.create(config().build())
              .scan("ZZZ") // this relation does not exist
              .build();
      fail("expected error, got " + root);
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Table 'ZZZ' not found"));
    }
  }

  @Test void testScanInvalidSchema() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM "zzz"."emp"
    try {
      final RelNode root =
          RelBuilder.create(config().build())
              .scan("ZZZ", "EMP") // the table exists, but the schema does not
              .build();
      fail("expected error, got " + root);
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Table 'ZZZ.EMP' not found"));
    }
  }

  @Test void testScanInvalidQualifiedTable() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM "scott"."zzz"
    try {
      final RelNode root =
          RelBuilder.create(config().build())
              .scan("scott", "ZZZ") // the schema is valid, but the table does not exist
              .build();
      fail("expected error, got " + root);
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Table 'scott.ZZZ' not found"));
    }
  }

  @Test void testScanValidTableWrongCase() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM "emp"
    try {
      final RelNode root =
          RelBuilder.create(config().build())
              .scan("emp") // the table is named 'EMP', not 'emp'
              .build();
      fail("Expected error (table names are case-sensitive), but got " + root);
    } catch (Exception e) {
      assertThat(e.getMessage(), is("Table 'emp' not found"));
    }
  }

  @Test void testScanFilterTrue() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   WHERE TRUE
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .filter(builder.literal(true))
            .build();
    assertThat(root,
        hasTree("LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  @Test void testScanFilterTriviallyFalse() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   WHERE 1 = 2
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .filter(builder.equals(builder.literal(1), builder.literal(2)))
            .build();
    assertThat(root,
        hasTree("LogicalValues(tuples=[[]])\n"));
  }

  @Test void testScanFilterEquals() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   WHERE deptno = 20
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .filter(
                builder.equals(builder.field("DEPTNO"), builder.literal(20)))
            .build();
    final String expected = "LogicalFilter(condition=[=($7, 20)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testScanFilterGreaterThan() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   WHERE deptno > 20
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .filter(
                builder.greaterThan(builder.field("DEPTNO"), builder.literal(20)))
            .build();
    final String expected = "LogicalFilter(condition=[>($7, 20)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testSnapshotTemporalTable() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM products_temporal FOR SYSTEM_TIME AS OF TIMESTAMP '2011-07-20 12:34:56'
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("products_temporal")
            .snapshot(
                builder.getRexBuilder().makeTimestampLiteral(
                    new TimestampString("2011-07-20 12:34:56"), 0))
            .build();
    final String expected = "LogicalSnapshot(period=[2011-07-20 12:34:56])\n"
            + "  LogicalTableScan(table=[[scott, products_temporal]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testTableFunctionScan() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM TABLE(
    //       DEDUP(CURSOR(select * from emp),
    //             CURSOR(select * from DEPT), 'NAME'))
    final RelBuilder builder = RelBuilder.create(config().build());
    final SqlOperator dedupFunction =
        new MockSqlOperatorTable.DedupFunction();
    RelNode root = builder.scan("EMP")
        .scan("DEPT")
        .functionScan(dedupFunction, 2, builder.cursor(2, 0),
            builder.cursor(2, 1))
        .build();
    final String expected = "LogicalTableFunctionScan("
        + "invocation=[DEDUP(CURSOR($0), CURSOR($1))], "
        + "rowType=[RecordType(VARCHAR(1024) NAME)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));

    // Make sure that the builder's stack is empty.
    try {
      RelNode node = builder.build();
      fail("expected error, got " + node);
    } catch (NoSuchElementException e) {
      assertNull(e.getMessage());
    }
  }

  @Test void testTableFunctionScanZeroInputs() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM TABLE(RAMP(3))
    final RelBuilder builder = RelBuilder.create(config().build());
    final SqlOperator rampFunction = new MockSqlOperatorTable.RampFunction();
    RelNode root = builder.functionScan(rampFunction, 0, builder.literal(3))
        .build();
    final String expected = "LogicalTableFunctionScan(invocation=[RAMP(3)], "
        + "rowType=[RecordType(INTEGER I)])\n";
    assertThat(root, hasTree(expected));

    // Make sure that the builder's stack is empty.
    try {
      RelNode node = builder.build();
      fail("expected error, got " + node);
    } catch (NoSuchElementException e) {
      assertNull(e.getMessage());
    }
  }

  /** Tests scanning a table function whose row type is determined by parsing a
   * JSON argument. The arguments must therefore be available at prepare
   * time. */
  @Test void testTableFunctionScanDynamicType() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM TABLE("dynamicRowType"('{nullable:true,fields:[...]}', 3))
    final RelBuilder builder = RelBuilder.create(config().build());
    final Method m = Smalls.DYNAMIC_ROW_TYPE_TABLE_METHOD;
    final TableFunction tableFunction =
        TableFunctionImpl.create(m.getDeclaringClass(), m.getName());
    final SqlOperator operator =
        new SqlUserDefinedTableFunction(
            new SqlIdentifier("dynamicRowType", SqlParserPos.ZERO),
            SqlKind.OTHER_FUNCTION, ReturnTypes.CURSOR, InferTypes.ANY_NULLABLE,
            Arg.metadata(
                Arg.of("count", f -> f.createSqlType(SqlTypeName.INTEGER),
                    SqlTypeFamily.INTEGER, false),
                Arg.of("typeJson", f -> f.createSqlType(SqlTypeName.VARCHAR),
                    SqlTypeFamily.STRING, false)),
            tableFunction);

    final String jsonRowType = "{\"nullable\":false,\"fields\":["
        + "  {\"name\":\"i\",\"type\":\"INTEGER\",\"nullable\":false},"
        + "  {\"name\":\"d\",\"type\":\"DATE\",\"nullable\":true}"
        + "]}";
    final int rowCount = 3;
    RelNode root = builder.functionScan(operator, 0,
        builder.literal(jsonRowType), builder.literal(rowCount))
        .build();
    final String expected = "LogicalTableFunctionScan("
        + "invocation=[dynamicRowType('{\"nullable\":false,\"fields\":["
        + "  {\"name\":\"i\",\"type\":\"INTEGER\",\"nullable\":false},"
        + "  {\"name\":\"d\",\"type\":\"DATE\",\"nullable\":true}]}', 3)], "
        + "rowType=[RecordType(INTEGER i, DATE d)])\n";
    assertThat(root, hasTree(expected));

    // Make sure that the builder's stack is empty.
    try {
      RelNode node = builder.build();
      fail("expected error, got " + node);
    } catch (NoSuchElementException e) {
      assertNull(e.getMessage());
    }
  }

  @Test void testJoinTemporalTable() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM orders
    //   JOIN products_temporal FOR SYSTEM_TIME AS OF TIMESTAMP '2011-07-20 12:34:56'
    //   ON orders.product = products_temporal.id
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("orders")
            .scan("products_temporal")
            .snapshot(
                builder.getRexBuilder().makeTimestampLiteral(
                    new TimestampString("2011-07-20 12:34:56"), 0))
            .join(JoinRelType.INNER,
                builder.equals(builder.field(2, 0, "PRODUCT"),
                    builder.field(2, 1, "ID")))
            .build();
    final String expected = "LogicalJoin(condition=[=($2, $4)], joinType=[inner])\n"
        + "  LogicalTableScan(table=[[scott, orders]])\n"
        + "  LogicalSnapshot(period=[2011-07-20 12:34:56])\n"
        + "    LogicalTableScan(table=[[scott, products_temporal]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Tests that {@link RelBuilder#project} simplifies expressions if and only if
   * {@link RelBuilder.Config#simplify}. */
  @Test void testSimplify() {
    checkSimplify(c -> c.withSimplify(true),
        hasTree("LogicalProject($f0=[true])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"));
    checkSimplify(c -> c,
        hasTree("LogicalProject($f0=[true])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"));
    checkSimplify(c -> c.withSimplify(false),
        hasTree("LogicalProject($f0=[IS NOT NULL($0)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  private void checkSimplify(UnaryOperator<RelBuilder.Config> transform,
      Matcher<RelNode> matcher) {
    final RelBuilder builder = createBuilder(transform);
    final RelNode root =
        builder.scan("EMP")
            .project(builder.isNotNull(builder.field("EMPNO")))
            .build();
    assertThat(root, matcher);
  }

  @Test void testScanFilterOr() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   WHERE (deptno = 20 OR comm IS NULL) AND mgr IS NOT NULL
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .filter(
                builder.or(
                    builder.equals(builder.field("DEPTNO"),
                        builder.literal(20)),
                    builder.isNull(builder.field(6))),
                builder.isNotNull(builder.field(3)))
            .build();
    final String expected = ""
        + "LogicalFilter(condition=[AND(OR(=($7, 20), IS NULL($6)), IS NOT NULL($3))])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testScanFilterOr2() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   WHERE deptno = 20 OR deptno = 20
    // simplifies to
    //   SELECT *
    //   FROM emp
    //   WHERE deptno = 20
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .filter(
                builder.or(
                    builder.greaterThan(builder.field("DEPTNO"),
                        builder.literal(20)),
                    builder.greaterThan(builder.field("DEPTNO"),
                        builder.literal(20))))
            .build();
    final String expected = "LogicalFilter(condition=[>($7, 20)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testScanFilterAndFalse() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   WHERE deptno = 20 AND FALSE
    // simplifies to
    //   VALUES
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .filter(
                builder.greaterThan(builder.field("DEPTNO"),
                    builder.literal(20)),
                builder.literal(false))
            .build();
    final String expected = "LogicalValues(tuples=[[]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testScanFilterAndTrue() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   WHERE deptno = 20 AND TRUE
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .filter(
                builder.greaterThan(builder.field("DEPTNO"),
                    builder.literal(20)),
                builder.literal(true))
            .build();
    final String expected = "LogicalFilter(condition=[>($7, 20)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2730">[CALCITE-2730]
   * RelBuilder incorrectly simplifies a filter with duplicate conjunction to
   * empty</a>. */
  @Test void testScanFilterDuplicateAnd() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   WHERE deptno > 20 AND deptno > 20 AND deptno > 20
    final RelBuilder builder = RelBuilder.create(config().build());
    builder.scan("EMP");
    final RexNode condition =
        builder.greaterThan(builder.field("DEPTNO"), builder.literal(20));
    final RexNode condition2 =
        builder.lessThan(builder.field("DEPTNO"), builder.literal(30));
    final RelNode root = builder.filter(condition, condition, condition)
        .build();
    final String expected = "LogicalFilter(condition=[>($7, 20)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));

    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   WHERE deptno > 20 AND deptno < 30 AND deptno > 20
    final RelNode root2 = builder.scan("EMP")
        .filter(condition, condition2, condition, condition)
        .build();
    final String expected2 = ""
        + "LogicalFilter(condition=[SEARCH($7, Sarg[(20..30)])])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root2, hasTree(expected2));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4325">[CALCITE-4325]
   * RexSimplify incorrectly simplifies complex expressions with Sarg and
   * NULL</a>. */
  @Test void testFilterAndOrWithNull() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   WHERE (deptno <> 20 OR deptno IS NULL) AND deptno = 10
    // Should be simplified to:
    //   SELECT *
    //   FROM emp
    //   WHERE deptno = 10
    // With [CALCITE-4325], is incorrectly simplified to:
    //   SELECT *
    //   FROM emp
    //   WHERE deptno = 10 OR deptno IS NULL
    final Function<RelBuilder, RelNode> f = b ->
        b.scan("EMP")
            .filter(
                b.and(
                    b.or(
                        b.notEquals(b.field("DEPTNO"), b.literal(20)),
                        b.isNull(b.field("DEPTNO"))),
                    b.equals(b.field("DEPTNO"), b.literal(10))))
            .build();

    final String expected = "LogicalFilter(condition=[=($7, 10)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));
  }

  @Test void testFilterAndOrWithNull2() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   WHERE (deptno = 20 OR deptno IS NULL) AND deptno = 10
    // Should be simplified to:
    //   No rows (WHERE FALSE)
    // With [CALCITE-4325], is incorrectly simplified to:
    //   SELECT *
    //   FROM emp
    //   WHERE deptno IS NULL
    final Function<RelBuilder, RelNode> f = b ->
        b.scan("EMP")
            .filter(
                b.and(
                    b.or(b.equals(b.field("DEPTNO"), b.literal(20)),
                        b.isNull(b.field("DEPTNO"))),
                    b.equals(b.field("DEPTNO"), b.literal(10))))
            .build();

    final String expected = "LogicalValues(tuples=[[]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));
  }

  @Test void testBadFieldName() {
    final RelBuilder builder = RelBuilder.create(config().build());
    try {
      RexInputRef ref = builder.scan("EMP").field("deptno");
      fail("expected error, got " + ref);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("field [deptno] not found; input fields are: [EMPNO, ENAME, JOB, "
              + "MGR, HIREDATE, SAL, COMM, DEPTNO]"));
    }
  }

  @Test void testBadFieldOrdinal() {
    final RelBuilder builder = RelBuilder.create(config().build());
    try {
      RexInputRef ref = builder.scan("DEPT").field(20);
      fail("expected error, got " + ref);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("field ordinal [20] out of range; "
                  + "input fields are: [DEPTNO, DNAME, LOC]"));
    }
  }

  @Test void testBadType() {
    final RelBuilder builder = RelBuilder.create(config().build());
    try {
      builder.scan("EMP");
      RexNode call = builder.call(SqlStdOperatorTable.PLUS,
          builder.field(1),
          builder.field(3));
      fail("expected error, got " + call);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("Cannot infer return type for +; "
              + "operand types: [VARCHAR(10), SMALLINT]"));
    }
  }

  @Test void testProject() {
    // Equivalent SQL:
    //   SELECT deptno, CAST(comm AS SMALLINT) AS comm, 20 AS $f2,
    //     comm AS comm3, comm AS c
    //   FROM emp
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .project(builder.field("DEPTNO"),
                builder.cast(builder.field(6), SqlTypeName.SMALLINT),
                builder.literal(20),
                builder.field(6),
                builder.alias(builder.field(6), "C"))
            .build();
    // Note: CAST(COMM) gets the COMM alias because it occurs first
    // Note: AS(COMM, C) becomes just $6
    final String expected = ""
        + "LogicalProject(DEPTNO=[$7], COMM=[CAST($6):SMALLINT NOT NULL], $f2=[20], COMM0=[$6], C=[$6])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Tests each method that creates a scalar expression. */
  @Test void testProject2() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .project(builder.field("DEPTNO"),
                builder.cast(builder.field(6), SqlTypeName.SMALLINT),
                builder.or(
                    builder.equals(builder.field("DEPTNO"),
                        builder.literal(20)),
                    builder.and(
                        builder.cast(builder.literal(null),
                            SqlTypeName.BOOLEAN),
                        builder.equals(builder.field("DEPTNO"),
                            builder.literal(10)),
                        builder.and(builder.isNull(builder.field(6)),
                            builder.not(builder.isNotNull(builder.field(5))))),
                    builder.equals(builder.field("DEPTNO"),
                        builder.literal(20)),
                    builder.equals(builder.field("DEPTNO"),
                        builder.literal(30))),
                builder.alias(builder.isNull(builder.field(2)), "n2"),
                builder.alias(builder.isNotNull(builder.field(3)), "nn2"),
                builder.literal(20),
                builder.field(6),
                builder.alias(builder.field(6), "C"))
            .build();
    final String expected = ""
        + "LogicalProject(DEPTNO=[$7], COMM=[CAST($6):SMALLINT NOT NULL], "
        + "$f2=[OR(SEARCH($7, Sarg[20, 30]), AND(null, =($7, 10), "
        + "IS NULL($6), IS NULL($5)))], n2=[IS NULL($2)], "
        + "nn2=[IS NOT NULL($3)], $f5=[20], COMM0=[$6], C=[$6])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testProjectIdentity() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("DEPT")
            .project(builder.fields(Mappings.bijection(Arrays.asList(0, 1, 2))))
            .build();
    final String expected = "LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1297">[CALCITE-1297]
   * RelBuilder does not translate identity projects even if they rename
   * fields</a>. */
  @Test void testProjectIdentityWithFieldsRename() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("DEPT")
            .project(builder.alias(builder.field(0), "a"),
                builder.alias(builder.field(1), "b"),
                builder.alias(builder.field(2), "c"))
            .as("t1")
            .project(builder.field("a"),
                builder.field("t1", "c"))
            .build();
    final String expected = "LogicalProject(a=[$0], c=[$2])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Variation on {@link #testProjectIdentityWithFieldsRename}: don't use a
   * table alias, and make sure the field names propagate through a filter. */
  @Test void testProjectIdentityWithFieldsRenameFilter() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("DEPT")
            .project(builder.alias(builder.field(0), "a"),
                builder.alias(builder.field(1), "b"),
                builder.alias(builder.field(2), "c"))
            .filter(
                builder.equals(builder.field("a"),
                    builder.literal(20)))
            .aggregate(builder.groupKey(0, 1, 2),
                builder.aggregateCall(SqlStdOperatorTable.SUM,
                    builder.field(0)))
            .project(builder.field("c"),
                builder.field("a"))
            .build();
    final String expected = ""
        + "LogicalProject(c=[$2], a=[$0])\n"
        + "  LogicalAggregate(group=[{0, 1, 2}], agg#0=[SUM($0)])\n"
        + "    LogicalFilter(condition=[=($0, 20)])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testProjectLeadingEdge() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .project(builder.fields(Mappings.bijection(Arrays.asList(0, 1, 2))))
            .build();
    final String expected = "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testProjectWithAliasFromScan() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .project(builder.field(1, "EMP", "ENAME"))
            .build();
    final String expected =
        "LogicalProject(ENAME=[$1])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3228">[CALCITE-3228]
   * IllegalArgumentException in getMapping() for project containing same reference</a>. */
  @Test void testProjectMapping() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
            builder.scan("EMP")
                    .project(builder.field(0), builder.field(0))
                    .build();
    assertTrue(root instanceof Project);
    Project project = (Project) root;
    Mappings.TargetMapping mapping = project.getMapping();
    assertThat(mapping, nullValue());
  }

  private void project1(int value, SqlTypeName sqlTypeName, String message, String expected) {
    final RelBuilder builder = createBuilder(c -> c.withSimplifyValues(false));
    RexBuilder rex = builder.getRexBuilder();
    RelNode actual =
        builder.values(new String[]{"x"}, 42)
            .empty()
            .project(
                rex.makeLiteral(value,
                    rex.getTypeFactory().createSqlType(sqlTypeName)))
            .build();
    assertThat(message, actual, hasTree(expected));
  }

  @Test void testProject1asInt() {
    project1(1, SqlTypeName.INTEGER,
        "project(1 as INT) might omit type of 1 in the output plan as"
            + " it is convention to omit INTEGER for integer literals",
        "LogicalProject($f0=[1])\n"
            + "  LogicalValues(tuples=[[]])\n");
  }

  @Test void testProject1asBigInt() {
    project1(1, SqlTypeName.BIGINT, "project(1 as BIGINT) should contain"
            + " type of 1 in the output plan since the convention is to omit type of INTEGER",
        "LogicalProject($f0=[1:BIGINT])\n"
            + "  LogicalValues(tuples=[[]])\n");
  }

  @Test void testProjectBloat() {
    final Function<RelBuilder, RelNode> f = b ->
        b.scan("EMP")
            .project(
                b.alias(
                    caseCall(b, b.field("DEPTNO"),
                        b.literal(0), b.literal("zero"),
                        b.literal(1), b.literal("one"),
                        b.literal(2), b.literal("two"),
                        b.literal("other")),
                    "v"))
            .project(
                b.call(SqlStdOperatorTable.PLUS, b.field("v"), b.field("v")))
        .build();
    // Complexity of bottom is 14; top is 3; merged is 29; difference is -12.
    // So, we merge if bloat is 20 or 100 (the default),
    // but not if it is -1, 0 or 10.
    final String expected = "LogicalProject($f0=[+"
        + "(CASE(=($7, 0), 'zero', =($7, 1), 'one', =($7, 2), 'two', 'other'),"
        + " CASE(=($7, 0), 'zero', =($7, 1), 'one', =($7, 2), 'two', 'other'))])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    final String expectedNeg = "LogicalProject($f0=[+($0, $0)])\n"
        + "  LogicalProject(v=[CASE(=($7, 0), 'zero', =($7, 1), "
        + "'one', =($7, 2), 'two', 'other')])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));
    assertThat(f.apply(createBuilder(c -> c.withBloat(0))),
        hasTree(expectedNeg));
    assertThat(f.apply(createBuilder(c -> c.withBloat(-1))),
        hasTree(expectedNeg));
    assertThat(f.apply(createBuilder(c -> c.withBloat(10))),
        hasTree(expectedNeg));
    assertThat(f.apply(createBuilder(c -> c.withBloat(20))),
        hasTree(expected));
  }

  @Test void testProjectBloat2() {
    final Function<RelBuilder, RelNode> f = b ->
        b.scan("EMP")
            .project(
                b.field("DEPTNO"),
                b.field("SAL"),
                b.alias(
                    b.call(SqlStdOperatorTable.PLUS, b.field("DEPTNO"),
                        b.field("EMPNO")), "PLUS"))
            .project(
                b.call(SqlStdOperatorTable.MULTIPLY, b.field("SAL"),
                    b.field("PLUS")),
                b.field("SAL"))
        .build();
    // Complexity of bottom is 5; top is 4; merged is 6; difference is 3.
    // So, we merge except when bloat is -1.
    final String expected = "LogicalProject($f0=[*($5, +($7, $0))], SAL=[$5])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    final String expectedNeg = "LogicalProject($f0=[*($1, $2)], SAL=[$1])\n"
        + "  LogicalProject(DEPTNO=[$7], SAL=[$5], PLUS=[+($7, $0)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));
    assertThat(f.apply(createBuilder(c -> c.withBloat(0))),
        hasTree(expected));
    assertThat(f.apply(createBuilder(c -> c.withBloat(-1))),
        hasTree(expectedNeg));
    assertThat(f.apply(createBuilder(c -> c.withBloat(10))),
        hasTree(expected));
    assertThat(f.apply(createBuilder(c -> c.withBloat(20))),
        hasTree(expected));
  }

  private RexNode caseCall(RelBuilder b, RexNode ref, RexNode... nodes) {
    final List<RexNode> list = new ArrayList<>();
    for (int i = 0; i + 1 < nodes.length; i += 2) {
      list.add(b.equals(ref, nodes[i]));
      list.add(nodes[i + 1]);
    }
    list.add(nodes.length % 2 == 1 ? nodes[nodes.length - 1]
        : b.literal(null));
    return b.call(SqlStdOperatorTable.CASE, list);
  }

  /** Creates a {@link Project} that contains a windowed aggregate function.
   * Repeats the using {@link RelBuilder.AggCall#over} and
   * {@link RexBuilder#makeOver}. */
  @Test void testProjectOver() {
    final Function<RelBuilder, RelNode> f = b -> {
      final RelDataType intType =
          b.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
      return b.scan("EMP")
          .project(b.field("DEPTNO"),
              b.alias(
                  b.getRexBuilder().makeOver(intType,
                      SqlStdOperatorTable.ROW_NUMBER, ImmutableList.of(),
                      ImmutableList.of(),
                      ImmutableList.of(
                          new RexFieldCollation(b.field("EMPNO"),
                              ImmutableSet.of())),
                      RexWindowBounds.UNBOUNDED_PRECEDING,
                      RexWindowBounds.UNBOUNDED_FOLLOWING,
                      true, true, false, false, false),
                  "x"))
          .build();
    };
    final Function<RelBuilder, RelNode> f2 = b -> b.scan("EMP")
        .project(b.field("DEPTNO"),
            b.aggregateCall(SqlStdOperatorTable.ROW_NUMBER)
                .over()
                .partitionBy()
                .orderBy(b.field("EMPNO"))
                .rowsUnbounded()
                .allowPartial(true)
                .nullWhenCountZero(false)
                .as("x"))
        .build();
    final String expected = ""
        + "LogicalProject(DEPTNO=[$7], x=[ROW_NUMBER() OVER (ORDER BY $0)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));
    assertThat(f2.apply(createBuilder()), hasTree(expected));
  }

  /** Tests that RelBuilder does not merge a Project that contains a windowed
   * aggregate function into a lower Project. */
  @Test void testProjectOverOver() {
    final Function<RelBuilder, RelNode> f = b -> b.scan("EMP")
        .project(b.field("DEPTNO"),
            b.aggregateCall(SqlStdOperatorTable.ROW_NUMBER)
                .over()
                .partitionBy()
                .orderBy(b.field("EMPNO"))
                .rowsUnbounded()
                .as("x"))
        .project(b.field("DEPTNO"),
            b.aggregateCall(SqlStdOperatorTable.ROW_NUMBER)
                .over()
                .partitionBy()
                .orderBy(b.field("DEPTNO"))
                .rowsUnbounded()
                .as("y"))
        .build();
    final String expected = ""
        + "LogicalProject(DEPTNO=[$0], y=[ROW_NUMBER() OVER (ORDER BY $0)])\n"
        + "  LogicalProject(DEPTNO=[$7], x=[ROW_NUMBER() OVER (ORDER BY $0)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));
  }

  @Test void testRename() {
    final RelBuilder builder = RelBuilder.create(config().build());

    // No rename necessary (null name is ignored)
    RelNode root =
        builder.scan("DEPT")
            .rename(Arrays.asList("DEPTNO", null))
            .build();
    final String expected = "LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));

    // No rename necessary (prefix matches)
    root =
        builder.scan("DEPT")
            .rename(ImmutableList.of("DEPTNO"))
            .build();
    assertThat(root, hasTree(expected));

    // Add project to rename fields
    root =
        builder.scan("DEPT")
            .rename(Arrays.asList("NAME", null, "DEPTNO"))
            .build();
    final String expected2 = ""
        + "LogicalProject(NAME=[$0], DNAME=[$1], DEPTNO=[$2])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected2));

    // If our requested list has non-unique names, we might get the same field
    // names we started with. Don't add a useless project.
    root =
        builder.scan("DEPT")
            .rename(Arrays.asList("DEPTNO", null, "DEPTNO"))
            .build();
    final String expected3 = ""
        + "LogicalProject(DEPTNO=[$0], DNAME=[$1], DEPTNO0=[$2])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected3));
    root =
        builder.scan("DEPT")
            .rename(Arrays.asList("DEPTNO", null, "DEPTNO"))
            .rename(Arrays.asList("DEPTNO", null, "DEPTNO"))
            .build();
    // No extra Project
    assertThat(root, hasTree(expected3));

    // Name list too long
    try {
      root =
          builder.scan("DEPT")
              .rename(ImmutableList.of("NAME", "DEPTNO", "Y", "Z"))
              .build();
      fail("expected error, got " + root);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("More names than fields"));
    }
  }

  @Test void testRenameValues() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.values(new String[]{"a", "b"}, true, 1, false, -50)
            .build();
    final String expected =
        "LogicalValues(tuples=[[{ true, 1 }, { false, -50 }]])\n";
    assertThat(root, hasTree(expected));

    // When you rename Values, you get a Values with a new row type, no Project
    root =
        builder.push(root)
            .rename(ImmutableList.of("x", "y z"))
            .build();
    assertThat(root, hasTree(expected));
    assertThat(root.getRowType().getFieldNames().toString(), is("[x, y z]"));
  }

  /** Tests conditional rename using {@link RelBuilder#let}. */
  @Test void testLetRename() {
    final AtomicInteger i = new AtomicInteger();
    final Function<RelBuilder, String> f = builder ->
        builder.values(new String[]{"a", "b"}, 1, true)
            .rename(Arrays.asList("p", "q"))
            .let(r -> i.getAndIncrement() == 0
                ? r.rename(Arrays.asList("x", "y")) : r)
            .let(r -> i.getAndIncrement() == 1
                ? r.project(r.field(1), r.field(0)) : r)
            .let(r -> i.getAndIncrement() == 0
                ? r.rename(Arrays.asList("c", "d")) : r)
            .let(r -> r.build().getRowType().toString());
    final String expected = "RecordType(BOOLEAN y, INTEGER x)";
    assertThat(f.apply(createBuilder()), is(expected));
    assertThat(i.get(), is(3));
  }

  @Test void testPermute() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .permute(Mappings.bijection(Arrays.asList(1, 2, 0)))
            .build();
    final String expected = "LogicalProject(JOB=[$2], EMPNO=[$0], ENAME=[$1])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testConvert() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelDataType rowType =
        builder.getTypeFactory().builder()
            .add("a", SqlTypeName.BIGINT)
            .add("b", SqlTypeName.VARCHAR, 10)
            .add("c", SqlTypeName.VARCHAR, 10)
            .build();
    RelNode root =
        builder.scan("DEPT")
            .convert(rowType, false)
            .build();
    final String expected = ""
        + "LogicalProject(DEPTNO=[CAST($0):BIGINT NOT NULL], DNAME=[CAST($1):VARCHAR(10) NOT NULL], LOC=[CAST($2):VARCHAR(10) NOT NULL])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testConvertRename() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelDataType rowType =
        builder.getTypeFactory().builder()
            .add("a", SqlTypeName.BIGINT)
            .add("b", SqlTypeName.VARCHAR, 10)
            .add("c", SqlTypeName.VARCHAR, 10)
            .build();
    RelNode root =
        builder.scan("DEPT")
            .convert(rowType, true)
            .build();
    final String expected = ""
        + "LogicalProject(a=[CAST($0):BIGINT NOT NULL], b=[CAST($1):VARCHAR(10) NOT NULL], c=[CAST($2):VARCHAR(10) NOT NULL])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4429">[CALCITE-4429]
   * RelOptUtil#createCastRel should throw an exception when the desired row type
   * and the row type to be converted don't have the same number of fields</a>. */
  @Test void testConvertNegative() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelDataType rowType =
        builder.getTypeFactory().builder()
            .add("a", SqlTypeName.BIGINT)
            .add("b", SqlTypeName.VARCHAR, 10)
            .build();
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
      builder.scan("DEPT")
          .convert(rowType, false)
          .build();
    }, "Convert should fail since the field counts are not equal.");
    assertThat(ex.getMessage(), containsString("Field counts are not equal"));
  }

  @Test void testAggregate() {
    // Equivalent SQL:
    //   SELECT COUNT(DISTINCT deptno) AS c
    //   FROM emp
    //   GROUP BY ()
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .aggregate(builder.groupKey(),
                builder.count(true, "C", builder.field("DEPTNO")))
            .build();
    final String expected = ""
        + "LogicalAggregate(group=[{}], C=[COUNT(DISTINCT $7)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testAggregate2() {
    // Equivalent SQL:
    //   SELECT COUNT(*) AS c, SUM(mgr + 1) AS s
    //   FROM emp
    //   GROUP BY ename, hiredate + mgr
    final Function<RelBuilder, RelNode> f = builder ->
        builder.scan("EMP")
            .aggregate(
                builder.groupKey(builder.field(1),
                    builder.call(SqlStdOperatorTable.PLUS,
                        builder.field(4),
                        builder.field(3)),
                    builder.field(1)),
                builder.countStar("C"),
                builder.sum(
                    builder.call(SqlStdOperatorTable.PLUS, builder.field(3),
                        builder.literal(1))).as("S"))
            .build();
    final String expected = ""
        + "LogicalAggregate(group=[{0, 1}], C=[COUNT()], S=[SUM($2)])\n"
        + "  LogicalProject(ENAME=[$1], $f8=[+($4, $3)], $f9=[+($3, 1)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));

    // now without pruning
    final String expected2 = ""
        + "LogicalAggregate(group=[{1, 8}], C=[COUNT()], S=[SUM($9)])\n"
        + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], "
        + "HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], $f8=[+($4, $3)], "
        + "$f9=[+($3, 1)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder(c -> c.withPruneInputOfAggregate(false))),
        hasTree(expected2));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2192">[CALCITE-2192]
   * RelBuilder wrongly skips creation of Aggregate that prunes columns if input
   * is unique</a>. */
  @Test void testAggregate3() {
    // Equivalent SQL:
    //   SELECT DISTINCT deptno FROM (
    //     SELECT deptno, COUNT(*)
    //     FROM emp
    //     GROUP BY deptno)
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .aggregate(builder.groupKey(builder.field(1)),
                builder.count().as("C"))
            .aggregate(builder.groupKey(builder.field(0)))
            .build();
    final String expected = ""
        + "LogicalProject(ENAME=[$0])\n"
        + "  LogicalAggregate(group=[{1}], C=[COUNT()])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** As {@link #testAggregate3()} but with Filter. */
  @Test void testAggregate4() {
    // Equivalent SQL:
    //   SELECT DISTINCT deptno FROM (
    //     SELECT deptno, COUNT(*)
    //     FROM emp
    //     GROUP BY deptno
    //     HAVING COUNT(*) > 3)
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .aggregate(builder.groupKey(builder.field(1)),
                builder.count().as("C"))
            .filter(
                builder.greaterThan(builder.field(1), builder.literal(3)))
            .aggregate(builder.groupKey(builder.field(0)))
            .build();
    final String expected = ""
        + "LogicalProject(ENAME=[$0])\n"
        + "  LogicalFilter(condition=[>($1, 3)])\n"
        + "    LogicalAggregate(group=[{1}], C=[COUNT()])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2946">[CALCITE-2946]
   * RelBuilder wrongly skips creation of Aggregate that prunes columns if input
   * produces one row at most</a>. */
  @Test void testAggregate5() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .aggregate(builder.groupKey(), builder.count().as("C"))
            .project(builder.literal(4), builder.literal(2), builder.field(0))
            .aggregate(builder.groupKey(builder.field(0), builder.field(1)))
            .build();
    final String expected = ""
        + "LogicalProject($f0=[4], $f1=[2])\n"
        + "  LogicalAggregate(group=[{}], C=[COUNT()])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3839">[CALCITE-3839]
   * After calling RelBuilder.aggregate, cannot lookup field by name</a>. */
  @Test void testAggregateAndThenProjectNamedField() {
    final Function<RelBuilder, RelNode> f = builder ->
        builder.scan("EMP")
            .project(builder.field("EMPNO"), builder.field("ENAME"),
                builder.field("SAL"))
            .aggregate(builder.groupKey(builder.field("ENAME")),
                builder.sum(builder.field("SAL")))
            // Before [CALCITE-3839] was fixed, the following line gave
            // 'field [ENAME] not found'
            .project(builder.field("ENAME"))
            .build();
    final String expected = ""
        + "LogicalProject(ENAME=[$0])\n"
        + "  LogicalAggregate(group=[{0}], agg#0=[SUM($1)])\n"
        + "    LogicalProject(ENAME=[$1], SAL=[$5])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));
  }

  /** Tests that {@link RelBuilder#aggregate} eliminates duplicate aggregate
   * calls and creates a {@code Project} to compensate. */
  @Test void testAggregateEliminatesDuplicateCalls() {
    final String expected = ""
        + "LogicalProject(S1=[$0], C=[$1], S2=[$2], S1b=[$0])\n"
        + "  LogicalAggregate(group=[{}], S1=[SUM($1)], C=[COUNT()], S2=[SUM($2)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(
        buildRelWithDuplicateAggregates(c -> c.withDedupAggregateCalls(true)),
        hasTree(expected));

    // Now, disable the rewrite
    final String expected2 = ""
        + "LogicalAggregate(group=[{}], S1=[SUM($1)], C=[COUNT()], S2=[SUM($2)], S1b=[SUM($1)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(
        buildRelWithDuplicateAggregates(c -> c.withDedupAggregateCalls(false)),
        hasTree(expected2));
  }

  /** As {@link #testAggregateEliminatesDuplicateCalls()} but with a
   * single-column GROUP BY clause. */
  @Test void testAggregateEliminatesDuplicateCalls2() {
    RelNode root = buildRelWithDuplicateAggregates(c -> c, 0);
    final String expected = ""
        + "LogicalProject(EMPNO=[$0], S1=[$1], C=[$2], S2=[$3], S1b=[$1])\n"
        + "  LogicalAggregate(group=[{0}], S1=[SUM($1)], C=[COUNT()], S2=[SUM($2)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** As {@link #testAggregateEliminatesDuplicateCalls()} but with a
   * multi-column GROUP BY clause. */
  @Test void testAggregateEliminatesDuplicateCalls3() {
    RelNode root = buildRelWithDuplicateAggregates(c -> c, 2, 0, 4, 3);
    final String expected = ""
        + "LogicalProject(EMPNO=[$0], JOB=[$1], MGR=[$2], HIREDATE=[$3], S1=[$4], C=[$5], S2=[$6], S1b=[$4])\n"
        + "  LogicalAggregate(group=[{0, 2, 3, 4}], S1=[SUM($1)], C=[COUNT()], S2=[SUM($2)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  private RelNode buildRelWithDuplicateAggregates(
      UnaryOperator<RelBuilder.Config> transform,
      int... groupFieldOrdinals) {
    final RelBuilder builder = createBuilder(transform);
    return builder.scan("EMP")
        .aggregate(builder.groupKey(groupFieldOrdinals),
            builder.sum(builder.field(1)).as("S1"),
            builder.count().as("C"),
            builder.sum(builder.field(2)).as("S2"),
            builder.sum(builder.field(1)).as("S1b"))
        .build();
  }

  /** Tests eliminating duplicate aggregate calls, when some of them are only
   * seen to be duplicates when a spurious "DISTINCT" has been eliminated.
   *
   * <p>Note that "M2" and "MD2" are based on the same field, because
   * "MIN(DISTINCT $2)" is identical to "MIN($2)". The same is not true for
   * "SUM". */
  @Test void testAggregateEliminatesDuplicateDistinctCalls() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root = builder.scan("EMP")
        .aggregate(builder.groupKey(2),
            builder.sum(builder.field(1)).as("S1"),
            builder.sum(builder.field(1)).distinct().as("SD1"),
            builder.count().as("C"),
            builder.min(builder.field(2)).distinct().as("MD2"),
            builder.min(builder.field(2)).as("M2"),
            builder.min(builder.field(2)).distinct().as("MD2b"),
            builder.sum(builder.field(1)).distinct().as("S1b"))
        .build();
    final String expected = ""
        + "LogicalProject(JOB=[$0], S1=[$1], SD1=[$2], C=[$3], MD2=[$4], "
        + "M2=[$4], MD2b=[$4], S1b=[$2])\n"
        + "  LogicalAggregate(group=[{2}], S1=[SUM($1)], "
        + "SD1=[SUM(DISTINCT $1)], C=[COUNT()], MD2=[MIN($2)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testAggregateFilter() {
    // Equivalent SQL:
    //   SELECT deptno, COUNT(*) FILTER (WHERE empno > 100) AS c
    //   FROM emp
    //   GROUP BY ROLLUP(deptno)
    final Function<RelBuilder, RelNode> f = builder ->
        builder.scan("EMP")
            .aggregate(
                builder.groupKey(ImmutableBitSet.of(7),
                    ImmutableList.of(ImmutableBitSet.of(7), ImmutableBitSet.of())),
                builder.count()
                    .filter(
                        builder.greaterThan(builder.field("EMPNO"),
                            builder.literal(100)))
                    .as("C"))
            .build();
    final String expected = ""
        + "LogicalAggregate(group=[{0}], groups=[[{0}, {}]], C=[COUNT() FILTER $1])\n"
        + "  LogicalProject(DEPTNO=[$7], $f8=[>($0, 100)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));

    // now without pruning
    final String expected2 = ""
        + "LogicalAggregate(group=[{7}], groups=[[{7}, {}]], C=[COUNT() FILTER $8])\n"
        + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], "
        + "HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], $f8=[>($0, 100)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder(c -> c.withPruneInputOfAggregate(false))),
        hasTree(expected2));
  }

  @Test void testAggregateFilterFails() {
    // Equivalent SQL:
    //   SELECT deptno, SUM(sal) FILTER (WHERE comm) AS c
    //   FROM emp
    //   GROUP BY deptno
    try {
      final RelBuilder builder = RelBuilder.create(config().build());
      RelNode root =
          builder.scan("EMP")
              .aggregate(
                  builder.groupKey(builder.field("DEPTNO")),
                  builder.sum(builder.field("SAL"))
                      .filter(builder.field("COMM"))
                      .as("C"))
              .build();
      fail("expected error, got " + root);
    } catch (CalciteException e) {
      assertThat(e.getMessage(),
          is("FILTER expression must be of type BOOLEAN"));
    }
  }

  @Test void testAggregateFilterNullable() {
    // Equivalent SQL:
    //   SELECT deptno, SUM(sal) FILTER (WHERE comm < 100) AS c
    //   FROM emp
    //   GROUP BY deptno
    final Function<RelBuilder, RelNode> f = builder ->
        builder.scan("EMP")
            .aggregate(
                builder.groupKey(builder.field("DEPTNO")),
                builder.sum(builder.field("SAL"))
                    .filter(
                        builder.lessThan(builder.field("COMM"),
                            builder.literal(100)))
                    .as("C"))
            .build();
    final String expected = ""
        + "LogicalAggregate(group=[{1}], C=[SUM($0) FILTER $2])\n"
        + "  LogicalProject(SAL=[$5], DEPTNO=[$7], $f8=[IS TRUE(<($6, 100))])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));

    // now without pruning
    final String expected2 = ""
        + "LogicalAggregate(group=[{7}], C=[SUM($5) FILTER $8])\n"
        + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], $f8=[IS TRUE(<($6, 100))])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder(c -> c.withPruneInputOfAggregate(false))),
        hasTree(expected2));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1980">[CALCITE-1980]
   * RelBuilder gives NPE if groupKey contains alias</a>.
   *
   * <p>Now, the alias does not cause a new expression to be added to the input,
   * but causes the referenced fields to be renamed. */
  @Test void testAggregateProjectWithAliases() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .project(builder.field("DEPTNO"))
            .aggregate(
                builder.groupKey(
                    builder.alias(builder.field("DEPTNO"), "departmentNo")))
            .build();
    final String expected = ""
        + "LogicalAggregate(group=[{0}])\n"
        + "  LogicalProject(departmentNo=[$7])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testAggregateProjectWithExpression() {
    final Function<RelBuilder, RelNode> f = builder ->
        builder.scan("EMP")
            .project(builder.field("DEPTNO"))
            .aggregate(
                builder.groupKey(
                    builder.alias(
                        builder.call(SqlStdOperatorTable.PLUS,
                            builder.field("DEPTNO"), builder.literal(3)),
                        "d3")))
            .build();
    final String expected = ""
        + "LogicalAggregate(group=[{0}])\n"
        + "  LogicalProject(d3=[+($7, 3)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));

    // now without pruning
    final String expected2 = ""
        + "LogicalAggregate(group=[{1}])\n"
        + "  LogicalProject(DEPTNO=[$7], d3=[+($7, 3)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder(c -> c.withPruneInputOfAggregate(false))),
        hasTree(expected2));
  }

  /** Tests that {@link RelBuilder#aggregate} on top of a {@link Project} prunes
   * away expressions that are not used.
   *
   * @see RelBuilder.Config#pruneInputOfAggregate */
  @Test void testAggregateProjectPrune() {
    // SELECT deptno, SUM(sal) FILTER (WHERE b)
    // FROM (
    //   SELECT deptno, empno + 10, sal, job = 'CLERK' AS b
    //   FROM emp)
    // GROUP BY deptno
    //   -->
    // SELECT deptno, SUM(sal) FILTER (WHERE b)
    // FROM (
    //   SELECT deptno, sal, job = 'CLERK' AS b
    //   FROM emp)
    // GROUP BY deptno
    final Function<RelBuilder, RelNode> f = builder ->
        builder.scan("EMP")
            .project(builder.field("DEPTNO"),
                builder.call(SqlStdOperatorTable.PLUS,
                    builder.field("EMPNO"), builder.literal(10)),
                builder.field("SAL"),
                builder.field("JOB"))
            .aggregate(
                builder.groupKey(builder.field("DEPTNO")),
                    builder.sum(builder.field("SAL"))
                .filter(
                    builder.equals(builder.field("JOB"),
                        builder.literal("CLERK"))))
            .build();
    final String expected = ""
        + "LogicalAggregate(group=[{0}], agg#0=[SUM($1) FILTER $2])\n"
        + "  LogicalProject(DEPTNO=[$7], SAL=[$5], $f4=[IS TRUE(=($2, 'CLERK'))])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder()),
        hasTree(expected));

    // now with pruning disabled
    final String expected2 = ""
        + "LogicalAggregate(group=[{0}], agg#0=[SUM($2) FILTER $4])\n"
        + "  LogicalProject(DEPTNO=[$7], $f1=[+($0, 10)], SAL=[$5], JOB=[$2], "
        + "$f4=[IS TRUE(=($2, 'CLERK'))])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder(c -> c.withPruneInputOfAggregate(false))),
        hasTree(expected2));
  }

  /** Tests that (a) if the input is a project and no fields are used
   * we remove the project (rather than projecting zero fields, which
   * would be wrong), and (b) if the same aggregate function is used
   * twice, we add a project on top. */
  @Test void testAggregateProjectPruneEmpty() {
    // SELECT COUNT(*) AS C, COUNT(*) AS C2 FROM (
    //  SELECT deptno, empno + 10, sal, job = 'CLERK' AS b
    //  FROM emp)
    //   -->
    // SELECT C, C AS C2 FROM (
    //   SELECT COUNT(*) AS c
    //   FROM emp)
    final Function<RelBuilder, RelNode> f = builder ->
        builder.scan("EMP")
            .project(builder.field("DEPTNO"),
                builder.call(SqlStdOperatorTable.PLUS,
                    builder.field("EMPNO"), builder.literal(10)),
                builder.field("SAL"),
                builder.field("JOB"))
            .aggregate(
                builder.groupKey(),
                    builder.countStar("C"),
                    builder.countStar("C2"))
            .build();
    final String expected = ""
        + "LogicalProject(C=[$0], C2=[$0])\n"
        + "  LogicalAggregate(group=[{}], C=[COUNT()])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));

    // now with pruning disabled
    final String expected2 = ""
        + "LogicalProject(C=[$0], C2=[$0])\n"
        + "  LogicalAggregate(group=[{}], C=[COUNT()])\n"
        + "    LogicalProject(DEPTNO=[$7], $f1=[+($0, 10)], SAL=[$5], JOB=[$2])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder(c -> c.withPruneInputOfAggregate(false))),
        hasTree(expected2));
  }

  @Test void testAggregateGroupingKeyOutOfRangeFails() {
    final RelBuilder builder = RelBuilder.create(config().build());
    try {
      RelNode root =
          builder.scan("EMP")
              .aggregate(builder.groupKey(ImmutableBitSet.of(17)))
              .build();
      fail("expected error, got " + root);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("out of bounds: {17}"));
    }
  }

  @Test void testAggregateGroupingSetNotSubsetFails() {
    final RelBuilder builder = RelBuilder.create(config().build());
    try {
      RelNode root =
          builder.scan("EMP")
              .aggregate(
                  builder.groupKey(ImmutableBitSet.of(7),
                      ImmutableList.of(ImmutableBitSet.of(4), ImmutableBitSet.of())))
              .build();
      fail("expected error, got " + root);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("group set element [$4] must be a subset of group key"));
    }
  }

  /** Tests that, if you try to create an Aggregate with duplicate grouping
   * sets, RelBuilder creates a Union. Each branch of the Union has an
   * Aggregate that has distinct grouping sets. */
  @Test void testAggregateGroupingSetDuplicate() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .aggregate(
                builder.groupKey(ImmutableBitSet.of(7, 6),
                    ImmutableList.of(ImmutableBitSet.of(7),
                            ImmutableBitSet.of(6),
                            ImmutableBitSet.of(7))))
            .build();
    final String expected = ""
        + "LogicalUnion(all=[true])\n"
        + "  LogicalAggregate(group=[{6, 7}], groups=[[{6}, {7}]])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalAggregate(group=[{6, 7}], groups=[[{7}]])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4665">[CALCITE-4665]
   * Allow Aggregate.groupSet to contain columns not in any of the groupSets.</a>. */
  @Test void testGroupingSetWithGroupKeysContainingUnusedColumn() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root = builder.scan("EMP")
        .aggregate(
            builder.groupKey(
                ImmutableBitSet.of(0, 1, 2),
                ImmutableList.of(ImmutableBitSet.of(0, 1), ImmutableBitSet.of(0))),
            builder.count(false, "C"),
            builder.sum(false, "S", builder.field("SAL")))
        .filter(
            builder.call(
                SqlStdOperatorTable.GREATER_THAN,
                builder.field("C"),
                builder.literal(10)))
        .filter(
            builder.call(
                SqlStdOperatorTable.EQUALS,
                builder.field("JOB"),
                builder.literal("DEVELOP")))
        .project(builder.field("JOB")).build();
    final String expected = ""
        + "LogicalProject(JOB=[$2])\n"
        + "  LogicalFilter(condition=[=($2, 'DEVELOP')])\n"
        + "    LogicalFilter(condition=[>($3, 10)])\n"
        + "      LogicalAggregate(group=[{0, 1, 2}], groups=[[{0, 1}, {0}]], C=[COUNT()], S=[SUM"
        + "($5)])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testAggregateGrouping() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .aggregate(builder.groupKey(6, 7),
                builder.aggregateCall(SqlStdOperatorTable.GROUPING,
                    builder.field("DEPTNO")).as("g"))
            .build();
    final String expected = ""
        + "LogicalAggregate(group=[{6, 7}], g=[GROUPING($7)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testAggregateGroupingWithDistinctFails() {
    final RelBuilder builder = RelBuilder.create(config().build());
    try {
      RelNode root =
          builder.scan("EMP")
              .aggregate(builder.groupKey(6, 7),
                  builder.aggregateCall(SqlStdOperatorTable.GROUPING,
                      builder.field("DEPTNO"))
                      .distinct(true)
                      .as("g"))
              .build();
      fail("expected error, got " + root);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("DISTINCT not allowed"));
    }
  }

  @Test void testAggregateGroupingWithFilterFails() {
    final RelBuilder builder = RelBuilder.create(config().build());
    try {
      RelNode root =
          builder.scan("EMP")
              .aggregate(builder.groupKey(6, 7),
                  builder.aggregateCall(SqlStdOperatorTable.GROUPING,
                      builder.field("DEPTNO"))
                      .filter(builder.literal(true))
                      .as("g"))
              .build();
      fail("expected error, got " + root);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("FILTER not allowed"));
    }
  }

  @Test void testAggregateOneRow() {
    final Function<RelBuilder, RelNode> f = builder ->
        builder.values(new String[] {"a", "b"}, 1, 2)
            .aggregate(builder.groupKey(1))
            .build();
    final String plan = "LogicalProject(b=[$1])\n"
        + "  LogicalValues(tuples=[[{ 1, 2 }]])\n";
    assertThat(f.apply(createBuilder()), hasTree(plan));

    final String plan2 = "LogicalAggregate(group=[{1}])\n"
        + "  LogicalValues(tuples=[[{ 1, 2 }]])\n";
    assertThat(f.apply(createBuilder(c -> c.withAggregateUnique(true))),
        hasTree(plan2));
  }

  /** Tests that we do not convert an Aggregate to a Project if there are
   * multiple group sets. */
  @Test void testAggregateGroupingSetsOneRow() {
    final Function<RelBuilder, RelNode> f = builder -> {
      final List<Integer> list01 = Arrays.asList(0, 1);
      final List<Integer> list0 = Collections.singletonList(0);
      final List<Integer> list1 = Collections.singletonList(1);
      return builder.values(new String[] {"a", "b"}, 1, 2)
          .aggregate(
              builder.groupKey(builder.fields(list01),
                  ImmutableList.of(builder.fields(list0),
                      builder.fields(list1),
                      builder.fields(list01))))
          .build();
    };
    final String plan = ""
        + "LogicalAggregate(group=[{0, 1}], groups=[[{0, 1}, {0}, {1}]])\n"
        + "  LogicalValues(tuples=[[{ 1, 2 }]])\n";
    assertThat(f.apply(createBuilder()), hasTree(plan));
    assertThat(f.apply(createBuilder(c -> c.withAggregateUnique(true))),
        hasTree(plan));
  }

  /** Tests creating (and expanding) a call to {@code GROUP_ID()} in a
   * {@code GROUPING SETS} query. Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4199">[CALCITE-4199]
   * RelBuilder throws NullPointerException while implementing
   * GROUP_ID()</a>. */
  @Test void testAggregateGroupingSetsGroupId() {
    final String plan = ""
        + "LogicalProject(JOB=[$0], DEPTNO=[$1], $f2=[0:BIGINT])\n"
        + "  LogicalAggregate(group=[{2, 7}], groups=[[{2, 7}, {2}, {7}]])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(groupIdRel(createBuilder(), false), hasTree(plan));
    assertThat(
        groupIdRel(createBuilder(c -> c.withAggregateUnique(true)), false),
        hasTree(plan));

    // If any group occurs more than once, we need a UNION ALL.
    final String plan2 = ""
        + "LogicalUnion(all=[true])\n"
        + "  LogicalProject(JOB=[$0], DEPTNO=[$1], $f2=[0:BIGINT])\n"
        + "    LogicalAggregate(group=[{2, 7}], groups=[[{2, 7}, {2}, {7}]])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalProject(JOB=[$0], DEPTNO=[$1], $f2=[1:BIGINT])\n"
        + "    LogicalAggregate(group=[{2, 7}])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(groupIdRel(createBuilder(), true), hasTree(plan2));
  }

  private static RelNode groupIdRel(RelBuilder builder, boolean extra) {
    final List<String> djList = Arrays.asList("DEPTNO", "JOB");
    final List<String> dList = Collections.singletonList("DEPTNO");
    final List<String> jList = Collections.singletonList("JOB");
    return builder.scan("EMP")
        .aggregate(
            builder.groupKey(builder.fields(djList),
                ImmutableList.<List<? extends RexNode>>builder()
                    .add(builder.fields(dList))
                    .add(builder.fields(jList))
                    .add(builder.fields(djList))
                    .addAll(extra ? ImmutableList.of(builder.fields(djList))
                        : ImmutableList.of())
                    .build()),
            builder.aggregateCall(SqlStdOperatorTable.GROUP_ID))
        .build();
  }

  @Test void testWithinDistinct() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .aggregate(builder.groupKey(),
                builder.avg(builder.field("SAL"))
                    .as("g"),
                builder.avg(builder.field("SAL"))
                    .unique(builder.field("DEPTNO"))
                    .as("g2"))
            .build();
    final String expected = ""
        + "LogicalAggregate(group=[{}], g=[AVG($5)],"
        + " g2=[AVG($5) WITHIN DISTINCT ($7)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testDistinct() {
    // Equivalent SQL:
    //   SELECT DISTINCT deptno
    //   FROM emp
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .project(builder.field("DEPTNO"))
            .distinct()
            .build();
    final String expected = "LogicalAggregate(group=[{0}])\n"
        + "  LogicalProject(DEPTNO=[$7])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testDistinctAlready() {
    // DEPT is already distinct
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("DEPT")
            .distinct()
            .build();
    final String expected = "LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testDistinctEmpty() {
    // Is a relation with zero columns distinct?
    // What about if we know there are zero rows?
    // It is a matter of definition: there are no duplicate rows,
    // but applying "select ... group by ()" to it would change the result.
    // In theory, we could omit the distinct if we know there is precisely one
    // row, but we don't currently.
    final Function<RelBuilder, RelNode> f = builder ->
        builder.scan("EMP")
            .filter(
                builder.call(SqlStdOperatorTable.IS_NULL,
                    builder.field("COMM")))
            .project()
            .distinct()
            .build();
    final String expected = "LogicalAggregate(group=[{}])\n"
        + "  LogicalFilter(condition=[IS NULL($6)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));

    // now without pruning
    // (The empty LogicalProject is dubious, but it's what we've always done)
    final String expected2 = "LogicalAggregate(group=[{}])\n"
        + "  LogicalProject\n"
        + "    LogicalFilter(condition=[IS NULL($6)])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder(c -> c.withPruneInputOfAggregate(false))),
        hasTree(expected2));
  }

  @Test void testUnion() {
    // Equivalent SQL:
    //   SELECT deptno FROM emp
    //   UNION ALL
    //   SELECT deptno FROM dept
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("DEPT")
            .project(builder.field("DEPTNO"))
            .scan("EMP")
            .filter(
                builder.equals(builder.field("DEPTNO"),
                    builder.literal(20)))
            .project(builder.field("EMPNO"))
            .union(true)
            .build();
    final String expected = ""
        + "LogicalUnion(all=[true])\n"
        + "  LogicalProject(DEPTNO=[$0])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "  LogicalProject(EMPNO=[$0])\n"
        + "    LogicalFilter(condition=[=($7, 20)])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1522">[CALCITE-1522]
   * Fix error message for SetOp with incompatible args</a>. */
  @Test void testBadUnionArgsErrorMessage() {
    // Equivalent SQL:
    //   SELECT EMPNO, SAL FROM emp
    //   UNION ALL
    //   SELECT DEPTNO FROM dept
    final RelBuilder builder = RelBuilder.create(config().build());
    try {
      final RelNode root =
          builder.scan("DEPT")
              .project(builder.field("DEPTNO"))
              .scan("EMP")
              .project(builder.field("EMPNO"), builder.field("SAL"))
              .union(true)
              .build();
      fail("Expected error, got " + root);
    } catch (IllegalArgumentException e) {
      final String expected = "Cannot compute compatible row type for "
          + "arguments to set op: RecordType(TINYINT DEPTNO), "
          + "RecordType(SMALLINT EMPNO, DECIMAL(7, 2) SAL)";
      assertThat(e.getMessage(), is(expected));
    }
  }

  @Test void testUnion3() {
    // Equivalent SQL:
    //   SELECT deptno FROM dept
    //   UNION ALL
    //   SELECT empno FROM emp
    //   UNION ALL
    //   SELECT deptno FROM emp
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("DEPT")
            .project(builder.field("DEPTNO"))
            .scan("EMP")
            .project(builder.field("EMPNO"))
            .scan("EMP")
            .project(builder.field("DEPTNO"))
            .union(true, 3)
            .build();
    final String expected = ""
        + "LogicalUnion(all=[true])\n"
        + "  LogicalProject(DEPTNO=[$0])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "  LogicalProject(EMPNO=[$0])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalProject(DEPTNO=[$7])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testUnion1() {
    // Equivalent SQL:
    //   SELECT deptno FROM dept
    //   UNION ALL
    //   SELECT empno FROM emp
    //   UNION ALL
    //   SELECT deptno FROM emp
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("DEPT")
            .project(builder.field("DEPTNO"))
            .scan("EMP")
            .project(builder.field("EMPNO"))
            .scan("EMP")
            .project(builder.field("DEPTNO"))
            .union(true, 1)
            .build();
    final String expected = "LogicalProject(DEPTNO=[$7])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testRepeatUnion1() {
    // Generates the sequence 1,2,3,...10 using a repeat union. Equivalent SQL:
    //   WITH RECURSIVE delta(n) AS (
    //     VALUES (1)
    //     UNION ALL
    //     SELECT n+1 FROM delta WHERE n < 10
    //   )
    //   SELECT * FROM delta
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.values(new String[] { "i" }, 1)
            .transientScan("DELTA_TABLE")
            .filter(
                builder.call(
                    SqlStdOperatorTable.LESS_THAN,
                    builder.field(0),
                    builder.literal(10)))
            .project(
                builder.call(SqlStdOperatorTable.PLUS,
                    builder.field(0),
                    builder.literal(1)))
            .repeatUnion("DELTA_TABLE", true)
            .build();
    final String expected = "LogicalRepeatUnion(all=[true])\n"
        + "  LogicalTableSpool(readType=[LAZY], writeType=[LAZY], table=[[DELTA_TABLE]])\n"
        + "    LogicalValues(tuples=[[{ 1 }]])\n"
        + "  LogicalTableSpool(readType=[LAZY], writeType=[LAZY], table=[[DELTA_TABLE]])\n"
        + "    LogicalProject($f0=[+($0, 1)])\n"
        + "      LogicalFilter(condition=[<($0, 10)])\n"
        + "        LogicalTableScan(table=[[DELTA_TABLE]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testRepeatUnion2() {
    // Generates the factorial function from 0 to 7. Equivalent SQL:
    //   WITH RECURSIVE delta (n, fact) AS (
    //     VALUES (0, 1)
    //     UNION ALL
    //     SELECT n+1, (n+1)*fact FROM delta WHERE n < 7
    //   )
    //   SELECT * FROM delta
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.values(new String[] { "n", "fact" }, 0, 1)
            .transientScan("AUX")
            .filter(
                builder.call(
                    SqlStdOperatorTable.LESS_THAN,
                    builder.field("n"),
                    builder.literal(7)))
            .project(
                Arrays.asList(
                    builder.call(SqlStdOperatorTable.PLUS,
                        builder.field("n"),
                        builder.literal(1)),
                    builder.call(SqlStdOperatorTable.MULTIPLY,
                        builder.call(SqlStdOperatorTable.PLUS,
                            builder.field("n"),
                            builder.literal(1)),
                        builder.field("fact"))),
                Arrays.asList("n", "fact"))
            .repeatUnion("AUX", true)
            .build();
    final String expected = "LogicalRepeatUnion(all=[true])\n"
        + "  LogicalTableSpool(readType=[LAZY], writeType=[LAZY], table=[[AUX]])\n"
        + "    LogicalValues(tuples=[[{ 0, 1 }]])\n"
        + "  LogicalTableSpool(readType=[LAZY], writeType=[LAZY], table=[[AUX]])\n"
        + "    LogicalProject(n=[+($0, 1)], fact=[*(+($0, 1), $1)])\n"
        + "      LogicalFilter(condition=[<($0, 7)])\n"
        + "        LogicalTableScan(table=[[AUX]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testIntersect() {
    // Equivalent SQL:
    //   SELECT empno FROM emp
    //   WHERE deptno = 20
    //   INTERSECT
    //   SELECT deptno FROM dept
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("DEPT")
            .project(builder.field("DEPTNO"))
            .scan("EMP")
            .filter(
                builder.equals(builder.field("DEPTNO"),
                    builder.literal(20)))
            .project(builder.field("EMPNO"))
            .intersect(false)
            .build();
    final String expected = ""
        + "LogicalIntersect(all=[false])\n"
        + "  LogicalProject(DEPTNO=[$0])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "  LogicalProject(EMPNO=[$0])\n"
        + "    LogicalFilter(condition=[=($7, 20)])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testIntersect3() {
    // Equivalent SQL:
    //   SELECT deptno FROM dept
    //   INTERSECT ALL
    //   SELECT empno FROM emp
    //   INTERSECT ALL
    //   SELECT deptno FROM emp
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("DEPT")
            .project(builder.field("DEPTNO"))
            .scan("EMP")
            .project(builder.field("EMPNO"))
            .scan("EMP")
            .project(builder.field("DEPTNO"))
            .intersect(true, 3)
            .build();
    final String expected = ""
        + "LogicalIntersect(all=[true])\n"
        + "  LogicalProject(DEPTNO=[$0])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "  LogicalProject(EMPNO=[$0])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalProject(DEPTNO=[$7])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testExcept() {
    // Equivalent SQL:
    //   SELECT empno FROM emp
    //   WHERE deptno = 20
    //   MINUS
    //   SELECT deptno FROM dept
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("DEPT")
            .project(builder.field("DEPTNO"))
            .scan("EMP")
            .filter(
                builder.equals(builder.field("DEPTNO"),
                    builder.literal(20)))
            .project(builder.field("EMPNO"))
            .minus(false)
            .build();
    final String expected = ""
        + "LogicalMinus(all=[false])\n"
        + "  LogicalProject(DEPTNO=[$0])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "  LogicalProject(EMPNO=[$0])\n"
        + "    LogicalFilter(condition=[=($7, 20)])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Tests building a simple join. Also checks {@link RelBuilder#size()}
   * at every step. */
  @Test void testJoin() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM (SELECT * FROM emp WHERE comm IS NULL)
    //   JOIN dept ON emp.deptno = dept.deptno
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.let(b -> assertSize(b, is(0)))
            .scan("EMP")
            .let(b -> assertSize(b, is(1)))
            .filter(
                builder.call(SqlStdOperatorTable.IS_NULL,
                    builder.field("COMM")))
            .let(b -> assertSize(b, is(1)))
            .scan("DEPT")
            .let(b -> assertSize(b, is(2)))
            .join(JoinRelType.INNER,
                builder.equals(builder.field(2, 0, "DEPTNO"),
                    builder.field(2, 1, "DEPTNO")))
            .let(b -> assertSize(b, is(1)))
            .build();
    assertThat(builder.size(), is(0));
    final String expected = ""
        + "LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
        + "  LogicalFilter(condition=[IS NULL($6)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  private static RelBuilder assertSize(RelBuilder b,
      Matcher<Integer> sizeMatcher) {
    assertThat(b.size(), sizeMatcher);
    return b;
  }

  /** Same as {@link #testJoin} using USING. */
  @Test void testJoinUsing() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root2 =
        builder.scan("EMP")
            .filter(
                builder.call(SqlStdOperatorTable.IS_NULL,
                    builder.field("COMM")))
            .scan("DEPT")
            .join(JoinRelType.INNER, "DEPTNO")
            .build();
    final String expected = ""
        + "LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
        + "  LogicalFilter(condition=[IS NULL($6)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root2, hasTree(expected));
  }

  @Test void testJoin2() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   LEFT JOIN dept ON emp.deptno = dept.deptno
    //     AND emp.empno = 123
    //     AND dept.deptno IS NOT NULL
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .scan("DEPT")
            .join(JoinRelType.LEFT,
                builder.equals(builder.field(2, 0, "DEPTNO"),
                    builder.field(2, 1, "DEPTNO")),
                builder.equals(builder.field(2, 0, "EMPNO"),
                    builder.literal(123)),
                builder.isNotNull(builder.field(2, 1, "DEPTNO")))
            .build();
    // Note that "dept.deptno IS NOT NULL" has been simplified away.
    final String expected = ""
        + "LogicalJoin(condition=[AND(=($7, $8), =($0, 123))], joinType=[left])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Tests that simplification is run in
   * {@link org.apache.calcite.rex.RexUnknownAs#FALSE} mode for join
   * conditions. */
  @Test void testJoinConditionSimplification() {
    final Function<RelBuilder, RelNode> f = b ->
        b.scan("EMP")
            .scan("DEPT")
            .join(JoinRelType.INNER,
                b.or(b.literal(null),
                    b.and(b.equals(b.field(2, 0, "DEPTNO"), b.literal(1)),
                        b.equals(b.field(2, 0, "DEPTNO"), b.literal(2)),
                        b.equals(b.field(2, 1, "DEPTNO"),
                            b.field(2, 0, "DEPTNO")))))
            .build();
    final String expected = ""
        + "LogicalJoin(condition=[false], joinType=[inner])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    final String expectedWithoutSimplify = ""
        + "LogicalJoin(condition=[OR(null:NULL, "
        + "AND(=($7, 1), =($7, 2), =($8, $7)))], joinType=[inner])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));
    assertThat(f.apply(createBuilder(c -> c.withSimplify(true))),
        hasTree(expected));
    assertThat(f.apply(createBuilder(c -> c.withSimplify(false))),
        hasTree(expectedWithoutSimplify));
  }

  @Test void testJoinPushCondition() {
    final Function<RelBuilder, RelNode> f = b ->
        b.scan("EMP")
            .scan("DEPT")
            .join(JoinRelType.INNER,
                b.equals(
                    b.call(SqlStdOperatorTable.PLUS,
                        b.field(2, 0, "DEPTNO"),
                        b.field(2, 0, "EMPNO")),
                    b.field(2, 1, "DEPTNO")))
            .build();
    // SELECT * FROM EMP AS e JOIN DEPT AS d ON e.DEPTNO + e.EMPNO = d.DEPTNO
    //  becomes
    // SELECT * FROM (SELECT *, EMPNO + DEPTNO AS x FROM EMP) AS e
    // JOIN DEPT AS d ON e.x = d.DEPTNO
    final String expectedWithoutPush = ""
        + "LogicalJoin(condition=[=(+($7, $0), $8)], joinType=[inner])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    final String expected = ""
        + "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], "
        + "HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], DEPTNO0=[$9], "
        + "DNAME=[$10], LOC=[$11])\n"
        + "  LogicalJoin(condition=[=($8, $9)], joinType=[inner])\n"
        + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], "
        + "HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], $f8=[+($7, $0)])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expectedWithoutPush));
    assertThat(f.apply(createBuilder(c -> c.withPushJoinCondition(true))),
        hasTree(expected));
    assertThat(f.apply(createBuilder(c -> c.withPushJoinCondition(false))),
        hasTree(expectedWithoutPush));
  }

  @Test void testJoinCartesian() {
    // Equivalent SQL:
    //   SELECT * emp CROSS JOIN dept
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .scan("DEPT")
            .join(JoinRelType.INNER)
            .build();
    final String expected =
        "LogicalJoin(condition=[true], joinType=[inner])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testCorrelationFails() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final Holder<@Nullable RexCorrelVariable> v = Holder.empty();
    try {
      builder.scan("EMP")
          .variable(v)
          .filter(builder.equals(builder.field(0), v.get()))
          .scan("DEPT")
          .join(JoinRelType.INNER, builder.literal(true),
              ImmutableSet.of(v.get().id));
      fail("expected error");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          containsString("variable $cor0 must not be used by left input to correlation"));
    }
  }

  @Test void testCorrelationWithCondition() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final Holder<@Nullable RexCorrelVariable> v = Holder.empty();
    RelNode root = builder.scan("EMP")
        .variable(v)
        .scan("DEPT")
        .filter(
            builder.equals(builder.field(0),
                builder.field(v.get(), "DEPTNO")))
        .join(JoinRelType.LEFT,
            builder.equals(builder.field(2, 0, "SAL"),
                builder.literal(1000)),
            ImmutableSet.of(v.get().id))
        .build();
    // Note that the join filter gets pushed to the right-hand input of
    // LogicalCorrelate
    final String expected = ""
        + "LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{5, 7}])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalFilter(condition=[=($cor0.SAL, 1000)])\n"
        + "    LogicalFilter(condition=[=($0, $cor0.DEPTNO)])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testTrivialCorrelation() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final Holder<@Nullable RexCorrelVariable> v = Holder.empty();
    RelNode root = builder.scan("EMP")
        .variable(v)
        .scan("DEPT")
        .join(JoinRelType.LEFT,
            builder.equals(builder.field(2, 0, "SAL"),
                builder.literal(1000)),
            ImmutableSet.of(v.get().id))
        .build();
    // Note that the join is emitted since the query is not actually a correlated.
    final String expected = ""
        + "LogicalJoin(condition=[=($5, 1000)], joinType=[left], variablesSet=[[$cor0]])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testAntiJoin() {
    // Equivalent SQL:
    //   SELECT * FROM dept d
    //   WHERE NOT EXISTS (SELECT 1 FROM emp e WHERE e.deptno = d.deptno)
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root = builder
        .scan("DEPT")
        .scan("EMP")
        .antiJoin(
            builder.equals(
                builder.field(2, 0, "DEPTNO"),
                builder.field(2, 1, "DEPTNO")))
        .build();
    final String expected = ""
        + "LogicalJoin(condition=[=($0, $10)], joinType=[anti])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testInQuery() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   WHERE deptno IN (
    //     SELECT deptno
    //     FROM dept
    //     WHERE dname = 'Accounting')
    final Function<RelBuilder, RelNode> f = b ->
        b.scan("EMP")
            .filter(
                b.in(b.field("DEPTNO"),
                    b2 ->
                        b2.scan("DEPT")
                            .filter(
                                b2.equals(b2.field("DNAME"),
                                    b2.literal("Accounting")))
                            .project(b2.field("DEPTNO"))
                            .build()))
            .build();

    final String expected = "LogicalFilter(condition=[IN($7, {\n"
        + "LogicalProject(DEPTNO=[$0])\n"
        + "  LogicalFilter(condition=[=($1, 'Accounting')])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "})])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));
  }

  @Test void testExists() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   WHERE EXISTS (
    //     SELECT null
    //     FROM dept
    //     WHERE dname = 'Accounting')
    final Function<RelBuilder, RelNode> f = b ->
        b.scan("EMP")
            .filter(
                b.exists(b2 ->
                    b2.scan("DEPT")
                        .filter(
                            b2.equals(b2.field("DNAME"),
                                b2.literal("Accounting")))
                        .build()))
            .build();

    final String expected = "LogicalFilter(condition=[EXISTS({\n"
        + "LogicalFilter(condition=[=($1, 'Accounting')])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n"
        + "})])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));
  }

  @Test void testExistsCorrelated() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   WHERE EXISTS (
    //     SELECT null
    //     FROM dept
    //     WHERE deptno = emp.deptno)
    final Function<RelBuilder, RelNode> f = b -> {
      final Holder<@Nullable RexCorrelVariable> v = Holder.empty();
      return b.scan("EMP")
          .variable(v)
          .filter(ImmutableList.of(v.get().id),
              b.exists(b2 ->
                  b2.scan("DEPT")
                      .filter(
                          b2.equals(b2.field("DEPTNO"),
                              b2.field(v.get(), "DEPTNO")))
                      .build()))
          .build();
    };

    final String expected = "LogicalFilter(condition=[EXISTS({\n"
        + "LogicalFilter(condition=[=($0, $cor0.DEPTNO)])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n"
        + "})], variablesSet=[[$cor0]])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));
  }

  @Test void testSomeAll() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   WHERE sal > SOME (SELECT comm FROM emp)
    final Function<RelBuilder, RelNode> f = b ->
        b.scan("EMP")
            .filter(
                b.some(b.field("SAL"),
                    SqlStdOperatorTable.GREATER_THAN,
                    b2 ->
                        b2.scan("EMP")
                            .project(b2.field("COMM"))
                        .build()))
            .build();

    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   WHERE NOT (sal <= ALL (SELECT comm FROM emp))
    final Function<RelBuilder, RelNode> f2 = b ->
        b.scan("EMP")
            .filter(
                b.not(
                    b.all(b.field("SAL"),
                        SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                        b2 ->
                            b2.scan("EMP")
                                .project(b2.field("COMM"))
                                .build())))
            .build();

    final String expected = "LogicalFilter(condition=[> SOME($5, {\n"
        + "LogicalProject(COMM=[$6])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "})])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));
    assertThat(f2.apply(createBuilder()), hasTree(expected));
  }

  @Test void testUnique() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM dept
    //   WHERE UNIQUE (SELECT deptno FROM emp WHERE job = 'MANAGER')
    final Function<RelBuilder, RelNode> f = b ->
        b.scan("DEPT")
            .filter(
                b.unique(b2 ->
                    b2.scan("EMP")
                        .filter(
                            b2.equals(b2.field("JOB"),
                                b2.literal("MANAGER")))
                        .build()))
            .build();

    final String expected = "LogicalFilter(condition=[UNIQUE({\n"
        + "LogicalFilter(condition=[=($2, 'MANAGER')])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "})])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));
  }

  @Test void testScalarQuery() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   WHERE sal > (
    //     SELECT AVG(sal)
    //     FROM emp)
    final Function<RelBuilder, RelNode> f = b ->
        b.scan("EMP")
            .filter(
                b.greaterThan(b.field("SAL"),
                    b.scalarQuery(b2 ->
                        b2.scan("EMP")
                            .aggregate(b2.groupKey(),
                                b2.avg(b2.field("SAL")))
                            .build())))
            .build();

    final String expected = "LogicalFilter(condition=[>($5, $SCALAR_QUERY({\n"
        + "LogicalAggregate(group=[{}], agg#0=[AVG($5)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "}))])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));
  }

  @Test void testArrayQuery() {
    // Equivalent SQL:
    //   SELECT deptno, ARRAY (SELECT * FROM Emp)
    //   FROM Dept AS d
    final Function<RelBuilder, RelNode> f = b ->
        b.scan("DEPT")
            .project(
                b.field("DEPTNO"),
                b.arrayQuery(b2 ->
                        b2.scan("EMP")
                            .build()))
            .build();

    final String expected = "LogicalProject(DEPTNO=[$0], $f1=[ARRAY({\n"
        + "LogicalTableScan(table=[[scott, EMP]])\n"
        + "})])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));
  }

  @Test void testMultisetQuery() {
    // Equivalent SQL:
    //   SELECT deptno, MULTISET (SELECT * FROM Emp)
    //   FROM Dept AS d
    final Function<RelBuilder, RelNode> f = b ->
        b.scan("DEPT")
            .project(
                b.field("DEPTNO"),
                b.multisetQuery(b2 ->
                        b2.scan("EMP")
                            .build()))
            .build();

    final String expected = "LogicalProject(DEPTNO=[$0], $f1=[MULTISET({\n"
        + "LogicalTableScan(table=[[scott, EMP]])\n"
        + "})])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));
  }

  @Test void testMapQuery() {
    // Equivalent SQL:
    //   SELECT deptno, MAP (SELECT empno, job FROM Emp)
    //   FROM Dept AS d
    final Function<RelBuilder, RelNode> f = b ->
        b.scan("DEPT")
            .project(
                b.field("DEPTNO"),
                b.mapQuery(b2 ->
                        b2.scan("EMP")
                            .project(b2.field("EMPNO"), b2.field("JOB"))
                            .build()))
            .build();

    final String expected = "LogicalProject(DEPTNO=[$0], $f1=[MAP({\n"
        + "LogicalProject(EMPNO=[$0], JOB=[$2])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "})])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));
  }

  @Test void testAlias() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp AS e, dept
    //   WHERE e.deptno = dept.deptno
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .as("e")
            .scan("DEPT")
            .join(JoinRelType.LEFT)
            .filter(
                builder.equals(builder.field("e", "DEPTNO"),
                    builder.field("DEPT", "DEPTNO")))
            .project(builder.field("e", "ENAME"),
                builder.field("DEPT", "DNAME"))
            .build();
    final String expected = "LogicalProject(ENAME=[$1], DNAME=[$9])\n"
        + "  LogicalFilter(condition=[=($7, $8)])\n"
        + "    LogicalJoin(condition=[true], joinType=[left])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
    final RelDataTypeField field = root.getRowType().getFieldList().get(1);
    assertThat(field.getName(), is("DNAME"));
    assertThat(field.getType().isNullable(), is(true));
  }

  @Test void testAlias2() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp AS e, emp as m, dept
    //   WHERE e.deptno = dept.deptno
    //   AND m.empno = e.mgr
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .as("e")
            .scan("EMP")
            .as("m")
            .scan("DEPT")
            .join(JoinRelType.INNER)
            .join(JoinRelType.INNER)
            .filter(
                builder.equals(builder.field("e", "DEPTNO"),
                    builder.field("DEPT", "DEPTNO")),
                builder.equals(builder.field("m", "EMPNO"),
                    builder.field("e", "MGR")))
            .build();
    final String expected = ""
        + "LogicalFilter(condition=[AND(=($7, $16), =($8, $3))])\n"
        + "  LogicalJoin(condition=[true], joinType=[inner])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalJoin(condition=[true], joinType=[inner])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testAliasSort() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .as("e")
            .sort(0)
            .project(builder.field("e", "EMPNO"))
            .build();
    final String expected = ""
        + "LogicalProject(EMPNO=[$0])\n"
        + "  LogicalSort(sort0=[$0], dir0=[ASC])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testAliasLimit() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .as("e")
            .sort(1)
            .sortLimit(10, 20) // aliases were lost here if preceded by sort()
            .project(builder.field("e", "EMPNO"))
            .build();
    final String expected = ""
        + "LogicalProject(EMPNO=[$0])\n"
        + "  LogicalSort(sort0=[$1], dir0=[ASC], offset=[10], fetch=[20])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1551">[CALCITE-1551]
   * RelBuilder's project() doesn't preserve alias</a>. */
  @Test void testAliasProject() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .as("EMP_alias")
            .project(builder.field("DEPTNO"),
                builder.literal(20))
            .project(builder.field("EMP_alias", "DEPTNO"))
            .build();
    final String expected = ""
        + "LogicalProject(DEPTNO=[$7])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Tests that table aliases are propagated even when there is a project on
   * top of a project. (Aliases tend to get lost when projects are merged). */
  @Test void testAliasProjectProject() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .as("EMP_alias")
            .project(builder.field("DEPTNO"),
                builder.literal(20))
            .project(builder.field(1),
                builder.literal(10),
                builder.field(0))
            .project(builder.alias(builder.field(1), "sum"),
                builder.field("EMP_alias", "DEPTNO"))
            .build();
    final String expected = ""
        + "LogicalProject(sum=[10], DEPTNO=[$7])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Tests that table aliases are propagated and are available to a filter,
   * even when there is a project on top of a project. (Aliases tend to get lost
   * when projects are merged). */
  @Test void testAliasFilter() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .as("EMP_alias")
            .project(builder.field("DEPTNO"),
                builder.literal(20))
            .project(builder.field(1), // literal 20
                builder.literal(10),
                builder.field(0)) // DEPTNO
            .filter(
                builder.greaterThan(builder.field(1),
                    builder.field("EMP_alias", "DEPTNO")))
            .build();
    final String expected = ""
        + "LogicalFilter(condition=[>($1, $2)])\n"
        + "  LogicalProject($f1=[20], $f2=[10], DEPTNO=[$7])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Tests that the {@link RelBuilder#alias(RexNode, String)} function is
   * idempotent. */
  @Test void testScanAlias() {
    final RelBuilder builder = RelBuilder.create(config().build());
    builder.scan("EMP");

    // Simplify "emp.deptno as d as d" to "emp.deptno as d".
    final RexNode e0 =
        builder.alias(builder.alias(builder.field("DEPTNO"), "D"), "D");
    assertThat(e0.toString(), is("AS($7, 'D')"));

    // It would be nice if RelBuilder could simplify
    // "emp.deptno as deptno" to "emp.deptno", but there is not
    // enough information in RexInputRef.
    final RexNode e1 = builder.alias(builder.field("DEPTNO"), "DEPTNO");
    assertThat(e1.toString(), is("AS($7, 'DEPTNO')"));

    // The intervening alias 'DEPTNO' is removed
    final RexNode e2 =
        builder.alias(builder.alias(builder.field("DEPTNO"), "DEPTNO"), "D1");
    assertThat(e2.toString(), is("AS($7, 'D1')"));

    // Simplify "emp.deptno as d2 as d3" to "emp.deptno as d3"
    // because "d3" alias overrides "d2".
    final RexNode e3 =
        builder.alias(builder.alias(builder.field("DEPTNO"), "D2"), "D3");
    assertThat(e3.toString(), is("AS($7, 'D3')"));

    final RelNode root = builder.project(e0, e1, e2, e3).build();
    final String expected = ""
        + "LogicalProject(D=[$7], DEPTNO=[$7], D1=[$7], D3=[$7])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /**
   * Tests that project field name aliases are suggested incrementally.
   */
  @Test void testAliasSuggester() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root = builder.scan("EMP")
        .project(builder.field(0),
            builder.field(0),
            builder.field(0),
            builder.field(0),
            builder.field(0),
            builder.field(0),
            builder.field(0),
            builder.field(0),
            builder.field(0),
            builder.field(0),
            builder.field(0),
            builder.field(0))
        .build();
    final String expected = ""
        + "LogicalProject(EMPNO=[$0], EMPNO0=[$0], EMPNO1=[$0], "
        + "EMPNO2=[$0], EMPNO3=[$0], EMPNO4=[$0], EMPNO5=[$0], "
        + "EMPNO6=[$0], EMPNO7=[$0], EMPNO8=[$0], EMPNO9=[$0], EMPNO10=[$0])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testAliasAggregate() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .as("EMP_alias")
            .project(builder.field("DEPTNO"),
                builder.literal(20))
            .aggregate(builder.groupKey(builder.field("EMP_alias", "DEPTNO")),
                builder.sum(builder.field(1)))
            .project(builder.alias(builder.field(1), "sum"),
                builder.field("EMP_alias", "DEPTNO"))
            .build();
    final String expected = ""
        + "LogicalProject(sum=[$1], DEPTNO=[$0])\n"
        + "  LogicalAggregate(group=[{0}], agg#0=[SUM($1)])\n"
        + "    LogicalProject(DEPTNO=[$7], $f1=[20])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Tests that a projection retains field names after a join. */
  @Test void testProjectJoin() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .as("e")
            .scan("DEPT")
            .join(JoinRelType.INNER)
            .project(builder.field("DEPT", "DEPTNO"),
                builder.field(0),
                builder.field("e", "MGR"))
            // essentially a no-op, was previously throwing exception due to
            // project() using join-renamed fields
            .project(builder.field("DEPT", "DEPTNO"),
                builder.field(1),
                builder.field("e", "MGR"))
            .build();
    final String expected = ""
        + "LogicalProject(DEPTNO=[$8], EMPNO=[$0], MGR=[$3])\n"
        + "  LogicalJoin(condition=[true], joinType=[inner])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Tests that a projection after a projection. */
  @Test void testProjectProject() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .as("e")
            .projectPlus(
                builder.alias(
                    builder.call(SqlStdOperatorTable.PLUS, builder.field(0),
                        builder.field(3)), "x"))
            .project(builder.field("e", "DEPTNO"),
                builder.field(0),
                builder.field("e", "MGR"),
                Util.last(builder.fields()))
            .build();
    final String expected = ""
        + "LogicalProject(DEPTNO=[$7], EMPNO=[$0], MGR=[$3], x=[+($0, $3)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3462">[CALCITE-3462]
   * Add projectExcept method in RelBuilder for projecting out expressions</a>. */
  @Test void testProjectExceptWithOrdinal() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .projectExcept(
                builder.field(2),
                builder.field(3))
            .build();
    final String expected = ""
        + "LogicalProject(EMPNO=[$0], ENAME=[$1], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3462">[CALCITE-3462]
   * Add projectExcept method in RelBuilder for projecting out expressions</a>. */
  @Test void testProjectExceptWithName() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .projectExcept(
                builder.field("MGR"),
                builder.field("JOB"))
            .build();
    final String expected = ""
        + "LogicalProject(EMPNO=[$0], ENAME=[$1], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3462">[CALCITE-3462]
   * Add projectExcept method in RelBuilder for projecting out expressions</a>. */
  @Test void testProjectExceptWithExplicitAliasAndName() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .as("e")
            .projectExcept(
                builder.field("e", "MGR"),
                builder.field("e", "JOB"))
            .build();
    final String expected = ""
        + "LogicalProject(EMPNO=[$0], ENAME=[$1], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3462">[CALCITE-3462]
   * Add projectExcept method in RelBuilder for projecting out expressions</a>. */
  @Test void testProjectExceptWithImplicitAliasAndName() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .projectExcept(
                builder.field("EMP", "MGR"),
                builder.field("EMP", "JOB"))
            .build();
    final String expected = ""
        + "LogicalProject(EMPNO=[$0], ENAME=[$1], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3462">[CALCITE-3462]
   * Add projectExcept method in RelBuilder for projecting out expressions</a>. */
  @Test void testProjectExceptWithDuplicateField() {
    final RelBuilder builder = RelBuilder.create(config().build());
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
      builder.scan("EMP")
          .projectExcept(
            builder.field("EMP", "MGR"),
            builder.field("EMP", "MGR"));
    }, "Project should fail since we are trying to remove the same field two times.");
    assertThat(ex.getMessage(), containsString("Input list contains duplicates."));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3462">[CALCITE-3462]
   * Add projectExcept method in RelBuilder for projecting out expressions</a>. */
  @Test void testProjectExceptWithMissingField() {
    final RelBuilder builder = RelBuilder.create(config().build());
    builder.scan("EMP");
    RexNode deptnoField = builder.field("DEPTNO");
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
      builder.project(
            builder.field("EMPNO"),
            builder.field("ENAME"))
          .projectExcept(deptnoField);
    }, "Project should fail since we are trying to remove a field that does not exist.");
    assertThat(ex.getMessage(), allOf(containsString("Expression"), containsString("not found")));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5083">[CALCITE-5083]
   * In RelBuilder.project_, do not unwrap SARGs</a>. */
  @Test void testProjectWithSarg() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .project(
                builder.between(
                    builder.field("DEPTNO"),
                    builder.literal(20),
                    builder.literal(30)))
            .build();
    final String expected = "LogicalProject($f0=[SEARCH($7, Sarg[[20..30]])])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4409">[CALCITE-4409]
   * Improve exception when RelBuilder tries to create a field on a non-struct expression</a>. */
  @Test void testFieldOnNonStructExpression() {
    final RelBuilder builder = RelBuilder.create(config().build());
    IllegalStateException ex = assertThrows(IllegalStateException.class, () -> {
      builder.scan("EMP")
          .project(
              builder.field(builder.field("EMPNO"), "abc"))
          .build();
    }, "Field should fail since we are trying access a field on expression with non-struct type");
    assertThat(ex.getMessage(),
        is("Trying to access field abc in a type with no fields: SMALLINT"));
  }

  @Test void testMultiLevelAlias() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .as("e")
            .scan("EMP")
            .as("m")
            .scan("DEPT")
            .join(JoinRelType.INNER)
            .join(JoinRelType.INNER)
            .project(builder.field("DEPT", "DEPTNO"),
                builder.field(16),
                builder.field("m", "EMPNO"),
                builder.field("e", "MGR"))
            .as("all")
            .filter(
                builder.greaterThan(builder.field("DEPT", "DEPTNO"),
                    builder.literal(100)))
            .project(builder.field("DEPT", "DEPTNO"),
                builder.field("all", "EMPNO"))
            .build();
    final String expected = ""
        + "LogicalProject(DEPTNO=[$0], EMPNO=[$2])\n"
        + "  LogicalFilter(condition=[>($0, 100)])\n"
        + "    LogicalProject(DEPTNO=[$16], DEPTNO0=[$16], EMPNO=[$8], MGR=[$3])\n"
        + "      LogicalJoin(condition=[true], joinType=[inner])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n"
        + "        LogicalJoin(condition=[true], joinType=[inner])\n"
        + "          LogicalTableScan(table=[[scott, EMP]])\n"
        + "          LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testUnionAlias() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .as("e1")
            .project(builder.field("EMPNO"),
                builder.call(SqlStdOperatorTable.CONCAT,
                    builder.field("ENAME"),
                    builder.literal("-1")))
            .scan("EMP")
            .as("e2")
            .project(builder.field("EMPNO"),
                builder.call(SqlStdOperatorTable.CONCAT,
                    builder.field("ENAME"),
                    builder.literal("-2")))
            .union(false) // aliases lost here
            .project(builder.fields(Lists.newArrayList(1, 0)))
            .build();
    final String expected = ""
        + "LogicalProject($f1=[$1], EMPNO=[$0])\n"
        + "  LogicalUnion(all=[false])\n"
        + "    LogicalProject(EMPNO=[$0], $f1=[||($1, '-1')])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalProject(EMPNO=[$0], $f1=[||($1, '-2')])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1523">[CALCITE-1523]
   * Add RelBuilder field() method to reference aliased relations not on top of
   * stack</a>, accessing tables aliased that are not accessible in the top
   * RelNode. */
  @Test void testAliasPastTop() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   LEFT JOIN dept ON emp.deptno = dept.deptno
    //     AND emp.empno = 123
    //     AND dept.deptno IS NOT NULL
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .scan("DEPT")
            .join(JoinRelType.LEFT,
                builder.equals(builder.field(2, "EMP", "DEPTNO"),
                    builder.field(2, "DEPT", "DEPTNO")),
                builder.equals(builder.field(2, "EMP", "EMPNO"),
                    builder.literal(123)))
            .build();
    final String expected = ""
        + "LogicalJoin(condition=[AND(=($7, $8), =($0, 123))], joinType=[left])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  /** As {@link #testAliasPastTop()}. */
  @Test void testAliasPastTop2() {
    // Equivalent SQL:
    //   SELECT t1.EMPNO, t2.EMPNO, t3.DEPTNO
    //   FROM emp t1
    //   INNER JOIN emp t2 ON t1.EMPNO = t2.EMPNO
    //   INNER JOIN dept t3 ON t1.DEPTNO = t3.DEPTNO
    //     AND t2.JOB != t3.LOC
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP").as("t1")
            .scan("EMP").as("t2")
            .join(JoinRelType.INNER,
                builder.equals(builder.field(2, "t1", "EMPNO"),
                    builder.field(2, "t2", "EMPNO")))
            .scan("DEPT").as("t3")
            .join(JoinRelType.INNER,
                builder.equals(builder.field(2, "t1", "DEPTNO"),
                    builder.field(2, "t3", "DEPTNO")),
                builder.not(
                    builder.equals(builder.field(2, "t2", "JOB"),
                        builder.field(2, "t3", "LOC"))))
            .build();
    // Cols:
    // 0-7   EMP as t1
    // 8-15  EMP as t2
    // 16-18 DEPT as t3
    final String expected = ""
        + "LogicalJoin(condition=[AND(=($7, $16), <>($10, $18))], joinType=[inner])\n"
        + "  LogicalJoin(condition=[=($0, $8)], joinType=[inner])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testEmpty() {
    // Equivalent SQL:
    //   SELECT deptno, true FROM dept LIMIT 0
    // optimized to
    //   VALUES
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("DEPT")
            .project(builder.field(0), builder.literal(false))
            .empty()
            .build();
    final String expected =
        "LogicalValues(tuples=[[]])\n";
    assertThat(root, hasTree(expected));
    final String expectedType =
        "RecordType(TINYINT NOT NULL DEPTNO, BOOLEAN NOT NULL $f1) NOT NULL";
    assertThat(root.getRowType().getFullTypeString(), is(expectedType));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3172">[CALCITE-3172]
   * RelBuilder#empty does not keep aliases</a>. */
  @Test void testEmptyWithAlias() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final String expected =
        "LogicalProject(DEPTNO=[$0], DNAME=[$1])\n  LogicalValues(tuples=[[]])\n";
    final String expectedType =
        "RecordType(TINYINT NOT NULL DEPTNO, VARCHAR(14) DNAME) NOT NULL";

    // Scan + Empty + Project (without alias)
    RelNode root =
        builder.scan("DEPT")
            .empty()
            .project(
                builder.field("DEPTNO"),
                builder.field("DNAME"))
            .build();
    assertThat(root, hasTree(expected));
    assertThat(root.getRowType().getFullTypeString(), is(expectedType));

    // Scan + Empty + Project (with alias)
    root =
        builder.scan("DEPT").as("d")
            .empty()
            .project(
                builder.field(1, "d", "DEPTNO"),
                builder.field(1, "d", "DNAME"))
            .build();
    assertThat(root, hasTree(expected));
    assertThat(root.getRowType().getFullTypeString(), is(expectedType));

    // Scan + Filter false (implicitly converted into Empty) + Project (with alias)
    root =
        builder.scan("DEPT").as("d")
            .filter(builder.literal(false))
            .project(
                builder.field(1, "d", "DEPTNO"),
                builder.field(1, "d", "DNAME"))
            .build();
    assertThat(root, hasTree(expected));
    assertThat(root.getRowType().getFullTypeString(), is(expectedType));
  }

  @Test void testValues() {
    // Equivalent SQL:
    //   VALUES (true, 1), (false, -50) AS t(a, b)
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.values(new String[]{"a", "b"}, true, 1, false, -50)
            .build();
    final String expected =
        "LogicalValues(tuples=[[{ true, 1 }, { false, -50 }]])\n";
    assertThat(root, hasTree(expected));
    final String expectedType =
        "RecordType(BOOLEAN NOT NULL a, INTEGER NOT NULL b) NOT NULL";
    assertThat(root.getRowType().getFullTypeString(), is(expectedType));
  }

  /** Tests creating Values with some field names and some values null. */
  @Test void testValuesNullable() {
    // Equivalent SQL:
    //   VALUES (null, 1, 'abc'), (false, null, 'longer string')
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.values(new String[]{"a", null, "c"},
            null, 1, "abc",
            false, null, "longer string").build();
    final String expected =
        "LogicalValues(tuples=[[{ null, 1, 'abc' }, { false, null, 'longer string' }]])\n";
    assertThat(root, hasTree(expected));
    final String expectedType =
        "RecordType(BOOLEAN a, INTEGER EXPR$1, CHAR(13) NOT NULL c) NOT NULL";
    assertThat(root.getRowType().getFullTypeString(), is(expectedType));
  }

  @Test void testValuesBadNullFieldNames() {
    try {
      final RelBuilder builder = RelBuilder.create(config().build());
      RelBuilder root = builder.values((String[]) null, "a", "b");
      fail("expected error, got " + root);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("Value count must be a positive multiple of field count"));
    }
  }

  @Test void testValuesBadNoFields() {
    try {
      final RelBuilder builder = RelBuilder.create(config().build());
      RelBuilder root = builder.values(new String[0], 1, 2, 3);
      fail("expected error, got " + root);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("Value count must be a positive multiple of field count"));
    }
  }

  @Test void testValuesBadNoValues() {
    try {
      final RelBuilder builder = RelBuilder.create(config().build());
      RelBuilder root = builder.values(new String[]{"a", "b"});
      fail("expected error, got " + root);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("Value count must be a positive multiple of field count"));
    }
  }

  @Test void testValuesBadOddMultiple() {
    try {
      final RelBuilder builder = RelBuilder.create(config().build());
      RelBuilder root = builder.values(new String[] {"a", "b"}, 1, 2, 3, 4, 5);
      fail("expected error, got " + root);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("Value count must be a positive multiple of field count"));
    }
  }

  @Test void testValuesBadAllNull() {
    try {
      final RelBuilder builder = RelBuilder.create(config().build());
      RelBuilder root =
          builder.values(new String[] {"a", "b"}, null, null, 1, null);
      fail("expected error, got " + root);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("All values of field 'b' (field index 1) are null; cannot deduce type"));
    }
  }

  @Test void testValuesAllNull() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelDataType rowType =
        builder.getTypeFactory().builder()
            .add("a", SqlTypeName.BIGINT)
            .add("a", SqlTypeName.VARCHAR, 10)
            .build();
    RelNode root =
        builder.values(rowType, null, null, 1, null).build();
    final String expected =
        "LogicalValues(tuples=[[{ null, null }, { 1, null }]])\n";
    assertThat(root, hasTree(expected));
    final String expectedType =
        "RecordType(BIGINT NOT NULL a, VARCHAR(10) NOT NULL a) NOT NULL";
    assertThat(root.getRowType().getFullTypeString(), is(expectedType));
  }

  @Test void testValuesRename() {
    final Function<RelBuilder, RelNode> f = b ->
        b.values(new String[] {"a", "b"}, 1, true, 2, false)
            .rename(Arrays.asList("x", "y"))
            .build();
    final String expected =
        "LogicalValues(tuples=[[{ 1, true }, { 2, false }]])\n";
    final String expectedRowType = "RecordType(INTEGER x, BOOLEAN y)";
    assertThat(f.apply(createBuilder()), hasTree(expected));
    assertThat(f.apply(createBuilder()).getRowType().toString(),
        is(expectedRowType));
  }

  /** Tests that {@code Union(Project(Values), ... Project(Values))} is
   * simplified to {@code Values}. It occurs commonly: people write
   * {@code SELECT 1 UNION SELECT 2}. */
  @Test void testUnionProjectValues() {
    // Equivalent SQL:
    //   SELECT 'a', 1
    //   UNION ALL
    //   SELECT 'b', 2
    final BiFunction<RelBuilder, Boolean, RelNode> f = (b, all) ->
        b.values(new String[] {"zero"}, 0)
            .project(b.literal("a"), b.literal(1))
            .values(new String[] {"zero"}, 0)
            .project(b.literal("b"), b.literal(2))
            .union(all, 2)
            .build();
    final String expected =
        "LogicalValues(tuples=[[{ 'a', 1 }, { 'b', 2 }]])\n";

    // Same effect with and without ALL because tuples are distinct
    assertThat(f.apply(createBuilder(), true), hasTree(expected));
    assertThat(f.apply(createBuilder(), false), hasTree(expected));
  }

  @Test void testUnionProjectValues2() {
    // Equivalent SQL:
    //   SELECT 'a', 1 FROM (VALUES (0), (0))
    //   UNION ALL
    //   SELECT 'b', 2
    final BiFunction<RelBuilder, Boolean, RelNode> f = (b, all) ->
        b.values(new String[] {"zero"}, 0)
            .project(b.literal("a"), b.literal(1))
            .values(new String[] {"zero"}, 0, 0)
            .project(b.literal("b"), b.literal(2))
            .union(all, 2)
            .build();

    // Different effect with and without ALL because tuples are not distinct.
    final String expectedAll =
        "LogicalValues(tuples=[[{ 'a', 1 }, { 'b', 2 }, { 'b', 2 }]])\n";
    final String expectedDistinct =
        "LogicalValues(tuples=[[{ 'a', 1 }, { 'b', 2 }]])\n";
    assertThat(f.apply(createBuilder(), true), hasTree(expectedAll));
    assertThat(f.apply(createBuilder(), false), hasTree(expectedDistinct));
  }

  @Test void testSort() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   ORDER BY 3. 1 DESC
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .sort(builder.field(2), builder.desc(builder.field(0)))
            .build();
    final String expected =
        "LogicalSort(sort0=[$2], sort1=[$0], dir0=[ASC], dir1=[DESC])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
    assertThat(((Sort) root).getSortExps().toString(), is("[$2, $0]"));

    // same result using ordinals
    final RelNode root2 =
        builder.scan("EMP")
            .sort(2, -1)
            .build();
    assertThat(root2, hasTree(expected));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1015">[CALCITE-1015]
   * OFFSET 0 causes AssertionError</a>. */
  @Test void testTrivialSort() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   OFFSET 0
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .sortLimit(0, -1, ImmutableList.of())
            .build();
    final String expected = "LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testSortDuplicate() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   ORDER BY empno DESC, deptno, empno ASC, hiredate
    //
    // The sort key "empno ASC" is unnecessary and is ignored.
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .sort(builder.desc(builder.field("EMPNO")),
                builder.field("DEPTNO"),
                builder.field("EMPNO"),
                builder.field("HIREDATE"))
            .build();
    final String expected = "LogicalSort(sort0=[$0], sort1=[$7], sort2=[$4], "
        + "dir0=[DESC], dir1=[ASC], dir2=[ASC])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testSortByExpression() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   ORDER BY ename ASC NULLS LAST, hiredate + mgr DESC NULLS FIRST
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .sort(builder.nullsLast(builder.desc(builder.field(1))),
                builder.nullsFirst(
                    builder.call(SqlStdOperatorTable.PLUS, builder.field(4),
                        builder.field(3))))
            .build();
    final String expected =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalSort(sort0=[$1], sort1=[$8], dir0=[DESC-nulls-last], dir1=[ASC-nulls-first])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], $f8=[+($4, $3)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testLimit() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   OFFSET 2 FETCH 10
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .limit(2, 10)
            .build();
    final String expected =
        "LogicalSort(offset=[2], fetch=[10])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testSortLimit() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   ORDER BY deptno DESC FETCH 10
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .sortLimit(-1, 10, builder.desc(builder.field("DEPTNO")))
            .build();
    final String expected =
        "LogicalSort(sort0=[$7], dir0=[DESC], fetch=[10])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testSortLimit0() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   ORDER BY deptno DESC FETCH 0
    final Function<RelBuilder, RelNode> f = b ->
        b.scan("EMP")
            .sortLimit(-1, 0, b.desc(b.field("DEPTNO")))
            .build();
    final String expected = "LogicalValues(tuples=[[]])\n";
    final String expectedNoSimplify = ""
        + "LogicalSort(sort0=[$7], dir0=[DESC], fetch=[0])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));
    assertThat(f.apply(createBuilder(c -> c.withSimplifyLimit(true))),
        hasTree(expected));
    assertThat(f.apply(createBuilder(c -> c.withSimplifyLimit(false))),
        hasTree(expectedNoSimplify));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1610">[CALCITE-1610]
   * RelBuilder sort-combining optimization treats aliases incorrectly</a>. */
  @Test void testSortOverProjectSort() {
    final RelBuilder builder = RelBuilder.create(config().build());
    builder.scan("EMP")
        .sort(0)
        .project(builder.field(1))
        // was throwing exception here when attempting to apply to
        // inner sort node
        .limit(0, 1)
        .build();
    RelNode root = builder.scan("EMP")
        .sort(0)
        .project(Lists.newArrayList(builder.field(1)),
            Lists.newArrayList("F1"))
        .limit(0, 1)
        // make sure we can still access the field by alias
        .project(builder.field("F1"))
        .build();
    String expected = "LogicalProject(F1=[$1])\n"
        + "  LogicalSort(sort0=[$0], dir0=[ASC], fetch=[1])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Tests that a sort on a field followed by a limit gives the same
   * effect as calling sortLimit.
   *
   * <p>In general a relational operator cannot rely on the order of its input,
   * but it is reasonable to merge sort and limit if they were created by
   * consecutive builder operations. And clients such as Piglet rely on it. */
  @Test void testSortThenLimit() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .sort(builder.desc(builder.field("DEPTNO")))
            .limit(-1, 10)
            .build();
    final String expected = ""
        + "LogicalSort(sort0=[$7], dir0=[DESC], fetch=[10])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));

    final RelNode root2 =
        builder.scan("EMP")
            .sortLimit(-1, 10, builder.desc(builder.field("DEPTNO")))
            .build();
    assertThat(root2, hasTree(expected));
  }

  /** Tests that a sort on an expression followed by a limit gives the same
   * effect as calling sortLimit. */
  @Test void testSortExpThenLimit() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("DEPT")
            .sort(
                builder.desc(
                    builder.call(SqlStdOperatorTable.PLUS,
                        builder.field("DEPTNO"), builder.literal(1))))
            .limit(3, 10)
            .build();
    final String expected = ""
        + "LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2])\n"
        + "  LogicalSort(sort0=[$3], dir0=[DESC], offset=[3], fetch=[10])\n"
        + "    LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2], $f3=[+($0, 1)])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));

    final RelNode root2 =
        builder.scan("DEPT")
            .sortLimit(3, 10,
                builder.desc(
                    builder.call(SqlStdOperatorTable.PLUS,
                        builder.field("DEPTNO"), builder.literal(1))))
            .build();
    assertThat(root2, hasTree(expected));
  }

  /** Tests {@link org.apache.calcite.tools.RelRunner} for a VALUES query. */
  @Test void testRunValues() throws Exception {
    // Equivalent SQL:
    //   VALUES (true, 1), (false, -50) AS t(a, b)
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.values(new String[]{"a", "b"}, true, 1, false, -50)
            .build();
    try (PreparedStatement preparedStatement = RelRunners.run(root)) {
      String s = CalciteAssert.toString(preparedStatement.executeQuery());
      final String result = "a=true; b=1\n"
          + "a=false; b=-50\n";
      assertThat(s, is(result));
    }
  }

  /** Tests {@link org.apache.calcite.tools.RelRunner} for a table scan + filter
   * query. */
  @Test void testRun() throws Exception {
    // Equivalent SQL:
    //   SELECT * FROM EMP WHERE DEPTNO = 20
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .filter(
                builder.equals(builder.field("DEPTNO"), builder.literal(20)))
            .build();

    // Note that because the table has been resolved in the RelNode tree
    // we do not need to supply a "schema" as context to the runner.
    try (PreparedStatement preparedStatement = RelRunners.run(root)) {
      String s = CalciteAssert.toString(preparedStatement.executeQuery());
      final String result = ""
          + "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00; COMM=null; DEPTNO=20\n"
          + "EMPNO=7566; ENAME=JONES; JOB=MANAGER; MGR=7839; HIREDATE=1981-02-04; SAL=2975.00; COMM=null; DEPTNO=20\n"
          + "EMPNO=7788; ENAME=SCOTT; JOB=ANALYST; MGR=7566; HIREDATE=1987-04-19; SAL=3000.00; COMM=null; DEPTNO=20\n"
          + "EMPNO=7876; ENAME=ADAMS; JOB=CLERK; MGR=7788; HIREDATE=1987-05-23; SAL=1100.00; COMM=null; DEPTNO=20\n"
          + "EMPNO=7902; ENAME=FORD; JOB=ANALYST; MGR=7566; HIREDATE=1981-12-03; SAL=3000.00; COMM=null; DEPTNO=20\n";
      assertThat(s, is(result));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1595">[CALCITE-1595]
   * RelBuilder.call throws NullPointerException if argument types are
   * invalid</a>. */
  @Test void testTypeInferenceValidation() {
    final RelBuilder builder = RelBuilder.create(config().build());
    // test for a) call(operator, Iterable<RexNode>)
    final RexNode arg0 = builder.literal(0);
    final RexNode arg1 = builder.literal("xyz");
    try {
      builder.call(SqlStdOperatorTable.PLUS, Lists.newArrayList(arg0, arg1));
      fail("Invalid combination of parameter types");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Cannot infer return type"));
    }

    // test for b) call(operator, RexNode...)
    try {
      builder.call(SqlStdOperatorTable.PLUS, arg0, arg1);
      fail("Invalid combination of parameter types");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Cannot infer return type"));
    }
  }

  @Test void testPivot() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM (SELECT mgr, deptno, job, sal FROM emp)
    //   PIVOT (SUM(sal) AS ss, COUNT(*) AS c
    //       FOR (job, deptno)
    //       IN (('CLERK', 10) AS c10, ('MANAGER', 20) AS m20))
    //
    // translates to
    //   SELECT mgr,
    //     SUM(sal) FILTER (WHERE job = 'CLERK' AND deptno = 10) AS c10_ss,
    //     COUNT(*) FILTER (WHERE job = 'CLERK' AND deptno = 10) AS c10_c,
    //     SUM(sal) FILTER (WHERE job = 'MANAGER' AND deptno = 20) AS m20_ss,
    //     COUNT(*) FILTER (WHERE job = 'MANAGER' AND deptno = 20) AS m20_c
    //   FROM emp
    //   GROUP BY mgr
    //
    final Function<RelBuilder, RelNode> f = b ->
        b.scan("EMP")
            .pivot(b.groupKey("MGR"),
                Arrays.asList(
                    b.sum(b.field("SAL")).as("SS"),
                    b.count().as("C")),
                b.fields(Arrays.asList("JOB", "DEPTNO")),
                ImmutableMap.<String, List<RexNode>>builder()
                    .put("C10",
                        Arrays.asList(b.literal("CLERK"), b.literal(10)))
                    .put("M20",
                        Arrays.asList(b.literal("MANAGER"), b.literal(20)))
                    .build()
                    .entrySet())
            .build();
    final String expected = ""
        + "LogicalAggregate(group=[{0}], C10_SS=[SUM($1) FILTER $2], "
        + "C10_C=[COUNT() FILTER $2], M20_SS=[SUM($1) FILTER $3], "
        + "M20_C=[COUNT() FILTER $3])\n"
        + "  LogicalProject(MGR=[$3], SAL=[$5], "
        + "$f8=[IS TRUE(AND(=($2, 'CLERK'), =($7, 10)))], "
        + "$f9=[IS TRUE(AND(=($2, 'MANAGER'), =($7, 20)))])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));
  }

  @Test void testUnpivot() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM (SELECT deptno, job, sal, comm FROM emp)
    //   UNPIVOT INCLUDE NULLS (remuneration
    //     FOR remuneration_type IN (comm AS 'commission',
    //                               sal AS 'salary'))
    //
    // translates to
    //   SELECT e.deptno, e.job,
    //     CASE t.remuneration_type
    //     WHEN 'commission' THEN comm
    //     ELSE sal
    //     END AS remuneration
    //   FROM emp
    //   CROSS JOIN VALUES ('commission', 'salary') AS t (remuneration_type)
    //
    final BiFunction<RelBuilder, Boolean, RelNode> f = (b, includeNulls) ->
        b.scan("EMP")
            .unpivot(includeNulls, ImmutableList.of("REMUNERATION"),
                ImmutableList.of("REMUNERATION_TYPE"),
                Pair.zip(
                    Arrays.asList(ImmutableList.of(b.literal("commission")),
                        ImmutableList.of(b.literal("salary"))),
                    Arrays.asList(ImmutableList.of(b.field("COMM")),
                        ImmutableList.of(b.field("SAL")))))
            .build();
    final String expectedIncludeNulls = ""
        + "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], "
        + "HIREDATE=[$4], DEPTNO=[$7], REMUNERATION_TYPE=[$8], "
        + "REMUNERATION=[CASE(=($8, 'commission'), $6, =($8, 'salary'), $5, "
        + "null:NULL)])\n"
        + "  LogicalJoin(condition=[true], joinType=[inner])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalValues(tuples=[[{ 'commission' }, { 'salary' }]])\n";
    final String expectedExcludeNulls = ""
        + "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], "
        + "HIREDATE=[$4], DEPTNO=[$5], REMUNERATION_TYPE=[$6], "
        + "REMUNERATION=[CAST($7):DECIMAL(7, 2) NOT NULL])\n"
        + "  LogicalFilter(condition=[IS NOT NULL($7)])\n"
        + "    " + expectedIncludeNulls.replace("\n  ", "\n      ");
    assertThat(f.apply(createBuilder(), true), hasTree(expectedIncludeNulls));
    assertThat(f.apply(createBuilder(), false), hasTree(expectedExcludeNulls));
  }

  @Test void testMatchRecognize() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   MATCH_RECOGNIZE (
    //     PARTITION BY deptno
    //     ORDER BY empno asc
    //     MEASURES
    //       STRT.mgr as start_nw,
    //       LAST(DOWN.mgr) as bottom_nw,
    //     PATTERN (STRT DOWN+ UP+) WITHIN INTERVAL '5' SECOND
    //     DEFINE
    //       DOWN as DOWN.mgr < PREV(DOWN.mgr),
    //       UP as UP.mgr > PREV(UP.mgr)
    //   )
    final RelBuilder builder = RelBuilder.create(config().build()).scan("EMP");
    final RelDataTypeFactory typeFactory = builder.getTypeFactory();
    final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

    RexNode pattern = builder.patternConcat(
        builder.literal("STRT"),
        builder.patternQuantify(builder.literal("DOWN"), builder.literal(1),
            builder.literal(-1), builder.literal(false)),
        builder.patternQuantify(builder.literal("UP"), builder.literal(1),
            builder.literal(-1), builder.literal(false)));

    ImmutableMap.Builder<String, RexNode> pdBuilder = new ImmutableMap.Builder<>();
    RexNode downDefinition = builder.lessThan(
        builder.call(SqlStdOperatorTable.PREV,
            builder.patternField("DOWN", intType, 3),
            builder.literal(0)),
        builder.call(SqlStdOperatorTable.PREV,
            builder.patternField("DOWN", intType, 3),
            builder.literal(1)));
    pdBuilder.put("DOWN", downDefinition);
    RexNode upDefinition = builder.greaterThan(
        builder.call(SqlStdOperatorTable.PREV,
            builder.patternField("UP", intType, 3),
            builder.literal(0)),
        builder.call(SqlStdOperatorTable.PREV,
            builder.patternField("UP", intType, 3),
            builder.literal(1)));
    pdBuilder.put("UP", upDefinition);

    ImmutableList.Builder<RexNode> measuresBuilder = new ImmutableList.Builder<>();
    measuresBuilder.add(
        builder.alias(builder.patternField("STRT", intType, 3),
            "start_nw"));
    measuresBuilder.add(
        builder.alias(
            builder.call(SqlStdOperatorTable.LAST,
                builder.patternField("DOWN", intType, 3),
                builder.literal(0)),
            "bottom_nw"));

    RexNode after = builder.getRexBuilder().makeFlag(
        SqlMatchRecognize.AfterOption.SKIP_TO_NEXT_ROW);

    ImmutableList.Builder<RexNode> partitionKeysBuilder = new ImmutableList.Builder<>();
    partitionKeysBuilder.add(builder.field("DEPTNO"));

    ImmutableList.Builder<RexNode> orderKeysBuilder = new ImmutableList.Builder<>();
    orderKeysBuilder.add(builder.field("EMPNO"));

    RexNode interval = builder.literal("INTERVAL '5' SECOND");

    final ImmutableMap<String, TreeSet<String>> subsets = ImmutableMap.of();
    final RelNode root = builder
        .match(pattern, false, false, pdBuilder.build(),
            measuresBuilder.build(), after, subsets, false,
            partitionKeysBuilder.build(), orderKeysBuilder.build(), interval)
        .build();
    final String expected = "LogicalMatch(partition=[[7]], order=[[0]], "
        + "outputFields=[[$7, 'start_nw', 'bottom_nw']], allRows=[false], "
        + "after=[FLAG(SKIP TO NEXT ROW)], pattern=[(('STRT', "
        + "PATTERN_QUANTIFIER('DOWN', 1, -1, false)), "
        + "PATTERN_QUANTIFIER('UP', 1, -1, false))], "
        + "isStrictStarts=[false], isStrictEnds=[false], "
        + "interval=['INTERVAL ''5'' SECOND'], subsets=[[]], "
        + "patternDefinitions=[[<(PREV(DOWN.$3, 0), PREV(DOWN.$3, 1)), "
        + ">(PREV(UP.$3, 0), PREV(UP.$3, 1))]], "
        + "inputFields=[[EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO]])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testFilterCastAny() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelDataType anyType =
        builder.getTypeFactory().createSqlType(SqlTypeName.ANY);
    final RelNode root =
        builder.scan("EMP")
            .filter(
                builder.cast(
                    builder.getRexBuilder().makeInputRef(anyType, 0),
                    SqlTypeName.BOOLEAN))
            .build();
    final String expected = ""
        + "LogicalFilter(condition=[CAST($0):BOOLEAN NOT NULL])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testFilterCastNull() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelDataTypeFactory typeFactory = builder.getTypeFactory();
    final RelNode root =
        builder.scan("EMP")
            .filter(
                builder.getRexBuilder().makeCast(
                    typeFactory.createTypeWithNullability(
                        typeFactory.createSqlType(SqlTypeName.BOOLEAN), true),
                    builder.equals(builder.field("DEPTNO"),
                        builder.literal(10))))
            .build();
    final String expected = ""
        + "LogicalFilter(condition=[=($7, 10)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Tests {@link RelBuilder#in} with duplicate values. */
  @Test void testFilterIn() {
    final Function<RelBuilder, RelNode> f = b ->
        b.scan("EMP")
            .filter(
                b.in(b.field("DEPTNO"), b.literal(10), b.literal(20),
                    b.literal(10)))
            .build();
    final String expected = ""
        + "LogicalFilter(condition=[SEARCH($7, Sarg[10, 20])])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));
    assertThat(f.apply(createBuilder(c -> c.withSimplify(false))),
        hasTree(expected));
  }

  @Test void testFilterOrIn() {
    final Function<RelBuilder, RelNode> f = b ->
        b.scan("EMP")
            .filter(
                b.or(
                    b.greaterThan(b.field("DEPTNO"), b.literal(15)),
                    b.in(b.field("JOB"), b.literal("CLERK")),
                    b.in(b.field("DEPTNO"), b.literal(10), b.literal(20),
                        b.literal(11), b.literal(10))))
            .build();
    final String expected = ""
        + "LogicalFilter(condition=[OR(SEARCH($7, Sarg[10, 11, (15..+∞)]), =($2, 'CLERK'))])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    final String expectedWithoutSimplify = ""
        + "LogicalFilter(condition=[OR(>($7, 15), SEARCH($2, Sarg['CLERK']:CHAR(5)), SEARCH($7, "
        + "Sarg[10, 11, 20]))])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));
    assertThat(f.apply(createBuilder(c -> c.withSimplify(false))),
        hasTree(expectedWithoutSimplify));
  }

  /** Tests filter builder with correlation variables. */
  @Test void testFilterWithCorrelationVariables() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final Holder<@Nullable RexCorrelVariable> v = Holder.empty();
    RelNode root = builder.scan("EMP")
        .variable(v)
        .scan("DEPT")
        .filter(Collections.singletonList(v.get().id),
            builder.or(
                builder.and(
                    builder.lessThan(builder.field(v.get(), "DEPTNO"),
                        builder.literal(30)),
                    builder.greaterThan(builder.field(v.get(), "DEPTNO"),
                        builder.literal(20))),
                builder.isNull(builder.field(2))))
        .join(JoinRelType.LEFT,
            builder.equals(builder.field(2, 0, "SAL"),
                builder.literal(1000)),
            ImmutableSet.of(v.get().id))
        .build();

    final String expected = ""
        + "LogicalCorrelate(correlation=[$cor0], joinType=[left], "
        + "requiredColumns=[{5, 7}])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalFilter(condition=[=($cor0.SAL, 1000)])\n"
        + "    LogicalFilter(condition=[OR("
        + "SEARCH($cor0.DEPTNO, Sarg[(20..30)]), "
        + "IS NULL($2))], variablesSet=[[$cor0]])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";

    assertThat(root, hasTree(expected));
  }

  @Test void testFilterEmpty() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            // We intend to call
            //   filter(Iterable<CorrelationId>, RexNode...)
            // with zero varargs, not
            //   filter(Iterable<RexNode>)
            // Let's hope they're distinct after type erasure.
            .filter(ImmutableSet.<CorrelationId>of())
            .build();
    assertThat(root, hasTree("LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  /** Checks if simplification is run in
   * {@link org.apache.calcite.rex.RexUnknownAs#FALSE} mode for filter
   * conditions. */
  @Test void testFilterSimplification() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .filter(
                builder.or(
                     builder.literal(null),
                     builder.and(
                         builder.equals(builder.field(2), builder.literal(1)),
                         builder.equals(builder.field(2), builder.literal(2))
             )))
            .build();
    assertThat(root, hasTree("LogicalValues(tuples=[[]])\n"));
  }

  @Test void testFilterWithoutSimplification() {
    final RelBuilder builder = createBuilder(c -> c.withSimplify(false));
    final RelNode root =
        builder.scan("EMP")
            .filter(
                builder.or(
                    builder.literal(null),
                    builder.and(
                        builder.equals(builder.field(2), builder.literal(1)),
                        builder.equals(builder.field(2), builder.literal(2))
                    )))
            .build();
    final String expected = ""
        + "LogicalFilter(condition=[OR(null:NULL, AND(=($2, 1), =($2, 2)))])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testRelBuilderToString() {
    final RelBuilder builder = RelBuilder.create(config().build());
    builder.scan("EMP");

    // One entry on the stack, a single-node tree
    final String expected1 = "LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(Util.toLinux(builder.toString()), is(expected1));

    // One entry on the stack, a two-node tree
    builder.filter(builder.equals(builder.field(2), builder.literal(3)));
    final String expected2 = "LogicalFilter(condition=[=($2, 3)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(Util.toLinux(builder.toString()), is(expected2));

    // Two entries on the stack
    builder.scan("DEPT");
    final String expected3 = "LogicalTableScan(table=[[scott, DEPT]])\n"
        + "LogicalFilter(condition=[=($2, 3)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(Util.toLinux(builder.toString()), is(expected3));
  }

  /**
   * Ensures that relational algebra ({@link RelBuilder}) works with SQL views.
   *
   * <p>This test currently fails (thus ignored).
   */
  @Test void testExpandViewInRelBuilder() throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:")) {
      final Frameworks.ConfigBuilder configBuilder =
          expandingConfig(connection);
      final RelOptTable.ViewExpander viewExpander =
          (RelOptTable.ViewExpander) Frameworks.getPlanner(configBuilder.build());
      configBuilder.context(Contexts.of(viewExpander));
      final RelBuilder builder = RelBuilder.create(configBuilder.build());
      RelNode node = builder.scan("MYVIEW").build();

      int count = 0;
      try (PreparedStatement statement =
               connection.unwrap(RelRunner.class).prepareStatement(node);
           ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          count++;
        }
      }

      assertTrue(count > 1);
    }
  }

  @Test void testExpandViewShouldKeepAlias() throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:")) {
      final Frameworks.ConfigBuilder configBuilder =
          expandingConfig(connection);
      final RelOptTable.ViewExpander viewExpander =
          (RelOptTable.ViewExpander) Frameworks.getPlanner(configBuilder.build());
      configBuilder.context(Contexts.of(viewExpander));
      final RelBuilder builder = RelBuilder.create(configBuilder.build());
      RelNode node =
          builder.scan("MYVIEW")
              .project(
                  builder.field(1, "MYVIEW", "EMPNO"),
                  builder.field(1, "MYVIEW", "ENAME"))
              .build();
      String expected =
          "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
              + "  LogicalFilter(condition=[=(1, 1)])\n"
                  + "    LogicalTableScan(table=[[scott, EMP]])\n";
      assertThat(node, hasTree(expected));
    }
  }

  @Test void testExpandTable() throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:")) {
      // RelBuilder expands as default. Plan contains JdbcTableScan,
      // because RelBuilder.scan has called RelOptTable.toRel.
      final Frameworks.ConfigBuilder configBuilder =
          expandingConfig(connection);
      final RelBuilder builder = RelBuilder.create(configBuilder.build());
      final String expected = "LogicalFilter(condition=[>($2, 10)])\n"
          + "  JdbcTableScan(table=[[JDBC_SCOTT, EMP]])\n";
      checkExpandTable(builder, hasTree(expected));
    }
  }

  private void checkExpandTable(RelBuilder builder, Matcher<RelNode> matcher) {
    final RelNode root =
        builder.scan("JDBC_SCOTT", "EMP")
            .filter(
                builder.greaterThan(builder.field(2), builder.literal(10)))
            .build();
    assertThat(root, matcher);
  }

  @Test void testExchange() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root = builder.scan("EMP")
        .exchange(RelDistributions.hash(Lists.newArrayList(0)))
        .build();
    final String expected =
        "LogicalExchange(distribution=[hash[0]])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testSortExchange() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .sortExchange(RelDistributions.hash(Lists.newArrayList(0)),
                RelCollations.of(0))
            .build();
    final String expected =
        "LogicalSortExchange(distribution=[hash[0]], collation=[[0]])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testCorrelate() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final Holder<@Nullable RexCorrelVariable> v = Holder.empty();
    RelNode root = builder.scan("EMP")
        .variable(v)
        .scan("DEPT")
        .filter(
            builder.equals(builder.field(0),
                builder.field(v.get(), "DEPTNO")))
        .correlate(JoinRelType.LEFT, v.get().id, builder.field(2, 0, "DEPTNO"))
        .build();

    final String expected = ""
        + "LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{7}])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalFilter(condition=[=($0, $cor0.DEPTNO)])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testSimpleSemiCorrelateViaJoin() {
    RelNode root = buildSimpleCorrelateWithJoin(JoinRelType.SEMI);
    final String expected = ""
        + "LogicalJoin(condition=[=($7, $8)], joinType=[semi], variablesSet=[[$cor0]])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(
        "Join with correlate id but the id never used should be simplified to a join.",
        root, hasTree(expected));
  }

  @Test void testSemiCorrelatedViaJoin() {
    RelNode root = buildCorrelateWithJoin(JoinRelType.SEMI);
    final String expected = ""
        + "LogicalCorrelate(correlation=[$cor0], joinType=[semi], requiredColumns=[{0, 7}])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalFilter(condition=[=($cor0.DEPTNO, $0)])\n"
        + "    LogicalFilter(condition=[=($cor0.EMPNO, 'NaN')])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(
        "Correlated semi joins should emmit a correlate with a filter on the right side.",
        root, hasTree(expected));
  }

  @Test void testSimpleAntiCorrelateViaJoin() {
    RelNode root = buildSimpleCorrelateWithJoin(JoinRelType.ANTI);
    final String expected = ""
        + "LogicalJoin(condition=[=($7, $8)], joinType=[anti], variablesSet=[[$cor0]])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(
        "Join with correlate id but the id never used should be simplified to a join.",
        root, hasTree(expected));
  }

  @Test void testAntiCorrelateViaJoin() {
    RelNode root = buildCorrelateWithJoin(JoinRelType.ANTI);
    final String expected = ""
        + "LogicalCorrelate(correlation=[$cor0], joinType=[anti], requiredColumns=[{0, 7}])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalFilter(condition=[=($cor0.DEPTNO, $0)])\n"
        + "    LogicalFilter(condition=[=($cor0.EMPNO, 'NaN')])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(
        "Correlated anti joins should emmit a correlate with a filter on the right side.",
        root, hasTree(expected));  }

  @Test void testSimpleLeftCorrelateViaJoin() {
    RelNode root = buildSimpleCorrelateWithJoin(JoinRelType.LEFT);
    final String expected = ""
        + "LogicalJoin(condition=[=($7, $8)], joinType=[left], variablesSet=[[$cor0]])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(
        "Join with correlate id but the id never used should be simplified to a join.",
        root, hasTree(expected));
  }

  @Test void testLeftCorrelateViaJoin() {
    RelNode root = buildCorrelateWithJoin(JoinRelType.LEFT);
    final String expected = ""
        + "LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0, 7}])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalFilter(condition=[=($cor0.DEPTNO, $0)])\n"
        + "    LogicalFilter(condition=[=($cor0.EMPNO, 'NaN')])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(
        "Correlated left joins should emmit a correlate with a filter on the right side.",
        root, hasTree(expected));
  }

  @Test void testSimpleInnerCorrelateViaJoin() {
    RelNode root = buildSimpleCorrelateWithJoin(JoinRelType.INNER);
    final String expected = ""
        + "LogicalJoin(condition=[=($7, $8)], joinType=[inner], variablesSet=[[$cor0]])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat("Join with correlate id but never used should be simplified to a join.",
        root, hasTree(expected));
  }

  @Test void testInnerCorrelateViaJoin() {
    RelNode root = buildCorrelateWithJoin(JoinRelType.INNER);
    final String expected = ""
        + "LogicalFilter(condition=[=($7, $8)])\n"
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalFilter(condition=[=($cor0.EMPNO, 'NaN')])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(
        "Correlated inner joins should emmit a correlate with a filter on top.",
        root, hasTree(expected));
  }

  @Test void testSimpleRightCorrelateViaJoinThrowsException() {
    assertThrows(IllegalArgumentException.class,
        () -> buildSimpleCorrelateWithJoin(JoinRelType.RIGHT),
        "Right outer joins with correlated ids are invalid even if id is not used.");
  }

  @Test void testSimpleFullCorrelateViaJoinThrowsException() {
    assertThrows(IllegalArgumentException.class,
        () -> buildSimpleCorrelateWithJoin(JoinRelType.FULL),
        "Full outer joins with correlated ids are invalid even if id is not used.");
  }

  @Test void testRightCorrelateViaJoinThrowsException() {
    assertThrows(IllegalArgumentException.class,
        () -> buildCorrelateWithJoin(JoinRelType.RIGHT),
        "Right outer joins with correlated ids are invalid.");
  }

  @Test void testFullCorrelateViaJoinThrowsException() {
    assertThrows(IllegalArgumentException.class,
        () -> buildCorrelateWithJoin(JoinRelType.FULL),
        "Full outer joins with correlated ids are invalid.");
  }

  private static RelNode buildSimpleCorrelateWithJoin(JoinRelType type) {
    final RelBuilder builder = RelBuilder.create(config().build());
    final Holder<@Nullable RexCorrelVariable> v = Holder.empty();
    return builder
        .scan("EMP")
        .variable(v)
        .scan("DEPT")
        .join(type,
            builder.equals(
                builder.field(2, 0, "DEPTNO"),
                builder.field(2, 1, "DEPTNO")), ImmutableSet.of(v.get().id))
        .build();
  }

  private static RelNode buildCorrelateWithJoin(JoinRelType type) {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RexBuilder rexBuilder = builder.getRexBuilder();
    final Holder<@Nullable RexCorrelVariable> v = Holder.empty();
    return builder
        .scan("EMP")
        .variable(v)
        .scan("DEPT")
        .filter(
            builder.equals(
                rexBuilder.makeFieldAccess(v.get(), 0),
                builder.literal("NaN")))
        .join(type,
            builder.equals(
                builder.field(2, 0, "DEPTNO"),
                builder.field(2, 1, "DEPTNO")), ImmutableSet.of(v.get().id))
        .build();
  }

  @Test void testCorrelateWithComplexFields() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final Holder<@Nullable RexCorrelVariable> v = Holder.empty();
    RelNode root = builder.scan("EMP")
        .variable(v)
        .scan("DEPT")
        .filter(
            builder.equals(builder.field(0),
                builder.field(v.get(), "DEPTNO")))
        .correlate(JoinRelType.LEFT, v.get().id,
            builder.field(2, 0, "DEPTNO"),
            builder.getRexBuilder().makeCall(SqlStdOperatorTable.AS,
                builder.field(2, 0, "EMPNO"),
                builder.literal("RENAMED_EMPNO")))
        .build();

    final String expected = ""
        + "LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0, 7}])\n"
        + "  LogicalProject(RENAMED_EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalFilter(condition=[=($0, $cor0.DEPTNO)])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testDynamicParameterInLimitOffset() {
    final RelBuilder relBuilder = RelBuilder.create(config().build());
    final RelDataType intType = relBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
    final RexBuilder rexBuilder = relBuilder.getRexBuilder();

    RelNode planBefore = relBuilder
        .scan("DEPT")
        .sortLimit(rexBuilder.makeDynamicParam(intType, 1),
            rexBuilder.makeDynamicParam(intType, 0),
            ImmutableList.of())
        .build();
    String expectedLogicalPlan = "LogicalSort(offset=[?1], fetch=[?0])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(planBefore, hasTree(expectedLogicalPlan));

    RuleSet prepareRules =
        RuleSets.ofList(
            EnumerableRules.ENUMERABLE_FILTER_RULE,
            EnumerableRules.ENUMERABLE_SORT_RULE,
            EnumerableRules.ENUMERABLE_LIMIT_RULE,
            EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
    RelTraitSet desiredTraits = planBefore.getTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    Program program = Programs.of(prepareRules);
    RelNode planAfter = program.run(planBefore.getCluster().getPlanner(), planBefore,
        desiredTraits, ImmutableList.of(), ImmutableList.of());
    String expectedEnumerablePlan = "EnumerableLimit(offset=[?1], fetch=[?0])\n"
        + "  EnumerableTableScan(table=[[scott, DEPT]])\n";
    assertThat(planAfter, hasTree(expectedEnumerablePlan));

    RelMetadataQuery mq = planAfter.getCluster().getMetadataQuery();
    assertThat(mq.getMinRowCount(planAfter), is(0D));
    assertThat(mq.getMaxRowCount(planAfter), is(Double.POSITIVE_INFINITY));
  }

  @Test void testAdoptConventionEnumerable() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root = builder
        .adoptConvention(EnumerableConvention.INSTANCE)
        .scan("DEPT")
        .filter(
            builder.equals(builder.field("DEPTNO"), builder.literal(20)))
        .sort(builder.field(2), builder.desc(builder.field(0)))
        .project(builder.field(0))
        .build();
    String expected = ""
        + "EnumerableProject(DEPTNO=[$0])\n"
        + "  EnumerableSort(sort0=[$2], sort1=[$0], dir0=[ASC], dir1=[DESC])\n"
        + "    EnumerableFilter(condition=[=($0, 20)])\n"
        + "      EnumerableTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testSwitchConventions() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root = builder
        .scan("DEPT")
        .adoptConvention(EnumerableConvention.INSTANCE)
        .filter(
            builder.equals(builder.field("DEPTNO"), builder.literal(20)))
        .sort(builder.field(2), builder.desc(builder.field(0)))
        .adoptConvention(Convention.NONE)
        .project(builder.field(0))
        .build();
    String expected = ""
        + "LogicalProject(DEPTNO=[$0])\n"
        + "  EnumerableSort(sort0=[$2], sort1=[$0], dir0=[ASC], dir1=[DESC])\n"
        + "    EnumerableFilter(condition=[=($0, 20)])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testHints() {
    final RelHint indexHint = RelHint.builder("INDEX")
        .hintOption("_idx1")
        .hintOption("_idx2")
        .build();
    final RelHint propsHint = RelHint.builder("PROPERTIES")
        .inheritPath(0)
        .hintOption("parallelism", "3")
        .hintOption("mem", "20Mb")
        .build();
    final RelHint noHashJoinHint = RelHint.builder("NO_HASH_JOIN")
        .inheritPath(0)
        .build();
    final RelHint hashJoinHint = RelHint.builder("USE_HASH_JOIN")
        .hintOption("orders")
        .hintOption("products_temporal")
        .build();
    final RelBuilder builder = RelBuilder.create(config().build());
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp /*+ INDEX(_idx1, _idx2) */
    final RelNode root = builder
            .scan("EMP")
            .hints(indexHint)
            .build();
    assertThat(root,
        hasHints("[[INDEX inheritPath:[] options:[_idx1, _idx2]]]"));
    // Equivalent SQL:
    //   SELECT /*+  PROPERTIES(parallelism='3', mem='20Mb') */
    //   *
    //   FROM emp /*+ INDEX(_idx1, _idx2) */
    final RelNode root1 = builder
            .scan("EMP")
            .hints(indexHint, propsHint)
            .build();
    assertThat(root1,
        hasHints("[[INDEX inheritPath:[] options:[_idx1, _idx2]], "
            + "[PROPERTIES inheritPath:[0] options:{parallelism=3, mem=20Mb}]]"));
    // Equivalent SQL:
    //   SELECT /*+ NO_HASH_JOIN */
    //   *
    //   FROM emp
    //     join dept
    //     on emp.deptno = dept.deptno
    final RelNode root2 = builder
        .scan("EMP")
        .scan("DEPT")
        .join(JoinRelType.INNER,
            builder.equals(
                builder.field(2, 0, "DEPTNO"),
                builder.field(2, 1, "DEPTNO")))
        .hints(noHashJoinHint)
        .build();
    assertThat(root2, hasHints("[[NO_HASH_JOIN inheritPath:[0]]]"));

    // Equivalent SQL:
    //   SELECT *
    //   FROM orders
    //   JOIN products_temporal FOR SYSTEM_TIME AS OF orders.rowtime
    //   ON orders.product = products_temporal.id
    RelNode left = builder.scan("orders").build();
    RelNode right = builder.scan("products_temporal").build();
    RexNode period = builder.getRexBuilder().makeFieldAccess(
        builder.getRexBuilder().makeCorrel(left.getRowType(), new CorrelationId(0)),
        0);
    RelNode root3 =
        builder
            .push(left)
            .push(right)
            .snapshot(period)
            .correlate(
                JoinRelType.INNER,
                new CorrelationId(0),
                builder.field(2, 0, "ROWTIME"),
                builder.field(2, 0, "ID"),
                builder.field(2, 0, "PRODUCT"))
            .hints(hashJoinHint)
            .build();
    assertThat(root3,
        hasHints("[[USE_HASH_JOIN inheritPath:[] options:[orders, products_temporal]]]"));
  }

  @Test void testHintsOnEmptyStack() {
    final RelHint indexHint = RelHint.builder("INDEX")
        .hintOption("_idx1")
        .hintOption("_idx2")
        .build();
    // Attach hints on empty stack.
    final AssertionError error = assertThrows(
        AssertionError.class,
        () -> RelBuilder.create(config().build()).hints(indexHint),
        "hints() should fail on empty stack");
    assertThat(error.getMessage(),
        containsString("There is no relational expression to attach the hints"));
  }

  @Test void testHintsOnNonHintable() {
    final RelHint indexHint = RelHint.builder("INDEX")
        .hintOption("_idx1")
        .hintOption("_idx2")
        .build();
    // Attach hints on non hintable.
    final AssertionError error1 = assertThrows(
        AssertionError.class,
        () -> {
          // Equivalent SQL:
          //   SELECT *
          //   FROM emp
          //   MATCH_RECOGNIZE (
          //     PARTITION BY deptno
          //     ORDER BY empno asc
          //     MEASURES
          //       STRT.mgr as start_nw,
          //       LAST(DOWN.mgr) as bottom_nw,
          //     PATTERN (STRT DOWN+ UP+) WITHIN INTERVAL '5' SECOND
          //     DEFINE
          //       DOWN as DOWN.mgr < PREV(DOWN.mgr),
          //       UP as UP.mgr > PREV(UP.mgr)
          //   )
          final RelBuilder builder = RelBuilder.create(config().build()).scan("EMP");
          final RelDataTypeFactory typeFactory = builder.getTypeFactory();
          final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

          RexNode pattern = builder.patternConcat(
              builder.literal("STRT"),
              builder.patternQuantify(builder.literal("DOWN"), builder.literal(1),
                  builder.literal(-1), builder.literal(false)),
              builder.patternQuantify(builder.literal("UP"), builder.literal(1),
                  builder.literal(-1), builder.literal(false)));

          ImmutableMap.Builder<String, RexNode> pdBuilder = new ImmutableMap.Builder<>();
          RexNode downDefinition = builder.lessThan(
              builder.call(SqlStdOperatorTable.PREV,
                  builder.patternField("DOWN", intType, 3),
                  builder.literal(0)),
              builder.call(SqlStdOperatorTable.PREV,
                  builder.patternField("DOWN", intType, 3),
                  builder.literal(1)));
          pdBuilder.put("DOWN", downDefinition);
          RexNode upDefinition = builder.greaterThan(
              builder.call(SqlStdOperatorTable.PREV,
                  builder.patternField("UP", intType, 3),
                  builder.literal(0)),
              builder.call(SqlStdOperatorTable.PREV,
                  builder.patternField("UP", intType, 3),
                  builder.literal(1)));
          pdBuilder.put("UP", upDefinition);

          ImmutableList.Builder<RexNode> measuresBuilder = new ImmutableList.Builder<>();
          measuresBuilder.add(
              builder.alias(builder.patternField("STRT", intType, 3), "start_nw"));
          measuresBuilder.add(
              builder.alias(
                  builder.call(SqlStdOperatorTable.LAST,
                          builder.patternField("DOWN", intType, 3),
                          builder.literal(0)),
                  "bottom_nw"));

          RexNode after = builder.getRexBuilder().makeFlag(
              SqlMatchRecognize.AfterOption.SKIP_TO_NEXT_ROW);

          ImmutableList.Builder<RexNode> partitionKeysBuilder = new ImmutableList.Builder<>();
          partitionKeysBuilder.add(builder.field("DEPTNO"));

          ImmutableList.Builder<RexNode> orderKeysBuilder = new ImmutableList.Builder<>();
          orderKeysBuilder.add(builder.field("EMPNO"));

          RexNode interval = builder.literal("INTERVAL '5' SECOND");

          builder
              .match(pattern, false, false, pdBuilder.build(),
                  measuresBuilder.build(), after, ImmutableMap.of(), false,
                  partitionKeysBuilder.build(), orderKeysBuilder.build(), interval)
              .hints(indexHint);
        },
        "hints() should fail on non Hintable relational expression");
    assertThat(error1.getMessage(),
        containsString("The top relational expression is not a Hintable"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3747">[CALCITE-3747]
   * Constructing BETWEEN with RelBuilder throws class cast exception</a>.
   *
   * <p>BETWEEN is no longer allowed in RexCall. 'a BETWEEN b AND c' is expanded
   * 'a >= b AND a <= c', whether created via
   * {@link RelBuilder#call(SqlOperator, RexNode...)} or
   * {@link RelBuilder#between(RexNode, RexNode, RexNode)}.*/
  @Test void testCallBetweenOperator() {
    final RelBuilder builder = RelBuilder.create(config().build()).scan("EMP");

    final String expected = "SEARCH($0, Sarg[[1..5]])";
    final RexNode call =
        builder.call(SqlStdOperatorTable.BETWEEN,
            builder.field("EMPNO"),
            builder.literal(1),
            builder.literal(5));
    assertThat(call.toString(), is(expected));

    final RexNode call2 =
        builder.between(builder.field("EMPNO"),
            builder.literal(1),
            builder.literal(5));
    assertThat(call2.toString(), is(expected));

    final RelNode root = builder.filter(call2).build();
    final String expectedRel = ""
        + "LogicalFilter(condition=[SEARCH($0, Sarg[[1..5]])])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expectedRel));

    // Consecutive filters are not merged. (For now, anyway.)
    builder.push(root)
        .filter(
            builder.not(
                builder.equals(builder.field("EMPNO"), builder.literal(3))),
            builder.equals(builder.field("DEPTNO"), builder.literal(10)));
    final RelNode root2 = builder.build();
    final String expectedRel2 = ""
        + "LogicalFilter(condition=[AND(<>($0, 3), =($7, 10))])\n"
        + "  LogicalFilter(condition=[SEARCH($0, Sarg[[1..5]])])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root2, hasTree(expectedRel2));

    // The conditions in one filter are simplified.
    builder.scan("EMP")
        .filter(
            builder.between(builder.field("EMPNO"),
                builder.literal(1),
                builder.literal(5)),
            builder.not(
                builder.equals(builder.field("EMPNO"), builder.literal(3))),
            builder.equals(builder.field("DEPTNO"), builder.literal(10)));
    final RelNode root3 = builder.build();
    final String expectedRel3 = ""
        + "LogicalFilter(condition=[AND(SEARCH($0, Sarg[[1..3), (3..5]]), =($7, 10))])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root3, hasTree(expectedRel3));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3926">[CALCITE-3926]
   * CannotPlanException when an empty LogicalValues requires a certain collation</a>. */
  @Test void testEmptyValuesWithCollation() throws Exception {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder
            .scan("DEPT").empty()
            .sort(
                builder.field("DNAME"),
                builder.field("DEPTNO"))
            .build();
    try (PreparedStatement preparedStatement = RelRunners.run(root)) {
      final String result = CalciteAssert.toString(preparedStatement.executeQuery());
      final String expectedResult = "";
      assertThat(result, is(expectedResult));
    }
  }

  /** Tests {@link RelBuilder#isDistinctFrom} and
   * {@link RelBuilder#isNotDistinctFrom}. */
  @Test void testIsDistinctFrom() {
    final Function<RelBuilder, RelNode> f = b -> b.scan("EMP")
        .project(b.field("DEPTNO"),
            b.isNotDistinctFrom(b.field("SAL"), b.field("DEPTNO")),
            b.isNotDistinctFrom(b.field("EMPNO"), b.field("DEPTNO")),
            b.isDistinctFrom(b.field("EMPNO"), b.field("DEPTNO")))
        .build();
    // Note: skip IS NULL check when both fields are NOT NULL;
    // enclose in IS TRUE or IS NOT TRUE so that the result is BOOLEAN NOT NULL.
    final String expected = ""
        + "LogicalProject(DEPTNO=[$7], "
        + "$f1=[OR(AND(IS NULL($5), IS NULL($7)), IS TRUE(=($5, $7)))], "
        + "$f2=[IS TRUE(=($0, $7))], "
        + "$f3=[IS NOT TRUE(=($0, $7))])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(f.apply(createBuilder()), hasTree(expected));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4415">[CALCITE-4415]
   * SqlStdOperatorTable.NOT_LIKE has a wrong implementor</a>. */
  @Test void testNotLike() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .filter(
                builder.call(SqlStdOperatorTable.NOT_LIKE,
                    builder.field("ENAME"),
                    builder.literal("a%b%c")))
            .build();
    final String expected = ""
        + "LogicalFilter(condition=[NOT(LIKE($1, 'a%b%c'))])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test void testNotIlike() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .filter(
                builder.call(SqlLibraryOperators.NOT_ILIKE,
                    builder.field("ENAME"),
                    builder.literal("a%b%c")))
            .build();
    final String expected = ""
        + "LogicalFilter(condition=[NOT(ILIKE($1, 'a%b%c'))])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4415">[CALCITE-4415]
   * SqlStdOperatorTable.NOT_LIKE has a wrong implementor</a>. */
  @Test void testNotSimilarTo() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .filter(
                builder.call(
                    SqlStdOperatorTable.NOT_SIMILAR_TO,
                    builder.field("ENAME"),
                    builder.literal("a%b%c")))
            .build();
    final String expected = ""
        + "LogicalFilter(condition=[NOT(SIMILAR TO($1, 'a%b%c'))])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4415">[CALCITE-4415]
   * SqlStdOperatorTable.NOT_LIKE has a wrong implementor</a>. */
  @Test void testExecuteNotLike() {
    CalciteAssert.that()
        .withSchema("s", new ReflectiveSchema(new HrSchema()))
        .withRel(
            builder -> builder
                .scan("s", "emps")
                .filter(
                    builder.call(
                        SqlStdOperatorTable.NOT_LIKE,
                        builder.field("name"),
                        builder.literal("%r%c")))
                .project(
                    builder.field("empid"),
                    builder.field("name"))
                .build())
        .returnsUnordered(
            "empid=100; name=Bill",
            "empid=110; name=Theodore",
            "empid=150; name=Sebastian");
  }

  /** Operand to a user-defined function. */
  private interface Arg {
    String name();
    RelDataType type(RelDataTypeFactory typeFactory);
    SqlTypeFamily family();
    boolean optional();

    static SqlOperandMetadata metadata(Arg... args) {
      return OperandTypes.operandMetadata(
          Arrays.stream(args).map(Arg::family).collect(Collectors.toList()),
          typeFactory ->
              Arrays.stream(args).map(arg -> arg.type(typeFactory))
                  .collect(Collectors.toList()),
          i -> args[i].name(), i -> args[i].optional());
    }

    static Arg of(String name,
        Function<RelDataTypeFactory, RelDataType> protoType,
        SqlTypeFamily family, boolean optional) {
      return new Arg() {
        @Override public String name() {
          return name;
        }

        @Override public RelDataType type(RelDataTypeFactory typeFactory) {
          return protoType.apply(typeFactory);
        }

        @Override public SqlTypeFamily family() {
          return family;
        }

        @Override public boolean optional() {
          return optional;
        }
      };
    }
  }
}
