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

import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunners;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.junit.Test;

import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

import static org.apache.calcite.test.Matchers.hasTree;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

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
 *   <li>{@link RelBuilder#aggregateCall} with filter</li>
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
            CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.SCOTT))
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  @Test public void testScan() {
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

  @Test public void testScanQualifiedTable() {
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

  @Test public void testScanInvalidTable() {
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

  @Test public void testScanInvalidSchema() {
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

  @Test public void testScanInvalidQualifiedTable() {
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

  @Test public void testScanValidTableWrongCase() {
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

  @Test public void testScanFilterTrue() {
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

  @Test public void testScanFilterTriviallyFalse() {
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

  @Test public void testScanFilterEquals() {
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

  @Test public void testScanFilterOr() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   WHERE (deptno = 20 OR comm IS NULL) AND mgr IS NOT NULL
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .filter(
                builder.call(SqlStdOperatorTable.OR,
                    builder.call(SqlStdOperatorTable.EQUALS,
                        builder.field("DEPTNO"),
                        builder.literal(20)),
                    builder.isNull(builder.field(6))),
                builder.isNotNull(builder.field(3)))
            .build();
    final String expected = ""
        + "LogicalFilter(condition=[AND(OR(=($7, 20), IS NULL($6)), IS NOT NULL($3))])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test public void testScanFilterOr2() {
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
                builder.call(SqlStdOperatorTable.OR,
                    builder.call(SqlStdOperatorTable.GREATER_THAN,
                        builder.field("DEPTNO"),
                        builder.literal(20)),
                    builder.call(SqlStdOperatorTable.GREATER_THAN,
                        builder.field("DEPTNO"),
                        builder.literal(20))))
            .build();
    final String expected = "LogicalFilter(condition=[>($7, 20)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test public void testScanFilterAndFalse() {
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
                builder.call(SqlStdOperatorTable.GREATER_THAN,
                    builder.field("DEPTNO"),
                    builder.literal(20)),
                builder.literal(false))
            .build();
    final String expected = "LogicalValues(tuples=[[]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test public void testScanFilterAndTrue() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   WHERE deptno = 20 AND TRUE
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .filter(
                builder.call(SqlStdOperatorTable.GREATER_THAN,
                    builder.field("DEPTNO"),
                    builder.literal(20)),
                builder.literal(true))
            .build();
    final String expected = "LogicalFilter(condition=[>($7, 20)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test public void testBadFieldName() {
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

  @Test public void testBadFieldOrdinal() {
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

  @Test public void testBadType() {
    final RelBuilder builder = RelBuilder.create(config().build());
    try {
      builder.scan("EMP");
      RexNode call = builder.call(SqlStdOperatorTable.PLUS,
          builder.field(1),
          builder.field(3));
      fail("expected error, got " + call);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("cannot derive type: +; "
              + "operands: [$1: VARCHAR(10), $3: SMALLINT]"));
    }
  }

  @Test public void testProject() {
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
  @Test public void testProject2() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .project(builder.field("DEPTNO"),
                builder.cast(builder.field(6), SqlTypeName.SMALLINT),
                builder.or(
                    builder.equals(builder.field("DEPTNO"),
                        builder.literal(20)),
                    builder.and(builder.literal(null),
                        builder.equals(builder.field("DEPTNO"),
                            builder.literal(10)),
                        builder.and(builder.isNull(builder.field(6)),
                            builder.not(builder.isNotNull(builder.field(7))))),
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
        + "LogicalProject(DEPTNO=[$7], COMM=[CAST($6):SMALLINT NOT NULL],"
        + " $f2=[OR(=($7, 20), AND(null, =($7, 10), IS NULL($6),"
        + " IS NULL($7)), =($7, 30))], n2=[IS NULL($2)],"
        + " nn2=[IS NOT NULL($3)], $f5=[20], COMM0=[$6], C=[$6])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test public void testProjectIdentity() {
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
  @Test public void testProjectIdentityWithFieldsRename() {
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
  @Test public void testProjectIdentityWithFieldsRenameFilter() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("DEPT")
            .project(builder.alias(builder.field(0), "a"),
                builder.alias(builder.field(1), "b"),
                builder.alias(builder.field(2), "c"))
            .filter(
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field("a"),
                    builder.literal(20)))
            .aggregate(builder.groupKey(0, 1, 2),
                builder.aggregateCall(SqlStdOperatorTable.SUM,
                    false, false, null, null,
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

  @Test public void testProjectLeadingEdge() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .project(builder.fields(Mappings.bijection(Arrays.asList(0, 1, 2))))
            .build();
    final String expected = "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test public void testRename() {
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

  @Test public void testRenameValues() {
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

  @Test public void testPermute() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .permute(Mappings.bijection(Arrays.asList(1, 2, 0)))
            .build();
    final String expected = "LogicalProject(JOB=[$2], EMPNO=[$0], ENAME=[$1])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test public void testConvert() {
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
        + "LogicalProject(DEPTNO=[CAST($0):BIGINT NOT NULL], DNAME=[CAST($1):VARCHAR(10) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL], LOC=[CAST($2):VARCHAR(10) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test public void testConvertRename() {
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
        + "LogicalProject(a=[CAST($0):BIGINT NOT NULL], b=[CAST($1):VARCHAR(10) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL], c=[CAST($2):VARCHAR(10) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test public void testAggregate() {
    // Equivalent SQL:
    //   SELECT COUNT(DISTINCT deptno) AS c
    //   FROM emp
    //   GROUP BY ()
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .aggregate(builder.groupKey(),
                builder.aggregateCall(SqlStdOperatorTable.COUNT, true, false,
                    null, "C", builder.field("DEPTNO")))
            .build();
    final String expected = ""
        + "LogicalAggregate(group=[{}], C=[COUNT(DISTINCT $7)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test public void testAggregate2() {
    // Equivalent SQL:
    //   SELECT COUNT(*) AS c, SUM(mgr + 1) AS s
    //   FROM emp
    //   GROUP BY ename, hiredate + mgr
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .aggregate(
                builder.groupKey(builder.field(1),
                    builder.call(SqlStdOperatorTable.PLUS,
                        builder.field(4),
                        builder.field(3)),
                    builder.field(1)),
                builder.aggregateCall(SqlStdOperatorTable.COUNT, false, false,
                    null, "C"),
                builder.aggregateCall(SqlStdOperatorTable.SUM, false, false,
                    null, "S",
                    builder.call(SqlStdOperatorTable.PLUS, builder.field(3),
                        builder.literal(1))))
            .build();
    final String expected = ""
        + "LogicalAggregate(group=[{1, 8}], C=[COUNT()], S=[SUM($9)])\n"
        + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], $f8=[+($4, $3)], $f9=[+($3, 1)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2192">[CALCITE-2192]
   * RelBuilder wrongly skips creation of Aggregate that prunes columns if input
   * is unique</a>. */
  @Test public void testAggregate3() {
    // Equivalent SQL:
    //   SELECT DISTINCT deptno FROM (
    //     SELECT deptno, COUNT(*)
    //     FROM emp
    //     GROUP BY deptno)
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .aggregate(
                builder.groupKey(builder.field(1)),
                builder.aggregateCall(SqlStdOperatorTable.COUNT, false, false,
                    null, "C"))
            .aggregate(
                builder.groupKey(builder.field(0)))
            .build();
    final String expected = ""
        + "LogicalProject(ENAME=[$0])\n"
        + "  LogicalAggregate(group=[{1}], C=[COUNT()])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** As {@link #testAggregate3()} but with Filter. */
  @Test public void testAggregate4() {
    // Equivalent SQL:
    //   SELECT DISTINCT deptno FROM (
    //     SELECT deptno, COUNT(*)
    //     FROM emp
    //     GROUP BY deptno
    //     HAVING COUNT(*) > 3)
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .aggregate(
                builder.groupKey(builder.field(1)),
                builder.aggregateCall(SqlStdOperatorTable.COUNT, false, false,
                    null, "C"))
            .filter(
                builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field(1),
                    builder.literal(3)))
            .aggregate(
                builder.groupKey(builder.field(0)))
            .build();
    final String expected = ""
        + "LogicalProject(ENAME=[$0])\n"
        + "  LogicalFilter(condition=[>($1, 3)])\n"
        + "    LogicalAggregate(group=[{1}], C=[COUNT()])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test public void testAggregateFilter() {
    // Equivalent SQL:
    //   SELECT deptno, COUNT(*) FILTER (WHERE empno > 100) AS c
    //   FROM emp
    //   GROUP BY ROLLUP(deptno)
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .aggregate(
                builder.groupKey(ImmutableBitSet.of(7),
                    ImmutableList.of(ImmutableBitSet.of(7),
                        ImmutableBitSet.of())),
                builder.aggregateCall(SqlStdOperatorTable.COUNT, false, false,
                    builder.call(SqlStdOperatorTable.GREATER_THAN,
                        builder.field("EMPNO"), builder.literal(100)), "C"))
            .build();
    final String expected = ""
        + "LogicalAggregate(group=[{7}], groups=[[{7}, {}]], C=[COUNT() FILTER $8])\n"
        + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], $f8=[>($0, 100)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test public void testAggregateFilterFails() {
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
                  builder.aggregateCall(SqlStdOperatorTable.SUM, false, false,
                      builder.field("COMM"), "C", builder.field("SAL")))
              .build();
      fail("expected error, got " + root);
    } catch (CalciteException e) {
      assertThat(e.getMessage(),
          is("FILTER expression must be of type BOOLEAN"));
    }
  }

  @Test public void testAggregateFilterNullable() {
    // Equivalent SQL:
    //   SELECT deptno, SUM(sal) FILTER (WHERE comm < 100) AS c
    //   FROM emp
    //   GROUP BY deptno
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .aggregate(
                builder.groupKey(builder.field("DEPTNO")),
                builder.aggregateCall(SqlStdOperatorTable.SUM, false, false,
                    builder.call(SqlStdOperatorTable.LESS_THAN,
                        builder.field("COMM"), builder.literal(100)), "C",
                    builder.field("SAL")))
            .build();
    final String expected = ""
        + "LogicalAggregate(group=[{7}], C=[SUM($5) FILTER $8])\n"
        + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], $f8=[IS TRUE(<($6, 100))])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1980">[CALCITE-1980]
   * RelBuilder gives NPE if groupKey contains alias</a>.
   *
   * <p>Now, the alias does not cause a new expression to be added to the input,
   * but causes the referenced fields to be renamed. */
  @Test public void testAggregateProjectWithAliases() {
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
        + "  LogicalProject(departmentNo=[$0])\n"
        + "    LogicalProject(DEPTNO=[$7])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test public void testAggregateProjectWithExpression() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
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
        + "LogicalAggregate(group=[{1}])\n"
        + "  LogicalProject(DEPTNO=[$0], d3=[$1])\n"
        + "    LogicalProject(DEPTNO=[$0], $f1=[+($0, 3)])\n"
        + "      LogicalProject(DEPTNO=[$7])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test public void testAggregateGroupingKeyOutOfRangeFails() {
    final RelBuilder builder = RelBuilder.create(config().build());
    try {
      RelNode root =
          builder.scan("EMP")
              .aggregate(builder.groupKey(ImmutableBitSet.of(17), null))
              .build();
      fail("expected error, got " + root);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("out of bounds: {17}"));
    }
  }

  @Test public void testAggregateGroupingSetNotSubsetFails() {
    final RelBuilder builder = RelBuilder.create(config().build());
    try {
      RelNode root =
          builder.scan("EMP")
              .aggregate(
                  builder.groupKey(ImmutableBitSet.of(7),
                      ImmutableList.of(ImmutableBitSet.of(4),
                          ImmutableBitSet.of())))
              .build();
      fail("expected error, got " + root);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("group set element [$4] must be a subset of group key"));
    }
  }

  @Test public void testAggregateGroupingSetDuplicateIgnored() {
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
        + "LogicalAggregate(group=[{6, 7}], groups=[[{6}, {7}]])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test public void testAggregateGrouping() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .aggregate(builder.groupKey(6, 7),
                builder.aggregateCall(SqlStdOperatorTable.GROUPING, false,
                    false, null, "g", builder.field("DEPTNO")))
            .build();
    final String expected = ""
        + "LogicalAggregate(group=[{6, 7}], g=[GROUPING($7)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test public void testAggregateGroupingWithDistinctFails() {
    final RelBuilder builder = RelBuilder.create(config().build());
    try {
      RelNode root =
          builder.scan("EMP")
              .aggregate(builder.groupKey(6, 7),
                  builder.aggregateCall(SqlStdOperatorTable.GROUPING, true,
                      false, null, "g", builder.field("DEPTNO")))
              .build();
      fail("expected error, got " + root);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("DISTINCT not allowed"));
    }
  }

  @Test public void testAggregateGroupingWithFilterFails() {
    final RelBuilder builder = RelBuilder.create(config().build());
    try {
      RelNode root =
          builder.scan("EMP")
              .aggregate(builder.groupKey(6, 7),
                  builder.aggregateCall(SqlStdOperatorTable.GROUPING, false,
                      false, builder.literal(true), "g",
                      builder.field("DEPTNO")))
              .build();
      fail("expected error, got " + root);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("FILTER not allowed"));
    }
  }

  @Test public void testDistinct() {
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

  @Test public void testDistinctAlready() {
    // DEPT is already distinct
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("DEPT")
            .distinct()
            .build();
    final String expected = "LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test public void testDistinctEmpty() {
    // Is a relation with zero columns distinct?
    // What about if we know there are zero rows?
    // It is a matter of definition: there are no duplicate rows,
    // but applying "select ... group by ()" to it would change the result.
    // In theory, we could omit the distinct if we know there is precisely one
    // row, but we don't currently.
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .filter(
                builder.call(SqlStdOperatorTable.IS_NULL,
                    builder.field("COMM")))
            .project()
            .distinct()
            .build();
    final String expected = "LogicalAggregate(group=[{}])\n"
        + "  LogicalProject\n"
        + "    LogicalFilter(condition=[IS NULL($6)])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test public void testUnion() {
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
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field("DEPTNO"),
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
  @Test public void testBadUnionArgsErrorMessage() {
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

  @Test public void testUnion3() {
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

  @Test public void testUnion1() {
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

  @Test public void testIntersect() {
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
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field("DEPTNO"),
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

  @Test public void testIntersect3() {
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

  @Test public void testExcept() {
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
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field("DEPTNO"),
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

  @Test public void testJoin() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM (SELECT * FROM emp WHERE comm IS NULL)
    //   JOIN dept ON emp.deptno = dept.deptno
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .filter(
                builder.call(SqlStdOperatorTable.IS_NULL,
                    builder.field("COMM")))
            .scan("DEPT")
            .join(JoinRelType.INNER,
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, "DEPTNO"),
                    builder.field(2, 1, "DEPTNO")))
            .build();
    final String expected = ""
        + "LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
        + "  LogicalFilter(condition=[IS NULL($6)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Same as {@link #testJoin} using USING. */
  @Test public void testJoinUsing() {
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

  @Test public void testJoin2() {
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
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, "DEPTNO"),
                    builder.field(2, 1, "DEPTNO")),
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, "EMPNO"),
                    builder.literal(123)),
                builder.call(SqlStdOperatorTable.IS_NOT_NULL,
                    builder.field(2, 1, "DEPTNO")))
            .build();
    // Note that "dept.deptno IS NOT NULL" has been simplified away.
    final String expected = ""
        + "LogicalJoin(condition=[AND(=($7, $8), =($0, 123))], joinType=[left])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test public void testJoinCartesian() {
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

  @Test public void testCorrelationFails() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final Holder<RexCorrelVariable> v = Holder.of(null);
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

  @Test public void testCorrelationWithCondition() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final Holder<RexCorrelVariable> v = Holder.of(null);
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
        + "LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{7}])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalFilter(condition=[=($cor0.SAL, 1000)])\n"
        + "    LogicalFilter(condition=[=($0, $cor0.DEPTNO)])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test public void testAlias() {
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

  @Test public void testAlias2() {
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

  @Test public void testAliasSort() {
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

  @Test public void testAliasLimit() {
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
  @Test public void testAliasProject() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .as("EMP_alias")
            .project(builder.field("DEPTNO"),
                builder.literal(20))
            .project(builder.field("EMP_alias", "DEPTNO"))
            .build();
    final String expected = ""
        + "LogicalProject(DEPTNO=[$0])\n"
        + "  LogicalProject(DEPTNO=[$7], $f1=[20])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test public void testAliasAggregate() {
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .as("EMP_alias")
            .project(builder.field("DEPTNO"),
                builder.literal(20))
            .aggregate(builder.groupKey(builder.field("EMP_alias", "DEPTNO")),
                builder.aggregateCall(SqlStdOperatorTable.SUM, false, false,
                    null, null, builder.field(1)))
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
  @Test public void testProjectJoin() {
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

  @Test public void testMultiLevelAlias() {
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
                builder.call(SqlStdOperatorTable.GREATER_THAN,
                    builder.field("DEPT", "DEPTNO"),
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

  @Test public void testUnionAlias() {
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
  @Test public void testAliasPastTop() {
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
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, "EMP", "DEPTNO"),
                    builder.field(2, "DEPT", "DEPTNO")),
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, "EMP", "EMPNO"),
                    builder.literal(123)))
            .build();
    final String expected = ""
        + "LogicalJoin(condition=[AND(=($7, $8), =($0, 123))], joinType=[left])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(root, hasTree(expected));
  }

  /** As {@link #testAliasPastTop()}. */
  @Test public void testAliasPastTop2() {
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

  @Test public void testEmpty() {
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

  @Test public void testValues() {
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
  @Test public void testValuesNullable() {
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
        "RecordType(BOOLEAN a, INTEGER expr$1, CHAR(13) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL c) NOT NULL";
    assertThat(root.getRowType().getFullTypeString(), is(expectedType));
  }

  @Test public void testValuesBadNullFieldNames() {
    try {
      final RelBuilder builder = RelBuilder.create(config().build());
      RelBuilder root = builder.values((String[]) null, "a", "b");
      fail("expected error, got " + root);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("Value count must be a positive multiple of field count"));
    }
  }

  @Test public void testValuesBadNoFields() {
    try {
      final RelBuilder builder = RelBuilder.create(config().build());
      RelBuilder root = builder.values(new String[0], 1, 2, 3);
      fail("expected error, got " + root);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("Value count must be a positive multiple of field count"));
    }
  }

  @Test public void testValuesBadNoValues() {
    try {
      final RelBuilder builder = RelBuilder.create(config().build());
      RelBuilder root = builder.values(new String[]{"a", "b"});
      fail("expected error, got " + root);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("Value count must be a positive multiple of field count"));
    }
  }

  @Test public void testValuesBadOddMultiple() {
    try {
      final RelBuilder builder = RelBuilder.create(config().build());
      RelBuilder root = builder.values(new String[] {"a", "b"}, 1, 2, 3, 4, 5);
      fail("expected error, got " + root);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("Value count must be a positive multiple of field count"));
    }
  }

  @Test public void testValuesBadAllNull() {
    try {
      final RelBuilder builder = RelBuilder.create(config().build());
      RelBuilder root =
          builder.values(new String[] {"a", "b"}, null, null, 1, null);
      fail("expected error, got " + root);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("All values of field 'b' are null; cannot deduce type"));
    }
  }

  @Test public void testValuesAllNull() {
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
        "RecordType(BIGINT NOT NULL a, VARCHAR(10) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL a) NOT NULL";
    assertThat(root.getRowType().getFullTypeString(), is(expectedType));
  }

  @Test public void testSort() {
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
  @Test public void testTrivialSort() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   OFFSET 0
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .sortLimit(0, -1, ImmutableList.<RexNode>of())
            .build();
    final String expected = "LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test public void testSortDuplicate() {
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

  @Test public void testSortByExpression() {
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

  @Test public void testLimit() {
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

  @Test public void testSortLimit() {
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

  @Test public void testSortLimit0() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   ORDER BY deptno DESC FETCH 0
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .sortLimit(-1, 0, builder.desc(builder.field("DEPTNO")))
            .build();
    final String expected = "LogicalValues(tuples=[[]])\n";
    assertThat(root, hasTree(expected));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1610">[CALCITE-1610]
   * RelBuilder sort-combining optimization treats aliases incorrectly</a>. */
  @Test public void testSortOverProjectSort() {
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
  @Test public void testSortThenLimit() {
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
  @Test public void testSortExpThenLimit() {
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
  @Test public void testRunValues() throws Exception {
    // Equivalent SQL:
    //   VALUES (true, 1), (false, -50) AS t(a, b)
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.values(new String[]{"a", "b"}, true, 1, false, -50)
            .build();
    try (final PreparedStatement preparedStatement = RelRunners.run(root)) {
      String s = CalciteAssert.toString(preparedStatement.executeQuery());
      final String result = "a=true; b=1\n"
          + "a=false; b=-50\n";
      assertThat(s, is(result));
    }
  }

  /** Tests {@link org.apache.calcite.tools.RelRunner} for a table scan + filter
   * query. */
  @Test public void testRun() throws Exception {
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
    try (final PreparedStatement preparedStatement = RelRunners.run(root)) {
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
  @Test public void testTypeInferenceValidation() {
    final RelBuilder builder = RelBuilder.create(config().build());
    // test for a) call(operator, Iterable<RexNode>)
    final RexNode arg0 = builder.literal(0);
    final RexNode arg1 = builder.literal("xyz");
    try {
      builder.call(SqlStdOperatorTable.PLUS, Lists.newArrayList(arg0, arg1));
      fail("Invalid combination of parameter types");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("cannot derive type"));
    }

    // test for b) call(operator, RexNode...)
    try {
      builder.call(SqlStdOperatorTable.PLUS, arg0, arg1);
      fail("Invalid combination of parameter types");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("cannot derive type"));
    }
  }

  @Test public void testMatchRecognize() {
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
    RexNode downDefinition = builder.call(SqlStdOperatorTable.LESS_THAN,
        builder.call(SqlStdOperatorTable.PREV,
            builder.patternField("DOWN", intType, 3),
            builder.literal(0)),
        builder.call(SqlStdOperatorTable.PREV,
            builder.patternField("DOWN", intType, 3),
            builder.literal(1)));
    pdBuilder.put("DOWN", downDefinition);
    RexNode upDefinition = builder.call(SqlStdOperatorTable.GREATER_THAN,
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
    final String expected = "LogicalMatch(partition=[[$7]], order=[[0]], "
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

  @Test public void testFilterCastAny() {
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

  @Test public void testFilterCastNull() {
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
}

// End RelBuilderTest.java
