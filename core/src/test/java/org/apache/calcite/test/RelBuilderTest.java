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

import org.apache.calcite.plan.RelOptUtil;
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
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;

import org.junit.Test;

import java.util.List;

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
    assertThat(RelOptUtil.toString(root),
        is("LogicalTableScan(table=[[scott, EMP]])\n"));
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
    assertThat(RelOptUtil.toString(root),
        is("LogicalTableScan(table=[[scott, EMP]])\n"));
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
    assertThat(RelOptUtil.toString(root),
        is("LogicalFilter(condition=[=($7, 20)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"));
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
    assertThat(RelOptUtil.toString(root),
        is("LogicalFilter(condition=[AND(OR(=($7, 20), IS NULL($6)), IS NOT NULL($3))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"));
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
      RexNode call = builder.call(SqlStdOperatorTable.PLUS, builder.field(1),
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
    assertThat(RelOptUtil.toString(root),
        is(
            "LogicalProject(DEPTNO=[$7], COMM=[CAST($6):SMALLINT NOT NULL], $f2=[20], COMM3=[$6], C=[$6])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"));
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
                    builder.and(builder.literal(false),
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
    assertThat(RelOptUtil.toString(root),
        is("LogicalProject(DEPTNO=[$7], COMM=[CAST($6):SMALLINT NOT NULL],"
                + " $f2=[OR(=($7, 20), AND(false, =($7, 10), IS NULL($6),"
                + " NOT(IS NOT NULL($7))), =($7, 30))], n2=[IS NULL($2)],"
                + " nn2=[IS NOT NULL($3)], $f5=[20], COMM6=[$6], C=[$6])\n"
                + "  LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  @Test public void testAggregate() {
    // Equivalent SQL:
    //   SELECT COUNT(DISTINCT deptno) AS c
    //   FROM emp
    //   GROUP BY ()
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .aggregate(
                builder.groupKey(), builder.aggregateCall(
                    SqlStdOperatorTable.COUNT,
                    true,
                    "C",
                    builder.field("DEPTNO")))
            .build();
    assertThat(RelOptUtil.toString(root),
        is("LogicalAggregate(group=[{}], C=[COUNT(DISTINCT $7)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"));
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
                    builder.call(SqlStdOperatorTable.PLUS, builder.field(4),
                        builder.field(3)),
                    builder.field(1)),
                builder.aggregateCall(SqlStdOperatorTable.COUNT, false, "C"),
                builder.aggregateCall(SqlStdOperatorTable.SUM, false, "S",
                    builder.call(
                        SqlStdOperatorTable.PLUS,
                        builder.field(3),
                        builder.literal(1))))
            .build();
    assertThat(RelOptUtil.toString(root),
        is(""
            + "LogicalAggregate(group=[{1, 8}], C=[COUNT()], S=[SUM($9)])\n"
            + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], $f8=[+($4, $3)], $f9=[+($3, 1)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  @Test public void testDistinct() {
    // Equivalent SQL:
    //   SELECT DISTINCT *
    //   FROM emp
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .distinct()
            .build();
    assertThat(RelOptUtil.toString(root),
        is("LogicalAggregate(group=[{}])\n"
                + "  LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  @Test public void testUnion() {
    // Equivalent SQL:
    //   SELECT deptno FROM emp
    //   UNION ALL
    //   SELECT deptno FROM dept
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .filter(
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field("DEPTNO"),
                    builder.literal(20)))
            .project(builder.field("EMPNO"))
            .scan("DEPT")
            .project(builder.field("DEPTNO"))
            .union(true)
            .build();
    assertThat(RelOptUtil.toString(root),
        is("LogicalUnion(all=[true])\n"
            + "  LogicalProject(DEPTNO=[$0])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n"
            + "  LogicalProject(EMPNO=[$0])\n"
            + "    LogicalFilter(condition=[=($7, 20)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  @Test public void testIntersect() {
    // Equivalent SQL:
    //   SELECT empno FROM emp
    //   WHERE deptno = 20
    //   INTERSECT
    //   SELECT deptno FROM dept
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .filter(
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field("DEPTNO"),
                    builder.literal(20)))
            .project(builder.field("EMPNO"))
            .scan("DEPT")
            .project(builder.field("DEPTNO"))
            .intersect(false)
            .build();
    assertThat(RelOptUtil.toString(root),
        is("LogicalIntersect(all=[false])\n"
            + "  LogicalProject(DEPTNO=[$0])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n"
            + "  LogicalProject(EMPNO=[$0])\n"
            + "    LogicalFilter(condition=[=($7, 20)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  @Test public void testExcept() {
    // Equivalent SQL:
    //   SELECT empno FROM emp
    //   WHERE deptno = 20
    //   MINUS
    //   SELECT deptno FROM dept
    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode root =
        builder.scan("EMP")
            .filter(
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field("DEPTNO"),
                    builder.literal(20)))
            .project(builder.field("EMPNO"))
            .scan("DEPT")
            .project(builder.field("DEPTNO"))
            .minus(false)
            .build();
    assertThat(RelOptUtil.toString(root),
        is("LogicalMinus(all=[false])\n"
            + "  LogicalProject(DEPTNO=[$0])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n"
            + "  LogicalProject(EMPNO=[$0])\n"
            + "    LogicalFilter(condition=[=($7, 20)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n"));
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
    final String expected =
        "LogicalJoin(condition=[=($7, $0)], joinType=[inner])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n"
            + "  LogicalFilter(condition=[IS NULL($6)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(RelOptUtil.toString(root), is(expected));

    // Using USING method
    final RelNode root2 =
        builder.scan("EMP")
            .filter(
                builder.call(SqlStdOperatorTable.IS_NULL,
                    builder.field("COMM")))
            .scan("DEPT")
            .join(JoinRelType.INNER, "DEPTNO")
            .build();
    assertThat(RelOptUtil.toString(root2), is(expected));
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
            + "  LogicalTableScan(table=[[scott, DEPT]])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(RelOptUtil.toString(root), is(expected));
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
    assertThat(RelOptUtil.toString(root), is(expected));
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
    assertThat(RelOptUtil.toString(root), is(expected));
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
    assertThat(RelOptUtil.toString(root), is(expected));
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
    assertThat(RelOptUtil.toString(root), is(expected));

    // same result using ordinals
    final RelNode root2 =
        builder.scan("EMP")
            .sort(2, -1)
            .build();
    assertThat(RelOptUtil.toString(root2), is(expected));
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
    assertThat(RelOptUtil.toString(root), is(expected));
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
    assertThat(RelOptUtil.toString(root), is(expected));
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
    assertThat(RelOptUtil.toString(root), is(expected));
  }
}

// End RelBuilderTest.java
