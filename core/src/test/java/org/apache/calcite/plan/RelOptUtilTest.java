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
package org.apache.calcite.plan;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Unit test for {@link RelOptUtil} and other classes in this package.
 */
public class RelOptUtilTest {
  /** Creates a config based on the "scott" schema. */
  private static Frameworks.ConfigBuilder config() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.SCOTT));
  }

  private static final RelNode EMP_SCAN;
  private static final RelNode DEPT_SCAN;
  static {
    final RelBuilder builder = RelBuilder.create(config().build());
    EMP_SCAN = builder.scan("EMP").build();
    DEPT_SCAN = builder.scan("DEPT").build();
  }

  private static final RelDataType EMP_ROW = EMP_SCAN.getRowType();
  private static final RelDataType DEPT_ROW = DEPT_SCAN.getRowType();

  private static final List<RelDataTypeField> EMP_DEPT_JOIN_REL_FIELDS =
      Lists.newArrayList(Iterables.concat(EMP_ROW.getFieldList(), DEPT_ROW.getFieldList()));

  private RelBuilder relBuilder;

  //~ Constructors -----------------------------------------------------------

  public RelOptUtilTest() {
  }

  //~ Methods ----------------------------------------------------------------

  @Before public void setUp() {
    relBuilder = RelBuilder.create(config().build());
  }

  @Test public void testTypeDump() {
    RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType t1 =
        typeFactory.builder()
            .add("f0", SqlTypeName.DECIMAL, 5, 2)
            .add("f1", SqlTypeName.VARCHAR, 10)
            .build();
    TestUtil.assertEqualsVerbose(
        TestUtil.fold(
            "f0 DECIMAL(5, 2) NOT NULL,",
            "f1 VARCHAR(10) NOT NULL"),
        Util.toLinux(RelOptUtil.dumpType(t1) + "\n"));

    RelDataType t2 =
        typeFactory.builder()
            .add("f0", t1)
            .add("f1", typeFactory.createMultisetType(t1, -1))
            .build();
    TestUtil.assertEqualsVerbose(
        TestUtil.fold(
            "f0 RECORD (",
            "  f0 DECIMAL(5, 2) NOT NULL,",
            "  f1 VARCHAR(10) NOT NULL) NOT NULL,",
            "f1 RECORD (",
            "  f0 DECIMAL(5, 2) NOT NULL,",
            "  f1 VARCHAR(10) NOT NULL) NOT NULL MULTISET NOT NULL"),
        Util.toLinux(RelOptUtil.dumpType(t2) + "\n"));
  }

  /**
   * Tests the rules for how we name rules.
   */
  @Test public void testRuleGuessDescription() {
    assertEquals("Bar", RelOptRule.guessDescription("com.foo.Bar"));
    assertEquals("Baz", RelOptRule.guessDescription("com.flatten.Bar$Baz"));

    // yields "1" (which as an integer is an invalid
    try {
      Util.discard(RelOptRule.guessDescription("com.foo.Bar$1"));
      fail("expected exception");
    } catch (RuntimeException e) {
      assertEquals("Derived description of rule class com.foo.Bar$1 is an "
              + "integer, not valid. Supply a description manually.",
          e.getMessage());
    }
  }

  /**
   * Test {@link RelOptUtil#splitJoinCondition(RelNode, RelNode, RexNode, List, List, List)}
   * where the join condition contains just one which is a EQUAL operator.
   */
  @Test public void testSplitJoinConditionEquals() {
    int leftJoinIndex = EMP_SCAN.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = DEPT_ROW.getFieldNames().indexOf("DEPTNO");

    RexNode joinCond = relBuilder.call(SqlStdOperatorTable.EQUALS,
        RexInputRef.of(leftJoinIndex, EMP_DEPT_JOIN_REL_FIELDS),
        RexInputRef.of(EMP_ROW.getFieldCount() + rightJoinIndex, EMP_DEPT_JOIN_REL_FIELDS));

    splitJoinConditionHelper(
        joinCond,
        Collections.singletonList(leftJoinIndex),
        Collections.singletonList(rightJoinIndex),
        Collections.singletonList(true),
        relBuilder.literal(true));
  }

  /**
   * Test {@link RelOptUtil#splitJoinCondition(RelNode, RelNode, RexNode, List, List, List)}
   * where the join condition contains just one which is a IS NOT DISTINCT operator.
   */
  @Test public void testSplitJoinConditionIsNotDistinctFrom() {
    int leftJoinIndex = EMP_SCAN.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = DEPT_ROW.getFieldNames().indexOf("DEPTNO");

    RexNode joinCond = relBuilder.call(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
        RexInputRef.of(leftJoinIndex, EMP_DEPT_JOIN_REL_FIELDS),
        RexInputRef.of(EMP_ROW.getFieldCount() + rightJoinIndex, EMP_DEPT_JOIN_REL_FIELDS));

    splitJoinConditionHelper(
        joinCond,
        Collections.singletonList(leftJoinIndex),
        Collections.singletonList(rightJoinIndex),
        Collections.singletonList(false),
        relBuilder.literal(true));
  }

  /**
   * Test {@link RelOptUtil#splitJoinCondition(RelNode, RelNode, RexNode, List, List, List)}
   * where the join condition contains an expanded version of IS NOT DISTINCT
   */
  @Test public void testSplitJoinConditionExpandedIsNotDistinctFrom() {
    int leftJoinIndex = EMP_SCAN.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = DEPT_ROW.getFieldNames().indexOf("DEPTNO");

    RexInputRef leftKeyInputRef = RexInputRef.of(leftJoinIndex, EMP_DEPT_JOIN_REL_FIELDS);
    RexInputRef rightKeyInputRef =
        RexInputRef.of(EMP_ROW.getFieldCount() + rightJoinIndex, EMP_DEPT_JOIN_REL_FIELDS);
    RexNode joinCond = relBuilder.call(SqlStdOperatorTable.OR,
        relBuilder.call(SqlStdOperatorTable.EQUALS, leftKeyInputRef, rightKeyInputRef),
        relBuilder.call(SqlStdOperatorTable.AND,
            relBuilder.call(SqlStdOperatorTable.IS_NULL, leftKeyInputRef),
            relBuilder.call(SqlStdOperatorTable.IS_NULL, rightKeyInputRef)));

    splitJoinConditionHelper(
        joinCond,
        Collections.singletonList(leftJoinIndex),
        Collections.singletonList(rightJoinIndex),
        Collections.singletonList(false),
        relBuilder.literal(true));
  }

  /**
   * Test {@link RelOptUtil#splitJoinCondition(RelNode, RelNode, RexNode, List, List, List)}
   * where the join condition contains an expanded version of IS NOT DISTINCT using CASE
   */
  @Test public void testSplitJoinConditionExpandedIsNotDistinctFromUsingCase() {
    int leftJoinIndex = EMP_SCAN.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = DEPT_ROW.getFieldNames().indexOf("DEPTNO");

    RexInputRef leftKeyInputRef = RexInputRef.of(leftJoinIndex, EMP_DEPT_JOIN_REL_FIELDS);
    RexInputRef rightKeyInputRef =
        RexInputRef.of(EMP_ROW.getFieldCount() + rightJoinIndex, EMP_DEPT_JOIN_REL_FIELDS);
    RexNode joinCond = RelOptUtil.isDistinctFrom(
        relBuilder.getRexBuilder(),
        leftKeyInputRef,
        rightKeyInputRef,
        true);


    splitJoinConditionHelper(
        joinCond,
        Collections.singletonList(leftJoinIndex),
        Collections.singletonList(rightJoinIndex),
        Collections.singletonList(false),
        relBuilder.literal(true));
  }

  /**
   * Test {@link RelOptUtil#splitJoinCondition(RelNode, RelNode, RexNode, List, List, List)}
   * where the join condition contains an expanded version of IS NOT DISTINCT using CASE
   */
  @Test public void testSplitJoinConditionExpandedIsNotDistinctFromUsingCase2() {
    int leftJoinIndex = EMP_SCAN.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = DEPT_ROW.getFieldNames().indexOf("DEPTNO");

    RexInputRef leftKeyInputRef = RexInputRef.of(leftJoinIndex, EMP_DEPT_JOIN_REL_FIELDS);
    RexInputRef rightKeyInputRef =
        RexInputRef.of(EMP_ROW.getFieldCount() + rightJoinIndex, EMP_DEPT_JOIN_REL_FIELDS);
    RexNode joinCond = relBuilder.call(SqlStdOperatorTable.CASE,
        relBuilder.call(SqlStdOperatorTable.IS_NULL, leftKeyInputRef),
        relBuilder.call(SqlStdOperatorTable.IS_NULL, rightKeyInputRef),
        relBuilder.call(SqlStdOperatorTable.IS_NULL, rightKeyInputRef),
        relBuilder.call(SqlStdOperatorTable.IS_NULL, leftKeyInputRef),
        relBuilder.call(SqlStdOperatorTable.EQUALS, leftKeyInputRef, rightKeyInputRef));

    splitJoinConditionHelper(
        joinCond,
        Collections.singletonList(leftJoinIndex),
        Collections.singletonList(rightJoinIndex),
        Collections.singletonList(false),
        relBuilder.literal(true));
  }

  private static void splitJoinConditionHelper(RexNode joinCond, List<Integer> expLeftKeys,
      List<Integer> expRightKeys, List<Boolean> expFilterNulls, RexNode expRemaining) {
    List<Integer> actLeftKeys = new ArrayList<>();
    List<Integer> actRightKeys = new ArrayList<>();
    List<Boolean> actFilterNulls = new ArrayList<>();

    RexNode actRemaining = RelOptUtil.splitJoinCondition(EMP_SCAN, DEPT_SCAN, joinCond, actLeftKeys,
        actRightKeys, actFilterNulls);

    assertEquals(expRemaining, actRemaining);
    assertEquals(expFilterNulls, actFilterNulls);
    assertEquals(expLeftKeys, actLeftKeys);
    assertEquals(expRightKeys, actRightKeys);
  }

  /**
   * Test {@link RelOptUtil#pushDownJoinConditions(org.apache.calcite.rel.core.Join, RelBuilder)}
   * where the join condition contains a complex expression
   */
  @Test public void testPushDownJoinConditions() {
    int leftJoinIndex = EMP_SCAN.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = DEPT_ROW.getFieldNames().indexOf("DEPTNO");

    RexInputRef leftKeyInputRef = RexInputRef.of(leftJoinIndex, EMP_DEPT_JOIN_REL_FIELDS);
    RexInputRef rightKeyInputRef =
        RexInputRef.of(EMP_ROW.getFieldCount() + rightJoinIndex, EMP_DEPT_JOIN_REL_FIELDS);
    RexNode joinCond = relBuilder.call(SqlStdOperatorTable.EQUALS,
        relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef, relBuilder.literal(1)),
        rightKeyInputRef);


    // Build the join operator and push down join conditions
    relBuilder.push(EMP_SCAN);
    relBuilder.push(DEPT_SCAN);
    relBuilder.join(JoinRelType.INNER, joinCond);
    Join join = (Join) relBuilder.build();
    RelNode transformed = RelOptUtil.pushDownJoinConditions(join, relBuilder);

    // Assert the new join operator
    assertThat(transformed.getRowType(), is(join.getRowType()));
    assertThat(transformed, is(instanceOf(Project.class)));
    RelNode transformedInput = transformed.getInput(0);
    assertThat(transformedInput, is(instanceOf(Join.class)));
    Join newJoin = (Join) transformedInput;
    assertThat(newJoin.getCondition().toString(),
        is(
            relBuilder.call(
                SqlStdOperatorTable.EQUALS,
                // Computed field is added at the end (and index start at 0)
                RexInputRef.of(EMP_ROW.getFieldCount(), join.getRowType()),
                // Right side is shifted by 1
                RexInputRef.of(EMP_ROW.getFieldCount() + 1 + rightJoinIndex, join.getRowType()))
            .toString()));
    assertThat(newJoin.getLeft(), is(instanceOf(Project.class)));
    Project leftInput = (Project) newJoin.getLeft();
    assertThat(leftInput.getChildExps().get(EMP_ROW.getFieldCount()).toString(),
        is(relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef, relBuilder.literal(1))
            .toString()));
  }

  /**
   * Test {@link RelOptUtil#pushDownJoinConditions(org.apache.calcite.rel.core.Join, RelBuilder)}
   * where the join condition contains a complex expression
   */
  @Test public void testPushDownJoinConditionsWithIsNotDistinct() {
    int leftJoinIndex = EMP_SCAN.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = DEPT_ROW.getFieldNames().indexOf("DEPTNO");

    RexInputRef leftKeyInputRef = RexInputRef.of(leftJoinIndex, EMP_DEPT_JOIN_REL_FIELDS);
    RexInputRef rightKeyInputRef =
        RexInputRef.of(EMP_ROW.getFieldCount() + rightJoinIndex, EMP_DEPT_JOIN_REL_FIELDS);
    RexNode joinCond = relBuilder.call(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
        relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef, relBuilder.literal(1)),
        rightKeyInputRef);


    // Build the join operator and push down join conditions
    relBuilder.push(EMP_SCAN);
    relBuilder.push(DEPT_SCAN);
    relBuilder.join(JoinRelType.INNER, joinCond);
    Join join = (Join) relBuilder.build();
    RelNode transformed = RelOptUtil.pushDownJoinConditions(join, relBuilder);

    // Assert the new join operator
    assertThat(transformed.getRowType(), is(join.getRowType()));
    assertThat(transformed, is(instanceOf(Project.class)));
    RelNode transformedInput = transformed.getInput(0);
    assertThat(transformedInput, is(instanceOf(Join.class)));
    Join newJoin = (Join) transformedInput;
    assertThat(newJoin.getCondition().toString(),
        is(
            relBuilder.call(
                SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                // Computed field is added at the end (and index start at 0)
                RexInputRef.of(EMP_ROW.getFieldCount(), join.getRowType()),
                // Right side is shifted by 1
                RexInputRef.of(EMP_ROW.getFieldCount() + 1 + rightJoinIndex, join.getRowType()))
            .toString()));
    assertThat(newJoin.getLeft(), is(instanceOf(Project.class)));
    Project leftInput = (Project) newJoin.getLeft();
    assertThat(leftInput.getChildExps().get(EMP_ROW.getFieldCount()).toString(),
        is(relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef, relBuilder.literal(1))
            .toString()));

  }

  /**
   * Test {@link RelOptUtil#pushDownJoinConditions(org.apache.calcite.rel.core.Join, RelBuilder)}
   * where the join condition contains a complex expression
   */
  @Test public void testPushDownJoinConditionsWithExpandedIsNotDistinct() {
    int leftJoinIndex = EMP_SCAN.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = DEPT_ROW.getFieldNames().indexOf("DEPTNO");

    RexInputRef leftKeyInputRef = RexInputRef.of(leftJoinIndex, EMP_DEPT_JOIN_REL_FIELDS);
    RexInputRef rightKeyInputRef =
        RexInputRef.of(EMP_ROW.getFieldCount() + rightJoinIndex, EMP_DEPT_JOIN_REL_FIELDS);
    RexNode joinCond = relBuilder.call(SqlStdOperatorTable.OR,
        relBuilder.call(SqlStdOperatorTable.EQUALS,
            relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef, relBuilder.literal(1)),
            rightKeyInputRef),
        relBuilder.call(SqlStdOperatorTable.AND,
            relBuilder.call(SqlStdOperatorTable.IS_NULL,
                relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef,
                    relBuilder.literal(1))),
            relBuilder.call(SqlStdOperatorTable.IS_NULL, rightKeyInputRef)));


    // Build the join operator and push down join conditions
    relBuilder.push(EMP_SCAN);
    relBuilder.push(DEPT_SCAN);
    relBuilder.join(JoinRelType.INNER, joinCond);
    Join join = (Join) relBuilder.build();
    RelNode transformed = RelOptUtil.pushDownJoinConditions(join, relBuilder);

    // Assert the new join operator
    assertThat(transformed.getRowType(), is(join.getRowType()));
    assertThat(transformed, is(instanceOf(Project.class)));
    RelNode transformedInput = transformed.getInput(0);
    assertThat(transformedInput, is(instanceOf(Join.class)));
    Join newJoin = (Join) transformedInput;
    assertThat(newJoin.getCondition().toString(),
        is(
            relBuilder.call(
                SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                // Computed field is added at the end (and index start at 0)
                RexInputRef.of(EMP_ROW.getFieldCount(), join.getRowType()),
                // Right side is shifted by 1
                RexInputRef.of(EMP_ROW.getFieldCount() + 1 + rightJoinIndex, join.getRowType()))
                .toString()));
    assertThat(newJoin.getLeft(), is(instanceOf(Project.class)));
    Project leftInput = (Project) newJoin.getLeft();
    assertThat(leftInput.getChildExps().get(EMP_ROW.getFieldCount()).toString(),
        is(relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef, relBuilder.literal(1))
            .toString()));
  }

  /**
   * Test {@link RelOptUtil#pushDownJoinConditions(org.apache.calcite.rel.core.Join, RelBuilder)}
   * where the join condition contains a complex expression
   */
  @Test public void testPushDownJoinConditionsWithExpandedIsNotDistinctUsingCase() {
    int leftJoinIndex = EMP_SCAN.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = DEPT_ROW.getFieldNames().indexOf("DEPTNO");

    RexInputRef leftKeyInputRef = RexInputRef.of(leftJoinIndex, EMP_DEPT_JOIN_REL_FIELDS);
    RexInputRef rightKeyInputRef =
        RexInputRef.of(EMP_ROW.getFieldCount() + rightJoinIndex, EMP_DEPT_JOIN_REL_FIELDS);
    RexNode joinCond = relBuilder.call(SqlStdOperatorTable.CASE,
        relBuilder.call(SqlStdOperatorTable.IS_NULL,
            relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef, relBuilder.literal(1))),
        relBuilder.call(SqlStdOperatorTable.IS_NULL, rightKeyInputRef),
        relBuilder.call(SqlStdOperatorTable.IS_NULL, rightKeyInputRef),
        relBuilder.call(SqlStdOperatorTable.IS_NULL,
            relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef, relBuilder.literal(1))),
        relBuilder.call(SqlStdOperatorTable.EQUALS,
            relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef, relBuilder.literal(1)),
            rightKeyInputRef));


    // Build the join operator and push down join conditions
    relBuilder.push(EMP_SCAN);
    relBuilder.push(DEPT_SCAN);
    relBuilder.join(JoinRelType.INNER, joinCond);
    Join join = (Join) relBuilder.build();
    RelNode transformed = RelOptUtil.pushDownJoinConditions(join, relBuilder);

    // Assert the new join operator
    assertThat(transformed.getRowType(), is(join.getRowType()));
    assertThat(transformed, is(instanceOf(Project.class)));
    RelNode transformedInput = transformed.getInput(0);
    assertThat(transformedInput, is(instanceOf(Join.class)));
    Join newJoin = (Join) transformedInput;
    assertThat(newJoin.getCondition().toString(),
        is(
            relBuilder.call(
                SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                // Computed field is added at the end (and index start at 0)
                RexInputRef.of(EMP_ROW.getFieldCount(), join.getRowType()),
                // Right side is shifted by 1
                RexInputRef.of(EMP_ROW.getFieldCount() + 1 + rightJoinIndex, join.getRowType()))
              .toString()));
    assertThat(newJoin.getLeft(), is(instanceOf(Project.class)));
    Project leftInput = (Project) newJoin.getLeft();
    assertThat(leftInput.getChildExps().get(EMP_ROW.getFieldCount()).toString(),
        is(relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef, relBuilder.literal(1))
            .toString()));
  }
}

// End RelOptUtilTest.java
