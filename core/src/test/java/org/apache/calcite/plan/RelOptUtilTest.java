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

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
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
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit test for {@link RelOptUtil} and other classes in this package.
 */
class RelOptUtilTest {
  /** Creates a config based on the "scott" schema. */
  private static Frameworks.ConfigBuilder config() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.SCOTT));
  }

  private RelBuilder relBuilder;

  private RelNode empScan;
  private RelNode deptScan;

  private RelDataType empRow;
  private RelDataType deptRow;

  private List<RelDataTypeField> empDeptJoinRelFields;

  @BeforeEach public void setUp() {
    relBuilder = RelBuilder.create(config().build());

    empScan = relBuilder.scan("EMP").build();
    deptScan = relBuilder.scan("DEPT").build();

    empRow = empScan.getRowType();
    deptRow = deptScan.getRowType();

    empDeptJoinRelFields =
        Lists.newArrayList(Iterables.concat(empRow.getFieldList(), deptRow.getFieldList()));
  }

  @Test void testTypeDump() {
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
   * Test {@link RelOptUtil#getFullTypeDifferenceString(String, RelDataType, String, RelDataType)}
   * which returns the detained difference of two types.
   */
  @Test void testTypeDifference() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    final RelDataType t0 =
        typeFactory.builder()
            .add("f0", SqlTypeName.DECIMAL, 5, 2)
            .build();

    final RelDataType t1 =
        typeFactory.builder()
            .add("f0", SqlTypeName.DECIMAL, 5, 2)
            .add("f1", SqlTypeName.VARCHAR, 10)
            .build();

    TestUtil.assertEqualsVerbose(
        TestUtil.fold(
            "Type mismatch: the field sizes are not equal.",
            "source: RecordType(DECIMAL(5, 2) NOT NULL f0) NOT NULL",
            "target: RecordType(DECIMAL(5, 2) NOT NULL f0, VARCHAR(10) NOT NULL f1) NOT NULL"),
        Util.toLinux(RelOptUtil.getFullTypeDifferenceString("source", t0, "target", t1) + "\n"));

    RelDataType t2 =
        typeFactory.builder()
            .add("f0", SqlTypeName.DECIMAL, 5, 2)
            .add("f1", SqlTypeName.VARCHAR, 5)
            .build();

    TestUtil.assertEqualsVerbose(
        TestUtil.fold(
            "Type mismatch:",
            "source: RecordType(DECIMAL(5, 2) NOT NULL f0, VARCHAR(10) NOT NULL f1) NOT NULL",
            "target: RecordType(DECIMAL(5, 2) NOT NULL f0, VARCHAR(5) NOT NULL f1) NOT NULL",
            "Difference:",
            "f1: VARCHAR(10) NOT NULL -> VARCHAR(5) NOT NULL",
            ""),
        Util.toLinux(RelOptUtil.getFullTypeDifferenceString("source", t1, "target", t2) + "\n"));

    t2 =
        typeFactory.builder()
            .add("f0", SqlTypeName.DECIMAL, 4, 2)
            .add("f1", SqlTypeName.BIGINT)
            .build();

    TestUtil.assertEqualsVerbose(
        TestUtil.fold(
            "Type mismatch:",
            "source: RecordType(DECIMAL(5, 2) NOT NULL f0, VARCHAR(10) NOT NULL f1) NOT NULL",
            "target: RecordType(DECIMAL(4, 2) NOT NULL f0, BIGINT NOT NULL f1) NOT NULL",
            "Difference:",
            "f0: DECIMAL(5, 2) NOT NULL -> DECIMAL(4, 2) NOT NULL",
            "f1: VARCHAR(10) NOT NULL -> BIGINT NOT NULL",
            ""),
        Util.toLinux(RelOptUtil.getFullTypeDifferenceString("source", t1, "target", t2) + "\n"));

    t2 =
        typeFactory.builder()
            .add("f0", SqlTypeName.DECIMAL, 5, 2)
            .add("f1", SqlTypeName.VARCHAR, 10)
            .build();
    // Test identical types.
    assertThat(RelOptUtil.getFullTypeDifferenceString("source", t1, "target", t2), equalTo(""));
    assertThat(RelOptUtil.getFullTypeDifferenceString("source", t1, "target", t1), equalTo(""));
  }

  /**
   * Tests the rules for how we name rules.
   */
  @Test void testRuleGuessDescription() {
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3136">[CALCITE-3136]
   * Fix the default rule description of ConverterRule</a>. */
  @Test void testConvertRuleDefaultRuleDescription() {
    final RelCollation collation1 =
        RelCollations.of(new RelFieldCollation(4, RelFieldCollation.Direction.DESCENDING));
    final RelCollation collation2 =
        RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING));
    final RelDistribution distribution1 = RelDistributions.hash(ImmutableList.of(0, 1));
    final RelDistribution distribution2 = RelDistributions.range(ImmutableList.of());
    final RelOptRule collationConvertRule =
        MyConverterRule.create(collation1, collation2);
    final RelOptRule distributionConvertRule =
        MyConverterRule.create(distribution1, distribution2);
    final RelOptRule compositeConvertRule =
        MyConverterRule.create(
            RelCompositeTrait.of(RelCollationTraitDef.INSTANCE,
                ImmutableList.of(collation2, collation1)),
            RelCompositeTrait.of(RelCollationTraitDef.INSTANCE,
                ImmutableList.of(collation1)));
    final RelOptRule compositeConvertRule0 =
        MyConverterRule.create(
            RelCompositeTrait.of(RelDistributionTraitDef.INSTANCE,
                ImmutableList.of(distribution1, distribution2)),
            RelCompositeTrait.of(RelDistributionTraitDef.INSTANCE,
                ImmutableList.of(distribution1)));
    assertThat(collationConvertRule.toString(),
        is("ConverterRule(in:[4 DESC],out:[0 DESC])"));
    assertThat(distributionConvertRule.toString(),
        is("ConverterRule(in:hash[0, 1],out:range)"));
    assertThat(compositeConvertRule.toString(),
        is("ConverterRule(in:[[0 DESC], [4 DESC]],out:[4 DESC])"));
    assertThat(compositeConvertRule0.toString(),
        is("ConverterRule(in:[hash[0, 1], range],out:hash[0, 1])"));
    try {
      Util.discard(
          MyConverterRule.create(
              new Convention.Impl("{sourceConvention}", RelNode.class),
              new Convention.Impl("<targetConvention>", RelNode.class)));
      fail("expected exception");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(),
          is("Rule description 'ConverterRule(in:{sourceConvention},"
              + "out:<targetConvention>)' is not valid"));
    }
  }

  /**
   * Test {@link RelOptUtil#splitJoinCondition(RelNode, RelNode, RexNode, List, List, List)}
   * where the join condition contains just one which is a EQUAL operator.
   */
  @Test void testSplitJoinConditionEquals() {
    int leftJoinIndex = empScan.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = deptRow.getFieldNames().indexOf("DEPTNO");

    RexNode joinCond = relBuilder.equals(
        RexInputRef.of(leftJoinIndex, empDeptJoinRelFields),
        RexInputRef.of(empRow.getFieldCount() + rightJoinIndex, empDeptJoinRelFields));

    splitJoinConditionHelper(
        joinCond,
        Collections.singletonList(leftJoinIndex),
        Collections.singletonList(rightJoinIndex),
        Collections.singletonList(true),
        relBuilder.literal(true));
  }

  @Test void testSplitJoinConditionWithoutEqualCondition() {
    final List<RelDataTypeField> sysFieldList = Collections.emptyList();
    final List<List<RexNode>> joinKeys = Arrays.asList(new ArrayList<>(), new ArrayList<>());
    final RexNode joinCondition = relBuilder.equals(
        RexInputRef.of(0, empDeptJoinRelFields),
        relBuilder.literal(1));
    final RexNode result = RelOptUtil.splitJoinCondition(
        sysFieldList,
        Arrays.asList(empScan, deptScan),
        joinCondition,
        joinKeys,
        null,
        null
    );
    final List<List<RexNode>> expectedJoinKeys = Arrays.asList(
        Collections.emptyList(),
        Collections.emptyList());
    assertEquals(joinKeys, expectedJoinKeys);
    assertEquals(result, joinCondition);
  }

  /**
   * Test {@link RelOptUtil#splitJoinCondition(RelNode, RelNode, RexNode, List, List, List)}
   * where the join condition contains just one which is a IS NOT DISTINCT operator.
   */
  @Test void testSplitJoinConditionIsNotDistinctFrom() {
    int leftJoinIndex = empScan.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = deptRow.getFieldNames().indexOf("DEPTNO");

    RexNode joinCond = relBuilder.isNotDistinctFrom(
        RexInputRef.of(leftJoinIndex, empDeptJoinRelFields),
        RexInputRef.of(empRow.getFieldCount() + rightJoinIndex, empDeptJoinRelFields));

    splitJoinConditionHelper(
        joinCond,
        Collections.singletonList(leftJoinIndex),
        Collections.singletonList(rightJoinIndex),
        Collections.singletonList(false),
        relBuilder.literal(true));
  }

  /**
   * Tests {@link RelOptUtil#splitJoinCondition(RelNode, RelNode, RexNode, List, List, List)}
   * where the join condition contains an expanded version of IS NOT DISTINCT.
   */
  @Test void testSplitJoinConditionExpandedIsNotDistinctFrom() {
    int leftJoinIndex = empScan.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = deptRow.getFieldNames().indexOf("DEPTNO");

    RexInputRef leftKeyInputRef = RexInputRef.of(leftJoinIndex, empDeptJoinRelFields);
    RexInputRef rightKeyInputRef =
        RexInputRef.of(empRow.getFieldCount() + rightJoinIndex, empDeptJoinRelFields);
    RexNode joinCond = relBuilder.or(
        relBuilder.equals(leftKeyInputRef, rightKeyInputRef),
        relBuilder.call(SqlStdOperatorTable.AND,
            relBuilder.isNull(leftKeyInputRef),
            relBuilder.isNull(rightKeyInputRef)));

    splitJoinConditionHelper(
        joinCond,
        Collections.singletonList(leftJoinIndex),
        Collections.singletonList(rightJoinIndex),
        Collections.singletonList(false),
        relBuilder.literal(true));
  }

  /**
   * Tests {@link RelOptUtil#splitJoinCondition(RelNode, RelNode, RexNode, List, List, List)}
   * where the join condition contains an expanded version of IS NOT DISTINCT
   * using CASE.
   */
  @Test void testSplitJoinConditionExpandedIsNotDistinctFromUsingCase() {
    int leftJoinIndex = empScan.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = deptRow.getFieldNames().indexOf("DEPTNO");

    RexInputRef leftKeyInputRef = RexInputRef.of(leftJoinIndex, empDeptJoinRelFields);
    RexInputRef rightKeyInputRef =
        RexInputRef.of(empRow.getFieldCount() + rightJoinIndex, empDeptJoinRelFields);
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
   * Tests {@link RelOptUtil#splitJoinCondition(RelNode, RelNode, RexNode, List, List, List)}
   * where the join condition contains an expanded version of IS NOT DISTINCT
   * using CASE.
   */
  @Test void testSplitJoinConditionExpandedIsNotDistinctFromUsingCase2() {
    int leftJoinIndex = empScan.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = deptRow.getFieldNames().indexOf("DEPTNO");

    RexInputRef leftKeyInputRef = RexInputRef.of(leftJoinIndex, empDeptJoinRelFields);
    RexInputRef rightKeyInputRef =
        RexInputRef.of(empRow.getFieldCount() + rightJoinIndex, empDeptJoinRelFields);
    RexNode joinCond = relBuilder.call(SqlStdOperatorTable.CASE,
        relBuilder.isNull(leftKeyInputRef),
        relBuilder.isNull(rightKeyInputRef),
        relBuilder.isNull(rightKeyInputRef),
        relBuilder.isNull(leftKeyInputRef),
        relBuilder.equals(leftKeyInputRef, rightKeyInputRef));

    splitJoinConditionHelper(
        joinCond,
        Collections.singletonList(leftJoinIndex),
        Collections.singletonList(rightJoinIndex),
        Collections.singletonList(false),
        relBuilder.literal(true));
  }

  private void splitJoinConditionHelper(RexNode joinCond, List<Integer> expLeftKeys,
      List<Integer> expRightKeys, List<Boolean> expFilterNulls, RexNode expRemaining) {
    List<Integer> actLeftKeys = new ArrayList<>();
    List<Integer> actRightKeys = new ArrayList<>();
    List<Boolean> actFilterNulls = new ArrayList<>();

    RexNode actRemaining = RelOptUtil.splitJoinCondition(empScan, deptScan, joinCond, actLeftKeys,
        actRightKeys, actFilterNulls);

    assertEquals(expRemaining, actRemaining);
    assertEquals(expFilterNulls, actFilterNulls);
    assertEquals(expLeftKeys, actLeftKeys);
    assertEquals(expRightKeys, actRightKeys);
  }

  /**
   * Tests {@link RelOptUtil#pushDownJoinConditions(org.apache.calcite.rel.core.Join, RelBuilder)}
   * where the join condition contains a complex expression.
   */
  @Test void testPushDownJoinConditions() {
    int leftJoinIndex = empScan.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = deptRow.getFieldNames().indexOf("DEPTNO");

    RexInputRef leftKeyInputRef = RexInputRef.of(leftJoinIndex, empDeptJoinRelFields);
    RexInputRef rightKeyInputRef =
        RexInputRef.of(empRow.getFieldCount() + rightJoinIndex, empDeptJoinRelFields);
    RexNode joinCond = relBuilder.equals(
        relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef,
            relBuilder.literal(1)),
        rightKeyInputRef);


    // Build the join operator and push down join conditions
    relBuilder.push(empScan);
    relBuilder.push(deptScan);
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
                RexInputRef.of(empRow.getFieldCount(), join.getRowType()),
                // Right side is shifted by 1
                RexInputRef.of(empRow.getFieldCount() + 1 + rightJoinIndex, join.getRowType()))
            .toString()));
    assertThat(newJoin.getLeft(), is(instanceOf(Project.class)));
    Project leftInput = (Project) newJoin.getLeft();
    assertThat(leftInput.getProjects().get(empRow.getFieldCount()).toString(),
        is(relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef, relBuilder.literal(1))
            .toString()));
  }

  /**
   * Tests {@link RelOptUtil#pushDownJoinConditions(org.apache.calcite.rel.core.Join, RelBuilder)}
   * where the join condition contains a complex expression.
   */
  @Test void testPushDownJoinConditionsWithIsNotDistinct() {
    int leftJoinIndex = empScan.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = deptRow.getFieldNames().indexOf("DEPTNO");

    RexInputRef leftKeyInputRef = RexInputRef.of(leftJoinIndex, empDeptJoinRelFields);
    RexInputRef rightKeyInputRef =
        RexInputRef.of(empRow.getFieldCount() + rightJoinIndex, empDeptJoinRelFields);
    RexNode joinCond = relBuilder.call(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
        relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef, relBuilder.literal(1)),
        rightKeyInputRef);


    // Build the join operator and push down join conditions
    relBuilder.push(empScan);
    relBuilder.push(deptScan);
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
                RexInputRef.of(empRow.getFieldCount(), join.getRowType()),
                // Right side is shifted by 1
                RexInputRef.of(empRow.getFieldCount() + 1 + rightJoinIndex, join.getRowType()))
            .toString()));
    assertThat(newJoin.getLeft(), is(instanceOf(Project.class)));
    Project leftInput = (Project) newJoin.getLeft();
    assertThat(leftInput.getProjects().get(empRow.getFieldCount()).toString(),
        is(relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef, relBuilder.literal(1))
            .toString()));
  }

  /**
   * Tests {@link RelOptUtil#pushDownJoinConditions(org.apache.calcite.rel.core.Join, RelBuilder)}
   * where the join condition contains a complex expression.
   */
  @Test void testPushDownJoinConditionsWithExpandedIsNotDistinct() {
    int leftJoinIndex = empScan.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = deptRow.getFieldNames().indexOf("DEPTNO");

    RexInputRef leftKeyInputRef = RexInputRef.of(leftJoinIndex, empDeptJoinRelFields);
    RexInputRef rightKeyInputRef =
        RexInputRef.of(empRow.getFieldCount() + rightJoinIndex, empDeptJoinRelFields);
    RexNode joinCond = relBuilder.or(
        relBuilder.equals(
            relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef, relBuilder.literal(1)),
            rightKeyInputRef),
        relBuilder.call(SqlStdOperatorTable.AND,
            relBuilder.isNull(
                relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef,
                    relBuilder.literal(1))),
            relBuilder.isNull(rightKeyInputRef)));


    // Build the join operator and push down join conditions
    relBuilder.push(empScan);
    relBuilder.push(deptScan);
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
                RexInputRef.of(empRow.getFieldCount(), join.getRowType()),
                // Right side is shifted by 1
                RexInputRef.of(empRow.getFieldCount() + 1 + rightJoinIndex, join.getRowType()))
                .toString()));
    assertThat(newJoin.getLeft(), is(instanceOf(Project.class)));
    Project leftInput = (Project) newJoin.getLeft();
    assertThat(leftInput.getProjects().get(empRow.getFieldCount()).toString(),
        is(relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef, relBuilder.literal(1))
            .toString()));
  }

  /**
   * Tests {@link RelOptUtil#pushDownJoinConditions(org.apache.calcite.rel.core.Join, RelBuilder)}
   * where the join condition contains a complex expression.
   */
  @Test void testPushDownJoinConditionsWithExpandedIsNotDistinctUsingCase() {
    int leftJoinIndex = empScan.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = deptRow.getFieldNames().indexOf("DEPTNO");

    RexInputRef leftKeyInputRef = RexInputRef.of(leftJoinIndex, empDeptJoinRelFields);
    RexInputRef rightKeyInputRef =
        RexInputRef.of(empRow.getFieldCount() + rightJoinIndex, empDeptJoinRelFields);
    RexNode joinCond = relBuilder.call(SqlStdOperatorTable.CASE,
        relBuilder.isNull(
            relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef,
                relBuilder.literal(1))),
        relBuilder.isNull(rightKeyInputRef),
        relBuilder.isNull(rightKeyInputRef),
        relBuilder.isNull(
            relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef,
                relBuilder.literal(1))),
        relBuilder.equals(
            relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef,
                relBuilder.literal(1)),
            rightKeyInputRef));


    // Build the join operator and push down join conditions
    relBuilder.push(empScan);
    relBuilder.push(deptScan);
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
                RexInputRef.of(empRow.getFieldCount(), join.getRowType()),
                // Right side is shifted by 1
                RexInputRef.of(empRow.getFieldCount() + 1 + rightJoinIndex, join.getRowType()))
              .toString()));
    assertThat(newJoin.getLeft(), is(instanceOf(Project.class)));
    Project leftInput = (Project) newJoin.getLeft();
    assertThat(leftInput.getProjects().get(empRow.getFieldCount()).toString(),
        is(relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef, relBuilder.literal(1))
            .toString()));
  }

  /**
   * Test {@link RelOptUtil#createCastRel(RelNode, RelDataType, boolean)}
   * with changed field nullability or field name.
   */
  @Test void testCreateCastRel() {
    // Equivalent SQL:
    // select empno, ename, count(job)
    // from emp
    // group by empno, ename

    // Row type:
    // RecordType(SMALLINT NOT NULL EMPNO, VARCHAR(10) ENAME, BIGINT NOT NULL $f2) NOT NULL
    final RelNode agg = relBuilder
        .push(empScan)
        .aggregate(
            relBuilder.groupKey("EMPNO", "ENAME"),
            relBuilder.count(relBuilder.field("JOB")))
        .build();
    // Cast with row type(change nullability):
    // RecordType(SMALLINT EMPNO, VARCHAR(10) ENAME, BIGINT $f2) NOT NULL
    // The fields.
    final RelDataTypeField fieldEmpno = agg.getRowType().getField("EMPNO", false, false);
    final RelDataTypeField fieldEname = agg.getRowType().getField("ENAME", false, false);
    final RelDataTypeField fieldJobCnt = Util.last(agg.getRowType().getFieldList());
    final RelDataTypeFactory typeFactory = relBuilder.getTypeFactory();
    // The field types.
    final RelDataType fieldTypeEmpnoNullable = typeFactory
        .createTypeWithNullability(fieldEmpno.getType(), true);
    final RelDataType fieldTypeJobCntNullable = typeFactory
        .createTypeWithNullability(fieldJobCnt.getType(), true);

    final RexBuilder rexBuilder = relBuilder.getRexBuilder();
    final RelDataType castRowType = typeFactory
        .createStructType(
            ImmutableList.of(
            Pair.of(fieldEmpno.getName(), fieldTypeEmpnoNullable),
            Pair.of(fieldEname.getName(), fieldEname.getType()),
            Pair.of(fieldJobCnt.getName(), fieldTypeJobCntNullable)));
    final RelNode castNode = RelOptUtil.createCastRel(agg, castRowType, false);
    final RelNode expectNode = relBuilder
        .push(agg)
        .project(
            rexBuilder.makeCast(
                fieldTypeEmpnoNullable,
                RexInputRef.of(0, agg.getRowType()),
                true),
            RexInputRef.of(1, agg.getRowType()),
            rexBuilder.makeCast(
                fieldTypeJobCntNullable,
                RexInputRef.of(2, agg.getRowType()),
                true))
        .build();
    assertThat(castNode.explain(), is(expectNode.explain()));

    // Cast with row type(change field name):
    // RecordType(SMALLINT NOT NULL EMPNO, VARCHAR(10) ENAME, BIGINT NOT NULL JOB_CNT) NOT NULL
    final RelDataType castRowType1 = typeFactory
        .createStructType(
            ImmutableList.of(
            Pair.of(fieldEmpno.getName(), fieldEmpno.getType()),
            Pair.of(fieldEname.getName(), fieldEname.getType()),
            Pair.of("JOB_CNT", fieldJobCnt.getType())));
    final RelNode castNode1 = RelOptUtil.createCastRel(agg, castRowType1, true);
    final RelNode expectNode1 = RelFactories
        .DEFAULT_PROJECT_FACTORY
        .createProject(
            agg,
            ImmutableList.of(),
            ImmutableList.of(
                RexInputRef.of(0, agg.getRowType()),
                RexInputRef.of(1, agg.getRowType()),
                RexInputRef.of(2, agg.getRowType())),
            ImmutableList.of(
                fieldEmpno.getName(),
                fieldEname.getName(),
                "JOB_CNT"),
            ImmutableSet.of());
    assertThat(castNode1.explain(), is(expectNode1.explain()));
    // Change the field JOB_CNT field name again.
    // The projection expect to be merged.
    final RelDataType castRowType2 = typeFactory
        .createStructType(
            ImmutableList.of(
            Pair.of(fieldEmpno.getName(), fieldEmpno.getType()),
            Pair.of(fieldEname.getName(), fieldEname.getType()),
            Pair.of("JOB_CNT2", fieldJobCnt.getType())));
    final RelNode castNode2 = RelOptUtil.createCastRel(agg, castRowType2, true);
    final RelNode expectNode2 = RelFactories
        .DEFAULT_PROJECT_FACTORY
        .createProject(
            agg,
            ImmutableList.of(),
            ImmutableList.of(
                RexInputRef.of(0, agg.getRowType()),
                RexInputRef.of(1, agg.getRowType()),
                RexInputRef.of(2, agg.getRowType())),
            ImmutableList.of(
                fieldEmpno.getName(),
                fieldEname.getName(),
                "JOB_CNT2"),
            ImmutableSet.of());
    assertThat(castNode2.explain(), is(expectNode2.explain()));
  }

  /** Dummy sub-class of ConverterRule, to check whether generated descriptions
   * are OK. */
  private static class MyConverterRule extends ConverterRule {
    static MyConverterRule create(RelTrait in, RelTrait out) {
      return Config.INSTANCE.withConversion(RelNode.class, in, out, null)
          .withRuleFactory(MyConverterRule::new)
          .toRule(MyConverterRule.class);
    }

    MyConverterRule(Config config) {
      super(config);
    }

    @Override public RelNode convert(RelNode rel) {
      throw new UnsupportedOperationException();
    }
  }
}
