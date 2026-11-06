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
import org.apache.calcite.rel.core.ConditionalCorrelate;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalConditionalCorrelate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.RexRewritingRelShuttle;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.SmallStack;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.calcite.test.Matchers.isListOf;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
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
    assertThat(RelOptRule.guessDescription("com.foo.Bar"), is("Bar"));
    assertThat(RelOptRule.guessDescription("com.flatten.Bar$Baz"), is("Baz"));

    // yields "1" (which as an integer is an invalid
    try {
      Util.discard(RelOptRule.guessDescription("com.foo.Bar$1"));
      fail("expected exception");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(),
          is("Derived description of rule class com.foo.Bar$1 is an "
              + "integer, not valid. Supply a description manually."));
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
    assertThat(collationConvertRule,
        hasToString("ConverterRule(in:[4 DESC],out:[0 DESC])"));
    assertThat(distributionConvertRule,
        hasToString("ConverterRule(in:hash[0, 1],out:range)"));
    assertThat(compositeConvertRule,
        hasToString("ConverterRule(in:[[0 DESC], [4 DESC]],out:[4 DESC])"));
    assertThat(compositeConvertRule0,
        hasToString("ConverterRule(in:[hash[0, 1], range],out:hash[0, 1])"));
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

    RexNode joinCond =
        relBuilder.equals(RexInputRef.of(leftJoinIndex, empDeptJoinRelFields),
            RexInputRef.of(empRow.getFieldCount() + rightJoinIndex,
                empDeptJoinRelFields));

    splitJoinConditionHelper(
        joinCond,
        Collections.singletonList(leftJoinIndex),
        Collections.singletonList(rightJoinIndex),
        Collections.singletonList(true),
        relBuilder.literal(true));
  }

  @Test void testSplitJoinConditionWithoutEqualCondition() {
    final List<RelDataTypeField> sysFieldList = Collections.emptyList();
    final List<List<RexNode>> joinKeys =
        Arrays.asList(new ArrayList<>(), new ArrayList<>());
    final RexNode joinCondition =
        relBuilder.equals(RexInputRef.of(0, empDeptJoinRelFields),
            relBuilder.literal(1));
    final RexNode result =
        RelOptUtil.splitJoinCondition(sysFieldList,
            Arrays.asList(empScan, deptScan), joinCondition, joinKeys, null,
            null);
    assertThat(joinKeys,
        isListOf(Collections.emptyList(), Collections.emptyList()));
    assertThat(result, is(joinCondition));
  }

  /**
   * Test {@link RelOptUtil#splitJoinCondition(RelNode, RelNode, RexNode, List, List, List)}
   * where the join condition contains just one which is a IS NOT DISTINCT operator.
   */
  @Test void testSplitJoinConditionIsNotDistinctFrom() {
    int leftJoinIndex = empScan.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = deptRow.getFieldNames().indexOf("DEPTNO");

    RexNode joinCond =
        relBuilder.isNotDistinctFrom(
            RexInputRef.of(leftJoinIndex, empDeptJoinRelFields),
            RexInputRef.of(empRow.getFieldCount() + rightJoinIndex,
                empDeptJoinRelFields));

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
    RexNode joinCond =
        relBuilder.or(relBuilder.equals(leftKeyInputRef, rightKeyInputRef),
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
    RexNode joinCond =
        RelOptUtil.isDistinctFrom(relBuilder.getRexBuilder(),
            leftKeyInputRef, rightKeyInputRef, true);

    splitJoinConditionHelper(joinCond,
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
    RexNode joinCond =
        relBuilder.call(SqlStdOperatorTable.CASE,
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

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7013">[CALCITE-7013]
   * Support building RexLiterals from Character values</a>. */
  @Test void testCharacterLiteral() {
    char c = 'c';
    relBuilder.literal(c);
  }

  private void splitJoinConditionHelper(RexNode joinCond, List<Integer> expLeftKeys,
      List<Integer> expRightKeys, List<Boolean> expFilterNulls, RexNode expRemaining) {
    List<Integer> actLeftKeys = new ArrayList<>();
    List<Integer> actRightKeys = new ArrayList<>();
    List<Boolean> actFilterNulls = new ArrayList<>();

    RexNode actRemaining =
        RelOptUtil.splitJoinCondition(empScan, deptScan, joinCond, actLeftKeys,
            actRightKeys, actFilterNulls);

    assertThat(actRemaining, is(expRemaining));
    assertThat(actFilterNulls, is(expFilterNulls));
    assertThat(actLeftKeys, is(expLeftKeys));
    assertThat(actRightKeys, is(expRightKeys));
  }

  /**
   * Test that {@link RelOptUtil#collapseExpandedIsNotDistinctFromExpr(RexCall, RexBuilder)}
   * collapses an expanded version of IS NOT DISTINCT using OR.
   */
  @Test void testCollapseExpandedIsNotDistinctFromUsingOr() {
    final RexBuilder rexBuilder = relBuilder.getRexBuilder();

    final RexNode leftEmpNo =
        RexInputRef.of(empScan.getRowType().getFieldNames().indexOf("EMPNO"),
            empDeptJoinRelFields);
    final RexNode rightEmpNo =
        RexInputRef.of(empRow.getFieldCount() + deptRow.getFieldNames().indexOf("EMPNO"),
            empDeptJoinRelFields);

    // OR(AND(IS NULL($0), IS NULL($7)), IS TRUE(=($0, $7)))
    RexNode expanded = relBuilder.isNotDistinctFrom(leftEmpNo, rightEmpNo);

    // IS NOT DISTINCT FROM($0, $7)
    RexNode collapsed =
        RelOptUtil.collapseExpandedIsNotDistinctFromExpr((RexCall) expanded, rexBuilder);

    RexNode expected =
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, leftEmpNo, rightEmpNo);

    assertThat(collapsed, is(expected));
  }

  /**
   * Test that {@link RelOptUtil#collapseExpandedIsNotDistinctFromExpr(RexCall, RexBuilder)}
   * collapses an expanded version of IS NOT DISTINCT using CASE.
   */
  @Test void testCollapseExpandedIsNotDistinctFromUsingCase() {
    final RexBuilder rexBuilder = relBuilder.getRexBuilder();

    final RexNode leftEmpNo =
        RexInputRef.of(empScan.getRowType().getFieldNames().indexOf("EMPNO"),
            empDeptJoinRelFields);
    final RexNode rightEmpNo =
        RexInputRef.of(empRow.getFieldCount() + deptRow.getFieldNames().indexOf("EMPNO"),
            empDeptJoinRelFields);

    // CASE(IS NULL($0), IS NULL($7), IS NULL($7), IS NULL($0), =($0, $7))
    RexNode expanded =
        relBuilder.call(SqlStdOperatorTable.CASE, relBuilder.isNull(leftEmpNo),
            relBuilder.isNull(rightEmpNo),
            relBuilder.isNull(rightEmpNo),
            relBuilder.isNull(leftEmpNo),
            relBuilder.equals(leftEmpNo, rightEmpNo));

    // IS NOT DISTINCT FROM($0, $7)
    RexNode collapsed =
        RelOptUtil.collapseExpandedIsNotDistinctFromExpr((RexCall) expanded, rexBuilder);

    RexNode expected =
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, leftEmpNo, rightEmpNo);

    assertThat(collapsed, is(expected));
  }

  /**
   * Test that {@link RelOptUtil#collapseExpandedIsNotDistinctFromExpr(RexCall, RexBuilder)}
   * collapses an expression with expanded versions of IS NOT DISTINCT using OR and CASE.
   */
  @Test void testCollapseExpandedIsNotDistinctFromUsingOrAndCase() {
    final RexBuilder rexBuilder = relBuilder.getRexBuilder();

    final RexNode leftEmpNo =
        RexInputRef.of(empScan.getRowType().getFieldNames().indexOf("EMPNO"),
            empDeptJoinRelFields);
    final RexNode rightEmpNo =
        RexInputRef.of(empRow.getFieldCount() + deptRow.getFieldNames().indexOf("EMPNO"),
            empDeptJoinRelFields);

    final RexNode leftDeptNo =
        RexInputRef.of(empScan.getRowType().getFieldNames().indexOf("DEPTNO"),
            empDeptJoinRelFields);
    final RexNode rightDeptNo =
        RexInputRef.of(empRow.getFieldCount() + deptRow.getFieldNames().indexOf("DEPTNO"),
            empDeptJoinRelFields);

    // An IS NOT DISTINCT FROM expanded in "CASE" shape
    // CASE(IS NULL($0), IS NULL($7), IS NULL($7), IS NULL($0), =($0, $7))
    RexNode expandedCase =
        relBuilder.call(SqlStdOperatorTable.CASE, relBuilder.isNull(leftEmpNo),
            relBuilder.isNull(rightEmpNo),
            relBuilder.isNull(rightEmpNo),
            relBuilder.isNull(leftEmpNo),
            relBuilder.equals(leftEmpNo, rightEmpNo));

    // An IS NOT DISTINCT FROM expanded in "OR" shape
    // OR(AND(IS NULL($7), IS NULL($8)), =($7, $8))
    RexNode expandedOr =
        relBuilder.call(
            SqlStdOperatorTable.OR, relBuilder.call(SqlStdOperatorTable.AND,
                relBuilder.isNull(leftDeptNo),
                relBuilder.isNull(rightDeptNo)),
            relBuilder.call(SqlStdOperatorTable.EQUALS, leftDeptNo, rightDeptNo));

    // AND(
    //  OR(AND(IS NULL($7), IS NULL($8)), =($7, $8)),
    //  CASE(IS NULL($0), IS NULL($7), IS NULL($7), IS NULL($0), =($0, $7))
    // )
    RexNode expanded = relBuilder.and(expandedOr, expandedCase);

    // AND(IS NOT DISTINCT FROM($7, $8), IS NOT DISTINCT FROM($0, $7))
    RexNode collapsed =
        RelOptUtil.collapseExpandedIsNotDistinctFromExpr((RexCall) expanded, rexBuilder);

    RexNode expected =
        rexBuilder.makeCall(
            // Expected is nullable because `expandedCase` is nullable
            relBuilder.getTypeFactory().createTypeWithNullability(expanded.getType(), true),
            SqlStdOperatorTable.AND,
            ImmutableList.of(
                rexBuilder.makeCall(
                    SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                    leftEmpNo,
                    rightEmpNo),
                rexBuilder.makeCall(
                    SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                    leftDeptNo,
                    rightDeptNo)));

    assertThat(collapsed, is(expected));
  }

  /**
   * Test that {@link RelOptUtil#collapseExpandedIsNotDistinctFromExpr(RexCall, RexBuilder)}
   * recursively collapses expanded versions of IS NOT DISTINCT.
   */
  @Test void testCollapseExpandedIsNotDistinctFromRecursively() {
    final RexBuilder rexBuilder = relBuilder.getRexBuilder();

    final RexNode leftEmpNo =
        RexInputRef.of(empScan.getRowType().getFieldNames().indexOf("EMPNO"),
            empDeptJoinRelFields);
    final RexNode rightEmpNo =
        RexInputRef.of(empRow.getFieldCount() + deptRow.getFieldNames().indexOf("EMPNO"),
            empDeptJoinRelFields);

    // OR(
    //  AND(
    //   IS NULL(OR(AND(IS NULL($0), IS NULL($7)), IS TRUE(=($0, $7)))),
    //   IS NULL(OR(AND(IS NULL($0), IS NULL($7)), IS TRUE(=($0, $7))))),
    //   IS TRUE(=(
    //    OR(AND(IS NULL($0), IS NULL($7)), IS TRUE(=($0, $7))),
    //    OR(AND(IS NULL($0), IS NULL($7)), IS TRUE(=($0, $7)))
    //   )
    //  )
    // )
    RexNode expanded =
        relBuilder.isNotDistinctFrom(
            relBuilder.isNotDistinctFrom(leftEmpNo, rightEmpNo),
            relBuilder.isNotDistinctFrom(leftEmpNo, rightEmpNo));

    // IS NOT DISTINCT FROM(IS NOT DISTINCT FROM($0, $7), IS NOT DISTINCT FROM($0, $7))
    RexNode collapsed =
        RelOptUtil.collapseExpandedIsNotDistinctFromExpr((RexCall) expanded, rexBuilder);

    RexNode expected =
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
            rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                leftEmpNo,
                rightEmpNo),

            rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                leftEmpNo,
                rightEmpNo));

    assertThat(collapsed, is(expected));
  }

  /**
   * Test that {@link RelOptUtil#collapseExpandedIsNotDistinctFromExpr(RexCall, RexBuilder)}
   * will collapse IS NOT DISTINCT FROM nested within RexNodes.
   */
  @Test void testCollapseExpandedIsNotDistinctFromInsideRexNode() {
    final RexBuilder rexBuilder = relBuilder.getRexBuilder();

    final RexNode leftEmpNo =
        RexInputRef.of(empScan.getRowType().getFieldNames().indexOf("EMPNO"),
            empDeptJoinRelFields);
    final RexNode rightEmpNo =
        RexInputRef.of(empRow.getFieldCount() + deptRow.getFieldNames().indexOf("EMPNO"),
            empDeptJoinRelFields);

    RexNode expandedIsNotDistinctFrom = relBuilder.isNotDistinctFrom(leftEmpNo, rightEmpNo);
    // NULLIF(
    //  NOT(OR(AND(IS NULL($0), IS NULL($7)), IS TRUE(=($0, $7)))),
    //  IS NOT NULL(OR(AND(IS NULL($0), IS NULL($7)), IS TRUE(=($0, $7))))
    // )
    RexNode expanded =
        relBuilder.call(SqlStdOperatorTable.NULLIF,
            relBuilder.not(expandedIsNotDistinctFrom),
            relBuilder.isNotNull(expandedIsNotDistinctFrom));

    // NULLIF(NOT(IS NOT DISTINCT FROM($0, $7)), IS NOT NULL(IS NOT DISTINCT FROM($0, $7)))
    RexNode collapsed =
        RelOptUtil.collapseExpandedIsNotDistinctFromExpr((RexCall) expanded, rexBuilder);

    RexNode collapsedIsNotDistinctFrom =
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, leftEmpNo, rightEmpNo);
    RexNode expected =
        rexBuilder.makeCall(
            SqlStdOperatorTable.NULLIF,
            relBuilder.not(collapsedIsNotDistinctFrom),
            relBuilder.isNotNull(collapsedIsNotDistinctFrom));

    assertThat(collapsed, is(expected));
  }

  /**
   * Test that {@link RelOptUtil#collapseExpandedIsNotDistinctFromExpr(RexCall, RexBuilder)}
   * can handle collapsing IS NOT DISTINCT FROM composed of other RexNodes.
   */
  @Test void testCollapseExpandedIsNotDistinctFromOnContainingRexNodes() {
    final RexBuilder rexBuilder = relBuilder.getRexBuilder();

    final RexNode leftEmpNo =
        RexInputRef.of(empScan.getRowType().getFieldNames().indexOf("EMPNO"),
            empDeptJoinRelFields);
    final RexNode rightEmpNo =
        RexInputRef.of(empRow.getFieldCount() + deptRow.getFieldNames().indexOf("EMPNO"),
            empDeptJoinRelFields);

    final RexNode leftDeptNo =
        RexInputRef.of(empScan.getRowType().getFieldNames().indexOf("DEPTNO"),
            empDeptJoinRelFields);
    final RexNode rightDeptNo =
        RexInputRef.of(empRow.getFieldCount() + deptRow.getFieldNames().indexOf("DEPTNO"),
            empDeptJoinRelFields);

    // OR(
    //  AND(IS NULL(NULLIF($0, $7)), IS NULL(NULLIF($7, $8))),
    //  IS TRUE(=(NULLIF($0, $7), NULLIF($7, $8)))
    // )
    RexNode expanded =
        relBuilder.isNotDistinctFrom(
            relBuilder.call(SqlStdOperatorTable.NULLIF, leftEmpNo, rightEmpNo),
            relBuilder.call(SqlStdOperatorTable.NULLIF, leftDeptNo, rightDeptNo));

    // IS NOT DISTINCT FROM(NULLIF($0, $7), NULLIF($7, $8))
    RexNode collapsed =
        RelOptUtil.collapseExpandedIsNotDistinctFromExpr((RexCall) expanded, rexBuilder);

    RexNode expected =
        rexBuilder.makeCall(
            SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
            rexBuilder.makeCall(SqlStdOperatorTable.NULLIF, leftEmpNo, rightEmpNo),
            rexBuilder.makeCall(SqlStdOperatorTable.NULLIF, leftDeptNo, rightDeptNo));

    assertThat(collapsed, is(expected));
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
    RexNode joinCond =
        relBuilder.equals(
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
    assertThat(newJoin.getCondition(),
        hasToString(
            relBuilder.call(
                SqlStdOperatorTable.EQUALS,
                // Computed field is added at the end (and index start at 0)
                RexInputRef.of(empRow.getFieldCount(), join.getRowType()),
                // Right side is shifted by 1
                RexInputRef.of(empRow.getFieldCount() + 1 + rightJoinIndex, join.getRowType()))
            .toString()));
    assertThat(newJoin.getLeft(), is(instanceOf(Project.class)));
    Project leftInput = (Project) newJoin.getLeft();
    assertThat(leftInput.getProjects().get(empRow.getFieldCount()),
        hasToString(
            relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef,
                    relBuilder.literal(1)).toString()));
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
    RexNode joinCond =
        relBuilder.call(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
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
    assertThat(newJoin.getCondition(),
        hasToString(
            relBuilder.call(
                SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                // Computed field is added at the end (and index start at 0)
                RexInputRef.of(empRow.getFieldCount(), join.getRowType()),
                // Right side is shifted by 1
                RexInputRef.of(empRow.getFieldCount() + 1 + rightJoinIndex, join.getRowType()))
            .toString()));
    assertThat(newJoin.getLeft(), is(instanceOf(Project.class)));
    Project leftInput = (Project) newJoin.getLeft();
    assertThat(leftInput.getProjects().get(empRow.getFieldCount()),
        hasToString(
            relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef,
                    relBuilder.literal(1)).toString()));
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
    RexNode joinCond =
        relBuilder.or(
            relBuilder.equals(
                relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef,
                    relBuilder.literal(1)),
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
    assertThat(newJoin.getCondition(),
        hasToString(
            relBuilder.call(
                SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                // Computed field is added at the end (and index start at 0)
                RexInputRef.of(empRow.getFieldCount(), join.getRowType()),
                // Right side is shifted by 1
                RexInputRef.of(empRow.getFieldCount() + 1 + rightJoinIndex, join.getRowType()))
                .toString()));
    assertThat(newJoin.getLeft(), is(instanceOf(Project.class)));
    Project leftInput = (Project) newJoin.getLeft();
    assertThat(leftInput.getProjects().get(empRow.getFieldCount()),
        hasToString(
            relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef,
                    relBuilder.literal(1)).toString()));
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
    RexNode joinCond =
        relBuilder.call(SqlStdOperatorTable.CASE,
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
    assertThat(newJoin.getCondition(),
        hasToString(
            relBuilder.call(
                SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                // Computed field is added at the end (and index start at 0)
                RexInputRef.of(empRow.getFieldCount(), join.getRowType()),
                // Right side is shifted by 1
                RexInputRef.of(empRow.getFieldCount() + 1 + rightJoinIndex,
                    join.getRowType())).toString()));
    assertThat(newJoin.getLeft(), is(instanceOf(Project.class)));
    Project leftInput = (Project) newJoin.getLeft();
    assertThat(leftInput.getProjects().get(empRow.getFieldCount()),
        hasToString(
            relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef,
                    relBuilder.literal(1)).toString()));
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
                true, false),
            RexInputRef.of(1, agg.getRowType()),
            rexBuilder.makeCast(
                fieldTypeJobCntNullable,
                RexInputRef.of(2, agg.getRowType()),
                true, false))
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

  @Test void testRemapCorrelHandlesNestedSubQueries() {
    // Equivalent SQL:
    // select
    //    (
    //        select count(*)
    //          from emp as middle_emp
    //         where exists (
    //                   select true
    //                     from emp as innermost_emp
    //                    where outermost_emp.deptno = innermost_emp.deptno
    //                      and middle_emp.sal < innermost_emp.sal
    //               )
    //    ) as c
    // from emp as outermost_emp

    int deptNoIdx = empRow.getFieldNames().indexOf("DEPTNO");
    int salIdx = empRow.getFieldNames().indexOf("SAL");

    RelOptCluster cluster = relBuilder.getCluster();
    RexBuilder rexBuilder = relBuilder.getRexBuilder();

    CorrelationId outermostCorrelationId = cluster.createCorrel();
    RexNode outermostCorrelate =
        rexBuilder.makeFieldAccess(
            rexBuilder.makeCorrel(empRow, outermostCorrelationId), deptNoIdx);
    CorrelationId middleCorrelationId = cluster.createCorrel();
    RexNode middleCorrelate =
        rexBuilder.makeFieldAccess(rexBuilder.makeCorrel(empRow, middleCorrelationId), salIdx);

    RelNode innermostQuery = relBuilder
        .push(empScan)
        .filter(
            rexBuilder.makeCall(
                SqlStdOperatorTable.AND,
                rexBuilder.makeCall(
                    SqlStdOperatorTable.EQUALS,
                    outermostCorrelate,
                    rexBuilder.makeInputRef(empRow, deptNoIdx)),
                rexBuilder.makeCall(
                    SqlStdOperatorTable.LESS_THAN,
                    middleCorrelate,
                    rexBuilder.makeInputRef(empRow, salIdx)
                )
            )
        )
        .project(rexBuilder.makeLiteral(true))
        .build();

    RelNode middleQuery = relBuilder
        .push(empScan)
        .filter(relBuilder.exists(ignored -> innermostQuery))
        .aggregate(
            relBuilder.groupKey(),
            relBuilder.countStar("COUNT_ALL")
        )
        .build();

    RelNode outermostQuery = relBuilder
        .push(empScan)
        .project(relBuilder.scalarQuery(ignored -> middleQuery))
        .build();

    // Wrap the outermost query in RexSubQuery since RelOptUtil.remapCorrelatesInSuqQuery
    // accepts RexSubQuery as input.
    RexSubQuery subQuery = relBuilder.exists(ignored -> outermostQuery);

    RelDataType newType = cluster.getTypeFactory().builder()
            .add(empRow.getFieldList().get(deptNoIdx))
            .build();

    Mapping mapping =
            Mappings.create(MappingType.INVERSE_SURJECTION,
            empRow.getFieldCount(),
            1);
    mapping.set(deptNoIdx, 0);

    RexSubQuery newSubQuery =
        RelOptUtil.remapCorrelatesInSuqQuery(
            rexBuilder, subQuery, outermostCorrelationId, newType, mapping);

    List<RexFieldAccess> variablesUsed = new ArrayList<>();

    newSubQuery.accept(new RexShuttle() {
      @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
        if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
          variablesUsed.add(fieldAccess);
        }

        return super.visitFieldAccess(fieldAccess);
      }

      @Override public RexNode visitSubQuery(RexSubQuery subQuery) {
        subQuery.rel.accept(new RexRewritingRelShuttle(this));

        return super.visitSubQuery(subQuery);
      }
    });

    assertThat(variablesUsed, hasSize(2));

    variablesUsed.sort(
        Comparator.comparingInt(v ->
            ((RexCorrelVariable) v.getReferenceExpr()).id.getId()));

    RexFieldAccess firstFieldAccess = variablesUsed.get(0);
    assertThat(firstFieldAccess.getField().getIndex(), is(0));

    RexCorrelVariable firstVar = (RexCorrelVariable) firstFieldAccess.getReferenceExpr();
    assertThat(firstVar.id, is(outermostCorrelationId));
    assertThat(firstVar.getType(), is(newType));

    RexFieldAccess secondFieldAccess = variablesUsed.get(1);
    assertThat(secondFieldAccess.getField().getIndex(), is(salIdx));

    RexCorrelVariable secondVar = (RexCorrelVariable) secondFieldAccess.getReferenceExpr();
    assertThat(secondVar.id, is(middleCorrelationId));
    assertThat(secondVar.getType(), is(empRow));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7914">[CALCITE-7914]
   * RelDecorrelator produces wrong results for non-deterministic inputs</a>.
   *
   * <p>Sub-query plans and operands must both be checked for determinism.
   * Query-stable calls are allowed; a window aggregate declared non-deterministic
   * must be rejected even with constant operands. */
  @Test void testIsDeterministic() {
    final RelNode values = relBuilder.values(new String[] {"X"}, 1D).build();
    final RelNode stable = relBuilder.push(values)
        .project(relBuilder.call(SqlStdOperatorTable.CURRENT_TIMESTAMP)).build();
    final RelNode random = relBuilder.push(values)
        .project(relBuilder.call(SqlStdOperatorTable.RAND)).build();
    assertThat(RelOptUtil.containsSubQuery(stable), is(false));

    final RelNode stableSubQuery = relBuilder.push(values)
        .project(RexSubQuery.scalar(stable)).build();
    final RelNode randomSubQuery = relBuilder.push(values)
        .project(RexSubQuery.scalar(random)).build();
    assertThat(RelOptUtil.isDeterministic(stableSubQuery), is(true));
    assertThat(RelOptUtil.containsSubQuery(stableSubQuery), is(true));
    assertThat(RelOptUtil.isDeterministic(randomSubQuery), is(false));

    final RelNode randomOperand = relBuilder.push(values)
        .project(
            RexSubQuery.in(values,
            ImmutableList.of(relBuilder.call(SqlStdOperatorTable.RAND)))).build();
    assertThat(RelOptUtil.isDeterministic(randomOperand), is(false));

    final SqlSumAggFunction aggregate =
        new SqlSumAggFunction(relBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER)) {
          @Override public boolean isDeterministic() {
            return false;
          }
        };
    final RelNode window = relBuilder.push(values)
        .project(relBuilder.aggregateCall(aggregate, relBuilder.literal(1)).over().as("K"))
        .build();
    assertThat(RelOptUtil.isDeterministic(window), is(false));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7914">[CALCITE-7914]
   * RelDecorrelator produces wrong results for non-deterministic inputs</a>.
   *
   * <p>A correlation used only in a Match measure must still contribute its
   * field. A whole-row reference must contribute every field. */
  @Test void testCorrelationColumns() {
    final CorrelationId id = relBuilder.getCluster().createCorrel();
    final RexBuilder rexBuilder = relBuilder.getRexBuilder();
    final RexNode measure = rexBuilder.makeFieldAccess(rexBuilder.makeCorrel(empRow, id), 0);
    final RelDataType rowType = relBuilder.getTypeFactory().builder()
        .add("M", measure.getType()).build();
    final RelNode match =
        LogicalMatch.create(empScan, rowType, relBuilder.literal("A"),
            false, false, ImmutableMap.of("A", relBuilder.literal(true)),
            ImmutableMap.of("M", measure),
            rexBuilder.makeFlag(SqlMatchRecognize.AfterOption.SKIP_TO_NEXT_ROW),
            ImmutableMap.of(), false, ImmutableBitSet.of(), RelCollations.EMPTY, null);
    assertThat(RelOptUtil.getVariablesUsed(match), is(ImmutableSet.of(id)));
    assertThat(RelOptUtil.correlationColumns(id, match), is(ImmutableBitSet.of(0)));

    final RelNode wholeRow = relBuilder.push(empScan)
        .project(rexBuilder.makeCorrel(empRow, id)).build();
    assertThat(RelOptUtil.correlationColumns(id, wholeRow),
        is(ImmutableBitSet.range(empRow.getFieldCount())));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7914">[CALCITE-7914]
   * RelDecorrelator produces wrong results for non-deterministic inputs</a>.
   *
   * <p>The condition uses the outer binding for both matching and distinct ids.
   * Mapping its field 1 to 0 must visit the condition once and leave the nested
   * RHS's locally bound field 0 unchanged. */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testConditionalCorrelateReferences(boolean sameId) {
    final RexBuilder rexBuilder = relBuilder.getRexBuilder();
    final RelNode outer = relBuilder.values(new String[] {"A", "B"}, 10, 20).build();
    final CorrelationId id = relBuilder.getCluster().createCorrel();
    final CorrelationId innerId = sameId ? id : relBuilder.getCluster().createCorrel();
    final RexNode outerVariable = rexBuilder.makeCorrel(outer.getRowType(), id);
    final RelNode left = relBuilder.values(new String[] {"A", "B"}, 1, 2).build();
    final RexNode innerVariable = rexBuilder.makeCorrel(left.getRowType(), innerId);
    final RexNode local = rexBuilder.makeFieldAccess(innerVariable, 0);
    final RelNode right = relBuilder.values(new String[] {"V"}, 0)
        .project(relBuilder.alias(local, "Q")).build();
    final RexNode condition =
        relBuilder.equals(rexBuilder.makeInputRef(left, 0),
            rexBuilder.makeFieldAccess(outerVariable, 1));
    final ConditionalCorrelate correlate =
        LogicalConditionalCorrelate.create(left, right, ImmutableList.of(),
            innerId, ImmutableBitSet.of(0), JoinRelType.LEFT_MARK, condition);
    assertThat(RelOptUtil.correlationColumns(id, correlate), is(ImmutableBitSet.of(1)));

    final Mapping mapping = Mappings.create(MappingType.INVERSE_SURJECTION, 2, 1);
    mapping.set(1, 0);
    final RelNode newOuter = relBuilder.push(outer).project(relBuilder.field("B")).build();
    final ConditionalCorrelate result = (ConditionalCorrelate)
        new RelOptUtil.RexCorrelVariableMapShuttle(id, newOuter.getRowType(), mapping, rexBuilder)
            .remapCorrelationFields(correlate);
    assertThat(RelOptUtil.correlationColumns(id, result), is(ImmutableBitSet.of(0)));
    assertThat(((Project) result.getRight()).getProjects(), is(ImmutableList.of(local)));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7914">[CALCITE-7914]
   * RelDecorrelator produces wrong results for non-deterministic inputs</a>.
   *
   * <p>Remapping through RexSubQuery must honor a nested Correlate that reuses
   * the outer id. The outer B reference is renumbered; the nested RHS's local A
   * reference must keep its original field. */
  @Test void testRemapSubQueryCorrelationScope() {
    final RexBuilder rexBuilder = relBuilder.getRexBuilder();
    final RelNode outer = relBuilder.values(new String[] {"A", "B"}, 10, 20).build();
    final CorrelationId id = relBuilder.getCluster().createCorrel();
    final RexNode outerVariable = rexBuilder.makeCorrel(outer.getRowType(), id);
    final RelNode left = relBuilder.values(new String[] {"A", "B"}, 1, 2).build();
    final RexNode innerVariable = rexBuilder.makeCorrel(left.getRowType(), id);
    final RexNode local = rexBuilder.makeFieldAccess(innerVariable, 0);
    final RelNode right = relBuilder.values(new String[] {"V"}, 0)
        .project(relBuilder.alias(local, "Q")).build();
    final RelNode nested =
        LogicalCorrelate.create(left, right, ImmutableList.of(),
            id, ImmutableBitSet.of(0), JoinRelType.INNER);
    final RelNode filter =
        LogicalFilter.create(
            nested, relBuilder.equals(rexBuilder.makeInputRef(nested, 2),
            rexBuilder.makeFieldAccess(outerVariable, 1)));
    final RelNode scalar =
        LogicalProject.create(filter, ImmutableList.of(),
            ImmutableList.of(rexBuilder.makeInputRef(filter, 2)),
            ImmutableList.of("Q"), ImmutableSet.of());
    final Mapping mapping = Mappings.create(MappingType.INVERSE_SURJECTION, 2, 1);
    mapping.set(1, 0);
    final RelNode newOuter = relBuilder.push(outer).project(relBuilder.field("B")).build();
    final RexSubQuery result =
        RelOptUtil.remapCorrelatesInSuqQuery(rexBuilder, RexSubQuery.scalar(scalar),
            id, newOuter.getRowType(), mapping);
    assertThat(RelOptUtil.correlationColumns(id, result.rel), is(ImmutableBitSet.of(0)));
    final Filter newFilter = (Filter) result.rel.getInput(0);
    final Correlate newNested = (Correlate) newFilter.getInput();
    assertThat(((Project) newNested.getRight()).getProjects(), is(ImmutableList.of(local)));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5845">[CALCITE-5845]
   * StackOverflowError when SQL has a very large predicate with many ANDs</a>.
   *
   * <p>Verifies that {@link RelOptUtil#conjunctions} computes a deeply nested
   * binary AND tree on a small-stack thread; the previous recursive
   * implementation overflows. */
  @Test void testConjunctionsDeeplyNested() {
    final RexBuilder rexBuilder = relBuilder.getCluster().getRexBuilder();
    final RelDataType booleanType =
        relBuilder.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);
    final int depth = 20_000;
    // Build AND(AND(AND(..., $1), $2), $3, ...) as a left-deep binary tree,
    // iteratively.
    RexNode node = rexBuilder.makeInputRef(booleanType, 0);
    for (int i = 1; i < depth; i++) {
      node =
          rexBuilder.makeCall(SqlStdOperatorTable.AND, node,
              rexBuilder.makeInputRef(booleanType, i));
    }
    final RexNode deepTree = node;
    final List<RexNode> conjunctions =
        SmallStack.run(() -> RelOptUtil.conjunctions(deepTree));
    assertThat(conjunctions, hasSize(depth));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5845">[CALCITE-5845]
   * StackOverflowError when SQL has a very large predicate with many ANDs</a>.
   *
   * <p>Verifies that {@link RelOptUtil#decomposeConjunction(RexNode, List, List)}
   * computes a deeply nested binary AND tree on a small-stack thread; the
   * previous recursive implementation overflows. */
  @Test void testDecomposeConjunctionNotDeeplyNested() {
    final RexBuilder rexBuilder = relBuilder.getCluster().getRexBuilder();
    final RelDataType booleanType =
        relBuilder.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);
    final int depth = 20_000;
    RexNode node = rexBuilder.makeInputRef(booleanType, 0);
    for (int i = 1; i < depth; i++) {
      node =
          rexBuilder.makeCall(SqlStdOperatorTable.AND, node,
              rexBuilder.makeInputRef(booleanType, i));
    }
    final RexNode deepTree = node;
    final List<RexNode> rexList = new ArrayList<>();
    final List<RexNode> notList = new ArrayList<>();
    SmallStack.run(() -> {
      RelOptUtil.decomposeConjunction(deepTree, rexList, notList);
      return null;
    });
    assertThat(rexList, hasSize(depth));
    assertThat(notList, hasSize(0));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5845">[CALCITE-5845]
   * StackOverflowError when SQL has a very large predicate with many ANDs</a>.
   *
   * <p>Verifies that {@link RelOptUtil#disjunctions} computes a deeply nested
   * binary OR tree on a small-stack thread; the previous recursive
   * implementation overflows. */
  @Test void testDisjunctionsDeeplyNested() {
    final RexBuilder rexBuilder = relBuilder.getCluster().getRexBuilder();
    final RelDataType booleanType =
        relBuilder.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);
    final int depth = 20_000;
    // Build OR(OR(OR(..., $1), $2), $3, ...) as a left-deep binary tree,
    // iteratively.
    RexNode node = rexBuilder.makeInputRef(booleanType, 0);
    for (int i = 1; i < depth; i++) {
      node =
          rexBuilder.makeCall(SqlStdOperatorTable.OR, node,
              rexBuilder.makeInputRef(booleanType, i));
    }
    final RexNode deepTree = node;
    final List<RexNode> disjunctions =
        SmallStack.run(() -> RelOptUtil.disjunctions(deepTree));
    assertThat(disjunctions, hasSize(depth));
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
