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

import com.google.common.collect.ImmutableList;
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

  private RelBuilder relBuilder;

  private RelNode empScan;
  private RelNode deptScan;

  private RelDataType empRow;
  private RelDataType deptRow;

  private List<RelDataTypeField> empDeptJoinRelFields;

  //~ Constructors -----------------------------------------------------------

  public RelOptUtilTest() {
  }

  //~ Methods ----------------------------------------------------------------

  @Before public void setUp() {
    relBuilder = RelBuilder.create(config().build());

    empScan = relBuilder.scan("EMP").build();
    deptScan = relBuilder.scan("DEPT").build();

    empRow = empScan.getRowType();
    deptRow = deptScan.getRowType();

    empDeptJoinRelFields =
        Lists.newArrayList(Iterables.concat(empRow.getFieldList(), deptRow.getFieldList()));
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3136">[CALCITE-3136]
   * Fix the default rule description of ConverterRule</a>. */
  @Test public void testConvertRuleDefaultRuleDescription() {
    RelCollation collation1 =
            RelCollations.of(new RelFieldCollation(4, RelFieldCollation.Direction.DESCENDING));
    RelCollation collation2 =
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING));
    RelDistribution distribution1 = RelDistributions.hash(ImmutableList.of(0, 1));
    RelDistribution distribution2 =  RelDistributions.range(ImmutableList.of());
    RelOptRule collationConvertRule = new ConverterRule(RelNode.class,
            collation1,
            collation2,
            null) {
      @Override public RelNode convert(RelNode rel) {
        return null;
      }
    };
    RelOptRule distributionConvertRule = new ConverterRule(RelNode.class,
            distribution1,
            distribution2,
            null) {
      @Override public RelNode convert(RelNode rel) {
        return null;
      }
    };
    RelOptRule compositeConvertRule = new ConverterRule(RelNode.class,
            RelCompositeTrait.of(RelCollationTraitDef.INSTANCE,
                    ImmutableList.of(collation2, collation1)),
            RelCompositeTrait.of(RelCollationTraitDef.INSTANCE,
                    ImmutableList.of(collation1)),
            null) {
      @Override public RelNode convert(RelNode rel) {
        return null;
      }
    };
    RelOptRule compositeConvertRule0 = new ConverterRule(RelNode.class,
            RelCompositeTrait.of(RelDistributionTraitDef.INSTANCE,
                    ImmutableList.of(distribution1, distribution2)),
            RelCompositeTrait.of(RelDistributionTraitDef.INSTANCE,
                    ImmutableList.of(distribution1)),
            null) {
      @Override public RelNode convert(RelNode rel) {
        return null;
      }
    };
    assertEquals("ConverterRule(in:[4 DESC],out:[0 DESC])", collationConvertRule.toString());
    assertEquals("ConverterRule(in:hash[0, 1],out:range)", distributionConvertRule.toString());
    assertEquals("ConverterRule(in:[[0 DESC], [4 DESC]],out:[4 DESC])",
            compositeConvertRule.toString());
    assertEquals("ConverterRule(in:[hash[0, 1], range],out:hash[0, 1])",
            compositeConvertRule0.toString());
    try {
      Util.discard(
              new ConverterRule(RelNode.class,
              new Convention.Impl("{sourceConvention}", RelNode.class),
              new Convention.Impl("<targetConvention>", RelNode.class),
              null) {
          @Override public RelNode convert(RelNode rel) {
            return null;
          } });
      fail("expected exception");
    } catch (RuntimeException e) {
      assertEquals(
              "Rule description 'ConverterRule(in:{sourceConvention},out:<targetConvention>)' is not valid",
              e.getMessage());
    }
  }

  /**
   * Test {@link RelOptUtil#splitJoinCondition(RelNode, RelNode, RexNode, List, List, List)}
   * where the join condition contains just one which is a EQUAL operator.
   */
  @Test public void testSplitJoinConditionEquals() {
    int leftJoinIndex = empScan.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = deptRow.getFieldNames().indexOf("DEPTNO");

    RexNode joinCond = relBuilder.call(SqlStdOperatorTable.EQUALS,
        RexInputRef.of(leftJoinIndex, empDeptJoinRelFields),
        RexInputRef.of(empRow.getFieldCount() + rightJoinIndex, empDeptJoinRelFields));

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
    int leftJoinIndex = empScan.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = deptRow.getFieldNames().indexOf("DEPTNO");

    RexNode joinCond = relBuilder.call(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
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
   * Test {@link RelOptUtil#splitJoinCondition(RelNode, RelNode, RexNode, List, List, List)}
   * where the join condition contains an expanded version of IS NOT DISTINCT
   */
  @Test public void testSplitJoinConditionExpandedIsNotDistinctFrom() {
    int leftJoinIndex = empScan.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = deptRow.getFieldNames().indexOf("DEPTNO");

    RexInputRef leftKeyInputRef = RexInputRef.of(leftJoinIndex, empDeptJoinRelFields);
    RexInputRef rightKeyInputRef =
        RexInputRef.of(empRow.getFieldCount() + rightJoinIndex, empDeptJoinRelFields);
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
   * Test {@link RelOptUtil#splitJoinCondition(RelNode, RelNode, RexNode, List, List, List)}
   * where the join condition contains an expanded version of IS NOT DISTINCT using CASE
   */
  @Test public void testSplitJoinConditionExpandedIsNotDistinctFromUsingCase2() {
    int leftJoinIndex = empScan.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = deptRow.getFieldNames().indexOf("DEPTNO");

    RexInputRef leftKeyInputRef = RexInputRef.of(leftJoinIndex, empDeptJoinRelFields);
    RexInputRef rightKeyInputRef =
        RexInputRef.of(empRow.getFieldCount() + rightJoinIndex, empDeptJoinRelFields);
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
   * Test {@link RelOptUtil#pushDownJoinConditions(org.apache.calcite.rel.core.Join, RelBuilder)}
   * where the join condition contains a complex expression
   */
  @Test public void testPushDownJoinConditions() {
    int leftJoinIndex = empScan.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = deptRow.getFieldNames().indexOf("DEPTNO");

    RexInputRef leftKeyInputRef = RexInputRef.of(leftJoinIndex, empDeptJoinRelFields);
    RexInputRef rightKeyInputRef =
        RexInputRef.of(empRow.getFieldCount() + rightJoinIndex, empDeptJoinRelFields);
    RexNode joinCond = relBuilder.call(SqlStdOperatorTable.EQUALS,
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
                SqlStdOperatorTable.EQUALS,
                // Computed field is added at the end (and index start at 0)
                RexInputRef.of(empRow.getFieldCount(), join.getRowType()),
                // Right side is shifted by 1
                RexInputRef.of(empRow.getFieldCount() + 1 + rightJoinIndex, join.getRowType()))
            .toString()));
    assertThat(newJoin.getLeft(), is(instanceOf(Project.class)));
    Project leftInput = (Project) newJoin.getLeft();
    assertThat(leftInput.getChildExps().get(empRow.getFieldCount()).toString(),
        is(relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef, relBuilder.literal(1))
            .toString()));
  }

  /**
   * Test {@link RelOptUtil#pushDownJoinConditions(org.apache.calcite.rel.core.Join, RelBuilder)}
   * where the join condition contains a complex expression
   */
  @Test public void testPushDownJoinConditionsWithIsNotDistinct() {
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
    assertThat(leftInput.getChildExps().get(empRow.getFieldCount()).toString(),
        is(relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef, relBuilder.literal(1))
            .toString()));

  }

  /**
   * Test {@link RelOptUtil#pushDownJoinConditions(org.apache.calcite.rel.core.Join, RelBuilder)}
   * where the join condition contains a complex expression
   */
  @Test public void testPushDownJoinConditionsWithExpandedIsNotDistinct() {
    int leftJoinIndex = empScan.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = deptRow.getFieldNames().indexOf("DEPTNO");

    RexInputRef leftKeyInputRef = RexInputRef.of(leftJoinIndex, empDeptJoinRelFields);
    RexInputRef rightKeyInputRef =
        RexInputRef.of(empRow.getFieldCount() + rightJoinIndex, empDeptJoinRelFields);
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
    assertThat(leftInput.getChildExps().get(empRow.getFieldCount()).toString(),
        is(relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef, relBuilder.literal(1))
            .toString()));
  }

  /**
   * Test {@link RelOptUtil#pushDownJoinConditions(org.apache.calcite.rel.core.Join, RelBuilder)}
   * where the join condition contains a complex expression
   */
  @Test public void testPushDownJoinConditionsWithExpandedIsNotDistinctUsingCase() {
    int leftJoinIndex = empScan.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = deptRow.getFieldNames().indexOf("DEPTNO");

    RexInputRef leftKeyInputRef = RexInputRef.of(leftJoinIndex, empDeptJoinRelFields);
    RexInputRef rightKeyInputRef =
        RexInputRef.of(empRow.getFieldCount() + rightJoinIndex, empDeptJoinRelFields);
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
    assertThat(leftInput.getChildExps().get(empRow.getFieldCount()).toString(),
        is(relBuilder.call(SqlStdOperatorTable.PLUS, leftKeyInputRef, relBuilder.literal(1))
            .toString()));
  }
}

// End RelOptUtilTest.java
