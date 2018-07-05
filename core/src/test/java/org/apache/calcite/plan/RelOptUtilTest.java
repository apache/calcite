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

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
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

  private static final RelBuilder REL_BUILDER = RelBuilder.create(config().build());
  private static final RelNode EMP_SCAN = REL_BUILDER.scan("EMP").build();
  private static final RelNode DEPT_SCAN = REL_BUILDER.scan("DEPT").build();

  private static final RelDataType EMP_ROW = EMP_SCAN.getRowType();
  private static final RelDataType DEPT_ROW = DEPT_SCAN.getRowType();

  private static final List<RelDataTypeField> EMP_DEPT_JOIN_REL_FIELDS =
      Lists.newArrayList(Iterables.concat(EMP_ROW.getFieldList(), DEPT_ROW.getFieldList()));

  //~ Constructors -----------------------------------------------------------

  public RelOptUtilTest() {
  }

  //~ Methods ----------------------------------------------------------------

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
            "f1 VARCHAR(10) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL"),
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
            "  f1 VARCHAR(10) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL) NOT NULL,",
            "f1 RECORD (",
            "  f0 DECIMAL(5, 2) NOT NULL,",
            "  f1 VARCHAR(10) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL) NOT NULL MULTISET NOT NULL"),
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

    RexNode joinCond = REL_BUILDER.call(SqlStdOperatorTable.EQUALS,
        RexInputRef.of(leftJoinIndex, EMP_DEPT_JOIN_REL_FIELDS),
        RexInputRef.of(EMP_ROW.getFieldCount() + rightJoinIndex, EMP_DEPT_JOIN_REL_FIELDS));

    splitJoinConditionHelper(
        joinCond,
        Collections.singletonList(leftJoinIndex),
        Collections.singletonList(rightJoinIndex),
        Collections.singletonList(true),
        REL_BUILDER.literal(true));
  }

  /**
   * Test {@link RelOptUtil#splitJoinCondition(RelNode, RelNode, RexNode, List, List, List)}
   * where the join condition contains just one which is a IS NOT DISTINCT operator.
   */
  @Test public void testSplitJoinConditionIsNotDistinctFrom() {
    int leftJoinIndex = EMP_SCAN.getRowType().getFieldNames().indexOf("DEPTNO");
    int rightJoinIndex = DEPT_ROW.getFieldNames().indexOf("DEPTNO");

    RexNode joinCond = REL_BUILDER.call(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
        RexInputRef.of(leftJoinIndex, EMP_DEPT_JOIN_REL_FIELDS),
        RexInputRef.of(EMP_ROW.getFieldCount() + rightJoinIndex, EMP_DEPT_JOIN_REL_FIELDS));

    splitJoinConditionHelper(
        joinCond,
        Collections.singletonList(leftJoinIndex),
        Collections.singletonList(rightJoinIndex),
        Collections.singletonList(false),
        REL_BUILDER.literal(true));
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
    RexNode joinCond = REL_BUILDER.call(SqlStdOperatorTable.OR,
        REL_BUILDER.call(SqlStdOperatorTable.EQUALS, leftKeyInputRef, rightKeyInputRef),
        REL_BUILDER.call(SqlStdOperatorTable.AND,
            REL_BUILDER.call(SqlStdOperatorTable.IS_NULL, leftKeyInputRef),
            REL_BUILDER.call(SqlStdOperatorTable.IS_NULL, rightKeyInputRef)));

    splitJoinConditionHelper(
        joinCond,
        Collections.singletonList(leftJoinIndex),
        Collections.singletonList(rightJoinIndex),
        Collections.singletonList(false),
        REL_BUILDER.literal(true));
  }

  private static void splitJoinConditionHelper(RexNode joinCond, List<Integer> expLeftKeys,
      List<Integer> expRightKeys, List<Boolean> expFilterNulls, RexNode expRemaining) {
    List<Integer> actLeftKeys = new ArrayList<>();
    List<Integer> actRightKeys = new ArrayList<>();
    List<Boolean> actFilterNulls = new ArrayList<>();

    RexNode actRemaining = RelOptUtil.splitJoinCondition(EMP_SCAN, DEPT_SCAN, joinCond, actLeftKeys,
        actRightKeys, actFilterNulls);

    assertEquals(expRemaining.toString(), actRemaining.toString());
    assertEquals(expFilterNulls, actFilterNulls);
    assertEquals(expLeftKeys, actLeftKeys);
    assertEquals(expRightKeys, actRightKeys);
  }
}

// End RelOptUtilTest.java
