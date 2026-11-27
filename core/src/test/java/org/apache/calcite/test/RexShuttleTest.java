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

import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link RexShuttle}.
 */
class RexShuttleTest {

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7809">[CALCITE-7809]
   * Avoid copying unchanged SQL and Rex operands</a>. */
  @Test void testVisitListReusesUnchangedImmutableList() {
    final RelDataType type = createIntegerType();
    final ImmutableList<RexNode> operands = createInputRefs(type, 3);
    final RexNode absent = new RexInputRef(3, type);
    final boolean[] update = {false};

    final List<RexNode> result =
        new ReplacingListShuttle(absent, absent)
            .visitListForTest(operands, update);

    assertSame(operands, result);
    assertFalse(update[0]);
  }

  @Test void testVisitListPreservesExistingUpdate() {
    final RelDataType type = createIntegerType();
    final ImmutableList<RexNode> operands = createInputRefs(type, 3);
    final RexNode absent = new RexInputRef(3, type);
    final boolean[] update = {true};

    final List<RexNode> result =
        new ReplacingListShuttle(absent, absent)
            .visitListForTest(operands, update);

    assertSame(operands, result);
    assertTrue(update[0]);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  void testVisitListCopiesOnFirstChange(int changedIndex) {
    final RelDataType type = createIntegerType();
    final ImmutableList<RexNode> operands = createInputRefs(type, 3);
    final RexNode replacement = new RexInputRef(3, type);
    final boolean[] update = {false};
    final List<RexNode> expected = new ArrayList<>(operands);
    expected.set(changedIndex, replacement);

    final List<RexNode> result =
        new ReplacingListShuttle(operands.get(changedIndex), replacement)
            .visitListForTest(operands, update);

    assertNotSame(operands, result);
    assertThat(result, is(expected));
    assertTrue(update[0]);
  }

  @Test void testVisitListCopiesOnMultipleChanges() {
    final RelDataType type = createIntegerType();
    final ImmutableList<RexNode> operands = createInputRefs(type, 3);
    final RexNode firstReplacement = new RexInputRef(3, type);
    final RexNode lastReplacement = new RexInputRef(4, type);
    final boolean[] update = {false};
    final List<RexNode> expected =
        ImmutableList.of(firstReplacement, operands.get(1), lastReplacement);

    final List<RexNode> result =
        new ReplacingListShuttle(
            operands.get(0), firstReplacement,
            operands.get(2), lastReplacement)
            .visitListForTest(operands, update);

    assertNotSame(operands, result);
    assertThat(result, is(expected));
    assertTrue(update[0]);
  }

  @Test void testVisitListCopiesUnchangedMutableInput() {
    final RelDataType type = createIntegerType();
    final List<RexNode> operands = new ArrayList<>(createInputRefs(type, 3));
    final RexNode absent = new RexInputRef(3, type);
    final boolean[] update = {false};

    final List<RexNode> result =
        new ReplacingListShuttle(absent, absent)
            .visitListForTest(operands, update);

    assertNotSame(operands, result);
    assertEquals(operands, result);
    assertInstanceOf(ImmutableList.class, result);
    assertFalse(update[0]);
  }

  @Test void testVisitListCopiesImmutableListPartialView() {
    final RelDataType type = createIntegerType();
    final ImmutableList<RexNode> backingList = createInputRefs(type, 5);
    final List<RexNode> operands = backingList.subList(1, 4);
    final RexNode absent = new RexInputRef(5, type);
    final boolean[] update = {false};

    final List<RexNode> result =
        new ReplacingListShuttle(absent, absent)
            .visitListForTest(operands, update);

    assertNotSame(operands, result);
    assertEquals(operands, result);
    assertInstanceOf(ImmutableList.class, result);
    assertFalse(update[0]);
  }

  private static RelDataType createIntegerType() {
    return RelBuilder.create(RelBuilderTest.config().build())
        .getTypeFactory().createSqlType(SqlTypeName.INTEGER);
  }

  private static ImmutableList<RexNode> createInputRefs(
      RelDataType type, int count) {
    final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
    for (int i = 0; i < count; i++) {
      builder.add(new RexInputRef(i, type));
    }
    return builder.build();
  }

  /** Shuttle that replaces target nodes and exposes {@link #visitList} for testing. */
  private static class ReplacingListShuttle extends RexShuttle {
    private final RexNode[] targetsAndReplacements;

    ReplacingListShuttle(RexNode... targetsAndReplacements) {
      checkArgument(
          targetsAndReplacements.length > 0
              && targetsAndReplacements.length % 2 == 0,
          "targetsAndReplacements must contain target/replacement pairs");
      this.targetsAndReplacements = targetsAndReplacements;
    }

    @Override public RexNode visitInputRef(RexInputRef inputRef) {
      for (int i = 0; i < targetsAndReplacements.length; i += 2) {
        // Match the exact operand instance rather than a structurally equal node.
        if (inputRef == targetsAndReplacements[i]) {
          return targetsAndReplacements[i + 1];
        }
      }
      return inputRef;
    }

    List<RexNode> visitListForTest(
        List<? extends RexNode> exprs, boolean[] update) {
      return visitList(exprs, update);
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3165">[CALCITE-3165]
   * Project#accept(RexShuttle shuttle) does not update rowType</a>. */
  @Test void testProjectUpdatesRowType() {
    final RelBuilder builder = RelBuilder.create(RelBuilderTest.config().build());

    // Equivalent SQL: SELECT deptno, sal FROM emp
    final RelNode root =
        builder
            .scan("EMP")
            .project(
                builder.field("DEPTNO"),
                builder.field("SAL"))
            .build();

    // Equivalent SQL: SELECT CAST(deptno AS VARCHAR), CAST(sal AS VARCHAR) FROM emp
    final RelNode rootWithCast =
        builder
            .scan("EMP")
            .project(
                builder.cast(builder.field("DEPTNO"), SqlTypeName.VARCHAR),
                builder.cast(builder.field("SAL"), SqlTypeName.VARCHAR))
            .build();
    final RelDataType type = rootWithCast.getRowType();

    // Transform the first expression into the second one, by using a RexShuttle
    // that converts every RexInputRef into a 'CAST(RexInputRef AS VARCHAR)'
    final RelNode rootWithCastViaRexShuttle = root.accept(new RexShuttle() {
      @Override public RexNode visitInputRef(RexInputRef inputRef) {
        return  builder.cast(inputRef, SqlTypeName.VARCHAR);
      }
    });
    final RelDataType type2 = rootWithCastViaRexShuttle.getRowType();

    assertThat(type, is(type2));
  }

  @Test void testCalcUpdatesRowType() {
    final RelBuilder builder = RelBuilder.create(RelBuilderTest.config().build());

    // Equivalent SQL: SELECT deptno, sal, sal + 20 FROM emp
    final RelNode root =
        builder
            .scan("EMP")
            .project(
                builder.field("DEPTNO"),
                builder.field("SAL"),
                builder.call(SqlStdOperatorTable.PLUS,
                    builder.field("SAL"), builder.literal(20)))
            .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_TO_CALC)
        .build();
    HepPlanner planner = new HepPlanner(program);
    planner.setRoot(root);
    LogicalCalc calc = (LogicalCalc) planner.findBestExp();

    final RelNode calcWithCastViaRexShuttle = calc.accept(new RexShuttle() {
      @Override public RexNode visitCall(RexCall call) {
        return builder.cast(call, SqlTypeName.VARCHAR);
      }

      @Override public RexNode visitLocalRef(RexLocalRef localRef) {
        if (calc.getProgram().getExprList().get(localRef.getIndex())
            instanceof RexCall) {
          return new RexLocalRef(localRef.getIndex(),
              builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR));
        } else {
          return localRef;
        }
      }
    });

    // Equivalent SQL: SELECT deptno, sal, CAST(sal + 20 AS VARCHAR) FROM emp
    final RelNode rootWithCast =
        builder
            .scan("EMP")
            .project(
                builder.field("DEPTNO"),
                builder.field("SAL"),
                builder.cast(
                    builder.call(SqlStdOperatorTable.PLUS,
                        builder.field("SAL"), builder.literal(20)), SqlTypeName.VARCHAR))
            .build();
    assertThat(calcWithCastViaRexShuttle.getRowType(), is(rootWithCast.getRowType()));
  }
}
