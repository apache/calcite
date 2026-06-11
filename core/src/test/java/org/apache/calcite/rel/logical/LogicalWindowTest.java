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
package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.rex.RexWindowExclusion;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.MockRelOptPlanner;
import org.apache.calcite.util.ImmutableBitSet;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.calcite.rel.core.Window.Group;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Test for {@link org.apache.calcite.rel.logical.LogicalWindow}.
 */
public class LogicalWindowTest {
  @Test void testCopyWithConstants() {
    final MockRelOptPlanner planner = new MockRelOptPlanner(Contexts.empty());
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    final RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
    final RelTraitSet traitSet = RelTraitSet.createEmpty();
    final RelNode relNode = new AbstractRelNode(cluster, traitSet) {
    };
    final RelDataTypeSystem dataTypeSystem = new RelDataTypeSystemImpl() {
    };

    final RelDataType dataType = new BasicSqlType(dataTypeSystem, SqlTypeName.BOOLEAN);
    final List<RexLiteral> constants =
        Collections.singletonList(
            RexLiteral.fromJdbcString(dataType,
                SqlTypeName.BOOLEAN,
                "TRUE"));
    final RelDataType rowDataType = new BasicSqlType(dataTypeSystem, SqlTypeName.ROW);
    final List<Group> groups = Collections.emptyList();

    final LogicalWindow original =
        new LogicalWindow(cluster, traitSet, relNode, constants, rowDataType, groups);
    final List<RexLiteral> newConstants =
        Collections.singletonList(
            RexLiteral.fromJdbcString(dataType,
                SqlTypeName.BOOLEAN,
                "FALSE"));
    final Window updated = original.copy(newConstants);

    assertNotSame(original, updated);
    assertThat(original.getConstants(), hasSize(1));
    assertSame(constants.get(0), original.getConstants().get(0));
    assertThat(updated.getConstants(), hasSize(1));
    assertSame(newConstants.get(0), updated.getConstants().get(0));
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5929">[CALCITE-5929]
   * Improve LogicalWindow print plan to add the constant value</a>. */
  @Test void testComputeDisplayStringWithLiteralConstant() {
    // Test that computeDisplayString() correctly expands literal constants
    // in window bounds (e.g., "10 PRECEDING" instead of "$1 PRECEDING")
    final MockRelOptPlanner planner = new MockRelOptPlanner(Contexts.empty());
    final SqlTypeFactoryImpl typeFactory =
        new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
    final RelTraitSet traitSet = RelTraitSet.createEmpty();
    final RelNode relNode = new AbstractRelNode(cluster, traitSet) {
    };

    // Create a literal constant: 10
    final RexLiteral literalTen =
        rexBuilder.makeExactLiteral(java.math.BigDecimal.TEN,
        typeFactory.createSqlType(SqlTypeName.BIGINT));
    final List<RexLiteral> constants = Collections.singletonList(literalTen);

    // Create window bounds: 10 PRECEDING to CURRENT ROW
    // The offset is RexInputRef(1) which maps to constants[0] = 10
    final int inputFieldCount = 1;
    final RexInputRef offsetRef = new RexInputRef(inputFieldCount, literalTen.getType());
    final RexWindowBound lowerBound = RexWindowBounds.preceding(offsetRef);

    // Create a window group with this bound
    final List<Window.RexWinAggCall> aggCalls = new ArrayList<>();
    final Group group =
        new Group(ImmutableBitSet.of(),
        true, // isRows
        lowerBound,
        RexWindowBounds.CURRENT_ROW,
        RexWindowExclusion.EXCLUDE_NO_OTHER,
        RelCollations.EMPTY,
        aggCalls);

    // Call computeDisplayString and verify it expands "10 PRECEDING"
    final String displayString = group.computeDisplayString(constants, inputFieldCount);
    assertThat(displayString, containsString("10 PRECEDING"));
    assertThat(displayString, containsString("CURRENT ROW"));
  }

  @Test void testComputeDisplayStringWithConstantExpression() {
    // Test that computeDisplayString() correctly handles constant expressions
    // (not just literals) in window bounds. For example, when a window bound
    // contains RexCall representing an expression like 5+5.
    final MockRelOptPlanner planner = new MockRelOptPlanner(Contexts.empty());
    final SqlTypeFactoryImpl typeFactory =
        new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
    final RelTraitSet traitSet = RelTraitSet.createEmpty();
    final RelNode relNode = new AbstractRelNode(cluster, traitSet) {
    };

    // Create a constant expression: 5 + 5
    final RexLiteral five =
        rexBuilder.makeExactLiteral(java.math.BigDecimal.valueOf(5),
        typeFactory.createSqlType(SqlTypeName.BIGINT));
    final SqlOperator plusOp = SqlStdOperatorTable.PLUS;
    final RexCall addExpr =
        (RexCall) rexBuilder.makeCall(plusOp, five, five);

    // Test that expandBound() correctly handles both literals and expressions.
    // Although the API accepts List<RexLiteral>, at runtime constants can include
    // expressions like RexCall(+, 5, 5). We use an unchecked cast to simulate this.
    @SuppressWarnings("unchecked")
    final List<RexLiteral> constants =
        (List<RexLiteral>) (List<?>) Collections.singletonList(addExpr);

    // Create window bounds with RexInputRef pointing to this expression
    final int inputFieldCount = 1;
    final RexInputRef offsetRef = new RexInputRef(inputFieldCount, addExpr.getType());
    final RexWindowBound lowerBound = RexWindowBounds.preceding(offsetRef);

    // Create a window group
    final List<Window.RexWinAggCall> aggCalls = new ArrayList<>();
    final Group group =
        new Group(ImmutableBitSet.of(),
        true, // isRows
        lowerBound,
        RexWindowBounds.CURRENT_ROW,
        RexWindowExclusion.EXCLUDE_NO_OTHER,
        RelCollations.EMPTY,
        aggCalls);

    // Call computeDisplayString and verify it correctly renders the expression.
    // Since the constant is RexCall(+, 5, 5), expandBound() should call toString()
    // on it (the non-literal branch), which returns the digest representation.
    final String displayString = group.computeDisplayString(constants, inputFieldCount);

    // Verify the expression 5+5 is shown as "+(5:BIGINT, 5:BIGINT) PRECEDING",
    // not as unexpanded "$1 PRECEDING"
    assertThat(displayString, containsString("+(5:BIGINT, 5:BIGINT) PRECEDING"));
    assertThat(displayString, containsString("CURRENT ROW"));
  }
}
