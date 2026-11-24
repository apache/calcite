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
package org.apache.calcite.rel.core;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalConditionalCorrelate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.test.RelBuilderTest;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import static org.apache.calcite.test.Matchers.hasTree;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link ConditionalCorrelate}.
 */
class ConditionalCorrelateTest {
  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7960">[CALCITE-7960]
   * ConditionalCorrelate.accept(RexShuttle) does not exist</a>. A shuttle
   * that gives a correlation variable a new id must also change the uses of
   * that variable in the condition. */
  @Test void testAcceptRexShuttleVisitsCondition() {
    final RelBuilder b = RelBuilder.create(RelBuilderTest.config().build());
    final RelDataType deptType = b.scan("DEPT").build().getRowType();
    final RexCorrelVariable v = (RexCorrelVariable)
        b.getRexBuilder().makeCorrel(deptType, b.getCluster().createCorrel());
    final RexCorrelVariable w = (RexCorrelVariable)
        b.getRexBuilder().makeCorrel(deptType, b.getCluster().createCorrel());
    final RelNode emps = b.scan("EMP")
        .filter(b.equals(b.field("DEPTNO"), b.field(v, "DEPTNO")))
        .build();
    b.scan("DEPT").push(emps);
    final RexNode condition =
        b.and(b.equals(b.field(2, 0, "DEPTNO"), b.field(2, 1, "DEPTNO")),
            b.equals(b.field(2, 0, "DNAME"), b.field(w, "DNAME")));
    final RelNode right = b.build();
    final RelNode left = b.build();
    final RelNode correlate =
        LogicalConditionalCorrelate.create(left, right, ImmutableList.of(), v.id,
            ImmutableBitSet.of(0), JoinRelType.LEFT_MARK, condition);
    final String planBefore = ""
        + "LogicalConditionalCorrelate(correlation=[$cor0], joinType=[left_mark], "
        + "requiredColumns=[{0}], condition=[AND(=($0, $10), =($1, $cor1.DNAME))])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n"
        + "  LogicalFilter(condition=[=($7, $cor0.DEPTNO)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(correlate, hasTree(planBefore));

    // Gives $cor1 the id $cor2
    final CorrelationId newId = b.getCluster().createCorrel();
    final RelNode renamed = correlate.accept(new RexShuttle() {
      @Override public RexNode visitCorrelVariable(RexCorrelVariable variable) {
        return variable.id.equals(w.id)
            ? b.getRexBuilder().makeCorrel(variable.getType(), newId)
            : variable;
      }
    });
    final String planAfter = ""
        + "LogicalConditionalCorrelate(correlation=[$cor0], joinType=[left_mark], "
        + "requiredColumns=[{0}], condition=[AND(=($0, $10), =($1, $cor2.DNAME))])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n"
        + "  LogicalFilter(condition=[=($7, $cor0.DEPTNO)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(renamed, hasTree(planAfter));
  }
}
