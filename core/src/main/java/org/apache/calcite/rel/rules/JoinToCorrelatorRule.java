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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptQuery;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlation;
import org.apache.calcite.rel.core.Correlator;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.IntPair;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Rule that converts a {@link org.apache.calcite.rel.logical.LogicalJoin}
 * into a {@link org.apache.calcite.rel.core.Correlator}, which can
 * then be implemented using nested loops.
 *
 * <p>For example,</p>
 *
 * <blockquote><code>select * from emp join dept on emp.deptno =
 * dept.deptno</code></blockquote>
 *
 * <p>becomes a Correlator which restarts LogicalTableScan("DEPT") for each
 * row read from LogicalTableScan("EMP").</p>
 *
 * <p>This rule is not applicable if for certain types of outer join. For
 * example,</p>
 *
 * <blockquote><code>select * from emp right join dept on emp.deptno =
 * dept.deptno</code></blockquote>
 *
 * <p>would require emitting a NULL emp row if a certain department contained no
 * employees, and Correlator cannot do that.</p>
 */
public class JoinToCorrelatorRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  public static final JoinToCorrelatorRule INSTANCE =
      new JoinToCorrelatorRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Private constructor; use singleton {@link #INSTANCE}.
   */
  private JoinToCorrelatorRule() {
    super(operand(LogicalJoin.class, any()));
  }

  //~ Methods ----------------------------------------------------------------

  public boolean matches(RelOptRuleCall call) {
    LogicalJoin join = call.rel(0);
    switch (join.getJoinType()) {
    case INNER:
    case LEFT:
      return true;
    case FULL:
    case RIGHT:
      return false;
    default:
      throw Util.unexpected(join.getJoinType());
    }
  }

  public void onMatch(RelOptRuleCall call) {
    assert matches(call);
    final LogicalJoin join = call.rel(0);
    RelNode right = join.getRight();
    final RelNode left = join.getLeft();
    final JoinInfo joinInfo = join.analyzeCondition();
    final List<Correlation> correlationList = Lists.newArrayList();
    final RelOptCluster cluster = join.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final List<RexNode> conditions = Lists.newArrayList();
    for (IntPair p : joinInfo.pairs()) {
      final String dynInIdStr = cluster.getQuery().createCorrel();
      final int dynInId = RelOptQuery.getCorrelOrdinal(dynInIdStr);

      // Create correlation to say 'each row, set variable #id
      // to the value of column #leftKey'.
      correlationList.add(new Correlation(dynInId, p.source));
      conditions.add(
          rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
              rexBuilder.makeInputRef(right, p.target),
              rexBuilder.makeCorrel(
                  left.getRowType().getFieldList().get(p.source).getType(),
                  dynInIdStr)));
    }
    final RelNode filteredRight = RelOptUtil.createFilter(right, conditions);
    RelNode newRel =
        new Correlator(
            join.getCluster(),
            left,
            filteredRight,
            joinInfo.getRemaining(join.getCluster().getRexBuilder()),
            correlationList,
            join.getJoinType());
    call.transformTo(newRel);
  }
}

// End JoinToCorrelatorRule.java
