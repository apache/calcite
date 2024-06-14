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
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.immutables.value.Value;

import java.math.BigDecimal;

/**
 * Planner rule that translates a distinct
 * {@link org.apache.calcite.rel.core.Minus}
 * (<code>all</code> = <code>false</code>)
 * into a group of operators composed of
 * {@link org.apache.calcite.rel.core.Union},
 * {@link org.apache.calcite.rel.core.Aggregate},
 * {@link org.apache.calcite.rel.core.Filter},etc.
 *
 * <p>For example, the query plan

 * <blockquote><pre>{@code
 *  LogicalMinus(all=[false])
 *    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2])
 *      LogicalFilter(condition=[=($7, 10)])
 *        LogicalTableScan(table=[[CATALOG, SALES, EMP]])
 *    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2])
 *      LogicalFilter(condition=[=($7, 20)])
 *        LogicalTableScan(table=[[CATALOG, SALES, EMP]])
 * }</pre></blockquote>
 *
 * <p> will convert to
 *
 * <blockquote><pre>{@code
 *  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2])
 *    LogicalFilter(condition=[AND(>($3, 0), =($4, 0))])
 *      LogicalAggregate(group=[{0, 1, 2}], agg#0=[COUNT() FILTER $3], agg#1=[COUNT() FILTER $4])
 *        LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], $f3=[=($3, 0)], $f4=[=($3, 1)])
 *          LogicalUnion(all=[true])
 *            LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], $f3=[0])
 *              LogicalFilter(condition=[=($7, 10)])
 *                LogicalTableScan(table=[[CATALOG, SALES, EMP]])
 *            LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], $f3=[1])
 *              LogicalFilter(condition=[=($7, 20)])
 *                LogicalTableScan(table=[[CATALOG, SALES, EMP]])
 * }</pre></blockquote>
 *
 * @see CoreRules#MINUS_TO_DISTINCT
 */
@Value.Enclosing
public class MinusToDistinctRule
    extends RelRule<MinusToDistinctRule.Config>
    implements TransformationRule {

  protected MinusToDistinctRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public MinusToDistinctRule(Class<? extends Minus> minusClass,
      RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(MinusToDistinctRule.Config.class)
        .withOperandFor(minusClass));
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Minus minus = call.rel(0);

    if (minus.all) {
      // Nothing we can do
      return;
    }

    final RelOptCluster cluster = minus.getCluster();
    final RelBuilder relBuilder = call.builder();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final int branchCount = minus.getInputs().size();

    // For each child branch in minus, add a column which indicates the branch index
    //
    // e.g., select EMPNO from emp -> select EMPNO, 0 from emp
    // 0 indicates that it comes from the first child branch (operand) of the minus operator
    for (int i = 0; i < branchCount; i++) {
      relBuilder.push(minus.getInput(i));
      relBuilder.projectPlus(relBuilder.literal(new BigDecimal(i)));
    }

    // create a union above all the branches
    relBuilder.union(true, branchCount);

    final RelNode union = relBuilder.peek();
    final int originalFieldCnt = union.getRowType().getFieldCount() - 1;

    ImmutableList.Builder<RexNode> projects = ImmutableList.builder();
    // skip the branch index column
    projects.addAll(Util.first(relBuilder.fields(), originalFieldCnt));

    // On top of the Union, add a Project and add one boolean column per branch counter,
    // where the i-th boolean column is true iff the tuple comes from the i-th branch
    //
    // e.g., LogicalProject(EMPNO=[$0], $f1=[=($1, 0)], $f2=[=($1, 1)], $f3=[=($1, 2)])
    // $f1,$f2,$f3 are the boolean indicate whether it comes from the corresponding branch
    for (int i = 0; i < branchCount; i++) {
      projects.add(
          relBuilder.equals(relBuilder.field(originalFieldCnt),
              relBuilder.literal(new BigDecimal(i))));
    }

    relBuilder.project(projects.build());

    // Add the count(*) filter $f1(..) for each branch
    ImmutableList.Builder<RelBuilder.AggCall> aggCalls = ImmutableList.builder();
    for (int i = 0; i < branchCount; i++) {
      aggCalls.add(relBuilder.countStar(null).filter(relBuilder.field(originalFieldCnt + i)));
    }

    final ImmutableBitSet groupSet = ImmutableBitSet.range(originalFieldCnt);
    relBuilder.aggregate(relBuilder.groupKey(groupSet), aggCalls.build());

    ImmutableList.Builder<RexNode> filters = ImmutableList.builder();
    for (int i = 0; i < branchCount; i++) {
      SqlOperator operator =
          i == 0 ? SqlStdOperatorTable.GREATER_THAN
              : SqlStdOperatorTable.EQUALS;
      filters.add(
          rexBuilder.makeCall(operator, relBuilder.field(originalFieldCnt + i),
          relBuilder.literal(new BigDecimal(0))));
    }

    relBuilder.filter(filters.build());
    relBuilder.project(Util.first(relBuilder.fields(), originalFieldCnt));
    call.transformTo(relBuilder.build());
  }

  /**
   * Rule configuration.
   */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableMinusToDistinctRule.Config.of()
        .withOperandFor(LogicalMinus.class);

    @Override default MinusToDistinctRule toRule() {
      return new MinusToDistinctRule(this);
    }

    default MinusToDistinctRule.Config withOperandFor(Class<? extends Minus> minusClass) {
      return withOperandSupplier(b -> b.operand(minusClass).anyInputs())
          .as(MinusToDistinctRule.Config.class);
    }
  }
}
