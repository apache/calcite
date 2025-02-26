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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import org.immutables.value.Value;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Planner rule that matches a {@link Project} on a {@link Aggregate}
 * and projects away aggregate calls that are not used.
 *
 * <p>Also converts {@code COALESCE(SUM(x), 0)} to {@code SUM0(x)}.
 * This transformation is useful because there are cases where
 * {@link AggregateMergeRule} can merge {@code SUM0} but not {@code SUM}.
 *
 * @see CoreRules#PROJECT_AGGREGATE_MERGE
 */
@Value.Enclosing
public class ProjectAggregateMergeRule
    extends RelRule<ProjectAggregateMergeRule.Config>
    implements TransformationRule {

  /** Creates a ProjectAggregateMergeRule. */
  protected ProjectAggregateMergeRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Project project = call.rel(0);
    final Aggregate aggregate = call.rel(1);
    final RelOptCluster cluster = aggregate.getCluster();

    // Do a quick check. If all aggregate calls are used, and there are no CASE
    // expressions, there is nothing to do.
    final ImmutableBitSet bits =
        RelOptUtil.InputFinder.bits(project.getProjects(), null);
    if (bits.contains(
        ImmutableBitSet.range(aggregate.getGroupCount(),
            aggregate.getRowType().getFieldCount()))
        && kindCount(project.getProjects(), SqlKind.CASE) == 0) {
      return;
    }

    // Replace 'COALESCE(SUM(x), 0)' with 'SUM0(x)' wherever it occurs.
    // Add 'SUM0(x)' to the aggregate call list, if necessary.
    final List<AggregateCall> aggCallList =
        new ArrayList<>(aggregate.getAggCallList());
    final RexShuttle shuttle = new RexShuttle() {
      @Override public RexNode visitCall(RexCall call) {
        switch (call.getKind()) {
        case CASE:
          // Do we have "CASE(IS NOT NULL($0), CAST($0):INTEGER NOT NULL, 0)"?
          final List<RexNode> operands = call.operands;
          if (operands.size() == 3
              && operands.get(0).getKind() == SqlKind.IS_NOT_NULL
              && ((RexCall) operands.get(0)).operands.get(0).getKind()
              == SqlKind.INPUT_REF
              && operands.get(1).getKind() == SqlKind.CAST
              && ((RexCall) operands.get(1)).operands.get(0).getKind()
              == SqlKind.INPUT_REF
              && operands.get(2).getKind() == SqlKind.LITERAL) {
            final RexCall isNotNull = (RexCall) operands.get(0);
            final RexInputRef ref0 = (RexInputRef) isNotNull.operands.get(0);
            final RexCall cast = (RexCall) operands.get(1);
            final RexInputRef ref1 = (RexInputRef) cast.operands.get(0);
            if (ref0.getIndex() != ref1.getIndex()) {
              break;
            }
            final int aggCallIndex = ref1.getIndex() - aggregate.getGroupCount();
            if (aggCallIndex < 0) {
              break;
            }
            final AggregateCall aggCall = aggregate.getAggCallList().get(aggCallIndex);
            if (aggCall.getAggregation().getKind() != SqlKind.SUM) {
              break;
            }
            final RexLiteral literal = (RexLiteral) operands.get(2);
            if (Objects.equals(literal.getValueAs(BigDecimal.class), BigDecimal.ZERO)) {
              int j = findSum0(cluster.getTypeFactory(), aggCall, aggCallList);
              return cluster.getRexBuilder().makeInputRef(call.type, j);
            }
          }
          break;
        default:
          break;
        }
        return super.visitCall(call);
      }
    };
    final List<RexNode> projects2 = shuttle.visitList(project.getProjects());
    final ImmutableBitSet bits2 =
        RelOptUtil.InputFinder.bits(projects2, null);

    // Build the mapping that we will apply to the project expressions.
    final Mappings.TargetMapping mapping =
        Mappings.create(MappingType.FUNCTION,
            aggregate.getGroupCount() + aggCallList.size(), -1);
    int j = 0;
    for (int i = 0; i < mapping.getSourceCount(); i++) {
      if (i < aggregate.getGroupCount()) {
        // Field is a group key. All group keys are retained.
        mapping.set(i, j++);
      } else if (bits2.get(i)) {
        // Field is an aggregate call. It is used.
        mapping.set(i, j++);
      } else {
        // Field is an aggregate call. It is not used. Remove it.
        aggCallList.remove(j - aggregate.getGroupCount());
      }
    }

    final RelBuilder builder = call.builder();
    builder.push(aggregate.getInput());
    builder.aggregate(
        builder.groupKey(aggregate.getGroupSet(), aggregate.groupSets), aggCallList);
    builder.project(
        RexPermuteInputsShuttle.of(mapping).visitList(projects2));
    call.transformTo(builder.build());
  }

  /** Given a call to SUM, finds a call to SUM0 with identical arguments,
   * or creates one and adds it to the list. Returns the index. */
  private static int findSum0(RelDataTypeFactory typeFactory, AggregateCall sum,
      List<AggregateCall> aggCallList) {
    final AggregateCall sum0 =
        AggregateCall.create(sum.getParserPosition(), SqlStdOperatorTable.SUM0, sum.isDistinct(),
            sum.isApproximate(), sum.ignoreNulls(), sum.rexList,
            sum.getArgList(), sum.filterArg, sum.distinctKeys, sum.collation,
            typeFactory.createTypeWithNullability(sum.type, false), null);
    final int i = aggCallList.indexOf(sum0);
    if (i >= 0) {
      return i;
    }
    aggCallList.add(sum0);
    return aggCallList.size() - 1;
  }

  /** Returns the number of calls of a given kind in a list of expressions. */
  private static int kindCount(Iterable<? extends RexNode> nodes,
      final SqlKind kind) {
    final AtomicInteger kindCount = new AtomicInteger(0);
    new RexVisitorImpl<Void>(true) {
      @Override public Void visitCall(RexCall call) {
        if (call.getKind() == kind) {
          kindCount.incrementAndGet();
        }
        return super.visitCall(call);
      }
    }.visitEach(nodes);
    return kindCount.get();
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableProjectAggregateMergeRule.Config.of()
        .withOperandSupplier(b0 ->
            b0.operand(Project.class)
                .oneInput(b1 ->
                    b1.operand(Aggregate.class).anyInputs()));

    @Override default ProjectAggregateMergeRule toRule() {
      return new ProjectAggregateMergeRule(this);
    }
  }
}
