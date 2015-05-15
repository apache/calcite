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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.IntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Permutation;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Planner rule that removes constant expressions from the
 * group list of an {@link org.apache.calcite.rel.logical.LogicalAggregate}.
 *
 * <p><b>Effect of the rule</b></p>
 *
 * <p>Since the transformed relational expression has to match the original
 * relational expression, the constants are placed in a projection above the
 * reduced aggregate. If those constants are not used, another rule will remove
 * them from the project.
 *
 * <p>LogicalAggregate needs its group columns to be on the prefix of its input
 * relational expression. Therefore, if a constant is not on the trailing edge
 * of the group list, removing it will leave a hole. In this case, the rule adds
 * a project before the aggregate to reorder the columns, and permutes them back
 * afterwards.
 */
public class AggregateProjectPullUpConstantsRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  /** The singleton. */
  public static final AggregateProjectPullUpConstantsRule INSTANCE =
      new AggregateProjectPullUpConstantsRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Private: use singleton
   */
  private AggregateProjectPullUpConstantsRule() {
    super(
        operand(LogicalAggregate.class, null, Aggregate.IS_SIMPLE,
            operand(LogicalProject.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    LogicalAggregate aggregate = call.rel(0);
    LogicalProject input = call.rel(1);

    final int groupCount = aggregate.getGroupCount();
    if (groupCount == 1) {
      // No room for optimization since we cannot convert from non-empty
      // GROUP BY list to the empty one.
      return;
    }

    final RexProgram program =
      RexProgram.create(input.getInput().getRowType(),
          input.getProjects(),
          null,
          input.getRowType(),
          input.getCluster().getRexBuilder());

    final RelDataType childRowType = input.getRowType();
    IntList constantList = new IntList();
    Map<Integer, RexNode> constants = new HashMap<Integer, RexNode>();
    for (int i : aggregate.getGroupSet()) {
      final RexLocalRef ref = program.getProjectList().get(i);
      if (program.isConstant(ref)) {
        constantList.add(i);
        constants.put(
            i,
            program.gatherExpr(ref));
      }
    }

    // None of the group expressions are constant. Nothing to do.
    if (constantList.size() == 0) {
      return;
    }

    if (groupCount == constantList.size()) {
      // At least a single item in group by is required.
      // Otherwise group by 1,2 might be altered to group by ()
      // Removing of the first element is not optimal here,
      // however it will allow us to use fast path below (just trim
      // groupCount)
      constantList.remove(0);
    }

    final int newGroupCount = groupCount - constantList.size();
    final RelNode newAggregate;

    // If the constants are on the trailing edge of the group list, we just
    // reduce the group count.
    if (constantList.get(0) == newGroupCount) {
      // Clone aggregate calls.
      final List<AggregateCall> newAggCalls =
          new ArrayList<AggregateCall>();
      for (AggregateCall aggCall : aggregate.getAggCallList()) {
        newAggCalls.add(
            aggCall.adaptTo(input, aggCall.getArgList(), aggCall.filterArg,
                groupCount, newGroupCount));
      }
      newAggregate =
          LogicalAggregate.create(input, false,
              ImmutableBitSet.range(newGroupCount), null, newAggCalls);
    } else {
      // Create the mapping from old field positions to new field
      // positions.
      final Permutation mapping =
          new Permutation(childRowType.getFieldCount());
      mapping.identity();

      // Ensure that the first positions in the mapping are for the new
      // group columns.
      for (int i = 0, groupOrdinal = 0, constOrdinal = newGroupCount;
          i < groupCount;
          ++i) {
        if (i >= groupCount) {
          mapping.set(i, i);
        } else if (constants.containsKey(i)) {
          mapping.set(i, constOrdinal++);
        } else {
          mapping.set(i, groupOrdinal++);
        }
      }

      // Create a projection to permute fields into these positions.
      final RelNode project = createProjection(mapping, input);

      // Adjust aggregate calls for new field positions.
      final List<AggregateCall> newAggCalls =
          new ArrayList<AggregateCall>();
      for (AggregateCall aggCall : aggregate.getAggCallList()) {
        final int argCount = aggCall.getArgList().size();
        final List<Integer> args = new ArrayList<Integer>(argCount);
        for (int j = 0; j < argCount; j++) {
          final Integer arg = aggCall.getArgList().get(j);
          args.add(mapping.getTarget(arg));
        }
        final int filterArg = aggCall.filterArg < 0 ? aggCall.filterArg
            : mapping.getTarget(aggCall.filterArg);
        newAggCalls.add(
            aggCall.adaptTo(project, args, filterArg, groupCount,
                newGroupCount));
      }

      // Aggregate on projection.
      newAggregate =
          LogicalAggregate.create(project, false,
              ImmutableBitSet.range(newGroupCount), null, newAggCalls);
    }

    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();

    // Create a projection back again.
    List<Pair<RexNode, String>> projects =
        new ArrayList<Pair<RexNode, String>>();
    int source = 0;
    for (RelDataTypeField field : aggregate.getRowType().getFieldList()) {
      RexNode expr;
      final int i = field.getIndex();
      if (i >= groupCount) {
        // Aggregate expressions' names and positions are unchanged.
        expr = rexBuilder.makeInputRef(newAggregate, i - constantList.size());
      } else if (constantList.contains(i)) {
        // Re-generate the constant expression in the project.
        expr = constants.get(i);
      } else {
        // Project the aggregation expression, in its original
        // position.
        expr = rexBuilder.makeInputRef(newAggregate, source);
        ++source;
      }
      projects.add(Pair.of(expr, field.getName()));
    }
    final RelNode inverseProject =
        RelOptUtil.createProject(newAggregate, projects, false);

    call.transformTo(inverseProject);
  }

  /**
   * Creates a projection which permutes the fields of a given relational
   * expression.
   *
   * <p>For example, given a relational expression [A, B, C, D] and a mapping
   * [2:1, 3:0], returns a projection [$3 AS C, $2 AS B].
   *
   * @param mapping Mapping to apply to source columns
   * @param child   Relational expression
   * @return Relational expressions with permutation applied
   */
  private static RelNode createProjection(
      final Mapping mapping,
      RelNode child) {
    // Every target has precisely one source; every source has at most
    // one target.
    assert mapping.getMappingType().isA(MappingType.INVERSE_SURJECTION);
    final RelDataType childRowType = child.getRowType();
    assert mapping.getSourceCount() == childRowType.getFieldCount();
    List<Pair<RexNode, String>> projects =
        new ArrayList<Pair<RexNode, String>>();
    for (int target = 0; target < mapping.getTargetCount(); ++target) {
      int source = mapping.getSource(target);
      final RexBuilder rexBuilder = child.getCluster().getRexBuilder();
      projects.add(
          Pair.of(
              (RexNode) rexBuilder.makeInputRef(child, source),
              childRowType.getFieldList().get(source).getName()));
    }
    return RelOptUtil.createProject(child, projects, false);
  }
}

// End AggregateProjectPullUpConstantsRule.java
