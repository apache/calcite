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
package org.eigenbase.rel.rules;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.*;
import org.eigenbase.util.mapping.*;

import net.hydromatic.optiq.util.BitSets;

/**
 * PullConstantsThroughAggregatesRule removes constant expressions from the
 * group list of an {@link AggregateRel}.
 *
 * <p><b>Effect of the rule</b></p>
 *
 * <p>Since the transformed relational expression has to match the original
 * relational expression, the constants are placed in a projection above the
 * reduced aggregate. If those constants are not used, another rule will remove
 * them from the project.
 *
 * <p>AggregateRel needs its group columns to be on the prefix of its input
 * relational expression. Therefore, if a constant is not on the trailing edge
 * of the group list, removing it will leave a hole. In this case, the rule adds
 * a project before the aggregate to reorder the columns, and permutes them back
 * afterwards.
 */
public class PullConstantsThroughAggregatesRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  /** The singleton. */
  public static final PullConstantsThroughAggregatesRule INSTANCE =
      new PullConstantsThroughAggregatesRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Private: use singleton
   */
  private PullConstantsThroughAggregatesRule() {
    super(
        operand(
            AggregateRel.class,
            operand(ProjectRel.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    AggregateRel aggregate = call.rel(0);
    ProjectRel child = call.rel(1);

    final int groupCount = aggregate.getGroupCount();
    if (groupCount == 1) {
      // No room for optimization since we cannot convert from non-empty
      // GROUP BY list to the empty one.
      return;
    }

    final RexProgram program =
      RexProgram.create(child.getChild().getRowType(),
        child.getProjects(), null, child.getRowType(),
        child.getCluster().getRexBuilder());

    final RelDataType childRowType = child.getRowType();
    IntList constantList = new IntList();
    Map<Integer, RexNode> constants = new HashMap<Integer, RexNode>();
    for (int i : BitSets.toIter(aggregate.getGroupSet())) {
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
            aggCall.adaptTo(child, aggCall.getArgList(), groupCount,
                newGroupCount));
      }
      newAggregate =
          new AggregateRel(
              aggregate.getCluster(),
              child,
              BitSets.range(newGroupCount),
              newAggCalls);
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
      final RelNode project = createProjection(mapping, child);

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
        newAggCalls.add(
            aggCall.adaptTo(project, args, groupCount, newGroupCount));
      }

      // Aggregate on projection.
      newAggregate =
          new AggregateRel(
              aggregate.getCluster(),
              project,
              BitSets.range(newGroupCount),
              newAggCalls);
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

// End PullConstantsThroughAggregatesRule.java
