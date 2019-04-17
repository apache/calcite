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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Planner rule that removes a {@link org.apache.calcite.rel.core.SemiJoin}s
 * from a join tree.
 */
public abstract class SemiJoinRemoveRule extends RelOptRule {
  private static final Predicate<Join> IS_LEFT_OR_INNER =
      join -> {
        switch (join.getJoinType()) {
        case LEFT:
        case INNER:
          return true;
        default:
          return false;
        }
      };

  /**
   * Flag to indicate if we need the semijoin keys to be unique when removing it.
   */
  private boolean needsSemiJoinKeysUnique = false;

  //It is invoked after attempts have been made to convert a SemiJoin to an
  //indexed scan on a join factor have failed. Namely, if the join factor does
  //not reduce to a single table that can be scanned using an index.
  //
  //It should only be enabled if all SemiJoins in the plan are advisory; that
  //is, they can be safely dropped without affecting the semantics of the query.
  public static final SemiJoinRemoveRule INSTANCE =
      new AdvisorySemiJoinRemoveRule(SemiJoin.class,
          RelFactories.LOGICAL_BUILDER, "SemiJoinRemoveRule:advisory");

  public static final SemiJoinRemoveRule PROJECT =
      new ProjectSemiJoinRemoveRule(SemiJoin.class, false,
          TableScan.class, Project.class, Join.class,
          RelFactories.LOGICAL_BUILDER, "SemiJoinRemoveRule:project");

  public static final SemiJoinRemoveRule JOIN =
      new JoinSemiJoinRemoveRule(SemiJoin.class, false,
          TableScan.class, Join.class,
          RelFactories.LOGICAL_BUILDER, "SemiJoinRemoveRule:join");

  //~ Constructors -----------------------------------------------------------

  protected SemiJoinRemoveRule(Class<SemiJoin> semiJoinClass,
      RelBuilderFactory relBuilderFactory, String description) {
    super(operand(semiJoinClass, any()), relBuilderFactory, description);
  }

  protected SemiJoinRemoveRule(Class<SemiJoin> semiJoinClass, boolean needsSemiJoinKeysUnique,
      Class<TableScan> tableScanClass, Class<Join> joinClass,
      RelBuilderFactory relBuilderFactory, String description) {
    super(
        operand(semiJoinClass,
            some(operand(tableScanClass, any()),
                operandJ(joinClass,
                null,
                IS_LEFT_OR_INNER,
                    some(operand(tableScanClass, any()),
                        operand(RelNode.class, any()))))),
        relBuilderFactory, description);
    this.needsSemiJoinKeysUnique = needsSemiJoinKeysUnique;
  }

  protected SemiJoinRemoveRule(Class<SemiJoin> semiJoinClass, boolean needsSemiJoinKeysUnique,
      Class<TableScan> tableScanClass, Class<Project> projectClass,
      Class<Join> joinClass, RelBuilderFactory relBuilderFactory, String description) {
    super(
        operand(semiJoinClass,
            some(operand(tableScanClass, any()),
                operand(projectClass,
                    some(
                        operandJ(joinClass,
                            null,
                            IS_LEFT_OR_INNER,
                            some(operand(tableScanClass, any()),
                                operand(RelNode.class, any()))))))),
        relBuilderFactory, description);
    this.needsSemiJoinKeysUnique = needsSemiJoinKeysUnique;
  }

  //~ Methods ----------------------------------------------------------------

  protected void perform(RelOptRuleCall call,
      SemiJoin semiJoin,
      RelNode left1,
      RelNode left2,
      Join join) {
    if (!left1.getDigest().equals(left2.getDigest())) {
      return;
    }
    final RelMetadataQuery mq = call.getMetadataQuery();
    // Only if the join keys are all unique to their sources, we can remove the above semijoin,
    // or the semantics would change cause the duplicate join keys will swell one join row
    // (of left) to multi rows, this would never happen if the semijoin
    // comes from a predicate subquery.
    if (needsSemiJoinKeysUnique) {
      boolean leftKeysUnique = mq.areColumnsUnique(semiJoin.getLeft(),
          ImmutableBitSet.of(semiJoin.leftKeys));
      boolean rightKeysUnique = mq.areColumnsUnique(semiJoin.getRight(),
          ImmutableBitSet.of(semiJoin.rightKeys));
      if (!(leftKeysUnique && rightKeysUnique)) {
        return;
      }
    }
    // Check if the semijoin is like a dim join: the equal join keys are all from
    // the same table/view.
    for (int i = 0; i < semiJoin.leftKeys.size(); i++) {
      final RelColumnOrigin originLeft = mq.getColumnOrigin(semiJoin.getLeft(),
          semiJoin.leftKeys.get(i));
      final RelColumnOrigin originRight = mq.getColumnOrigin(semiJoin.getRight(),
          semiJoin.rightKeys.get(i));
      if (!originLeft.equals(originRight)) {
        return;
      }
    }

    final RelBuilder builder = call.builder();
    final JoinInfo joinInfo = JoinInfo.of(join.getLeft(), join.getRight(), join.getCondition());
    RelNode result;
    if (joinInfo.isEqui()) {
      // If this join condition is equi, construct semijoin directly.
      result = builder.push(join.getLeft())
          .push(join.getRight())
          .semiJoin(join.getCondition())
          .build();
    } else {
      // Or project the left input fields of the join.
      final List<RelDataTypeField> projectFields = join.getLeft().getRowType().getFieldList();
      final List<RexNode> projects = projectFields
          .stream()
          .map(f -> RexInputRef.of(f.getIndex(), projectFields))
          .collect(Collectors.toList());
      final List<String> fieldNames = projectFields.stream().map(RelDataTypeField::getName)
          .collect(Collectors.toList());

      result = builder
          .push(join.getLeft())
          .push(join.getRight())
          .join(join.getJoinType(), join.getCondition())
          .project(projects, fieldNames).build();
    }
    call.transformTo(result);
  }


  //~ SubClasses -------------------------------------------------------------

  /**
   * Rule to remove all the semijoins the in the join tree.
   */
  public static class AdvisorySemiJoinRemoveRule extends SemiJoinRemoveRule {
    public AdvisorySemiJoinRemoveRule(Class<SemiJoin> semiJoinClass,
        RelBuilderFactory relBuilderFactory, String description) {
      super(semiJoinClass, relBuilderFactory, description);
    }

    public void onMatch(RelOptRuleCall call) {
      call.transformTo(call.rel(0).getInput(0));
    }
  }

  /**
   * Rule to remove semijoin with a Project as its right child, this project is on top of an
   * inner or left join.
   */
  public static class ProjectSemiJoinRemoveRule extends SemiJoinRemoveRule {
    public ProjectSemiJoinRemoveRule(Class<SemiJoin> semiJoinClass,
        boolean needsSemiJoinKeysUnique, Class<TableScan> tableScanClass,
        Class<Project> projectClass, Class<Join> joinClass,
        RelBuilderFactory relBuilderFactory, String description) {
      super(semiJoinClass, needsSemiJoinKeysUnique, tableScanClass, projectClass,
          joinClass, relBuilderFactory, description);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      SemiJoin semiJoin = call.rel(0);
      TableScan tableScan1 = call.rel(1);
      TableScan tableScan2 = call.rel(4);
      LogicalJoin join = call.rel(3);
      if (!tableScan1.getDigest().equals(tableScan2.getDigest())) {
        return;
      }
      perform(call, semiJoin, tableScan1, tableScan2, join);
    }
  }

  /**
   * Rule to remove semijoin which has an inner or left join as its right child.
   */
  public static class JoinSemiJoinRemoveRule extends SemiJoinRemoveRule {
    public JoinSemiJoinRemoveRule(Class<SemiJoin> semiJoinClass, boolean needsSemiJoinKeysUnique,
        Class<TableScan> tableScanClass, Class<Join> joinClass,
        RelBuilderFactory relBuilderFactory, String description) {
      super(semiJoinClass, needsSemiJoinKeysUnique, tableScanClass, joinClass,
          relBuilderFactory, description);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      SemiJoin semiJoin = call.rel(0);
      TableScan tableScan1 = call.rel(1);
      TableScan tableScan2 = call.rel(3);
      Join join = call.rel(2);
      perform(call, semiJoin, tableScan1, tableScan2, join);
    }
  }
}

// End SemiJoinRemoveRule.java
