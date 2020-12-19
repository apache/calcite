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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableIntList;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that pushes a {@link Join#isSemiJoin semi-join}
 * down in a tree past a {@link org.apache.calcite.rel.core.Join}
 * in order to trigger other rules that will convert {@code SemiJoin}s.
 *
 * <ul>
 * <li>SemiJoin(LogicalJoin(X, Y), Z) &rarr; LogicalJoin(SemiJoin(X, Z), Y)
 * <li>SemiJoin(LogicalJoin(X, Y), Z) &rarr; LogicalJoin(X, SemiJoin(Y, Z))
 * </ul>
 *
 * <p>Whether this
 * first or second conversion is applied depends on which operands actually
 * participate in the semi-join.
 *
 * @see CoreRules#SEMI_JOIN_JOIN_TRANSPOSE
 */
public class SemiJoinJoinTransposeRule
    extends RelRule<SemiJoinJoinTransposeRule.Config>
    implements TransformationRule {

  /** Creates a SemiJoinJoinTransposeRule. */
  protected SemiJoinJoinTransposeRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public SemiJoinJoinTransposeRule(RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(Config.class));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Join semiJoin = call.rel(0);
    final Join join = call.rel(1);
    if (join.isSemiJoin()) {
      return;
    }
    final ImmutableIntList leftKeys = semiJoin.analyzeCondition().leftKeys;

    // X is the left child of the join below the semi-join
    // Y is the right child of the join below the semi-join
    // Z is the right child of the semi-join
    final int nFieldsX = join.getLeft().getRowType().getFieldList().size();
    final int nFieldsY = join.getRight().getRowType().getFieldList().size();
    final int nFieldsZ = semiJoin.getRight().getRowType().getFieldList().size();
    final int nTotalFields = nFieldsX + nFieldsY + nFieldsZ;
    final List<RelDataTypeField> fields = new ArrayList<>();

    // create a list of fields for the full join result; note that
    // we can't simply use the fields from the semi-join because the
    // row-type of a semi-join only includes the left hand side fields
    List<RelDataTypeField> joinFields =
        semiJoin.getRowType().getFieldList();
    for (int i = 0; i < (nFieldsX + nFieldsY); i++) {
      fields.add(joinFields.get(i));
    }
    joinFields = semiJoin.getRight().getRowType().getFieldList();
    for (int i = 0; i < nFieldsZ; i++) {
      fields.add(joinFields.get(i));
    }

    // determine which operands below the semi-join are the actual
    // Rels that participate in the semi-join
    int nKeysFromX = 0;
    for (int leftKey : leftKeys) {
      if (leftKey < nFieldsX) {
        nKeysFromX++;
      }
    }

    // the keys must all originate from either the left or right;
    // otherwise, a semi-join wouldn't have been created
    assert (nKeysFromX == 0) || (nKeysFromX == leftKeys.size());

    // need to convert the semi-join condition and possibly the keys
    final RexNode newSemiJoinFilter;
    int[] adjustments = new int[nTotalFields];
    if (nKeysFromX > 0) {
      // (X, Y, Z) --> (X, Z, Y)
      // semiJoin(X, Z)
      // pass 0 as Y's adjustment because there shouldn't be any
      // references to Y in the semi-join filter
      setJoinAdjustments(
          adjustments,
          nFieldsX,
          nFieldsY,
          nFieldsZ,
          0,
          -nFieldsY);
      newSemiJoinFilter =
          semiJoin.getCondition().accept(
              new RelOptUtil.RexInputConverter(
                  semiJoin.getCluster().getRexBuilder(),
                  fields,
                  adjustments));
    } else {
      // (X, Y, Z) --> (X, Y, Z)
      // semiJoin(Y, Z)
      setJoinAdjustments(
          adjustments,
          nFieldsX,
          nFieldsY,
          nFieldsZ,
          -nFieldsX,
          -nFieldsX);
      newSemiJoinFilter =
          semiJoin.getCondition().accept(
              new RelOptUtil.RexInputConverter(
                  semiJoin.getCluster().getRexBuilder(),
                  fields,
                  adjustments));
    }

    // create the new join
    final RelNode leftSemiJoinOp;
    if (nKeysFromX > 0) {
      leftSemiJoinOp = join.getLeft();
    } else {
      leftSemiJoinOp = join.getRight();
    }
    final LogicalJoin newSemiJoin =
        LogicalJoin.create(leftSemiJoinOp,
            semiJoin.getRight(),
            // No need to copy the hints, the framework would try to do that.
            ImmutableList.of(),
            newSemiJoinFilter,
            ImmutableSet.of(),
            JoinRelType.SEMI);

    final RelNode left;
    final RelNode right;
    if (nKeysFromX > 0) {
      left = newSemiJoin;
      right = join.getRight();
    } else {
      left = join.getLeft();
      right = newSemiJoin;
    }

    final RelNode newJoin =
        join.copy(
            join.getTraitSet(),
            join.getCondition(),
            left,
            right,
            join.getJoinType(),
            join.isSemiJoinDone());

    call.transformTo(newJoin);
  }

  /**
   * Sets an array to reflect how much each index corresponding to a field
   * needs to be adjusted. The array corresponds to fields in a 3-way join
   * between (X, Y, and Z). X remains unchanged, but Y and Z need to be
   * adjusted by some fixed amount as determined by the input.
   *
   * @param adjustments array to be filled out
   * @param nFieldsX    number of fields in X
   * @param nFieldsY    number of fields in Y
   * @param nFieldsZ    number of fields in Z
   * @param adjustY     the amount to adjust Y by
   * @param adjustZ     the amount to adjust Z by
   */
  private static void setJoinAdjustments(
      int[] adjustments,
      int nFieldsX,
      int nFieldsY,
      int nFieldsZ,
      int adjustY,
      int adjustZ) {
    for (int i = 0; i < nFieldsX; i++) {
      adjustments[i] = 0;
    }
    for (int i = nFieldsX; i < (nFieldsX + nFieldsY); i++) {
      adjustments[i] = adjustY;
    }
    for (int i = nFieldsX + nFieldsY;
        i < (nFieldsX + nFieldsY + nFieldsZ);
        i++) {
      adjustments[i] = adjustZ;
    }
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandFor(LogicalJoin.class, Join.class);

    @Override default SemiJoinJoinTransposeRule toRule() {
      return new SemiJoinJoinTransposeRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Join> joinClass,
        Class<? extends Join> join2Class) {
      return withOperandSupplier(b0 ->
          b0.operand(joinClass).predicate(Join::isSemiJoin).inputs(b1 ->
              b1.operand(join2Class).anyInputs()))
          .as(Config.class);
    }
  }
}
