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

import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Rule that flattens a tree of {@link LogicalJoin}s
 * into a single {@link HyperGraph} with N inputs.
 *
 * @see CoreRules#JOIN_TO_HYPER_GRAPH
 */
@Value.Enclosing
@Experimental
public class JoinToHyperGraphRule
    extends RelRule<JoinToHyperGraphRule.Config>
    implements TransformationRule {

  protected JoinToHyperGraphRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Join origJoin = call.rel(0);
    final RelNode left = call.rel(1);
    final RelNode right = call.rel(2);
    if (unSupportedJoinType(origJoin.getJoinType())) {
      return;
    }

    HyperGraph result;
    long notProjectInputs = 0;
    List<RelNode> inputs = new ArrayList<>();
    List<HyperEdge> leftSubEdges = new ArrayList<>();
    List<HyperEdge> rightSubEdges = new ArrayList<>();
    List<RexNode> joinConds = new ArrayList<>();

    if (origJoin.getCondition().isAlwaysTrue()) {
      joinConds.add(origJoin.getCondition());
    } else {
      RelOptUtil.decomposeConjunction(origJoin.getCondition(), joinConds);
    }

    // when right is HyperGraph, need shift fields related to bitmap of HyperEdge
    int leftNodeCount;
    if (left instanceof HyperGraph && right instanceof HyperGraph) {
      leftNodeCount = left.getInputs().size();
      inputs.addAll(left.getInputs());
      inputs.addAll(right.getInputs());

      notProjectInputs |= ((HyperGraph) left).getNotProjectInputs();
      notProjectInputs |= ((HyperGraph) right).getNotProjectInputs() << leftNodeCount;

      leftSubEdges.addAll(((HyperGraph) left).getEdges());
      rightSubEdges.addAll(
          ((HyperGraph) right).getEdges().stream()
              .map(hyperEdge -> hyperEdge.adjustNodeBit(leftNodeCount))
              .collect(Collectors.toList()));
    } else if (left instanceof HyperGraph) {
      leftNodeCount = left.getInputs().size();
      inputs.addAll(left.getInputs());
      inputs.add(right);

      notProjectInputs |= ((HyperGraph) left).getNotProjectInputs();

      leftSubEdges.addAll(((HyperGraph) left).getEdges());
    } else if (right instanceof HyperGraph) {
      leftNodeCount = 1;
      inputs.add(left);
      inputs.addAll(right.getInputs());

      notProjectInputs |= ((HyperGraph) right).getNotProjectInputs() << leftNodeCount;

      rightSubEdges.addAll(
          ((HyperGraph) right).getEdges().stream()
              .map(hyperEdge -> hyperEdge.adjustNodeBit(leftNodeCount))
              .collect(Collectors.toList()));
    } else {
      leftNodeCount = 1;
      inputs.add(left);
      inputs.add(right);
    }

    // calculate conflict rules
    Map<Long, Long> conflictRules =
        ConflictDetectionHelper.makeConflictRules(
            leftSubEdges,
            rightSubEdges,
            origJoin.getJoinType());
    leftSubEdges.addAll(rightSubEdges);

    Map<Integer, Integer> fieldIndexToNodeIndexMap = new HashMap<>();
    // the map from input index to the count of fields before it, used to convert RexInputRef to
    // RexNodeAndFieldIndex
    Map<Integer, Integer> relativePositionInNode = new HashMap<>();
    int fieldCount = 0;
    for (int i = 0; i < inputs.size(); i++) {
      if (LongBitmap.isOverlap(notProjectInputs, LongBitmap.newBitmap(i))) {
        continue;
      }
      relativePositionInNode.put(i, fieldCount);
      for (int j = 0; j < inputs.get(i).getRowType().getFieldCount(); j++) {
        fieldIndexToNodeIndexMap.put(fieldCount++, i);
      }
    }
    // convert current join condition to hyper edge condition
    for (RexNode joinCond : joinConds) {
      long leftEndpoint;
      long rightEndpoint;
      long leftNodeUsedInPredicate;
      long rightNodeUsedInPredicate;
      long initialLeftNodeBits = LongBitmap.newBitmapBetween(0, leftNodeCount);
      long initialRightNodeBits = LongBitmap.newBitmapBetween(leftNodeCount, inputs.size());
      List<Integer> leftRefs = new ArrayList<>();
      List<Integer> rightRefs = new ArrayList<>();

      RexShuttle shuttle = new RexShuttle() {
        @Override public RexNode visitInputRef(RexInputRef inputRef) {
          Integer nodeIndex = fieldIndexToNodeIndexMap.get(inputRef.getIndex());
          if (nodeIndex == null) {
            throw new AssertionError("When build hyper graph, RexInputRef refers "
                + "a dummy field: " + inputRef + ", rowType is: " + origJoin.getRowType());
          }
          if (nodeIndex < leftNodeCount) {
            leftRefs.add(nodeIndex);
          } else {
            rightRefs.add(nodeIndex);
          }
          Integer fieldOffset = relativePositionInNode.get(nodeIndex);
          if (fieldOffset == null) {
            throw new AssertionError("When build hyper graph, failed to map "
                + "input index to field count before it");
          }
          int fieldIndex = inputRef.getIndex() - fieldOffset;
          if (fieldIndex < 0) {
            throw new AssertionError("When build hyper graph, failed to convert "
                + "the input ref to the relative position of the field in the input");
          }
          return new HyperGraph.RexNodeAndFieldIndex(
              nodeIndex,
              fieldIndex,
              inputRef.getName(),
              inputRef.getType());
        }
      };
      RexNode hyperEdgeCondition = joinCond.accept(shuttle);

      Map<Long, Long> conflictRulesAfterAbsorb = new HashMap<>();
      leftNodeUsedInPredicate = LongBitmap.newBitmapFromList(leftRefs);
      rightNodeUsedInPredicate = LongBitmap.newBitmapFromList(rightRefs);
      if (leftRefs.isEmpty() || rightRefs.isEmpty()) {
        // when cartesian product or degenerate predicate, a complex hyperedge is generated to fix
        // current join operator without exploring more possibilities. See section 6.2 in CD-C paper
        leftEndpoint = initialLeftNodeBits;
        rightEndpoint = initialRightNodeBits;
      } else {
        // simplify conflict rules. See section 5.5 in CD-C paper
        long tes =
            ConflictDetectionHelper.absorbConflictRulesIntoTES(
                leftNodeUsedInPredicate | rightNodeUsedInPredicate,
                conflictRulesAfterAbsorb,
                conflictRules);
        rightEndpoint = tes & initialRightNodeBits;
        leftEndpoint = tes & ~rightEndpoint;
      }
      leftSubEdges.add(
          new HyperEdge(
              leftEndpoint,
              rightEndpoint,
              leftNodeUsedInPredicate,
              rightNodeUsedInPredicate,
              conflictRulesAfterAbsorb,
              initialLeftNodeBits,
              initialRightNodeBits,
              origJoin.getJoinType(),
              hyperEdgeCondition));
    }

    if (!origJoin.getJoinType().projectsRight()) {
      notProjectInputs |= LongBitmap.newBitmapBetween(leftNodeCount, inputs.size());
    }
    result =
        new HyperGraph(
            origJoin.getCluster(),
            origJoin.getTraitSet(),
            inputs,
            notProjectInputs,
            leftSubEdges,
            origJoin.getRowType());

    call.transformTo(result);
  }

  private static boolean unSupportedJoinType(JoinRelType joinType) {
    return joinType == JoinRelType.RIGHT
        || joinType == JoinRelType.ASOF
        || joinType == JoinRelType.LEFT_ASOF;
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableJoinToHyperGraphRule.Config.of()
        .withOperandSupplier(b1 ->
            b1.operand(Join.class).inputs(
                b2 -> b2.operand(RelNode.class).anyInputs(),
                b3 -> b3.operand(RelNode.class).anyInputs()));

    @Override default JoinToHyperGraphRule toRule() {
      return new JoinToHyperGraphRule(this);
    }
  }

}
