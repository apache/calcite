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
import org.apache.calcite.rex.RexNode;

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
    if (!supportedJoinType(origJoin.getJoinType())) {
      return;
    }

    long notProjectInputs = 0;
    List<RelNode> inputs = new ArrayList<>();
    List<HyperEdge> leftSubEdges = new ArrayList<>();
    List<HyperEdge> rightSubEdges = new ArrayList<>();
    List<RexNode> joinConds = new ArrayList<>();

    if (origJoin.getCondition().isAlwaysTrue() || origJoin.getJoinType() != JoinRelType.INNER) {
      joinConds.add(origJoin.getCondition());
    } else {
      // inner join can be rewritten as cross product + filter. Due to this property, inner join
      // condition can be split by conjunction and freely combine with other inner join conditions.
      // So we split inner join condition by conjunction and build multiple hyperedges, which is
      // helpful to obtain the potential optimal plan
      RelOptUtil.decomposeConjunction(origJoin.getCondition(), joinConds);
    }

    // when right is HyperGraph, need shift fields related to bitmap of HyperEdge
    int leftNodeCount;
    if (left instanceof HyperGraph && right instanceof HyperGraph) {
      //
      //              LogicalJoin
      //             /           \
      //       HyperGraph      HyperGraph (need to shift bitmap)
      //
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
      //
      //              LogicalJoin
      //             /           \
      //       HyperGraph      RelNode
      //
      leftNodeCount = left.getInputs().size();
      inputs.addAll(left.getInputs());
      inputs.add(right);

      notProjectInputs |= ((HyperGraph) left).getNotProjectInputs();

      leftSubEdges.addAll(((HyperGraph) left).getEdges());
    } else if (right instanceof HyperGraph) {
      //
      //              LogicalJoin
      //             /           \
      //         RelNode      HyperGraph (need to shift bitmap)
      //
      leftNodeCount = 1;
      inputs.add(left);
      inputs.addAll(right.getInputs());

      notProjectInputs |= ((HyperGraph) right).getNotProjectInputs() << leftNodeCount;

      rightSubEdges.addAll(
          ((HyperGraph) right).getEdges().stream()
              .map(hyperEdge -> hyperEdge.adjustNodeBit(leftNodeCount))
              .collect(Collectors.toList()));
    } else {
      //
      //              LogicalJoin
      //             /           \
      //         RelNode        RelNode
      //
      leftNodeCount = 1;
      inputs.add(left);
      inputs.add(right);
    }
    // the number of inputs cannot exceed 64 with long type as the bitmap of hypergraph inputs.
    if (inputs.size() > 64) {
      return;
    }

    // calculate conflict rules
    List<ConflictRule> conflictRules =
        ConflictDetectionHelper.makeConflictRules(
            leftSubEdges,
            rightSubEdges,
            origJoin.getJoinType());

    // build the map from input ref to node index and field index. The RexInputRef in the join
    // condition will be converted to RexNodeAndFieldIndex and stored in the hyperedge. The reason
    // for the replacement is that the order of inputs will be constantly adjusted during
    // enumeration, and maintaining the correct RexInputRef is too complicated.
    Map<Integer, Integer> inputRefToNodeIndexMap = new HashMap<>();
    Map<Integer, Integer> inputRefToFieldIndexMap = new HashMap<>();
    int inputRef = 0;
    for (int nodeIndex = 0; nodeIndex < inputs.size(); nodeIndex++) {
      if (LongBitmap.isOverlap(notProjectInputs, LongBitmap.newBitmap(nodeIndex))) {
        continue;
      }
      int fieldCount = inputs.get(nodeIndex).getRowType().getFieldCount();
      for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
        inputRefToNodeIndexMap.put(inputRef, nodeIndex);
        inputRefToFieldIndexMap.put(inputRef, fieldIndex);
        inputRef++;
      }
    }

    List<HyperEdge> edges =
        HyperEdge.createHyperEdgesFromJoinConds(
            inputRefToNodeIndexMap,
            inputRefToFieldIndexMap,
            joinConds,
            conflictRules,
            origJoin.getJoinType(),
            leftNodeCount,
            inputs.size());
    edges.addAll(leftSubEdges);
    edges.addAll(rightSubEdges);

    if (!origJoin.getJoinType().projectsRight()) {
      notProjectInputs |= LongBitmap.newBitmapBetween(leftNodeCount, inputs.size());
    }
    HyperGraph hyperGraph =
        new HyperGraph(
            origJoin.getCluster(),
            origJoin.getTraitSet(),
            inputs,
            notProjectInputs,
            edges,
            origJoin.getRowType());
    call.transformTo(hyperGraph);
  }

  private static boolean supportedJoinType(JoinRelType joinType) {
    return joinType == JoinRelType.INNER
        || joinType == JoinRelType.LEFT
        || joinType == JoinRelType.FULL
        || joinType == JoinRelType.SEMI
        || joinType == JoinRelType.ANTI;
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
