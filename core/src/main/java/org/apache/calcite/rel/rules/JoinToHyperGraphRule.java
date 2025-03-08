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
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
    if (origJoin.getJoinType() != JoinRelType.INNER) {
      return;
    }

    HyperGraph result;
    List<RelNode> inputs = new ArrayList<>();
    List<HyperEdge> edges = new ArrayList<>();
    List<RexNode> joinConds = new ArrayList<>();

    if (origJoin.getCondition().isAlwaysTrue()) {
      joinConds.add(origJoin.getCondition());
    } else {
      RelOptUtil.decomposeConjunction(origJoin.getCondition(), joinConds);
    }

    // when right is HyperGraph, need shift the leftNodeBit, rightNodeBit, condition of HyperEdge
    int leftNodeCount;
    int leftFieldCount = left.getRowType().getFieldCount();
    if (left instanceof HyperGraph && right instanceof HyperGraph) {
      leftNodeCount = left.getInputs().size();
      inputs.addAll(left.getInputs());
      inputs.addAll(right.getInputs());

      edges.addAll(((HyperGraph) left).getEdges());
      edges.addAll(
          ((HyperGraph) right).getEdges().stream()
              .map(hyperEdge -> adjustNodeBit(hyperEdge, leftNodeCount, leftFieldCount))
              .collect(Collectors.toList()));
    } else if (left instanceof HyperGraph) {
      leftNodeCount = left.getInputs().size();
      inputs.addAll(left.getInputs());
      inputs.add(right);

      edges.addAll(((HyperGraph) left).getEdges());
    } else if (right instanceof HyperGraph) {
      leftNodeCount = 1;
      inputs.add(left);
      inputs.addAll(right.getInputs());

      edges.addAll(
          ((HyperGraph) right).getEdges().stream()
              .map(hyperEdge -> adjustNodeBit(hyperEdge, leftNodeCount, leftFieldCount))
              .collect(Collectors.toList()));
    } else {
      leftNodeCount = 1;
      inputs.add(left);
      inputs.add(right);
    }

    HashMap<Integer, Integer> fieldIndexToNodeIndexMap = new HashMap<>();
    int fieldCount = 0;
    for (int i = 0; i < inputs.size(); i++) {
      for (int j = 0; j < inputs.get(i).getRowType().getFieldCount(); j++) {
        fieldIndexToNodeIndexMap.put(fieldCount++, i);
      }
    }
    // convert current join condition to hyper edge condition
    for (RexNode joinCond : joinConds) {
      long leftNodeBits;
      long rightNodeBits;
      List<Integer> leftRefs = new ArrayList<>();
      List<Integer> rightRefs = new ArrayList<>();

      RexVisitorImpl visitor = new RexVisitorImpl<Void>(true) {
        @Override public Void visitInputRef(RexInputRef inputRef) {
          Integer nodeIndex = fieldIndexToNodeIndexMap.get(inputRef.getIndex());
          if (nodeIndex == null) {
            throw new IllegalArgumentException("RexInputRef refers a dummy field: "
                + inputRef + ", rowType is: " + origJoin.getRowType());
          }
          if (nodeIndex < leftNodeCount) {
            leftRefs.add(nodeIndex);
          } else {
            rightRefs.add(nodeIndex);
          }
          return null;
        }
      };
      joinCond.accept(visitor);

      // when cartesian product, make it to complex hyper edge
      if (leftRefs.isEmpty() || rightRefs.isEmpty()) {
        leftNodeBits = LongBitmap.newBitmapBetween(0, leftNodeCount);
        rightNodeBits = LongBitmap.newBitmapBetween(leftNodeCount, inputs.size());
      } else {
        leftNodeBits = LongBitmap.newBitmapFromList(leftRefs);
        rightNodeBits = LongBitmap.newBitmapFromList(rightRefs);
      }
      edges.add(
          new HyperEdge(
              leftNodeBits,
              rightNodeBits,
              origJoin.getJoinType(),
              joinCond));
    }
    result =
        new HyperGraph(
            origJoin.getCluster(),
            origJoin.getTraitSet(),
            inputs,
            edges,
            origJoin.getRowType());

    call.transformTo(result);
  }

  private static HyperEdge adjustNodeBit(HyperEdge hyperEdge, int nodeOffset, int fieldOffset) {
    RexNode newCondition = RexUtil.shift(hyperEdge.getCondition(), fieldOffset);
    return new HyperEdge(
        hyperEdge.getLeftNodeBitmap() << nodeOffset,
        hyperEdge.getRightNodeBitmap() << nodeOffset,
        hyperEdge.getJoinType(),
        newCondition);
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
