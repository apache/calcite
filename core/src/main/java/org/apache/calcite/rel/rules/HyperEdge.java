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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexNodeAndFieldIndex;
import org.apache.calcite.rex.RexShuttle;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Edge in HyperGraph, that represents a join predicate.
 */
@Experimental
public class HyperEdge {

  private final TotalEligibilitySet tes;

  private final ImmutableList<ConflictRule> conflictRules;

  // equivalent to the T(left(o)) ∩ F_T(o) in CD-C paper
  private final long leftNodeUsedInPredicate;

  // equivalent to the T(right(o)) ∩ F_T(o) in CD-C paper
  private final long rightNodeUsedInPredicate;

  // bitmaps of the nodes on the left side of join operator, short for T(left(o))
  private final long initialLeftNodeBits;

  // bitmaps of the nodes on the right side of join operator, short for T(right(o))
  private final long initialRightNodeBits;

  private final JoinRelType joinType;

  // converted from join condition, using RexNodeAndFieldIndex instead of RexInputRef
  private final RexNode condition;

  public HyperEdge(
      TotalEligibilitySet tes,
      List<ConflictRule> conflictRules,
      long leftNodeUsedInPredicate,
      long rightNodeUsedInPredicate,
      long initialLeftNodeBits,
      long initialRightNodeBits,
      JoinRelType joinType,
      RexNode condition) {
    this.tes = tes;
    this.conflictRules = ImmutableList.copyOf(conflictRules);
    this.leftNodeUsedInPredicate = leftNodeUsedInPredicate;
    this.rightNodeUsedInPredicate = rightNodeUsedInPredicate;
    this.initialLeftNodeBits = initialLeftNodeBits;
    this.initialRightNodeBits = initialRightNodeBits;
    this.joinType = joinType;
    this.condition = condition;
  }

  // use tes to generate hyperedge endpoint
  public long getEndpoint() {
    return tes.totalSet;
  }

  public long getLeftEndpoint() {
    return tes.leftSet;
  }

  public long getRightEndpoint() {
    return tes.rightSet;
  }

  public long getLeftNodeUsedInPredicate() {
    return leftNodeUsedInPredicate;
  }

  public long getRightNodeUsedInPredicate() {
    return rightNodeUsedInPredicate;
  }

  public List<ConflictRule> getConflictRules() {
    return conflictRules;
  }

  public long getInitialLeftNodeBits() {
    return initialLeftNodeBits;
  }

  public long getInitialRightNodeBits() {
    return initialRightNodeBits;
  }

  // hyperedge (u, v) is simple if |u| = |v| = 1
  public boolean isSimple() {
    return tes.isSimple();
  }

  public JoinRelType getJoinType() {
    return joinType;
  }

  public RexNode getCondition() {
    return condition;
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(LongBitmap.printBitmap(getLeftEndpoint()))
        .append("——[").append(joinType).append(", ").append(condition).append("]——")
        .append(LongBitmap.printBitmap(getRightEndpoint()));
    return sb.toString();
  }

  // It is convenient for the accept method in the HyperGraph class to call
  public HyperEdge accept(RexShuttle shuttle) {
    RexNode shuttleCondition = condition.accept(shuttle);
    return new HyperEdge(
        tes,
        conflictRules,
        leftNodeUsedInPredicate,
        rightNodeUsedInPredicate,
        initialLeftNodeBits,
        initialRightNodeBits,
        joinType,
        shuttleCondition);
  }

  /**
   * When {@link JoinToHyperGraphRule} constructs a hypergraph , if the right child of Join is a
   * HyperGraph class, all bitmaps related to inputs in this right child need to be shifted
   * according to the number of inputs in the left child of Join.
   *
   * @param nodeOffset number of inputs in the left child
   * @return  HyperEdge produced after shifting
   */
  public HyperEdge adjustNodeBit(int nodeOffset) {
    RexShuttle shiftNodeIndexShuttle = new RexShuttle() {
      @Override public RexNode visitNodeAndFieldIndex(RexNodeAndFieldIndex nodeAndFieldIndex) {
        return new RexNodeAndFieldIndex(
            nodeAndFieldIndex.getNodeIndex() + nodeOffset,
            nodeAndFieldIndex.getFieldIndex(),
            nodeAndFieldIndex.getName(),
            nodeAndFieldIndex.getType());
      }
    };
    RexNode shiftedCondition = condition.accept(shiftNodeIndexShuttle);
    List<ConflictRule> shiftedConflictRules = new ArrayList<>();
    for (ConflictRule conflictRule : conflictRules) {
      shiftedConflictRules.add(conflictRule.shift(nodeOffset));
    }
    return new HyperEdge(
        tes.shift(nodeOffset),
        shiftedConflictRules,
        leftNodeUsedInPredicate << nodeOffset,
        rightNodeUsedInPredicate << nodeOffset,
        initialLeftNodeBits << nodeOffset,
        initialRightNodeBits << nodeOffset,
        joinType,
        shiftedCondition);
  }

  /**
   * Build hyperedges corresponding to a join operator. The construction of a HyperEdge includes
   * total eligibility set, conflict rules, node-related bitmaps, predicate expressions (RexInputRef
   * is replaced by RexNodeAndFieldIndex) and join type.
   *
   * @param inputRefToNodeIndexMap  a map from inputRef to node index
   * @param inputRefToFieldIndexMap a map from inputRef to field index
   * @param joinConds               join conditions
   * @param conflictRules           all conflict rules
   * @param joinType                join type
   * @param leftNodeCount           number of inputs in the left child of Join
   * @param nodeCount               number of inputs of Join
   * @return  HyperEdge list
   */
  public static List<HyperEdge> createHyperEdgesFromJoinConds(
      Map<Integer, Integer> inputRefToNodeIndexMap,
      Map<Integer, Integer> inputRefToFieldIndexMap,
      List<RexNode> joinConds,
      List<ConflictRule> conflictRules,
      JoinRelType joinType,
      int leftNodeCount,
      int nodeCount) {
    // for join operator o, generate bitmaps of the relations on the left/right sides
    long initialLeftNodeBits = LongBitmap.newBitmapBetween(0, leftNodeCount);
    long initialRightNodeBits = LongBitmap.newBitmapBetween(leftNodeCount, nodeCount);

    List<HyperEdge> edges = new ArrayList<>();
    for (RexNode joinCond : joinConds) {
      List<Integer> leftRefs = new ArrayList<>();
      List<Integer> rightRefs = new ArrayList<>();
      RexShuttle inputRef2NodeAndFieldIndexShuttle = new RexShuttle() {
        @Override public RexNode visitInputRef(RexInputRef inputRef) {
          Integer nodeIndex = inputRefToNodeIndexMap.get(inputRef.getIndex());
          assert nodeIndex != null;
          // collect left input index and right input index
          if (nodeIndex < leftNodeCount) {
            leftRefs.add(nodeIndex);
          } else {
            rightRefs.add(nodeIndex);
          }
          Integer fieldIndex = inputRefToFieldIndexMap.get(inputRef.getIndex());
          assert fieldIndex != null;

          return new RexNodeAndFieldIndex(
              nodeIndex,
              fieldIndex,
              inputRef.getName(),
              inputRef.getType());
        }
      };
      RexNode hyperEdgeCondition = joinCond.accept(inputRef2NodeAndFieldIndexShuttle);

      long leftNodeUsedInPredicate = LongBitmap.newBitmapFromList(leftRefs);
      long rightNodeUsedInPredicate = LongBitmap.newBitmapFromList(rightRefs);
      TotalEligibilitySet tes;
      List<ConflictRule> conflictRulesAfterAbsorb = new ArrayList<>();
      if (leftNodeUsedInPredicate == 0 || rightNodeUsedInPredicate == 0) {
        // when dealing with cross products or degenerate predicates, construct total eligibility
        // set using all nodes on the left and right sides (as the endpoint of the hyperedge). This
        // behavior constrains the search space and make sure a correct plan.
        // See section 6.2 in CD-C paper

        // TODO: In some rare case, cross product might be beneficially introduced. Perhaps
        //  selectively add appropriate endpoints for the cross product
        tes = new TotalEligibilitySet(initialLeftNodeBits, initialRightNodeBits);
      } else {
        // Initialize total eligibility set with the nodes referenced by the predicate
        tes = new TotalEligibilitySet(leftNodeUsedInPredicate, rightNodeUsedInPredicate);
        // expand tes with conflict rules. This not only eliminates some conflict rules, but also
        // improves the efficiency of enumeration
        tes =
            tes.absorbFromConflictRules(
                conflictRules,
                conflictRulesAfterAbsorb,
                initialLeftNodeBits,
                initialRightNodeBits);
      }
      edges.add(
          new HyperEdge(
              tes,
              conflictRulesAfterAbsorb,
              leftNodeUsedInPredicate,
              rightNodeUsedInPredicate,
              initialLeftNodeBits,
              initialRightNodeBits,
              joinType,
              hyperEdgeCondition));
    }
    return edges;
  }

  /**
   * Total eligibility set (tes for short) is a set of relations (inputs of HyperGraph). Each
   * hyperedge has a tes. When building a join operator through a hyperedge, the inputs
   * (csg-cmp pair) must contain the tes of the hyperedge. For a hyperedge, tes is its endpoint. Tes
   * is initialized by the relations referenced in the predicate.
   */
  private static class TotalEligibilitySet {

    final long leftSet;

    final long rightSet;

    final long totalSet;

    // as the endpoint of the hyperedge, it indicates whether the hyperedge is simple
    final boolean isSimple;

    TotalEligibilitySet(long leftSet, long rightSet) {
      assert !LongBitmap.isOverlap(leftSet, rightSet);
      this.leftSet = leftSet;
      this.rightSet = rightSet;
      this.totalSet = leftSet | rightSet;
      boolean leftSimple = (leftSet & (leftSet - 1)) == 0;
      boolean rightSimple = (rightSet & (rightSet - 1)) == 0;
      this.isSimple = leftSimple && rightSimple;
    }

    TotalEligibilitySet shift(int offset) {
      return new TotalEligibilitySet(leftSet << offset, rightSet << offset);
    }

    boolean isSimple() {
      return isSimple;
    }

    /**
     * For a conflict rule T1 → T2, if T1 ∩ tes != empty set, we can add T2 to tes due to the
     * nature of conflict rule. Further, if T2 ⊆ tes, we can safely eliminate this conflict rule.
     * This behavior has two benefits:
     *
     * <p> 1. decrease the number of conflicting rules
     *
     * <p> 2. larger tes means larger endpoints of the hyperedge, and enlarging endpoints can
     * decreases the number of csg-cmp-pairs when dphyp enumerating.
     *
     * <p> See section 5.5 and 6.1 in paper.
     *
     * @param conflictRules             initial conflict rules
     * @param conflictRulesAfterAbsorb  conflict rules after absorbing
     * @param initialLeftNodeBits       relations on the left side of the join operator
     * @param initialRightNodeBits      relations on the right side of the join operator
     * @return total eligibility set after absorbing conflict rules
     */
    TotalEligibilitySet absorbFromConflictRules(
        List<ConflictRule> conflictRules,
        List<ConflictRule> conflictRulesAfterAbsorb,
        long initialLeftNodeBits,
        long initialRightNodeBits) {
      long totalSetAbsorb = totalSet;
      for (ConflictRule conflictRule : conflictRules) {
        if (LongBitmap.isOverlap(totalSetAbsorb, conflictRule.from)) {
          totalSetAbsorb |= conflictRule.to;
          continue;
        }
        conflictRulesAfterAbsorb.add(conflictRule);
      }
      assert LongBitmap.isSubSet(totalSetAbsorb, initialLeftNodeBits | initialRightNodeBits);
      // right endpoints = tes ∩ T(right(o)),
      // left endpoints = tes \ right endpoints, see section 6.1 in paper
      long rightSetAbsorb = totalSetAbsorb & initialRightNodeBits;
      long leftSetAbsorb = totalSetAbsorb & ~rightSetAbsorb;
      return new TotalEligibilitySet(leftSetAbsorb, rightSetAbsorb);
    }
  }

}
