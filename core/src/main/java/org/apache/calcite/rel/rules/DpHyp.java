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
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.List;

/**
 * The core process of dphyp enumeration algorithm.
 */
@Experimental
public class DpHyp {

  private final HyperGraph hyperGraph;

  private final HashMap<Long, RelNode> dpTable;

  private final RelBuilder builder;

  private final RelMetadataQuery mq;

  public DpHyp(HyperGraph hyperGraph, RelBuilder builder, RelMetadataQuery relMetadataQuery) {
    this.hyperGraph =
        hyperGraph.copy(
            hyperGraph.getTraitSet(),
            hyperGraph.getInputs());
    this.dpTable = new HashMap<>();
    this.builder = builder;
    this.mq = relMetadataQuery;
    // make all field name unique and convert the
    // HyperEdge condition from RexInputRef to RexInputFieldName
    this.hyperGraph.convertHyperEdgeCond(builder);
  }

  /**
   * The entry function of the algorithm. We use a bitmap to represent a leaf node,
   * which indicates the position of the corresponding leaf node in {@link HyperGraph}.
   *
   * <p>After the enumeration is completed, the best join order will be stored
   * in the {@link DpHyp#dpTable}.
   */
  public void startEnumerateJoin() {
    int size = hyperGraph.getInputs().size();
    for (int i = 0; i < size; i++) {
      long singleNode = LongBitmap.newBitmap(i);
      dpTable.put(singleNode, hyperGraph.getInput(i));
      hyperGraph.initEdgeBitMap(singleNode);
    }

    // start enumerating from the second to last
    for (int i = size - 2; i >= 0; i--) {
      long csg = LongBitmap.newBitmap(i);
      long forbidden = csg - 1;
      emitCsg(csg);
      enumerateCsgRec(csg, forbidden);
    }
  }

  /**
   * Given a connected subgraph (csg), enumerate all possible complements subgraph (cmp)
   * that do not include anything from the exclusion subset.
   *
   * <p>Corresponding to EmitCsg in origin paper.
   */
  private void emitCsg(long csg) {
    long forbidden = csg | LongBitmap.getBvBitmap(csg);
    long neighbors = hyperGraph.getNeighborBitmap(csg, forbidden);

    LongBitmap.ReverseIterator reverseIterator = new LongBitmap.ReverseIterator(neighbors);
    for (long cmp : reverseIterator) {
      List<HyperEdge> edges = hyperGraph.connectCsgCmp(csg, cmp);
      if (!edges.isEmpty()) {
        emitCsgCmp(csg, cmp, edges);
      }
      // forbidden the nodes that smaller than current cmp when extend cmp, e.g.
      // neighbors = {t1, t2}, t1 and t2 are connected.
      // when extented t2, we will get (t1, t2)
      // when extented t1, we will get (t1, t2) repeated
      long newForbidden =
              (cmp | LongBitmap.getBvBitmap(cmp)) & neighbors;
      newForbidden = newForbidden | forbidden;
      enumerateCmpRec(csg, cmp, newForbidden);
    }
  }

  /**
   * Given a connected subgraph (csg), expands it recursively by its neighbors.
   * If the expanded csg is connected, try to enumerate its cmp (note that for complex hyperedge,
   * we only select a single representative node to add to the neighbors, so csg and subNeighbor
   * are not necessarily connected. However, it still needs to be expanded to prevent missing
   * complex hyperedge). This method is called after the enumeration of csg is completed,
   * that is, after {@link DpHyp#emitCsg(long csg)}.
   *
   * <p>Corresponding to EnumerateCsgRec in origin paper.
   */
  private void enumerateCsgRec(long csg, long forbidden) {
    long neighbors = hyperGraph.getNeighborBitmap(csg, forbidden);
    LongBitmap.SubsetIterator subsetIterator = new LongBitmap.SubsetIterator(neighbors);
    for (long subNeighbor : subsetIterator) {
      hyperGraph.updateEdgesForUnion(csg, subNeighbor);
      long newCsg = csg | subNeighbor;
      if (dpTable.containsKey(newCsg)) {
        emitCsg(newCsg);
      }
    }
    long newForbidden = forbidden | neighbors;
    subsetIterator.reset();
    for (long subNeighbor : subsetIterator) {
      long newCsg = csg | subNeighbor;
      enumerateCsgRec(newCsg, newForbidden);
    }
  }

  /**
   * Given a connected subgraph (csg) and its complement subgraph (cmp), expands the cmp
   * recursively by neighbors of cmp (cmp and subNeighbor are not necessarily connected,
   * which is the same logic as in {@link DpHyp#enumerateCsgRec}).
   *
   * <p>Corresponding to EnumerateCmpRec in origin paper.
   */
  private void enumerateCmpRec(long csg, long cmp, long forbidden) {
    long neighbors = hyperGraph.getNeighborBitmap(cmp, forbidden);
    LongBitmap.SubsetIterator subsetIterator = new LongBitmap.SubsetIterator(neighbors);
    for (long subNeighbor : subsetIterator) {
      long newCmp = cmp | subNeighbor;
      hyperGraph.updateEdgesForUnion(cmp, subNeighbor);
      if (dpTable.containsKey(newCmp)) {
        List<HyperEdge> edges = hyperGraph.connectCsgCmp(csg, newCmp);
        if (!edges.isEmpty()) {
          emitCsgCmp(csg, newCmp, edges);
        }
      }
    }
    long newForbidden = forbidden | neighbors;
    subsetIterator.reset();
    for (long subNeighbor : subsetIterator) {
      long newCmp = cmp | subNeighbor;
      enumerateCmpRec(csg, newCmp, newForbidden);
    }
  }

  /**
   * Given a connected csg-cmp pair and the hyperedges that connect them, build the
   * corresponding Join plan. If the new Join plan is better than the existing plan,
   * update the {@link DpHyp#dpTable}.
   *
   * <p>Corresponding to EmitCsgCmp in origin paper.
   */
  private void emitCsgCmp(long csg, long cmp, List<HyperEdge> edges) {
    RelNode child1 = dpTable.get(csg);
    RelNode child2 = dpTable.get(cmp);
    if (child1 == null || child2 == null) {
      throw new IllegalArgumentException(
          "csg and cmp were not enumerated in the previous dp process");
    }

    JoinRelType joinType = hyperGraph.extractJoinType(edges);
    if (joinType == null) {
      return;
    }
    RexNode joinCond1 = hyperGraph.extractJoinCond(child1, child2, edges);
    RelNode newPlan1 = builder
        .push(child1)
        .push(child2)
        .join(joinType, joinCond1)
        .build();

    // swap left and right
    RexNode joinCond2 = hyperGraph.extractJoinCond(child2, child1, edges);
    RelNode newPlan2 = builder
        .push(child2)
        .push(child1)
        .join(joinType, joinCond2)
        .build();
    RelNode winPlan = chooseBetterPlan(newPlan1, newPlan2);

    RelNode oriPlan = dpTable.get(csg | cmp);
    if (oriPlan != null) {
      winPlan = chooseBetterPlan(winPlan, oriPlan);
    }
    dpTable.put(csg | cmp, winPlan);
  }

  public @Nullable RelNode getBestPlan() {
    int size = hyperGraph.getInputs().size();
    long wholeGraph = LongBitmap.newBitmapBetween(0, size);
    return dpTable.get(wholeGraph);
  }

  private RelNode chooseBetterPlan(RelNode plan1, RelNode plan2) {
    RelOptCost cost1 = mq.getCumulativeCost(plan1);
    RelOptCost cost2 = mq.getCumulativeCost(plan2);
    if (cost1 != null && cost2 != null) {
      return cost1.isLt(cost2) ? plan1 : plan2;
    } else if (cost1 != null) {
      return plan1;
    } else {
      return plan2;
    }
  }

}
