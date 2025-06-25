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
import org.apache.calcite.plan.PlanTooComplexError;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The core process of dphyp enumeration algorithm.
 */
@Experimental
public class DpHyp {

  private static final Logger LOGGER = CalciteTrace.getDpHypJoinReorderTracer();

  protected final HyperGraph hyperGraph;

  private final Map<Long, RelNode> dpTable;

  // a map from subgraph to the node list of the best join tree. The node list records the node
  // index and whether it is projected, which is used to convert the RexNodeAndFieldIndex in
  // hyperedge to the RexInputRef in join condition
  private final Map<Long, ImmutableList<HyperGraph.NodeState>> resultInputOrder;

  protected final RelBuilder builder;

  private final RelMetadataQuery mq;

  private final int bloat;

  public DpHyp(HyperGraph hyperGraph, RelBuilder builder, RelMetadataQuery relMetadataQuery,
      int bloat) {
    this.hyperGraph =
        hyperGraph.copy(
            hyperGraph.getTraitSet(),
            hyperGraph.getInputs());
    this.dpTable = new HashMap<>();
    this.resultInputOrder = new HashMap<>();
    this.builder = builder;
    this.mq = relMetadataQuery;
    this.bloat = bloat;
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
      LOGGER.debug("Initialize the dp table. Node {{}} is:\n {}",
          i,
          RelOptUtil.toString(hyperGraph.getInput(i)));
      dpTable.put(singleNode, hyperGraph.getInput(i));
      resultInputOrder.put(
          singleNode,
          ImmutableList.of(new HyperGraph.NodeState(i, true)));
      hyperGraph.initEdgeBitMap(singleNode);
    }

    try {
      // start enumerating from the second to last
      for (int i = size - 2; i >= 0; i--) {
        long csg = LongBitmap.newBitmap(i);
        long forbidden = csg - 1;
        emitCsg(csg);
        enumerateCsgRec(csg, forbidden);
      }
    } catch (PlanTooComplexError e) {
      LOGGER.error("The dp table is too large, and the enumeration ends automatically.");
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
    ImmutableList<HyperGraph.NodeState> csgOrder = resultInputOrder.get(csg);
    ImmutableList<HyperGraph.NodeState> cmpOrder = resultInputOrder.get(cmp);
    assert child1 != null && child2 != null && csgOrder != null && cmpOrder != null;
    assert Long.bitCount(csg) == csgOrder.size() && Long.bitCount(cmp) == cmpOrder.size();

    JoinRelType joinType = hyperGraph.extractJoinType(edges);
    if (joinType == null) {
      return;
    }
    // verify whether the subgraph is legal by using the conflict rules in hyperedges
    if (!hyperGraph.applicable(csg | cmp, edges)) {
      return;
    }

    List<HyperGraph.NodeState> unionOrder = new ArrayList<>(csgOrder);
    unionOrder.addAll(cmpOrder);
    // build join condition from hyperedges. e.g.
    // case.1
    // csg: node0_projected [field0, field1], node1_projected [field0, field1],
    //
    //          join
    //          /  \
    //      node0  node1
    //
    // cmp: node2_projected [field0, field1]
    // hyperedge1: node0.field0 = node2.field0
    // hyperedge2: node1.field1 = node2.field1
    // we will get join condition: ($0 = $4) and ($3 = $5)
    //
    //         new_join(condition=[AND(($0 = $4), ($3 = $5))])
    //           /  \
    //        join  node2
    //        /  \
    //    node0  node1
    //
    // case.2
    // csg: node0_projected [field0, field1], node1_not_projected [field0, field1],
    //
    //          join(joinType=semi/anti)
    //          /  \
    //      node0  node1
    //
    // cmp: node2_projected [field0, field1]
    // hyperedge1: node0.field0 = node2.field0
    // hyperedge2: node0.field1 = node2.field1
    // we will get join condition: ($0 = $2) and ($1 = $3)
    //
    //         new_join(condition=[AND(($0 = $2), ($1 = $3))])
    //           /                              \
    //  join(joinType=semi/anti)                node2
    //        /  \
    //    node0  node1
    RexNode joinCond1 = hyperGraph.extractJoinCond(unionOrder, csgOrder.size(), edges, joinType);
    RelNode newPlan1 = builder
        .push(child1)
        .push(child2)
        .join(joinType, joinCond1)
        .build();
    RelNode winPlan = newPlan1;
    ImmutableList<HyperGraph.NodeState> winOrder = ImmutableList.copyOf(unionOrder);
    assert verifyDpResultRowType(newPlan1, unionOrder);

    if (ConflictDetectionHelper.isCommutative(joinType)) {
      // swap left and right
      unionOrder = new ArrayList<>(cmpOrder);
      unionOrder.addAll(csgOrder);
      RexNode joinCond2 = hyperGraph.extractJoinCond(unionOrder, cmpOrder.size(), edges, joinType);
      RelNode newPlan2 = builder
          .push(child2)
          .push(child1)
          .join(joinType, joinCond2)
          .build();
      winPlan = chooseBetterPlan(winPlan, newPlan2);
      assert verifyDpResultRowType(newPlan2, unionOrder);
      if (winPlan.equals(newPlan2)) {
        winOrder = ImmutableList.copyOf(unionOrder);
      }
    }
    LOGGER.debug("Found set {} and {}, connected by condition {}. [cost={}, rows={}]",
        LongBitmap.printBitmap(csg),
        LongBitmap.printBitmap(cmp),
        RexUtil.composeConjunction(
            builder.getRexBuilder(),
            edges.stream()
                .map(edge -> edge.getCondition()).collect(Collectors.toList())),
        mq.getCumulativeCost(winPlan),
        mq.getRowCount(winPlan));

    RelNode oriPlan = dpTable.get(csg | cmp);
    boolean dpTableUpdated = true;
    if (oriPlan != null) {
      winPlan = chooseBetterPlan(winPlan, oriPlan);
      if (winPlan.equals(oriPlan)) {
        winOrder = resultInputOrder.get(csg | cmp);
        dpTableUpdated = false;
      }
    } else {
      // when enumerating a new connected subgraph, check whether the dpTable size is too large
      if (dpTable.size() > bloat) {
        throw new PlanTooComplexError();
      }
    }

    assert winOrder != null;
    if (dpTableUpdated) {
      LOGGER.debug("Dp table is updated. The better plan for subgraph {} now is:\n {}",
          LongBitmap.printBitmap(csg | cmp),
          RelOptUtil.toString(winPlan));
    }
    dpTable.put(csg | cmp, winPlan);
    resultInputOrder.put(csg | cmp, winOrder);
  }

  public @Nullable RelNode getBestPlan() {
    int size = hyperGraph.getInputs().size();
    long wholeGraph = LongBitmap.newBitmapBetween(0, size);
    RelNode orderedJoin = dpTable.get(wholeGraph);
    if (orderedJoin == null) {
      LOGGER.error("The optimal plan was not generated because the enumeration ended prematurely");
      return null;
    }
    LOGGER.debug("Enumeration completed. The best plan is:\n {}", RelOptUtil.toString(orderedJoin));
    ImmutableList<HyperGraph.NodeState> resultOrder = resultInputOrder.get(wholeGraph);
    assert resultOrder != null && resultOrder.size() == size;

    // ensure that the fields produced by the reordered join are in the same order as in the
    // original plan.
    List<RexNode> projects =
        hyperGraph.restoreProjectionOrder(resultOrder,
        orderedJoin.getRowType().getFieldList());
    return builder
        .push(orderedJoin)
        .project(projects)
        .build();
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

  /**
   * Verify that the row type of plans generated by dphyp is equivalent to the origin plan.
   *
   * @param plan          plan generated by dphyp
   * @param resultOrder   node status ordered list
   * @return  true if the plan row type equivalent to the hyperGraph row type
   */
  protected boolean verifyDpResultRowType(RelNode plan, List<HyperGraph.NodeState> resultOrder) {
    // only verify the whole graph
    if (resultOrder.size() != hyperGraph.getInputs().size()) {
      return true;
    }
    List<RexNode> projects =
        hyperGraph.restoreProjectionOrder(resultOrder,
            plan.getRowType().getFieldList());
    RelNode resultNode = builder
        .push(plan)
        .project(projects)
        .build();
    return RelOptUtil.areRowTypesEqual(resultNode.getRowType(), hyperGraph.getRowType(), false);
  }
}
