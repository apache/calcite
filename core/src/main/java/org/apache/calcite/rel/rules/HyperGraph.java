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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexNodeAndFieldIndex;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import com.google.common.collect.Lists;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * HyperGraph represents a join graph.
 */
@Experimental
public class HyperGraph extends AbstractRelNode {

  private final List<RelNode> inputs;

  // unprojected input (from the right child of the semi/anti join) bitmap. Used to convert
  // RexInputRef to RexNodeAndFieldIndex when building hypergraph
  private final long notProjectInputs;

  @SuppressWarnings("HidingField")
  private final RelDataType rowType;

  private final List<HyperEdge> edges;

  // record the indices of complex hyper edges in the 'edges'
  private final ImmutableBitSet complexEdgesBitmap;

  /**
   * For the HashMap fields, key is the bitmap for inputs,
   * value is the hyper edge bitmap in edges.
   */
  // record which hyper edges have been used by the enumerated csg-cmp pairs
  private final HashMap<Long, BitSet> ccpUsedEdgesMap;

  private final HashMap<Long, BitSet> simpleEdgesMap;

  private final HashMap<Long, BitSet> complexEdgesMap;

  // node bitmap overlaps edge's leftNodeBits or rightNodeBits, but does not completely cover
  private final HashMap<Long, BitSet> overlapEdgesMap;

  protected HyperGraph(RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelNode> inputs,
      long notProjectInputs,
      List<HyperEdge> edges,
      RelDataType rowType) {
    super(cluster, traitSet);
    this.inputs = Lists.newArrayList(inputs);
    this.notProjectInputs = notProjectInputs;
    this.edges = Lists.newArrayList(edges);
    this.rowType = rowType;
    ImmutableBitSet.Builder bitSetBuilder = ImmutableBitSet.builder();
    for (int i = 0; i < edges.size(); i++) {
      if (!edges.get(i).isSimple()) {
        bitSetBuilder.set(i);
      }
    }
    this.complexEdgesBitmap = bitSetBuilder.build();
    this.ccpUsedEdgesMap = new HashMap<>();
    this.simpleEdgesMap = new HashMap<>();
    this.complexEdgesMap = new HashMap<>();
    this.overlapEdgesMap = new HashMap<>();
  }

  protected HyperGraph(RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelNode> inputs,
      long notProjectInputs,
      List<HyperEdge> edges,
      RelDataType rowType,
      ImmutableBitSet complexEdgesBitmap,
      HashMap<Long, BitSet> ccpUsedEdgesMap,
      HashMap<Long, BitSet> simpleEdgesMap,
      HashMap<Long, BitSet> complexEdgesMap,
      HashMap<Long, BitSet> overlapEdgesMap) {
    super(cluster, traitSet);
    this.inputs = Lists.newArrayList(inputs);
    this.notProjectInputs = notProjectInputs;
    this.edges = Lists.newArrayList(edges);
    this.rowType = rowType;
    this.complexEdgesBitmap = complexEdgesBitmap;
    this.ccpUsedEdgesMap = new HashMap<>(ccpUsedEdgesMap);
    this.simpleEdgesMap = new HashMap<>(simpleEdgesMap);
    this.complexEdgesMap = new HashMap<>(complexEdgesMap);
    this.overlapEdgesMap = new HashMap<>(overlapEdgesMap);
  }

  @Override public HyperGraph copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new HyperGraph(
        getCluster(),
        traitSet,
        inputs,
        notProjectInputs,
        edges,
        rowType,
        complexEdgesBitmap,
        ccpUsedEdgesMap,
        simpleEdgesMap,
        complexEdgesMap,
        overlapEdgesMap);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    for (Ord<RelNode> ord : Ord.zip(inputs)) {
      pw.input("input#" + ord.i, ord.e);
    }
    List<String> hyperEdges = edges.stream()
        .map(hyperEdge -> hyperEdge.toString())
        .collect(Collectors.toList());
    pw.item("edges", String.join(",", hyperEdges));
    return pw;
  }

  @Override public List<RelNode> getInputs() {
    return inputs;
  }

  @Override public void replaceInput(int ordinalInParent, RelNode p) {
    inputs.set(ordinalInParent, p);
    recomputeDigest();
  }

  @Override public RelDataType deriveRowType() {
    return rowType;
  }

  @Override public RelNode accept(RexShuttle shuttle) {
    List<HyperEdge> shuttleEdges = new ArrayList<>();
    for (HyperEdge edge : edges) {
      HyperEdge shuttleEdge =
          edge.accept(shuttle);
      shuttleEdges.add(shuttleEdge);
    }

    return new HyperGraph(
        getCluster(),
        traitSet,
        inputs,
        notProjectInputs,
        shuttleEdges,
        rowType,
        complexEdgesBitmap,
        ccpUsedEdgesMap,
        simpleEdgesMap,
        complexEdgesMap,
        overlapEdgesMap);
  }

  //~ hyper graph method ----------------------------------------------------------

  public List<HyperEdge> getEdges() {
    return edges;
  }

  public long getNotProjectInputs() {
    return notProjectInputs;
  }

  public long getNeighborBitmap(long csg, long forbidden) {
    long neighbors = 0L;
    List<HyperEdge> simpleEdges = simpleEdgesMap.getOrDefault(csg, new BitSet()).stream()
        .mapToObj(edges::get)
        .collect(Collectors.toList());
    for (HyperEdge edge : simpleEdges) {
      neighbors |= edge.getEndpoint();
    }

    forbidden = forbidden | csg;
    neighbors = neighbors & ~forbidden;
    forbidden = forbidden | neighbors;

    List<HyperEdge> complexEdges = complexEdgesMap.getOrDefault(csg, new BitSet()).stream()
        .mapToObj(edges::get)
        .collect(Collectors.toList());
    for (HyperEdge edge : complexEdges) {
      long leftBitmap = edge.getLeftEndpoint();
      long rightBitmap = edge.getRightEndpoint();
      if (LongBitmap.isSubSet(leftBitmap, csg) && !LongBitmap.isOverlap(rightBitmap, forbidden)) {
        neighbors |= Long.lowestOneBit(rightBitmap);
      } else if (LongBitmap.isSubSet(rightBitmap, csg)
          && !LongBitmap.isOverlap(leftBitmap, forbidden)) {
        neighbors |= Long.lowestOneBit(leftBitmap);
      }
    }
    return neighbors;
  }

  /**
   * If csg and cmp are connected, return the edges that connect them.
   */
  public List<HyperEdge> connectCsgCmp(long csg, long cmp) {
    checkArgument(simpleEdgesMap.containsKey(csg));
    checkArgument(simpleEdgesMap.containsKey(cmp));
    List<HyperEdge> connectedEdges = new ArrayList<>();
    BitSet connectedEdgesBitmap = new BitSet();
    connectedEdgesBitmap.or(simpleEdgesMap.getOrDefault(csg, new BitSet()));
    connectedEdgesBitmap.or(complexEdgesMap.getOrDefault(csg, new BitSet()));

    BitSet cmpEdgesBitmap = new BitSet();
    cmpEdgesBitmap.or(simpleEdgesMap.getOrDefault(cmp, new BitSet()));
    cmpEdgesBitmap.or(complexEdgesMap.getOrDefault(cmp, new BitSet()));
    connectedEdgesBitmap.and(cmpEdgesBitmap);

    // only consider the records related to csg and cmp in the simpleEdgesMap/complexEdgesMap,
    // may omit some complex hyper edges. e.g.
    // csg = {t1, t3}, cmp = {t2}, will omit the edge (t1, t2)——(t3)
    BitSet mayMissedEdges = new BitSet();
    mayMissedEdges.or(complexEdgesBitmap.toBitSet());
    mayMissedEdges.andNot(ccpUsedEdgesMap.getOrDefault(csg, new BitSet()));
    mayMissedEdges.andNot(ccpUsedEdgesMap.getOrDefault(cmp, new BitSet()));
    mayMissedEdges.andNot(connectedEdgesBitmap);
    mayMissedEdges.stream()
            .forEach(index -> {
              HyperEdge edge = edges.get(index);
              if (LongBitmap.isSubSet(edge.getEndpoint(), csg | cmp)) {
                connectedEdgesBitmap.set(index);
              }
            });

    // record hyper edges are used by current csg ∪ cmp
    BitSet curUsedEdges = new BitSet();
    curUsedEdges.or(connectedEdgesBitmap);
    curUsedEdges.or(ccpUsedEdgesMap.getOrDefault(csg, new BitSet()));
    curUsedEdges.or(ccpUsedEdgesMap.getOrDefault(cmp, new BitSet()));
    if (ccpUsedEdgesMap.containsKey(csg | cmp)) {
      checkArgument(
          curUsedEdges.equals(ccpUsedEdgesMap.get(csg | cmp)));
    }
    ccpUsedEdgesMap.put(csg | cmp, curUsedEdges);

    connectedEdgesBitmap.stream()
        .forEach(index -> connectedEdges.add(edges.get(index)));
    return connectedEdges;
  }

  public void initEdgeBitMap(long subset) {
    BitSet simpleBitSet = new BitSet();
    BitSet complexBitSet = new BitSet();
    BitSet overlapBitSet = new BitSet();
    for (int i = 0; i < edges.size(); i++) {
      HyperEdge edge = edges.get(i);
      if (isAccurateEdge(edge, subset)) {
        if (edge.isSimple()) {
          simpleBitSet.set(i);
        } else {
          complexBitSet.set(i);
        }
      } else if (isOverlapEdge(edge, subset)) {
        overlapBitSet.set(i);
      }
    }
    simpleEdgesMap.put(subset, simpleBitSet);
    complexEdgesMap.put(subset, complexBitSet);
    overlapEdgesMap.put(subset, overlapBitSet);
  }

  public void updateEdgesForUnion(long subset1, long subset2) {
    if (!simpleEdgesMap.containsKey(subset1)) {
      initEdgeBitMap(subset1);
    }
    if (!simpleEdgesMap.containsKey(subset2)) {
      initEdgeBitMap(subset2);
    }
    long unionSet = subset1 | subset2;
    if (simpleEdgesMap.containsKey(unionSet)) {
      return;
    }

    BitSet unionSimpleBitSet = new BitSet();
    unionSimpleBitSet.or(simpleEdgesMap.getOrDefault(subset1, new BitSet()));
    unionSimpleBitSet.or(simpleEdgesMap.getOrDefault(subset2, new BitSet()));

    BitSet unionComplexBitSet = new BitSet();
    unionComplexBitSet.or(complexEdgesMap.getOrDefault(subset1, new BitSet()));
    unionComplexBitSet.or(complexEdgesMap.getOrDefault(subset2, new BitSet()));

    BitSet unionOverlapBitSet = new BitSet();
    unionOverlapBitSet.or(overlapEdgesMap.getOrDefault(subset1, new BitSet()));
    unionOverlapBitSet.or(overlapEdgesMap.getOrDefault(subset2, new BitSet()));

    // the overlaps edge that belongs to subset1/subset2
    // may be complex edge for subset1 union subset2
    for (int index : unionOverlapBitSet.stream().toArray()) {
      HyperEdge edge = edges.get(index);
      if (isAccurateEdge(edge, unionSet)) {
        unionComplexBitSet.set(index);
        unionOverlapBitSet.set(index, false);
      }
    }

    // remove cycle in subset1 union subset2
    for (int index : unionSimpleBitSet.stream().toArray()) {
      HyperEdge edge = edges.get(index);
      if (!isAccurateEdge(edge, unionSet)) {
        unionSimpleBitSet.set(index, false);
      }
    }
    for (int index : unionComplexBitSet.stream().toArray()) {
      HyperEdge edge = edges.get(index);
      if (!isAccurateEdge(edge, unionSet)) {
        unionComplexBitSet.set(index, false);
      }
    }

    simpleEdgesMap.put(unionSet, unionSimpleBitSet);
    complexEdgesMap.put(unionSet, unionComplexBitSet);
    overlapEdgesMap.put(unionSet, unionOverlapBitSet);
  }

  private static boolean isAccurateEdge(HyperEdge edge, long subset) {
    boolean isLeftEnd = LongBitmap.isSubSet(edge.getLeftEndpoint(), subset)
        && !LongBitmap.isOverlap(edge.getRightEndpoint(), subset);
    boolean isRightEnd = LongBitmap.isSubSet(edge.getRightEndpoint(), subset)
        && !LongBitmap.isOverlap(edge.getLeftEndpoint(), subset);
    return isLeftEnd || isRightEnd;
  }

  private static boolean isOverlapEdge(HyperEdge edge, long subset) {
    boolean isLeftEnd = LongBitmap.isOverlap(edge.getLeftEndpoint(), subset)
        && !LongBitmap.isOverlap(edge.getRightEndpoint(), subset);
    boolean isRightEnd = LongBitmap.isOverlap(edge.getRightEndpoint(), subset)
        && !LongBitmap.isOverlap(edge.getLeftEndpoint(), subset);
    return isLeftEnd || isRightEnd;
  }

  public @Nullable JoinRelType extractJoinType(List<HyperEdge> edges) {
    JoinRelType joinType = edges.get(0).getJoinType();
    for (int i = 1; i < edges.size(); i++) {
      if (edges.get(i).getJoinType() != joinType) {
        return null;
      }
    }
    return joinType;
  }

  /**
   * Check whether the csg-cmp pair is applicable through the conflict rule.
   *
   * @param csgcmp  csg-cmp pair
   * @param edges   hyper edges with conflict rules
   * @return  true if the csg-cmp pair is applicable
   */
  public boolean applicable(long csgcmp, List<HyperEdge> edges) {
    for (HyperEdge edge : edges) {
      if (!LongBitmap.isSubSet(edge.getEndpoint(), csgcmp)) {
        return false;
      }
      for (ConflictRule conflictRule : edge.getConflictRules()) {
        if (LongBitmap.isOverlap(csgcmp, conflictRule.from)
            && !LongBitmap.isSubSet(conflictRule.to, csgcmp)) {
          // for conflict rule T1 → T2, if T1 ∩ csgcmp != empty set, then T2 must be
          // included in csgcmp
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Build an RexNode expression for the predicate corresponding to a set of hyperedges.
   *
   * @param inputOrder  node status ordered list for current csg-cmp
   * @param leftCount   number of tables in left child
   * @param edges       hyper edges
   * @param joinType    join type
   * @return  join condition
   */
  public RexNode extractJoinCond(
      List<NodeState> inputOrder,
      int leftCount,
      List<HyperEdge> edges,
      JoinRelType joinType) {
    // a map from (node index, field index) to input ref
    Map<Pair<Integer, Integer>, Integer> nodeAndFieldIndex2InputRefMap = new HashMap<>();
    int inputRef = 0;
    for (int i = 0; i < inputOrder.size(); i++) {
      NodeState nodeState = inputOrder.get(i);
      if (nodeState.projected) {
        int fieldCount = inputs.get(nodeState.nodeIndex).getRowType().getFieldCount();
        for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
          nodeAndFieldIndex2InputRefMap.put(
              Pair.of(nodeState.nodeIndex, fieldIndex), inputRef++);
        }
      }
    }

    RexShuttle nodeAndFieldIndex2InputRefShuttle = new RexShuttle() {
      @Override public RexNode visitNodeAndFieldIndex(RexNodeAndFieldIndex nodeAndFieldIndex) {
        Integer inputRef =
            nodeAndFieldIndex2InputRefMap.get(
                Pair.of(nodeAndFieldIndex.getNodeIndex(), nodeAndFieldIndex.getFieldIndex()));
        assert inputRef != null;
        return new RexInputRef(inputRef, nodeAndFieldIndex.getType());
      }
    };
    List<RexNode> joinConds = new ArrayList<>();
    for (HyperEdge edge : edges) {
      RexNode inputRefCond = edge.getCondition().accept(nodeAndFieldIndex2InputRefShuttle);
      joinConds.add(inputRefCond);
    }

    // update the node status of subgraph(csg-cmp pair) according to the join type.
    if (!joinType.projectsRight()) {
      for (int i = leftCount; i < inputOrder.size(); i++) {
        int nodeIndex = inputOrder.get(i).nodeIndex;
        inputOrder.set(i, new NodeState(nodeIndex, false));
      }
    }
    return RexUtil.composeConjunction(getCluster().getRexBuilder(), joinConds);
  }

  /**
   * Ensure that the fields produced by the reordered join are in the same order as in the original
   * plan.
   *
   * @param resultOrder node status ordered list for the final result
   * @param rowTypeList rowType of the final result
   * @return  list of RexInputRef
   */
  public List<RexNode> restoreProjectionOrder(
      List<NodeState> resultOrder,
      List<RelDataTypeField> rowTypeList) {
    // a map from node index to the number of fields projected before this node
    Map<Integer, Integer> nodeIndexToFieldCountBefore = new HashMap<>();
    int projectedFieldCount = 0;
    for (NodeState nodeState : resultOrder) {
      nodeIndexToFieldCountBefore.put(nodeState.nodeIndex, projectedFieldCount);
      if (nodeState.projected) {
        projectedFieldCount += inputs.get(nodeState.nodeIndex).getRowType().getFieldCount();
      }
    }
    // origin inputs order is [n0, n1, n2]
    // n0_projected [field0, field1]
    // n1_projected [field0, field1]
    // n2_projected [field0, field1]
    // rowType is [n0.field0, n0.field1, n1.field0, n1.field1, n2.field0, n2.field1]
    // assume that the original plan tree is:
    //        inner join
    //         /      \
    //   inner join    n2
    //    /      \
    //  n0        n1
    //
    // if the node order of the final result is [n2, n1, n0], rowType is
    // [n2.field0, n2.field1, n1.field0, n1.field1, n0.field0, n0.field1]
    // assume that the reordered plan tree is:
    //        inner join
    //         /      \
    //   inner join    n0
    //    /      \
    //  n2        n1
    //
    // we need a projection like [$4, $5, $2, $3, $0, $1]
    List<RexNode> projects = new ArrayList<>();
    for (int inputIndex = 0; inputIndex < inputs.size(); inputIndex++) {
      if (LongBitmap.isOverlap(notProjectInputs, LongBitmap.newBitmap(inputIndex))) {
        continue;
      }
      int fieldCount = inputs.get(inputIndex).getRowType().getFieldCount();
      for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
        Integer fieldOffset = nodeIndexToFieldCountBefore.get(inputIndex);
        if (fieldOffset == null) {
          throw new AssertionError(
              "The result order looses the " + inputIndex + "-th input");
        }
        int inputRef = fieldIndex + fieldOffset;
        projects.add(
            new RexInputRef(inputRef, rowTypeList.get(inputRef).getType()));
      }
    }
    return projects;
  }

  /**
   * Record the projection state of vertices in the hypergraph during enumerating. It is used to
   * calculate the index of RexInputRef when building join conditions from hyperedges.
   */
  public static class NodeState {
    final int nodeIndex;

    final boolean projected;

    NodeState(int nodeIndex, boolean projected) {
      this.nodeIndex = nodeIndex;
      this.projected = projected;
    }
  }

}
