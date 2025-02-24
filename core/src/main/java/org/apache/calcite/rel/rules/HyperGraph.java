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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBiVisitor;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVariable;
import org.apache.calcite.rex.RexVisitor;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * HyperGraph represents a join graph.
 */
public class HyperGraph extends AbstractRelNode {

  private final List<RelNode> inputs;

  @SuppressWarnings("HidingField")
  private final RelDataType rowType;

  private final List<HyperEdge> edges;

  // key is the bitmap for inputs, value is the hyper edge bitmap in edges
  private final HashMap<Long, BitSet> simpleEdgesMap;

  private final HashMap<Long, BitSet> complexEdgesMap;

  // node bitmap overlaps edge's leftNodeBits or rightNodeBits, but does not completely cover
  private final HashMap<Long, BitSet> overlapEdgesMap;

  protected HyperGraph(RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelNode> inputs,
      List<HyperEdge> edges,
      RelDataType rowType) {
    super(cluster, traitSet);
    this.inputs = inputs;
    this.edges = edges;
    this.rowType = rowType;
    this.simpleEdgesMap = new HashMap<>();
    this.complexEdgesMap = new HashMap<>();
    this.overlapEdgesMap = new HashMap<>();
  }

  protected HyperGraph(RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelNode> inputs,
      List<HyperEdge> edges,
      RelDataType rowType,
      HashMap<Long, BitSet> simpleEdgesMap,
      HashMap<Long, BitSet> complexEdgesMap,
      HashMap<Long, BitSet> overlapEdgesMap) {
    super(cluster, traitSet);
    this.inputs = inputs;
    this.edges = edges;
    this.rowType = rowType;
    this.simpleEdgesMap = simpleEdgesMap;
    this.complexEdgesMap = complexEdgesMap;
    this.overlapEdgesMap = overlapEdgesMap;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new HyperGraph(
        getCluster(),
        traitSet,
        inputs,
        edges,
        rowType,
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

  //~ hyper graph method ----------------------------------------------------------

  public List<HyperEdge> getEdges() {
    return edges;
  }

  public long getNeighborBitmap(long csg, long forbidden) {
    long neighbors = 0L;
    List<HyperEdge> simpleEdges = simpleEdgesMap.getOrDefault(csg, new BitSet()).stream()
        .mapToObj(edges::get)
        .collect(Collectors.toList());
    for (HyperEdge edge : simpleEdges) {
      neighbors |= edge.getNodeBitmap();
    }

    forbidden = forbidden | csg;
    neighbors = neighbors & ~forbidden;
    forbidden = forbidden | neighbors;

    List<HyperEdge> complexEdges = complexEdgesMap.getOrDefault(csg, new BitSet()).stream()
        .mapToObj(edges::get)
        .collect(Collectors.toList());
    for (HyperEdge edge : complexEdges) {
      long leftBitmap = edge.getLeftNodeBitmap();
      long rightBitmap = edge.getRightNodeBitmap();
      if (LongBitmap.isSubSet(leftBitmap, csg) && !LongBitmap.isOverlap(rightBitmap, forbidden)) {
        neighbors |= Long.lowestOneBit(rightBitmap);
      } else if (LongBitmap.isSubSet(rightBitmap, csg)
          && !LongBitmap.isOverlap(leftBitmap, forbidden)) {
        neighbors |= Long.lowestOneBit(leftBitmap);
      }
    }
    return neighbors;
  }

  public List<HyperEdge> connectCsgCmp(long csg, long cmp) {
    checkArgument(simpleEdgesMap.containsKey(csg));
    checkArgument(simpleEdgesMap.containsKey(cmp));
    List<HyperEdge> connectedEdges = new ArrayList<>();
    BitSet connectedEdgesBitmap = new BitSet();
    connectedEdgesBitmap.or(simpleEdgesMap.getOrDefault(csg, new BitSet()));
    connectedEdgesBitmap.or(complexEdgesMap.getOrDefault(csg, new BitSet()));
    connectedEdgesBitmap.or(overlapEdgesMap.getOrDefault(csg, new BitSet()));

    BitSet cmpEdgesBitmap = new BitSet();
    cmpEdgesBitmap.or(simpleEdgesMap.getOrDefault(cmp, new BitSet()));
    cmpEdgesBitmap.or(complexEdgesMap.getOrDefault(cmp, new BitSet()));
    cmpEdgesBitmap.or(overlapEdgesMap.getOrDefault(cmp, new BitSet()));

    connectedEdgesBitmap.and(cmpEdgesBitmap);
    connectedEdgesBitmap.stream()
        .filter(index -> LongBitmap.isSubSet(edges.get(index).getNodeBitmap(), csg | cmp))
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
    boolean isLeftEnd = LongBitmap.isSubSet(edge.getLeftNodeBitmap(), subset)
        && !LongBitmap.isOverlap(edge.getRightNodeBitmap(), subset);
    boolean isRightEnd = LongBitmap.isSubSet(edge.getRightNodeBitmap(), subset)
        && !LongBitmap.isOverlap(edge.getLeftNodeBitmap(), subset);
    return isLeftEnd || isRightEnd;
  }

  private static boolean isOverlapEdge(HyperEdge edge, long subset) {
    boolean isLeftEnd = LongBitmap.isOverlap(edge.getLeftNodeBitmap(), subset)
        && !LongBitmap.isOverlap(edge.getRightNodeBitmap(), subset);
    boolean isRightEnd = LongBitmap.isOverlap(edge.getRightNodeBitmap(), subset)
        && !LongBitmap.isOverlap(edge.getLeftNodeBitmap(), subset);
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

  public RexNode extractJoinCond(RelNode left, RelNode right, List<HyperEdge> edges) {
    List<RexNode> joinConds = new ArrayList<>();
    List<RelDataTypeField> fieldList = new ArrayList<>(left.getRowType().getFieldList());
    fieldList.addAll(right.getRowType().getFieldList());

    List<String> names = new ArrayList<>(left.getRowType().getFieldNames());
    names.addAll(right.getRowType().getFieldNames());

    // convert the HyperEdge's condition from RexInputFieldName to RexInputRef
    RexShuttle inputName2InputRefShuttle = new RexShuttle() {
      @Override protected List<RexNode> visitList(
          List<? extends RexNode> exprs,
          boolean @Nullable [] update) {
        ImmutableList.Builder<RexNode> clonedOperands = ImmutableList.builder();
        for (RexNode operand : exprs) {
          RexNode clonedOperand;
          if (operand instanceof RexInputFieldName) {
            int index = names.indexOf(((RexInputFieldName) operand).getName());
            clonedOperand = new RexInputRef(index, fieldList.get(index).getType());
          } else {
            clonedOperand = operand.accept(this);
          }
          if ((clonedOperand != operand) && (update != null)) {
            update[0] = true;
          }
          clonedOperands.add(clonedOperand);
        }
        return clonedOperands.build();
      }
    };

    for (HyperEdge edge : edges) {
      RexNode inputRefCond = edge.getCondition().accept(inputName2InputRefShuttle);
      joinConds.add(inputRefCond);
    }
    return RexUtil.composeConjunction(left.getCluster().getRexBuilder(), joinConds);
  }

  /**
   * Before starting enumeration, add Project on every input, make all field name unique.
   * Convert the HyperEdge condition from RexInputRef to RexInputFieldName
   */
  public void convertHyperEdgeCond() {
    int fieldIndex = 0;
    List<RelDataTypeField> fieldList = rowType.getFieldList();
    for (int nodeIndex = 0; nodeIndex < inputs.size(); nodeIndex++) {
      RelNode input = inputs.get(nodeIndex);
      List<RexNode> projects = new ArrayList<>();
      List<String> names = new ArrayList<>();
      for (int i = 0; i < input.getRowType().getFieldCount(); i++) {
        projects.add(
            new RexInputRef(
            i,
            fieldList.get(fieldIndex).getType()));
        names.add(fieldList.get(fieldIndex).getName());
        fieldIndex++;
      }
      RelNode renameProject =
          LogicalProject.create(
              input,
              ImmutableList.of(),
              projects,
              names,
              input.getVariablesSet());
      replaceInput(nodeIndex, renameProject);
    }

    RexShuttle inputRef2inputNameShuttle = new RexShuttle() {
      @Override public RexNode visitInputRef(RexInputRef inputRef) {
        int index = inputRef.getIndex();
        return new RexInputFieldName(
            fieldList.get(index).getName(),
            fieldList.get(index).getType());
      }
    };

    for (HyperEdge hyperEdge : edges) {
      RexNode convertCond = hyperEdge.getCondition().accept(inputRef2inputNameShuttle);
      hyperEdge.replaceCondition(convertCond);
    }
  }

  /**
   * Adjusting RexInputRef in enumeration process is too complicated,
   * so use unique name replace input ref.
   * Before starting enumeration, convert RexInputRef to RexInputFieldName.
   * When connect csgcmp to Join, convert RexInputFieldName to RexInputRef.
   */
  private static class RexInputFieldName extends RexVariable {

    RexInputFieldName(final String fieldName, final RelDataType type) {
      super(fieldName, type);
    }

    @Override public <R> R accept(RexVisitor<R> visitor) {
      throw new UnsupportedOperationException();
    }

    @Override public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean equals(@Nullable Object obj) {
      return this == obj
          || obj instanceof RexInputFieldName
          && name == ((RexInputFieldName) obj).name
          && type.equals(((RexInputFieldName) obj).type);
    }

    @Override public int hashCode() {
      return name.hashCode();
    }

    @Override public String toString() {
      return name;
    }
  }
}
