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
package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.AsofJoin;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Sub-class of {@link AsofJoin} encoding ASOF joins.
 * Adapted from the {@link LogicalJoin} implementation.
 */
public final class LogicalAsofJoin extends AsofJoin {
  //~ Instance fields --------------------------------------------------------

  private final ImmutableList<RelDataTypeField> systemFieldList;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a LogicalAsofJoin.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   *
   * @param cluster          Cluster
   * @param traitSet         Trait set
   * @param hints            Hints
   * @param left             Left input
   * @param right            Right input
   * @param condition        Join condition
   * @param matchCondition   Temporal condition
   * @param systemFieldList  List of system fields that will be prefixed to
   *                         output row type; typically empty but must not be null
   */
  public LogicalAsofJoin(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelNode left,
      RelNode right,
      RexNode condition,
      RexNode matchCondition,
      JoinRelType joinType,
      ImmutableList<RelDataTypeField> systemFieldList) {
    super(cluster, traitSet, hints, left, right,
        condition, matchCondition, ImmutableSet.of(), joinType);
    this.systemFieldList = requireNonNull(systemFieldList, "systemFieldList");
  }

  /**
   * Creates a LogicalAsofJoin by parsing serialized output.
   */
  public LogicalAsofJoin(RelInput input) {
    this(input.getCluster(), input.getCluster().traitSetOf(Convention.NONE),
        new ArrayList<>(),
        input.getInputs().get(0), input.getInputs().get(1),
        requireNonNull(input.getExpression("condition"), "condition"),
        requireNonNull(input.getExpression("matchCondition"), "matchCondition"),
        requireNonNull(input.getEnum("joinType", JoinRelType.class), "joinType"),
        ImmutableList.of());
  }

  /** Creates a LogicalAsofJoin. */
  public static LogicalAsofJoin create(RelNode left, RelNode right, List<RelHint> hints,
      RexNode condition, RexNode matchCondition,
      JoinRelType joinType,
      ImmutableList<RelDataTypeField> systemFieldList) {
    final RelOptCluster cluster = left.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalAsofJoin(cluster, traitSet, hints, left, right, condition, matchCondition,
        joinType, systemFieldList);
  }

  //~ Methods ----------------------------------------------------------------

  public LogicalAsofJoin copy(
      RelTraitSet traitSet, RexNode conditionExpr, RexNode matchConditionExpr,
      RelNode left, RelNode right) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new LogicalAsofJoin(getCluster(),
        getCluster().traitSetOf(Convention.NONE), hints, left, right, conditionExpr,
        matchConditionExpr, joinType, systemFieldList);
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }

  @Override public boolean deepEquals(@Nullable Object obj) {
    if (this == obj) {
      return true;
    }
    LogicalAsofJoin asofObj = requireNonNull((LogicalAsofJoin) obj);
    return deepEquals0(obj)
        && matchCondition.equals(asofObj.matchCondition)
        && systemFieldList.equals(asofObj.systemFieldList);
  }

  @Override public int deepHashCode() {
    return Objects.hash(deepHashCode0(), systemFieldList);
  }

  @Override public ImmutableList<RelDataTypeField> getSystemFieldList() {
    return systemFieldList;
  }

  @Override public Join copy(
      RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right,
      JoinRelType joinType, boolean semiJoinDone) {
    // This method does not provide the matchCondition as an argument, so it should never be called
    throw new RuntimeException("This method should not be called");
  }

  @Override public Join copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.size() == 2;
    return new LogicalAsofJoin(getCluster(), traitSet, hints,
        inputs.get(0), inputs.get(1),
        getCondition(), getMatchCondition(), joinType, systemFieldList);
  }

  @Override public RelNode withHints(List<RelHint> hintList) {
    return new LogicalAsofJoin(getCluster(), traitSet, hintList,
        left, right, condition, matchCondition, joinType, systemFieldList);
  }
}
