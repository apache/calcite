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
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.Join}
 * not targeted at any particular engine or calling convention.
 *
 * <p>Some rules:
 *
 * <ul>
 * <li>{@link org.apache.calcite.rel.rules.JoinExtractFilterRule} converts an
 * {@link LogicalJoin inner join} to a {@link LogicalFilter filter} on top of a
 * {@link LogicalJoin cartesian inner join}.
 * </ul>
 */
public final class LogicalJoin extends Join {
  //~ Instance fields --------------------------------------------------------

  // NOTE jvs 14-Mar-2006:  Normally we don't use state like this
  // to control rule firing, but due to the non-local nature of
  // semijoin optimizations, it's pretty much required.
  private final boolean semiJoinDone;

  private final ImmutableList<RelDataTypeField> systemFieldList;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a LogicalJoin.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   *
   * @param cluster          Cluster
   * @param traitSet         Trait set
   * @param hints            Hints
   * @param left             Left input
   * @param right            Right input
   * @param condition        Join condition
   * @param joinType         Join type
   * @param variablesSet     Set of variables that are set by the
   *                         LHS and used by the RHS and are not available to
   *                         nodes above this LogicalJoin in the tree
   * @param semiJoinDone     Whether this join has been translated to a
   *                         semi-join
   * @param systemFieldList  List of system fields that will be prefixed to
   *                         output row type; typically empty but must not be
   *                         null
   * @see #isSemiJoinDone()
   */
  public LogicalJoin(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelNode left,
      RelNode right,
      RexNode condition,
      Set<CorrelationId> variablesSet,
      JoinRelType joinType,
      boolean semiJoinDone,
      ImmutableList<RelDataTypeField> systemFieldList) {
    super(cluster, traitSet, hints, left, right, condition, variablesSet, joinType);
    this.semiJoinDone = semiJoinDone;
    this.systemFieldList = requireNonNull(systemFieldList, "systemFieldList");
  }

  @Deprecated // to be removed before 2.0
  public LogicalJoin(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode left, RelNode right, RexNode condition, Set<CorrelationId> variablesSet,
      JoinRelType joinType, boolean semiJoinDone,
      ImmutableList<RelDataTypeField> systemFieldList) {
    this(cluster, traitSet, ImmutableList.of(), left, right, condition,
        variablesSet, joinType, semiJoinDone, systemFieldList);
  }

  @Deprecated // to be removed before 2.0
  public LogicalJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left,
      RelNode right, RexNode condition, JoinRelType joinType,
      Set<String> variablesStopped, boolean semiJoinDone,
      ImmutableList<RelDataTypeField> systemFieldList) {
    this(cluster, traitSet, ImmutableList.of(), left, right, condition,
        CorrelationId.setOf(variablesStopped), joinType, semiJoinDone,
        systemFieldList);
  }

  @Deprecated // to be removed before 2.0
  public LogicalJoin(RelOptCluster cluster, RelNode left, RelNode right,
      RexNode condition, JoinRelType joinType, Set<String> variablesStopped) {
    this(cluster, cluster.traitSetOf(Convention.NONE), ImmutableList.of(),
        left, right, condition, CorrelationId.setOf(variablesStopped),
        joinType, false, ImmutableList.of());
  }

  @Deprecated // to be removed before 2.0
  public LogicalJoin(RelOptCluster cluster, RelNode left, RelNode right,
      RexNode condition, JoinRelType joinType, Set<String> variablesStopped,
      boolean semiJoinDone, ImmutableList<RelDataTypeField> systemFieldList) {
    this(cluster, cluster.traitSetOf(Convention.NONE), ImmutableList.of(),
        left, right, condition, CorrelationId.setOf(variablesStopped), joinType,
        semiJoinDone, systemFieldList);
  }

  /**
   * Creates a LogicalJoin by parsing serialized output.
   */
  public LogicalJoin(RelInput input) {
    this(input.getCluster(), input.getCluster().traitSetOf(Convention.NONE),
        ImmutableList.of(),
        input.getInputs().get(0), input.getInputs().get(1),
        requireNonNull(input.getExpression("condition"), "condition"),
        ImmutableSet.of(),
        requireNonNull(input.getEnum("joinType", JoinRelType.class), "joinType"),
        false,
        ImmutableList.of());
  }

  /** Creates a LogicalJoin. */
  public static LogicalJoin create(RelNode left, RelNode right, List<RelHint> hints,
      RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
    return create(left, right, hints, condition, variablesSet, joinType, false,
        ImmutableList.of());
  }

  /** Creates a LogicalJoin, flagged with whether it has been translated to a
   * semi-join. */
  public static LogicalJoin create(RelNode left, RelNode right, List<RelHint> hints,
      RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType,
      boolean semiJoinDone, ImmutableList<RelDataTypeField> systemFieldList) {
    final RelOptCluster cluster = left.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalJoin(cluster, traitSet, hints, left, right, condition,
        variablesSet, joinType, semiJoinDone, systemFieldList);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public LogicalJoin copy(RelTraitSet traitSet, RexNode conditionExpr,
      RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new LogicalJoin(getCluster(),
        getCluster().traitSetOf(Convention.NONE), hints, left, right, conditionExpr,
        variablesSet, joinType, semiJoinDone, systemFieldList);
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    // Don't ever print semiJoinDone=false. This way, we
    // don't clutter things up in optimizers that don't use semi-joins.
    return super.explainTerms(pw)
        .itemIf("semiJoinDone", semiJoinDone, semiJoinDone);
  }

  @Override public boolean deepEquals(@Nullable Object obj) {
    if (this == obj) {
      return true;
    }
    return deepEquals0(obj)
        && semiJoinDone == ((LogicalJoin) obj).semiJoinDone
        && systemFieldList.equals(((LogicalJoin) obj).systemFieldList);
  }

  @Override public int deepHashCode() {
    return Objects.hash(deepHashCode0(), semiJoinDone, systemFieldList);
  }

  @Override public boolean isSemiJoinDone() {
    return semiJoinDone;
  }

  @Override public List<RelDataTypeField> getSystemFieldList() {
    return systemFieldList;
  }

  @Override public RelNode withHints(List<RelHint> hintList) {
    return new LogicalJoin(getCluster(), traitSet, hintList,
        left, right, condition, variablesSet, joinType, semiJoinDone, systemFieldList);
  }
}
