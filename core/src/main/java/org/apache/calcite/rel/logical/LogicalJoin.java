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
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

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
 *
 * <li>{@code net.sf.farrago.fennel.rel.FennelCartesianJoinRule}
 * implements a LogicalJoin as a cartesian product.
 *
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
   * @param cluster          Cluster
   * @param left             Left input
   * @param right            Right input
   * @param condition        Join condition
   * @param joinType         Join type
   * @param variablesStopped Set of names of variables which are set by the
   *                         LHS and used by the RHS and are not available to
   *                         nodes above this LogicalJoin in the tree
   */
  public LogicalJoin(
      RelOptCluster cluster,
      RelNode left,
      RelNode right,
      RexNode condition,
      JoinRelType joinType,
      Set<String> variablesStopped) {
    this(
        cluster,
        left,
        right,
        condition,
        joinType,
        variablesStopped,
        false,
        ImmutableList.<RelDataTypeField>of());
  }

  /**
   * Creates a LogicalJoin, flagged with whether it has been translated to a
   * semi-join.
   *
   * @param cluster          Cluster
   * @param left             Left input
   * @param right            Right input
   * @param condition        Join condition
   * @param joinType         Join type
   * @param variablesStopped Set of names of variables which are set by the
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
      RelNode left,
      RelNode right,
      RexNode condition,
      JoinRelType joinType,
      Set<String> variablesStopped,
      boolean semiJoinDone,
      ImmutableList<RelDataTypeField> systemFieldList) {
    super(
        cluster,
        cluster.traitSetOf(Convention.NONE),
        left,
        right,
        condition,
        joinType,
        variablesStopped);
    assert systemFieldList != null;
    this.semiJoinDone = semiJoinDone;
    this.systemFieldList = systemFieldList;
  }

  /**
   * Creates a LogicalJoin by parsing serialized output.
   */
  public LogicalJoin(RelInput input) {
    this(
        input.getCluster(), input.getInputs().get(0),
        input.getInputs().get(1), input.getExpression("condition"),
        input.getEnum("joinType", JoinRelType.class),
        ImmutableSet.<String>of(), false,
        ImmutableList.<RelDataTypeField>of());
  }

  //~ Methods ----------------------------------------------------------------

  @Override public LogicalJoin copy(RelTraitSet traitSet, RexNode conditionExpr,
      RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new LogicalJoin(
        getCluster(),
        left,
        right,
        conditionExpr,
        joinType,
        this.variablesStopped,
        semiJoinDone,
        this.systemFieldList);
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }

  public RelWriter explainTerms(RelWriter pw) {
    // Don't ever print semiJoinDone=false. This way, we
    // don't clutter things up in optimizers that don't use semi-joins.
    return super.explainTerms(pw)
        .itemIf("semiJoinDone", semiJoinDone, semiJoinDone);
  }

  @Override public boolean isSemiJoinDone() {
    return semiJoinDone;
  }

  public List<RelDataTypeField> getSystemFieldList() {
    return systemFieldList;
  }
}

// End LogicalJoin.java
