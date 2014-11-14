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
package org.eigenbase.rel;

import java.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * A <code>CorrelatorRel</code> behaves like a kind of {@link JoinRel}, but
 * works by setting variables in its environment and restarting its right-hand
 * input.
 *
 * <p>A CorrelatorRel is used to represent a correlated query. One
 * implementation strategy is to de-correlate the expression.
 */
public final class CorrelatorRel extends JoinRelBase {
  //~ Instance fields --------------------------------------------------------

  protected final ImmutableList<Correlation> correlations;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a CorrelatorRel.
   *
   * @param cluster      cluster this relational expression belongs to
   * @param left         left input relational expression
   * @param right        right input relational expression
   * @param joinCond     join condition
   * @param correlations set of expressions to set as variables each time a
   *                     row arrives from the left input
   * @param joinType     join type
   */
  public CorrelatorRel(
      RelOptCluster cluster,
      RelNode left,
      RelNode right,
      RexNode joinCond,
      List<Correlation> correlations,
      JoinRelType joinType) {
    super(
        cluster,
        cluster.traitSetOf(Convention.NONE),
        left,
        right,
        joinCond,
        joinType,
        ImmutableSet.<String>of());
    this.correlations = ImmutableList.copyOf(correlations);
    assert (joinType == JoinRelType.LEFT)
        || (joinType == JoinRelType.INNER);
  }

  /**
   * Creates a CorrelatorRel with no join condition.
   *
   * @param cluster      cluster this relational expression belongs to
   * @param left         left input relational expression
   * @param right        right input relational expression
   * @param correlations set of expressions to set as variables each time a
   *                     row arrives from the left input
   * @param joinType     join type
   */
  public CorrelatorRel(
      RelOptCluster cluster,
      RelNode left,
      RelNode right,
      List<Correlation> correlations,
      JoinRelType joinType) {
    this(
        cluster,
        left,
        right,
        cluster.getRexBuilder().makeLiteral(true),
        correlations,
        joinType);
  }

  /**
   * Creates a CorrelatorRel by parsing serialized output.
   */
  public CorrelatorRel(RelInput input) {
    this(
        input.getCluster(), input.getInputs().get(0),
        input.getInputs().get(1), getCorrelations(input),
        input.getEnum("joinType", JoinRelType.class));
  }

  private static List<Correlation> getCorrelations(RelInput input) {
    final List<Correlation> list = new ArrayList<Correlation>();
    //noinspection unchecked
    final List<Map<String, Object>> correlations1 =
        (List<Map<String, Object>>) input.get("correlations");
    for (Map<String, Object> correlation : correlations1) {
      list.add(
          new Correlation(
              (Integer) correlation.get("correlation"),
              (Integer) correlation.get("offset")));
    }
    return list;
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public CorrelatorRel copy(RelTraitSet traitSet, RexNode conditionExpr,
      RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new CorrelatorRel(
        getCluster(),
        left,
        right,
        correlations,
        this.joinType);
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }

  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("correlations", correlations);
  }

  /**
   * Returns the correlating expressions.
   *
   * @return correlating expressions
   */
  public List<Correlation> getCorrelations() {
    return correlations;
  }
}

// End CorrelatorRel.java
