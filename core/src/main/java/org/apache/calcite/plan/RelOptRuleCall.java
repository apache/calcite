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
package org.apache.calcite.plan;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A <code>RelOptRuleCall</code> is an invocation of a {@link RelOptRule} with a
 * set of {@link RelNode relational expression}s as arguments.
 */
public abstract class RelOptRuleCall {
  //~ Static fields/initializers ---------------------------------------------

  protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  /**
   * Generator for {@link #id} values.
   */
  private static int nextId = 0;

  //~ Instance fields --------------------------------------------------------

  public final int id;
  protected final RelOptRuleOperand operand0;
  protected Map<RelNode, List<RelNode>> nodeInputs;
  public final RelOptRule rule;
  public final RelNode[] rels;
  private final RelOptPlanner planner;
  private final @Nullable List<RelNode> parents;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a RelOptRuleCall.
   *
   * @param planner      Planner
   * @param operand      Root operand
   * @param rels         Array of relational expressions which matched each
   *                     operand
   * @param nodeInputs   For each node which matched with
   *                     {@code matchAnyChildren}=true, a list of the node's
   *                     inputs
   * @param parents      list of parent RelNodes corresponding to the first
   *                     relational expression in the array argument, if known;
   *                     otherwise, null
   */
  protected RelOptRuleCall(
      RelOptPlanner planner,
      RelOptRuleOperand operand,
      RelNode[] rels,
      Map<RelNode, List<RelNode>> nodeInputs,
      @Nullable List<RelNode> parents) {
    this.id = nextId++;
    this.planner = planner;
    this.operand0 = operand;
    this.nodeInputs = nodeInputs;
    this.rule = operand.getRule();
    this.rels = rels;
    this.parents = parents;
    assert rels.length == rule.operands.size();
  }

  protected RelOptRuleCall(
      RelOptPlanner planner,
      RelOptRuleOperand operand,
      RelNode[] rels,
      Map<RelNode, List<RelNode>> nodeInputs) {
    this(planner, operand, rels, nodeInputs, null);
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns the root operand matched by this rule.
   *
   * @return root operand
   */
  public RelOptRuleOperand getOperand0() {
    return operand0;
  }

  /**
   * Returns the invoked planner rule.
   *
   * @return planner rule
   */
  public RelOptRule getRule() {
    return rule;
  }

  /**
   * Returns a list of matched relational expressions.
   *
   * @return matched relational expressions
   * @deprecated Use {@link #getRelList()} or {@link #rel(int)}
   */
  @Deprecated // to be removed before 2.0
  public RelNode[] getRels() {
    return rels;
  }

  /**
   * Returns a list of matched relational expressions.
   *
   * @return matched relational expressions
   * @see #rel(int)
   */
  public List<RelNode> getRelList() {
    return ImmutableList.copyOf(rels);
  }

  /**
   * Retrieves the {@code ordinal}th matched relational expression. This
   * corresponds to the {@code ordinal}th operand of the rule.
   *
   * @param ordinal Ordinal
   * @param <T>     Type
   * @return Relational expression
   */
  public <T extends RelNode> T rel(int ordinal) {
    //noinspection unchecked
    return (T) rels[ordinal];
  }

  /**
   * Returns the children of a given relational expression node matched in a
   * rule.
   *
   * <p>If the policy of the operand which caused the match is not
   * {@link org.apache.calcite.plan.RelOptRuleOperandChildPolicy#ANY},
   * the children will have their
   * own operands and therefore be easily available in the array returned by
   * the {@link #getRelList()} method, so this method returns null.
   *
   * <p>This method is for
   * {@link org.apache.calcite.plan.RelOptRuleOperandChildPolicy#ANY},
   * which is generally used when a node can have a variable number of
   * children, and hence where the matched children are not retrievable by any
   * other means.
   *
   * <p>Warning: it produces wrong result for {@code unordered(...)} case.
   *
   * @param rel Relational expression
   * @return Children of relational expression
   */
  public @Nullable List<RelNode> getChildRels(RelNode rel) {
    return nodeInputs.get(rel);
  }

  /** Assigns the input relational expressions of a given relational expression,
   * as seen by this particular call. Is only called when the operand is
   * {@link RelRule.OperandDetailBuilder#anyInputs() any}. */
  protected void setChildRels(RelNode rel, List<RelNode> inputs) {
    if (nodeInputs.isEmpty()) {
      nodeInputs = new HashMap<>();
    }
    nodeInputs.put(rel, inputs);
  }

  /**
   * Returns the planner.
   *
   * @return planner
   */
  public RelOptPlanner getPlanner() {
    return planner;
  }

  /**
   * Determines whether the rule is excluded by any root node hint.
   *
   * @return true iff rule should be excluded
   */
  public boolean isRuleExcluded() {
    for (RelNode rel : rels) {
      if (!(rel instanceof Hintable)) {
        continue;
      }
      if (rel.getCluster()
              .getHintStrategies()
              .isRuleExcluded((Hintable) rel, rule)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns the current RelMetadataQuery
   * to be used for instance by
   * {@link RelOptRule#onMatch(RelOptRuleCall)}.
   */
  public RelMetadataQuery getMetadataQuery() {
    return rel(0).getCluster().getMetadataQuery();
  }

  /**
   * Returns a list of parents of the first relational expression.
   */
  public @Nullable List<RelNode> getParents() {
    return parents;
  }

  /**
   * Registers that a rule has produced an equivalent relational expression.
   *
   * <p>Called by the rule whenever it finds a match. The implementation of
   * this method guarantees that the original relational expression (that is,
   * <code>this.rels[0]</code>) has its traits propagated to the new
   * relational expression (<code>rel</code>) and its unregistered children.
   * Any trait not specifically set in the RelTraitSet returned by <code>
   * rel.getTraits()</code> will be copied from <code>
   * this.rels[0].getTraitSet()</code>.
   *
   * <p>The hints of the root relational expression of
   * the rule call(<code>this.rels[0]</code>)
   * are copied to the new relational expression(<code>rel</code>)
   * with specified handler {@code handler}.
   *
   * @param rel     Relational expression equivalent to the root relational
   *                expression of the rule call, {@code call.rels(0)}
   * @param equiv   Map of other equivalences
   * @param handler Handler to customize the relational expression that registers
   *                into the planner, the first parameter is the root relational expression
   *                and the second parameter is the new relational expression
   */
  public abstract void transformTo(RelNode rel,
      Map<RelNode, RelNode> equiv,
      RelHintsPropagator handler);

  /**
   * Registers that a rule has produced an equivalent relational expression,
   * with specified equivalences.
   *
   * <p>The hints are copied with filter strategies from
   * the root relational expression of the rule call(<code>this.rels[0]</code>)
   * to the new relational expression(<code>rel</code>).
   *
   * @param rel   Relational expression equivalent to the root relational
   *              expression of the rule call, {@code call.rels(0)}
   * @param equiv Map of other equivalences
   */
  public void transformTo(RelNode rel, Map<RelNode, RelNode> equiv) {
    transformTo(rel, equiv, RelOptUtil::propagateRelHints);
  }

  /**
   * Registers that a rule has produced an equivalent relational expression,
   * but no other equivalences.
   *
   * <p>The hints are copied with filter strategies from
   * the root relational expression of the rule call(<code>this.rels[0]</code>)
   * to the new relational expression(<code>rel</code>).
   *
   * @param rel Relational expression equivalent to the root relational
   *            expression of the rule call, {@code call.rels(0)}
   */
  public final void transformTo(RelNode rel) {
    transformTo(rel, ImmutableMap.of());
  }

  /**
   * Registers that a rule has produced an equivalent relational expression,
   * but no other equivalences.
   *
   * <p>The hints of the root relational expression of
   * the rule call(<code>this.rels[0]</code>)
   * are copied to the new relational expression(<code>rel</code>)
   * with specified handler {@code handler}.
   *
   * @param rel     Relational expression equivalent to the root relational
   *                expression of the rule call, {@code call.rels(0)}
   * @param handler Handler to customize the relational expression that registers
   *                into the planner, the first parameter is the root relational expression
   *                and the second parameter is the new relational expression
   *
   */
  public final void transformTo(RelNode rel, RelHintsPropagator handler) {
    transformTo(rel, ImmutableMap.of(), handler);
  }

  /** Creates a {@link org.apache.calcite.tools.RelBuilder} to be used by
   * code within the call. The {@link RelOptRule#relBuilderFactory} argument contains policies
   * such as what implementation of {@link Filter} to create. */
  public RelBuilder builder() {
    return rule.relBuilderFactory.create(rel(0).getCluster(), null);
  }
}
