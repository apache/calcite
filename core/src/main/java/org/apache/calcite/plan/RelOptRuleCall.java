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
package org.eigenbase.relopt;

import java.util.*;
import java.util.logging.*;

import org.eigenbase.rel.*;
import org.eigenbase.trace.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * A <code>RelOptRuleCall</code> is an invocation of a {@link RelOptRule} with a
 * set of {@link RelNode relational expression}s as arguments.
 */
public abstract class RelOptRuleCall {
  //~ Static fields/initializers ---------------------------------------------

  protected static final Logger LOGGER = EigenbaseTrace.getPlannerTracer();

  /**
   * Generator for {@link #id} values.
   */
  private static int nextId = 0;

  //~ Instance fields --------------------------------------------------------

  public final int id;
  private final RelOptRuleOperand operand0;
  private final Map<RelNode, List<RelNode>> nodeChildren;
  public final RelOptRule rule;
  public final RelNode[] rels;
  private final RelOptPlanner planner;
  private final List<RelNode> parents;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a RelOptRuleCall.
   *
   * @param planner      Planner
   * @param operand      Root operand
   * @param rels         Array of relational expressions which matched each
   *                     operand
   * @param nodeChildren For each node which matched with <code>
   *                     matchAnyChildren</code>=true, a list of the node's
   *                     children
   * @param parents      list of parent RelNodes corresponding to the first
   *                     relational expression in the array argument, if known;
   *                     otherwise, null
   */
  protected RelOptRuleCall(
      RelOptPlanner planner,
      RelOptRuleOperand operand,
      RelNode[] rels,
      Map<RelNode, List<RelNode>> nodeChildren,
      List<RelNode> parents) {
    this.id = nextId++;
    this.planner = planner;
    this.operand0 = operand;
    this.nodeChildren = nodeChildren;
    this.rule = operand.getRule();
    this.rels = rels;
    this.parents = parents;
    assert rels.length == rule.operands.size();
  }

  protected RelOptRuleCall(
      RelOptPlanner planner,
      RelOptRuleOperand operand,
      RelNode[] rels,
      Map<RelNode, List<RelNode>> nodeChildren) {
    this(planner, operand, rels, nodeChildren, null);
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
   * {@link org.eigenbase.relopt.RelOptRuleOperandChildPolicy#ANY},
   * the children will have their
   * own operands and therefore be easily available in the array returned by
   * the {@link #getRels} method, so this method returns null.
   *
   * <p>This method is for
   * {@link org.eigenbase.relopt.RelOptRuleOperandChildPolicy#ANY},
   * which is generally used when a node can have a variable number of
   * children, and hence where the matched children are not retrievable by any
   * other means.
   *
   * @param rel Relational expression
   * @return Children of relational expression
   */
  public List<RelNode> getChildRels(RelNode rel) {
    return nodeChildren.get(rel);
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
   * @return list of parents of the first relational expression
   */
  public List<RelNode> getParents() {
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
   * @param rel   Relational expression equivalent to the root relational
   *              expression of the rule call, {@code call.rels(0)}
   * @param equiv Map of other equivalences
   */
  public abstract void transformTo(RelNode rel, Map<RelNode, RelNode> equiv);

  /**
   * Registers that a rule has produced an equivalent relational expression,
   * but no other equivalences.
   *
   * @param rel Relational expression equivalent to the root relational
   *            expression of the rule call, {@code call.rels(0)}
   */
  public final void transformTo(RelNode rel) {
    transformTo(rel, ImmutableMap.<RelNode, RelNode>of());
  }
}

// End RelOptRuleCall.java
