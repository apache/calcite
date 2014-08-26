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

import org.eigenbase.rel.RelNode;
import org.eigenbase.util.*;

import com.google.common.collect.ImmutableList;

/**
 * A <code>RelOptRuleOperand</code> determines whether a {@link
 * org.eigenbase.relopt.RelOptRule} can be applied to a particular expression.
 *
 * <p>For example, the rule to pull a filter up from the left side of a join
 * takes operands: <code>(Join (Filter) (Any))</code>.</p>
 *
 * <p>Note that <code>children</code> means different things if it is empty or
 * it is <code>null</code>: <code>(Join (Filter <b>()</b>) (Any))</code> means
 * that, to match the rule, <code>Filter</code> must have no operands.</p>
 */
public class RelOptRuleOperand {
  //~ Instance fields --------------------------------------------------------

  private RelOptRuleOperand parent;
  private RelOptRule rule;

  // REVIEW jvs 29-Aug-2004: some of these are Volcano-specific and should be
  // factored out
  public int[] solveOrder;
  public int ordinalInParent;
  public int ordinalInRule;
  private final RelTrait trait;
  private final Class<? extends RelNode> clazz;
  private final ImmutableList<RelOptRuleOperand> children;

  /**
   * Whether child operands can be matched in any order.
   */
  public final RelOptRuleOperandChildPolicy childPolicy;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an operand.
   *
   * <p>The {@code childOperands} argument is often populated by calling one
   * of the following methods:
   * {@link RelOptRule#some},
   * {@link RelOptRule#none()},
   * {@link RelOptRule#any},
   * {@link RelOptRule#unordered},
   * See {@link org.eigenbase.relopt.RelOptRuleOperandChildren} for more
   * details.</p>
   *
   * @param clazz    Class of relational expression to match (must not be null)
   * @param trait    Trait to match, or null to match any trait
   * @param children Child operands
   */
  protected RelOptRuleOperand(
      Class<? extends RelNode> clazz,
      RelTrait trait,
      RelOptRuleOperandChildren children) {
    assert clazz != null;
    switch (children.policy) {
    case ANY:
      break;
    case LEAF:
      assert children.operands.size() == 0;
      break;
    case UNORDERED:
      assert children.operands.size() == 1;
      break;
    default:
      assert children.operands.size() > 0;
    }
    this.childPolicy = children.policy;
    this.clazz = clazz;
    this.trait = trait;
    this.children = children.operands;
    for (RelOptRuleOperand child : this.children) {
      assert child.parent == null : "cannot re-use operands";
      child.parent = this;
    }
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns the parent operand.
   *
   * @return parent operand
   */
  public RelOptRuleOperand getParent() {
    return parent;
  }

  /**
   * Sets the parent operand.
   *
   * @param parent Parent operand
   */
  public void setParent(RelOptRuleOperand parent) {
    this.parent = parent;
  }

  /**
   * Returns the rule this operand belongs to.
   *
   * @return containing rule
   */
  public RelOptRule getRule() {
    return rule;
  }

  /**
   * Sets the rule this operand belongs to
   *
   * @param rule containing rule
   */
  public void setRule(RelOptRule rule) {
    this.rule = rule;
  }

  public int hashCode() {
    int h = clazz.hashCode();
    h = Util.hash(h, trait);
    h = Util.hash(h, children);
    return h;
  }

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof RelOptRuleOperand)) {
      return false;
    }
    RelOptRuleOperand that = (RelOptRuleOperand) obj;

    return (this.clazz == that.clazz)
        && Util.equal(this.trait, that.trait)
        && this.children.equals(that.children);
  }

  /**
   * @return relational expression class matched by this operand
   */
  public Class<? extends RelNode> getMatchedClass() {
    return clazz;
  }

  /**
   * Returns the child operands.
   *
   * @return child operands
   */
  public List<RelOptRuleOperand> getChildOperands() {
    return children;
  }

  /**
   * Returns whether a relational expression matches this operand. It must be
   * of the right class and trait.
   */
  public boolean matches(RelNode rel) {
    if (!clazz.isInstance(rel)) {
      return false;
    }
    if ((trait != null) && !rel.getTraitSet().contains(trait)) {
      return false;
    }
    return true;
  }
}

// End RelOptRuleOperand.java
