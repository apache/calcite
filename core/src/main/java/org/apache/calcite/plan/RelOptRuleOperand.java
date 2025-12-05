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

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.initialization.qual.NotOnlyInitialized;
import org.checkerframework.checker.initialization.qual.UnknownInitialization;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;

import static java.util.Objects.requireNonNull;

/**
 * Operand that determines whether a {@link RelOptRule}
 * can be applied to a particular expression.
 *
 * <p>For example, the rule to pull a filter up from the left side of a join
 * takes operands: <code>Join(Filter, Any)</code>.
 *
 * <p>Note that <code>children</code> means different things if it is empty or
 * it is <code>null</code>: <code>Join(Filter <b>()</b>, Any)</code> means
 * that, to match the rule, <code>Filter</code> must have no operands.
 */
public class RelOptRuleOperand {
  //~ Instance fields --------------------------------------------------------

  private @Nullable RelOptRuleOperand parent;
  private @NotOnlyInitialized RelOptRule rule;
  private final Predicate<RelNode> predicate;

  // REVIEW jvs 29-Aug-2004: some of these are Volcano-specific and should be
  // factored out
  public int @MonotonicNonNull [] solveOrder;
  public int ordinalInParent;
  public int ordinalInRule;
  public final @Nullable RelTrait trait;
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
   * See {@link org.apache.calcite.plan.RelOptRuleOperandChildren} for more
   * details.
   *
   * @param clazz    Class of relational expression to match (must not be null)
   * @param trait    Trait to match, or null to match any trait
   * @param predicate Predicate to apply to relational expression
   * @param children Child operands
   *
   * @deprecated Use
   * {@link RelOptRule#operand(Class, RelOptRuleOperandChildren)} or one of its
   * overloaded methods.
   */
  @Deprecated // to be removed before 2.0; see [CALCITE-1166]
  protected <R extends RelNode> RelOptRuleOperand(
      Class<R> clazz,
      RelTrait trait,
      Predicate<? super R> predicate,
      RelOptRuleOperandChildren children) {
    this(clazz, trait, predicate, children.policy, children.operands);
  }

  /** Private constructor.
   *
   * <p>Do not call from outside package, and do not create a sub-class.
   *
   * <p>The other constructor is deprecated; when it is removed, make fields
   * {@link #parent}, {@link #ordinalInParent} and {@link #solveOrder} final,
   * and add constructor parameters for them. See
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1166">[CALCITE-1166]
   * Disallow sub-classes of RelOptRuleOperand</a>. */
  @SuppressWarnings({"initialization.fields.uninitialized",
      "initialization.invalid.field.write.initialized", "unchecked"})
  <R extends RelNode> RelOptRuleOperand(
      Class<R> clazz,
      @Nullable RelTrait trait,
      Predicate<? super R> predicate,
      RelOptRuleOperandChildPolicy childPolicy,
      ImmutableList<RelOptRuleOperand> children) {
    this.clazz = requireNonNull(clazz, "clazz");
    switch (childPolicy) {
    case ANY:
      break;
    case LEAF:
      checkArgument(children.isEmpty());
      break;
    case UNORDERED:
      assert children.size() == 1;
      break;
    default:
      checkArgument(!children.isEmpty());
    }
    this.childPolicy = requireNonNull(childPolicy, "childPolicy");
    this.trait = trait;
    this.predicate = requireNonNull((Predicate<RelNode>) predicate);
    this.children = requireNonNull(children, "children");
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
  public @Nullable RelOptRuleOperand getParent() {
    return parent;
  }

  /**
   * Sets the parent operand.
   *
   * @param parent Parent operand
   */
  public void setParent(@Nullable RelOptRuleOperand parent) {
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
   * Sets the rule this operand belongs to.
   *
   * @param rule containing rule
   */
  @SuppressWarnings("initialization.invalid.field.write.initialized")
  public void setRule(@UnknownInitialization RelOptRule rule) {
    this.rule = rule;
  }

  @Override public int hashCode() {
    return Objects.hash(clazz, trait, children);
  }

  @Override public boolean equals(@Nullable Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof RelOptRuleOperand)) {
      return false;
    }
    RelOptRuleOperand that = (RelOptRuleOperand) obj;

    return (this.clazz == that.clazz)
        && Objects.equals(this.trait, that.trait)
        && this.children.equals(that.children);
  }

  /**
   * <b>FOR DEBUG ONLY.</b>
   *
   * <p>To facilitate IDE shows the operand description in the debugger,
   * returns the root operand description, but highlight current
   * operand's matched class with '*' in the description.
   *
   * <p>e.g. The following are examples of rule operand description for
   * the operands that match with {@code LogicalFilter}.
   *
   * <ul>
   * <li>SemiJoinRule:project: Project(Join(*RelNode*, Aggregate))</li>
   * <li>ProjectFilterTransposeRule: LogicalProject(*LogicalFilter*)</li>
   * <li>FilterProjectTransposeRule: *Filter*(Project)</li>
   * <li>ReduceExpressionsRule(Filter): *LogicalFilter*</li>
   * <li>PruneEmptyJoin(right): Join(*RelNode*, Values)</li>
   * </ul>
   *
   * @see #describeIt(RelOptRuleOperand)
   */
  @Override public String toString() {
    RelOptRuleOperand root = this;
    while (root.parent != null) {
      root = root.parent;
    }
    StringBuilder s = root.describeIt(this);
    return s.toString();
  }

  /**
   * Returns this rule operand description, and highlight the operand's
   * class name with '*' if {@code that} operand equals current operand.
   *
   * @param that The rule operand that needs to be highlighted
   * @return The string builder that describes this rule operand
   * @see #toString()
   */
  private StringBuilder describeIt(RelOptRuleOperand that) {
    StringBuilder s = new StringBuilder();
    if (parent == null) {
      s.append(rule).append(": ");
    }
    if (this == that) {
      s.append('*');
    }
    s.append(clazz.getSimpleName());
    if (this == that) {
      s.append('*');
    }
    if (!children.isEmpty()) {
      s.append('(');
      boolean first = true;
      for (RelOptRuleOperand child : children) {
        if (!first) {
          s.append(", ");
        }
        s.append(child.describeIt(that));
        first = false;
      }
      s.append(')');
    }
    return s;
  }

  /**
   * Returns relational expression class matched by this operand.
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
    return predicate.test(rel);
  }
}
