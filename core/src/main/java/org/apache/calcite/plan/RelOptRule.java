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
import org.apache.calcite.rel.convert.Converter;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * A <code>RelOptRule</code> transforms an expression into another. It has a
 * list of {@link RelOptRuleOperand}s, which determine whether the rule can be
 * applied to a particular section of the tree.
 *
 * <p>The optimizer figures out which rules are applicable, then calls
 * {@link #onMatch} on each of them.</p>
 */
public abstract class RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  //~ Instance fields --------------------------------------------------------

  /**
   * Description of rule, must be unique within planner. Default is the name
   * of the class sans package name, but derived classes are encouraged to
   * override.
   */
  protected final String description;

  /**
   * Root of operand tree.
   */
  private final RelOptRuleOperand operand;

  /** Factory for a builder for relational expressions.
   *
   * <p>The actual builder is available via {@link RelOptRuleCall#builder()}. */
  public final RelBuilderFactory relBuilderFactory;

  /**
   * Flattened list of operands.
   */
  public final List<RelOptRuleOperand> operands;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a rule.
   *
   * @param operand root operand, must not be null
   */
  public RelOptRule(RelOptRuleOperand operand) {
    this(operand, RelFactories.LOGICAL_BUILDER, null);
  }

  /**
   * Creates a rule with an explicit description.
   *
   * @param operand     root operand, must not be null
   * @param description Description, or null to guess description
   */
  public RelOptRule(RelOptRuleOperand operand, String description) {
    this(operand, RelFactories.LOGICAL_BUILDER, description);
  }

  /**
   * Creates a rule with an explicit description.
   *
   * @param operand     root operand, must not be null
   * @param description Description, or null to guess description
   * @param relBuilderFactory Builder for relational expressions
   */
  public RelOptRule(RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory, String description) {
    this.operand = Objects.requireNonNull(operand);
    this.relBuilderFactory = Objects.requireNonNull(relBuilderFactory);
    if (description == null) {
      description = guessDescription(getClass().getName());
    }
    if (!description.matches("[A-Za-z][-A-Za-z0-9_.(),\\[\\]\\s:]*")) {
      throw new RuntimeException("Rule description '" + description
          + "' is not valid");
    }
    this.description = description;
    this.operands = flattenOperands(operand);
    assignSolveOrder();
  }

  //~ Methods for creating operands ------------------------------------------

  /**
   * Creates an operand that matches a relational expression that has no
   * children.
   *
   * @param clazz Class of relational expression to match (must not be null)
   * @param operandList Child operands
   * @param <R> Class of relational expression to match
   * @return Operand that matches a relational expression that has no
   *   children
   */
  public static <R extends RelNode> RelOptRuleOperand operand(
      Class<R> clazz,
      RelOptRuleOperandChildren operandList) {
    return new RelOptRuleOperand(clazz, null, r -> true,
        operandList.policy, operandList.operands);
  }

  /**
   * Creates an operand that matches a relational expression that has no
   * children.
   *
   * @param clazz Class of relational expression to match (must not be null)
   * @param trait Trait to match, or null to match any trait
   * @param operandList Child operands
   * @param <R> Class of relational expression to match
   * @return Operand that matches a relational expression that has no
   *   children
   */
  public static <R extends RelNode> RelOptRuleOperand operand(
      Class<R> clazz,
      RelTrait trait,
      RelOptRuleOperandChildren operandList) {
    return new RelOptRuleOperand(clazz, trait, r -> true,
        operandList.policy, operandList.operands);
  }

  /**
   * Creates an operand that matches a relational expression that has a
   * particular trait and predicate.
   *
   * @param clazz Class of relational expression to match (must not be null)
   * @param trait Trait to match, or null to match any trait
   * @param predicate Additional match predicate
   * @param operandList Child operands
   * @param <R> Class of relational expression to match
   * @return Operand that matches a relational expression that has a
   *   particular trait and predicate
   */
  public static <R extends RelNode> RelOptRuleOperand operandJ(
      Class<R> clazz,
      RelTrait trait,
      Predicate<? super R> predicate,
      RelOptRuleOperandChildren operandList) {
    return new RelOptRuleOperand(clazz, trait, predicate, operandList.policy,
        operandList.operands);
  }

  /** @deprecated Use {@link #operandJ} */
  @SuppressWarnings("Guava")
  @Deprecated // to be removed before 2.0
  public static <R extends RelNode> RelOptRuleOperand operand(
      Class<R> clazz,
      RelTrait trait,
      com.google.common.base.Predicate<? super R> predicate,
      RelOptRuleOperandChildren operandList) {
    return operandJ(clazz, trait, (Predicate<? super R>) predicate::apply,
        operandList);
  }

  /**
   * Creates an operand that matches a relational expression that has no
   * children.
   *
   * @param clazz Class of relational expression to match (must not be null)
   * @param trait Trait to match, or null to match any trait
   * @param predicate Additional match predicate
   * @param first First operand
   * @param rest Rest operands
   * @param <R> Class of relational expression to match
   * @return Operand
   */
  public static <R extends RelNode> RelOptRuleOperand operandJ(
      Class<R> clazz,
      RelTrait trait,
      Predicate<? super R> predicate,
      RelOptRuleOperand first,
      RelOptRuleOperand... rest) {
    return operandJ(clazz, trait, predicate, some(first, rest));
  }

  @SuppressWarnings("Guava")
  @Deprecated // to be removed before 2.0
  public static <R extends RelNode> RelOptRuleOperand operand(
      Class<R> clazz,
      RelTrait trait,
      com.google.common.base.Predicate<? super R> predicate,
      RelOptRuleOperand first,
      RelOptRuleOperand... rest) {
    return operandJ(clazz, trait, (Predicate<? super R>) predicate::apply,
        first, rest);
  }

  /**
   * Creates an operand that matches a relational expression with a given
   * list of children.
   *
   * <p>Shorthand for <code>operand(clazz, some(...))</code>.
   *
   * <p>If you wish to match a relational expression that has no children
   * (that is, a leaf node), write <code>operand(clazz, none())</code></p>.
   *
   * <p>If you wish to match a relational expression that has any number of
   * children, write <code>operand(clazz, any())</code></p>.
   *
   * @param clazz Class of relational expression to match (must not be null)
   * @param first First operand
   * @param rest Rest operands
   * @param <R> Class of relational expression to match
   * @return Operand that matches a relational expression with a given
   *   list of children
   */
  public static <R extends RelNode> RelOptRuleOperand operand(
      Class<R> clazz,
      RelOptRuleOperand first,
      RelOptRuleOperand... rest) {
    return operand(clazz, some(first, rest));
  }

  /**
   * Creates an operand for a converter rule.
   *
   * @param clazz    Class of relational expression to match (must not be null)
   * @param trait    Trait to match, or null to match any trait
   * @param predicate Predicate to apply to relational expression
   */
  protected static <R extends RelNode> ConverterRelOptRuleOperand
      convertOperand(Class<R> clazz, Predicate<? super R> predicate,
      RelTrait trait) {
    return new ConverterRelOptRuleOperand(clazz, trait, predicate);
  }

  /** @deprecated Use {@link #convertOperand(Class, Predicate, RelTrait)}. */
  @SuppressWarnings("Guava")
  @Deprecated // to be removed before 2.0
  protected static <R extends RelNode> ConverterRelOptRuleOperand
      convertOperand(Class<R> clazz,
      com.google.common.base.Predicate<? super R> predicate,
      RelTrait trait) {
    return new ConverterRelOptRuleOperand(clazz, trait, predicate::apply);
  }

  //~ Methods for creating lists of child operands ---------------------------

  /**
   * Creates a list of child operands that matches child relational
   * expressions in the order they appear.
   *
   * @param first First child operand
   * @param rest  Remaining child operands (may be empty)
   * @return List of child operands that matches child relational
   *   expressions in the order
   */
  public static RelOptRuleOperandChildren some(
      RelOptRuleOperand first,
      RelOptRuleOperand... rest) {
    return new RelOptRuleOperandChildren(RelOptRuleOperandChildPolicy.SOME,
        Lists.asList(first, rest));
  }


  /**
   * Creates a list of child operands that matches child relational
   * expressions in any order.
   *
   * <p>This is useful when matching a relational expression which
   * can have a variable number of children. For example, the rule to
   * eliminate empty children of a Union would have operands</p>
   *
   * <blockquote>Operand(Union, true, Operand(Empty))</blockquote>
   *
   * <p>and given the relational expressions</p>
   *
   * <blockquote>Union(LogicalFilter, Empty, LogicalProject)</blockquote>
   *
   * <p>would fire the rule with arguments</p>
   *
   * <blockquote>{Union, Empty}</blockquote>
   *
   * <p>It is up to the rule to deduce the other children, or indeed the
   * position of the matched child.</p>
   *
   * @param first First child operand
   * @param rest  Remaining child operands (may be empty)
   * @return List of child operands that matches child relational
   *   expressions in any order
   */
  public static RelOptRuleOperandChildren unordered(
      RelOptRuleOperand first,
      RelOptRuleOperand... rest) {
    return new RelOptRuleOperandChildren(
        RelOptRuleOperandChildPolicy.UNORDERED,
        Lists.asList(first, rest));
  }

  /**
   * Creates an empty list of child operands.
   *
   * @return Empty list of child operands
   */
  public static RelOptRuleOperandChildren none() {
    return RelOptRuleOperandChildren.LEAF_CHILDREN;
  }

  /**
   * Creates a list of child operands that signifies that the operand matches
   * any number of child relational expressions.
   *
   * @return List of child operands that signifies that the operand matches
   *   any number of child relational expressions
   */
  public static RelOptRuleOperandChildren any() {
    return RelOptRuleOperandChildren.ANY_CHILDREN;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Creates a flattened list of this operand and its descendants in prefix
   * order.
   *
   * @param rootOperand Root operand
   * @return Flattened list of operands
   */
  private List<RelOptRuleOperand> flattenOperands(
      RelOptRuleOperand rootOperand) {
    final List<RelOptRuleOperand> operandList = new ArrayList<>();

    // Flatten the operands into a list.
    rootOperand.setRule(this);
    rootOperand.setParent(null);
    rootOperand.ordinalInParent = 0;
    rootOperand.ordinalInRule = operandList.size();
    operandList.add(rootOperand);
    flattenRecurse(operandList, rootOperand);
    return ImmutableList.copyOf(operandList);
  }

  /**
   * Adds the operand and its descendants to the list in prefix order.
   *
   * @param operandList   Flattened list of operands
   * @param parentOperand Parent of this operand
   */
  private void flattenRecurse(
      List<RelOptRuleOperand> operandList,
      RelOptRuleOperand parentOperand) {
    int k = 0;
    for (RelOptRuleOperand operand : parentOperand.getChildOperands()) {
      operand.setRule(this);
      operand.setParent(parentOperand);
      operand.ordinalInParent = k++;
      operand.ordinalInRule = operandList.size();
      operandList.add(operand);
      flattenRecurse(operandList, operand);
    }
  }

  /**
   * Builds each operand's solve-order. Start with itself, then its parent, up
   * to the root, then the remaining operands in prefix order.
   */
  private void assignSolveOrder() {
    for (RelOptRuleOperand operand : operands) {
      operand.solveOrder = new int[operands.size()];
      int m = 0;
      for (RelOptRuleOperand o = operand; o != null; o = o.getParent()) {
        operand.solveOrder[m++] = o.ordinalInRule;
      }
      for (int k = 0; k < operands.size(); k++) {
        boolean exists = false;
        for (int n = 0; n < m; n++) {
          if (operand.solveOrder[n] == k) {
            exists = true;
          }
        }
        if (!exists) {
          operand.solveOrder[m++] = k;
        }
      }

      // Assert: operand appears once in the sort-order.
      assert m == operands.size();
    }
  }

  /**
   * Returns the root operand of this rule
   *
   * @return the root operand of this rule
   */
  public RelOptRuleOperand getOperand() {
    return operand;
  }

  /**
   * Returns a flattened list of operands of this rule.
   *
   * @return flattened list of operands
   */
  public List<RelOptRuleOperand> getOperands() {
    return ImmutableList.copyOf(operands);
  }

  public int hashCode() {
    // Conventionally, hashCode() and equals() should use the same
    // criteria, whereas here we only look at the description. This is
    // okay, because the planner requires all rule instances to have
    // distinct descriptions.
    return description.hashCode();
  }

  public boolean equals(Object obj) {
    return (obj instanceof RelOptRule)
        && equals((RelOptRule) obj);
  }

  /**
   * Returns whether this rule is equal to another rule.
   *
   * <p>The base implementation checks that the rules have the same class and
   * that the operands are equal; derived classes can override.
   *
   * @param that Another rule
   * @return Whether this rule is equal to another rule
   */
  protected boolean equals(RelOptRule that) {
    // Include operands and class in the equality criteria just in case
    // they have chosen a poor description.
    return this.description.equals(that.description)
        && (this.getClass() == that.getClass())
        && this.operand.equals(that.operand);
  }

  /**
   * Returns whether this rule could possibly match the given operands.
   *
   * <p>This method is an opportunity to apply side-conditions to a rule. The
   * {@link RelOptPlanner} calls this method after matching all operands of
   * the rule, and before calling {@link #onMatch(RelOptRuleCall)}.
   *
   * <p>In implementations of {@link RelOptPlanner} which may queue up a
   * matched {@link RelOptRuleCall} for a long time before calling
   * {@link #onMatch(RelOptRuleCall)}, this method is beneficial because it
   * allows the planner to discard rules earlier in the process.
   *
   * <p>The default implementation of this method returns <code>true</code>.
   * It is acceptable for any implementation of this method to give a false
   * positives, that is, to say that the rule matches the operands but have
   * {@link #onMatch(RelOptRuleCall)} subsequently not generate any
   * successors.
   *
   * <p>The following script is useful to identify rules which commonly
   * produce no successors. You should override this method for these rules:
   *
   * <blockquote>
   * <pre><code>awk '
   * /Apply rule/ {rule=$4; ruleCount[rule]++;}
   * /generated 0 successors/ {ruleMiss[rule]++;}
   * END {
   *   printf "%-30s %s %s\n", "Rule", "Fire", "Miss";
   *   for (i in ruleCount) {
   *     printf "%-30s %5d %5d\n", i, ruleCount[i], ruleMiss[i];
   *   }
   * } ' FarragoTrace.log</code></pre>
   * </blockquote>
   *
   * @param call Rule call which has been determined to match all operands of
   *             this rule
   * @return whether this RelOptRule matches a given RelOptRuleCall
   */
  public boolean matches(RelOptRuleCall call) {
    return true;
  }

  /**
   * Receives notification about a rule match. At the time that this method is
   * called, {@link RelOptRuleCall#rels call.rels} holds the set of relational
   * expressions which match the operands to the rule; <code>
   * call.rels[0]</code> is the root expression.
   *
   * <p>Typically a rule would check that the nodes are valid matches, creates
   * a new expression, then calls back {@link RelOptRuleCall#transformTo} to
   * register the expression.</p>
   *
   * @param call Rule call
   * @see #matches(RelOptRuleCall)
   */
  public abstract void onMatch(RelOptRuleCall call);

  /**
   * Returns the convention of the result of firing this rule, null if
   * not known.
   *
   * @return Convention of the result of firing this rule, null if
   *   not known
   */
  public Convention getOutConvention() {
    return null;
  }

  /**
   * Returns the trait which will be modified as a result of firing this rule,
   * or null if the rule is not a converter rule.
   *
   * @return Trait which will be modified as a result of firing this rule,
   *   or null if the rule is not a converter rule
   */
  public RelTrait getOutTrait() {
    return null;
  }

  /**
   * Returns the description of this rule.
   *
   * <p>It must be unique (for rules that are not equal) and must consist of
   * only the characters A-Z, a-z, 0-9, '_', '.', '(', ')', '-', ',', '[', ']', ':', ' '.
   * It must start with a letter. */
  public final String toString() {
    return description;
  }

  /**
   * Converts a relation expression to a given set of traits, if it does not
   * already have those traits.
   *
   * @param rel      Relational expression to convert
   * @param toTraits desired traits
   * @return a relational expression with the desired traits; never null
   */
  public static RelNode convert(RelNode rel, RelTraitSet toTraits) {
    RelOptPlanner planner = rel.getCluster().getPlanner();

    RelTraitSet outTraits = rel.getTraitSet();
    for (int i = 0; i < toTraits.size(); i++) {
      RelTrait toTrait = toTraits.getTrait(i);
      if (toTrait != null) {
        outTraits = outTraits.replace(i, toTrait);
      }
    }

    if (rel.getTraitSet().matches(outTraits)) {
      return rel;
    }

    return planner.changeTraits(rel, outTraits);
  }

  /**
   * Converts one trait of a relational expression, if it does not
   * already have that trait.
   *
   * @param rel      Relational expression to convert
   * @param toTrait  Desired trait
   * @return a relational expression with the desired trait; never null
   */
  public static RelNode convert(RelNode rel, RelTrait toTrait) {
    RelOptPlanner planner = rel.getCluster().getPlanner();
    RelTraitSet outTraits = rel.getTraitSet();
    if (toTrait != null) {
      outTraits = outTraits.replace(toTrait);
    }

    if (rel.getTraitSet().matches(outTraits)) {
      return rel;
    }

    return planner.changeTraits(rel, outTraits.simplify());
  }

  /**
   * Converts a list of relational expressions.
   *
   * @param rels     Relational expressions
   * @param trait   Trait to add to each relational expression
   * @return List of converted relational expressions, never null
   */
  protected static List<RelNode> convertList(List<RelNode> rels,
      final RelTrait trait) {
    return Lists.transform(rels,
        rel -> convert(rel, rel.getTraitSet().replace(trait)));
  }

  /**
   * Deduces a name for a rule by taking the name of its class and returning
   * the segment after the last '.' or '$'.
   *
   * <p>Examples:
   * <ul>
   * <li>"com.foo.Bar" yields "Bar";</li>
   * <li>"com.flatten.Bar$Baz" yields "Baz";</li>
   * <li>"com.foo.Bar$1" yields "1" (which as an integer is an invalid
   * name, and writer of the rule is encouraged to give it an
   * explicit name).</li>
   * </ul>
   *
   * @param className Name of the rule's class
   * @return Last segment of the class
   */
  static String guessDescription(String className) {
    String description = className;
    int punc =
        Math.max(
            className.lastIndexOf('.'),
            className.lastIndexOf('$'));
    if (punc >= 0) {
      description = className.substring(punc + 1);
    }
    if (description.matches("[0-9]+")) {
      throw new RuntimeException("Derived description of rule class "
          + className + " is an integer, not valid. "
          + "Supply a description manually.");
    }
    return description;
  }

  /**
   * Operand to an instance of the converter rule.
   */
  private static class ConverterRelOptRuleOperand extends RelOptRuleOperand {
    <R extends RelNode> ConverterRelOptRuleOperand(Class<R> clazz, RelTrait in,
        Predicate<? super R> predicate) {
      super(clazz, in, predicate, RelOptRuleOperandChildPolicy.ANY,
          ImmutableList.of());
    }

    public boolean matches(RelNode rel) {
      // Don't apply converters to converters that operate
      // on the same RelTraitDef -- otherwise we get
      // an n^2 effect.
      if (rel instanceof Converter) {
        if (((ConverterRule) getRule()).getTraitDef()
            == ((Converter) rel).getTraitDef()) {
          return false;
        }
      }
      return super.matches(rel);
    }
  }
}

// End RelOptRule.java
