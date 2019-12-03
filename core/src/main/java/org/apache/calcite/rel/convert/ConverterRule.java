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
package org.apache.calcite.rel.convert;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.Locale;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Abstract base class for a rule which converts from one calling convention to
 * another without changing semantics.
 */
public abstract class ConverterRule extends RelOptRule {
  //~ Instance fields --------------------------------------------------------

  private final RelTrait inTrait;
  private final RelTrait outTrait;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a <code>ConverterRule</code>.
   *
   * @param clazz       Type of relational expression to consider converting
   * @param in          Trait of relational expression to consider converting
   * @param out         Trait which is converted to
   * @param descriptionPrefix Description prefix of rule
   */
  public ConverterRule(Class<? extends RelNode> clazz, RelTrait in,
      RelTrait out, String descriptionPrefix) {
    this(clazz, (Predicate<RelNode>) r -> true, in, out,
        RelFactories.LOGICAL_BUILDER, descriptionPrefix);
  }

  @SuppressWarnings("Guava")
  @Deprecated // to be removed before 2.0
  public <R extends RelNode> ConverterRule(Class<R> clazz,
      com.google.common.base.Predicate<? super R> predicate,
      RelTrait in, RelTrait out, String descriptionPrefix) {
    this(clazz, predicate, in, out, RelFactories.LOGICAL_BUILDER, descriptionPrefix);
  }

  /**
   * Creates a <code>ConverterRule</code> with a predicate.
   *
   * @param clazz       Type of relational expression to consider converting
   * @param predicate   Predicate on the relational expression
   * @param in          Trait of relational expression to consider converting
   * @param out         Trait which is converted to
   * @param relBuilderFactory Builder for relational expressions
   * @param descriptionPrefix Description prefix of rule
   */
  public <R extends RelNode> ConverterRule(Class<R> clazz,
      Predicate<? super R> predicate, RelTrait in, RelTrait out,
      RelBuilderFactory relBuilderFactory, String descriptionPrefix) {
    super(convertOperand(clazz, predicate, in),
        relBuilderFactory,
        createDescription(descriptionPrefix, in, out));
    this.inTrait = Objects.requireNonNull(in);
    this.outTrait = Objects.requireNonNull(out);

    // Source and target traits must have same type
    assert in.getTraitDef() == out.getTraitDef();
  }

  @SuppressWarnings("Guava")
  @Deprecated // to be removed before 2.0
  public <R extends RelNode> ConverterRule(Class<R> clazz,
      com.google.common.base.Predicate<? super R> predicate, RelTrait in,
      RelTrait out, RelBuilderFactory relBuilderFactory, String description) {
    this(clazz, (Predicate<? super R>) predicate::apply, in, out,
        relBuilderFactory, description);
  }

  //~ Methods ----------------------------------------------------------------

  public Convention getOutConvention() {
    return (Convention) outTrait;
  }

  public RelTrait getOutTrait() {
    return outTrait;
  }

  public RelTrait getInTrait() {
    return inTrait;
  }

  public RelTraitDef getTraitDef() {
    return inTrait.getTraitDef();
  }

  private static String createDescription(String descriptionPrefix,
      RelTrait in, RelTrait out) {
    return String.format(Locale.ROOT, "%s(in:%s,out:%s)",
        Objects.toString(descriptionPrefix, "ConverterRule"), in, out);
  }

  /** Converts a relational expression to the target trait(s) of this rule.
   *
   * <p>Returns null if conversion is not possible. */
  public abstract RelNode convert(RelNode rel);

  /**
   * Returns true if this rule can convert <em>any</em> relational expression
   * of the input convention.
   *
   * <p>The union-to-java converter, for example, is not guaranteed, because
   * it only works on unions.</p>
   *
   * @return {@code true} if this rule can convert <em>any</em> relational
   *   expression
   */
  public boolean isGuaranteed() {
    return false;
  }

  public void onMatch(RelOptRuleCall call) {
    RelNode rel = call.rel(0);
    if (rel.getTraitSet().contains(inTrait)) {
      final RelNode converted = convert(rel);
      if (converted != null) {
        call.transformTo(converted);
      }
    }
  }

  //~ Inner Classes ----------------------------------------------------------

}

// End ConverterRule.java
