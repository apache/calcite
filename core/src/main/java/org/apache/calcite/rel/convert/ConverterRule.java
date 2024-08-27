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
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilderFactory;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.Locale;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.util.Objects.requireNonNull;

/**
 * Abstract base class for a rule which converts from one calling convention to
 * another without changing semantics.
 */
@Value.Enclosing
public abstract class ConverterRule
    extends RelRule<ConverterRule.Config> {
  //~ Instance fields --------------------------------------------------------

  private final RelTrait inTrait;
  private final RelTrait outTrait;
  protected final Convention out;

  //~ Constructors -----------------------------------------------------------

  /** Creates a <code>ConverterRule</code>. */
  protected ConverterRule(Config config) {
    super(config);
    this.inTrait = requireNonNull(config.inTrait());
    this.outTrait = requireNonNull(config.outTrait());

    // Source and target traits must have same type
    assert inTrait.getTraitDef() == outTrait.getTraitDef();

    // Most sub-classes are concerned with converting one convention to
    // another, and for them, the "out" field is a convenient short-cut.
    this.out =
        outTrait instanceof Convention ? (Convention) outTrait
            : castNonNull(null);
  }

  /**
   * Creates a <code>ConverterRule</code>.
   *
   * @param clazz       Type of relational expression to consider converting
   * @param in          Trait of relational expression to consider converting
   * @param out         Trait which is converted to
   * @param descriptionPrefix Description prefix of rule
   *
   * @deprecated Use {@link #ConverterRule(Config)}
   */
  @Deprecated // to be removed before 2.0
  protected ConverterRule(Class<? extends RelNode> clazz, RelTrait in,
      RelTrait out, String descriptionPrefix) {
    this(Config.INSTANCE
        .withConversion(clazz, in, out, descriptionPrefix));
  }

  @SuppressWarnings("Guava")
  @Deprecated // to be removed before 2.0
  protected <R extends RelNode> ConverterRule(Class<R> clazz,
      com.google.common.base.Predicate<? super R> predicate,
      RelTrait in, RelTrait out, String descriptionPrefix) {
    this(Config.INSTANCE
        .withConversion(clazz, (Predicate<? super R>) predicate::apply,
            in, out, descriptionPrefix));
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
   *
   * @deprecated Use {@link #ConverterRule(Config)}
   */
  @Deprecated // to be removed before 2.0
  protected <R extends RelNode> ConverterRule(Class<R> clazz,
      Predicate<? super R> predicate, RelTrait in, RelTrait out,
      RelBuilderFactory relBuilderFactory, String descriptionPrefix) {
    this(ImmutableConverterRule.Config.builder()
        .withRelBuilderFactory(relBuilderFactory)
        .build()
        .withConversion(clazz, predicate, in, out, descriptionPrefix));
  }

  @SuppressWarnings("Guava")
  @Deprecated // to be removed before 2.0
  protected <R extends RelNode> ConverterRule(Class<R> clazz,
      com.google.common.base.Predicate<? super R> predicate, RelTrait in,
      RelTrait out, RelBuilderFactory relBuilderFactory, String description) {
    this(clazz, (Predicate<? super R>) predicate::apply, in, out,
        relBuilderFactory, description);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public Convention getOutConvention() {
    return (Convention) outTrait;
  }

  @Override public RelTrait getOutTrait() {
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
  public abstract @Nullable RelNode convert(RelNode rel);

  /**
   * Returns true if this rule can convert <em>any</em> relational expression
   * of the input convention.
   *
   * <p>The union-to-java converter, for example, is not guaranteed, because
   * it only works on unions.
   *
   * @return {@code true} if this rule can convert <em>any</em> relational
   *   expression
   */
  public boolean isGuaranteed() {
    return false;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    RelNode rel = call.rel(0);
    if (rel.getTraitSet().contains(inTrait)) {
      final RelNode converted = convert(rel);
      if (converted != null) {
        call.transformTo(converted);
      }
    }
  }

  //~ Inner Classes ----------------------------------------------------------

  /** Rule configuration. */
  @Value.Immutable(singleton = false)
  public interface Config extends RelRule.Config {
    Config INSTANCE = ImmutableConverterRule.Config.builder()
        .withInTrait(Convention.NONE)
        .withOutTrait(Convention.NONE)
        .withRuleFactory(new Function<Config, ConverterRule>() {
          @Override public ConverterRule apply(final Config config) {
            throw new UnsupportedOperationException("A rule factory must be provided");
          }
        }).build();

    RelTrait inTrait();

    /** Sets {@link #inTrait}. */
    Config withInTrait(RelTrait trait);

    RelTrait outTrait();

    /** Sets {@link #outTrait}. */
    Config withOutTrait(RelTrait trait);

    Function<Config, ConverterRule> ruleFactory();

    /** Sets {@link #outTrait}. */
    Config withRuleFactory(Function<Config, ConverterRule> factory);

    default <R extends RelNode> Config withConversion(Class<R> clazz,
        Predicate<? super R> predicate, RelTrait in, RelTrait out,
        String descriptionPrefix) {
      return withInTrait(in)
          .withOutTrait(out)
          .withOperandSupplier(b ->
              b.operand(clazz).predicate(predicate).convert(in))
          .withDescription(createDescription(descriptionPrefix, in, out))
          .as(Config.class);
    }

    default Config withConversion(Class<? extends RelNode> clazz, RelTrait in,
        RelTrait out, String descriptionPrefix) {
      return withConversion(clazz, r -> true, in, out, descriptionPrefix);
    }

    @Override default RelOptRule toRule() {
      return toRule(ConverterRule.class);
    }

    default <R extends ConverterRule> R toRule(Class<R> ruleClass) {
      return ruleClass.cast(ruleFactory().apply(this));
    }
  }

}
