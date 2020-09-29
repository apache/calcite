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
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBeans;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nonnull;

/**
 * Rule that is parameterized via a configuration.
 *
 * <p>Eventually (before Calcite version 2.0), this class will replace
 * {@link RelOptRule}. Constructors of {@code RelOptRule} are deprecated, so new
 * rule classes should extend {@code RelRule}, not {@code RelOptRule}.
 * Next, we will deprecate {@code RelOptRule}, so that variables that reference
 * rules will be of type {@code RelRule}.
 *
 * <p><b>Guidelines for writing rules</b>
 *
 * <p>1. If your rule is a sub-class of
 * {@link org.apache.calcite.rel.convert.ConverterRule}
 * and does not need any extra properties,
 * there's no need to create an {@code interface Config} inside your class.
 * In your class, create a constant
 * {@code public static final Config DEFAULT_CONFIG}. Goto step 5.
 *
 * <p>2. If your rule is not a sub-class of
 * {@link org.apache.calcite.rel.convert.ConverterRule},
 * create an inner {@code interface Config extends RelRule.Config}.
 * Implement {@link Config#toRule() toRule} using a {@code default} method:
 *
 * <blockquote>
 * <code>
 * &#x40;Override default CsvProjectTableScanRule toRule() {<br>
 * &nbsp;&nbsp;return new CsvProjectTableScanRule(this);<br>
 * }
 * </code>
 * </blockquote>
 *
 * <p>3. For each configuration property, create a pair of methods in your
 * {@code Config} interface. For example, for a property {@code foo} of type
 * {@code int}, create methods {@code foo} and {@code withFoo}:
 *
 * <blockquote><pre><code>
 * &#x2f;** Returns foo. *&#x2f;
 * &#x40;ImmutableBeans.Property
 * int foo();
 *
 * &#x2f;** Sets {&#x40;link #foo}. *&#x2f;
 * Config withFoo(int x);
 * </code></pre></blockquote>
 *
 * <p>4. In your {@code Config} interface, create a {@code DEFAULT} constant
 * that represents the most typical configuration of your rule. For example,
 * {@code CsvProjectTableScanRule.Config} has the following:
 *
 * <blockquote><pre><code>
 * Config DEFAULT = EMPTY
 *     .withOperandSupplier(b0 -&gt;
 *         b0.operand(LogicalProject.class).oneInput(b1 -&gt;
 *             b1.operand(CsvTableScan.class).noInputs()))
 *      .as(Config.class);
 * </code></pre></blockquote>
 *
 * <p>5. Do not create an {@code INSTANCE} constant inside your rule.
 * Instead, create a named instance of your rule, with default configuration,
 * in a holder class. The holder class must not be a sub-class of
 * {@code RelOptRule} (otherwise cyclic class-loading issues may arise).
 * Generally it will be called <code><i>Xxx</i>Rules</code>, for example
 * {@code CsvRules}. The rule instance is named after your rule, and is based
 * on the default config ({@code Config.DEFAULT}, or {@code DEFAULT_CONFIG} for
 * converter rules):
 *
 * <blockquote><pre><code>
 * &#x2f;** Rule that matches a {&#x40;code Project} on a
 *  * {&#x40;code CsvTableScan} and pushes down projects if possible. *&#x2f;
 * public static final CsvProjectTableScanRule PROJECT_SCAN =
 *     CsvProjectTableScanRule.Config.DEFAULT.toRule();
 * </code></pre></blockquote>
 *
 * @param <C> Configuration type
 */
public abstract class RelRule<C extends RelRule.Config> extends RelOptRule {
  public final C config;

  /** Creates a RelRule. */
  protected RelRule(C config) {
    super(OperandBuilderImpl.operand(config.operandSupplier()),
        config.relBuilderFactory(), config.description());
    this.config = config;
  }

  /** Rule configuration. */
  public interface Config {
    /** Empty configuration. */
    RelRule.Config EMPTY = ImmutableBeans.create(Config.class)
        .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
        .withOperandSupplier(b -> {
          throw new IllegalArgumentException("Rules must have at least one "
              + "operand. Call Config.withOperandSupplier to specify them.");
        });

    /** Creates a rule that uses this configuration. Sub-class must override. */
    RelOptRule toRule();

    /** Casts this configuration to another type, usually a sub-class. */
    default <T> T as(Class<T> class_) {
      return ImmutableBeans.copy(class_, this);
    }

    /** The factory that is used to create a
     * {@link org.apache.calcite.tools.RelBuilder} during rule invocations. */
    @ImmutableBeans.Property
    RelBuilderFactory relBuilderFactory();

    /** Sets {@link #relBuilderFactory()}. */
    Config withRelBuilderFactory(RelBuilderFactory factory);

    /** Description of the rule instance. */
    @ImmutableBeans.Property
    String description();

    /** Sets {@link #description()}. */
    Config withDescription(String description);

    /** Creates the operands for the rule instance. */
    @ImmutableBeans.Property
    OperandTransform operandSupplier();

    /** Sets {@link #operandSupplier()}. */
    Config withOperandSupplier(OperandTransform transform);
  }

  /** Function that creates an operand.
   *
   * @see Config#withOperandSupplier(OperandTransform) */
  @FunctionalInterface
  public interface OperandTransform extends Function<OperandBuilder, Done> {
  }

  /** Callback to create an operand.
   *
   * @see OperandTransform */
  public interface OperandBuilder {
    /** Starts building an operand by specifying its class.
     * Call further methods on the returned {@link OperandDetailBuilder} to
     * complete the operand. */
    <R extends RelNode> OperandDetailBuilder<R> operand(Class<R> relClass);

    /** Supplies an operand that has been built manually. */
    Done exactly(RelOptRuleOperand operand);
  }

  /** Indicates that an operand is complete.
   *
   * @see OperandTransform */
  public interface Done {
  }

  /** Add details about an operand, such as its inputs.
   *
   * @param <R> Type of relational expression */
  public interface OperandDetailBuilder<R extends RelNode> {
    /** Sets a trait of this operand. */
    OperandDetailBuilder<R> trait(@Nonnull RelTrait trait);

    /** Sets the predicate of this operand. */
    OperandDetailBuilder<R> predicate(Predicate<? super R> predicate);

    /** Indicates that this operand has a single input. */
    Done oneInput(OperandTransform transform);

    /** Indicates that this operand has several inputs. */
    Done inputs(OperandTransform... transforms);

    /** Indicates that this operand has several inputs, unordered. */
    Done unorderedInputs(OperandTransform... transforms);

    /** Indicates that this operand takes any number or type of inputs. */
    Done anyInputs();

    /** Indicates that this operand takes no inputs. */
    Done noInputs();

    /** Indicates that this operand converts a relational expression to
     * another trait. */
    Done convert(RelTrait in);
  }

  /** Implementation of {@link OperandBuilder}. */
  private static class OperandBuilderImpl implements OperandBuilder {
    final List<RelOptRuleOperand> operands = new ArrayList<>();

    static RelOptRuleOperand operand(OperandTransform transform) {
      final OperandBuilderImpl b = new OperandBuilderImpl();
      final Done done = transform.apply(b);
      Objects.requireNonNull(done);
      if (b.operands.size() != 1) {
        throw new IllegalArgumentException("operand supplier must call one of "
            + "the following methods: operand or exactly");
      }
      return b.operands.get(0);
    }

    public <R extends RelNode> OperandDetailBuilder<R> operand(Class<R> relClass) {
      return new OperandDetailBuilderImpl<>(this, relClass);
    }

    public Done exactly(RelOptRuleOperand operand) {
      operands.add(operand);
      return DoneImpl.INSTANCE;
    }
  }

  /** Implementation of {@link OperandDetailBuilder}.
   *
   * @param <R> Type of relational expression */
  private static class OperandDetailBuilderImpl<R extends RelNode>
      implements OperandDetailBuilder<R> {
    private final OperandBuilderImpl parent;
    private final Class<R> relClass;
    final OperandBuilderImpl inputBuilder = new OperandBuilderImpl();
    private RelTrait trait;
    private Predicate<? super R> predicate = r -> true;

    OperandDetailBuilderImpl(OperandBuilderImpl parent, Class<R> relClass) {
      this.parent = Objects.requireNonNull(parent);
      this.relClass = Objects.requireNonNull(relClass);
    }

    public OperandDetailBuilderImpl<R> trait(@Nonnull RelTrait trait) {
      this.trait = Objects.requireNonNull(trait);
      return this;
    }

    public OperandDetailBuilderImpl<R> predicate(Predicate<? super R> predicate) {
      this.predicate = predicate;
      return this;
    }

    /** Indicates that there are no more inputs. */
    Done done(RelOptRuleOperandChildPolicy childPolicy) {
      parent.operands.add(
          new RelOptRuleOperand(relClass, trait, predicate, childPolicy,
              ImmutableList.copyOf(inputBuilder.operands)));
      return DoneImpl.INSTANCE;
    }

    public Done convert(RelTrait in) {
      parent.operands.add(
          new ConverterRelOptRuleOperand(relClass, in, predicate));
      return DoneImpl.INSTANCE;
    }

    public Done noInputs() {
      return done(RelOptRuleOperandChildPolicy.LEAF);
    }

    public Done anyInputs() {
      return done(RelOptRuleOperandChildPolicy.ANY);
    }

    public Done oneInput(OperandTransform transform) {
      final Done done = transform.apply(inputBuilder);
      Objects.requireNonNull(done);
      return done(RelOptRuleOperandChildPolicy.SOME);
    }

    public Done inputs(OperandTransform... transforms) {
      for (OperandTransform transform : transforms) {
        final Done done = transform.apply(inputBuilder);
        Objects.requireNonNull(done);
      }
      return done(RelOptRuleOperandChildPolicy.SOME);
    }

    public Done unorderedInputs(OperandTransform... transforms) {
      for (OperandTransform transform : transforms) {
        final Done done = transform.apply(inputBuilder);
        Objects.requireNonNull(done);
      }
      return done(RelOptRuleOperandChildPolicy.UNORDERED);
    }
  }

  /** Singleton instance of {@link Done}. */
  private enum DoneImpl implements Done {
    INSTANCE
  }

  /** Callback interface that helps you avoid creating sub-classes of
   * {@link RelRule} that differ only in implementations of
   * {@link #onMatch(RelOptRuleCall)} method.
   *
   * @param <R> Rule type */
  public interface MatchHandler<R extends RelOptRule>
      extends BiConsumer<R, RelOptRuleCall> {
  }
}
