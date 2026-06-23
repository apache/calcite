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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.NumberUtil;
import org.apache.calcite.util.Util;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/** Relational expression that applies a limit and/or offset to its input. */
public class EnumerableLimit extends SingleRel implements EnumerableRel {
  public final @Nullable RexNode offset;
  public final @Nullable RexNode fetch;

  /** Creates an EnumerableLimit.
   *
   * <p>Use {@link #create} unless you know what you're doing. */
  public EnumerableLimit(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      @Nullable RexNode offset,
      @Nullable RexNode fetch) {
    super(cluster, traitSet, input);
    validateLiteralFetch(fetch);
    this.offset = offset;
    this.fetch = fetch;
    assert getConvention() instanceof EnumerableConvention;
    assert getConvention() == input.getConvention();
  }

  /** Creates an EnumerableLimit. */
  public static EnumerableLimit create(final RelNode input, @Nullable RexNode offset,
      @Nullable RexNode fetch) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet =
        cluster.traitSetOf(EnumerableConvention.INSTANCE)
            .replaceIfs(
                RelCollationTraitDef.INSTANCE,
                () -> RelMdCollation.limit(mq, input))
            .replaceIf(RelDistributionTraitDef.INSTANCE,
                () -> RelMdDistribution.limit(mq, input));
    return new EnumerableLimit(cluster, traitSet, input, offset, fetch);
  }

  @Override public EnumerableLimit copy(
      RelTraitSet traitSet,
      List<RelNode> newInputs) {
    return new EnumerableLimit(
        getCluster(),
        traitSet,
        sole(newInputs),
        offset,
        fetch);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .itemIf("offset", offset, offset != null)
        .itemIf("fetch", fetch, fetch != null);
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final EnumerableRel child = (EnumerableRel) getInput();
    final Result result = implementor.visitChild(this, 0, child, pref);
    final PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(),
            result.format);

    Expression v = builder.append("child", result.block);
    if (offset != null) {
      v =
          builder.append("offset",
              Expressions.call(v, BuiltInMethod.SKIP.method,
                  getExpression(offset)));
    }
    if (fetch != null) {
      v =
          builder.append("fetch",
              Expressions.call(EnumerableLimit.class, "take", v,
                  getExpressionForFetch(fetch, implementor, builder)));
    }

    builder.add(Expressions.return_(null, v));
    return implementor.result(physType, builder.toBlock());
  }

  static Expression getExpression(RexNode rexNode) {
    if (rexNode instanceof RexDynamicParam) {
      final RexDynamicParam param = (RexDynamicParam) rexNode;
      return Expressions.convert_(
          Expressions.call(DataContext.ROOT,
              BuiltInMethod.DATA_CONTEXT_GET.method,
              Expressions.constant("?" + param.getIndex())),
          Integer.class);
    } else {
      // TODO: Enumerable runtime only supports INT types for OFFSET, not BIGINT types.
      //  Currently, using BIGINT types for OFFSET will result in an error message.
      //  This issue needs to be fixed. For more information, see CALCITE-7156.
      return Expressions.constant(RexLiteral.intValue(rexNode));
    }
  }

  static Expression getExpressionForFetch(RexNode rexNode,
      EnumerableRelImplementor implementor, BlockBuilder builder) {
    if (rexNode instanceof RexDynamicParam) {
      final RexDynamicParam param = (RexDynamicParam) rexNode;
      return Expressions.call(EnumerableLimit.class, "toFetchValue",
          Expressions.convert_(
              Expressions.call(DataContext.ROOT,
                  BuiltInMethod.DATA_CONTEXT_GET.method,
                  Expressions.constant("?" + param.getIndex())),
              Number.class));
    } else if (rexNode instanceof RexLiteral) {
      return Expressions.constant(
          toFetchValue(((RexLiteral) rexNode).getValueAs(Number.class)));
    } else {
      final Expression expression =
          RexToLixTranslator.forAggregation(implementor.getTypeFactory(),
              builder, null, implementor.getConformance())
              .translate(rexNode);
      return Expressions.call(EnumerableLimit.class, "toFetchValue",
          Expressions.convert_(Expressions.box(expression), Number.class));
    }
  }

  /** Converts a FETCH expression result to Calcite's canonical representation. */
  public static BigDecimal toFetchValue(@Nullable Number value) {
    final BigDecimal decimal = validateFetchValue(value);
    if (decimal.signum() < 0) {
      throw new IllegalArgumentException("FETCH value " + value
          + " is out of range; expected a non-negative value");
    }
    return decimal;
  }

  /** Applies a FETCH value without narrowing it to {@code int} or {@code long}. */
  public static <T> Enumerable<T> take(Enumerable<T> source, BigDecimal fetch) {
    final BigInteger count = fetch.toBigIntegerExact();
    return new AbstractEnumerable<T>() {
      @Override public Enumerator<T> enumerator() {
        final Enumerator<T> input = source.enumerator();
        return new Enumerator<T>() {
          private BigInteger remaining = count;
          private boolean done;

          @Override public T current() {
            return input.current();
          }

          @Override public boolean moveNext() {
            if (done) {
              return false;
            }
            if (remaining.signum() == 0 || !input.moveNext()) {
              done = true;
              return false;
            }
            // Preserve take(int)'s eager evaluation of the current row.
            input.current();
            remaining = remaining.subtract(BigInteger.ONE);
            return true;
          }

          @Override public void reset() {
            input.reset();
            remaining = count;
            done = false;
          }

          @Override public void close() {
            input.close();
          }
        };
      }
    };
  }

  /** Sorts and applies FETCH while preserving an arbitrary-precision value. */
  public static <T, TKey> Enumerable<T> orderBy(Enumerable<T> source,
      Function1<T, TKey> keySelector, @Nullable Comparator<TKey> comparator,
      int offset, @Nullable BigDecimal fetch) {
    if (fetch != null
        && fetch.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) <= 0) {
      return EnumerableDefaults.orderBy(source, keySelector, comparator,
          offset, fetch.intValueExact());
    }
    Enumerable<T> result = EnumerableDefaults.orderBy(source, keySelector, comparator);
    if (offset > 0) {
      result = result.skip(offset);
    }
    return fetch == null ? result : take(result, fetch);
  }

  private static BigDecimal validateFetchValue(@Nullable Number value) {
    if (value == null) {
      throw new IllegalArgumentException("FETCH expression evaluated to NULL");
    }
    final BigDecimal decimal = NumberUtil.toBigDecimal(value);
    if (decimal == null) {
      throw new IllegalArgumentException("FETCH value is not numeric: " + value);
    }
    try {
      decimal.toBigIntegerExact();
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException("FETCH value " + value
          + " is not an integer", e);
    }
    return decimal;
  }

  static void validateLiteralFetch(@Nullable RexNode fetch) {
    if (fetch instanceof RexLiteral) {
      final Number value = ((RexLiteral) fetch).getValueAs(Number.class);
      toFetchValue(value);
    }
  }

  /** Reduces a constant FETCH expression to a validated literal. */
  public static @Nullable RexLiteral reduceFetchToLiteral(
      RelOptCluster cluster, RexNode fetch) {
    final RexLiteral literal;
    if (fetch instanceof RexLiteral) {
      literal = (RexLiteral) fetch;
    } else {
      if (!RexUtil.isConstant(fetch)
          || !RexUtil.isDeterministic(fetch)
          || RexUtil.containsDynamicFunction(fetch)
          || RexUtil.containsDynamicParam(fetch)) {
        return null;
      }
      final RexExecutor executor =
          Util.first(cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR);
      final List<RexNode> reducedValues = new ArrayList<>(1);
      executor.reduce(cluster.getRexBuilder(),
          Collections.singletonList(fetch), reducedValues);
      final RexNode reduced = reducedValues.get(0);
      if (!(reduced instanceof RexLiteral)) {
        return null;
      }
      literal = (RexLiteral) reduced;
    }
    toFetchValue(literal.getValueAs(Number.class));
    return literal;
  }

}
