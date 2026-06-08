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
import java.util.ArrayList;
import java.util.Collections;
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
              Expressions.call(v, BuiltInMethod.TAKE.method,
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
      // TODO: Enumerable runtime only supports INT types for FETCH and OFFSET, not BIGINT types.
      //  Currently, using BIGINT types for execution will result in an error message.
      //  This issue needs to be fixed. For more information, see CALCITE-7156.
      return Expressions.constant(RexLiteral.intValue(rexNode));
    }
  }

  static Expression getExpressionForFetch(RexNode rexNode,
      EnumerableRelImplementor implementor, BlockBuilder builder) {
    if (rexNode instanceof RexDynamicParam) {
      final RexDynamicParam param = (RexDynamicParam) rexNode;
      return Expressions.call(EnumerableLimit.class, "toIntFetch",
          Expressions.convert_(
              Expressions.call(DataContext.ROOT,
                  BuiltInMethod.DATA_CONTEXT_GET.method,
                  Expressions.constant("?" + param.getIndex())),
              Number.class));
    } else if (rexNode instanceof RexLiteral) {
      return Expressions.constant(
          toIntFetch(((RexLiteral) rexNode).getValueAs(Number.class)));
    } else {
      final Expression expression =
          RexToLixTranslator.forAggregation(implementor.getTypeFactory(),
              builder, null, implementor.getConformance())
              .translate(rexNode);
      return Expressions.call(EnumerableLimit.class, "toIntFetch",
          Expressions.convert_(Expressions.box(expression), Number.class));
    }
  }

  /** Converts a FETCH expression result to the range supported by Enumerable. */
  public static int toIntFetch(@Nullable Number value) {
    if (value == null) {
      throw new IllegalArgumentException("FETCH expression evaluated to NULL");
    }
    final BigDecimal decimal = NumberUtil.toBigDecimal(value);
    if (decimal == null) {
      throw new IllegalArgumentException("FETCH value is not numeric: " + value);
    }
    final int result;
    try {
      result = decimal.intValueExact();
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException("FETCH value " + value
          + " is out of range; expected a value between 0 and "
          + Integer.MAX_VALUE, e);
    }
    if (result < 0) {
      throw new IllegalArgumentException("FETCH value " + value
          + " is out of range; expected a value between 0 and "
          + Integer.MAX_VALUE);
    }
    return result;
  }

  static void validateLiteralFetch(@Nullable RexNode fetch) {
    if (fetch instanceof RexLiteral) {
      toIntFetch(((RexLiteral) fetch).getValueAs(Number.class));
    }
  }

  /** Reduces a constant FETCH expression to a validated integer literal. */
  public static @Nullable RexLiteral reduceFetchToLiteral(
      RelOptCluster cluster, RexNode fetch) {
    final RexLiteral literal;
    if (fetch instanceof RexLiteral) {
      literal = (RexLiteral) fetch;
    } else {
      if (!RexUtil.isConstant(fetch) || !RexUtil.isDeterministic(fetch)) {
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
    final int value = toIntFetch(literal.getValueAs(Number.class));
    return cluster.getRexBuilder().makeExactLiteral(BigDecimal.valueOf(value));
  }
}
