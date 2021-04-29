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

import org.apache.calcite.adapter.enumerable.impl.AggAddContextImpl;
import org.apache.calcite.adapter.enumerable.impl.AggResultContextImpl;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/** Base class for EnumerableAggregate and EnumerableSortedAggregate. */
public abstract class EnumerableAggregateBase extends Aggregate {
  protected EnumerableAggregateBase(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelNode input,
      ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls);
  }

  protected static boolean hasOrderedCall(List<AggImpState> aggs) {
    for (AggImpState agg : aggs) {
      if (!agg.call.collation.equals(RelCollations.EMPTY)) {
        return true;
      }
    }
    return false;
  }

  protected void declareParentAccumulator(List<Expression> initExpressions,
      BlockBuilder initBlock, PhysType accPhysType) {
    if (accPhysType.getJavaRowType()
        instanceof JavaTypeFactoryImpl.SyntheticRecordType) {
      // We have to initialize the SyntheticRecordType instance this way, to
      // avoid using a class constructor with too many parameters.
      final JavaTypeFactoryImpl.SyntheticRecordType synType =
          (JavaTypeFactoryImpl.SyntheticRecordType)
              accPhysType.getJavaRowType();
      final ParameterExpression record0_ =
          Expressions.parameter(accPhysType.getJavaRowType(), "record0");
      initBlock.add(Expressions.declare(0, record0_, null));
      initBlock.add(
          Expressions.statement(
              Expressions.assign(record0_,
                  Expressions.new_(accPhysType.getJavaRowType()))));
      List<Types.RecordField> fieldList = synType.getRecordFields();
      for (int i = 0; i < initExpressions.size(); i++) {
        Expression right = initExpressions.get(i);
        initBlock.add(
            Expressions.statement(
                Expressions.assign(
                    Expressions.field(record0_, fieldList.get(i)), right)));
      }
      initBlock.add(record0_);
    } else {
      initBlock.add(accPhysType.record(initExpressions));
    }
  }

  /**
   * Implements the {@link AggregateLambdaFactory}.
   *
   * <p>Behavior depends upon ordering:
   * <ul>
   *
   * <li>{@code hasOrderedCall == true} means there is at least one aggregate
   * call including sort spec. We use {@link LazyAggregateLambdaFactory}
   * implementation to implement sorted aggregates for that.
   *
   * <li>{@code hasOrderedCall == false} indicates to use
   * {@link BasicAggregateLambdaFactory} to implement a non-sort
   * aggregate.
   *
   * </ul>
   */
  protected void implementLambdaFactory(BlockBuilder builder,
      PhysType inputPhysType, List<AggImpState> aggs,
      Expression accumulatorInitializer, boolean hasOrderedCall,
      ParameterExpression lambdaFactory) {
    if (hasOrderedCall) {
      ParameterExpression pe = Expressions.parameter(List.class,
          builder.newName("lazyAccumulators"));
      builder.add(
          Expressions.declare(0, pe, Expressions.new_(LinkedList.class)));

      for (AggImpState agg : aggs) {
        if (agg.call.collation.equals(RelCollations.EMPTY)) {
          // if the call does not require ordering, fallback to
          // use a non-sorted lazy accumulator.
          builder.add(
              Expressions.statement(
                  Expressions.call(pe,
                      BuiltInMethod.COLLECTION_ADD.method,
                      Expressions.new_(BuiltInMethod.BASIC_LAZY_ACCUMULATOR.constructor,
                          requireNonNull(agg.accumulatorAdder, "agg.accumulatorAdder")))));
          continue;
        }
        final Pair<Expression, Expression> pair =
            inputPhysType.generateCollationKey(
                agg.call.collation.getFieldCollations());
        builder.add(
            Expressions.statement(
                Expressions.call(pe,
                    BuiltInMethod.COLLECTION_ADD.method,
                    Expressions.new_(BuiltInMethod.SOURCE_SORTER.constructor,
                        requireNonNull(agg.accumulatorAdder, "agg.accumulatorAdder"),
                        pair.left, pair.right))));
      }
      builder.add(
          Expressions.declare(0, lambdaFactory,
              Expressions.new_(
                  BuiltInMethod.LAZY_AGGREGATE_LAMBDA_FACTORY.constructor,
                  accumulatorInitializer, pe)));
    } else {
      // when hasOrderedCall == false
      ParameterExpression pe = Expressions.parameter(List.class,
          builder.newName("accumulatorAdders"));
      builder.add(
          Expressions.declare(0, pe, Expressions.new_(LinkedList.class)));

      for (AggImpState agg : aggs) {
        builder.add(
            Expressions.statement(
                Expressions.call(pe, BuiltInMethod.COLLECTION_ADD.method,
                    requireNonNull(agg.accumulatorAdder, "agg.accumulatorAdder"))));
      }
      builder.add(
          Expressions.declare(0, lambdaFactory,
              Expressions.new_(
                  BuiltInMethod.BASIC_AGGREGATE_LAMBDA_FACTORY.constructor,
                  accumulatorInitializer, pe)));
    }
  }

  /** An implementation of {@link AggContext}. */
  protected class AggContextImpl implements AggContext {
    private final AggImpState agg;
    private final JavaTypeFactory typeFactory;

    AggContextImpl(AggImpState agg, JavaTypeFactory typeFactory) {
      this.agg = agg;
      this.typeFactory = typeFactory;
    }

    @Override public SqlAggFunction aggregation() {
      return agg.call.getAggregation();
    }

    @Override public RelDataType returnRelType() {
      return agg.call.type;
    }

    @Override public Type returnType() {
      return EnumUtils.javaClass(typeFactory, returnRelType());
    }

    @Override public List<? extends RelDataType> parameterRelTypes() {
      return EnumUtils.fieldRowTypes(getInput().getRowType(), null,
          agg.call.getArgList());
    }

    @Override public List<? extends Type> parameterTypes() {
      return EnumUtils.fieldTypes(
          typeFactory,
          parameterRelTypes());
    }

    @Override public List<ImmutableBitSet> groupSets() {
      return groupSets;
    }

    @Override public List<Integer> keyOrdinals() {
      return groupSet.asList();
    }

    @Override public List<? extends RelDataType> keyRelTypes() {
      return EnumUtils.fieldRowTypes(getInput().getRowType(), null,
          groupSet.asList());
    }

    @Override public List<? extends Type> keyTypes() {
      return EnumUtils.fieldTypes(typeFactory, keyRelTypes());
    }
  }

  protected void createAccumulatorAdders(
      final ParameterExpression inParameter,
      final List<AggImpState> aggs,
      final PhysType accPhysType,
      final ParameterExpression accExpr,
      final PhysType inputPhysType,
      final BlockBuilder builder,
      EnumerableRelImplementor implementor,
      JavaTypeFactory typeFactory) {
    for (int i = 0, stateOffset = 0; i < aggs.size(); i++) {
      final BlockBuilder builder2 = new BlockBuilder();
      final AggImpState agg = aggs.get(i);

      final int stateSize = requireNonNull(agg.state, "agg.state").size();
      final List<Expression> accumulator = new ArrayList<>(stateSize);
      for (int j = 0; j < stateSize; j++) {
        accumulator.add(accPhysType.fieldReference(accExpr, j + stateOffset));
      }
      agg.state = accumulator;

      stateOffset += stateSize;

      AggAddContext addContext =
          new AggAddContextImpl(builder2, accumulator) {
            @Override public List<RexNode> rexArguments() {
              List<RelDataTypeField> inputTypes =
                  inputPhysType.getRowType().getFieldList();
              List<RexNode> args = new ArrayList<>();
              for (int index : agg.call.getArgList()) {
                args.add(RexInputRef.of(index, inputTypes));
              }
              return args;
            }

            @Override public @Nullable RexNode rexFilterArgument() {
              return agg.call.filterArg < 0
                  ? null
                  : RexInputRef.of(agg.call.filterArg,
                      inputPhysType.getRowType());
            }

            @Override public RexToLixTranslator rowTranslator() {
              return RexToLixTranslator.forAggregation(typeFactory,
                  currentBlock(),
                  new RexToLixTranslator.InputGetterImpl(
                      Collections.singletonList(
                          Pair.of(inParameter, inputPhysType))),
                  implementor.getConformance());
            }
          };

      agg.implementor.implementAdd(requireNonNull(agg.context, "agg.context"), addContext);
      builder2.add(accExpr);
      agg.accumulatorAdder = builder.append("accumulatorAdder",
          Expressions.lambda(Function2.class, builder2.toBlock(), accExpr,
              inParameter));
    }
  }

  protected List<Type> createAggStateTypes(
      final List<Expression> initExpressions,
      final BlockBuilder initBlock,
      final List<AggImpState> aggs,
      JavaTypeFactory typeFactory) {
    final List<Type> aggStateTypes = new ArrayList<>();
    for (final AggImpState agg : aggs) {
      agg.context = new AggContextImpl(agg, typeFactory);
      final List<Type> state = agg.implementor.getStateType(agg.context);

      if (state.isEmpty()) {
        agg.state = ImmutableList.of();
        continue;
      }

      aggStateTypes.addAll(state);

      final List<Expression> decls = new ArrayList<>(state.size());
      for (int i = 0; i < state.size(); i++) {
        String aggName = "a" + agg.aggIdx;
        if (CalciteSystemProperty.DEBUG.value()) {
          aggName = Util.toJavaId(agg.call.getAggregation().getName(), 0)
              .substring("ID$0$".length()) + aggName;
        }
        Type type = state.get(i);
        ParameterExpression pe =
            Expressions.parameter(type,
                initBlock.newName(aggName + "s" + i));
        initBlock.add(Expressions.declare(0, pe, null));
        decls.add(pe);
      }
      agg.state = decls;
      initExpressions.addAll(decls);
      agg.implementor.implementReset(agg.context,
          new AggResultContextImpl(initBlock, agg.call, decls, null, null));
    }
    return aggStateTypes;
  }
}
