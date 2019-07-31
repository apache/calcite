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
package org.apache.calcite.interpreter;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.AggAddContext;
import org.apache.calcite.adapter.enumerable.AggImpState;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.enumerable.impl.AggAddContextImpl;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.interpreter.Row.RowBuilder;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Interpreter node that implements an
 * {@link org.apache.calcite.rel.core.Aggregate}.
 */
public class AggregateNode extends AbstractSingleNode<Aggregate> {
  private final List<Grouping> groups = new ArrayList<>();
  private final ImmutableBitSet unionGroups;
  private final int outputRowLength;
  private final ImmutableList<AccumulatorFactory> accumulatorFactories;
  private final DataContext dataContext;

  public AggregateNode(Compiler compiler, Aggregate rel) {
    super(compiler, rel);
    this.dataContext = compiler.getDataContext();

    ImmutableBitSet union = ImmutableBitSet.of();

    if (rel.getGroupSets() != null) {
      for (ImmutableBitSet group : rel.getGroupSets()) {
        union = union.union(group);
        groups.add(new Grouping(group));
      }
    }

    this.unionGroups = union;
    this.outputRowLength = unionGroups.cardinality()
        + rel.getAggCallList().size();

    ImmutableList.Builder<AccumulatorFactory> builder = ImmutableList.builder();
    for (AggregateCall aggregateCall : rel.getAggCallList()) {
      builder.add(getAccumulator(aggregateCall, false));
    }
    accumulatorFactories = builder.build();
  }

  public void run() throws InterruptedException {
    Row r;
    while ((r = source.receive()) != null) {
      for (Grouping group : groups) {
        group.send(r);
      }
    }

    for (Grouping group : groups) {
      group.end(sink);
    }
  }

  private AccumulatorFactory getAccumulator(final AggregateCall call,
      boolean ignoreFilter) {
    if (call.filterArg >= 0 && !ignoreFilter) {
      final AccumulatorFactory factory = getAccumulator(call, true);
      return () -> {
        final Accumulator accumulator = factory.get();
        return new FilterAccumulator(accumulator, call.filterArg);
      };
    }
    if (call.getAggregation() == SqlStdOperatorTable.COUNT) {
      return () -> new CountAccumulator(call);
    } else if (call.getAggregation() == SqlStdOperatorTable.SUM
        || call.getAggregation() == SqlStdOperatorTable.SUM0) {
      final Class<?> clazz;
      switch (call.type.getSqlTypeName()) {
      case DOUBLE:
      case REAL:
      case FLOAT:
        clazz = DoubleSum.class;
        break;
      case INTEGER:
        clazz = IntSum.class;
        break;
      case BIGINT:
      default:
        clazz = LongSum.class;
        break;
      }
      if (call.getAggregation() == SqlStdOperatorTable.SUM) {
        return new UdaAccumulatorFactory(
            AggregateFunctionImpl.create(clazz), call, true);
      } else {
        return new UdaAccumulatorFactory(
            AggregateFunctionImpl.create(clazz), call, false);
      }
    } else if (call.getAggregation() == SqlStdOperatorTable.MIN) {
      final Class<?> clazz;
      switch (call.getType().getSqlTypeName()) {
      case INTEGER:
        clazz = MinInt.class;
        break;
      case FLOAT:
        clazz = MinFloat.class;
        break;
      case DOUBLE:
      case REAL:
        clazz = MinDouble.class;
        break;
      default:
        clazz = MinLong.class;
        break;
      }
      return new UdaAccumulatorFactory(
          AggregateFunctionImpl.create(clazz), call, true);
    } else if (call.getAggregation() == SqlStdOperatorTable.MAX) {
      final Class<?> clazz;
      switch (call.getType().getSqlTypeName()) {
      case INTEGER:
        clazz = MaxInt.class;
        break;
      case FLOAT:
        clazz = MaxFloat.class;
        break;
      case DOUBLE:
      case REAL:
        clazz = MaxDouble.class;
        break;
      default:
        clazz = MaxLong.class;
        break;
      }
      return new UdaAccumulatorFactory(
          AggregateFunctionImpl.create(clazz), call, true);
    } else {
      final JavaTypeFactory typeFactory =
          (JavaTypeFactory) rel.getCluster().getTypeFactory();
      int stateOffset = 0;
      final AggImpState agg = new AggImpState(0, call, false);
      int stateSize = agg.state.size();

      final BlockBuilder builder2 = new BlockBuilder();
      final PhysType inputPhysType =
          PhysTypeImpl.of(typeFactory, rel.getInput().getRowType(),
              JavaRowFormat.ARRAY);
      final RelDataTypeFactory.Builder builder = typeFactory.builder();
      for (Expression expression : agg.state) {
        builder.add("a",
            typeFactory.createJavaType((Class) expression.getType()));
      }
      final PhysType accPhysType =
          PhysTypeImpl.of(typeFactory, builder.build(), JavaRowFormat.ARRAY);
      final ParameterExpression inParameter =
          Expressions.parameter(inputPhysType.getJavaRowType(), "in");
      final ParameterExpression acc_ =
          Expressions.parameter(accPhysType.getJavaRowType(), "acc");

      List<Expression> accumulator = new ArrayList<>(stateSize);
      for (int j = 0; j < stateSize; j++) {
        accumulator.add(accPhysType.fieldReference(acc_, j + stateOffset));
      }
      agg.state = accumulator;

      AggAddContext addContext =
          new AggAddContextImpl(builder2, accumulator) {
            public List<RexNode> rexArguments() {
              List<RexNode> args = new ArrayList<>();
              for (int index : agg.call.getArgList()) {
                args.add(RexInputRef.of(index, inputPhysType.getRowType()));
              }
              return args;
            }

            public RexNode rexFilterArgument() {
              return agg.call.filterArg < 0
                  ? null
                  : RexInputRef.of(agg.call.filterArg,
                      inputPhysType.getRowType());
            }

            public RexToLixTranslator rowTranslator() {
              final SqlConformance conformance =
                  SqlConformanceEnum.DEFAULT; // TODO: get this from implementor
              return RexToLixTranslator.forAggregation(typeFactory,
                  currentBlock(),
                  new RexToLixTranslator.InputGetterImpl(
                      Collections.singletonList(
                          Pair.of((Expression) inParameter, inputPhysType))),
                  conformance)
                  .setNullable(currentNullables());
            }
          };

      agg.implementor.implementAdd(agg.context, addContext);

      final ParameterExpression context_ =
          Expressions.parameter(Context.class, "context");
      final ParameterExpression outputValues_ =
          Expressions.parameter(Object[].class, "outputValues");
      Scalar addScalar =
          JaninoRexCompiler.baz(context_, outputValues_, builder2.toBlock());
      return new ScalarAccumulatorDef(null, addScalar, null,
          rel.getInput().getRowType().getFieldCount(), stateSize, dataContext);
    }
  }

  /** Accumulator for calls to the COUNT function. */
  private static class CountAccumulator implements Accumulator {
    private final AggregateCall call;
    long cnt;

    CountAccumulator(AggregateCall call) {
      this.call = call;
      cnt = 0;
    }

    public void send(Row row) {
      boolean notNull = true;
      for (Integer i : call.getArgList()) {
        if (row.getObject(i) == null) {
          notNull = false;
          break;
        }
      }
      if (notNull) {
        cnt++;
      }
    }

    public Object end() {
      return cnt;
    }
  }

  /** Creates an {@link Accumulator}. */
  private interface AccumulatorFactory extends Supplier<Accumulator> {
  }

  /** Accumulator powered by {@link Scalar} code fragments. */
  private static class ScalarAccumulatorDef implements AccumulatorFactory {
    final Scalar initScalar;
    final Scalar addScalar;
    final Scalar endScalar;
    final Context sendContext;
    final Context endContext;
    final int rowLength;
    final int accumulatorLength;

    private ScalarAccumulatorDef(Scalar initScalar, Scalar addScalar,
        Scalar endScalar, int rowLength, int accumulatorLength,
        DataContext root) {
      this.initScalar = initScalar;
      this.addScalar = addScalar;
      this.endScalar = endScalar;
      this.accumulatorLength = accumulatorLength;
      this.rowLength = rowLength;
      this.sendContext = new Context(root);
      this.sendContext.values = new Object[rowLength + accumulatorLength];
      this.endContext = new Context(root);
      this.endContext.values = new Object[accumulatorLength];
    }

    public Accumulator get() {
      return new ScalarAccumulator(this, new Object[accumulatorLength]);
    }
  }

  /** Accumulator powered by {@link Scalar} code fragments. */
  private static class ScalarAccumulator implements Accumulator {
    final ScalarAccumulatorDef def;
    final Object[] values;

    private ScalarAccumulator(ScalarAccumulatorDef def, Object[] values) {
      this.def = def;
      this.values = values;
    }

    public void send(Row row) {
      System.arraycopy(row.getValues(), 0, def.sendContext.values, 0,
          def.rowLength);
      System.arraycopy(values, 0, def.sendContext.values, def.rowLength,
          values.length);
      def.addScalar.execute(def.sendContext, values);
    }

    public Object end() {
      System.arraycopy(values, 0, def.endContext.values, 0, values.length);
      return def.endScalar.execute(def.endContext);
    }
  }

  /**
   * Internal class to track groupings.
   */
  private class Grouping {
    private final ImmutableBitSet grouping;
    private final Map<Row, AccumulatorList> accumulators = new HashMap<>();

    private Grouping(ImmutableBitSet grouping) {
      this.grouping = grouping;
    }

    public void send(Row row) {
      // TODO: fix the size of this row.
      RowBuilder builder = Row.newBuilder(grouping.cardinality());
      int j = 0;
      for (Integer i : grouping) {
        builder.set(j++, row.getObject(i));
      }
      Row key = builder.build();

      if (!accumulators.containsKey(key)) {
        AccumulatorList list = new AccumulatorList();
        for (AccumulatorFactory factory : accumulatorFactories) {
          list.add(factory.get());
        }
        accumulators.put(key, list);
      }

      accumulators.get(key).send(row);
    }

    public void end(Sink sink) throws InterruptedException {
      for (Map.Entry<Row, AccumulatorList> e : accumulators.entrySet()) {
        final Row key = e.getKey();
        final AccumulatorList list = e.getValue();

        RowBuilder rb = Row.newBuilder(outputRowLength);
        int index = 0;
        for (Integer groupPos : unionGroups) {
          if (grouping.get(groupPos)) {
            rb.set(index, key.getObject(index));
          }
          // need to set false when not part of grouping set.

          index++;
        }

        list.end(rb);

        sink.send(rb.build());
      }
    }
  }

  /**
   * A list of accumulators used during grouping.
   */
  private static class AccumulatorList extends ArrayList<Accumulator> {
    public void send(Row row) {
      for (Accumulator a : this) {
        a.send(row);
      }
    }

    public void end(RowBuilder r) {
      for (int accIndex = 0, rowIndex = r.size() - size();
          rowIndex < r.size(); rowIndex++, accIndex++) {
        r.set(rowIndex, get(accIndex).end());
      }
    }
  }

  /**
   * Defines function implementation for
   * things like {@code count()} and {@code sum()}.
   */
  private interface Accumulator {
    void send(Row row);
    Object end();
  }

  /** Implementation of {@code SUM} over INTEGER values as a user-defined
   * aggregate. */
  public static class IntSum {
    public IntSum() {
    }
    public int init() {
      return 0;
    }
    public int add(int accumulator, int v) {
      return accumulator + v;
    }
    public int merge(int accumulator0, int accumulator1) {
      return accumulator0 + accumulator1;
    }
    public int result(int accumulator) {
      return accumulator;
    }
  }

  /** Implementation of {@code SUM} over BIGINT values as a user-defined
   * aggregate. */
  public static class LongSum {
    public LongSum() {
    }
    public long init() {
      return 0L;
    }
    public long add(long accumulator, long v) {
      return accumulator + v;
    }
    public long merge(long accumulator0, long accumulator1) {
      return accumulator0 + accumulator1;
    }
    public long result(long accumulator) {
      return accumulator;
    }
  }

  /** Implementation of {@code SUM} over DOUBLE values as a user-defined
   * aggregate. */
  public static class DoubleSum {
    public DoubleSum() {
    }
    public double init() {
      return 0D;
    }
    public double add(double accumulator, double v) {
      return accumulator + v;
    }
    public double merge(double accumulator0, double accumulator1) {
      return accumulator0 + accumulator1;
    }
    public double result(double accumulator) {
      return accumulator;
    }
  }

  /** Common implementation of comparison aggregate methods over numeric
   * values as a user-defined aggregate.
   * @param <T> The numeric type
   */
  public static class NumericComparison<T> {
    private final T initialValue;
    private final BiFunction<T, T, T> comparisonFunction;

    public NumericComparison(T initialValue, BiFunction<T, T, T> comparisonFunction) {
      this.initialValue = initialValue;
      this.comparisonFunction = comparisonFunction;
    }

    public T init() {
      return this.initialValue;
    }

    public T add(T accumulator, T value) {
      return this.comparisonFunction.apply(accumulator, value);
    }

    public T merge(T accumulator0, T accumulator1) {
      return add(accumulator0, accumulator1);
    }

    public T result(T accumulator) {
      return accumulator;
    }
  }

  /** Implementation of {@code MIN} function to calculate the minimum of
   * {@code integer} values as a user-defined aggregate.
   */
  public static class MinInt extends NumericComparison<Integer> {
    public MinInt() {
      super(Integer.MAX_VALUE, Math::min);
    }
  }

  /** Implementation of {@code MIN} function to calculate the minimum of
   * {@code long} values as a user-defined aggregate.
   */
  public static class MinLong extends NumericComparison<Long> {
    public MinLong() {
      super(Long.MAX_VALUE, Math::min);
    }
  }

  /** Implementation of {@code MIN} function to calculate the minimum of
   * {@code float} values as a user-defined aggregate.
   */
  public static class MinFloat extends NumericComparison<Float> {
    public MinFloat() {
      super(Float.MAX_VALUE, Math::min);
    }
  }

  /** Implementation of {@code MIN} function to calculate the minimum of
   * {@code double} and {@code real} values as a user-defined aggregate.
   */
  public static class MinDouble extends NumericComparison<Double> {
    public MinDouble() {
      super(Double.MAX_VALUE, Math::min);
    }
  }

  /** Implementation of {@code MAX} function to calculate the minimum of
   * {@code integer} values as a user-defined aggregate.
   */
  public static class MaxInt extends NumericComparison<Integer> {
    public MaxInt() {
      super(Integer.MIN_VALUE, Math::max);
    }
  }

  /** Implementation of {@code MAX} function to calculate the minimum of
   * {@code long} values as a user-defined aggregate.
   */
  public static class MaxLong extends NumericComparison<Long> {
    public MaxLong() {
      super(Long.MIN_VALUE, Math::max);
    }
  }

  /** Implementation of {@code MAX} function to calculate the minimum of
   * {@code float} values as a user-defined aggregate.
   */
  public static class MaxFloat extends NumericComparison<Float> {
    public MaxFloat() {
      super(Float.MIN_VALUE, Math::max);
    }
  }

  /** Implementation of {@code MAX} function to calculate the minimum of
   * {@code double} and {@code real} values as a user-defined aggregate.
   */
  public static class MaxDouble extends NumericComparison<Double> {
    public MaxDouble() {
      super(Double.MIN_VALUE, Math::max);
    }
  }

  /** Accumulator factory based on a user-defined aggregate function. */
  private static class UdaAccumulatorFactory implements AccumulatorFactory {
    final AggregateFunctionImpl aggFunction;
    final int argOrdinal;
    public final Object instance;
    public final boolean nullIfEmpty;

    UdaAccumulatorFactory(AggregateFunctionImpl aggFunction,
        AggregateCall call, boolean nullIfEmpty) {
      this.aggFunction = aggFunction;
      if (call.getArgList().size() != 1) {
        throw new UnsupportedOperationException("in current implementation, "
            + "aggregate must have precisely one argument");
      }
      argOrdinal = call.getArgList().get(0);
      if (aggFunction.isStatic) {
        instance = null;
      } else {
        try {
          final Constructor<?> constructor =
              aggFunction.declaringClass.getConstructor();
          instance = constructor.newInstance();
        } catch (InstantiationException | IllegalAccessException
            | NoSuchMethodException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }
      this.nullIfEmpty = nullIfEmpty;
    }

    public Accumulator get() {
      return new UdaAccumulator(this);
    }
  }

  /** Accumulator based upon a user-defined aggregate. */
  private static class UdaAccumulator implements Accumulator {
    private final UdaAccumulatorFactory factory;
    private Object value;
    private boolean empty;

    UdaAccumulator(UdaAccumulatorFactory factory) {
      this.factory = factory;
      try {
        this.value = factory.aggFunction.initMethod.invoke(factory.instance);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
      this.empty = true;
    }

    public void send(Row row) {
      final Object[] args = {value, row.getValues()[factory.argOrdinal]};
      for (int i = 1; i < args.length; i++) {
        if (args[i] == null) {
          return; // one of the arguments is null; don't add to the total
        }
      }
      try {
        value = factory.aggFunction.addMethod.invoke(factory.instance, args);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
      empty = false;
    }

    public Object end() {
      if (factory.nullIfEmpty && empty) {
        return null;
      }
      final Object[] args = {value};
      try {
        return factory.aggFunction.resultMethod.invoke(factory.instance, args);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** Accumulator that applies a filter to another accumulator.
   * The filter is a BOOLEAN field in the input row. */
  private static class FilterAccumulator implements Accumulator {
    private final Accumulator accumulator;
    private final int filterArg;

    FilterAccumulator(Accumulator accumulator, int filterArg) {
      this.accumulator = accumulator;
      this.filterArg = filterArg;
    }

    public void send(Row row) {
      if (row.getValues()[filterArg] == Boolean.TRUE) {
        accumulator.send(row);
      }
    }

    public Object end() {
      return accumulator.end();
    }
  }
}

// End AggregateNode.java
