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
package org.apache.calcite.rel.rules;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static java.util.Objects.requireNonNull;

/**
 * Planner rule that pushes an
 * {@link org.apache.calcite.rel.core.Aggregate}
 * past a {@link org.apache.calcite.rel.core.Join}.
 *
 * @see CoreRules#AGGREGATE_JOIN_TRANSPOSE
 * @see CoreRules#AGGREGATE_JOIN_TRANSPOSE_EXTENDED
 */
@Value.Enclosing
public class AggregateJoinTransposeRule
    extends RelRule<AggregateJoinTransposeRule.Config>
    implements TransformationRule {

  /** Creates an AggregateJoinTransposeRule. */
  protected AggregateJoinTransposeRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public AggregateJoinTransposeRule(Class<? extends Aggregate> aggregateClass,
      Class<? extends Join> joinClass, RelBuilderFactory relBuilderFactory,
      boolean allowFunctions) {
    this(Config.DEFAULT
        .withRelBuilderFactory(relBuilderFactory)
        .as(Config.class)
        .withOperandFor(aggregateClass, joinClass, allowFunctions));
  }

  @Deprecated // to be removed before 2.0
  public AggregateJoinTransposeRule(Class<? extends Aggregate> aggregateClass,
      RelFactories.AggregateFactory aggregateFactory,
      Class<? extends Join> joinClass,
      RelFactories.JoinFactory joinFactory) {
    this(aggregateClass, joinClass,
        RelBuilder.proto(aggregateFactory, joinFactory), false);
  }

  @Deprecated // to be removed before 2.0
  public AggregateJoinTransposeRule(Class<? extends Aggregate> aggregateClass,
      RelFactories.AggregateFactory aggregateFactory,
      Class<? extends Join> joinClass,
      RelFactories.JoinFactory joinFactory,
      boolean allowFunctions) {
    this(aggregateClass, joinClass,
        RelBuilder.proto(aggregateFactory, joinFactory), allowFunctions);
  }

  @Deprecated // to be removed before 2.0
  public AggregateJoinTransposeRule(Class<? extends Aggregate> aggregateClass,
      RelFactories.AggregateFactory aggregateFactory,
      Class<? extends Join> joinClass,
      RelFactories.JoinFactory joinFactory,
      RelFactories.ProjectFactory projectFactory) {
    this(aggregateClass, joinClass,
        RelBuilder.proto(aggregateFactory, joinFactory, projectFactory), false);
  }

  @Deprecated // to be removed before 2.0
  public AggregateJoinTransposeRule(Class<? extends Aggregate> aggregateClass,
      RelFactories.AggregateFactory aggregateFactory,
      Class<? extends Join> joinClass,
      RelFactories.JoinFactory joinFactory,
      RelFactories.ProjectFactory projectFactory,
      boolean allowFunctions) {
    this(aggregateClass, joinClass,
        RelBuilder.proto(aggregateFactory, joinFactory, projectFactory),
        allowFunctions);
  }

  private static boolean isAggregateSupported(Aggregate aggregate,
      boolean allowFunctions) {
    if (!allowFunctions && !aggregate.getAggCallList().isEmpty()) {
      return false;
    }
    if (aggregate.getGroupType() != Aggregate.Group.SIMPLE) {
      return false;
    }
    // If any aggregate functions do not support splitting, bail out
    // If any aggregate call has a filter or is distinct, bail out
    for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
      if (aggregateCall.getAggregation().unwrap(SqlSplittableAggFunction.class)
          == null) {
        return false;
      }
      if (aggregateCall.filterArg >= 0 || aggregateCall.isDistinct()) {
        return false;
      }
    }
    return true;
  }

  // OUTER joins are supported for group by without aggregate functions
  // FULL OUTER JOIN is not supported since it could produce wrong result
  // due to bug (CALCITE-3012)
  private static boolean isJoinSupported(final Join join, final Aggregate aggregate) {
    return join.getJoinType() == JoinRelType.INNER || aggregate.getAggCallList().isEmpty();
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final Join join = call.rel(1);
    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    final RelBuilder relBuilder = call.builder();

    if (!isJoinSupported(join, aggregate)) {
      return;
    }

    // Do the columns used by the join appear in the output of the aggregate?
    final ImmutableBitSet aggregateColumns = aggregate.getGroupSet();
    final RelMetadataQuery mq = call.getMetadataQuery();
    final ImmutableBitSet keyColumns = keyColumns(aggregateColumns,
        mq.getPulledUpPredicates(join).pulledUpPredicates);
    final ImmutableBitSet joinColumns =
        RelOptUtil.InputFinder.bits(join.getCondition());
    final boolean allColumnsInAggregate =
        keyColumns.contains(joinColumns);
    final ImmutableBitSet belowAggregateColumns =
        aggregateColumns.union(joinColumns);

    // Split join condition
    final List<Integer> leftKeys = new ArrayList<>();
    final List<Integer> rightKeys = new ArrayList<>();
    final List<Boolean> filterNulls = new ArrayList<>();
    RexNode nonEquiConj =
        RelOptUtil.splitJoinCondition(join.getLeft(), join.getRight(),
            join.getCondition(), leftKeys, rightKeys, filterNulls);
    // If it contains non-equi join conditions, we bail out
    if (!nonEquiConj.isAlwaysTrue()) {
      return;
    }

    // Push each aggregate function down to each side that contains all of its
    // arguments. Note that COUNT(*), because it has no arguments, can go to
    // both sides.
    final Map<Integer, Integer> map = new HashMap<>();
    final List<Side> sides = new ArrayList<>();
    int uniqueCount = 0;
    int offset = 0;
    int belowOffset = 0;
    for (int s = 0; s < 2; s++) {
      final Side side = new Side();
      final RelNode joinInput = join.getInput(s);
      int fieldCount = joinInput.getRowType().getFieldCount();
      final ImmutableBitSet fieldSet =
          ImmutableBitSet.range(offset, offset + fieldCount);
      final ImmutableBitSet belowAggregateKeyNotShifted =
          belowAggregateColumns.intersect(fieldSet);
      for (Ord<Integer> c : Ord.zip(belowAggregateKeyNotShifted)) {
        map.put(c.e, belowOffset + c.i);
      }
      final Mappings.TargetMapping mapping =
          s == 0
              ? Mappings.createIdentity(fieldCount)
              : Mappings.createShiftMapping(fieldCount + offset, 0, offset,
                  fieldCount);
      final ImmutableBitSet belowAggregateKey =
          belowAggregateKeyNotShifted.shift(-offset);
      final boolean unique;
      if (!config.isAllowFunctions()) {
        assert aggregate.getAggCallList().isEmpty();
        // If there are no functions, it doesn't matter as much whether we
        // aggregate the inputs before the join, because there will not be
        // any functions experiencing a cartesian product effect.
        //
        // But finding out whether the input is already unique requires a call
        // to areColumnsUnique that currently (until [CALCITE-1048] "Make
        // metadata more robust" is fixed) places a heavy load on
        // the metadata system.
        //
        // So we choose to imagine the input is already unique, which is
        // untrue but harmless.
        //
        Util.discard(Bug.CALCITE_1048_FIXED);
        unique = true;
      } else {
        final Boolean unique0 =
            mq.areColumnsUnique(joinInput, belowAggregateKey);
        unique = unique0 != null && unique0;
      }
      if (unique) {
        ++uniqueCount;
        side.aggregate = false;
        relBuilder.push(joinInput);
        final List<RexNode> projects = new ArrayList<>();
        for (Integer i : belowAggregateKey) {
          projects.add(relBuilder.field(i));
        }
        for (Ord<AggregateCall> aggCall : Ord.zip(aggregate.getAggCallList())) {
          final SqlAggFunction aggregation = aggCall.e.getAggregation();
          final SqlSplittableAggFunction splitter =
              aggregation.unwrapOrThrow(SqlSplittableAggFunction.class);
          if (!aggCall.e.getArgList().isEmpty()
              && fieldSet.contains(ImmutableBitSet.of(aggCall.e.getArgList()))) {
            final RexNode singleton = splitter.singleton(rexBuilder,
                joinInput.getRowType(), aggCall.e.transform(mapping));

            if (singleton instanceof RexInputRef) {
              final int index = ((RexInputRef) singleton).getIndex();
              if (!belowAggregateKey.get(index)) {
                projects.add(singleton);
                side.split.put(aggCall.i, projects.size() - 1);
              } else {
                side.split.put(aggCall.i, index);
              }
            } else {
              projects.add(singleton);
              side.split.put(aggCall.i, projects.size() - 1);
            }
          }
        }
        relBuilder.project(projects);
        side.newInput = relBuilder.build();
      } else {
        side.aggregate = true;
        List<AggregateCall> belowAggCalls = new ArrayList<>();
        final SqlSplittableAggFunction.Registry<AggregateCall>
            belowAggCallRegistry = registry(belowAggCalls);
        final int oldGroupKeyCount = aggregate.getGroupCount();
        final int newGroupKeyCount = belowAggregateKey.cardinality();
        for (Ord<AggregateCall> aggCall : Ord.zip(aggregate.getAggCallList())) {
          final SqlAggFunction aggregation = aggCall.e.getAggregation();
          final SqlSplittableAggFunction splitter =
              aggregation.unwrapOrThrow(SqlSplittableAggFunction.class);
          final AggregateCall call1;
          if (fieldSet.contains(ImmutableBitSet.of(aggCall.e.getArgList()))) {
            final AggregateCall splitCall = splitter.split(aggCall.e, mapping);
            call1 = splitCall.adaptTo(joinInput, splitCall.getArgList(),
                splitCall.filterArg, oldGroupKeyCount, newGroupKeyCount);
          } else {
            call1 = splitter.other(rexBuilder.getTypeFactory(), aggCall.e);
          }
          if (call1 != null) {
            side.split.put(aggCall.i,
                belowAggregateKey.cardinality()
                    + belowAggCallRegistry.register(call1));
          }
        }
        side.newInput = relBuilder.push(joinInput)
            .aggregate(relBuilder.groupKey(belowAggregateKey), belowAggCalls)
            .build();
      }
      offset += fieldCount;
      belowOffset += side.newInput.getRowType().getFieldCount();
      sides.add(side);
    }

    if (uniqueCount == 2) {
      // Both inputs to the join are unique. There is nothing to be gained by
      // this rule. In fact, this aggregate+join may be the result of a previous
      // invocation of this rule; if we continue we might loop forever.
      return;
    }

    // Update condition
    final Mapping mapping = (Mapping) Mappings.target(
        map::get,
        join.getRowType().getFieldCount(),
        belowOffset);
    final RexNode newCondition =
        RexUtil.apply(mapping, join.getCondition());

    // Create new join
    RelNode side0 = requireNonNull(sides.get(0).newInput, "sides.get(0).newInput");
    relBuilder.push(side0)
        .push(requireNonNull(sides.get(1).newInput, "sides.get(1).newInput"))
        .join(join.getJoinType(), newCondition);

    // Aggregate above to sum up the sub-totals
    final List<AggregateCall> newAggCalls = new ArrayList<>();
    final int groupCount = aggregate.getGroupCount();
    final int newLeftWidth = side0.getRowType().getFieldCount();
    final List<RexNode> projects =
        new ArrayList<>(
            rexBuilder.identityProjects(relBuilder.peek().getRowType()));
    for (Ord<AggregateCall> aggCall : Ord.zip(aggregate.getAggCallList())) {
      final SqlAggFunction aggregation = aggCall.e.getAggregation();
      final SqlSplittableAggFunction splitter =
          aggregation.unwrapOrThrow(SqlSplittableAggFunction.class);
      final Integer leftSubTotal = sides.get(0).split.get(aggCall.i);
      final Integer rightSubTotal = sides.get(1).split.get(aggCall.i);
      newAggCalls.add(
          splitter.topSplit(rexBuilder, registry(projects),
              groupCount, relBuilder.peek().getRowType(), aggCall.e,
              leftSubTotal == null ? -1 : leftSubTotal,
              rightSubTotal == null ? -1 : rightSubTotal + newLeftWidth));
    }

    relBuilder.project(projects);

    boolean aggConvertedToProjects = false;
    if (allColumnsInAggregate && join.getJoinType() != JoinRelType.FULL) {
      // let's see if we can convert aggregate into projects
      // This shouldn't be done for FULL OUTER JOIN, aggregate on top is always required
      List<RexNode> projects2 = new ArrayList<>();
      for (int key : Mappings.apply(mapping, aggregate.getGroupSet())) {
        projects2.add(relBuilder.field(key));
      }
      for (AggregateCall newAggCall : newAggCalls) {
        newAggCall.getAggregation().maybeUnwrap(SqlSplittableAggFunction.class)
            .ifPresent(splitter -> {
              final RelDataType rowType = relBuilder.peek().getRowType();
              projects2.add(splitter.singleton(rexBuilder, rowType, newAggCall));
            });
      }
      if (projects2.size()
          == aggregate.getGroupSet().cardinality() + newAggCalls.size()) {
        // We successfully converted agg calls into projects.
        relBuilder.project(projects2);
        aggConvertedToProjects = true;
      }
    }

    if (!aggConvertedToProjects) {
      relBuilder.aggregate(
          relBuilder.groupKey(Mappings.apply(mapping, aggregate.getGroupSet()),
              Mappings.apply2(mapping, aggregate.getGroupSets())),
          newAggCalls);
    }

    call.transformTo(relBuilder.build());
  }

  /** Computes the closure of a set of columns according to a given list of
   * constraints. Each 'x = y' constraint causes bit y to be set if bit x is
   * set, and vice versa. */
  private static ImmutableBitSet keyColumns(ImmutableBitSet aggregateColumns,
      ImmutableList<RexNode> predicates) {
    NavigableMap<Integer, BitSet> equivalence = new TreeMap<>();
    for (RexNode predicate : predicates) {
      populateEquivalences(equivalence, predicate);
    }
    ImmutableBitSet keyColumns = aggregateColumns;
    for (Integer aggregateColumn : aggregateColumns) {
      final BitSet bitSet = equivalence.get(aggregateColumn);
      if (bitSet != null) {
        keyColumns = keyColumns.union(bitSet);
      }
    }
    return keyColumns;
  }

  private static void populateEquivalences(Map<Integer, BitSet> equivalence,
      RexNode predicate) {
    switch (predicate.getKind()) {
    case EQUALS:
      RexCall call = (RexCall) predicate;
      final List<RexNode> operands = call.getOperands();
      if (operands.get(0) instanceof RexInputRef) {
        final RexInputRef ref0 = (RexInputRef) operands.get(0);
        if (operands.get(1) instanceof RexInputRef) {
          final RexInputRef ref1 = (RexInputRef) operands.get(1);
          populateEquivalence(equivalence, ref0.getIndex(), ref1.getIndex());
          populateEquivalence(equivalence, ref1.getIndex(), ref0.getIndex());
        }
      }
      break;
    default:
      break;
    }
  }

  private static void populateEquivalence(Map<Integer, BitSet> equivalence,
      int i0, int i1) {
    BitSet bitSet = equivalence.get(i0);
    if (bitSet == null) {
      bitSet = new BitSet();
      equivalence.put(i0, bitSet);
    }
    bitSet.set(i1);
  }

  /** Creates a {@link org.apache.calcite.sql.SqlSplittableAggFunction.Registry}
   * that is a view of a list. */
  private static <E> SqlSplittableAggFunction.Registry<E> registry(
      final List<E> list) {
    return e -> {
      int i = list.indexOf(e);
      if (i < 0) {
        i = list.size();
        list.add(e);
      }
      return i;
    };
  }

  /** Work space for an input to a join. */
  private static class Side {
    final Map<Integer, Integer> split = new HashMap<>();
    @Nullable RelNode newInput;
    boolean aggregate;
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableAggregateJoinTransposeRule.Config.of()
        .withOperandFor(LogicalAggregate.class, LogicalJoin.class, false);

    /** Extended instance that can push down aggregate functions. */
    Config EXTENDED = ImmutableAggregateJoinTransposeRule.Config.of()
        .withOperandFor(LogicalAggregate.class, LogicalJoin.class, true);

    @Override default AggregateJoinTransposeRule toRule() {
      return new AggregateJoinTransposeRule(this);
    }

    /** Whether to push down aggregate functions, default false. */
    @Value.Default
    default boolean isAllowFunctions() {
      return false;
    }

    /** Sets {@link #isAllowFunctions()}. */
    Config withAllowFunctions(boolean allowFunctions);

    /** Defines an operand tree for the given classes, and also sets
     * {@link #isAllowFunctions()}. */
    default Config withOperandFor(Class<? extends Aggregate> aggregateClass,
        Class<? extends Join> joinClass, boolean allowFunctions) {
      return withAllowFunctions(allowFunctions)
          .withOperandSupplier(b0 ->
              b0.operand(aggregateClass)
                  .predicate(agg -> isAggregateSupported(agg, allowFunctions))
              .oneInput(b1 ->
                  b1.operand(joinClass).anyInputs()))
          .as(Config.class);
    }
  }
}
