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
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlAnyValueAggFunction;
import org.apache.calcite.sql.fun.SqlBitOpAggFunction;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlMinMaxAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;

/**
 * Planner rule that pushes an
 * {@link org.apache.calcite.rel.core.Aggregate}
 * past a non-distinct {@link org.apache.calcite.rel.core.Union}.
 *
 * @see CoreRules#AGGREGATE_UNION_TRANSPOSE
 */
@Value.Enclosing
public class AggregateUnionTransposeRule
    extends RelRule<AggregateUnionTransposeRule.Config>
    implements TransformationRule {

  private static final IdentityHashMap<Class<? extends SqlAggFunction>, Boolean>
      SUPPORTED_AGGREGATES = new IdentityHashMap<>();

  static {
    SUPPORTED_AGGREGATES.put(SqlMinMaxAggFunction.class, true);
    SUPPORTED_AGGREGATES.put(SqlCountAggFunction.class, true);
    SUPPORTED_AGGREGATES.put(SqlSumAggFunction.class, true);
    SUPPORTED_AGGREGATES.put(SqlSumEmptyIsZeroAggFunction.class, true);
    SUPPORTED_AGGREGATES.put(SqlAnyValueAggFunction.class, true);
    SUPPORTED_AGGREGATES.put(SqlBitOpAggFunction.class, true);
  }

  /** Creates an AggregateUnionTransposeRule. */
  protected AggregateUnionTransposeRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public AggregateUnionTransposeRule(Class<? extends Aggregate> aggregateClass,
      Class<? extends Union> unionClass, RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT
        .withRelBuilderFactory(relBuilderFactory)
        .as(Config.class)
        .withOperandFor(aggregateClass, unionClass));
  }

  @Deprecated // to be removed before 2.0
  public AggregateUnionTransposeRule(Class<? extends Aggregate> aggregateClass,
      RelFactories.AggregateFactory aggregateFactory,
      Class<? extends Union> unionClass,
      RelFactories.SetOpFactory setOpFactory) {
    this(aggregateClass, unionClass,
        RelBuilder.proto(aggregateFactory, setOpFactory));
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Aggregate aggRel = call.rel(0);
    Union union = call.rel(1);

    if (!union.all) {
      // This transformation is only valid for UNION ALL.
      // Consider t1(i) with rows (5), (5) and t2(i) with
      // rows (5), (10), and the query
      // select sum(i) from (select i from t1) union (select i from t2).
      // The correct answer is 15.  If we apply the transformation,
      // we get
      // select sum(i) from
      // (select sum(i) as i from t1) union (select sum(i) as i from t2)
      // which yields 25 (incorrect).
      return;
    }

    int groupCount = aggRel.getGroupSet().cardinality();

    List<AggregateCall> transformedAggCalls =
        transformAggCalls(
            aggRel.copy(aggRel.getTraitSet(), aggRel.getInput(),
                aggRel.getGroupSet(), null, aggRel.getAggCallList()),
            groupCount, aggRel.getAggCallList());
    if (transformedAggCalls == null) {
      // we've detected the presence of something like AVG,
      // which we can't handle
      return;
    }

    boolean hasUniqueKeyInAllInputs = true;
    final RelMetadataQuery mq = call.getMetadataQuery();
    for (RelNode input : union.getInputs()) {
      boolean alreadyUnique =
          RelMdUtil.areColumnsDefinitelyUnique(mq, input,
              aggRel.getGroupSet());

      if (!alreadyUnique) {
        hasUniqueKeyInAllInputs = false;
        break;
      }
    }

    if (hasUniqueKeyInAllInputs) {
      // none of the children could benefit from the push-down,
      // so bail out (preventing the infinite loop to which most
      // planners would succumb)
      return;
    }

    // create corresponding aggregates on top of each union child
    final RelBuilder relBuilder = call.builder();
    RelDataType origUnionType = union.getRowType();
    for (RelNode input : union.getInputs()) {
      List<AggregateCall> childAggCalls = new ArrayList<>(aggRel.getAggCallList());
      // if the nullability of a specific input column differs from the nullability
      // of the union'ed column, we need to re-evaluate the nullability of the aggregate
      RelDataType inputRowType = input.getRowType();
      for (int i = 0; i < childAggCalls.size(); ++i) {
        AggregateCall origCall = aggRel.getAggCallList().get(i);
        if (origCall.getAggregation() == SqlStdOperatorTable.COUNT) {
          continue;
        }
        assert origCall.getArgList().size() == 1;
        int field = origCall.getArgList().get(0);
        if (origUnionType.getFieldList().get(field).getType().isNullable()
            != inputRowType.getFieldList().get(field).getType().isNullable()) {
          AggregateCall newCall =
              AggregateCall.create(origCall.getParserPosition(), origCall.getAggregation(),
                  origCall.isDistinct(), origCall.isApproximate(), origCall.ignoreNulls(),
                  origCall.rexList, origCall.getArgList(), -1, origCall.distinctKeys,
                  origCall.collation, groupCount, input, null, origCall.getName());
          childAggCalls.set(i, newCall);
        }
      }
      relBuilder.push(input);
      relBuilder.aggregate(relBuilder.groupKey(aggRel.getGroupSet()),
          childAggCalls);
    }

    // create a new union whose children are the aggregates created above
    relBuilder.union(true, union.getInputs().size());

    // Create the top aggregate. We must adjust group key indexes of the
    // original aggregate. E.g., if the original tree was:
    //
    // Aggregate[groupSet=$1, ...]
    //   Union[...]
    //
    // Then the new tree should be:
    // Aggregate[groupSet=$0, ...]
    //   Union[...]
    //     Aggregate[groupSet=$1, ...]
    ImmutableBitSet groupSet = aggRel.getGroupSet();
    Mapping topGroupMapping =
        Mappings.create(MappingType.INVERSE_SURJECTION,
            union.getRowType().getFieldCount(), aggRel.getGroupCount());
    for (int i = 0; i < groupSet.cardinality(); i++) {
      topGroupMapping.set(groupSet.nth(i), i);
    }

    ImmutableBitSet topGroupSet = Mappings.apply(topGroupMapping, groupSet);
    ImmutableList<ImmutableBitSet> topGroupSets =
        Mappings.apply2(topGroupMapping, aggRel.getGroupSets());

    relBuilder.aggregate(
        relBuilder.groupKey(topGroupSet, topGroupSets),
        transformedAggCalls);
    call.transformTo(relBuilder.build());
  }

  private static @Nullable List<AggregateCall> transformAggCalls(RelNode input, int groupCount,
      List<AggregateCall> origCalls) {
    final List<AggregateCall> newCalls = new ArrayList<>();
    for (Ord<AggregateCall> ord : Ord.zip(origCalls)) {
      final AggregateCall origCall = ord.e;
      if (origCall.isDistinct()
          || !SUPPORTED_AGGREGATES.containsKey(origCall.getAggregation()
              .getClass())) {
        return null;
      }
      final SqlAggFunction aggFun;
      final RelDataType aggType;
      if (origCall.getAggregation() == SqlStdOperatorTable.COUNT) {
        aggFun = SqlStdOperatorTable.SUM0;
        // count(any) is always not null, however nullability of sum might
        // depend on the number of columns in GROUP BY.
        // Here we use SUM0 since we are sure we will not face nullable
        // inputs nor we'll face empty set.
        aggType = null;
      } else {
        aggFun = origCall.getAggregation();
        aggType = origCall.getType();
      }
      AggregateCall newCall =
          AggregateCall.create(ord.e.getParserPosition(), aggFun, origCall.isDistinct(),
              origCall.isApproximate(), origCall.ignoreNulls(),
              origCall.rexList, ImmutableList.of(groupCount + ord.i), -1,
              origCall.distinctKeys, origCall.collation,
              groupCount, input, aggType, origCall.getName());
      newCalls.add(newCall);
    }
    return newCalls;
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableAggregateUnionTransposeRule.Config.of()
        .withOperandFor(LogicalAggregate.class, LogicalUnion.class);

    @Override default AggregateUnionTransposeRule toRule() {
      return new AggregateUnionTransposeRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Aggregate> aggregateClass,
        Class<? extends Union> unionClass) {
      return withOperandSupplier(b0 ->
          b0.operand(aggregateClass).oneInput(b1 ->
              b1.operand(unionClass).anyInputs()))
          .as(Config.class);
    }
  }
}
