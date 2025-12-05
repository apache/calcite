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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.immutables.value.Value;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Rule that
 * 1. applies {@link Aggregate} to a {@link Values} (currently just an
 * empty {@code Value}s).
 * 2. {@link Aggregate} and {@link Values} to a distinct {@link Values}.
 *
 * <p>This is still useful because {@link PruneEmptyRules#AGGREGATE_INSTANCE}
 * doesn't handle {@code Aggregate}, which is in turn because {@code Aggregate}
 * of empty relations need some special handling: a single row will be
 * generated, where each column's value depends on the specific aggregate calls
 * (e.g. COUNT is 0, SUM is NULL).
 *
 * <p>Sample query where this matters:
 *
 * <blockquote><code>SELECT COUNT(*) FROM s.foo WHERE 1 = 0</code></blockquote>
 *
 * <p>This rule only applies to "grand totals", that is, {@code GROUP BY ()}.
 * Any non-empty {@code GROUP BY} clause will return one row per group key
 * value, and each group will consist of at least one row.
 *
 * <p>This is useful because {@link SubQueryRemoveRule}
 * doesn't remove duplicates for IN filter values.
 *
 * <blockquote><code>
 * LogicalAggregate(group=[{0}])
 *   LogicalValues(tuples=[[{ 1 }, { 1 }, { 3 }]])
 * </code></blockquote>
 *
 * <p>With the Rule would deduplicate the values and convert to:
 *
 * <blockquote><code>
 * LogicalValues(tuples=[[{ 1 }, { 3 }]])
 * </code></blockquote>
 *
 * @see CoreRules#AGGREGATE_VALUES
 */
@Value.Enclosing
public class AggregateValuesRule
    extends RelRule<AggregateValuesRule.Config>
    implements SubstitutionRule {

  /** Creates an AggregateValuesRule. */
  protected AggregateValuesRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public AggregateValuesRule(RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT
        .withRelBuilderFactory(relBuilderFactory)
        .as(Config.class));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final Values values = call.rel(1);
    Util.discard(values);
    final RelBuilder relBuilder = call.builder();
    final RexBuilder rexBuilder = relBuilder.getRexBuilder();

    if (aggregate.getGroupCount() == 0 && values.getTuples().isEmpty()) {
      final List<RexLiteral> literals = new ArrayList<>();
      for (final AggregateCall aggregateCall : aggregate.getAggCallList()) {
        switch (aggregateCall.getAggregation().getKind()) {
        case COUNT:
        case SUM0:
          literals.add(
              rexBuilder.makeLiteral(BigDecimal.ZERO, aggregateCall.getType()));
          break;

        case MIN:
        case MAX:
        case SUM:
          literals.add(rexBuilder.makeNullLiteral(aggregateCall.getType()));
          break;

        default:
          // Unknown what this aggregate call should do on empty Values. Bail out to be safe.
          return;
        }
      }

      call.transformTo(
          relBuilder.values(ImmutableList.of(literals), aggregate.getRowType())
              .build());

      // New plan is absolutely better than old plan.
      call.getPlanner().prune(aggregate);
      return;
    }

    if (Aggregate.isSimple(aggregate)
        && aggregate.getAggCallList().isEmpty()
        && aggregate.getRowType().equals(values.getRowType())) {
      List<ImmutableList<RexLiteral>> distinctValues =
          values.getTuples().stream().distinct().collect(Collectors.toList());
      relBuilder.values(distinctValues, values.getRowType());
      call.transformTo(relBuilder.build());
      call.getPlanner().prune(aggregate);
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableAggregateValuesRule.Config.of()
        .withOperandFor(Aggregate.class, Values.class);

    @Override default AggregateValuesRule toRule() {
      return new AggregateValuesRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Aggregate> aggregateClass,
        Class<? extends Values> valuesClass) {
      return withOperandSupplier(b0 ->
          b0.operand(aggregateClass)
              .oneInput(b1 ->
                  b1.operand(valuesClass)
                      .noInputs()))
          .as(Config.class);
    }
  }
}
