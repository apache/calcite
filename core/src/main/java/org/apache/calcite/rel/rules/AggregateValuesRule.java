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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Rule that applies {@link Aggregate} to a {@link Values} (currently just an
 * empty {@code Value}s).
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
 */
public class AggregateValuesRule extends RelOptRule {
  public static final AggregateValuesRule INSTANCE =
      new AggregateValuesRule(RelFactories.LOGICAL_BUILDER);

  /**
   * Creates an AggregateValuesRule.
   *
   * @param relBuilderFactory Builder for relational expressions
   */
  public AggregateValuesRule(RelBuilderFactory relBuilderFactory) {
    super(
        operandJ(Aggregate.class, null,
            aggregate -> aggregate.getGroupCount() == 0,
            operandJ(Values.class, null,
                values -> values.getTuples().isEmpty(), none())),
        relBuilderFactory, null);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final Values values = call.rel(1);
    Util.discard(values);
    final RelBuilder relBuilder = call.builder();
    final RexBuilder rexBuilder = relBuilder.getRexBuilder();

    final List<RexLiteral> literals = new ArrayList<>();
    for (final AggregateCall aggregateCall : aggregate.getAggCallList()) {
      switch (aggregateCall.getAggregation().getKind()) {
      case COUNT:
      case SUM0:
        literals.add((RexLiteral) rexBuilder.makeLiteral(
            BigDecimal.ZERO, aggregateCall.getType(), false));
        break;

      case MIN:
      case MAX:
      case SUM:
        literals.add((RexLiteral) rexBuilder.makeCast(
            aggregateCall.getType(), rexBuilder.constantNull()));
        break;

      default:
        // Unknown what this aggregate call should do on empty Values. Bail out to be safe.
        return;
      }
    }

    // New plan is absolutely better than old plan.
    call.getPlanner().setImportance(aggregate, 0.0);

    call.transformTo(
        relBuilder.values(ImmutableList.of(literals), aggregate.getRowType())
            .build());
  }
}

// End AggregateValuesRule.java
