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
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Rule that converts an aggregate on top of a filter into a filtered aggregate.
 *
 * <p>Before
 * <pre><code>
 *   SELECT SUM(salary)
 *   FROM Emp
 *   WHERE deptno = 10
 *  </code></pre>
 *
 * <p>After
 * <pre><code>
 *   SELECT SUM(salary) FILTER (WHERE deptno = 10)
 *   FROM Emp
 *  </code></pre>
 *
 * <p>The transformation is particularly useful in view-based rewriting.
 * The removal of the {@code Filter} operators lifts some restrictions when using
 * the {@link org.apache.calcite.rel.rules.materialize.MaterializedViewRules}.
 *
 * <p>Filtered aggregates can be transformed to other equivalent forms via other
 * transformation rules (e.g., {@link AggregateFilterToCaseRule}).
 */
@Value.Enclosing public class AggregateFilterToFilteredAggregateRule
    extends RelRule<AggregateFilterToFilteredAggregateRule.Config> {

  private AggregateFilterToFilteredAggregateRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Aggregate aggregate = call.rel(0);
    Filter filter = call.rel(1);
    if (!aggregate.getGroupSet().isEmpty()) {
      // At the moment we only support the transformation for grand totals, i.e.,
      // aggregates with no grouping keys.
      return;
    }
    RelBuilder builder = call.builder();
    builder.push(filter.getInput());
    List<RexNode> projects = new ArrayList<>(builder.fields());
    List<AggregateCall> newAggCalls = new ArrayList<>();
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      if (!aggCall.getAggregation().allowsFilter()) {
        return;
      }
      RexNode condition = filter.getCondition();
      // If the aggregate call has its own filter, combine it with the filter condition.
      if (aggCall.hasFilter()) {
        condition = builder.and(builder.field(aggCall.filterArg), condition);
      }
      int pos = projects.indexOf(condition);
      if (pos < 0) {
        pos = projects.size();
        projects.add(condition);
      }
      newAggCalls.add(aggCall.withFilter(pos));
    }
    assert newAggCalls.size() == aggregate.getAggCallList().size();
    builder.project(projects);
    builder.aggregate(builder.groupKey(), newAggCalls);
    call.transformTo(builder.build());
  }

  /** Rule configuration. */
  @Value.Immutable public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableAggregateFilterToFilteredAggregateRule.Config.of()
        .withOperandSupplier(
            a -> a.operand(Aggregate.class).oneInput(f -> f.operand(Filter.class).anyInputs()));

    @Override default AggregateFilterToFilteredAggregateRule toRule() {
      return new AggregateFilterToFilteredAggregateRule(this);
    }
  }
}
