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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

 /**
 * Rule that transforms an MIN/MAX {@link Aggregate} functions into equivalent subqueries
 * with ORDER BY and LIMIT 1 for potential performance optimization.
 *
 * <p>This rule converts queries of the form:
 * <pre>{@code
 * SELECT MIN(c1), MAX(c2) FROM t;
 * }</pre>
 * into:
 * <pre>{@code
 * SELECT
 *   (SELECT c1 FROM t WHERE c1 IS NOT NULL ORDER BY c1 ASC LIMIT 1)
 *     AS min_c1,
 *   (SELECT c2 FROM t WHERE c2 IS NOT NULL ORDER BY c2 DESC LIMIT 1)
 *    AS max_c2
 * FROM (VALUES(1));
 * }</pre>
 */
@Value.Enclosing
public class AggregateMinMaxToLimitRule
    extends RelRule<AggregateMinMaxToLimitRule.Config>
    implements TransformationRule {

  /** Creates a AggregateMinMaxToLimitRule. */
  protected AggregateMinMaxToLimitRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean matches(RelOptRuleCall call) {
    final Aggregate agg = call.rel(0);

    // Only match no group by aggregate
    if (!agg.getGroupSet().isEmpty()) {
      return false;
    }

    // Only match if all aggregate functions are MIN or MAX
    return agg.getAggCallList().stream()
        .allMatch(aggCall ->
            aggCall.getAggregation().getKind() == SqlKind.MIN
                || aggCall.getAggregation().getKind() == SqlKind.MAX);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate agg = call.rel(0);
    RelNode aggInput = agg.getInput().stripped();
    RelBuilder builder = call.builder();

    builder.push(aggInput);
    List<RexNode> newProjects = new ArrayList<>();
    for (AggregateCall aggCall : agg.getAggCallList()) {
      int idx = aggCall.getArgList().get(0);
      final RexNode r = builder.field(idx);
      if (!RexUtil.isDeterministic(r)) {
        return;
      }
      // MIN is ASC, MAX is DESC
      final boolean isDesc = aggCall.getAggregation().kind == SqlKind.MAX;

      RexNode subQuery = builder.scalarQuery(b -> b.push(aggInput)
          .project(r)
          .filter(b.isNotNull(r))
          .sortLimit(0, 1,
              isDesc ? builder.desc(r) : r)
          .build());

      newProjects.add(subQuery);
    }

    builder.clear();
    builder.values(new String[] {"i"}, 1)
        .project(newProjects);
    call.transformTo(builder.build());
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableAggregateMinMaxToLimitRule.Config.of()
        .withOperandFor(Aggregate.class);

    @Override default AggregateMinMaxToLimitRule toRule() {
      return new AggregateMinMaxToLimitRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Aggregate> aggregateClass) {
      return withOperandSupplier(
          b0 -> b0.operand(aggregateClass).anyInputs())
          .as(Config.class);
    }
  }
}
