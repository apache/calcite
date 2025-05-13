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
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import org.immutables.value.Value;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Planner rule that distinct {@link Filter} all conditions.
 *
 * @see CoreRules#FILTER_DISTINCT_RULE
 */
@Value.Enclosing
public class FilterDistinctRule extends RelRule<FilterDistinctRule.Config>
    implements TransformationRule{

  /** Creates a FilterDistinctRule. */
  protected FilterDistinctRule(FilterDistinctRule.Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    RelBuilder builder = call.builder();
    final Filter filter = call.rel(0);
    RexCall condition = (RexCall) filter.getCondition();
    List<RexNode> distinctOR = condition.operands.stream().distinct().collect(Collectors.toList());

    RexNode rexNode = builder.getRexBuilder().makeCall(condition.op, distinctOR);
    builder.push(filter.getInput())
        .filter(rexNode);

    call.transformTo(builder.build());
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    FilterDistinctRule.Config DEFAULT = ImmutableFilterDistinctRule.Config.of()
        .withOperandFor(Filter.class);

    @Override default FilterDistinctRule toRule() {
      return new FilterDistinctRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default FilterDistinctRule.Config withOperandFor(Class<? extends Filter> filterClass) {
      return withOperandSupplier(b0 ->
          b0.operand(filterClass).anyInputs())
          .as(Config.class);
    }
  }
}
