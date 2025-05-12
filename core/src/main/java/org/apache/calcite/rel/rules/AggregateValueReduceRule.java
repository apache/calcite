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
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.immutables.value.Value;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Rule that applies an {@link Aggregate} to a distinct {@link Values}.
 *
 * <p>This is useful because such as {@link SubQueryRemoveRule}
 * doesn't distinct values for IN filter values.
 *
 * <p>Sample query where this matters:
 *
 * <blockquote><code>
 * SELECT deptno, sal FROM EMP WHERE deptno IN (1,1,3,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1)
 * </code></blockquote>
 *
 * <p>should be converted to:
 *
 * <blockquote><code>
 * SELECT deptno, sal FROM EMP WHERE deptno IN (1,3)
 * </code></blockquote>
 *
 * @see CoreRules#AGGREGATE_VALUES_REDUCE
 */
@Value.Enclosing
public class AggregateValueReduceRule
    extends RelRule<AggregateValueReduceRule.Config>
    implements SubstitutionRule {

  /** Creates an AggregateValuesRule. */
  protected AggregateValueReduceRule(Config config) {
    super(config);
  }

  public AggregateValueReduceRule(RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT
        .withRelBuilderFactory(relBuilderFactory)
        .as(Config.class));
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Values values = call.rel(1);
    Util.discard(values);
    final RelBuilder relBuilder = call.builder();

    List<ImmutableList<RexLiteral>> distinctValues =
        values.getTuples().stream().distinct().collect(Collectors.toList());

    relBuilder.values(distinctValues, values.getRowType());
    call.transformTo(relBuilder.build());
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableAggregateValueReduceRule.Config.of()
        .withOperandFor(Aggregate.class, Values.class);

    @Override default AggregateValueReduceRule toRule() {
      return new AggregateValueReduceRule(this);
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
