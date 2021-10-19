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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalValues;

/** Planner rule that converts a {@link LogicalValues} to an {@link EnumerableValues}.
 * You may provide a custom config to convert other nodes that extend {@link Values}.
 *
 * @see EnumerableRules#ENUMERABLE_VALUES_RULE */
public class EnumerableValuesRule extends ConverterRule {
  /** Default configuration. */
  public static final Config DEFAULT_CONFIG = Config.INSTANCE
      .withConversion(LogicalValues.class, Convention.NONE,
          EnumerableConvention.INSTANCE, "EnumerableValuesRule")
      .withRuleFactory(EnumerableValuesRule::new);

  /** Creates an EnumerableValuesRule. */
  protected EnumerableValuesRule(Config config) {
    super(config);
  }

  @Override public RelNode convert(RelNode rel) {
    final Values logicalValues = (Values) rel;
    final EnumerableValues enumerableValues = EnumerableValues.create(
        logicalValues.getCluster(), logicalValues.getRowType(), logicalValues.getTuples());
    return enumerableValues.copy(
        logicalValues.getTraitSet().replace(EnumerableConvention.INSTANCE),
        enumerableValues.getInputs());
  }
}
