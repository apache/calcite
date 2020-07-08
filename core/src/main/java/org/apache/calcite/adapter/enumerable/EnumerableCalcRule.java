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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalCalc;


/**
 * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalCalc} to an
 * {@link EnumerableCalc}.
 *
 * @see EnumerableRules#ENUMERABLE_CALC_RULE
 */
class EnumerableCalcRule extends ConverterRule {
  /** Default configuration. */
  public static final Config DEFAULT_CONFIG = Config.EMPTY
      .as(Config.class)
      // The predicate ensures that if there's a multiset,
      // FarragoMultisetSplitter will work on it first.
      .withConversion(LogicalCalc.class, RelOptUtil::notContainsWindowedAgg,
          Convention.NONE, EnumerableConvention.INSTANCE,
          "EnumerableCalcRule")
      .withRuleFactory(EnumerableCalcRule::new);

  protected EnumerableCalcRule(Config config) {
    super(config);
  }

  @Override public RelNode convert(RelNode rel) {
    final LogicalCalc calc = (LogicalCalc) rel;
    final RelNode input = calc.getInput();
    return EnumerableCalc.create(
        convert(input,
            input.getTraitSet().replace(EnumerableConvention.INSTANCE)),
        calc.getProgram());
  }
}
