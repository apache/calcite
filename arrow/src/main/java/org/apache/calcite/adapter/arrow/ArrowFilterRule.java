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

package org.apache.calcite.adapter.arrow;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;

/**
 * ArrowFilterRule.
 */
public class ArrowFilterRule extends RelRule<ArrowFilterRule.Config> {

  protected ArrowFilterRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final LogicalFilter filter = call.rel(0);

    if (filter.getTraitSet().contains(Convention.NONE)) {
      final RelNode converted = convert(filter);
      if (converted != null) {
        call.transformTo(converted);
      }
    }
  }

  RelNode convert(LogicalFilter filter) {
    final RelTraitSet traitSet = filter.getTraitSet().replace(ArrowRel.CONVENTION);
    return new ArrowFilter(
        filter.getCluster(),
        traitSet,
        convert(filter.getInput(), ArrowRel.CONVENTION),
        filter.getCondition());
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    ArrowFilterRule.Config DEFAULT = EMPTY
        .withOperandSupplier(b0 ->
            b0.operand(LogicalFilter.class).oneInput(b1 ->
                b1.operand(ArrowTableScan.class).noInputs()))
        .as(ArrowFilterRule.Config.class);

    @Override default ArrowFilterRule toRule() {
      return new ArrowFilterRule(this);
    }
  }
}
