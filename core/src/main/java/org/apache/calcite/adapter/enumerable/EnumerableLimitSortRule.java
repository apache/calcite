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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;

/**
 * Rule to convert an {@link EnumerableLimit} of on
 * {@link EnumerableSort} into an {@link EnumerableLimitSort}.
 */
public class EnumerableLimitSortRule extends RelRule<EnumerableLimitSortRule.Config> {

  /**
   * Creates a EnumerableLimitSortRule.
   */
  public EnumerableLimitSortRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final LogicalSort sort = call.rel(0);
    RelNode input = sort.getInput();
    final Sort o = EnumerableLimitSort.create(
        convert(input, input.getTraitSet().replace(EnumerableConvention.INSTANCE)),
        sort.getCollation(),
        sort.offset, sort.fetch
    );

    call.transformTo(o);
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.withOperandSupplier(
        b0 -> b0.operand(LogicalSort.class).predicate(sort -> sort.fetch != null).anyInputs())
        .as(Config.class);

    @Override default EnumerableLimitSortRule toRule() {
      return new EnumerableLimitSortRule(this);
    }
  }
}
