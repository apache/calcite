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

/**
 * Rule to convert an {@link org.apache.calcite.rel.core.Sort} that has
 * {@code offset} or {@code fetch} set to an
 * {@link EnumerableLimit}
 * on top of a "pure" {@code Sort} that has no offset or fetch.
 *
 * @see EnumerableRules#ENUMERABLE_LIMIT_RULE
 */
public class EnumerableLimitRule
    extends RelRule<EnumerableLimitRule.Config> {
  /** Creates an EnumerableLimitRule. */
  protected EnumerableLimitRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  EnumerableLimitRule() {
    this(Config.DEFAULT);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    if (sort.offset == null && sort.fetch == null) {
      return;
    }
    RelNode input = sort.getInput();
    if (!sort.getCollation().getFieldCollations().isEmpty()) {
      // Create a sort with the same sort key, but no offset or fetch.
      input = sort.copy(
          sort.getTraitSet(),
          input,
          sort.getCollation(),
          null,
          null);
    }
    call.transformTo(
        EnumerableLimit.create(
            convert(input, input.getTraitSet().replace(EnumerableConvention.INSTANCE)),
            sort.offset,
            sort.fetch));
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY
        .withOperandSupplier(b -> b.operand(Sort.class).anyInputs())
        .as(Config.class);

    @Override default EnumerableLimitRule toRule() {
      return new EnumerableLimitRule(this);
    }
  }
}
