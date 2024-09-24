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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import org.immutables.value.Value;

/**
 * Rule to convert an {@link org.apache.calcite.rel.core.Sort} that has
 * {@code offset} or {@code fetch} set to an
 * {@link EnumerableLimit}
 * on top of a "pure" {@code Sort} that has no offset or fetch.
 *
 * @see EnumerableRules#ENUMERABLE_LIMIT_RULE
 */
@Value.Enclosing
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
    final RelOptCluster cluster = sort.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();

    if (sort.offset == null && sort.fetch == null) {
      return;
    }

    final RelNode input = sort.getCollation().getFieldCollations().isEmpty()
        ? sort.getInput()
        : sort.copy(sort.getTraitSet(), sort.getInput(), sort.getCollation(), null, null);
    final RelTraitSet traitSet =
        cluster.traitSetOf(EnumerableConvention.INSTANCE)
            .replaceIfs(RelCollationTraitDef.INSTANCE,
                () -> RelMdCollation.limit(mq, sort))
            .replaceIf(RelDistributionTraitDef.INSTANCE,
                () -> RelMdDistribution.limit(mq, input));
    call.transformTo(
        new EnumerableLimit(
            cluster,
            traitSet,
            convert(call.getPlanner(), input,
                    input.getTraitSet().replace(EnumerableConvention.INSTANCE)),
            sort.offset,
            sort.fetch));
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableEnumerableLimitRule.Config.of()
        .withOperandSupplier(b -> b.operand(Sort.class).anyInputs());

    @Override default EnumerableLimitRule toRule() {
      return new EnumerableLimitRule(this);
    }
  }
}
