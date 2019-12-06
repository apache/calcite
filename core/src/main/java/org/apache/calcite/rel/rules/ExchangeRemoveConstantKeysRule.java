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

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.SortExchange;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalSortExchange;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Planner rule that removes keys from
 * a {@link Exchange} if those keys are known to be constant.
 *
 * <p>For example,
 * <code>SELECT key,value FROM (SELECT 1 AS key, value FROM src) r DISTRIBUTE
 * BY key</code> can be reduced to
 * <code>SELECT 1 AS key, value FROM src</code>.</p>
 *
 */
public class ExchangeRemoveConstantKeysRule extends RelOptRule {
  /**
   * Singleton rule that removes constants inside a
   * {@link LogicalExchange}.
   */
  public static final ExchangeRemoveConstantKeysRule EXCHANGE_INSTANCE =
      new ExchangeRemoveConstantKeysRule(LogicalExchange.class,
          "ExchangeRemoveConstantKeysRule");

  /**
   * Singleton rule that removes constants inside a
   * {@link LogicalSortExchange}.
   */
  public static final ExchangeRemoveConstantKeysRule SORT_EXCHANGE_INSTANCE =
      new SortExchangeRemoveConstantKeysRule(LogicalSortExchange.class,
          "SortExchangeRemoveConstantKeysRule");

  private ExchangeRemoveConstantKeysRule(Class<? extends RelNode> clazz,
      String description) {
    super(operand(clazz, any()), RelFactories.LOGICAL_BUILDER, description);
  }

  /** Removes constant in distribution keys. */
  protected static List<Integer> simplifyDistributionKeys(RelDistribution distribution,
      Set<Integer> constants) {
    return distribution.getKeys().stream()
        .filter(key -> !constants.contains(key))
        .collect(Collectors.toList());
  }

  @Override public boolean matches(RelOptRuleCall call) {
    final Exchange exchange = call.rel(0);
    return exchange.getDistribution().getType()
        == RelDistribution.Type.HASH_DISTRIBUTED;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Exchange exchange = call.rel(0);
    final RelMetadataQuery mq = call.getMetadataQuery();
    final RelNode input = exchange.getInput();
    final RelOptPredicateList predicates = mq.getPulledUpPredicates(input);
    if (predicates == null) {
      return;
    }

    final Set<Integer> constants = new HashSet<>();
    predicates.constantMap.keySet().forEach(key -> {
      if (key instanceof RexInputRef) {
        constants.add(((RexInputRef) key).getIndex());
      }
    });
    if (constants.isEmpty()) {
      return;
    }

    final List<Integer> distributionKeys = simplifyDistributionKeys(
        exchange.getDistribution(), constants);

    if (distributionKeys.size() != exchange.getDistribution().getKeys()
        .size()) {
      call.transformTo(call.builder()
          .push(exchange.getInput())
          .exchange(distributionKeys.isEmpty()
              ? RelDistributions.SINGLETON
              : RelDistributions.hash(distributionKeys))
          .build());
      call.getPlanner().setImportance(exchange, 0.0);
    }
  }

  /**
   * Rule that reduces constants inside a {@link SortExchange}.
   */
  public static class SortExchangeRemoveConstantKeysRule
      extends ExchangeRemoveConstantKeysRule {

    private SortExchangeRemoveConstantKeysRule(Class<? extends RelNode> clazz,
            String description) {
      super(clazz, description);
    }

    @Override public boolean matches(RelOptRuleCall call) {
      final SortExchange sortExchange = call.rel(0);
      return  sortExchange.getDistribution().getType()
          == RelDistribution.Type.HASH_DISTRIBUTED
          || !sortExchange.getCollation().getFieldCollations().isEmpty();
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final SortExchange sortExchange = call.rel(0);
      final RelMetadataQuery mq = call.getMetadataQuery();
      final RelNode input = sortExchange.getInput();
      final RelOptPredicateList predicates = mq.getPulledUpPredicates(input);
      if (predicates == null) {
        return;
      }

      final Set<Integer> constants = new HashSet<>();
      predicates.constantMap.keySet().forEach(key -> {
        if (key instanceof RexInputRef) {
          constants.add(((RexInputRef) key).getIndex());
        }
      });

      if (constants.isEmpty()) {
        return;
      }

      List<Integer> distributionKeys = new ArrayList<>();
      boolean distributionSimplified = false;
      boolean hashDistribution = sortExchange.getDistribution().getType()
          == RelDistribution.Type.HASH_DISTRIBUTED;
      if (hashDistribution) {
        distributionKeys = simplifyDistributionKeys(
            sortExchange.getDistribution(), constants);
        distributionSimplified =
            distributionKeys.size() != sortExchange.getDistribution().getKeys()
                .size();
      }

      final List<RelFieldCollation> fieldCollations = sortExchange
          .getCollation().getFieldCollations().stream().filter(
              fc -> !constants.contains(fc.getFieldIndex()))
           .collect(Collectors.toList());

      boolean collationSimplified =
           fieldCollations.size() != sortExchange.getCollation()
               .getFieldCollations().size();
      if (distributionSimplified
           || collationSimplified) {
        RelDistribution distribution = distributionSimplified
            ? (distributionKeys.isEmpty()
                ? RelDistributions.SINGLETON
                : RelDistributions.hash(distributionKeys))
            : sortExchange.getDistribution();
        RelCollation collation = collationSimplified
            ? RelCollations.of(fieldCollations)
            : sortExchange.getCollation();

        call.transformTo(call.builder()
            .push(sortExchange.getInput())
            .sortExchange(distribution, collation)
            .build());
        call.getPlanner().setImportance(sortExchange, 0.0);
      }
    }
  }
}

// End ExchangeRemoveConstantKeysRule.java
