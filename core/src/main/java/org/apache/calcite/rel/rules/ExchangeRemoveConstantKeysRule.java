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
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.SortExchange;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalSortExchange;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.util.ImmutableBeans;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Planner rule that removes keys from
 * a {@link Exchange} if those keys are known to be constant.
 *
 * <p>For example,
 * <code>SELECT key,value FROM (SELECT 1 AS key, value FROM src) r DISTRIBUTE
 * BY key</code> can be reduced to
 * <code>SELECT 1 AS key, value FROM src</code>.
 *
 * @see CoreRules#EXCHANGE_REMOVE_CONSTANT_KEYS
 * @see CoreRules#SORT_EXCHANGE_REMOVE_CONSTANT_KEYS
 */
public class ExchangeRemoveConstantKeysRule
    extends RelRule<ExchangeRemoveConstantKeysRule.Config>
    implements SubstitutionRule {

  /** Creates an ExchangeRemoveConstantKeysRule. */
  protected ExchangeRemoveConstantKeysRule(Config config) {
    super(config);
  }

  /** Removes constant in distribution keys. */
  protected static List<Integer> simplifyDistributionKeys(RelDistribution distribution,
      Set<Integer> constants) {
    return distribution.getKeys().stream()
        .filter(key -> !constants.contains(key))
        .collect(Collectors.toList());
  }

  @Override public void onMatch(RelOptRuleCall call) {
    config.matchHandler().accept(this, call);
  }

  private static void matchExchange(ExchangeRemoveConstantKeysRule rule,
      RelOptRuleCall call) {
    final Exchange exchange = call.rel(0);
    final RelMetadataQuery mq = call.getMetadataQuery();
    final RelNode input = exchange.getInput();
    final RelOptPredicateList predicates = mq.getPulledUpPredicates(input);
    if (RelOptPredicateList.isEmpty(predicates)) {
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
      call.getPlanner().prune(exchange);
    }
  }

  private static void matchSortExchange(ExchangeRemoveConstantKeysRule rule,
      RelOptRuleCall call) {
    final SortExchange sortExchange = call.rel(0);
    final RelMetadataQuery mq = call.getMetadataQuery();
    final RelNode input = sortExchange.getInput();
    final RelOptPredicateList predicates = mq.getPulledUpPredicates(input);
    if (RelOptPredicateList.isEmpty(predicates)) {
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
      call.getPlanner().prune(sortExchange);
    }
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY
        .as(Config.class)
        .withOperandFor(LogicalExchange.class,
            exchange -> exchange.getDistribution().getType()
                == RelDistribution.Type.HASH_DISTRIBUTED)
        .withMatchHandler(ExchangeRemoveConstantKeysRule::matchExchange);

    Config SORT = EMPTY
        .withDescription("SortExchangeRemoveConstantKeysRule")
        .as(Config.class)
        .withOperandFor(LogicalSortExchange.class,
            sortExchange -> sortExchange.getDistribution().getType()
                == RelDistribution.Type.HASH_DISTRIBUTED
                || !sortExchange.getCollation().getFieldCollations()
                .isEmpty())
        .withMatchHandler(ExchangeRemoveConstantKeysRule::matchSortExchange);

    @Override default ExchangeRemoveConstantKeysRule toRule() {
      return new ExchangeRemoveConstantKeysRule(this);
    }

    /** Forwards a call to {@link #onMatch(RelOptRuleCall)}. */
    @ImmutableBeans.Property
    <R extends RelOptRule> MatchHandler<R> matchHandler();

    /** Sets {@link #matchHandler()}. */
    <R extends RelOptRule> Config withMatchHandler(MatchHandler<R> matchHandler);

    /** Defines an operand tree for the given classes. */
    default <R extends Exchange> Config withOperandFor(Class<R> exchangeClass,
        Predicate<R> predicate) {
      return withOperandSupplier(b ->
          b.operand(exchangeClass).predicate(predicate)
              .anyInputs())
          .as(Config.class);
    }
  }
}
