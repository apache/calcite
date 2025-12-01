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

import org.apache.calcite.plan.RelDigest;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.SpoolRelOptTable;
import org.apache.calcite.rel.RelCommonExpressionBasicSuggester;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Combine;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalTableSpool;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.google.common.collect.ImmutableList;

import org.immutables.value.Value;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Rule that optimizes a {@link Combine} operator by detecting shared sub-expressions
 * across its inputs and introducing {@link Spool}s to avoid redundant computation.
 *
 * <p>This rule identifies structurally equivalent sub-plans within a Combine's inputs
 * and replaces them with a spool pattern: the first occurrence becomes a producer
 * (TableSpool that materializes the result), and subsequent occurrences become
 * consumers (TableScan reading from the spooled data).
 *
 * <h2>Example</h2>
 *
 * <p>Consider two queries combined that share a common filtered table scan:
 *
 * <pre>{@code
 * -- Query 1: Count high earners
 * SELECT COUNT(*) FROM EMP WHERE SAL > 2000
 * -- Query 2: Average salary of high earners
 * SELECT AVG(SAL) FROM EMP WHERE SAL > 2000
 * }</pre>
 *
 * <p>Before this rule applies, the plan looks like:
 *
 * <pre>{@code
 * Combine
 *   LogicalAggregate(group=[{}], CNT=[COUNT()])
 *     LogicalFilter(condition=[>(SAL, 2000)])
 *       LogicalTableScan(table=[EMP])
 *   LogicalAggregate(group=[{}], AVG_SAL=[AVG(SAL)])
 *     LogicalFilter(condition=[>(SAL, 2000)])
 *       LogicalTableScan(table=[EMP])
 * }</pre>
 *
 * <p>After this rule identifies the shared {@code Filter(SAL > 2000) -> TableScan(EMP)}
 * sub-expression, the plan becomes:
 *
 * <pre>{@code
 * Combine
 *   LogicalAggregate(group=[{}], CNT=[COUNT()])
 *     LogicalTableSpool(table=[spool_0])        -- Producer: materializes filtered rows
 *       LogicalFilter(condition=[>(SAL, 2000)])
 *         LogicalTableScan(table=[EMP])
 *   LogicalAggregate(group=[{}], AVG_SAL=[AVG(SAL)])
 *     LogicalTableScan(table=[spool_0])         -- Consumer: reads from spool
 * }</pre>
 *
 * @see Combine
 * @see Spool
 * @see RelCommonExpressionBasicSuggester
 */
@Value.Enclosing
public class CombineSimpleEquivalenceRule extends RelRule<CombineSimpleEquivalenceRule.Config> {

  /** Creates a CombineSharedComponentsRule. */
  protected CombineSimpleEquivalenceRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    RelNode combine = RelOptUtil.stripAll(call.rel(0));

    // Use the suggester to find shared components
    RelCommonExpressionBasicSuggester suggester = new RelCommonExpressionBasicSuggester();
    Collection<RelNode> sharedComponents = suggester.suggest(combine, null);

    // Filter out any components that are already spools or scans from spool tables
    // to avoid creating spools of spools
    sharedComponents = sharedComponents.stream()
        .filter(node -> {
          if (node instanceof Spool) {
            return false;
          }
          // Skip if it's a TableScan reading from a spool table
          if (node instanceof LogicalTableScan) {
            LogicalTableScan scan = (LogicalTableScan) node;
            // Check if the underlying table is a SpoolRelOptTable
            return !(scan.getTable() instanceof SpoolRelOptTable);
          }
          return true;
        })
        .collect(java.util.stream.Collectors.toList());

    // If no shared components found, nothing to do
    if (sharedComponents.isEmpty()) {
      return;
    }

    // Map to track which shared component digest gets which spool
    Map<RelDigest, LogicalTableSpool> digestToSpool = new HashMap<>();
    int spoolCounter = 0;

    // Get metadata query for row count estimation
    final RelMetadataQuery mq = call.getMetadataQuery();

    // For each shared component, create a spool
    for (RelNode sharedComponent : sharedComponents) {
      // Get the actual row count of the shared component being materialized
      double actualRowCount = mq.getRowCount(sharedComponent);

      SpoolRelOptTable spoolTable =
          new SpoolRelOptTable(null,  // no schema needed for temporary tables
          sharedComponent.getRowType(),
          "spool_" + spoolCounter++,
          actualRowCount); // Pass the actual row count for accurate cardinality);

      // Create the TableSpool that will produce/write to this table
      LogicalTableSpool spool =
          (LogicalTableSpool) RelFactories.DEFAULT_SPOOL_FACTORY.createTableSpool(
              sharedComponent,
              Spool.Type.LAZY,  // Read type
              Spool.Type.LAZY,  // Write type
              spoolTable);

      digestToSpool.put(sharedComponent.getRelDigest(), spool);
    }

    combine =
        combine.accept(getReplacer(digestToSpool));

    call.transformTo(combine);
  }

  private static RelHomogeneousShuttle getReplacer(
      Map<RelDigest, LogicalTableSpool> digestToSpool) {
    Set<RelDigest> producers = new HashSet<>();

    return new RelHomogeneousShuttle() {
      @Override public RelNode visit(RelNode node) {
        // Check if this node's digest matches any of our shared components
        RelDigest nodeDigest = node.getRelDigest();
        if (digestToSpool.containsKey(nodeDigest)) {
          LogicalTableSpool spool = digestToSpool.get(nodeDigest);

          if (producers.contains(nodeDigest)) {
            // Subsequent occurrence - replace with table scan (consumer)
            return LogicalTableScan.create(
                node.getCluster(),
                spool.getTable(),
                ImmutableList.of());
          } else {
            // First occurrence - replace with the spool (producer)
            producers.add(nodeDigest);
            return spool;
          }
        }

        return super.visit(node);
      }
    };
  }


  /** Rule configuration. */
  @Value.Immutable(singleton = true)
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableCombineSimpleEquivalenceRule.Config.builder()
        .build()
        .withOperandFor(Combine.class);

    @Override default CombineSimpleEquivalenceRule toRule() {
      return new CombineSimpleEquivalenceRule(this);
    }

    default Config withOperandFor(Class<? extends Combine> combineClass) {
      return withOperandSupplier(b -> b.operand(combineClass)
          .anyInputs())
          .as(Config.class);
    }
  }
}
