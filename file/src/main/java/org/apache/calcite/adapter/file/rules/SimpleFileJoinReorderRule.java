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
package org.apache.calcite.adapter.file.rules;

import org.apache.calcite.adapter.file.statistics.StatisticsProvider;
import org.apache.calcite.adapter.file.statistics.TableStatistics;
import org.apache.calcite.adapter.file.table.ParquetTranslatableTable;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;

import org.immutables.value.Value;

/**
 * Simple working join reorder rule that uses table statistics to put
 * smaller tables on the right side of joins for better hash join performance.
 */
@Value.Enclosing
public class SimpleFileJoinReorderRule extends RelRule<SimpleFileJoinReorderRule.Config> {

  public static final SimpleFileJoinReorderRule INSTANCE = 
      Config.DEFAULT.toRule();

  private SimpleFileJoinReorderRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalJoin join = call.rel(0);
    final RelNode left = call.rel(1);
    final RelNode right = call.rel(2);
    
    // Check if statistics-based join reordering is enabled
    if (!Boolean.getBoolean("calcite.file.statistics.join.reorder.enabled")) {
      return;
    }
    
    // Only optimize joins between table scans
    TableStatistics leftStats = getTableStatistics(left);
    TableStatistics rightStats = getTableStatistics(right);
    
    if (leftStats == null || rightStats == null) {
      return; // Need statistics for both sides
    }
    
    // Analyze join order optimization
    JoinReorderDecision decision = analyzeJoinOrder(leftStats, rightStats, join);
    
    if (decision.shouldReorder) {
      // Create reordered join with smaller table on right
      RelNode reorderedJoin = createReorderedJoin(join, left, right, call.builder(), decision);
      if (reorderedJoin != null) {
        call.transformTo(reorderedJoin);
      }
    }
  }
  
  private TableStatistics getTableStatistics(RelNode node) {
    TableScan tableScan = findTableScan(node);
    if (tableScan == null) {
      return null;
    }
    
    // First try to get statistics directly from the table if it's a StatisticsProvider
    RelOptTable table = tableScan.getTable();
    if (table != null) {
      // Try to unwrap as StatisticsProvider directly
      StatisticsProvider provider = table.unwrap(StatisticsProvider.class);
      if (provider != null) {
        return provider.getTableStatistics(table);
      }
      
      // Try ParquetTranslatableTable
      ParquetTranslatableTable parquetTable = table.unwrap(ParquetTranslatableTable.class);
      if (parquetTable != null && parquetTable instanceof StatisticsProvider) {
        return ((StatisticsProvider) parquetTable).getTableStatistics(table);
      }
    }
    return null;
  }
  
  private TableScan findTableScan(RelNode node) {
    if (node instanceof TableScan) {
      return (TableScan) node;
    }
    // For more complex cases, we'd need to traverse the plan
    // For now, only handle direct table scans
    return null;
  }
  
  private JoinReorderDecision analyzeJoinOrder(TableStatistics leftStats, 
                                              TableStatistics rightStats, 
                                              LogicalJoin join) {
    JoinReorderDecision decision = new JoinReorderDecision();
    
    // Calculate table costs based on row count and data size
    decision.leftCost = calculateTableCost(leftStats);
    decision.rightCost = calculateTableCost(rightStats);
    
    // Join reordering heuristic based on join type
    JoinRelType joinType = join.getJoinType();
    
    switch (joinType) {
      case INNER:
        // For inner joins, put smaller table on the right (build side of hash join)
        decision.shouldReorder = decision.leftCost < decision.rightCost;
        break;
      case LEFT:
        // For left joins, can't reorder (would change semantics)
        decision.shouldReorder = false;
        break;
      case RIGHT:
        // For right joins, can swap to left join with smaller table
        decision.shouldReorder = decision.rightCost < decision.leftCost;
        decision.swapToLeftJoin = true;
        break;
      case FULL:
        // For full outer joins, put smaller table first to reduce intermediate results
        decision.shouldReorder = decision.leftCost < decision.rightCost;
        break;
      default:
        decision.shouldReorder = false;
    }
    
    // Only reorder if cost difference is significant (>25%)
    double costRatio = Math.max(decision.leftCost, decision.rightCost) / 
                      Math.min(decision.leftCost, decision.rightCost);
    decision.shouldReorder = decision.shouldReorder && costRatio > 1.25;
    
    return decision;
  }
  
  private double calculateTableCost(TableStatistics stats) {
    long rowCount = stats.getRowCount();
    long dataSize = stats.getDataSize();
    
    // Cost model: combine CPU cost (row processing) and I/O cost (data reading)
    double cpuCost = rowCount * 0.001; // CPU cost per row
    double ioCost = dataSize * 0.0001; // I/O cost per byte
    
    return cpuCost + ioCost;
  }
  
  private RelNode createReorderedJoin(LogicalJoin originalJoin, 
                                     RelNode leftNode, 
                                     RelNode rightNode,
                                     RelBuilder builder, 
                                     JoinReorderDecision decision) {
    
    if (decision.swapToLeftJoin) {
      // RIGHT JOIN becomes LEFT JOIN with swapped inputs
      builder.push(rightNode);
      builder.push(leftNode);
      
      // Adjust join condition for swapped inputs: rightNode becomes new left, leftNode becomes new right
      RexNode swappedCondition = adjustJoinCondition(originalJoin.getCondition(), 
          leftNode.getRowType().getFieldCount(), rightNode.getRowType().getFieldCount());
      
      return builder.join(JoinRelType.LEFT, swappedCondition).build();
      
    } else if (decision.shouldReorder) {
      // Swap inputs for better hash join performance: rightNode becomes new left, leftNode becomes new right
      builder.push(rightNode); // Smaller table becomes left
      builder.push(leftNode);  // Larger table becomes right
      
      // Adjust join condition for swapped inputs  
      RexNode swappedCondition = adjustJoinCondition(originalJoin.getCondition(),
          leftNode.getRowType().getFieldCount(), rightNode.getRowType().getFieldCount());
      
      return builder.join(originalJoin.getJoinType(), swappedCondition).build();
    }
    
    return null;
  }
  
  private RexNode adjustJoinCondition(RexNode condition, int originalLeftFieldCount, int originalRightFieldCount) {
    // When we swap the join inputs in createReorderedJoin:
    // builder.push(rightNode);  // rightNode becomes new left input
    // builder.push(leftNode);   // leftNode becomes new right input
    //
    // Original field layout:
    // LEFT(0..originalLeftFieldCount-1) JOIN RIGHT(originalLeftFieldCount..originalLeftFieldCount+originalRightFieldCount-1)
    //
    // After swap field layout:
    // NEW_LEFT=originalRight(0..originalRightFieldCount-1) JOIN NEW_RIGHT=originalLeft(originalRightFieldCount..originalRightFieldCount+originalLeftFieldCount-1)
    //
    // Field index mapping:
    // - Original left field [i] where i < originalLeftFieldCount -> New right field [originalRightFieldCount + i]  
    // - Original right field [originalLeftFieldCount + j] where j < originalRightFieldCount -> New left field [j]
    
    return condition.accept(new RexShuttle() {
      @Override
      public RexNode visitInputRef(RexInputRef inputRef) {
        int originalIndex = inputRef.getIndex();
        int newIndex;
        
        if (originalIndex < originalLeftFieldCount) {
          // Original left field -> becomes new right field after swap
          newIndex = originalRightFieldCount + originalIndex;
        } else if (originalIndex < originalLeftFieldCount + originalRightFieldCount) {
          // Original right field -> becomes new left field after swap  
          newIndex = originalIndex - originalLeftFieldCount;
        } else {
          // This shouldn't happen in a well-formed join condition
          throw new IllegalArgumentException("Field index " + originalIndex + 
              " is out of range for join with " + originalLeftFieldCount + " left fields and " + 
              originalRightFieldCount + " right fields");
        }
        
        return new RexInputRef(newIndex, inputRef.getType());
      }
    });
  }
  
  /** Join reorder decision result */
  private static class JoinReorderDecision {
    boolean shouldReorder = false;
    boolean swapToLeftJoin = false;
    double leftCost = 0.0;
    double rightCost = 0.0;
  }

  /** Configuration for SimpleFileJoinReorderRule. */
  @Value.Immutable(singleton = false)
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableSimpleFileJoinReorderRule.Config.builder()
        .withOperandSupplier(b0 ->
            b0.operand(LogicalJoin.class)
                .inputs(
                    b1 -> b1.operand(RelNode.class).anyInputs(),
                    b2 -> b2.operand(RelNode.class).anyInputs()))
        .build();

    @Override default SimpleFileJoinReorderRule toRule() {
      return new SimpleFileJoinReorderRule(this);
    }
  }
}