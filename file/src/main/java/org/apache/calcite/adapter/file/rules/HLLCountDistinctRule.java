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

// Removed unused imports
import org.apache.calcite.adapter.file.statistics.HyperLogLogSketch;
import org.apache.calcite.adapter.file.statistics.StatisticsCache;
import org.apache.calcite.adapter.file.statistics.HLLSketchCache;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Rule that replaces COUNT(DISTINCT) operations with pre-computed HLL sketch lookups.
 * This provides instant responses for cardinality queries without scanning data.
 */
@Value.Enclosing
public class HLLCountDistinctRule extends RelRule<HLLCountDistinctRule.Config> {

  public static final HLLCountDistinctRule INSTANCE = 
      Config.DEFAULT.toRule();

  private HLLCountDistinctRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    // TODO: Temporarily disabled due to AggregateCall API changes
    // The HLL optimization rule needs to be updated to match the current Calcite API
    return;
  }
  
  private Long getHLLEstimate(RelNode input, AggregateCall aggCall) {
    // Find the table scan in the input
    TableScan tableScan = findTableScan(input);
    if (tableScan == null) {
      return null;
    }
    
    // Get column name from the aggregate call
    if (aggCall.getArgList().isEmpty()) {
      return null;
    }
    
    int columnIndex = aggCall.getArgList().get(0);
    String columnName = input.getRowType().getFieldNames().get(columnIndex);
    String tableName = tableScan.getTable().getQualifiedName().get(
        tableScan.getTable().getQualifiedName().size() - 1);
    
    // Use HLL sketch cache for fast retrieval
    HLLSketchCache cache = HLLSketchCache.getInstance();
    HyperLogLogSketch sketch = cache.getSketch(tableName, columnName);
    
    if (sketch != null) {
      return sketch.getEstimate();
    }
    
    return null;
  }
  
  private TableScan findTableScan(RelNode node) {
    if (node instanceof TableScan) {
      return (TableScan) node;
    }
    for (RelNode input : node.getInputs()) {
      TableScan scan = findTableScan(input);
      if (scan != null) {
        return scan;
      }
    }
    return null;
  }
  
  @SuppressWarnings("deprecation")
  private AggregateCall createConstantAgg(AggregateCall original, long value,
      RexBuilder rexBuilder, RelDataTypeFactory typeFactory) {
    // Disabled - use SimpleHLLCountDistinctRule instead
    return original;
  }
  
  private RelNode createHLLAggregate(Aggregate original, List<AggregateCall> newAggCalls,
      RelNode input, RelBuilder builder) {
    
    // Check if we have any HLL optimizations available
    boolean hasHLLOptimization = false;
    List<Long> hllEstimates = new ArrayList<>();
    
    for (AggregateCall aggCall : original.getAggCallList()) {
      if (aggCall.getAggregation().getKind() == SqlKind.COUNT && aggCall.isDistinct()) {
        Long estimate = getHLLEstimate(input, aggCall);
        if (estimate != null) {
          hasHLLOptimization = true;
          hllEstimates.add(estimate);
        } else {
          hllEstimates.add(null);
        }
      } else {
        hllEstimates.add(null);
      }
    }
    
    if (!hasHLLOptimization) {
      return null; // No HLL sketches available
    }
    
    // For simple aggregates without GROUP BY, create a VALUES node with the HLL results
    if (original.getGroupSet().isEmpty()) {
      RexBuilder rexBuilder = original.getCluster().getRexBuilder();
      RelDataTypeFactory typeFactory = original.getCluster().getTypeFactory();
      
      // Build row type for the VALUES node
      RelDataTypeFactory.Builder typeBuilder = typeFactory.builder();
      List<RexLiteral> values = new ArrayList<>();
      
      for (int i = 0; i < original.getAggCallList().size(); i++) {
        Long estimate = hllEstimates.get(i);
        if (estimate != null) {
          RelDataType bigIntType = typeFactory.createSqlType(SqlTypeName.BIGINT);
          AggregateCall aggCall = original.getAggCallList().get(i);
          String fieldName = aggCall.getName() != null ? aggCall.getName() : "EXPR$" + i;
          
          typeBuilder.add(fieldName, bigIntType);
          values.add((RexLiteral) rexBuilder.makeLiteral(estimate, bigIntType, true));
        } else {
          // Fallback - can't optimize this query
          return null;
        }
      }
      
      RelDataType rowType = typeBuilder.build();
      
      // Create VALUES node with single tuple containing HLL estimates
      org.apache.calcite.rel.logical.LogicalValues valuesNode = 
          org.apache.calcite.rel.logical.LogicalValues.create(
              original.getCluster(), 
              rowType,
              com.google.common.collect.ImmutableList.of(
                  com.google.common.collect.ImmutableList.copyOf(values)));
      
      return valuesNode;
    }
    
    // For GROUP BY queries, this is more complex - return null for now
    return null;
  }

  /** Configuration for HLLCountDistinctRule. */
  @Value.Immutable(singleton = false)
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableHLLCountDistinctRule.Config.builder()
        .withOperandSupplier(b0 ->
            b0.operand(Aggregate.class)
                .oneInput(b1 ->
                    b1.operand(RelNode.class)
                        .anyInputs()))
        .build();

    @Override default HLLCountDistinctRule toRule() {
      return new HLLCountDistinctRule(this);
    }
  }
}