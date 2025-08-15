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
package org.apache.calcite.adapter.file.duckdb;

import org.apache.calcite.adapter.file.statistics.HLLSketchCache;
import org.apache.calcite.adapter.file.statistics.HyperLogLogSketch;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Rule that intercepts COUNT(DISTINCT) on logical aggregates BEFORE
 * they can be converted to JDBC convention.
 */
public class DuckDBCountDistinctInterceptRule extends RelOptRule {
  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDBCountDistinctInterceptRule.class);
  
  public static final DuckDBCountDistinctInterceptRule INSTANCE = 
      new DuckDBCountDistinctInterceptRule();
  
  @SuppressWarnings("deprecation")
  private DuckDBCountDistinctInterceptRule() {
    super(
        operand(LogicalAggregate.class,
                operand(LogicalTableScan.class, any())),
        "DuckDBCountDistinctInterceptRule");
  }
  
  @Override
  public boolean matches(RelOptRuleCall call) {
    final LogicalAggregate aggregate = call.rel(0);
    
    // Only match if there's at least one COUNT(DISTINCT)
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      if (aggCall.getAggregation().getKind() == SqlKind.COUNT && aggCall.isDistinct()) {
        LOGGER.warn("[INTERCEPT] Found COUNT(DISTINCT) in logical plan - will try to optimize");
        return true;
      }
    }
    return false;
  }
  
  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalAggregate aggregate = call.rel(0);
    final LogicalTableScan scan = call.rel(1);
    
    List<String> qualifiedName = scan.getTable().getQualifiedName();
    String schemaName = qualifiedName.size() >= 2 ? qualifiedName.get(qualifiedName.size() - 2) : "";
    String tableName = qualifiedName.get(qualifiedName.size() - 1).replace("\"", "");
    
    LOGGER.warn("[INTERCEPT] Processing COUNT(DISTINCT) for {}.{}", schemaName, tableName);
    
    // Try to get HLL estimates
    List<Long> hllEstimates = new ArrayList<>();
    boolean hasHLL = false;
    
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      if (aggCall.getAggregation().getKind() == SqlKind.COUNT && aggCall.isDistinct()) {
        if (!aggCall.getArgList().isEmpty()) {
          int fieldIndex = aggCall.getArgList().get(0);
          String columnName = scan.getRowType().getFieldNames().get(fieldIndex);
          
          HLLSketchCache cache = HLLSketchCache.getInstance();
          HyperLogLogSketch sketch = cache.getSketch(schemaName, tableName, columnName);
          
          if (sketch != null) {
            long estimate = sketch.getEstimate();
            LOGGER.warn("[INTERCEPT] Found HLL sketch for {}.{}.{}: estimate={}", 
                       schemaName, tableName, columnName, estimate);
            hllEstimates.add(estimate);
            hasHLL = true;
          } else {
            LOGGER.warn("[INTERCEPT] No HLL sketch for {}.{}.{}", 
                       schemaName, tableName, columnName);
            hllEstimates.add(null);
          }
        } else {
          hllEstimates.add(null);
        }
      } else {
        hllEstimates.add(null);
      }
    }
    
    if (hasHLL) {
      // Create VALUES node with HLL estimates
      RelNode valuesNode = createHLLValues(aggregate, hllEstimates);
      if (valuesNode != null) {
        LOGGER.warn("[INTERCEPT] SUCCESS! Replaced COUNT(DISTINCT) with HLL estimates");
        call.transformTo(valuesNode);
      }
    }
  }
  
  private RelNode createHLLValues(Aggregate aggregate, List<Long> hllEstimates) {
    try {
      RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
      ImmutableList.Builder<ImmutableList<RexLiteral>> tuples = ImmutableList.builder();
      ImmutableList.Builder<RexLiteral> row = ImmutableList.builder();
      
      for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
        AggregateCall aggCall = aggregate.getAggCallList().get(i);
        Long estimate = i < hllEstimates.size() ? hllEstimates.get(i) : null;
        
        if (estimate != null) {
          // Use HLL estimate
          row.add(rexBuilder.makeBigintLiteral(BigDecimal.valueOf(estimate)));
        } else {
          // For non-COUNT(DISTINCT) aggregates, return null or default
          row.add(rexBuilder.makeNullLiteral(aggCall.getType()));
        }
      }
      
      tuples.add(row.build());
      
      return LogicalValues.create(aggregate.getCluster(),
          aggregate.getRowType(),
          tuples.build());
    } catch (Exception e) {
      LOGGER.error("Failed to create VALUES node", e);
      return null;
    }
  }
}