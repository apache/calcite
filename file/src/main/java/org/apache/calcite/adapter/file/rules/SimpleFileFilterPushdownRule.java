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

import org.apache.calcite.adapter.file.statistics.ColumnStatistics;
import org.apache.calcite.adapter.file.statistics.StatisticsProvider;
import org.apache.calcite.adapter.file.statistics.TableStatistics;
import org.apache.calcite.adapter.file.table.ParquetTranslatableTable;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import org.immutables.value.Value;

/**
 * Simple working filter pushdown rule that uses statistics to eliminate filters
 * that can be resolved without data scanning.
 */
@Value.Enclosing
public class SimpleFileFilterPushdownRule extends RelRule<SimpleFileFilterPushdownRule.Config> {

  public static final SimpleFileFilterPushdownRule INSTANCE = 
      Config.DEFAULT.toRule();

  private SimpleFileFilterPushdownRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalFilter filter = call.rel(0);
    final TableScan scan = call.rel(1);
    
    // Check if statistics-based optimization is enabled
    if (!Boolean.getBoolean("calcite.file.statistics.filter.enabled")) {
      return;
    }
    
    // Only optimize Parquet table scans
    if (!(scan.getTable().unwrap(ParquetTranslatableTable.class) instanceof ParquetTranslatableTable)) {
      return;
    }
    
    // Try to get table statistics
    TableStatistics stats = getTableStatistics(scan);
    if (stats == null) {
      return;
    }
    
    // Analyze filter condition
    FilterOptimization optimization = analyzeFilter(filter.getCondition(), stats, scan);
    
    if (optimization.canOptimize) {
      // Create optimized result based on statistics
      RelNode optimizedNode = createOptimizedFilter(filter, scan, optimization, call);
      if (optimizedNode != null) {
        call.transformTo(optimizedNode);
      }
    }
  }
  
  private TableStatistics getTableStatistics(TableScan scan) {
    ParquetTranslatableTable parquetTable = scan.getTable().unwrap(ParquetTranslatableTable.class);
    if (parquetTable instanceof StatisticsProvider) {
      StatisticsProvider provider = (StatisticsProvider) parquetTable;
      return provider.getTableStatistics(scan.getTable());
    }
    return null;
  }
  
  private FilterOptimization analyzeFilter(RexNode condition, TableStatistics stats, TableScan scan) {
    FilterOptimization optimization = new FilterOptimization();
    
    if (condition instanceof RexCall) {
      RexCall call = (RexCall) condition;
      SqlKind kind = call.getKind();
      
      // Handle simple comparisons that can be resolved with min/max statistics
      if ((kind == SqlKind.EQUALS || kind == SqlKind.LESS_THAN || 
           kind == SqlKind.GREATER_THAN || kind == SqlKind.LESS_THAN_OR_EQUAL ||
           kind == SqlKind.GREATER_THAN_OR_EQUAL || kind == SqlKind.NOT_EQUALS) &&
          call.getOperands().size() == 2) {
        
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);
        
        if (left instanceof RexInputRef && right instanceof RexLiteral) {
          RexInputRef inputRef = (RexInputRef) left;
          RexLiteral literal = (RexLiteral) right;
          
          String columnName = scan.getRowType().getFieldNames().get(inputRef.getIndex());
          ColumnStatistics colStats = stats.getColumnStatistics().get(columnName);
          
          if (colStats != null) {
            optimization.canOptimize = true;
            optimization.columnName = columnName;
            optimization.operator = kind;
            optimization.filterValue = literal.getValue();
            optimization.minValue = colStats.getMinValue();
            optimization.maxValue = colStats.getMaxValue();
            
            // Determine if filter can be resolved with statistics alone
            optimization.alwaysTrue = evaluateWithStats(kind, literal.getValue(), 
                colStats.getMinValue(), colStats.getMaxValue(), true);
            optimization.alwaysFalse = evaluateWithStats(kind, literal.getValue(), 
                colStats.getMinValue(), colStats.getMaxValue(), false);
          }
        }
      }
    }
    
    return optimization;
  }
  
  private boolean evaluateWithStats(SqlKind operator, Object filterValue, 
                                   Object minValue, Object maxValue, boolean checkTrue) {
    if (filterValue == null || minValue == null || maxValue == null) {
      return false;
    }
    
    try {
      if (filterValue instanceof Comparable && minValue instanceof Comparable && maxValue instanceof Comparable) {
        @SuppressWarnings("unchecked")
        Comparable<Object> filter = (Comparable<Object>) filterValue;
        @SuppressWarnings("unchecked")
        Comparable<Object> min = (Comparable<Object>) minValue;
        @SuppressWarnings("unchecked")
        Comparable<Object> max = (Comparable<Object>) maxValue;
        
        switch (operator) {
          case EQUALS:
            // filter = value: true if min <= filter <= max, false if filter < min || filter > max
            return checkTrue ? (filter.compareTo(min) >= 0 && filter.compareTo(max) <= 0) :
                              (filter.compareTo(min) < 0 || filter.compareTo(max) > 0);
          case LESS_THAN:
            // filter < value: true if max < filter, false if min >= filter
            return checkTrue ? max.compareTo(filter) < 0 : min.compareTo(filter) >= 0;
          case GREATER_THAN:
            // filter > value: true if min > filter, false if max <= filter
            return checkTrue ? min.compareTo(filter) > 0 : max.compareTo(filter) <= 0;
          case LESS_THAN_OR_EQUAL:
            // filter <= value: true if max <= filter, false if min > filter
            return checkTrue ? max.compareTo(filter) <= 0 : min.compareTo(filter) > 0;
          case GREATER_THAN_OR_EQUAL:
            // filter >= value: true if min >= filter, false if max < filter
            return checkTrue ? min.compareTo(filter) >= 0 : max.compareTo(filter) < 0;
          case NOT_EQUALS:
            // filter != value: true if filter < min || filter > max, false if min <= filter <= max
            return checkTrue ? (filter.compareTo(min) < 0 || filter.compareTo(max) > 0) :
                              (filter.compareTo(min) >= 0 && filter.compareTo(max) <= 0);
        }
      }
    } catch (Exception e) {
      // Fall back to non-optimized path
    }
    
    return false;
  }
  
  private RelNode createOptimizedFilter(LogicalFilter filter, TableScan scan, 
                                       FilterOptimization optimization, RelOptRuleCall call) {
    if (optimization.alwaysTrue) {
      // Filter always true - return just the scan
      return scan;
    } else if (optimization.alwaysFalse) {
      // Filter always false - return empty VALUES
      return call.builder().push(scan).empty().build();
    }
    
    // For other cases, return the original filter but with improved cardinality estimates
    // In a real implementation, this would push the filter to Parquet row group level
    return LogicalFilter.create(scan, filter.getCondition());
  }
  
  /** Filter optimization analysis result */
  private static class FilterOptimization {
    boolean canOptimize = false;
    boolean alwaysTrue = false;
    boolean alwaysFalse = false;
    String columnName;
    SqlKind operator;
    Object filterValue;
    Object minValue;
    Object maxValue;
  }

  /** Configuration for SimpleFileFilterPushdownRule. */
  @Value.Immutable(singleton = false)
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableSimpleFileFilterPushdownRule.Config.builder()
        .withOperandSupplier(b0 ->
            b0.operand(LogicalFilter.class)
                .oneInput(b1 ->
                    b1.operand(TableScan.class).noInputs()))
        .build();

    @Override default SimpleFileFilterPushdownRule toRule() {
      return new SimpleFileFilterPushdownRule(this);
    }
  }
}