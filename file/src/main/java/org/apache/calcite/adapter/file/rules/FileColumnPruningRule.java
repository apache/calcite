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
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.adapter.file.table.ParquetTranslatableTable.StatisticsAwareParquetTableScan;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;

import org.immutables.value.Value;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Rule that prunes columns not used in downstream operations.
 * Uses column statistics to identify unused columns early in the plan
 * and estimates the I/O savings from column pruning.
 * 
 * This is particularly effective for wide tables with many columns
 * where only a subset are actually needed.
 */
@Value.Enclosing
public class FileColumnPruningRule extends RelRule<FileColumnPruningRule.Config> {

  public static final FileColumnPruningRule INSTANCE = 
      Config.DEFAULT.toRule();

  private FileColumnPruningRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    // TODO: Temporarily disabled due to StatisticsAwareParquetTableScan API changes
    // The column pruning rule needs to be updated to match the current table scan API
    return;
  }
  
  private TableStatistics getTableStatistics(StatisticsAwareParquetTableScan scan) {
    if (scan.getTable() instanceof StatisticsProvider) {
      StatisticsProvider provider = (StatisticsProvider) scan.getTable();
      return provider.getTableStatistics(scan.getTable());
    }
    return null;
  }
  
  private ColumnUsageAnalysis analyzeColumnUsage(List<RexNode> projects, RelDataType inputRowType, TableStatistics stats) {
    ColumnUsageAnalysis analysis = new ColumnUsageAnalysis();
    
    // Find which columns are referenced in the projection
    Set<Integer> usedColumnIndices = new HashSet<>();
    for (RexNode expr : projects) {
      collectColumnReferences(expr, usedColumnIndices);
    }
    
    // Compare with available columns
    Map<String, ColumnStatistics> allColumns = stats.getColumnStatistics();
    List<String> columnNames = inputRowType.getFieldNames();
    
    long totalDataSize = 0;
    long usedDataSize = 0;
    
    for (int i = 0; i < columnNames.size(); i++) {
      String columnName = columnNames.get(i);
      ColumnStatistics colStats = allColumns.get(columnName);
      
      if (colStats != null) {
        long columnSize = estimateColumnSize(colStats);
        totalDataSize += columnSize;
        
        if (usedColumnIndices.contains(i)) {
          analysis.usedColumns.add(columnName);
          usedDataSize += columnSize;
        } else {
          analysis.unusedColumns.add(columnName);
        }
      }
    }
    
    // Calculate potential I/O savings
    if (totalDataSize > 0) {
      analysis.ioSavingsPercent = 100.0 * (totalDataSize - usedDataSize) / totalDataSize;
      analysis.canOptimize = analysis.ioSavingsPercent > 10.0; // Only optimize if >10% savings
    }
    
    return analysis;
  }
  
  private void collectColumnReferences(RexNode expr, Set<Integer> columnIndices) {
    if (expr instanceof RexInputRef) {
      columnIndices.add(((RexInputRef) expr).getIndex());
    } else if (expr instanceof org.apache.calcite.rex.RexCall) {
      org.apache.calcite.rex.RexCall call = (org.apache.calcite.rex.RexCall) expr;
      for (RexNode operand : call.getOperands()) {
        collectColumnReferences(operand, columnIndices);
      }
    }
  }
  
  private long estimateColumnSize(ColumnStatistics colStats) {
    // Rough estimate based on data type and cardinality
    long distinctCount = colStats.getDistinctCount();
    long totalCount = colStats.getTotalCount();
    
    // Assume average of 8 bytes per value (mix of numbers, strings, etc.)
    // In a real implementation, this would be based on actual column types
    long baseSize = totalCount * 8;
    
    // Adjust for compression potential based on cardinality
    double compressionRatio = Math.min(1.0, (double) distinctCount / totalCount * 4);
    
    return (long) (baseSize * compressionRatio);
  }
  
  @SuppressWarnings("deprecation")
  private RelNode createOptimizedProjection(LogicalProject project,
                                          StatisticsAwareParquetTableScan scan,
                                          ColumnUsageAnalysis analysis,
                                          RelOptRuleCall call) {
    // Create a new scan that only reads the used columns
    // In a real implementation, this would:
    // 1. Configure the Parquet reader to only read specific columns
    // 2. Reduce I/O by skipping unused column chunks
    // 3. Reduce memory usage by not materializing unused columns
    
    // Create a new scan with column pruning information
    StatisticsAwareParquetTableScan prunedScan = new StatisticsAwareParquetTableScan(
        scan.getCluster(),
        scan.getTable(),
        (org.apache.calcite.adapter.file.table.ParquetTranslatableTable) scan.getTable().unwrap(org.apache.calcite.adapter.file.table.ParquetTranslatableTable.class),
        scan.getRowType()
    );
    
    // The scan would be configured to only read columns in analysis.usedColumns
    // This provides significant I/O savings for wide tables
    
    // Disabled due to API changes
    return null;
  }
  
  /** Analysis result for column pruning optimization */
  private static class ColumnUsageAnalysis {
    boolean canOptimize = false;
    double ioSavingsPercent = 0.0;
    Set<String> usedColumns = new HashSet<>();
    Set<String> unusedColumns = new HashSet<>();
  }

  /** Rule configuration. */
  @Value.Immutable(singleton = false)
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableFileColumnPruningRule.Config.builder()
        .withOperandSupplier(b0 ->
            b0.operand(LogicalProject.class)
                .oneInput(b1 ->
                    b1.operand(StatisticsAwareParquetTableScan.class).noInputs()))
        .build();

    @Override default FileColumnPruningRule toRule() {
      return new FileColumnPruningRule(this);
    }
  }
}