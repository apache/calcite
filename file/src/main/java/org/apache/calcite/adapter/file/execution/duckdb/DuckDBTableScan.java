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
package org.apache.calcite.adapter.file.execution.duckdb;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.file.statistics.HLLSketchCache;
import org.apache.calcite.adapter.file.statistics.HyperLogLogSketch;
import org.apache.calcite.adapter.file.statistics.MultiSourceStatisticsProvider;
import org.apache.calcite.adapter.file.table.DuckDBParquetTable;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * TableScan implementation that pushes queries to DuckDB for execution.
 * This ensures that DuckDB handles the query processing rather than Calcite.
 */
public class DuckDBTableScan extends TableScan implements EnumerableRel {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDBTableScan.class);
  
  private final DuckDBParquetTable duckDBTable;
  private final String schemaName;
  private final String tableName;
  private final MultiSourceStatisticsProvider statisticsProvider;

  public DuckDBTableScan(RelOptCluster cluster, RelOptTable table, 
                         DuckDBParquetTable duckDBTable, String schemaName, String tableName) {
    super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), ImmutableList.of(), table);
    this.duckDBTable = duckDBTable;
    this.schemaName = schemaName;
    this.tableName = tableName;
    
    // Initialize multi-source statistics provider
    // Try to find the Parquet file for this table
    java.io.File parquetFile = findParquetFile(schemaName, tableName);
    this.statisticsProvider = new MultiSourceStatisticsProvider(schemaName, tableName, parquetFile);
    
    LOGGER.debug("Created DuckDBTableScan for {}.{} with statistics provider", schemaName, tableName);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DuckDBTableScan(getCluster(), table, duckDBTable, schemaName, tableName);
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    // Use multi-source statistics provider for best available estimate
    double rowCount = statisticsProvider.getTableRowCount();
    
    LOGGER.debug("Multi-source row count estimate for {}.{}: {} (sources: HLL + Parquet + DuckDB)", 
                 schemaName, tableName, rowCount);
    
    // This is MUCH better than standard JDBC adapter's hardcoded 100-row assumption!
    return rowCount;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    // Get accurate row count from multi-source statistics
    double rowCount = estimateRowCount(mq);
    
    // DuckDB scans should be low cost since they use vectorized execution
    // Scale cost based on quality of available statistics
    boolean hasHLLStats = getHLLRowCountEstimate().isPresent();
    boolean hasParquetStats = (statisticsProvider != null);
    
    // Lower costs when we have better statistics for more accurate planning
    double cpuCostFactor = 0.001;  // Base DuckDB efficiency
    double ioCostFactor = 0.0001;  // Columnar I/O efficiency
    
    if (hasHLLStats && hasParquetStats) {
      // Best case: HLL + Parquet + DuckDB statistics available
      cpuCostFactor *= 0.5;  // Even more confident in our cost estimates
      ioCostFactor *= 0.5;   // Better I/O planning with comprehensive stats
    } else if (hasHLLStats || hasParquetStats) {
      // Good case: Some enhanced statistics available
      cpuCostFactor *= 0.7;  // Better than default
      ioCostFactor *= 0.7;   // Better I/O planning
    }
    // else: standard DuckDB costs (still better than default Calcite!)
    
    double cpu = rowCount * cpuCostFactor;
    double io = rowCount * ioCostFactor;
    
    LOGGER.debug("Enhanced DuckDB cost estimation for {}.{}: rows={}, cpu={}, io={}, " +
                 "hasHLL={}, hasParquet={}, quality={}",
                 schemaName, tableName, rowCount, cpu, io, hasHLLStats, hasParquetStats,
                 hasHLLStats && hasParquetStats ? "excellent" : 
                 hasHLLStats || hasParquetStats ? "good" : "standard");
    
    return planner.getCostFactory().makeCost(rowCount, cpu, io);
  }
  
  /**
   * Get row count estimate from HLL sketches if available.
   * Uses the "*" column sketch which represents total row count.
   */
  private Optional<Double> getHLLRowCountEstimate() {
    try {
      HLLSketchCache cache = HLLSketchCache.getInstance();
      HyperLogLogSketch sketch = cache.getSketch(schemaName, tableName, "*");
      
      if (sketch != null) {
        double estimate = sketch.getEstimate();
        // Sanity check - HLL estimates should be positive
        if (estimate > 0) {
          return Optional.of(estimate);
        }
      }
    } catch (Exception e) {
      LOGGER.debug("Failed to get HLL row count estimate for {}.{}: {}", 
                   schemaName, tableName, e.getMessage());
    }
    
    return Optional.empty();
  }
  
  /**
   * Get comprehensive cardinality estimate for a specific column using multi-source statistics.
   * This provides MUCH better estimates than standard JDBC adapter for Calcite optimization.
   */
  public Optional<Double> getColumnCardinality(String columnName) {
    if (statisticsProvider != null) {
      MultiSourceStatisticsProvider.EnhancedColumnStatistics stats = 
          statisticsProvider.getColumnStatistics(columnName);
      
      // Get best available cardinality estimate
      double estimate = stats.getBestCardinalityEstimate(-1);
      if (estimate > 0) {
        LOGGER.debug("Multi-source cardinality estimate for {}.{}.{}: {} (sources: {})", 
                     schemaName, tableName, columnName, estimate,
                     (stats.getHllCardinality().isPresent() ? "HLL+" : "") +
                     (stats.getDistinctCount().isPresent() ? "DuckDB+" : "") + 
                     "Parquet");
        return Optional.of(estimate);
      }
    }
    
    // Fallback to HLL only
    return getHLLColumnCardinality(columnName);
  }
  
  /**
   * Get cardinality from HLL sketches only (fallback method).
   */
  private Optional<Double> getHLLColumnCardinality(String columnName) {
    try {
      HLLSketchCache cache = HLLSketchCache.getInstance();
      HyperLogLogSketch sketch = cache.getSketch(schemaName, tableName, columnName);
      
      if (sketch != null) {
        double estimate = sketch.getEstimate();
        if (estimate > 0) {
          LOGGER.debug("HLL-only cardinality estimate for {}.{}.{}: {}", 
                       schemaName, tableName, columnName, estimate);
          return Optional.of(estimate);
        }
      }
    } catch (Exception e) {
      LOGGER.debug("Failed to get HLL cardinality estimate for {}.{}.{}: {}", 
                   schemaName, tableName, columnName, e.getMessage());
    }
    
    return Optional.empty();
  }
  
  /**
   * Find the Parquet file associated with this table.
   * This enables integration with Parquet metadata statistics.
   */
  private java.io.File findParquetFile(String schemaName, String tableName) {
    // Try common naming patterns for Parquet files
    String[] possiblePaths = {
        tableName + ".parquet",
        tableName.toLowerCase() + ".parquet",
        schemaName + "/" + tableName + ".parquet",
        "data/" + tableName + ".parquet"
    };
    
    for (String path : possiblePaths) {
      java.io.File candidate = new java.io.File(path);
      if (candidate.exists() && candidate.isFile()) {
        LOGGER.debug("Found Parquet file for {}.{}: {}", schemaName, tableName, candidate.getAbsolutePath());
        return candidate;
      }
    }
    
    LOGGER.debug("No Parquet file found for {}.{}, statistics will be HLL + DuckDB only", 
                 schemaName, tableName);
    return null;
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.preferArray());

    // Create the enumerable expression that uses DuckDB for query execution
    BlockBuilder builder = new BlockBuilder();

    // Generate parameters for the DuckDBEnumerator
    Expression schemaNameExpr = Expressions.constant(schemaName);
    Expression tableNameExpr = Expressions.constant(tableName);
    // Properly quote identifiers for DuckDB to prevent case conversion
    String quotedSchema = DuckDBSqlGenerator.getDialect().quoteIdentifier(schemaName);
    String quotedTable = DuckDBSqlGenerator.getDialect().quoteIdentifier(tableName);
    Expression sqlExpr = Expressions.constant("SELECT * FROM " + quotedSchema + "." + quotedTable);

    LOGGER.debug("DuckDBTableScan implementing query: SELECT * FROM {}.{} (quoted: {}.{})", 
                 schemaName, tableName, quotedSchema, quotedTable);

    // Call DuckDBEnumerator to execute the query
    Expression enumerable = builder.append("enumerable",
        Expressions.call(
            DuckDBEnumerator.class,
            "createEnumerable",
            schemaNameExpr,
            tableNameExpr, 
            sqlExpr));

    builder.add(Expressions.return_(null, enumerable));

    return implementor.result(physType, builder.toBlock());
  }
}