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
package org.apache.calcite.adapter.file.table;

import org.apache.calcite.util.Source;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.adapter.file.execution.duckdb.DuckDBTableScan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Table that reads Parquet files using DuckDB's high-performance query engine.
 * 
 * <p>Extends ParquetTranslatableTable to inherit all standard Parquet metadata handling.
 * Simply registers the Parquet file as a view in DuckDB when created.
 */
public class DuckDBParquetTable extends ParquetTranslatableTable {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDBParquetTable.class);
  
  private String schemaName;  // The FileSchema name for DuckDB connection
  private String tableName;   // The table name in DuckDB
  
  public DuckDBParquetTable(Source source, RelDataType protoRowType, String columnNameCasing) {
    // Call parent constructor with File
    super(new File(source.path()));
    LOGGER.debug("Created DuckDBParquetTable for: {}", source.path());
  }
  
  /**
   * Sets the schema and table names for DuckDB queries.
   * This should be called by FileSchema after registering the view in DuckDB.
   */
  public void setDuckDBNames(String schemaName, String tableName) {
    this.schemaName = schemaName;
    this.tableName = tableName;
    LOGGER.debug("Set DuckDB names: {}.{}", schemaName, tableName);
  }
  
  /**
   * Gets the fully qualified table name for DuckDB queries.
   */
  public String getQualifiedTableName() {
    if (schemaName == null || tableName == null) {
      // If not set, just use the parent's behavior
      return null;
    }
    return schemaName + "." + tableName;
  }
  
  /**
   * Override toRel to create DuckDB-specific RelNode that pushes queries to DuckDB.
   * BUT we ensure HLL rules are registered FIRST in the planner.
   */
  @Override 
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    LOGGER.debug("Creating DuckDB RelNode for table: {}", getQualifiedTableName());
    
    // CRITICAL: Register HLL rules in THIS planner before creating DuckDB scan
    // This ensures COUNT(DISTINCT) can be optimized before DuckDB pushdown
    org.apache.calcite.plan.RelOptPlanner planner = context.getCluster().getPlanner();
    
    // Register HLL optimization rule if not already registered
    if (!"false".equals(System.getProperty("calcite.file.statistics.hll.enabled"))) {
      planner.addRule(org.apache.calcite.adapter.file.rules.SimpleHLLCountDistinctRule.INSTANCE);
      planner.addRule(org.apache.calcite.adapter.enumerable.EnumerableRules.ENUMERABLE_VALUES_RULE);
      LOGGER.info("Registered HLL rules in planner for DuckDB table: {}", tableName);
    }
    
    // Now create DuckDB table scan - HLL rules will be applied during optimization
    return new DuckDBTableScan(context.getCluster(), relOptTable, this, schemaName, tableName);
  }
}