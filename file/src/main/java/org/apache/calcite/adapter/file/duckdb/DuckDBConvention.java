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

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRel;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.sql.SqlDialect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * DuckDB-specific convention that ensures maximum query pushdown.
 * Extends JdbcConvention to leverage existing JDBC infrastructure
 * while customizing for DuckDB's capabilities.
 */
public class DuckDBConvention extends JdbcConvention {
  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDBConvention.class);
  
  static {
    System.err.println("[DUCKDB-CONVENTION] Class loaded");
    try {
      java.io.PrintWriter writer = new java.io.PrintWriter(new java.io.FileWriter("/tmp/duckdb_convention_loaded.txt"));
      writer.println("DuckDBConvention loaded at: " + new java.util.Date());
      writer.close();
    } catch (Exception e) {
      // Ignore
    }
  }
  
  public DuckDBConvention(SqlDialect dialect, Expression expression, String name) {
    super(dialect, expression, name);
    System.err.println("[DUCKDB-CONVENTION] Instance created for: " + name);
  }
  
  /**
   * Creates a DuckDB convention with aggressive pushdown rules.
   */
  public static DuckDBConvention of(SqlDialect dialect, Expression expression, String name) {
    return new DuckDBConvention(dialect, expression, name);
  }
  
  @Override
  public void register(RelOptPlanner planner) {
    try {
      java.io.PrintWriter writer = new java.io.PrintWriter(new java.io.FileWriter("/tmp/duckdb_register_called.txt", true));
      writer.println("register() called at: " + new java.util.Date());
      writer.close();
    } catch (Exception e) {
      // Ignore
    }
    
    // CRITICAL: Register HLL optimization rules FIRST before JDBC pushdown
    // This allows COUNT(DISTINCT) to be optimized with HLL sketches before
    // being pushed down to DuckDB as raw SQL
    String hllEnabled = System.getProperty("calcite.file.statistics.hll.enabled");
    try {
      java.io.PrintWriter writer = new java.io.PrintWriter(new java.io.FileWriter("/tmp/duckdb_hll_property.txt", true));
      writer.println("HLL property value: '" + hllEnabled + "' at: " + new java.util.Date());
      writer.close();
    } catch (Exception e) {
      // Ignore
    }
    
    if (!"false".equals(System.getProperty("calcite.file.statistics.hll.enabled", "true"))) {
      // Use the DuckDB-specific HLL rule that handles both JDBC and file adapter patterns
      planner.addRule(DuckDBHLLCountDistinctRule.INSTANCE);
      
      try {
        java.io.PrintWriter writer = new java.io.PrintWriter(new java.io.FileWriter("/tmp/duckdb_hll_rule_added.txt", true));
        writer.println("Added HLL rule to planner at: " + new java.util.Date());
        writer.close();
      } catch (Exception e) {
        // Ignore
      }
    }
    
    // Also register the VALUES converter rule so HLL results can become enumerable
    planner.addRule(org.apache.calcite.adapter.enumerable.EnumerableRules.ENUMERABLE_VALUES_RULE);
    
    // Register parquet statistics-based optimization rules for DuckDB engine
    // These provide the same optimizations available to the parquet engine
    
    // 1. Filter pushdown based on parquet min/max statistics
    if (!"false".equals(System.getProperty("calcite.file.statistics.filter.enabled"))) {
      planner.addRule(org.apache.calcite.adapter.file.rules.SimpleFileFilterPushdownRule.INSTANCE);
    }
    
    // 2. Join reordering based on table size statistics 
    if (!"false".equals(System.getProperty("calcite.file.statistics.join.reorder.enabled"))) {
      planner.addRule(org.apache.calcite.adapter.file.rules.SimpleFileJoinReorderRule.INSTANCE);
    }
    
    // 3. Column pruning to reduce I/O based on column statistics
    if (!"false".equals(System.getProperty("calcite.file.statistics.column.pruning.enabled"))) {
      planner.addRule(org.apache.calcite.adapter.file.rules.SimpleFileColumnPruningRule.INSTANCE);
    }
    
    // Register all standard JDBC rules for comprehensive pushdown
    // These will only apply to queries that weren't optimized by statistics-based rules
    for (RelOptRule rule : JdbcRules.rules(this)) {
      planner.addRule(rule);
    }
    
    LOGGER.debug("Registered DuckDB convention with HLL + parquet statistics optimizations + comprehensive JDBC pushdown rules");
  }
}