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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Analyzes queries to determine the optimal execution strategy.
 * 
 * <p>This analyzer determines whether a query can be:
 * <ul>
 *   <li>Fully pushed to DuckDB (no enumeration needed)</li>
 *   <li>Partially pushed (enumeration with Arrow optimization)</li>
 *   <li>Row-by-row processing (standard JDBC enumeration)</li>
 * </ul>
 */
public class DuckDBQueryAnalyzer {
  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDBQueryAnalyzer.class);
  
  public enum ExecutionStrategy {
    /** Full pushdown - DuckDB handles everything, return final result only */
    FULL_PUSHDOWN,
    
    /** Deprecated - Arrow has 400+ms setup overhead, not worth it */
    ARROW_ENUMERATION,
    
    /** Ultra-lazy JDBC - Defers all work until client iterates */
    JDBC_ENUMERATION,
    
    /** Direct result - Single value results (COUNT, SUM, etc.) */
    DIRECT_RESULT
  }
  
  /**
   * Analyzes a SQL query to determine the optimal execution strategy.
   * 
   * @param sql The SQL query to analyze
   * @param estimatedRows Estimated number of result rows (-1 if unknown)
   * @param needsEnumeration Whether Calcite needs to process individual rows
   * @return The recommended execution strategy
   */
  public static ExecutionStrategy analyzeQuery(String sql, long estimatedRows, boolean needsEnumeration) {
    String upperSql = sql.toUpperCase().trim();
    
    // Check if this is a single-result query that doesn't need enumeration
    if (!needsEnumeration && isSingleResultQuery(upperSql)) {
      LOGGER.debug("Query returns single result, using DIRECT_RESULT strategy");
      return ExecutionStrategy.DIRECT_RESULT;
    }
    
    // Check if this is a full aggregation that DuckDB can handle completely
    if (!needsEnumeration && isFullAggregation(upperSql)) {
      LOGGER.debug("Query is full aggregation, using FULL_PUSHDOWN strategy");
      return ExecutionStrategy.FULL_PUSHDOWN;
    }
    
    // If enumeration is required, decide between Arrow and JDBC
    if (needsEnumeration) {
      // Use Arrow for large result sets
      if (estimatedRows > 10000 || estimatedRows == -1) {
        LOGGER.debug("Large result set or unknown size, using ARROW_ENUMERATION strategy");
        return ExecutionStrategy.ARROW_ENUMERATION;
      }
      
      // Check if query has characteristics that benefit from Arrow
      if (hasArrowBenefits(upperSql)) {
        LOGGER.debug("Query has Arrow-beneficial characteristics, using ARROW_ENUMERATION strategy");
        return ExecutionStrategy.ARROW_ENUMERATION;
      }
      
      // Small result sets use JDBC for simplicity
      LOGGER.debug("Small result set, using JDBC_ENUMERATION strategy");
      return ExecutionStrategy.JDBC_ENUMERATION;
    }
    
    // Default to full pushdown if no enumeration needed
    LOGGER.debug("No enumeration needed, using FULL_PUSHDOWN strategy");
    return ExecutionStrategy.FULL_PUSHDOWN;
  }
  
  /**
   * Checks if query returns a single result value.
   */
  private static boolean isSingleResultQuery(String sql) {
    // Queries with COUNT(*), SUM, AVG, etc. without GROUP BY
    boolean hasAggregation = sql.contains("COUNT(") || 
                            sql.contains("SUM(") || 
                            sql.contains("AVG(") || 
                            sql.contains("MIN(") || 
                            sql.contains("MAX(");
    
    boolean hasGroupBy = sql.contains("GROUP BY");
    
    // Single result if aggregation without GROUP BY
    return hasAggregation && !hasGroupBy;
  }
  
  /**
   * Checks if query is a full aggregation that DuckDB handles well.
   */
  private static boolean isFullAggregation(String sql) {
    return sql.contains("GROUP BY") && 
           (sql.contains("COUNT(") || sql.contains("SUM(") || sql.contains("AVG("));
  }
  
  /**
   * Checks if query has characteristics that benefit from Arrow vectors.
   */
  private static boolean hasArrowBenefits(String sql) {
    // Wide tables (many columns)
    int selectCount = countOccurrences(sql, "SELECT");
    int commaCount = countOccurrences(sql.substring(sql.indexOf("SELECT"), 
                                      sql.contains("FROM") ? sql.indexOf("FROM") : sql.length()), ",");
    boolean wideTable = commaCount > 10;
    
    // Complex expressions that benefit from vectorization
    boolean hasComplexExpressions = sql.contains("CASE WHEN") || 
                                   sql.contains("EXTRACT(") ||
                                   countOccurrences(sql, "*") > 2 || // Multiple multiplications
                                   countOccurrences(sql, "/") > 2;   // Multiple divisions
    
    return wideTable || hasComplexExpressions;
  }
  
  private static int countOccurrences(String str, String substr) {
    int count = 0;
    int idx = 0;
    while ((idx = str.indexOf(substr, idx)) != -1) {
      count++;
      idx += substr.length();
    }
    return count;
  }
  
  /**
   * Determines the optimal batch size for Arrow processing.
   */
  public static int getOptimalBatchSize(long estimatedRows) {
    if (estimatedRows <= 0) {
      return 8192; // Default for unknown size
    } else if (estimatedRows < 1000) {
      return 256; // Small batch for small results
    } else if (estimatedRows < 10000) {
      return 1024; // Medium batch
    } else if (estimatedRows < 100000) {
      return 8192; // Large batch
    } else {
      return 65536; // Very large batch for huge datasets
    }
  }
}