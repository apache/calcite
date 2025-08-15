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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts Calcite RelNode trees to DuckDB-compatible SQL.
 * 
 * <p>DuckDB uses PostgreSQL-compatible SQL syntax, so we leverage
 * Calcite's PostgreSQL dialect with DuckDB-specific optimizations.
 * 
 * <p>Key features:
 * <ul>
 *   <li>PostgreSQL-compatible syntax generation</li>
 *   <li>DuckDB-specific function mappings</li>
 *   <li>Efficient pushdown of complex operations</li>
 *   <li>Window function and aggregation support</li>
 * </ul>
 */
public final class DuckDBSqlGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDBSqlGenerator.class);

  /**
   * DuckDB SQL dialect based on PostgreSQL.
   * DuckDB is largely PostgreSQL-compatible with some extensions.
   */
  private static final SqlDialect DUCKDB_DIALECT = PostgresqlSqlDialect.DEFAULT;

  private DuckDBSqlGenerator() {
    // Utility class
  }

  /**
   * Generates DuckDB-compatible SQL from a Calcite RelNode.
   * 
   * @param relNode the relational algebra tree to convert
   * @return SQL string that can be executed by DuckDB
   */
  public static String generate(RelNode relNode) {
    try {
      LOGGER.debug("Converting RelNode to DuckDB SQL: {}", relNode);
      
      // Use Calcite's built-in RelToSqlConverter with DuckDB dialect
      RelToSqlConverter converter = new RelToSqlConverter(DUCKDB_DIALECT);
      SqlNode sqlNode = converter.visitRoot(relNode).asStatement();
      
      String sql = sqlNode.toSqlString(DUCKDB_DIALECT).getSql();
      LOGGER.debug("Generated DuckDB SQL: {}", sql);
      
      return sql;
      
    } catch (Exception e) {
      LOGGER.error("Failed to convert RelNode to DuckDB SQL: {}", relNode, e);
      throw new RuntimeException("SQL generation failed", e);
    }
  }

  /**
   * Generates a simple SELECT * query for the given table.
   * This is used as a fallback when full RelNode conversion fails.
   * 
   * @param tableName name of the table to query
   * @return simple SELECT * SQL string
   */
  public static String generateSelectAll(String tableName) {
    String quotedTableName = DUCKDB_DIALECT.quoteIdentifier(tableName);
    String sql = String.format("SELECT * FROM %s", quotedTableName);
    LOGGER.debug("Generated simple DuckDB SQL: {}", sql);
    return sql;
  }

  /**
   * Generates a projection query with the specified column names.
   * 
   * @param tableName name of the table to query
   * @param columnNames list of column names to select
   * @return projection SQL string
   */
  public static String generateProjection(String tableName, Iterable<String> columnNames) {
    StringBuilder columns = new StringBuilder();
    boolean first = true;
    
    for (String column : columnNames) {
      if (!first) {
        columns.append(", ");
      }
      columns.append(DUCKDB_DIALECT.quoteIdentifier(column));
      first = false;
    }
    
    String quotedTableName = DUCKDB_DIALECT.quoteIdentifier(tableName);
    String sql = String.format("SELECT %s FROM %s", 
        columns.length() > 0 ? columns.toString() : "*", 
        quotedTableName);
        
    LOGGER.debug("Generated projection DuckDB SQL: {}", sql);
    return sql;
  }

  /**
   * Gets the DuckDB SQL dialect used for generation.
   * 
   * @return the SqlDialect instance
   */
  public static SqlDialect getDialect() {
    return DUCKDB_DIALECT;
  }
}