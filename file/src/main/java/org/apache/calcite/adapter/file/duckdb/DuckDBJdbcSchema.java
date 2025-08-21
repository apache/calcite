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
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.plan.RelOptPlanner;

import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;

/**
 * JDBC schema implementation for DuckDB that ensures complete query pushdown.
 * All aggregations, filters, joins, and other operations are executed in DuckDB.
 * Maintains a persistent connection to keep the named in-memory database alive.
 */
public class DuckDBJdbcSchema extends JdbcSchema {
  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDBJdbcSchema.class);
  
  private final File directory;
  private final boolean recursive;
  private final Connection persistentConnection;
  private final org.apache.calcite.adapter.file.FileSchema fileSchema; // Keep reference for refreshes
  
  public DuckDBJdbcSchema(DataSource dataSource, SqlDialect dialect,
                         JdbcConvention convention, String catalog, String schema,
                         File directory, boolean recursive, Connection persistentConnection,
                         org.apache.calcite.adapter.file.FileSchema fileSchema) {
    // DuckDB uses in-memory databases where catalog concept is irrelevant
    // Always pass null as catalog to ensure 2-part naming (schema.table)
    super(dataSource, dialect, convention, null, schema);
    this.directory = directory;
    this.recursive = recursive;
    this.persistentConnection = persistentConnection;
    this.fileSchema = fileSchema; // Keep FileSchema alive for refresh handling
    
    LOGGER.info("Created DuckDB JDBC schema for directory: {} (recursive={}) with persistent connection", 
                directory, recursive);
  }
  
  
  @Override
  public Set<String> getTableNames() {
    Set<String> tableNames = super.getTableNames();
    LOGGER.debug("DuckDB schema tables available: {}", tableNames);
    return tableNames;
  }
  
  @Override
  public Table getTable(String name) {
    LOGGER.info("Looking for table: '{}'", name);
    Table table = super.getTable(name);
    if (table != null) {
      LOGGER.info("Found DuckDB table '{}' - all operations will be pushed to DuckDB", name);
    } else {
      LOGGER.warn("Table '{}' not found in DuckDB schema", name);
      // Try lowercase version
      table = super.getTable(name.toLowerCase());
      if (table != null) {
        LOGGER.info("Found table with lowercase name: '{}'", name.toLowerCase());
      }
    }
    return table;
  }
}