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
package org.apache.calcite.adapter.file.metadata;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.CommentableSchema;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * PostgreSQL-compatible system catalog schema (pg_catalog).
 * 
 * <p>Provides PostgreSQL-style system tables for metadata access,
 * including schema comments via pg_description and pg_namespace.
 * Enables PostgreSQL-compatible queries for schema comments:
 * 
 * <pre>
 * SELECT n.nspname as schema_name, d.description 
 * FROM pg_namespace n 
 * LEFT JOIN pg_description d ON d.objoid = n.oid 
 * WHERE d.objsubid = 0;
 * </pre>
 */
public class PostgreSqlCatalogSchema extends AbstractSchema {
  private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSqlCatalogSchema.class);

  private final SchemaPlus rootSchema;
  private final String catalogName;

  public PostgreSqlCatalogSchema(SchemaPlus rootSchema, String catalogName) {
    this.rootSchema = rootSchema;
    this.catalogName = catalogName;
    LOGGER.debug("Created PostgreSQL catalog schema for catalog: {}", catalogName);
  }

  private final Map<String, Table> tableMap = createCaseInsensitiveTableMap();

  @Override protected Map<String, Table> getTableMap() {
    return tableMap;
  }

  private Map<String, Table> createCaseInsensitiveTableMap() {
    Map<String, Table> tables = new HashMap<>();

    // Add PostgreSQL system tables
    Map<String, Table> originalTables = ImmutableMap.<String, Table>builder()
        .put("PG_NAMESPACE", new PgNamespaceTable())
        .put("PG_DESCRIPTION", new PgDescriptionTable())
        .build();

    // Add each table with multiple case variations for case-insensitive access
    for (Map.Entry<String, Table> entry : originalTables.entrySet()) {
      String tableName = entry.getKey();
      Table table = entry.getValue();

      // Add upper, lower, and mixed case variations
      tables.put(tableName.toUpperCase(Locale.ROOT), table);  // PG_NAMESPACE
      tables.put(tableName.toLowerCase(Locale.ROOT), table); // pg_namespace
      // Add title case variation
      if (tableName.length() > 1) {
        String titleCase = tableName.charAt(0) + tableName.substring(1).toLowerCase(Locale.ROOT);
        tables.put(titleCase, table); // Pg_namespace
      }
    }

    return tables;
  }

  /**
   * PG_NAMESPACE - PostgreSQL schema catalog table.
   * Contains information about all schemas (namespaces).
   */
  private class PgNamespaceTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("oid", SqlTypeName.INTEGER)          // Schema OID (synthetic)
          .add("nspname", SqlTypeName.VARCHAR)      // Schema name
          .add("nspowner", SqlTypeName.INTEGER)     // Schema owner OID
          .add("nspacl", typeFactory.createArrayType(          // Access privileges (not used)
              typeFactory.createSqlType(SqlTypeName.VARCHAR), -1))
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();

      int oid = 1000; // Start with synthetic OIDs
      
      // Add all schemas from root
      for (String schemaName : rootSchema.subSchemas().getNames(LikePattern.any())) {
        rows.add(new Object[]{
            oid++,
            schemaName,
            10, // Owner OID (synthetic)
            null // Access control list (not implemented)
        });
      }

      // Add system schemas
      rows.add(new Object[]{11, "information_schema", 10, null});
      rows.add(new Object[]{12, "pg_catalog", 10, null});

      return Linq4j.asEnumerable(rows);
    }

    @Override public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }
  }

  /**
   * PG_DESCRIPTION - PostgreSQL description catalog table.
   * Contains comments for database objects including schemas.
   */
  private class PgDescriptionTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("objoid", SqlTypeName.INTEGER)      // Object OID
          .add("classoid", SqlTypeName.INTEGER)    // System catalog OID
          .add("objsubid", SqlTypeName.INTEGER)    // Object sub-ID (0 for schema)
          .add("description", SqlTypeName.VARCHAR) // Comment text
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();

      int oid = 1000; // Match OIDs from PgNamespaceTable
      
      // Scan all schemas for comments
      for (String schemaName : rootSchema.subSchemas().getNames(LikePattern.any())) {
        Schema schema = rootSchema.subSchemas().get(schemaName);
        if (schema != null && schema instanceof CommentableSchema) {
          CommentableSchema commentableSchema = (CommentableSchema) schema;
          String comment = commentableSchema.getComment();
          
          if (comment != null && !comment.trim().isEmpty()) {
            rows.add(new Object[]{
                oid,      // objoid - matches pg_namespace.oid
                2615,     // classoid - pg_namespace table OID in PostgreSQL
                0,        // objsubid - 0 for schema-level comments
                comment.trim()  // description
            });
          }
        }
        oid++;
      }

      return Linq4j.asEnumerable(rows);
    }

    @Override public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }
  }
}