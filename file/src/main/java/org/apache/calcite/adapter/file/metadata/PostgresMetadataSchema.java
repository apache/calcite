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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * PostgreSQL-compatible metadata schema implementation.
 * Provides pg_catalog tables for PostgreSQL client compatibility.
 */
public class PostgresMetadataSchema extends AbstractSchema {
  private final SchemaPlus rootSchema;
  private final String catalogName;

  public PostgresMetadataSchema(SchemaPlus rootSchema, String catalogName) {
    this.rootSchema = rootSchema;
    this.catalogName = catalogName;
  }

  private final Map<String, Table> tableMap = createCaseInsensitiveTableMap();

  @Override protected Map<String, Table> getTableMap() {
    return tableMap;
  }


  private Map<String, Table> createCaseInsensitiveTableMap() {
    Map<String, Table> tables = new HashMap<>();

    // Add tables with case variations for case-insensitive lookup
    Map<String, Table> originalTables = ImmutableMap.<String, Table>builder()
        .put("pg_namespace", new PgNamespaceTable())
        .put("pg_class", new PgClassTable())
        .put("pg_attribute", new PgAttributeTable())
        .put("pg_type", new PgTypeTable())
        .put("pg_database", new PgDatabaseTable())
        .put("pg_tables", new PgTablesView())
        .put("pg_views", new PgViewsView())
        .build();

    // Add each table with multiple case variations for case-insensitive access
    for (Map.Entry<String, Table> entry : originalTables.entrySet()) {
      String tableName = entry.getKey();
      Table table = entry.getValue();

      // Add lower, upper, and mixed case variations
      tables.put(tableName.toLowerCase(Locale.ROOT), table); // pg_namespace
      tables.put(tableName.toUpperCase(Locale.ROOT), table); // PG_NAMESPACE
      // Add title case variation for multi-part names
      if (tableName.contains("_")) {
        String[] parts = tableName.split("_");
        StringBuilder titleCase = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
          if (i > 0) titleCase.append("_");
          titleCase.append(parts[i].substring(0, 1).toUpperCase(Locale.ROOT))
                   .append(parts[i].substring(1).toLowerCase(Locale.ROOT));
        }
        tables.put(titleCase.toString(), table); // Pg_Namespace
      }
    }

    return tables;
  }

  /**
   * pg_namespace - schemas/namespaces
   */
  private class PgNamespaceTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("oid", SqlTypeName.INTEGER)
          .add("nspname", SqlTypeName.VARCHAR)
          .add("nspowner", SqlTypeName.INTEGER)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();
      int oid = 1000;

      // Add all schemas
      for (String schemaName : rootSchema.subSchemas().getNames(LikePattern.any())) {
        rows.add(new Object[]{oid++, schemaName, 1});
      }

      return Linq4j.asEnumerable(rows);
    }

    @Override public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }
  }

  /**
   * pg_class - tables, indexes, sequences, views
   */
  private class PgClassTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("oid", SqlTypeName.INTEGER)
          .add("relname", SqlTypeName.VARCHAR)
          .add("relnamespace", SqlTypeName.INTEGER)
          .add("reltype", SqlTypeName.INTEGER)
          .add("relowner", SqlTypeName.INTEGER)
          .add("relkind", SqlTypeName.CHAR)
          .add("relnatts", SqlTypeName.SMALLINT)
          .add("relhaspkey", SqlTypeName.BOOLEAN)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();
      int oid = 10000;
      int namespaceOid = 1000;

      // Iterate through all schemas
      for (String schemaName : rootSchema.subSchemas().getNames(LikePattern.any())) {
        SchemaPlus schema = rootSchema.subSchemas().get(schemaName);
        if (schema != null) {
          // Add all tables in this schema
          for (String tableName : schema.tables().getNames(LikePattern.any())) {
            Table table = schema.tables().get(tableName);
            if (table != null) {
              RelDataType rowType = table.getRowType(root.getTypeFactory());
              int columnCount = rowType.getFieldCount();

              rows.add(new Object[]{
                  oid++,                    // oid
                  tableName.toLowerCase(),  // relname
                  namespaceOid,            // relnamespace
                  oid,                     // reltype
                  1,                       // relowner
                  'r',                     // relkind ('r' for table)
                  (short) columnCount,     // relnatts
                  false                    // relhaspkey
              });
            }
          }
        }
        namespaceOid++;
      }

      return Linq4j.asEnumerable(rows);
    }

    @Override public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }
  }

  /**
   * pg_attribute - table columns
   */
  private class PgAttributeTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("attrelid", SqlTypeName.INTEGER)
          .add("attname", SqlTypeName.VARCHAR)
          .add("atttypid", SqlTypeName.INTEGER)
          .add("attlen", SqlTypeName.SMALLINT)
          .add("attnum", SqlTypeName.SMALLINT)
          .add("attnotnull", SqlTypeName.BOOLEAN)
          .add("atthasdef", SqlTypeName.BOOLEAN)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();
      int tableOid = 10000;

      // Iterate through all schemas
      for (String schemaName : rootSchema.subSchemas().getNames(LikePattern.any())) {
        SchemaPlus schema = rootSchema.subSchemas().get(schemaName);
        if (schema != null) {
          // Add columns for all tables
          for (String tableName : schema.tables().getNames(LikePattern.any())) {
            Table table = schema.tables().get(tableName);
            if (table != null) {
              RelDataType rowType = table.getRowType(root.getTypeFactory());
              short attnum = 1;

              for (RelDataTypeField field : rowType.getFieldList()) {
                rows.add(new Object[]{
                    tableOid,                        // attrelid
                    field.getName().toLowerCase(),   // attname
                    mapSqlTypeToOid(field.getType().getSqlTypeName()), // atttypid
                    (short) -1,                      // attlen (-1 for variable)
                    attnum++,                        // attnum
                    !field.getType().isNullable(),  // attnotnull
                    false                            // atthasdef
                });
              }
              tableOid++;
            }
          }
        }
      }

      return Linq4j.asEnumerable(rows);
    }

    @Override public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }
  }

  /**
   * pg_type - data types
   */
  private class PgTypeTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("oid", SqlTypeName.INTEGER)
          .add("typname", SqlTypeName.VARCHAR)
          .add("typnamespace", SqlTypeName.INTEGER)
          .add("typlen", SqlTypeName.SMALLINT)
          .add("typtype", SqlTypeName.CHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();

      // Add common PostgreSQL types
      rows.add(new Object[]{16, "bool", 11, (short) 1, 'b'});
      rows.add(new Object[]{20, "int8", 11, (short) 8, 'b'});
      rows.add(new Object[]{21, "int2", 11, (short) 2, 'b'});
      rows.add(new Object[]{23, "int4", 11, (short) 4, 'b'});
      rows.add(new Object[]{25, "text", 11, (short) -1, 'b'});
      rows.add(new Object[]{700, "float4", 11, (short) 4, 'b'});
      rows.add(new Object[]{701, "float8", 11, (short) 8, 'b'});
      rows.add(new Object[]{1043, "varchar", 11, (short) -1, 'b'});
      rows.add(new Object[]{1082, "date", 11, (short) 4, 'b'});
      rows.add(new Object[]{1083, "time", 11, (short) 8, 'b'});
      rows.add(new Object[]{1114, "timestamp", 11, (short) 8, 'b'});
      rows.add(new Object[]{1184, "timestamptz", 11, (short) 8, 'b'});
      rows.add(new Object[]{1700, "numeric", 11, (short) -1, 'b'});

      return Linq4j.asEnumerable(rows);
    }

    @Override public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }
  }

  /**
   * pg_database - databases
   */
  private class PgDatabaseTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("oid", SqlTypeName.INTEGER)
          .add("datname", SqlTypeName.VARCHAR)
          .add("datdba", SqlTypeName.INTEGER)
          .add("encoding", SqlTypeName.INTEGER)
          .add("datcollate", SqlTypeName.VARCHAR)
          .add("datctype", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();
      rows.add(new Object[]{1, catalogName, 1, 6, "en_US.UTF-8", "en_US.UTF-8"});
      return Linq4j.asEnumerable(rows);
    }

    @Override public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }
  }

  /**
   * pg_tables view - simplified table listing
   */
  private class PgTablesView extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("schemaname", SqlTypeName.VARCHAR)
          .add("tablename", SqlTypeName.VARCHAR)
          .add("tableowner", SqlTypeName.VARCHAR)
          .add("tablespace", SqlTypeName.VARCHAR)
          .add("hasindexes", SqlTypeName.BOOLEAN)
          .add("hasrules", SqlTypeName.BOOLEAN)
          .add("hastriggers", SqlTypeName.BOOLEAN)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();

      for (String schemaName : rootSchema.subSchemas().getNames(LikePattern.any())) {
        SchemaPlus schema = rootSchema.subSchemas().get(schemaName);
        if (schema != null) {
          for (String tableName : schema.tables().getNames(LikePattern.any())) {
            rows.add(new Object[]{
                schemaName.toLowerCase(),
                tableName.toLowerCase(),
                "calcite",
                null,
                false,
                false,
                false
            });
          }
        }
      }

      return Linq4j.asEnumerable(rows);
    }

    @Override public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }
  }

  /**
   * pg_views view - view listing (empty for file adapter)
   */
  private class PgViewsView extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("schemaname", SqlTypeName.VARCHAR)
          .add("viewname", SqlTypeName.VARCHAR)
          .add("viewowner", SqlTypeName.VARCHAR)
          .add("definition", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      // File adapter doesn't have views yet
      return Linq4j.asEnumerable(ImmutableList.of());
    }

    @Override public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }
  }

  // Helper to map Calcite SQL types to PostgreSQL type OIDs
  private int mapSqlTypeToOid(SqlTypeName typeName) {
    switch (typeName) {
      case BOOLEAN:
        return 16;  // bool
      case TINYINT:
      case SMALLINT:
        return 21;  // int2
      case INTEGER:
        return 23;  // int4
      case BIGINT:
        return 20;  // int8
      case FLOAT:
      case REAL:
        return 700; // float4
      case DOUBLE:
        return 701; // float8
      case DECIMAL:
        return 1700; // numeric
      case CHAR:
      case VARCHAR:
        return 1043; // varchar
      case DATE:
        return 1082; // date
      case TIME:
        return 1083; // time
      case TIMESTAMP:
        return 1114; // timestamp
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return 1184; // timestamptz
      default:
        return 25;   // text (fallback)
    }
  }

  // Removed duplicate helper class - use org.apache.calcite.rel.type.RelDataTypeField directly
}
