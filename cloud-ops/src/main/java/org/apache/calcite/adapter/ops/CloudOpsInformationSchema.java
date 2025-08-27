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
package org.apache.calcite.adapter.ops;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Schema that provides SQL standard information_schema tables for Cloud Governance.
 * Following the established File/Splunk adapter pattern.
 */
public class CloudOpsInformationSchema extends AbstractSchema {

  private final SchemaPlus parentSchema;
  private final String catalogName;

  public CloudOpsInformationSchema(SchemaPlus parentSchema, String catalogName) {
    this.parentSchema = parentSchema;
    this.catalogName = catalogName;
  }

  @Override protected Map<String, Table> getTableMap() {
    // Create a case-insensitive map that stores uppercase keys but accepts any case
    Map<String, Table> caseInsensitiveMap = new CaseInsensitiveTableMap();
    caseInsensitiveMap.put("SCHEMATA", new SchemataTable());
    caseInsensitiveMap.put("TABLES", new TablesTable());
    caseInsensitiveMap.put("COLUMNS", new ColumnsTable());
    caseInsensitiveMap.put("VIEWS", new ViewsTable());
    caseInsensitiveMap.put("TABLE_CONSTRAINTS", new TableConstraintsTable());
    caseInsensitiveMap.put("KEY_COLUMN_USAGE", new KeyColumnUsageTable());
    return caseInsensitiveMap;
  }


  /**
   * information_schema.SCHEMATA table.
   */
  private class SchemataTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("CATALOG_NAME", SqlTypeName.VARCHAR)
          .add("SCHEMA_NAME", SqlTypeName.VARCHAR)
          .add("SCHEMA_OWNER", SqlTypeName.VARCHAR)
          .add("DEFAULT_CHARACTER_SET_CATALOG", SqlTypeName.VARCHAR)
          .add("DEFAULT_CHARACTER_SET_SCHEMA", SqlTypeName.VARCHAR)
          .add("DEFAULT_CHARACTER_SET_NAME", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();

      // Add standard system schemas
      rows.add(new Object[] {catalogName, "information_schema", "SYSTEM", catalogName, "information_schema", "UTF8"});
      rows.add(new Object[] {catalogName, "pg_catalog", "SYSTEM", catalogName, "information_schema", "UTF8"});

      // Add all other schemas - navigate to root to see all sibling schemas
      SchemaPlus rootSchema = parentSchema;
      while (rootSchema.getParentSchema() != null) {
        rootSchema = rootSchema.getParentSchema();
      }

      for (String schemaName : rootSchema.subSchemas().getNames(LikePattern.any())) {
        if (!"information_schema".equals(schemaName) && !"pg_catalog".equals(schemaName)) {
          rows.add(new Object[] {catalogName, schemaName, "CLOUD_GOVERNANCE_ADMIN", catalogName, "information_schema", "UTF8"});
        }
      }

      return Linq4j.asEnumerable(rows);
    }
  }

  /**
   * information_schema.TABLES table.
   */
  private class TablesTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("TABLE_CATALOG", SqlTypeName.VARCHAR)
          .add("TABLE_SCHEMA", SqlTypeName.VARCHAR)
          .add("TABLE_NAME", SqlTypeName.VARCHAR)
          .add("TABLE_TYPE", SqlTypeName.VARCHAR)
          .add("IS_INSERTABLE_INTO", SqlTypeName.VARCHAR)
          .add("IS_TYPED", SqlTypeName.VARCHAR)
          .add("COMMIT_ACTION", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();

      // Need to check all schemas, not just direct children of root
      scanSchemaRecursively(null, rows);

      return Linq4j.asEnumerable(rows);
    }

    private void scanSchemaRecursively(SchemaPlus schema, List<Object[]> rows) {
      // Navigate to root schema to see all schemas
      SchemaPlus rootSchema = parentSchema;
      while (rootSchema.getParentSchema() != null) {
        rootSchema = rootSchema.getParentSchema();
      }

      for (String schemaName : rootSchema.subSchemas().getNames(LikePattern.any())) {
        SchemaPlus subSchema = rootSchema.subSchemas().get(schemaName);
        if (subSchema != null && !"information_schema".equals(schemaName) && !"pg_catalog".equals(schemaName)) {
          // Try to get tables directly from the SchemaPlus
          try {
            for (String tableName : subSchema.tables().getNames(LikePattern.any())) {
              rows.add(new Object[] {
                  catalogName,          // TABLE_CATALOG
                  schemaName,           // TABLE_SCHEMA
                  tableName,            // TABLE_NAME
                  "BASE TABLE",         // TABLE_TYPE
                  "NO",                 // IS_INSERTABLE_INTO (Cloud resources are read-only)
                  "NO",                 // IS_TYPED
                  null                  // COMMIT_ACTION
              });
            }
          } catch (Exception e) {
            // Ignore errors accessing tables
          }
        }
      }
    }
  }

  /**
   * information_schema.COLUMNS table.
   */
  private class ColumnsTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("TABLE_CATALOG", SqlTypeName.VARCHAR)
          .add("TABLE_SCHEMA", SqlTypeName.VARCHAR)
          .add("TABLE_NAME", SqlTypeName.VARCHAR)
          .add("COLUMN_NAME", SqlTypeName.VARCHAR)
          .add("ORDINAL_POSITION", SqlTypeName.INTEGER)
          .add("COLUMN_DEFAULT", SqlTypeName.VARCHAR)
          .add("IS_NULLABLE", SqlTypeName.VARCHAR)
          .add("DATA_TYPE", SqlTypeName.VARCHAR)
          .add("CHARACTER_MAXIMUM_LENGTH", SqlTypeName.INTEGER)
          .add("NUMERIC_PRECISION", SqlTypeName.INTEGER)
          .add("NUMERIC_SCALE", SqlTypeName.INTEGER)
          .add("DATETIME_PRECISION", SqlTypeName.INTEGER)
          .add("CHARACTER_SET_CATALOG", SqlTypeName.VARCHAR)
          .add("CHARACTER_SET_SCHEMA", SqlTypeName.VARCHAR)
          .add("CHARACTER_SET_NAME", SqlTypeName.VARCHAR)
          .add("COLLATION_CATALOG", SqlTypeName.VARCHAR)
          .add("COLLATION_SCHEMA", SqlTypeName.VARCHAR)
          .add("COLLATION_NAME", SqlTypeName.VARCHAR)
          .add("DOMAIN_CATALOG", SqlTypeName.VARCHAR)
          .add("DOMAIN_SCHEMA", SqlTypeName.VARCHAR)
          .add("DOMAIN_NAME", SqlTypeName.VARCHAR)
          .add("IS_IDENTITY", SqlTypeName.VARCHAR)
          .add("IS_GENERATED", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();
      RelDataTypeFactory typeFactory =
          root != null ? root.getTypeFactory()
              : new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
              org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

      // Need to check all schemas, not just direct children of root
      scanSchemaRecursively(null, rows, typeFactory);
      return Linq4j.asEnumerable(rows);
    }

    private void scanSchemaRecursively(SchemaPlus schema, List<Object[]> rows,
        RelDataTypeFactory typeFactory) {
      // Navigate to root schema to see all schemas
      SchemaPlus rootSchema = parentSchema;
      while (rootSchema.getParentSchema() != null) {
        rootSchema = rootSchema.getParentSchema();
      }

      for (String schemaName : rootSchema.subSchemas().getNames(LikePattern.any())) {
        SchemaPlus subSchema = rootSchema.subSchemas().get(schemaName);
        if (subSchema != null && !"information_schema".equals(schemaName) && !"pg_catalog".equals(schemaName)) {
          
          // Try multiple unwrapping approaches
          CloudOpsSchema cloudOpsSchema = null;
          
          // Approach 1: Direct unwrap attempt
          try {
            cloudOpsSchema = subSchema.unwrap(CloudOpsSchema.class);
          } catch (Exception e) {
            // Continue to other approaches
          }
          
          // Approach 2: If direct unwrap failed, try reflection on the SchemaPlusImpl
          if (cloudOpsSchema == null) {
            try {
              // Get the underlying CalciteSchema from SchemaPlusImpl
              java.lang.reflect.Field calciteSchemaField = subSchema.getClass().getDeclaredField("calciteSchema");
              calciteSchemaField.setAccessible(true);
              Object calciteSchema = calciteSchemaField.get(subSchema);

              if (calciteSchema != null) {
                // Get the actual schema from CalciteSchema
                java.lang.reflect.Field schemaField = calciteSchema.getClass().getDeclaredField("schema");
                schemaField.setAccessible(true);
                Object actualSchema = schemaField.get(calciteSchema);
                
                if (actualSchema instanceof CloudOpsSchema) {
                  cloudOpsSchema = (CloudOpsSchema) actualSchema;
                }
              }
            } catch (Exception e) {
              // Continue to next approach
            }
          }
          
          // Approach 3: If still null, try generic Schema unwrap then reflection
          if (cloudOpsSchema == null) {
            try {
              Schema unwrapped = subSchema.unwrap(Schema.class);
              
              if (unwrapped != null && unwrapped.getClass().getName().contains("CalciteSchema$SchemaPlusImpl")) {
                // Try the same reflection approach on the unwrapped Schema
                java.lang.reflect.Field calciteSchemaField = unwrapped.getClass().getDeclaredField("calciteSchema");
                calciteSchemaField.setAccessible(true);
                Object calciteSchema = calciteSchemaField.get(unwrapped);

                if (calciteSchema != null) {
                  java.lang.reflect.Field schemaField = calciteSchema.getClass().getDeclaredField("schema");
                  schemaField.setAccessible(true);
                  Object actualSchema = schemaField.get(calciteSchema);
                  if (actualSchema instanceof CloudOpsSchema) {
                    cloudOpsSchema = (CloudOpsSchema) actualSchema;
                  }
                }
              }
            } catch (Exception e) {
              // No more approaches available
            }
          }

          if (cloudOpsSchema != null) {
            Map<String, Table> tables = cloudOpsSchema.getTableMapForMetadata();

            for (Map.Entry<String, Table> entry : tables.entrySet()) {
              String tableName = entry.getKey();
              Table table = entry.getValue();
              RelDataType rowType = table.getRowType(typeFactory);

              int ordinalPosition = 1;
              for (RelDataTypeField field : rowType.getFieldList()) {
                RelDataType fieldType = field.getType();
                rows.add(new Object[] {
                    catalogName,                              // TABLE_CATALOG
                    schemaName,                               // TABLE_SCHEMA
                    tableName,                                // TABLE_NAME
                    field.getName(),                          // COLUMN_NAME
                    ordinalPosition++,                        // ORDINAL_POSITION
                    null,                                     // COLUMN_DEFAULT
                    fieldType.isNullable() ? "YES" : "NO",    // IS_NULLABLE
                    mapSqlTypeToStandardType(fieldType),      // DATA_TYPE
                    getCharacterMaxLength(fieldType),         // CHARACTER_MAXIMUM_LENGTH
                    getNumericPrecision(fieldType),           // NUMERIC_PRECISION
                    getNumericScale(fieldType),               // NUMERIC_SCALE
                    getDatetimePrecision(fieldType),          // DATETIME_PRECISION
                    null,                                     // CHARACTER_SET_CATALOG
                    null,                                     // CHARACTER_SET_SCHEMA
                    null,                                     // CHARACTER_SET_NAME
                    null,                                     // COLLATION_CATALOG
                    null,                                     // COLLATION_SCHEMA
                    null,                                     // COLLATION_NAME
                    null,                                     // DOMAIN_CATALOG
                    null,                                     // DOMAIN_SCHEMA
                    null,                                     // DOMAIN_NAME
                    "NO",                                     // IS_IDENTITY
                    "NEVER"                                   // IS_GENERATED
                });
              }
            }
          }
        }
      }
    }

    private String mapSqlTypeToStandardType(RelDataType type) {
      switch (type.getSqlTypeName()) {
        case INTEGER: return "INTEGER";
        case BIGINT: return "BIGINT";
        case SMALLINT: return "SMALLINT";
        case VARCHAR: return "CHARACTER VARYING";
        case CHAR: return "CHARACTER";
        case TIMESTAMP: return "TIMESTAMP";
        case BOOLEAN: return "BOOLEAN";
        case DOUBLE: return "DOUBLE PRECISION";
        case REAL: return "REAL";
        default: return "CHARACTER VARYING";
      }
    }

    private Integer getCharacterMaxLength(RelDataType type) {
      if (type.getSqlTypeName() == SqlTypeName.VARCHAR ||
          type.getSqlTypeName() == SqlTypeName.CHAR) {
        return type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED ?
            null : type.getPrecision();
      }
      return null;
    }

    private Integer getNumericPrecision(RelDataType type) {
      if (SqlTypeName.NUMERIC_TYPES.contains(type.getSqlTypeName())) {
        return type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED ?
            null : type.getPrecision();
      }
      return null;
    }

    private Integer getNumericScale(RelDataType type) {
      if (SqlTypeName.NUMERIC_TYPES.contains(type.getSqlTypeName())) {
        return type.getScale() == RelDataType.SCALE_NOT_SPECIFIED ?
            null : type.getScale();
      }
      return null;
    }

    private Integer getDatetimePrecision(RelDataType type) {
      if (SqlTypeName.DATETIME_TYPES.contains(type.getSqlTypeName())) {
        return type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED ?
            null : type.getPrecision();
      }
      return null;
    }
  }

  /**
   * information_schema.VIEWS table (empty for Cloud Governance).
   */
  private class ViewsTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("TABLE_CATALOG", SqlTypeName.VARCHAR)
          .add("TABLE_SCHEMA", SqlTypeName.VARCHAR)
          .add("TABLE_NAME", SqlTypeName.VARCHAR)
          .add("VIEW_DEFINITION", SqlTypeName.VARCHAR)
          .add("CHECK_OPTION", SqlTypeName.VARCHAR)
          .add("IS_UPDATABLE", SqlTypeName.VARCHAR)
          .add("IS_INSERTABLE_INTO", SqlTypeName.VARCHAR)
          .add("IS_TRIGGER_UPDATABLE", SqlTypeName.VARCHAR)
          .add("IS_TRIGGER_DELETABLE", SqlTypeName.VARCHAR)
          .add("IS_TRIGGER_INSERTABLE_INTO", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      // Cloud Governance adapter doesn't support views
      return Linq4j.asEnumerable(new ArrayList<Object[]>());
    }
  }

  /**
   * information_schema.TABLE_CONSTRAINTS table (empty for Cloud Governance).
   */
  private class TableConstraintsTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("CONSTRAINT_CATALOG", SqlTypeName.VARCHAR)
          .add("CONSTRAINT_SCHEMA", SqlTypeName.VARCHAR)
          .add("CONSTRAINT_NAME", SqlTypeName.VARCHAR)
          .add("TABLE_CATALOG", SqlTypeName.VARCHAR)
          .add("TABLE_SCHEMA", SqlTypeName.VARCHAR)
          .add("TABLE_NAME", SqlTypeName.VARCHAR)
          .add("CONSTRAINT_TYPE", SqlTypeName.VARCHAR)
          .add("IS_DEFERRABLE", SqlTypeName.VARCHAR)
          .add("INITIALLY_DEFERRED", SqlTypeName.VARCHAR)
          .add("ENFORCED", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      // Cloud Governance adapter doesn't have explicit constraints
      return Linq4j.asEnumerable(new ArrayList<Object[]>());
    }
  }

  /**
   * information_schema.KEY_COLUMN_USAGE table (empty for Cloud Governance).
   */
  private class KeyColumnUsageTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("CONSTRAINT_CATALOG", SqlTypeName.VARCHAR)
          .add("CONSTRAINT_SCHEMA", SqlTypeName.VARCHAR)
          .add("CONSTRAINT_NAME", SqlTypeName.VARCHAR)
          .add("TABLE_CATALOG", SqlTypeName.VARCHAR)
          .add("TABLE_SCHEMA", SqlTypeName.VARCHAR)
          .add("TABLE_NAME", SqlTypeName.VARCHAR)
          .add("COLUMN_NAME", SqlTypeName.VARCHAR)
          .add("ORDINAL_POSITION", SqlTypeName.INTEGER)
          .add("POSITION_IN_UNIQUE_CONSTRAINT", SqlTypeName.INTEGER)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      // Cloud Governance adapter doesn't have key constraints
      return Linq4j.asEnumerable(new ArrayList<Object[]>());
    }
  }

  /**
   * Case-insensitive map for table lookups.
   * Stores keys in uppercase but accepts lookups in any case.
   */
  private static class CaseInsensitiveTableMap extends AbstractMap<String, Table> {
    private final Map<String, Table> map = new LinkedHashMap<>();

    @Override public Table put(String key, Table value) {
      // Store with uppercase key for ORACLE lex compatibility
      return map.put(key.toUpperCase(), value);
    }

    @Override public Table get(Object key) {
      if (key instanceof String) {
        return map.get(((String) key).toUpperCase());
      }
      return null;
    }

    @Override public Set<Entry<String, Table>> entrySet() {
      return map.entrySet();
    }

    @Override public boolean containsKey(Object key) {
      if (key instanceof String) {
        return map.containsKey(((String) key).toUpperCase());
      }
      return false;
    }

    @Override public int size() {
      return map.size();
    }

    @Override public Collection<Table> values() {
      return map.values();
    }

    @Override public Set<String> keySet() {
      return map.keySet();
    }
  }
}
