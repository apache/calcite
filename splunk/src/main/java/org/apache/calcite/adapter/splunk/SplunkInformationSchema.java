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
package org.apache.calcite.adapter.splunk;

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

import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * SQL standard information_schema for Splunk adapter.
 * Provides standard metadata views for compatibility with SQL tools.
 */
public class SplunkInformationSchema extends AbstractSchema {

  private final SchemaPlus rootSchema;
  private final String catalogName;

  public SplunkInformationSchema(SchemaPlus rootSchema, String catalogName) {
    this.rootSchema = rootSchema;
    this.catalogName = catalogName;
  }

  @Override protected Map<String, Table> getTableMap() {
    return ImmutableMap.<String, Table>builder()
        .put("SCHEMATA", new CaseInsensitiveTableWrapper(new SchemataTable()))
        .put("TABLES", new CaseInsensitiveTableWrapper(new TablesTable()))
        .put("COLUMNS", new CaseInsensitiveTableWrapper(new ColumnsTable()))
        .put("TABLE_CONSTRAINTS", new CaseInsensitiveTableWrapper(new TableConstraintsTable()))
        .put("KEY_COLUMN_USAGE", new CaseInsensitiveTableWrapper(new KeyColumnUsageTable()))
        .put("VIEWS", new CaseInsensitiveTableWrapper(new ViewsTable()))
        .put("ROUTINES", new CaseInsensitiveTableWrapper(new RoutinesTable()))
        .put("PARAMETERS", new CaseInsensitiveTableWrapper(new ParametersTable()))
        .build();
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
          .add("SQL_PATH", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();

      for (String schemaName : rootSchema.subSchemas().getNames(LikePattern.any())) {
        rows.add(new Object[] {
            catalogName,              // CATALOG_NAME
            schemaName,               // SCHEMA_NAME
            "splunk_admin",           // SCHEMA_OWNER
            catalogName,              // DEFAULT_CHARACTER_SET_CATALOG
            "information_schema",     // DEFAULT_CHARACTER_SET_SCHEMA
            "UTF8",                   // DEFAULT_CHARACTER_SET_NAME
            null                      // SQL_PATH
        });
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
      scanSchemaRecursively(rootSchema, rows);

      return Linq4j.asEnumerable(rows);
    }

    private void scanSchemaRecursively(SchemaPlus schema, List<Object[]> rows) {
      for (String schemaName : schema.subSchemas().getNames(LikePattern.any())) {
        SchemaPlus subSchema = schema.subSchemas().get(schemaName);
        if (subSchema != null) {
          // Try to get tables directly from the SchemaPlus
          try {
            for (String tableName : subSchema.tables().getNames(LikePattern.any())) {
              // Only add tables from actual data schemas, not metadata schemas
              if (!"information_schema".equals(schemaName)
                  && !"pg_catalog".equals(schemaName)
                  && !"metadata".equals(schemaName)) {
                rows.add(new Object[] {
                    catalogName,          // TABLE_CATALOG
                    schemaName,           // TABLE_SCHEMA
                    tableName,            // TABLE_NAME
                    "BASE TABLE",         // TABLE_TYPE
                    "NO",                 // IS_INSERTABLE_INTO (Splunk is read-only)
                    "NO",                 // IS_TYPED
                    null                  // COMMIT_ACTION
                });
              }
            }
          } catch (Exception e) {
            // Ignore errors
          }

          // Recursively scan sub-schemas
          scanSchemaRecursively(subSchema, rows);
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
          .add("CHARACTER_OCTET_LENGTH", SqlTypeName.INTEGER)
          .add("NUMERIC_PRECISION", SqlTypeName.INTEGER)
          .add("NUMERIC_PRECISION_RADIX", SqlTypeName.INTEGER)
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
      scanSchemaRecursively(rootSchema, rows, typeFactory);

      return Linq4j.asEnumerable(rows);
    }

    private void scanSchemaRecursively(SchemaPlus schema, List<Object[]> rows,
        RelDataTypeFactory typeFactory) {
      for (String schemaName : schema.subSchemas().getNames(LikePattern.any())) {
        SchemaPlus subSchema = schema.subSchemas().get(schemaName);
        if (subSchema != null) {
          // Try to get tables and their columns directly from the SchemaPlus
          try {
            // Only process actual data schemas, not metadata schemas
            if (!"information_schema".equals(schemaName)
                && !"pg_catalog".equals(schemaName)
                && !"metadata".equals(schemaName)) {
              for (String tableName : subSchema.tables().getNames(LikePattern.any())) {
                Table table = subSchema.tables().get(tableName);
                if (table != null) {
                  RelDataType rowType = table.getRowType(typeFactory);

                  for (int i = 0; i < rowType.getFieldCount(); i++) {
                    RelDataTypeField field = rowType.getFieldList().get(i);
                    RelDataType fieldType = field.getType();

                    rows.add(new Object[] {
                        catalogName,                              // TABLE_CATALOG
                        schemaName,                               // TABLE_SCHEMA
                        tableName,                                // TABLE_NAME
                        field.getName(),                          // COLUMN_NAME
                        i + 1,                                    // ORDINAL_POSITION
                        null,                                     // COLUMN_DEFAULT
                        fieldType.isNullable() ? "YES" : "NO",    // IS_NULLABLE
                        fieldType.getSqlTypeName().getName(),     // DATA_TYPE
                        getCharacterMaxLength(fieldType),         // CHARACTER_MAXIMUM_LENGTH
                        null,                                     // CHARACTER_OCTET_LENGTH
                        getNumericPrecision(fieldType),           // NUMERIC_PRECISION
                        getNumericRadix(fieldType),               // NUMERIC_PRECISION_RADIX
                        getNumericScale(fieldType),               // NUMERIC_SCALE
                        getDatetimePrecision(fieldType),          // DATETIME_PRECISION
                        null,                                     // CHARACTER_SET_CATALOG
                        null,                                     // CHARACTER_SET_SCHEMA
                        "UTF8",                                   // CHARACTER_SET_NAME
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
          } catch (Exception e) {
            // Ignore errors
          }

          // Recursively scan sub-schemas
          scanSchemaRecursively(subSchema, rows, typeFactory);
        }
      }
    }

    private Integer getCharacterMaxLength(RelDataType type) {
      if (type.getSqlTypeName() == SqlTypeName.VARCHAR
          || type.getSqlTypeName() == SqlTypeName.CHAR) {
        return type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED
            ? null : type.getPrecision();
      }
      return null;
    }

    private Integer getNumericPrecision(RelDataType type) {
      if (SqlTypeName.NUMERIC_TYPES.contains(type.getSqlTypeName())) {
        return type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED
            ? null : type.getPrecision();
      }
      return null;
    }

    private Integer getNumericRadix(RelDataType type) {
      if (SqlTypeName.NUMERIC_TYPES.contains(type.getSqlTypeName())) {
        return 10; // Base 10 for decimal types
      }
      return null;
    }

    private Integer getNumericScale(RelDataType type) {
      if (SqlTypeName.NUMERIC_TYPES.contains(type.getSqlTypeName())) {
        return type.getScale() == RelDataType.SCALE_NOT_SPECIFIED
            ? null : type.getScale();
      }
      return null;
    }

    private Integer getDatetimePrecision(RelDataType type) {
      if (SqlTypeName.DATETIME_TYPES.contains(type.getSqlTypeName())) {
        return type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED
            ? null : type.getPrecision();
      }
      return null;
    }
  }

  /**
   * information_schema.TABLE_CONSTRAINTS table (empty for Splunk).
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
      // Splunk doesn't have constraints
      return Linq4j.asEnumerable(new ArrayList<Object[]>());
    }
  }

  /**
   * information_schema.KEY_COLUMN_USAGE table (empty for Splunk).
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
      // Splunk doesn't have key constraints
      return Linq4j.asEnumerable(new ArrayList<Object[]>());
    }
  }

  /**
   * information_schema.VIEWS table (empty for Splunk).
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
      // Splunk doesn't support views
      return Linq4j.asEnumerable(new ArrayList<Object[]>());
    }
  }

  /**
   * information_schema.ROUTINES table (empty for Splunk).
   */
  private class RoutinesTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("SPECIFIC_CATALOG", SqlTypeName.VARCHAR)
          .add("SPECIFIC_SCHEMA", SqlTypeName.VARCHAR)
          .add("SPECIFIC_NAME", SqlTypeName.VARCHAR)
          .add("ROUTINE_CATALOG", SqlTypeName.VARCHAR)
          .add("ROUTINE_SCHEMA", SqlTypeName.VARCHAR)
          .add("ROUTINE_NAME", SqlTypeName.VARCHAR)
          .add("ROUTINE_TYPE", SqlTypeName.VARCHAR)
          .add("DATA_TYPE", SqlTypeName.VARCHAR)
          .add("ROUTINE_DEFINITION", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      // Splunk doesn't support stored procedures
      return Linq4j.asEnumerable(new ArrayList<Object[]>());
    }
  }

  /**
   * information_schema.PARAMETERS table (empty for Splunk).
   */
  private class ParametersTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("SPECIFIC_CATALOG", SqlTypeName.VARCHAR)
          .add("SPECIFIC_SCHEMA", SqlTypeName.VARCHAR)
          .add("SPECIFIC_NAME", SqlTypeName.VARCHAR)
          .add("ORDINAL_POSITION", SqlTypeName.INTEGER)
          .add("PARAMETER_MODE", SqlTypeName.VARCHAR)
          .add("IS_RESULT", SqlTypeName.VARCHAR)
          .add("AS_LOCATOR", SqlTypeName.VARCHAR)
          .add("PARAMETER_NAME", SqlTypeName.VARCHAR)
          .add("DATA_TYPE", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      // Splunk doesn't support stored procedures
      return Linq4j.asEnumerable(new ArrayList<Object[]>());
    }
  }

  /**
   * Wrapper that makes table columns case-insensitive.
   * This allows queries to use both uppercase and lowercase column names.
   */
  private static class CaseInsensitiveTableWrapper extends AbstractTable implements ScannableTable {
    private final ScannableTable delegate;

    CaseInsensitiveTableWrapper(ScannableTable delegate) {
      this.delegate = delegate;
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      // Create a case-insensitive row type
      RelDataType originalType = delegate.getRowType(typeFactory);
      RelDataTypeFactory.Builder builder = typeFactory.builder();

      for (RelDataTypeField field : originalType.getFieldList()) {
        // Add field with uppercase name (standard for information_schema)
        builder.add(field.getName().toUpperCase(Locale.ROOT), field.getType());
      }

      return builder.build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      return delegate.scan(root);
    }

    @Override public Schema.TableType getJdbcTableType() {
      return Schema.TableType.TABLE;
    }
  }
}
