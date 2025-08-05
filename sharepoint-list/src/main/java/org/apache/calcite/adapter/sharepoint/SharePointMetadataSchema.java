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
package org.apache.calcite.adapter.sharepoint;

import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Schema that provides PostgreSQL-compatible metadata tables for SharePoint.
 * Implements pg_tables, information_schema.tables, information_schema.columns, etc.
 */
public class SharePointMetadataSchema extends AbstractSchema {

  private final SharePointListSchema sourceSchema;
  private final String catalogName;
  private final String schemaName;

  public SharePointMetadataSchema(SharePointListSchema sourceSchema, String catalogName, String schemaName) {
    this.sourceSchema = sourceSchema;
    this.catalogName = catalogName;
    this.schemaName = schemaName;
  }

  @Override protected Map<String, Table> getTableMap() {
    return ImmutableMap.<String, Table>builder()
        // PostgreSQL pg_catalog tables
        .put("pg_tables", new PgTablesTable())
        .put("pg_namespace", new PgNamespaceTable())
        .put("pg_class", new PgClassTable())
        .put("pg_attribute", new PgAttributeTable())
        // SQL standard information_schema tables (UPPERCASE as per SQL standard)
        .put("TABLES", new InformationSchemaTablesTable())
        .put("COLUMNS", new InformationSchemaColumnsTable())
        .put("SCHEMATA", new InformationSchemaSchemataTable())
        .put("VIEWS", new InformationSchemaViewsTable())
        .put("TABLE_CONSTRAINTS", new InformationSchemaTableConstraintsTable())
        .put("KEY_COLUMN_USAGE", new InformationSchemaKeyColumnUsageTable())
        // SharePoint-specific tables
        .put("sharepoint_lists", new SharePointListsTable())
        .build();
  }

  /**
   * PostgreSQL pg_tables system catalog equivalent.
   */
  private class PgTablesTable extends AbstractTable implements ScannableTable {
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
      Map<String, Table> tables = sourceSchema.getTableMapForMetadata();

      for (String tableName : tables.keySet()) {
        rows.add(new Object[] {
            schemaName,           // schemaname
            tableName,            // tablename
            "sharepoint_admin",   // tableowner
            null,                 // tablespace
            false,                // hasindexes
            false,                // hasrules
            false                 // hastriggers
        });
      }

      return Linq4j.asEnumerable(rows);
    }
  }

  /**
   * SQL standard information_schema.tables view.
   */
  private class InformationSchemaTablesTable extends AbstractTable implements ScannableTable {
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
      Map<String, Table> tables = sourceSchema.getTableMapForMetadata();

      for (String tableName : tables.keySet()) {
        rows.add(new Object[] {
            catalogName,          // table_catalog
            schemaName,           // table_schema
            tableName,            // table_name
            "BASE TABLE",         // table_type
            "YES",                // is_insertable_into
            "NO",                 // is_typed
            null                  // commit_action
        });
      }

      return Linq4j.asEnumerable(rows);
    }
  }

  /**
   * SQL standard information_schema.columns view.
   */
  private class InformationSchemaColumnsTable extends AbstractTable implements ScannableTable {
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
          .add("IS_IDENTITY", SqlTypeName.VARCHAR)
          .add("IS_GENERATED", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();
      Map<String, Table> tables = sourceSchema.getTableMapForMetadata();
      RelDataTypeFactory typeFactory =
          root != null ? root.getTypeFactory()
              : new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
              org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

      for (Map.Entry<String, Table> entry : tables.entrySet()) {
        String tableName = entry.getKey();
        Table table = entry.getValue();

        RelDataType rowType = table.getRowType(typeFactory);

        for (int i = 0; i < rowType.getFieldCount(); i++) {
          RelDataType fieldType = rowType.getFieldList().get(i).getType();
          String fieldName = rowType.getFieldList().get(i).getName();

          rows.add(new Object[] {
              catalogName,                              // table_catalog
              schemaName,                               // table_schema
              tableName,                                // table_name
              fieldName,                                // column_name
              i + 1,                                    // ordinal_position
              null,                                     // column_default
              fieldType.isNullable() ? "YES" : "NO",    // is_nullable
              fieldType.getSqlTypeName().getName(),     // data_type
              getCharacterMaxLength(fieldType),         // character_maximum_length
              getNumericPrecision(fieldType),           // numeric_precision
              getNumericScale(fieldType),               // numeric_scale
              "NO",                                     // is_identity
              "NEVER"                                   // is_generated
          });
        }
      }

      return Linq4j.asEnumerable(rows);
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
  }

  /**
   * SQL standard information_schema.schemata view.
   */
  private class InformationSchemaSchemataTable extends AbstractTable implements ScannableTable {
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

      rows.add(new Object[] {
          catalogName,              // catalog_name
          schemaName,               // schema_name
          "sharepoint_admin",       // schema_owner
          catalogName,              // default_character_set_catalog
          "information_schema",     // default_character_set_schema
          "UTF8"                    // default_character_set_name
      });

      return Linq4j.asEnumerable(rows);
    }
  }

  /**
   * SharePoint-specific metadata table for lists.
   */
  private class SharePointListsTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("list_id", SqlTypeName.VARCHAR)
          .add("list_name", SqlTypeName.VARCHAR)
          .add("display_name", SqlTypeName.VARCHAR)
          .add("entity_type_name", SqlTypeName.VARCHAR)
          .add("template_type", SqlTypeName.VARCHAR)
          .add("base_type", SqlTypeName.VARCHAR)
          .add("created_date", SqlTypeName.TIMESTAMP)
          .add("modified_date", SqlTypeName.TIMESTAMP)
          .add("item_count", SqlTypeName.INTEGER)
          .add("site_url", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();
      Map<String, Table> tables = sourceSchema.getTableMapForMetadata();

      for (Map.Entry<String, Table> entry : tables.entrySet()) {
        String tableName = entry.getKey();
        Table table = entry.getValue();

        if (table instanceof SharePointListTable) {
          SharePointListTable spTable = (SharePointListTable) table;
          SharePointListMetadata metadata = spTable.getMetadata();

          rows.add(new Object[] {
              metadata.getListId(),                  // list_id
              metadata.getListName(),                // list_name
              metadata.getDisplayName(),             // display_name
              metadata.getEntityTypeName(),          // entity_type_name
              inferTemplateType(metadata),           // template_type
              inferBaseType(metadata),               // base_type
              null,                                  // created_date (would need API call)
              null,                                  // modified_date (would need API call)
              null,                                  // item_count (would need API call)
              extractSiteUrl()                       // site_url
          });
        }
      }

      return Linq4j.asEnumerable(rows);
    }

    private String inferTemplateType(SharePointListMetadata metadata) {
      // Infer template type based on list characteristics
      String displayName = metadata.getDisplayName().toLowerCase(Locale.ROOT);
      if (displayName.contains("task")) {
        return "TasksList";
      } else if (displayName.contains("document") || displayName.contains("library")) {
        return "DocumentLibrary";
      } else if (displayName.contains("calendar")) {
        return "Events";
      } else if (displayName.contains("contact")) {
        return "Contacts";
      } else {
        return "GenericList";
      }
    }

    private String inferBaseType(SharePointListMetadata metadata) {
      String templateType = inferTemplateType(metadata);
      return templateType.equals("DocumentLibrary") ? "DocumentLibrary" : "GenericList";
    }

    private String extractSiteUrl() {
      // This would ideally come from the client, but for now return a placeholder
      return "https://sharepoint.com/sites/default";
    }
  }

  /**
   * PostgreSQL pg_namespace system catalog.
   * Lists all schemas (namespaces).
   */
  private class PgNamespaceTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("oid", SqlTypeName.INTEGER)
          .add("nspname", SqlTypeName.VARCHAR)
          .add("nspowner", SqlTypeName.INTEGER)
          .add("nspacl", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();

      // Add standard PostgreSQL schemas
      rows.add(new Object[] {11, "pg_catalog", 10, null});
      rows.add(new Object[] {99, "information_schema", 10, null});
      rows.add(new Object[] {2200, "public", 10, null});

      // Add our SharePoint schema
      rows.add(new Object[] {16384, schemaName, 10, null});

      return Linq4j.asEnumerable(rows);
    }
  }

  /**
   * PostgreSQL pg_class system catalog.
   * Lists all tables, indexes, sequences, views, etc.
   */
  private class PgClassTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("oid", SqlTypeName.INTEGER)
          .add("relname", SqlTypeName.VARCHAR)
          .add("relnamespace", SqlTypeName.INTEGER)
          .add("reltype", SqlTypeName.INTEGER)
          .add("relowner", SqlTypeName.INTEGER)
          .add("relam", SqlTypeName.INTEGER)
          .add("relfilenode", SqlTypeName.INTEGER)
          .add("reltablespace", SqlTypeName.INTEGER)
          .add("relpages", SqlTypeName.INTEGER)
          .add("reltuples", SqlTypeName.REAL)
          .add("relallvisible", SqlTypeName.INTEGER)
          .add("reltoastrelid", SqlTypeName.INTEGER)
          .add("relhasindex", SqlTypeName.BOOLEAN)
          .add("relisshared", SqlTypeName.BOOLEAN)
          .add("relpersistence", SqlTypeName.CHAR)
          .add("relkind", SqlTypeName.CHAR)
          .add("relnatts", SqlTypeName.SMALLINT)
          .add("relchecks", SqlTypeName.SMALLINT)
          .add("relhasrules", SqlTypeName.BOOLEAN)
          .add("relhastriggers", SqlTypeName.BOOLEAN)
          .add("relhassubclass", SqlTypeName.BOOLEAN)
          .add("relrowsecurity", SqlTypeName.BOOLEAN)
          .add("relforcerowsecurity", SqlTypeName.BOOLEAN)
          .add("relispopulated", SqlTypeName.BOOLEAN)
          .add("relreplident", SqlTypeName.CHAR)
          .add("relispartition", SqlTypeName.BOOLEAN)
          .add("relfrozenxid", SqlTypeName.INTEGER)
          .add("relminmxid", SqlTypeName.INTEGER)
          .add("relacl", SqlTypeName.VARCHAR)
          .add("reloptions", SqlTypeName.VARCHAR)
          .add("relpartbound", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();
      Map<String, Table> tables = sourceSchema.getTableMapForMetadata();

      int oid = 16385; // Start from a high OID for user tables
      for (Map.Entry<String, Table> entry : tables.entrySet()) {
        String tableName = entry.getKey();
        Table table = entry.getValue();
        RelDataType rowType = table.getRowType(new JavaTypeFactoryImpl());

        rows.add(new Object[] {
            oid++,              // oid
            tableName,          // relname
            16384,              // relnamespace (our schema OID)
            0,                  // reltype
            10,                 // relowner
            0,                  // relam
            0,                  // relfilenode
            0,                  // reltablespace
            0,                  // relpages
            0.0f,               // reltuples
            0,                  // relallvisible
            0,                  // reltoastrelid
            false,              // relhasindex
            false,              // relisshared
            'p',                // relpersistence (permanent)
            'r',                // relkind (regular table)
            (short) rowType.getFieldCount(), // relnatts
            (short) 0,          // relchecks
            false,              // relhasrules
            false,              // relhastriggers
            false,              // relhassubclass
            false,              // relrowsecurity
            false,              // relforcerowsecurity
            true,               // relispopulated
            'd',                // relreplident (default)
            false,              // relispartition
            0,                  // relfrozenxid
            0,                  // relminmxid
            null,               // relacl
            null,               // reloptions
            null                // relpartbound
        });
      }

      return Linq4j.asEnumerable(rows);
    }
  }

  /**
   * PostgreSQL pg_attribute system catalog.
   * Lists all table columns.
   */
  private class PgAttributeTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("attrelid", SqlTypeName.INTEGER)
          .add("attname", SqlTypeName.VARCHAR)
          .add("atttypid", SqlTypeName.INTEGER)
          .add("attstattarget", SqlTypeName.INTEGER)
          .add("attlen", SqlTypeName.SMALLINT)
          .add("attnum", SqlTypeName.SMALLINT)
          .add("attndims", SqlTypeName.INTEGER)
          .add("attcacheoff", SqlTypeName.INTEGER)
          .add("atttypmod", SqlTypeName.INTEGER)
          .add("attbyval", SqlTypeName.BOOLEAN)
          .add("attstorage", SqlTypeName.CHAR)
          .add("attalign", SqlTypeName.CHAR)
          .add("attnotnull", SqlTypeName.BOOLEAN)
          .add("atthasdef", SqlTypeName.BOOLEAN)
          .add("atthasmissing", SqlTypeName.BOOLEAN)
          .add("attidentity", SqlTypeName.CHAR)
          .add("attgenerated", SqlTypeName.CHAR)
          .add("attisdropped", SqlTypeName.BOOLEAN)
          .add("attislocal", SqlTypeName.BOOLEAN)
          .add("attinhcount", SqlTypeName.INTEGER)
          .add("attcollation", SqlTypeName.INTEGER)
          .add("attacl", SqlTypeName.VARCHAR)
          .add("attoptions", SqlTypeName.VARCHAR)
          .add("attfdwoptions", SqlTypeName.VARCHAR)
          .add("attmissingval", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();
      Map<String, Table> tables = sourceSchema.getTableMapForMetadata();

      int relationOid = 16385; // Match OIDs from pg_class
      for (Map.Entry<String, Table> entry : tables.entrySet()) {
        Table table = entry.getValue();
        RelDataType rowType = table.getRowType(new JavaTypeFactoryImpl());

        short attnum = 1;
        for (RelDataTypeField field : rowType.getFieldList()) {
          rows.add(new Object[] {
              relationOid,        // attrelid
              field.getName(),    // attname
              getPostgresTypeOid(field.getType()), // atttypid
              -1,                 // attstattarget
              getTypeLength(field.getType()), // attlen
              attnum++,           // attnum
              0,                  // attndims
              -1,                 // attcacheoff
              -1,                 // atttypmod
              isPassByValue(field.getType()), // attbyval
              'p',                // attstorage (plain)
              'i',                // attalign (int)
              !field.getType().isNullable(), // attnotnull
              false,              // atthasdef
              false,              // atthasmissing
              ' ',                // attidentity
              ' ',                // attgenerated
              false,              // attisdropped
              true,               // attislocal
              0,                  // attinhcount
              0,                  // attcollation
              null,               // attacl
              null,               // attoptions
              null,               // attfdwoptions
              null                // attmissingval
          });
        }
        relationOid++;
      }

      return Linq4j.asEnumerable(rows);
    }

    private int getPostgresTypeOid(RelDataType type) {
      switch (type.getSqlTypeName()) {
        case INTEGER: return 23;
        case BIGINT: return 20;
        case VARCHAR: return 1043;
        case CHAR: return 1042;
        case TIMESTAMP: return 1114;
        case BOOLEAN: return 16;
        case DOUBLE: return 701;
        case REAL: return 700;
        case SMALLINT: return 21;
        default: return 25; // text
      }
    }

    private short getTypeLength(RelDataType type) {
      switch (type.getSqlTypeName()) {
        case INTEGER: return 4;
        case BIGINT: return 8;
        case SMALLINT: return 2;
        case BOOLEAN: return 1;
        case DOUBLE: return 8;
        case REAL: return 4;
        default: return -1; // variable length
      }
    }

    private boolean isPassByValue(RelDataType type) {
      switch (type.getSqlTypeName()) {
        case INTEGER:
        case SMALLINT:
        case BOOLEAN:
        case REAL:
          return true;
        default:
          return false;
      }
    }
  }

  /**
   * SQL standard information_schema.views.
   */
  private class InformationSchemaViewsTable extends AbstractTable implements ScannableTable {
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
      List<Object[]> rows = new ArrayList<>();
      // SharePoint adapter doesn't currently support views, so return empty
      // In the future, this could include SharePoint views as views
      return Linq4j.asEnumerable(rows);
    }
  }

  /**
   * SQL standard information_schema.table_constraints.
   */
  private class InformationSchemaTableConstraintsTable extends AbstractTable implements ScannableTable {
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
      List<Object[]> rows = new ArrayList<>();
      // SharePoint adapter doesn't have constraints, so return empty
      // Could be extended to include implied constraints from required fields
      return Linq4j.asEnumerable(rows);
    }
  }

  /**
   * SQL standard information_schema.key_column_usage.
   */
  private class InformationSchemaKeyColumnUsageTable extends AbstractTable implements ScannableTable {
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
      List<Object[]> rows = new ArrayList<>();
      // SharePoint adapter doesn't have key constraints, so return empty
      // Could be extended to include ID column as natural key
      return Linq4j.asEnumerable(rows);
    }
  }
}
