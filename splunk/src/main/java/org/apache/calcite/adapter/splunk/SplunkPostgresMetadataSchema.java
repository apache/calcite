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
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * PostgreSQL-compatible metadata schema (pg_catalog) for Splunk adapter.
 * Provides PostgreSQL system catalog tables for tool compatibility.
 */
public class SplunkPostgresMetadataSchema extends AbstractSchema {

  private final SchemaPlus rootSchema;
  private final String catalogName;
  
  // Shared OID mappings for referential integrity
  private final Map<String, Integer> namespaceOids = new HashMap<>();
  private final Map<String, Map<String, Integer>> tableOids = new HashMap<>();
  private boolean oidsInitialized = false;

  public SplunkPostgresMetadataSchema(SchemaPlus rootSchema, String catalogName) {
    this.rootSchema = rootSchema;
    this.catalogName = catalogName;
    initializeOidMappings();
  }
  
  /**
   * Initialize OID mappings for all schemas and tables to ensure
   * referential integrity across pg_catalog tables.
   */
  private synchronized void initializeOidMappings() {
    if (oidsInitialized) {
      return;
    }
    
    // Standard PostgreSQL schemas
    namespaceOids.put("pg_catalog", 11);
    namespaceOids.put("information_schema", 99);
    namespaceOids.put("public", 2200);
    
    int namespaceOid = 16384;
    int tableOid = 16385;
    
    // Iterate through all schemas and tables once to assign consistent OIDs
    for (String schemaName : rootSchema.subSchemas().getNames(LikePattern.any())) {
      if (!"pg_catalog".equals(schemaName) && 
          !"information_schema".equals(schemaName) &&
          !"metadata".equals(schemaName)) {
        
        // Assign namespace OID
        namespaceOids.put(schemaName, namespaceOid++);
        
        // Get tables in this schema
        SchemaPlus subSchema = rootSchema.subSchemas().get(schemaName);
        if (subSchema != null) {
          Map<String, Integer> schemaTableOids = new HashMap<>();
          
          try {
            for (String tableName : subSchema.tables().getNames(LikePattern.any())) {
              schemaTableOids.put(tableName, tableOid++);
            }
          } catch (Exception e) {
            // Log but continue - some schemas might not support table listing
          }
          
          tableOids.put(schemaName, schemaTableOids);
        }
      }
    }
    
    oidsInitialized = true;
  }

  @Override protected Map<String, Table> getTableMap() {
    return ImmutableMap.<String, Table>builder()
        .put("pg_namespace", new PgNamespaceTable())
        .put("pg_class", new PgClassTable())
        .put("pg_attribute", new PgAttributeTable())
        .put("pg_type", new PgTypeTable())
        .put("pg_proc", new PgProcTable())
        .put("pg_database", new PgDatabaseTable())
        .put("pg_tables", new PgTablesView())
        .put("pg_views", new PgViewsView())
        .put("pg_indexes", new PgIndexesView())
        // Splunk-specific tables
        .put("splunk_indexes", new SplunkIndexesTable())
        .put("splunk_sources", new SplunkSourcesTable())
        .build();
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

      // Add all schemas from the OID mapping
      for (Map.Entry<String, Integer> entry : namespaceOids.entrySet()) {
        rows.add(new Object[] {entry.getValue(), entry.getKey(), 10, null});
      }

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
      RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

      // Iterate through all schemas using the consistent OID mappings
      for (Map.Entry<String, Integer> nsEntry : namespaceOids.entrySet()) {
        String schemaName = nsEntry.getKey();
        Integer namespaceOid = nsEntry.getValue();
        
        // Skip system schemas for table listing
        if ("pg_catalog".equals(schemaName) || "information_schema".equals(schemaName)) {
          continue;
        }
        
        Map<String, Integer> schemaTableOids = tableOids.get(schemaName);
        if (schemaTableOids == null) {
          continue;
        }
        
        SchemaPlus subSchema = rootSchema.subSchemas().get(schemaName);
        if (subSchema != null) {
          for (Map.Entry<String, Integer> tableEntry : schemaTableOids.entrySet()) {
            String tableName = tableEntry.getKey();
            Integer tableOid = tableEntry.getValue();
            
            try {
              Table table = subSchema.tables().get(tableName);
              if (table != null) {
                RelDataType rowType = table.getRowType(typeFactory);

                rows.add(new Object[] {
                    tableOid,               // oid
                    tableName,              // relname
                    namespaceOid,           // relnamespace
                    0,                      // reltype
                    10,                     // relowner
                    0,                      // relam
                    0,                      // relfilenode
                    0,                      // reltablespace
                    0,                      // relpages
                    0.0f,                   // reltuples
                    0,                      // relallvisible
                    0,                      // reltoastrelid
                    false,                  // relhasindex
                    false,                  // relisshared
                    'p',                    // relpersistence (permanent)
                    'r',                    // relkind (regular table)
                    (short) rowType.getFieldCount(), // relnatts
                    (short) 0,              // relchecks
                    false,                  // relhasrules
                    false,                  // relhastriggers
                    false,                  // relhassubclass
                    false,                  // relrowsecurity
                    false,                  // relforcerowsecurity
                    true,                   // relispopulated
                    'd',                    // relreplident (default)
                    false,                  // relispartition
                    0,                      // relfrozenxid
                    0,                      // relminmxid
                    null,                   // relacl
                    null,                   // reloptions
                    null                    // relpartbound
                });
              }
            } catch (Exception e) {
              // Ignore individual table errors
            }
          }
        }
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
      RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

      // Iterate through all schemas using the consistent OID mappings
      for (Map.Entry<String, Integer> nsEntry : namespaceOids.entrySet()) {
        String schemaName = nsEntry.getKey();
        
        // Skip system schemas for table listing
        if ("pg_catalog".equals(schemaName) || "information_schema".equals(schemaName)) {
          continue;
        }
        
        Map<String, Integer> schemaTableOids = tableOids.get(schemaName);
        if (schemaTableOids == null) {
          continue;
        }
        
        SchemaPlus subSchema = rootSchema.subSchemas().get(schemaName);
        if (subSchema != null) {
          for (Map.Entry<String, Integer> tableEntry : schemaTableOids.entrySet()) {
            String tableName = tableEntry.getKey();
            Integer tableOid = tableEntry.getValue();
            
            try {
              Table table = subSchema.tables().get(tableName);
              if (table != null) {
                RelDataType rowType = table.getRowType(typeFactory);

                short attnum = 1;
                for (RelDataTypeField field : rowType.getFieldList()) {
                  rows.add(new Object[] {
                      tableOid,               // attrelid (matches pg_class.oid)
                      field.getName(),        // attname
                      getPostgresTypeOid(field.getType()), // atttypid
                      -1,                     // attstattarget
                      getTypeLength(field.getType()), // attlen
                      attnum++,               // attnum
                      0,                      // attndims
                      -1,                     // attcacheoff
                      -1,                     // atttypmod
                      isPassByValue(field.getType()), // attbyval
                      'p',                    // attstorage (plain)
                      'i',                    // attalign (int)
                      !field.getType().isNullable(), // attnotnull
                      false,                  // atthasdef
                      false,                  // atthasmissing
                      ' ',                    // attidentity
                      ' ',                    // attgenerated
                      false,                  // attisdropped
                      true,                   // attislocal
                      0,                      // attinhcount
                      0,                      // attcollation
                      null,                   // attacl
                      null,                   // attoptions
                      null,                   // attfdwoptions
                      null                    // attmissingval
                  });
                }
              }
            } catch (Exception e) {
              // Ignore individual table errors
            }
          }
        }
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
        case DECIMAL: return 1700;
        case DATE: return 1082;
        case TIME: return 1083;
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
   * PostgreSQL pg_type system catalog.
   * Lists all data types.
   */
  private class PgTypeTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("oid", SqlTypeName.INTEGER)
          .add("typname", SqlTypeName.VARCHAR)
          .add("typnamespace", SqlTypeName.INTEGER)
          .add("typowner", SqlTypeName.INTEGER)
          .add("typlen", SqlTypeName.SMALLINT)
          .add("typbyval", SqlTypeName.BOOLEAN)
          .add("typtype", SqlTypeName.CHAR)
          .add("typcategory", SqlTypeName.CHAR)
          .add("typispreferred", SqlTypeName.BOOLEAN)
          .add("typisdefined", SqlTypeName.BOOLEAN)
          .add("typdelim", SqlTypeName.CHAR)
          .add("typrelid", SqlTypeName.INTEGER)
          .add("typelem", SqlTypeName.INTEGER)
          .add("typarray", SqlTypeName.INTEGER)
          .add("typinput", SqlTypeName.VARCHAR)
          .add("typoutput", SqlTypeName.VARCHAR)
          .add("typreceive", SqlTypeName.VARCHAR)
          .add("typsend", SqlTypeName.VARCHAR)
          .add("typmodin", SqlTypeName.VARCHAR)
          .add("typmodout", SqlTypeName.VARCHAR)
          .add("typanalyze", SqlTypeName.VARCHAR)
          .add("typalign", SqlTypeName.CHAR)
          .add("typstorage", SqlTypeName.CHAR)
          .add("typnotnull", SqlTypeName.BOOLEAN)
          .add("typbasetype", SqlTypeName.INTEGER)
          .add("typtypmod", SqlTypeName.INTEGER)
          .add("typndims", SqlTypeName.INTEGER)
          .add("typcollation", SqlTypeName.INTEGER)
          .add("typdefaultbin", SqlTypeName.VARCHAR)
          .add("typdefault", SqlTypeName.VARCHAR)
          .add("typacl", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();

      // Add basic PostgreSQL types
      rows.add(createTypeRow(16, "bool", 1, 'b', true));
      rows.add(createTypeRow(20, "int8", 8, 'b', false));
      rows.add(createTypeRow(21, "int2", 2, 'b', false));
      rows.add(createTypeRow(23, "int4", 4, 'b', false));
      rows.add(createTypeRow(25, "text", -1, 'b', false));
      rows.add(createTypeRow(700, "float4", 4, 'b', false));
      rows.add(createTypeRow(701, "float8", 8, 'b', false));
      rows.add(createTypeRow(1042, "bpchar", -1, 'b', false));
      rows.add(createTypeRow(1043, "varchar", -1, 'b', false));
      rows.add(createTypeRow(1082, "date", 4, 'b', false));
      rows.add(createTypeRow(1083, "time", 8, 'b', false));
      rows.add(createTypeRow(1114, "timestamp", 8, 'b', false));
      rows.add(createTypeRow(1700, "numeric", -1, 'b', false));

      return Linq4j.asEnumerable(rows);
    }

    private Object[] createTypeRow(int oid, String name, int len, char type, boolean byval) {
      return new Object[] {
          oid,                // oid
          name,               // typname
          11,                 // typnamespace (pg_catalog)
          10,                 // typowner
          (short) len,        // typlen
          byval,              // typbyval
          type,               // typtype
          'N',                // typcategory
          false,              // typispreferred
          true,               // typisdefined
          ',',                // typdelim
          0,                  // typrelid
          0,                  // typelem
          0,                  // typarray
          name + "_in",       // typinput
          name + "_out",      // typoutput
          name + "_recv",     // typreceive
          name + "_send",     // typsend
          null,               // typmodin
          null,               // typmodout
          null,               // typanalyze
          'i',                // typalign
          'p',                // typstorage
          false,              // typnotnull
          0,                  // typbasetype
          -1,                 // typtypmod
          0,                  // typndims
          0,                  // typcollation
          null,               // typdefaultbin
          null,               // typdefault
          null                // typacl
      };
    }
  }

  /**
   * PostgreSQL pg_proc system catalog.
   * Lists all functions and procedures.
   */
  private class PgProcTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("oid", SqlTypeName.INTEGER)
          .add("proname", SqlTypeName.VARCHAR)
          .add("pronamespace", SqlTypeName.INTEGER)
          .add("proowner", SqlTypeName.INTEGER)
          .add("prolang", SqlTypeName.INTEGER)
          .add("procost", SqlTypeName.REAL)
          .add("prorows", SqlTypeName.REAL)
          .add("provariadic", SqlTypeName.INTEGER)
          .add("prosupport", SqlTypeName.VARCHAR)
          .add("prokind", SqlTypeName.CHAR)
          .add("prosecdef", SqlTypeName.BOOLEAN)
          .add("proleakproof", SqlTypeName.BOOLEAN)
          .add("proisstrict", SqlTypeName.BOOLEAN)
          .add("proretset", SqlTypeName.BOOLEAN)
          .add("provolatile", SqlTypeName.CHAR)
          .add("proparallel", SqlTypeName.CHAR)
          .add("pronargs", SqlTypeName.SMALLINT)
          .add("pronargdefaults", SqlTypeName.SMALLINT)
          .add("prorettype", SqlTypeName.INTEGER)
          .add("proargtypes", SqlTypeName.VARCHAR)
          .add("proallargtypes", SqlTypeName.VARCHAR)
          .add("proargmodes", SqlTypeName.VARCHAR)
          .add("proargnames", SqlTypeName.VARCHAR)
          .add("proargdefaults", SqlTypeName.VARCHAR)
          .add("protrftypes", SqlTypeName.VARCHAR)
          .add("prosrc", SqlTypeName.VARCHAR)
          .add("probin", SqlTypeName.VARCHAR)
          .add("prosqlbody", SqlTypeName.VARCHAR)
          .add("proconfig", SqlTypeName.VARCHAR)
          .add("proacl", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();
      
      // Add essential pg_catalog functions
      int oid = 1299;
      
      // Information functions
      rows.add(createFunctionRow(oid++, "current_database", 11, 0, 25)); // returns text
      rows.add(createFunctionRow(oid++, "current_schema", 11, 0, 25)); // returns text
      rows.add(createFunctionRow(oid++, "current_schemas", 11, 1, 1009)); // returns text[]
      rows.add(createFunctionRow(oid++, "version", 11, 0, 25)); // returns text
      
      // Object visibility functions
      rows.add(createFunctionRow(oid++, "pg_table_is_visible", 11, 1, 16)); // returns boolean
      rows.add(createFunctionRow(oid++, "pg_type_is_visible", 11, 1, 16)); // returns boolean
      rows.add(createFunctionRow(oid++, "pg_function_is_visible", 11, 1, 16)); // returns boolean
      
      // Privilege functions
      rows.add(createFunctionRow(oid++, "has_table_privilege", 11, 2, 16)); // returns boolean
      rows.add(createFunctionRow(oid++, "has_schema_privilege", 11, 2, 16)); // returns boolean
      
      // Formatting functions
      rows.add(createFunctionRow(oid++, "quote_ident", 11, 1, 25)); // returns text
      rows.add(createFunctionRow(oid++, "format_type", 11, 2, 25)); // returns text
      rows.add(createFunctionRow(oid++, "pg_get_expr", 11, 2, 25)); // returns text
      
      // Type function
      rows.add(createFunctionRow(oid++, "pg_typeof", 11, 1, 25)); // returns text
      
      return Linq4j.asEnumerable(rows);
    }
    
    private Object[] createFunctionRow(int oid, String name, int namespace, 
                                        int argCount, int returnType) {
      return new Object[] {
          oid,                // oid
          name,               // proname
          namespace,          // pronamespace (11 = pg_catalog)
          10,                 // proowner
          12,                 // prolang (12 = internal)
          1.0f,               // procost
          0.0f,               // prorows
          0,                  // provariadic
          null,               // prosupport
          'f',                // prokind (f = function)
          false,              // prosecdef
          false,              // proleakproof
          true,               // proisstrict
          false,              // proretset
          's',                // provolatile (s = stable)
          's',                // proparallel (s = safe)
          (short) argCount,   // pronargs
          (short) 0,          // pronargdefaults
          returnType,         // prorettype
          "",                 // proargtypes (simplified)
          null,               // proallargtypes
          null,               // proargmodes
          null,               // proargnames
          null,               // proargdefaults
          null,               // protrftypes
          "internal",         // prosrc
          null,               // probin
          null,               // prosqlbody
          null,               // proconfig
          null                // proacl
      };
    }
  }

  /**
   * PostgreSQL pg_database system catalog.
   * Lists all databases (just returns the current catalog).
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
          .add("datistemplate", SqlTypeName.BOOLEAN)
          .add("datallowconn", SqlTypeName.BOOLEAN)
          .add("datconnlimit", SqlTypeName.INTEGER)
          .add("datlastsysoid", SqlTypeName.INTEGER)
          .add("datfrozenxid", SqlTypeName.INTEGER)
          .add("datminmxid", SqlTypeName.INTEGER)
          .add("dattablespace", SqlTypeName.INTEGER)
          .add("datacl", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();

      rows.add(new Object[] {
          1,                  // oid
          catalogName,        // datname
          10,                 // datdba
          6,                  // encoding (UTF8)
          "en_US.UTF-8",      // datcollate
          "en_US.UTF-8",      // datctype
          false,              // datistemplate
          true,               // datallowconn
          -1,                 // datconnlimit
          12000,              // datlastsysoid
          0,                  // datfrozenxid
          0,                  // datminmxid
          1663,               // dattablespace
          null                // datacl
      });

      return Linq4j.asEnumerable(rows);
    }
  }

  /**
   * PostgreSQL pg_tables view.
   * Simplified view of all tables.
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
          .add("rowsecurity", SqlTypeName.BOOLEAN)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();

      // Use the shared OID mappings to ensure consistency
      for (Map.Entry<String, Map<String, Integer>> schemaEntry : tableOids.entrySet()) {
        String schemaName = schemaEntry.getKey();
        Map<String, Integer> tables = schemaEntry.getValue();
        
        for (String tableName : tables.keySet()) {
          rows.add(new Object[] {
              schemaName,         // schemaname
              tableName,          // tablename
              "splunk_admin",     // tableowner
              null,               // tablespace
              false,              // hasindexes
              false,              // hasrules
              false,              // hastriggers
              false               // rowsecurity
          });
        }
      }

      return Linq4j.asEnumerable(rows);
    }
  }

  /**
   * PostgreSQL pg_views view (empty for Splunk).
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
      // Splunk doesn't support views
      return Linq4j.asEnumerable(new ArrayList<Object[]>());
    }
  }

  /**
   * PostgreSQL pg_indexes view (empty for Splunk).
   */
  private class PgIndexesView extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("schemaname", SqlTypeName.VARCHAR)
          .add("tablename", SqlTypeName.VARCHAR)
          .add("indexname", SqlTypeName.VARCHAR)
          .add("tablespace", SqlTypeName.VARCHAR)
          .add("indexdef", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      // Splunk doesn't support indexes
      return Linq4j.asEnumerable(new ArrayList<Object[]>());
    }
  }

  /**
   * Splunk-specific table: splunk_indexes.
   * Lists available Splunk indexes (placeholder - would require API access).
   */
  private class SplunkIndexesTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("index_name", SqlTypeName.VARCHAR)
          .add("earliest_time", SqlTypeName.TIMESTAMP)
          .add("latest_time", SqlTypeName.TIMESTAMP)
          .add("total_event_count", SqlTypeName.BIGINT)
          .add("total_size_mb", SqlTypeName.DOUBLE)
          .add("is_internal", SqlTypeName.BOOLEAN)
          .add("is_realtime", SqlTypeName.BOOLEAN)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      // This is a placeholder - actual implementation would query Splunk API
      // For now, return common Splunk indexes
      List<Object[]> rows = new ArrayList<>();

      rows.add(new Object[] {
          "main",                  // index_name
          null,                    // earliest_time
          null,                    // latest_time
          0L,                      // total_event_count
          0.0,                     // total_size_mb
          false,                   // is_internal
          false                    // is_realtime
      });

      rows.add(new Object[] {
          "_internal",             // index_name
          null,                    // earliest_time
          null,                    // latest_time
          0L,                      // total_event_count
          0.0,                     // total_size_mb
          true,                    // is_internal
          false                    // is_realtime
      });

      rows.add(new Object[] {
          "_audit",                // index_name
          null,                    // earliest_time
          null,                    // latest_time
          0L,                      // total_event_count
          0.0,                     // total_size_mb
          true,                    // is_internal
          false                    // is_realtime
      });

      return Linq4j.asEnumerable(rows);
    }
  }

  /**
   * Splunk-specific table: splunk_sources.
   * Lists data sources available in Splunk (placeholder).
   */
  private class SplunkSourcesTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("source", SqlTypeName.VARCHAR)
          .add("sourcetype", SqlTypeName.VARCHAR)
          .add("host", SqlTypeName.VARCHAR)
          .add("index", SqlTypeName.VARCHAR)
          .add("event_count", SqlTypeName.BIGINT)
          .add("earliest_time", SqlTypeName.TIMESTAMP)
          .add("latest_time", SqlTypeName.TIMESTAMP)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      // This is a placeholder - actual implementation would query Splunk API
      // Return empty for now as this would require runtime Splunk connection
      return Linq4j.asEnumerable(new ArrayList<Object[]>());
    }
  }
}
