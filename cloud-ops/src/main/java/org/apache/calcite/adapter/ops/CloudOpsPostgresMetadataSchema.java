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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Schema that provides PostgreSQL-compatible system catalog tables for Cloud Governance.
 * Following the established File/Splunk adapter pattern.
 */
public class CloudOpsPostgresMetadataSchema extends AbstractSchema {

  private final SchemaPlus parentSchema;
  private final String catalogName;

  public CloudOpsPostgresMetadataSchema(SchemaPlus parentSchema, String catalogName) {
    this.parentSchema = parentSchema;
    this.catalogName = catalogName;
  }

  @Override protected Map<String, Table> getTableMap() {
    // Create a case-insensitive map for PostgreSQL catalog tables
    // Table names should be UPPERCASE within pg_catalog schema
    Map<String, Table> caseInsensitiveMap = new CaseInsensitiveTableMap();
    caseInsensitiveMap.put("PG_NAMESPACE", new PgNamespaceTable());
    caseInsensitiveMap.put("PG_CLASS", new PgClassTable());
    caseInsensitiveMap.put("PG_ATTRIBUTE", new PgAttributeTable());
    caseInsensitiveMap.put("PG_TABLES", new PgTablesTable());
    caseInsensitiveMap.put("PG_TYPE", new PgTypeTable());
    caseInsensitiveMap.put("PG_DATABASE", new PgDatabaseTable());
    caseInsensitiveMap.put("PG_VIEWS", new PgViewsTable());
    // Cloud Governance-specific tables (also UPPERCASE)
    caseInsensitiveMap.put("CLOUD_RESOURCES", new CloudResourcesTable());
    caseInsensitiveMap.put("CLOUD_PROVIDERS", new CloudProvidersTable());
    caseInsensitiveMap.put("OPS_POLICIES", new GovernancePoliciesTable());
    return caseInsensitiveMap;
  }

  /**
   * PostgreSQL pg_namespace system catalog.
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

      // Add all other schemas found - navigate to root to see all sibling schemas
      SchemaPlus rootSchema = parentSchema;
      while (rootSchema.getParentSchema() != null) {
        rootSchema = rootSchema.getParentSchema();
      }

      int oid = 16384;
      for (String schemaName : rootSchema.subSchemas().getNames(LikePattern.any())) {
        if (!"information_schema".equals(schemaName) && !"pg_catalog".equals(schemaName)) {
          rows.add(new Object[] {oid++, schemaName, 10, null});
        }
      }

      return Linq4j.asEnumerable(rows);
    }
  }

  /**
   * PostgreSQL pg_tables system catalog.
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

      // Need to check all schemas recursively
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
                  schemaName,               // schemaname
                  tableName,                // tablename
                  "cloud_ops_admin", // tableowner
                  null,                     // tablespace
                  false,                    // hasindexes
                  false,                    // hasrules
                  false                     // hastriggers
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
   * PostgreSQL pg_class system catalog.
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

      int oid = 16385; // Start from a high OID for user tables
      scanSchemaRecursively(null, rows, oid);

      return Linq4j.asEnumerable(rows);
    }

    private void scanSchemaRecursively(SchemaPlus schema, List<Object[]> rows, int startOid) {
      // Navigate to root schema to see all schemas
      SchemaPlus rootSchema = parentSchema;
      while (rootSchema.getParentSchema() != null) {
        rootSchema = rootSchema.getParentSchema();
      }

      int oid = startOid;
      for (String schemaName : rootSchema.subSchemas().getNames(LikePattern.any())) {
        SchemaPlus subSchema = rootSchema.subSchemas().get(schemaName);
        if (subSchema != null && !"information_schema".equals(schemaName) && !"pg_catalog".equals(schemaName)) {
          for (String tableName : subSchema.tables().getNames(LikePattern.any())) {
            Table table = subSchema.tables().get(tableName);
            if (table != null) {
              RelDataType rowType = table.getRowType(new JavaTypeFactoryImpl());

              rows.add(new Object[] {
                  oid++,              // oid
                  tableName,          // relname
                  16384,              // relnamespace (schema OID)
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
          }
        }
      }
    }
  }

  /**
   * PostgreSQL pg_attribute system catalog.
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

      int relationOid = 16385; // Match OIDs from pg_class
      scanSchemaRecursively(null, rows, relationOid);

      return Linq4j.asEnumerable(rows);
    }

    private void scanSchemaRecursively(SchemaPlus schema, List<Object[]> rows, int startOid) {
      // Navigate to root schema to see all schemas
      SchemaPlus rootSchema = parentSchema;
      while (rootSchema.getParentSchema() != null) {
        rootSchema = rootSchema.getParentSchema();
      }

      int relationOid = startOid;
      for (String schemaName : rootSchema.subSchemas().getNames(LikePattern.any())) {
        SchemaPlus subSchema = rootSchema.subSchemas().get(schemaName);
        if (subSchema != null && !"information_schema".equals(schemaName) && !"pg_catalog".equals(schemaName)) {
          for (String tableName : subSchema.tables().getNames(LikePattern.any())) {
            Table table = subSchema.tables().get(tableName);
            if (table != null) {
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
          }
        }
      }
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
   * Cloud Governance-specific metadata table for cloud resources.
   */
  private class CloudResourcesTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("table_name", SqlTypeName.VARCHAR)
          .add("resource_type", SqlTypeName.VARCHAR)
          .add("cloud_provider", SqlTypeName.VARCHAR)
          .add("description", SqlTypeName.VARCHAR)
          .add("supported_providers", SqlTypeName.VARCHAR)
          .add("column_count", SqlTypeName.INTEGER)
          .add("has_security_fields", SqlTypeName.BOOLEAN)
          .add("has_encryption_fields", SqlTypeName.BOOLEAN)
          .add("has_compliance_fields", SqlTypeName.BOOLEAN)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();
      RelDataTypeFactory typeFactory =
          root != null ? root.getTypeFactory()
              : new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
              org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

      scanSchemaRecursively(null, rows, typeFactory);

      return Linq4j.asEnumerable(rows);
    }

    private void scanSchemaRecursively(SchemaPlus schema, List<Object[]> rows, RelDataTypeFactory typeFactory) {
      // Navigate to root schema to see all schemas
      SchemaPlus rootSchema = parentSchema;
      while (rootSchema.getParentSchema() != null) {
        rootSchema = rootSchema.getParentSchema();
      }

      for (String schemaName : rootSchema.subSchemas().getNames(LikePattern.any())) {
        SchemaPlus subSchema = rootSchema.subSchemas().get(schemaName);
        if (subSchema != null && !"information_schema".equals(schemaName) && !"pg_catalog".equals(schemaName)) {
          // Check if this schema has cloud ops tables by looking for expected table names
          for (String tableName : subSchema.tables().getNames(LikePattern.any())) {
            Table table = subSchema.tables().get(tableName);
            if (table != null) {
              RelDataType rowType = table.getRowType(typeFactory);

              // Analyze table structure to determine capabilities
              boolean hasSecurityFields = hasFields(rowType, Arrays.asList("rbac_enabled", "private_cluster", "public_access_enabled"));
              boolean hasEncryptionFields = hasFields(rowType, Arrays.asList("encryption_enabled", "encryption_type", "encryption_key_type"));
              boolean hasComplianceFields = hasFields(rowType, Arrays.asList("compliance_status", "policy_compliant", "governance_score"));

              rows.add(new Object[] {
                  tableName,                                    // table_name
                  inferResourceType(tableName),                 // resource_type
                  "multi-cloud",                                // cloud_provider
                  getResourceDescription(tableName),            // description
                  getSupportedProviders(tableName),             // supported_providers
                  rowType.getFieldCount(),                      // column_count
                  hasSecurityFields,                            // has_security_fields
                  hasEncryptionFields,                          // has_encryption_fields
                  hasComplianceFields                           // has_compliance_fields
              });
            }
          }
        }
      }
    }

    private boolean hasFields(RelDataType rowType, List<String> fieldNames) {
      for (String fieldName : fieldNames) {
        if (rowType.getField(fieldName, false, false) != null) {
          return true;
        }
      }
      return false;
    }

    private String inferResourceType(String tableName) {
      if (tableName.contains("kubernetes")) return "Container Orchestration";
      if (tableName.contains("storage")) return "Storage";
      if (tableName.contains("compute")) return "Compute";
      if (tableName.contains("network")) return "Networking";
      if (tableName.contains("iam")) return "Identity & Access Management";
      if (tableName.contains("database")) return "Database";
      if (tableName.contains("container_registries")) return "Container Registry";
      return "General";
    }

    private String getResourceDescription(String tableName) {
      switch (tableName) {
        case "kubernetes_clusters": return "Managed Kubernetes services across Azure, AWS, and GCP";
        case "storage_resources": return "Object and block storage resources";
        case "compute_resources": return "Virtual machine and compute instances";
        case "network_resources": return "Virtual networks, subnets, and security groups";
        case "iam_resources": return "Identity and access management resources";
        case "database_resources": return "Managed database services";
        case "container_registries": return "Container image repositories";
        default: return "Cloud governance resource table";
      }
    }

    private String getSupportedProviders(String tableName) {
      return "Azure, AWS, GCP"; // All tables support all providers
    }
  }

  /**
   * Cloud Governance-specific metadata table for cloud providers.
   */
  private class CloudProvidersTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("provider_name", SqlTypeName.VARCHAR)
          .add("provider_code", SqlTypeName.VARCHAR)
          .add("supported_services", SqlTypeName.VARCHAR)
          .add("authentication_methods", SqlTypeName.VARCHAR)
          .add("api_version", SqlTypeName.VARCHAR)
          .add("configuration_required", SqlTypeName.BOOLEAN)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();

      rows.add(new Object[] {
          "Microsoft Azure",                              // provider_name
          "azure",                                        // provider_code
          "AKS, Storage Accounts, VMs, Resource Graph",   // supported_services
          "Service Principal, Client Credentials",        // authentication_methods
          "2023-01-01",                                   // api_version
          true                                            // configuration_required
      });

      rows.add(new Object[] {
          "Amazon Web Services",                          // provider_name
          "aws",                                          // provider_code
          "EKS, S3, EC2, IAM, RDS, DynamoDB, ECR",       // supported_services
          "Access Key, IAM Role",                         // authentication_methods
          "2023-11-27",                                   // api_version
          true                                            // configuration_required
      });

      rows.add(new Object[] {
          "Google Cloud Platform",                        // provider_name
          "gcp",                                          // provider_code
          "GKE, Cloud Storage, Compute Engine",           // supported_services
          "Service Account Key",                          // authentication_methods
          "v1",                                           // api_version
          true                                            // configuration_required
      });

      return Linq4j.asEnumerable(rows);
    }
  }

  /**
   * Cloud Governance-specific metadata table for ops policies.
   */
  private class GovernancePoliciesTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("policy_name", SqlTypeName.VARCHAR)
          .add("policy_category", SqlTypeName.VARCHAR)
          .add("applicable_resources", SqlTypeName.VARCHAR)
          .add("policy_description", SqlTypeName.VARCHAR)
          .add("compliance_level", SqlTypeName.VARCHAR)
          .add("enforcement_level", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();

      rows.add(new Object[] {
          "Encryption at Rest",                           // policy_name
          "Security",                                     // policy_category
          "storage_resources, database_resources",        // applicable_resources
          "All data must be encrypted at rest",          // policy_description
          "High",                                         // compliance_level
          "Mandatory"                                     // enforcement_level
      });

      rows.add(new Object[] {
          "RBAC Enabled",                                 // policy_name
          "Security",                                     // policy_category
          "kubernetes_clusters",                          // applicable_resources
          "Role-based access control must be enabled",   // policy_description
          "High",                                         // compliance_level
          "Mandatory"                                     // enforcement_level
      });

      rows.add(new Object[] {
          "Private Network Access",                       // policy_name
          "Security",                                     // policy_category
          "kubernetes_clusters, database_resources",      // applicable_resources
          "Resources should not be publicly accessible", // policy_description
          "Medium",                                       // compliance_level
          "Recommended"                                   // enforcement_level
      });

      rows.add(new Object[] {
          "Resource Tagging",                             // policy_name
          "Governance",                                   // policy_category
          "all",                                          // applicable_resources
          "All resources must have proper tags",          // policy_description
          "Low",                                          // compliance_level
          "Recommended"                                   // enforcement_level
      });

      return Linq4j.asEnumerable(rows);
    }
  }

  // Empty tables for PostgreSQL compatibility
  private class PgTypeTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("oid", SqlTypeName.INTEGER)
          .add("typname", SqlTypeName.VARCHAR)
          .add("typnamespace", SqlTypeName.INTEGER)
          .add("typowner", SqlTypeName.INTEGER)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      return Linq4j.asEnumerable(new ArrayList<Object[]>());
    }
  }

  private class PgDatabaseTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("oid", SqlTypeName.INTEGER)
          .add("datname", SqlTypeName.VARCHAR)
          .add("datdba", SqlTypeName.INTEGER)
          .add("encoding", SqlTypeName.INTEGER)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      List<Object[]> rows = new ArrayList<>();
      rows.add(new Object[] {1, catalogName, 10, 6}); // UTF8 encoding
      return Linq4j.asEnumerable(rows);
    }
  }

  private class PgViewsTable extends AbstractTable implements ScannableTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("schemaname", SqlTypeName.VARCHAR)
          .add("viewname", SqlTypeName.VARCHAR)
          .add("viewowner", SqlTypeName.VARCHAR)
          .add("definition", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      // Cloud Governance adapter doesn't support views
      return Linq4j.asEnumerable(new ArrayList<Object[]>());
    }
  }

  /**
   * Case-insensitive map for table lookups.
   * Stores keys as-is but performs case-insensitive lookups.
   */
  private static class CaseInsensitiveTableMap extends AbstractMap<String, Table> {
    private final Map<String, Table> map = new LinkedHashMap<>();

    @Override public Table put(String key, Table value) {
      // Store with uppercase key for ORACLE lex compatibility
      return map.put(key.toUpperCase(Locale.ROOT), value);
    }

    @Override public Table get(Object key) {
      if (key instanceof String) {
        return map.get(((String) key).toUpperCase(Locale.ROOT));
      }
      return null;
    }

    @Override public Set<Entry<String, Table>> entrySet() {
      return map.entrySet();
    }

    @Override public boolean containsKey(Object key) {
      if (key instanceof String) {
        return map.containsKey(((String) key).toUpperCase(Locale.ROOT));
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
