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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.HashMap;
import java.util.Map;

/**
 * Builder for Splunk Common Information Model (CIM) schemas.
 * Provides complete implementations for all 24 official Splunk CIM data models.
 * Each model includes comprehensive field schemas, mappings, and search strings.
 */
public class CimModelBuilder {

  /**
   * Result of building a CIM schema, containing both the schema and field mapping.
   */
  public static class CimSchemaResult {
    private final RelDataType schema;
    private final Map<String, String> fieldMapping;
    private final String searchString;

    public CimSchemaResult(RelDataType schema, Map<String, String> fieldMapping, String searchString) {
      this.schema = schema;
      this.fieldMapping = fieldMapping;
      this.searchString = searchString;
    }

    public RelDataType getSchema() {
      return schema;
    }

    public Map<String, String> getFieldMapping() {
      return fieldMapping;
    }

    public String getSearchString() {
      return searchString;
    }
  }

  /**
   * Builds a CIM schema for the specified model type.
   */
  public static CimSchemaResult buildCimSchemaWithMapping(RelDataTypeFactory typeFactory, String cimModel) {
    switch (cimModel.toLowerCase()) {
    case "alerts":
      return buildAlertsSchemaWithMapping(typeFactory);
    case "authentication":
      return buildAuthenticationSchemaWithMapping(typeFactory);
    case "certificates":
      return buildCertificatesSchemaWithMapping(typeFactory);
    case "change":
      return buildChangeSchemaWithMapping(typeFactory);
    case "data_access":
    case "dataaccess":
      return buildDataAccessSchemaWithMapping(typeFactory);
    case "databases":
    case "database":
      return buildDatabasesSchemaWithMapping(typeFactory);
    case "data_loss_prevention":
    case "dlp":
      return buildDataLossPreventionSchemaWithMapping(typeFactory);
    case "email":
      return buildEmailSchemaWithMapping(typeFactory);
    case "endpoint":
      return buildEndpointSchemaWithMapping(typeFactory);
    case "event_signatures":
    case "eventsignatures":
      return buildEventSignaturesSchemaWithMapping(typeFactory);
    case "interprocess_messaging":
    case "messaging":
      return buildInterprocessMessagingSchemaWithMapping(typeFactory);
    case "intrusion_detection":
    case "ids":
      return buildIntrusionDetectionSchemaWithMapping(typeFactory);
    case "inventory":
      return buildInventorySchemaWithMapping(typeFactory);
    case "jvm":
      return buildJvmSchemaWithMapping(typeFactory);
    case "malware":
      return buildMalwareSchemaWithMapping(typeFactory);
    case "network_resolution":
    case "dns":
      return buildNetworkResolutionSchemaWithMapping(typeFactory);
    case "network_sessions":
    case "networksessions":
      return buildNetworkSessionsSchemaWithMapping(typeFactory);
    case "network_traffic":
    case "network":
      return buildNetworkTrafficSchemaWithMapping(typeFactory);
    case "performance":
      return buildPerformanceSchemaWithMapping(typeFactory);
    case "splunk_audit_logs":
    case "splunkaudit":
      return buildSplunkAuditLogsSchemaWithMapping(typeFactory);
    case "ticket_management":
    case "ticketing":
      return buildTicketManagementSchemaWithMapping(typeFactory);
    case "updates":
      return buildUpdatesSchemaWithMapping(typeFactory);
    case "vulnerabilities":
    case "vulnerability":
      return buildVulnerabilitiesSchemaWithMapping(typeFactory);
    case "web":
      return buildWebSchemaWithMapping(typeFactory);
    default:
      return buildBaseSchemaWithMapping(typeFactory);
    }
  }

  /**
   * Builds a CIM schema for the specified model type (backward compatibility).
   */
  public static RelDataType buildCimSchema(RelDataTypeFactory typeFactory, String cimModel) {
    return buildCimSchemaWithMapping(typeFactory, cimModel).getSchema();
  }

  /**
   * Base schema with common fields present in most CIM models.
   */
  private static CimSchemaResult buildBaseSchemaWithMapping(RelDataTypeFactory typeFactory) {
    RelDataType schema = typeFactory.builder()
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("_raw", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("_extra", typeFactory.createSqlType(SqlTypeName.ANY))
        .build();

    Map<String, String> fieldMapping = new HashMap<>();
    return new CimSchemaResult(schema, fieldMapping, "search");
  }

  /**
   * Alerts CIM model schema - External alerting system events
   */
  private static CimSchemaResult buildAlertsSchemaWithMapping(RelDataTypeFactory typeFactory) {
    RelDataType schema = typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Alerts-specific fields
        .add("action", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("alert_id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("alert_type", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("severity", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("signature", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("signature_id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("status", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("tag", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("vendor_product", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        .add("_extra", typeFactory.createSqlType(SqlTypeName.ANY))
        .build();

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("action", "Alerts.action");
    fieldMapping.put("alert_id", "Alerts.alert_id");
    fieldMapping.put("alert_type", "Alerts.alert_type");
    fieldMapping.put("dest", "Alerts.dest");
    fieldMapping.put("severity", "Alerts.severity");
    fieldMapping.put("signature", "Alerts.signature");
    fieldMapping.put("signature_id", "Alerts.signature_id");
    fieldMapping.put("src", "Alerts.src");
    fieldMapping.put("status", "Alerts.status");
    fieldMapping.put("tag", "Alerts.tag");
    fieldMapping.put("user", "Alerts.user");
    fieldMapping.put("vendor_product", "Alerts.vendor_product");

    String searchString = "| datamodel Alerts Alerts search";
    return new CimSchemaResult(schema, fieldMapping, searchString);
  }

  /**
   * Authentication CIM model schema with field mapping.
   */
  private static CimSchemaResult buildAuthenticationSchemaWithMapping(RelDataTypeFactory typeFactory) {
    RelDataType schema = typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Authentication-specific fields
        .add("action", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("app", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("authentication_method", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("authentication_service", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest_bunit", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest_category", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest_nt_domain", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest_priority", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("duration", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("reason", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("response_time", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("session_id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("signature", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("signature_id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src_bunit", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src_category", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src_nt_domain", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src_priority", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src_user", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src_user_bunit", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src_user_category", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src_user_id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src_user_priority", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src_user_role", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src_user_type", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("tag", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user_agent", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user_bunit", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user_category", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user_id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user_priority", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user_role", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user_type", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("vendor_account", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Status indicators
        .add("is_Failed_Authentication", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("is_not_Failed_Authentication", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("is_Successful_Authentication", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("is_not_Successful_Authentication", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("is_Default_Authentication", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("is_not_Default_Authentication", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("is_Insecure_Authentication", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("is_not_Insecure_Authentication", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("is_Privileged_Authentication", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("is_not_Privileged_Authentication", typeFactory.createSqlType(SqlTypeName.INTEGER))

        .add("_extra", typeFactory.createSqlType(SqlTypeName.ANY))
        .build();

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("action", "Authentication.action");
    fieldMapping.put("app", "Authentication.app");
    fieldMapping.put("authentication_method", "Authentication.authentication_method");
    fieldMapping.put("authentication_service", "Authentication.authentication_service");
    fieldMapping.put("dest", "Authentication.dest");
    fieldMapping.put("dest_bunit", "Authentication.dest_bunit");
    fieldMapping.put("dest_category", "Authentication.dest_category");
    fieldMapping.put("dest_nt_domain", "Authentication.dest_nt_domain");
    fieldMapping.put("dest_priority", "Authentication.dest_priority");
    fieldMapping.put("duration", "Authentication.duration");
    fieldMapping.put("reason", "Authentication.reason");
    fieldMapping.put("response_time", "Authentication.response_time");
    fieldMapping.put("session_id", "Authentication.session_id");
    fieldMapping.put("signature", "Authentication.signature");
    fieldMapping.put("signature_id", "Authentication.signature_id");
    fieldMapping.put("src", "Authentication.src");
    fieldMapping.put("src_bunit", "Authentication.src_bunit");
    fieldMapping.put("src_category", "Authentication.src_category");
    fieldMapping.put("src_nt_domain", "Authentication.src_nt_domain");
    fieldMapping.put("src_priority", "Authentication.src_priority");
    fieldMapping.put("src_user", "Authentication.src_user");
    fieldMapping.put("src_user_bunit", "Authentication.src_user_bunit");
    fieldMapping.put("src_user_category", "Authentication.src_user_category");
    fieldMapping.put("src_user_id", "Authentication.src_user_id");
    fieldMapping.put("src_user_priority", "Authentication.src_user_priority");
    fieldMapping.put("src_user_role", "Authentication.src_user_role");
    fieldMapping.put("src_user_type", "Authentication.src_user_type");
    fieldMapping.put("tag", "Authentication.tag");
    fieldMapping.put("user", "Authentication.user");
    fieldMapping.put("user_agent", "Authentication.user_agent");
    fieldMapping.put("user_bunit", "Authentication.user_bunit");
    fieldMapping.put("user_category", "Authentication.user_category");
    fieldMapping.put("user_id", "Authentication.user_id");
    fieldMapping.put("user_priority", "Authentication.user_priority");
    fieldMapping.put("user_role", "Authentication.user_role");
    fieldMapping.put("user_type", "Authentication.user_type");
    fieldMapping.put("vendor_account", "Authentication.vendor_account");

    // Map status indicators
    fieldMapping.put("is_Failed_Authentication", "Authentication.is_Failed_Authentication");
    fieldMapping.put("is_not_Failed_Authentication", "Authentication.is_not_Failed_Authentication");
    fieldMapping.put("is_Successful_Authentication", "Authentication.is_Successful_Authentication");
    fieldMapping.put("is_not_Successful_Authentication", "Authentication.is_not_Successful_Authentication");
    fieldMapping.put("is_Default_Authentication", "Authentication.is_Default_Authentication");
    fieldMapping.put("is_not_Default_Authentication", "Authentication.is_not_Default_Authentication");
    fieldMapping.put("is_Insecure_Authentication", "Authentication.is_Insecure_Authentication");
    fieldMapping.put("is_not_Insecure_Authentication", "Authentication.is_not_Insecure_Authentication");
    fieldMapping.put("is_Privileged_Authentication", "Authentication.is_Privileged_Authentication");
    fieldMapping.put("is_not_Privileged_Authentication", "Authentication.is_not_Privileged_Authentication");

    String searchString = "| datamodel Authentication Authentication search";
    return new CimSchemaResult(schema, fieldMapping, searchString);
  }

  /**
   * Certificates CIM model schema - SSL/TLS certificate data
   */
  private static CimSchemaResult buildCertificatesSchemaWithMapping(RelDataTypeFactory typeFactory) {
    RelDataType schema = typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Certificate-specific fields
        .add("action", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest_port", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("ssl_subject", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("ssl_issuer", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("ssl_serial", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("ssl_hash", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("ssl_subject_common_name", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("ssl_issuer_common_name", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("ssl_signature_algorithm", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("ssl_key_length", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("ssl_version", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("ssl_start_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("ssl_end_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))

        .add("_extra", typeFactory.createSqlType(SqlTypeName.ANY))
        .build();

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("action", "Certificates.action");
    fieldMapping.put("dest", "Certificates.dest");
    fieldMapping.put("dest_port", "Certificates.dest_port");
    fieldMapping.put("ssl_subject", "Certificates.ssl_subject");
    fieldMapping.put("ssl_issuer", "Certificates.ssl_issuer");
    fieldMapping.put("ssl_serial", "Certificates.ssl_serial");
    fieldMapping.put("ssl_hash", "Certificates.ssl_hash");
    fieldMapping.put("ssl_subject_common_name", "Certificates.ssl_subject_common_name");
    fieldMapping.put("ssl_issuer_common_name", "Certificates.ssl_issuer_common_name");
    fieldMapping.put("ssl_signature_algorithm", "Certificates.ssl_signature_algorithm");
    fieldMapping.put("ssl_key_length", "Certificates.ssl_key_length");
    fieldMapping.put("ssl_version", "Certificates.ssl_version");
    fieldMapping.put("ssl_start_time", "Certificates.ssl_start_time");
    fieldMapping.put("ssl_end_time", "Certificates.ssl_end_time");

    String searchString = "| datamodel Certificates Certificates search";
    return new CimSchemaResult(schema, fieldMapping, searchString);
  }

  /**
   * Change CIM model schema - CRUD operations on systems
   */
  private static CimSchemaResult buildChangeSchemaWithMapping(RelDataTypeFactory typeFactory) {
    RelDataType schema = typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Change-specific fields
        .add("action", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("change_type", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("command", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("object", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("object_attrs", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("object_category", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("object_id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("object_path", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("result", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("status", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("vendor_product", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        .add("_extra", typeFactory.createSqlType(SqlTypeName.ANY))
        .build();

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("action", "Change.action");
    fieldMapping.put("change_type", "Change.change_type");
    fieldMapping.put("command", "Change.command");
    fieldMapping.put("dest", "Change.dest");
    fieldMapping.put("object", "Change.object");
    fieldMapping.put("object_attrs", "Change.object_attrs");
    fieldMapping.put("object_category", "Change.object_category");
    fieldMapping.put("object_id", "Change.object_id");
    fieldMapping.put("object_path", "Change.object_path");
    fieldMapping.put("result", "Change.result");
    fieldMapping.put("src", "Change.src");
    fieldMapping.put("status", "Change.status");
    fieldMapping.put("user", "Change.user");
    fieldMapping.put("vendor_product", "Change.vendor_product");

    String searchString = "| datamodel Change All_Changes search";
    return new CimSchemaResult(schema, fieldMapping, searchString);
  }

  /**
   * Data Access CIM model schema - Shared data access monitoring
   */
  private static CimSchemaResult buildDataAccessSchemaWithMapping(RelDataTypeFactory typeFactory) {
    RelDataType schema = typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Data Access-specific fields
        .add("action", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("object", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("object_path", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("object_type", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("vendor_product", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        .add("_extra", typeFactory.createSqlType(SqlTypeName.ANY))
        .build();

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("action", "DataAccess.action");
    fieldMapping.put("dest", "DataAccess.dest");
    fieldMapping.put("object", "DataAccess.object");
    fieldMapping.put("object_path", "DataAccess.object_path");
    fieldMapping.put("object_type", "DataAccess.object_type");
    fieldMapping.put("src", "DataAccess.src");
    fieldMapping.put("user", "DataAccess.user");
    fieldMapping.put("vendor_product", "DataAccess.vendor_product");

    String searchString = "| datamodel Data_Access Data_Access search";
    return new CimSchemaResult(schema, fieldMapping, searchString);
  }

  /**
   * Databases CIM model schema - Database events
   */
  private static CimSchemaResult buildDatabasesSchemaWithMapping(RelDataTypeFactory typeFactory) {
    RelDataType schema = typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Database-specific fields
        .add("action", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("app", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("bytes_in", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("bytes_out", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest_port", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("duration", typeFactory.createSqlType(SqlTypeName.DOUBLE))
        .add("query", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("session_id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src_port", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("user", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("vendor_product", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        .add("_extra", typeFactory.createSqlType(SqlTypeName.ANY))
        .build();

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("action", "Databases.action");
    fieldMapping.put("app", "Databases.app");
    fieldMapping.put("bytes_in", "Databases.bytes_in");
    fieldMapping.put("bytes_out", "Databases.bytes_out");
    fieldMapping.put("dest", "Databases.dest");
    fieldMapping.put("dest_port", "Databases.dest_port");
    fieldMapping.put("duration", "Databases.duration");
    fieldMapping.put("query", "Databases.query");
    fieldMapping.put("session_id", "Databases.session_id");
    fieldMapping.put("src", "Databases.src");
    fieldMapping.put("src_port", "Databases.src_port");
    fieldMapping.put("user", "Databases.user");
    fieldMapping.put("vendor_product", "Databases.vendor_product");

    String searchString = "| datamodel Databases Databases search";
    return new CimSchemaResult(schema, fieldMapping, searchString);
  }

  /**
   * Data Loss Prevention CIM model schema - DLP policy violations
   */
  private static CimSchemaResult buildDataLossPreventionSchemaWithMapping(RelDataTypeFactory typeFactory) {
    RelDataType schema = typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // DLP-specific fields
        .add("action", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("bytes_out", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("file_hash", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("file_name", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("file_path", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("file_size", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("recipient", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("rule_name", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("severity", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("signature", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src_user", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("vendor_product", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("violation_count", typeFactory.createSqlType(SqlTypeName.INTEGER))

        .add("_extra", typeFactory.createSqlType(SqlTypeName.ANY))
        .build();

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("action", "DLP.action");
    fieldMapping.put("bytes_out", "DLP.bytes_out");
    fieldMapping.put("dest", "DLP.dest");
    fieldMapping.put("file_hash", "DLP.file_hash");
    fieldMapping.put("file_name", "DLP.file_name");
    fieldMapping.put("file_path", "DLP.file_path");
    fieldMapping.put("file_size", "DLP.file_size");
    fieldMapping.put("recipient", "DLP.recipient");
    fieldMapping.put("rule_name", "DLP.rule_name");
    fieldMapping.put("severity", "DLP.severity");
    fieldMapping.put("signature", "DLP.signature");
    fieldMapping.put("src_user", "DLP.src_user");
    fieldMapping.put("user", "DLP.user");
    fieldMapping.put("vendor_product", "DLP.vendor_product");
    fieldMapping.put("violation_count", "DLP.violation_count");

    String searchString = "| datamodel Data_Loss_Prevention DLP search";
    return new CimSchemaResult(schema, fieldMapping, searchString);
  }

  /**
   * Email CIM model schema - Email traffic and filtering
   */
  private static CimSchemaResult buildEmailSchemaWithMapping(RelDataTypeFactory typeFactory) {
    RelDataType schema = typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Email-specific fields
        .add("action", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("content_type", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("file_hash", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("file_name", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("file_size", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("message_id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("protocol", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("recipient", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("recipient_count", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("recipient_status", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("return_addr", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sender", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("size", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("src", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("subject", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("vendor_product", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        .add("_extra", typeFactory.createSqlType(SqlTypeName.ANY))
        .build();

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("action", "Email.action");
    fieldMapping.put("content_type", "Email.content_type");
    fieldMapping.put("dest", "Email.dest");
    fieldMapping.put("file_hash", "Email.file_hash");
    fieldMapping.put("file_name", "Email.file_name");
    fieldMapping.put("file_size", "Email.file_size");
    fieldMapping.put("message_id", "Email.message_id");
    fieldMapping.put("protocol", "Email.protocol");
    fieldMapping.put("recipient", "Email.recipient");
    fieldMapping.put("recipient_count", "Email.recipient_count");
    fieldMapping.put("recipient_status", "Email.recipient_status");
    fieldMapping.put("return_addr", "Email.return_addr");
    fieldMapping.put("sender", "Email.sender");
    fieldMapping.put("size", "Email.size");
    fieldMapping.put("src", "Email.src");
    fieldMapping.put("subject", "Email.subject");
    fieldMapping.put("user", "Email.user");
    fieldMapping.put("vendor_product", "Email.vendor_product");

    String searchString = "| datamodel Email Email search";
    return new CimSchemaResult(schema, fieldMapping, searchString);
  }

  /**
   * Endpoint CIM model schema - EDR events (replaces Application State)
   */
  private static CimSchemaResult buildEndpointSchemaWithMapping(RelDataTypeFactory typeFactory) {
    RelDataType schema = typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Endpoint-specific fields (processes, services, filesystem, registry, ports)
        .add("action", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Process fields
        .add("process", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("process_name", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("process_id", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("process_path", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("process_hash", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("parent_process", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("parent_process_id", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("parent_process_name", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Service fields
        .add("service", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("service_name", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("service_path", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("service_hash", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("start_mode", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Filesystem fields
        .add("file_name", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("file_path", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("file_hash", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("file_size", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("file_create_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("file_modify_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))

        // Registry fields
        .add("registry_path", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("registry_key_name", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("registry_value_name", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("registry_value_data", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("registry_value_type", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Port fields
        .add("dest_port", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("protocol", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("state", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        .add("user", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("vendor_product", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        .add("_extra", typeFactory.createSqlType(SqlTypeName.ANY))
        .build();

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("action", "Endpoint.action");
    fieldMapping.put("dest", "Endpoint.dest");
    fieldMapping.put("process", "Endpoint.Processes.process");
    fieldMapping.put("process_name", "Endpoint.Processes.process_name");
    fieldMapping.put("process_id", "Endpoint.Processes.process_id");
    fieldMapping.put("process_path", "Endpoint.Processes.process_path");
    fieldMapping.put("process_hash", "Endpoint.Processes.process_hash");
    fieldMapping.put("parent_process", "Endpoint.Processes.parent_process");
    fieldMapping.put("parent_process_id", "Endpoint.Processes.parent_process_id");
    fieldMapping.put("parent_process_name", "Endpoint.Processes.parent_process_name");
    fieldMapping.put("service", "Endpoint.Services.service");
    fieldMapping.put("service_name", "Endpoint.Services.service_name");
    fieldMapping.put("service_path", "Endpoint.Services.service_path");
    fieldMapping.put("service_hash", "Endpoint.Services.service_hash");
    fieldMapping.put("start_mode", "Endpoint.Services.start_mode");
    fieldMapping.put("file_name", "Endpoint.Filesystem.file_name");
    fieldMapping.put("file_path", "Endpoint.Filesystem.file_path");
    fieldMapping.put("file_hash", "Endpoint.Filesystem.file_hash");
    fieldMapping.put("file_size", "Endpoint.Filesystem.file_size");
    fieldMapping.put("file_create_time", "Endpoint.Filesystem.file_create_time");
    fieldMapping.put("file_modify_time", "Endpoint.Filesystem.file_modify_time");
    fieldMapping.put("registry_path", "Endpoint.Registry.registry_path");
    fieldMapping.put("registry_key_name", "Endpoint.Registry.registry_key_name");
    fieldMapping.put("registry_value_name", "Endpoint.Registry.registry_value_name");
    fieldMapping.put("registry_value_data", "Endpoint.Registry.registry_value_data");
    fieldMapping.put("registry_value_type", "Endpoint.Registry.registry_value_type");
    fieldMapping.put("dest_port", "Endpoint.Ports.dest_port");
    fieldMapping.put("protocol", "Endpoint.Ports.protocol");
    fieldMapping.put("state", "Endpoint.Ports.state");
    fieldMapping.put("user", "Endpoint.user");
    fieldMapping.put("vendor_product", "Endpoint.vendor_product");

    String searchString = "| datamodel Endpoint Endpoint search";
    return new CimSchemaResult(schema, fieldMapping, searchString);
  }

  /**
   * Event Signatures CIM model schema - Signature-based detection
   */
  private static CimSchemaResult buildEventSignaturesSchemaWithMapping(RelDataTypeFactory typeFactory) {
    RelDataType schema = typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Event Signatures-specific fields
        .add("action", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("severity", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("signature", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("signature_id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("vendor_product", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        .add("_extra", typeFactory.createSqlType(SqlTypeName.ANY))
        .build();

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("action", "EventSignatures.action");
    fieldMapping.put("dest", "EventSignatures.dest");
    fieldMapping.put("severity", "EventSignatures.severity");
    fieldMapping.put("signature", "EventSignatures.signature");
    fieldMapping.put("signature_id", "EventSignatures.signature_id");
    fieldMapping.put("src", "EventSignatures.src");
    fieldMapping.put("user", "EventSignatures.user");
    fieldMapping.put("vendor_product", "EventSignatures.vendor_product");

    String searchString = "| datamodel Event_Signatures Event_Signatures search";
    return new CimSchemaResult(schema, fieldMapping, searchString);
  }

  /**
   * Interprocess Messaging CIM model schema - IPC and message queues
   */
  private static CimSchemaResult buildInterprocessMessagingSchemaWithMapping(RelDataTypeFactory typeFactory) {
    RelDataType schema = typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Messaging-specific fields
        .add("correlation_id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest_process", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("message_id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("message_size", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("message_type", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("protocol", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("queue_depth", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("queue_name", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("response_time", typeFactory.createSqlType(SqlTypeName.DOUBLE))
        .add("src_process", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        .add("_extra", typeFactory.createSqlType(SqlTypeName.ANY))
        .build();

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("correlation_id", "Messaging.correlation_id");
    fieldMapping.put("dest_process", "Messaging.dest_process");
    fieldMapping.put("message_id", "Messaging.message_id");
    fieldMapping.put("message_size", "Messaging.message_size");
    fieldMapping.put("message_type", "Messaging.message_type");
    fieldMapping.put("protocol", "Messaging.protocol");
    fieldMapping.put("queue_depth", "Messaging.queue_depth");
    fieldMapping.put("queue_name", "Messaging.queue_name");
    fieldMapping.put("response_time", "Messaging.response_time");
    fieldMapping.put("src_process", "Messaging.src_process");

    String searchString = "| datamodel Interprocess_Messaging Messaging search";
    return new CimSchemaResult(schema, fieldMapping, searchString);
  }

  /**
   * Intrusion Detection CIM model schema - IDS/IPS alerts
   */
  private static CimSchemaResult buildIntrusionDetectionSchemaWithMapping(RelDataTypeFactory typeFactory) {
    RelDataType schema = typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // IDS-specific fields
        .add("action", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("category", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest_ip", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest_port", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("ids_type", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("protocol", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("severity", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("signature", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("signature_id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src_ip", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src_port", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("tag", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("transport", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("vendor_product", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        .add("_extra", typeFactory.createSqlType(SqlTypeName.ANY))
        .build();

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("action", "IDS.action");
    fieldMapping.put("category", "IDS.category");
    fieldMapping.put("dest", "IDS.dest");
    fieldMapping.put("dest_ip", "IDS.dest_ip");
    fieldMapping.put("dest_port", "IDS.dest_port");
    fieldMapping.put("ids_type", "IDS.ids_type");
    fieldMapping.put("protocol", "IDS.protocol");
    fieldMapping.put("severity", "IDS.severity");
    fieldMapping.put("signature", "IDS.signature");
    fieldMapping.put("signature_id", "IDS.signature_id");
    fieldMapping.put("src", "IDS.src");
    fieldMapping.put("src_ip", "IDS.src_ip");
    fieldMapping.put("src_port", "IDS.src_port");
    fieldMapping.put("tag", "IDS.tag");
    fieldMapping.put("transport", "IDS.transport");
    fieldMapping.put("user", "IDS.user");
    fieldMapping.put("vendor_product", "IDS.vendor_product");

    String searchString = "| datamodel Intrusion_Detection IDS search";
    return new CimSchemaResult(schema, fieldMapping, searchString);
  }

  /**
   * Inventory CIM model schema - Asset and network inventory
   */
  private static CimSchemaResult buildInventorySchemaWithMapping(RelDataTypeFactory typeFactory) {
    RelDataType schema = typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Inventory-specific fields
        .add("category", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("mac", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("os", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("vendor_product", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("version", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        .add("_extra", typeFactory.createSqlType(SqlTypeName.ANY))
        .build();

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("category", "Inventory.category");
    fieldMapping.put("dest", "Inventory.dest");
    fieldMapping.put("mac", "Inventory.mac");
    fieldMapping.put("os", "Inventory.os");
    fieldMapping.put("user", "Inventory.user");
    fieldMapping.put("vendor_product", "Inventory.vendor_product");
    fieldMapping.put("version", "Inventory.version");

    String searchString = "| datamodel Inventory Inventory search";
    return new CimSchemaResult(schema, fieldMapping, searchString);
  }

  /**
   * JVM CIM model schema - Java application performance
   */
  private static CimSchemaResult buildJvmSchemaWithMapping(RelDataTypeFactory typeFactory) {
    RelDataType schema = typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // JVM-specific fields
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("jvm_classes_loaded", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("jvm_gc_count", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("jvm_gc_time", typeFactory.createSqlType(SqlTypeName.DOUBLE))
        .add("jvm_memory_heap_max", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("jvm_memory_heap_used", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("jvm_memory_nonheap_used", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("jvm_threads_active", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("jvm_version", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("process_name", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        .add("_extra", typeFactory.createSqlType(SqlTypeName.ANY))
        .build();

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("dest", "JVM.dest");
    fieldMapping.put("jvm_classes_loaded", "JVM.jvm_classes_loaded");
    fieldMapping.put("jvm_gc_count", "JVM.jvm_gc_count");
    fieldMapping.put("jvm_gc_time", "JVM.jvm_gc_time");
    fieldMapping.put("jvm_memory_heap_max", "JVM.jvm_memory_heap_max");
    fieldMapping.put("jvm_memory_heap_used", "JVM.jvm_memory_heap_used");
    fieldMapping.put("jvm_memory_nonheap_used", "JVM.jvm_memory_nonheap_used");
    fieldMapping.put("jvm_threads_active", "JVM.jvm_threads_active");
    fieldMapping.put("jvm_version", "JVM.jvm_version");
    fieldMapping.put("process_name", "JVM.process_name");

    String searchString = "| datamodel JVM JVM search";
    return new CimSchemaResult(schema, fieldMapping, searchString);
  }

  /**
   * Malware CIM model schema - Malware detection events
   */
  private static CimSchemaResult buildMalwareSchemaWithMapping(RelDataTypeFactory typeFactory) {
    RelDataType schema = typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Malware-specific fields
        .add("action", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("category", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("file_hash", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("file_name", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("file_path", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("signature", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("signature_id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("vendor_product", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        .add("_extra", typeFactory.createSqlType(SqlTypeName.ANY))
        .build();

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("action", "Malware.action");
    fieldMapping.put("category", "Malware.category");
    fieldMapping.put("dest", "Malware.dest");
    fieldMapping.put("file_hash", "Malware.file_hash");
    fieldMapping.put("file_name", "Malware.file_name");
    fieldMapping.put("file_path", "Malware.file_path");
    fieldMapping.put("signature", "Malware.signature");
    fieldMapping.put("signature_id", "Malware.signature_id");
    fieldMapping.put("src", "Malware.src");
    fieldMapping.put("user", "Malware.user");
    fieldMapping.put("vendor_product", "Malware.vendor_product");

    String searchString = "| datamodel Malware Malware search";
    return new CimSchemaResult(schema, fieldMapping, searchString);
  }

  /**
   * Network Resolution (DNS) CIM model schema - DNS queries and responses
   */
  private static CimSchemaResult buildNetworkResolutionSchemaWithMapping(RelDataTypeFactory typeFactory) {
    RelDataType schema = typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // DNS-specific fields
        .add("answer", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("answer_count", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("duration", typeFactory.createSqlType(SqlTypeName.DOUBLE))
        .add("query", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("query_count", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("query_type", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("reply_code", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("reply_code_id", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("response_time", typeFactory.createSqlType(SqlTypeName.DOUBLE))
        .add("src", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("ttl", typeFactory.createSqlType(SqlTypeName.INTEGER))

        .add("_extra", typeFactory.createSqlType(SqlTypeName.ANY))
        .build();

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("answer", "DNS.answer");
    fieldMapping.put("answer_count", "DNS.answer_count");
    fieldMapping.put("dest", "DNS.dest");
    fieldMapping.put("duration", "DNS.duration");
    fieldMapping.put("query", "DNS.query");
    fieldMapping.put("query_count", "DNS.query_count");
    fieldMapping.put("query_type", "DNS.query_type");
    fieldMapping.put("reply_code", "DNS.reply_code");
    fieldMapping.put("reply_code_id", "DNS.reply_code_id");
    fieldMapping.put("response_time", "DNS.response_time");
    fieldMapping.put("src", "DNS.src");
    fieldMapping.put("ttl", "DNS.ttl");

    String searchString = "| datamodel Network_Resolution DNS search";
    return new CimSchemaResult(schema, fieldMapping, searchString);
  }

  /**
   * Network Sessions CIM model schema - DHCP, VPN, and proxy sessions
   */
  private static CimSchemaResult buildNetworkSessionsSchemaWithMapping(RelDataTypeFactory typeFactory) {
    RelDataType schema = typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Network Sessions-specific fields
        .add("bytes_in", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("bytes_out", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest_ip", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest_port", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("duration", typeFactory.createSqlType(SqlTypeName.DOUBLE))
        .add("lease_duration", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("packets_in", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("packets_out", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("session_id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("signature", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src_ip", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src_port", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("transport", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("vendor_class", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        .add("_extra", typeFactory.createSqlType(SqlTypeName.ANY))
        .build();

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("bytes_in", "NetworkSessions.bytes_in");
    fieldMapping.put("bytes_out", "NetworkSessions.bytes_out");
    fieldMapping.put("dest", "NetworkSessions.dest");
    fieldMapping.put("dest_ip", "NetworkSessions.dest_ip");
    fieldMapping.put("dest_port", "NetworkSessions.dest_port");
    fieldMapping.put("duration", "NetworkSessions.duration");
    fieldMapping.put("lease_duration", "NetworkSessions.lease_duration");
    fieldMapping.put("packets_in", "NetworkSessions.packets_in");
    fieldMapping.put("packets_out", "NetworkSessions.packets_out");
    fieldMapping.put("session_id", "NetworkSessions.session_id");
    fieldMapping.put("signature", "NetworkSessions.signature");
    fieldMapping.put("src", "NetworkSessions.src");
    fieldMapping.put("src_ip", "NetworkSessions.src_ip");
    fieldMapping.put("src_port", "NetworkSessions.src_port");
    fieldMapping.put("transport", "NetworkSessions.transport");
    fieldMapping.put("user", "NetworkSessions.user");
    fieldMapping.put("vendor_class", "NetworkSessions.vendor_class");

    String searchString = "| datamodel Network_Sessions All_Sessions search";
    return new CimSchemaResult(schema, fieldMapping, searchString);
  }

  /**
   * Network Traffic CIM model schema with field mapping.
   */
  private static CimSchemaResult buildNetworkTrafficSchemaWithMapping(RelDataTypeFactory typeFactory) {
    RelDataType schema = typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Network-specific fields
        .add("action", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("bytes", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("bytes_in", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("bytes_out", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest_ip", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest_mac", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest_port", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("direction", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("duration", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("packets", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("packets_in", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("packets_out", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("protocol", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src_ip", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src_mac", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src_port", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("transport", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("vlan", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        .add("_extra", typeFactory.createSqlType(SqlTypeName.ANY))
        .build();

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("action", "NetworkTraffic.action");
    fieldMapping.put("bytes", "NetworkTraffic.bytes");
    fieldMapping.put("bytes_in", "NetworkTraffic.bytes_in");
    fieldMapping.put("bytes_out", "NetworkTraffic.bytes_out");
    fieldMapping.put("dest", "NetworkTraffic.dest");
    fieldMapping.put("dest_ip", "NetworkTraffic.dest_ip");
    fieldMapping.put("dest_mac", "NetworkTraffic.dest_mac");
    fieldMapping.put("dest_port", "NetworkTraffic.dest_port");
    fieldMapping.put("direction", "NetworkTraffic.direction");
    fieldMapping.put("duration", "NetworkTraffic.duration");
    fieldMapping.put("packets", "NetworkTraffic.packets");
    fieldMapping.put("packets_in", "NetworkTraffic.packets_in");
    fieldMapping.put("packets_out", "NetworkTraffic.packets_out");
    fieldMapping.put("protocol", "NetworkTraffic.protocol");
    fieldMapping.put("src", "NetworkTraffic.src");
    fieldMapping.put("src_ip", "NetworkTraffic.src_ip");
    fieldMapping.put("src_mac", "NetworkTraffic.src_mac");
    fieldMapping.put("src_port", "NetworkTraffic.src_port");
    fieldMapping.put("transport", "NetworkTraffic.transport");
    fieldMapping.put("vlan", "NetworkTraffic.vlan");

    String searchString = "| datamodel Network_Traffic All_Traffic search";
    return new CimSchemaResult(schema, fieldMapping, searchString);
  }

  /**
   * Performance CIM model schema - System performance metrics
   */
  private static CimSchemaResult buildPerformanceSchemaWithMapping(RelDataTypeFactory typeFactory) {
    RelDataType schema = typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Performance-specific fields
        .add("cpu_load_percent", typeFactory.createSqlType(SqlTypeName.DOUBLE))
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("mem", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("mem_used", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("thruput", typeFactory.createSqlType(SqlTypeName.DOUBLE))

        .add("_extra", typeFactory.createSqlType(SqlTypeName.ANY))
        .build();

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("cpu_load_percent", "Performance.cpu_load_percent");
    fieldMapping.put("dest", "Performance.dest");
    fieldMapping.put("mem", "Performance.mem");
    fieldMapping.put("mem_used", "Performance.mem_used");
    fieldMapping.put("thruput", "Performance.thruput");

    String searchString = "| datamodel Performance Performance search";
    return new CimSchemaResult(schema, fieldMapping, searchString);
  }

  /**
   * Splunk Audit Logs CIM model schema - Splunk internal auditing
   */
  private static CimSchemaResult buildSplunkAuditLogsSchemaWithMapping(RelDataTypeFactory typeFactory) {
    RelDataType schema = typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Splunk Audit-specific fields
        .add("action", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("info", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("search", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("search_id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        .add("_extra", typeFactory.createSqlType(SqlTypeName.ANY))
        .build();

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("action", "SplunkAudit.action");
    fieldMapping.put("info", "SplunkAudit.info");
    fieldMapping.put("search", "SplunkAudit.search");
    fieldMapping.put("search_id", "SplunkAudit.search_id");
    fieldMapping.put("user", "SplunkAudit.user");

    String searchString = "| datamodel Splunk_Audit Splunk_Audit search";
    return new CimSchemaResult(schema, fieldMapping, searchString);
  }

  /**
   * Ticket Management CIM model schema - ITIL ticketing systems
   */
  private static CimSchemaResult buildTicketManagementSchemaWithMapping(RelDataTypeFactory typeFactory) {
    RelDataType schema = typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Ticket Management-specific fields
        .add("action", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("category", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("priority", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("severity", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("status", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("ticket_id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("vendor_product", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        .add("_extra", typeFactory.createSqlType(SqlTypeName.ANY))
        .build();

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("action", "TicketManagement.action");
    fieldMapping.put("category", "TicketManagement.category");
    fieldMapping.put("dest", "TicketManagement.dest");
    fieldMapping.put("priority", "TicketManagement.priority");
    fieldMapping.put("severity", "TicketManagement.severity");
    fieldMapping.put("status", "TicketManagement.status");
    fieldMapping.put("ticket_id", "TicketManagement.ticket_id");
    fieldMapping.put("user", "TicketManagement.user");
    fieldMapping.put("vendor_product", "TicketManagement.vendor_product");

    String searchString = "| datamodel Ticket_Management Ticket_Management search";
    return new CimSchemaResult(schema, fieldMapping, searchString);
  }

  /**
   * Updates CIM model schema - Patch management events
   */
  private static CimSchemaResult buildUpdatesSchemaWithMapping(RelDataTypeFactory typeFactory) {
    RelDataType schema = typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Updates-specific fields
        .add("category", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("file_hash", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("file_name", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("file_size", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("kb", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("severity", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("signature", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("status", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("update_name", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("vendor_product", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        .add("_extra", typeFactory.createSqlType(SqlTypeName.ANY))
        .build();

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("category", "Updates.category");
    fieldMapping.put("dest", "Updates.dest");
    fieldMapping.put("file_hash", "Updates.file_hash");
    fieldMapping.put("file_name", "Updates.file_name");
    fieldMapping.put("file_size", "Updates.file_size");
    fieldMapping.put("kb", "Updates.kb");
    fieldMapping.put("severity", "Updates.severity");
    fieldMapping.put("signature", "Updates.signature");
    fieldMapping.put("status", "Updates.status");
    fieldMapping.put("update_name", "Updates.update_name");
    fieldMapping.put("user", "Updates.user");
    fieldMapping.put("vendor_product", "Updates.vendor_product");

    String searchString = "| datamodel Updates Updates search";
    return new CimSchemaResult(schema, fieldMapping, searchString);
  }

  /**
   * Vulnerabilities CIM model schema - Vulnerability scan results
   */
  private static CimSchemaResult buildVulnerabilitiesSchemaWithMapping(RelDataTypeFactory typeFactory) {
    RelDataType schema = typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Vulnerabilities-specific fields
        .add("category", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("cve", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("cvss", typeFactory.createSqlType(SqlTypeName.DOUBLE))
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest_port", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("severity", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("signature", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("signature_id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("vendor_product", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        .add("_extra", typeFactory.createSqlType(SqlTypeName.ANY))
        .build();

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("category", "Vulnerabilities.category");
    fieldMapping.put("cve", "Vulnerabilities.cve");
    fieldMapping.put("cvss", "Vulnerabilities.cvss");
    fieldMapping.put("dest", "Vulnerabilities.dest");
    fieldMapping.put("dest_port", "Vulnerabilities.dest_port");
    fieldMapping.put("severity", "Vulnerabilities.severity");
    fieldMapping.put("signature", "Vulnerabilities.signature");
    fieldMapping.put("signature_id", "Vulnerabilities.signature_id");
    fieldMapping.put("vendor_product", "Vulnerabilities.vendor_product");

    String searchString = "| datamodel Vulnerabilities Vulnerabilities search";
    return new CimSchemaResult(schema, fieldMapping, searchString);
  }

  /**
   * Web CIM model schema with field mapping.
   */
  private static CimSchemaResult buildWebSchemaWithMapping(RelDataTypeFactory typeFactory) {
    RelDataType schema = typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Web-specific fields
        .add("action", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("app", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("bytes", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("bytes_in", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("bytes_out", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("cookie", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest_ip", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest_port", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("http_content_type", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("http_method", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("http_referrer", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("http_user_agent", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src_ip", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src_port", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("status", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("uri", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("uri_path", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("uri_query", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("url", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("url_domain", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        .add("_extra", typeFactory.createSqlType(SqlTypeName.ANY))
        .build();

    Map<String, String> fieldMapping = new HashMap<>();
    fieldMapping.put("action", "Web.action");
    fieldMapping.put("app", "Web.app");
    fieldMapping.put("bytes", "Web.bytes");
    fieldMapping.put("bytes_in", "Web.bytes_in");
    fieldMapping.put("bytes_out", "Web.bytes_out");
    fieldMapping.put("cookie", "Web.cookie");
    fieldMapping.put("dest", "Web.dest");
    fieldMapping.put("dest_ip", "Web.dest_ip");
    fieldMapping.put("dest_port", "Web.dest_port");
    fieldMapping.put("http_content_type", "Web.http_content_type");
    fieldMapping.put("http_method", "Web.http_method");
    fieldMapping.put("http_referrer", "Web.http_referrer");
    fieldMapping.put("http_user_agent", "Web.http_user_agent");
    fieldMapping.put("src", "Web.src");
    fieldMapping.put("src_ip", "Web.src_ip");
    fieldMapping.put("src_port", "Web.src_port");
    fieldMapping.put("status", "Web.status");
    fieldMapping.put("uri", "Web.uri");
    fieldMapping.put("uri_path", "Web.uri_path");
    fieldMapping.put("uri_query", "Web.uri_query");
    fieldMapping.put("url", "Web.url");
    fieldMapping.put("url_domain", "Web.url_domain");
    fieldMapping.put("user", "Web.user");

    String searchString = "| datamodel Web Web search";
    return new CimSchemaResult(schema, fieldMapping, searchString);
  }
}
