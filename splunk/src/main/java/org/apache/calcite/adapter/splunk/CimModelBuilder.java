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
 * Provides predefined field schemas for common Splunk CIM data models,
 * including all standard fields plus an "_extra" field for unmapped data.
 * Also generates field mappings between schema field names and Splunk CIM field names.
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
   *
   * @param typeFactory RelDataTypeFactory for creating field types
   * @param cimModel CIM model name (e.g., "authentication", "network_traffic")
   * @return CimSchemaResult containing the complete schema, field mapping, and search string
   */
  public static CimSchemaResult buildCimSchemaWithMapping(RelDataTypeFactory typeFactory, String cimModel) {
    switch (cimModel.toLowerCase()) {
    case "authentication":
      return buildAuthenticationSchemaWithMapping(typeFactory);
    case "network_traffic":
    case "network":
      return buildNetworkTrafficSchemaWithMapping(typeFactory);
    case "web":
      return buildWebSchemaWithMapping(typeFactory);
    case "malware":
      return buildMalwareSchemaWithMapping(typeFactory);
    case "email":
      return buildEmailSchemaWithMapping(typeFactory);
    case "vulnerability":
      return buildVulnerabilitySchemaWithMapping(typeFactory);
    case "intrusion_detection":
    case "ids":
      return buildIntrusionDetectionSchemaWithMapping(typeFactory);
    case "change":
      return buildChangeSchemaWithMapping(typeFactory);
    case "inventory":
      return buildInventorySchemaWithMapping(typeFactory);
    case "performance":
      return buildPerformanceSchemaWithMapping(typeFactory);
    default:
      return buildBaseSchemaWithMapping(typeFactory);
    }
  }

  /**
   * Builds a CIM schema for the specified model type (backward compatibility).
   *
   * @param typeFactory RelDataTypeFactory for creating field types
   * @param cimModel CIM model name (e.g., "authentication", "network_traffic")
   * @return RelDataType representing the complete schema
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
    // Base fields typically don't need mapping as they're standard Splunk fields

    return new CimSchemaResult(schema, fieldMapping, "search");
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

        // Authentication-specific fields (schema names without prefix)
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

        // Catch-all field
        .add("_extra", typeFactory.createSqlType(SqlTypeName.ANY))
        .build();

    // Create field mapping from schema names to Splunk CIM field names
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

        // Catch-all field
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

        // Catch-all field
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

  // Stub implementations for other CIM models - expand as needed
  private static CimSchemaResult buildMalwareSchemaWithMapping(RelDataTypeFactory typeFactory) {
    return buildBaseSchemaWithMapping(typeFactory);
  }

  private static CimSchemaResult buildEmailSchemaWithMapping(RelDataTypeFactory typeFactory) {
    return buildBaseSchemaWithMapping(typeFactory);
  }

  private static CimSchemaResult buildVulnerabilitySchemaWithMapping(RelDataTypeFactory typeFactory) {
    return buildBaseSchemaWithMapping(typeFactory);
  }

  private static CimSchemaResult buildIntrusionDetectionSchemaWithMapping(RelDataTypeFactory typeFactory) {
    return buildBaseSchemaWithMapping(typeFactory);
  }

  private static CimSchemaResult buildChangeSchemaWithMapping(RelDataTypeFactory typeFactory) {
    return buildBaseSchemaWithMapping(typeFactory);
  }

  private static CimSchemaResult buildInventorySchemaWithMapping(RelDataTypeFactory typeFactory) {
    return buildBaseSchemaWithMapping(typeFactory);
  }

  private static CimSchemaResult buildPerformanceSchemaWithMapping(RelDataTypeFactory typeFactory) {
    return buildBaseSchemaWithMapping(typeFactory);
  }
}
