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

/**
 * Builder for Splunk Common Information Model (CIM) schemas.
 * Provides predefined field schemas for common Splunk CIM data models,
 * including all standard fields plus an "_extra" field for unmapped data.
 */
public class CimModelBuilder {

  /**
   * Builds a CIM schema for the specified model type.
   *
   * @param typeFactory RelDataTypeFactory for creating field types
   * @param cimModel CIM model name (e.g., "authentication", "network_traffic")
   * @return RelDataType representing the complete schema
   */
  public static RelDataType buildCimSchema(RelDataTypeFactory typeFactory, String cimModel) {
    switch (cimModel.toLowerCase()) {
    case "authentication":
      return buildAuthenticationSchema(typeFactory);
    case "network_traffic":
    case "network":
      return buildNetworkTrafficSchema(typeFactory);
    case "web":
      return buildWebSchema(typeFactory);
    case "malware":
      return buildMalwareSchema(typeFactory);
    case "email":
      return buildEmailSchema(typeFactory);
    case "vulnerability":
      return buildVulnerabilitySchema(typeFactory);
    case "intrusion_detection":
    case "ids":
      return buildIntrusionDetectionSchema(typeFactory);
    case "change":
      return buildChangeSchema(typeFactory);
    case "inventory":
      return buildInventorySchema(typeFactory);
    case "performance":
      return buildPerformanceSchema(typeFactory);
    default:
      return buildBaseSchema(typeFactory);
    }
  }

  /**
   * Base schema with common fields present in most CIM models.
   */
  private static RelDataType buildBaseSchema(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("_raw", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("_extra", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();
  }

  /**
   * Authentication CIM model schema.
   */
  private static RelDataType buildAuthenticationSchema(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
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
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("signature", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("signature_id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src_user", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user_category", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user_type", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("result", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("reason", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("duration", typeFactory.createSqlType(SqlTypeName.INTEGER))

        // Catch-all field
        .add("_extra", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();
  }

  /**
   * Network Traffic CIM model schema.
   */
  private static RelDataType buildNetworkTrafficSchema(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
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
        .add("_extra", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();
  }

  /**
   * Web CIM model schema.
   */
  private static RelDataType buildWebSchema(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
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
        .add("_extra", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();
  }

  /**
   * Malware CIM model schema.
   */
  private static RelDataType buildMalwareSchema(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Malware-specific fields
        .add("action", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("file_create_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("file_hash", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("file_modify_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("file_name", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("file_path", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("file_size", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("signature", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("signature_id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("vendor_product", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Catch-all field
        .add("_extra", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();
  }

  /**
   * Email CIM model schema.
   */
  private static RelDataType buildEmailSchema(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Email-specific fields
        .add("action", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("delay", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("duration", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("message_id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("recipient", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("recipient_count", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("sender", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("size", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("src", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src_user", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("subject", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Catch-all field
        .add("_extra", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();
  }

  /**
   * Vulnerability CIM model schema.
   */
  private static RelDataType buildVulnerabilitySchema(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Vulnerability-specific fields
        .add("category", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("cve", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("cvss", typeFactory.createSqlType(SqlTypeName.DOUBLE))
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest_ip", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("severity", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("signature", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("signature_id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("vendor_product", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Catch-all field
        .add("_extra", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();
  }

  /**
   * Intrusion Detection CIM model schema.
   */
  private static RelDataType buildIntrusionDetectionSchema(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
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
        .add("dvc", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("ids_type", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("product", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("severity", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("signature", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("signature_id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src_ip", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src_port", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("transport", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("vendor", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Catch-all field
        .add("_extra", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();
  }

  /**
   * Change CIM model schema.
   */
  private static RelDataType buildChangeSchema(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
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
        .add("object_category", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("result", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("src", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("status", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("user", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("vendor_product", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Catch-all field
        .add("_extra", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();
  }

  /**
   * Inventory CIM model schema.
   */
  private static RelDataType buildInventorySchema(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Inventory-specific fields
        .add("category", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("cpu_cores", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("cpu_count", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("cpu_mhz", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("cpu_type", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dest_ip", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("dns", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("ip", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("mac", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("mem", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("nt_host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("os", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("version", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Catch-all field
        .add("_extra", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();
  }

  /**
   * Performance CIM model schema.
   */
  private static RelDataType buildPerformanceSchema(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        // Base fields
        .add("_time", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
        .add("host", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("sourcetype", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("index", typeFactory.createSqlType(SqlTypeName.VARCHAR))

        // Performance-specific fields
        .add("cpu_load_percent", typeFactory.createSqlType(SqlTypeName.DOUBLE))
        .add("mem_free", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("mem_used", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("storage_free", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("storage_used", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("thruput", typeFactory.createSqlType(SqlTypeName.BIGINT))

        // Catch-all field
        .add("_extra", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();
  }
}
