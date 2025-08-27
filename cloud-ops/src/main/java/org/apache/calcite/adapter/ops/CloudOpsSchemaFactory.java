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

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Factory for Cloud Governance schemas.
 */
public class CloudOpsSchemaFactory implements SchemaFactory {

  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    try {
      // Extract Azure configuration
      CloudOpsConfig.AzureConfig azure = null;
      String azureTenantId = getConfigValue(operand, "azure.tenantId", "AZURE_TENANT_ID");
      if (azureTenantId != null) {
        String azureClientId = getConfigValue(operand, "azure.clientId", "AZURE_CLIENT_ID");
        String azureClientSecret = getConfigValue(operand, "azure.clientSecret", "AZURE_CLIENT_SECRET");
        String azureSubscriptionIds = getConfigValue(operand, "azure.subscriptionIds", "AZURE_SUBSCRIPTION_IDS");

        if (azureClientId != null && azureClientSecret != null && azureSubscriptionIds != null) {
          azure =
              new CloudOpsConfig.AzureConfig(azureTenantId, azureClientId, azureClientSecret, parseList(azureSubscriptionIds));
        }
      }

      // Extract GCP configuration
      CloudOpsConfig.GCPConfig gcp = null;
      String gcpCredentialsPath = getConfigValue(operand, "gcp.credentialsPath", "GCP_CREDENTIALS_PATH");
      if (gcpCredentialsPath != null) {
        String gcpProjectIds = getConfigValue(operand, "gcp.projectIds", "GCP_PROJECT_IDS");
        if (gcpProjectIds != null) {
          gcp = new CloudOpsConfig.GCPConfig(parseList(gcpProjectIds), gcpCredentialsPath);
        }
      }

      // Extract AWS configuration
      CloudOpsConfig.AWSConfig aws = null;
      String awsAccessKeyId = getConfigValue(operand, "aws.accessKeyId", "AWS_ACCESS_KEY_ID");
      if (awsAccessKeyId != null) {
        String awsAccountIds = getConfigValue(operand, "aws.accountIds", "AWS_ACCOUNT_IDS");
        String awsRegion = getConfigValue(operand, "aws.region", "AWS_REGION");
        String awsSecretAccessKey = getConfigValue(operand, "aws.secretAccessKey", "AWS_SECRET_ACCESS_KEY");
        String awsRoleArn = getConfigValue(operand, "aws.roleArn", "AWS_ROLE_ARN");

        if (awsAccountIds != null && awsRegion != null && awsSecretAccessKey != null) {
          aws =
              new CloudOpsConfig.AWSConfig(parseList(awsAccountIds), awsRegion, awsAccessKeyId, awsSecretAccessKey, awsRoleArn);
        }
      }

      // Extract cache configuration
      Boolean cacheEnabled = getBooleanConfigValue(operand, "cache.enabled", "CLOUD_OPS_CACHE_ENABLED", true);
      Integer cacheTtlMinutes = getIntegerConfigValue(operand, "cache.ttlMinutes", "CLOUD_OPS_CACHE_TTL_MINUTES", 5);
      Boolean cacheDebugMode = getBooleanConfigValue(operand, "cache.debugMode", "CLOUD_OPS_CACHE_DEBUG_MODE", false);

      // Extract providers configuration
      String providersStr = getConfigValue(operand, "providers", "CLOUD_OPS_PROVIDERS");
      List<String> providers = null;
      if (providersStr != null) {
        providers = parseList(providersStr);
      }

      // Validate that at least one provider is configured
      if (azure == null && gcp == null && aws == null) {
        throw new IllegalArgumentException("At least one cloud provider must be configured");
      }

      final CloudOpsConfig config =
          new CloudOpsConfig(providers, azure, gcp, aws, cacheEnabled, cacheTtlMinutes, cacheDebugMode);

      // Create the main Cloud Governance schema
      CloudOpsSchema cloudGovernanceSchema = new CloudOpsSchema(config);

      // Navigate to root schema to add metadata schemas as siblings (following File/Splunk pattern)
      SchemaPlus rootSchema = parentSchema;
      while (rootSchema.getParentSchema() != null) {
        rootSchema = rootSchema.getParentSchema();
      }

      // Add information_schema if not already present (lowercase schema name only)
      if (rootSchema.subSchemas().get("information_schema") == null) {
        // Pass parentSchema so it can see sibling schemas (following Splunk pattern)
        CloudOpsInformationSchema infoSchema = new CloudOpsInformationSchema(parentSchema, "CALCITE");
        rootSchema.add("information_schema", infoSchema);
      }

      // Add pg_catalog if not already present (lowercase schema name only)
      if (rootSchema.subSchemas().get("pg_catalog") == null) {
        // Pass parentSchema so it can see sibling schemas (following Splunk pattern)
        CloudOpsPostgresMetadataSchema pgSchema = new CloudOpsPostgresMetadataSchema(parentSchema, "CALCITE");
        rootSchema.add("pg_catalog", pgSchema);
      }

      return cloudGovernanceSchema;
    } catch (Exception e) {
      throw new RuntimeException("Error creating Cloud Governance schema", e);
    }
  }

  /**
   * Gets a configuration value from query parameters first, then falls back to environment variables.
   *
   * @param operand The operand map containing query parameters
   * @param paramKey The query parameter key (e.g., "azure.tenantId")
   * @param envKey The environment variable key (e.g., "AZURE_TENANT_ID")
   * @return The configuration value, or null if not found in either location
   */
  private String getConfigValue(Map<String, Object> operand, String paramKey, String envKey) {
    // First check query parameters
    String value = (String) operand.get(paramKey);
    if (value != null && !value.trim().isEmpty()) {
      return value.trim();
    }

    // Fall back to environment variable
    value = System.getenv(envKey);
    if (value != null && !value.trim().isEmpty()) {
      return value.trim();
    }

    return null;
  }

  /**
   * Gets a boolean configuration value from query parameters first, then falls back to environment variables.
   */
  private Boolean getBooleanConfigValue(Map<String, Object> operand, String paramKey, String envKey, Boolean defaultValue) {
    String value = getConfigValue(operand, paramKey, envKey);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return defaultValue;
  }

  /**
   * Gets an integer configuration value from query parameters first, then falls back to environment variables.
   */
  private Integer getIntegerConfigValue(Map<String, Object> operand, String paramKey, String envKey, Integer defaultValue) {
    String value = getConfigValue(operand, paramKey, envKey);
    if (value != null) {
      try {
        return Integer.parseInt(value);
      } catch (NumberFormatException e) {
        // Log warning and use default
        System.err.println("Warning: Invalid integer value '" + value + "' for " + paramKey + ", using default: " + defaultValue);
      }
    }
    return defaultValue;
  }

  private List<String> parseList(String value) {
    if (value == null || value.trim().isEmpty()) {
      return Arrays.asList();
    }
    return Arrays.asList(value.split(","));
  }
}
