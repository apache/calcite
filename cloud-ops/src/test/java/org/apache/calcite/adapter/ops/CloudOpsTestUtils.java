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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Utility class for cloud ops tests.
 */
public class CloudOpsTestUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(CloudOpsTestUtils.class);
  
  /**
   * Load test configuration from local-test.properties file.
   */
  public static CloudOpsConfig loadTestConfig() {
    try {
      Properties props = new Properties();
      props.load(new FileInputStream("src/test/resources/local-test.properties"));
      
      List<String> providers = new ArrayList<>();
      
      // Azure config
      CloudOpsConfig.AzureConfig azureConfig = null;
      if (props.containsKey("azure.tenantId") && props.containsKey("azure.clientId")) {
        azureConfig = new CloudOpsConfig.AzureConfig(
            props.getProperty("azure.tenantId"),
            props.getProperty("azure.clientId"),
            props.getProperty("azure.clientSecret"),
            parseList(props.getProperty("azure.subscriptionIds")));
        providers.add("azure");
      }
      
      // AWS config
      CloudOpsConfig.AWSConfig awsConfig = null;
      if (props.containsKey("aws.accessKeyId") && props.containsKey("aws.secretAccessKey")) {
        awsConfig = new CloudOpsConfig.AWSConfig(
            parseList(props.getProperty("aws.accountIds")),
            props.getProperty("aws.region", "us-east-1"),
            props.getProperty("aws.accessKeyId"),
            props.getProperty("aws.secretAccessKey"),
            props.getProperty("aws.roleArn"));
        providers.add("aws");
      }
      
      // GCP config  
      CloudOpsConfig.GCPConfig gcpConfig = null;
      if (props.containsKey("gcp.credentialsPath") && props.containsKey("gcp.projectIds")) {
        gcpConfig = new CloudOpsConfig.GCPConfig(
            parseList(props.getProperty("gcp.projectIds")),
            props.getProperty("gcp.credentialsPath"));
        providers.add("gcp");
      }
      
      if (providers.isEmpty()) {
        LOGGER.warn("No cloud provider credentials found in local-test.properties");
        return null;
      }
      
      CloudOpsConfig config = new CloudOpsConfig(
          providers, azureConfig, gcpConfig, awsConfig, true, 15, false);
      
      LOGGER.info("Loaded test config with providers: {}", String.join(", ", providers));
      return config;
      
    } catch (IOException e) {
      LOGGER.warn("Could not load local-test.properties: {}", e.getMessage());
      return null;
    }
  }
  
  /**
   * Create model JSON for testing.
   */
  public static String createModelJson(CloudOpsConfig config) {
    StringBuilder json = new StringBuilder();
    json.append("{\n");
    json.append("  \"version\": \"1.0\",\n");
    json.append("  \"defaultSchema\": \"cloud_ops\",\n");
    json.append("  \"schemas\": [\n");
    json.append("    {\n");
    json.append("      \"name\": \"cloud_ops\",\n");
    json.append("      \"type\": \"custom\",\n");
    json.append("      \"factory\": \"org.apache.calcite.adapter.ops.CloudOpsSchemaFactory\",\n");
    json.append("      \"operand\": {\n");
    
    // Add provider configs
    if (config.azure != null) {
      json.append("        \"azure.tenantId\": \"").append(config.azure.tenantId).append("\",\n");
      json.append("        \"azure.clientId\": \"").append(config.azure.clientId).append("\",\n");
      json.append("        \"azure.clientSecret\": \"").append(config.azure.clientSecret).append("\",\n");
      json.append("        \"azure.subscriptionIds\": \"").append(String.join(",", config.azure.subscriptionIds)).append("\",\n");
    }
    
    if (config.aws != null) {
      json.append("        \"aws.accessKeyId\": \"").append(config.aws.accessKeyId).append("\",\n");
      json.append("        \"aws.secretAccessKey\": \"").append(config.aws.secretAccessKey).append("\",\n");
      json.append("        \"aws.region\": \"").append(config.aws.region).append("\",\n");
      json.append("        \"aws.accountIds\": \"").append(String.join(",", config.aws.accountIds)).append("\",\n");
    }
    
    if (config.gcp != null) {
      json.append("        \"gcp.credentialsPath\": \"").append(config.gcp.credentialsPath).append("\",\n");
      json.append("        \"gcp.projectIds\": \"").append(String.join(",", config.gcp.projectIds)).append("\",\n");
    }
    
    // Remove trailing comma
    if (json.toString().endsWith(",\n")) {
      json.setLength(json.length() - 2);
      json.append("\n");
    }
    
    json.append("      }\n");
    json.append("    }\n");
    json.append("  ]\n");
    json.append("}");
    
    return json.toString();
  }
  
  private static List<String> parseList(String value) {
    List<String> result = new ArrayList<>();
    if (value != null && !value.trim().isEmpty()) {
      for (String item : value.split(",")) {
        String trimmed = item.trim();
        if (!trimmed.isEmpty()) {
          result.add(trimmed);
        }
      }
    }
    return result;
  }
}