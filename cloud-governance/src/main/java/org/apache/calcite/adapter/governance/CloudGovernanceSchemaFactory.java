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
package org.apache.calcite.adapter.governance;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Factory for Cloud Governance schemas.
 */
public class CloudGovernanceSchemaFactory implements SchemaFactory {
  
  @Override
  public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    try {
      // Extract Azure configuration
      CloudGovernanceConfig.AzureConfig azure = null;
      if (operand.containsKey("azure.tenantId")) {
        azure = new CloudGovernanceConfig.AzureConfig(
            (String) operand.get("azure.tenantId"),
            (String) operand.get("azure.clientId"),
            (String) operand.get("azure.clientSecret"),
            parseList((String) operand.get("azure.subscriptionIds")));
      }
      
      // Extract GCP configuration
      CloudGovernanceConfig.GCPConfig gcp = null;
      if (operand.containsKey("gcp.credentialsPath")) {
        gcp = new CloudGovernanceConfig.GCPConfig(
            parseList((String) operand.get("gcp.projectIds")),
            (String) operand.get("gcp.credentialsPath"));
      }
      
      // Extract AWS configuration
      CloudGovernanceConfig.AWSConfig aws = null;
      if (operand.containsKey("aws.accessKeyId")) {
        aws = new CloudGovernanceConfig.AWSConfig(
            parseList((String) operand.get("aws.accountIds")),
            (String) operand.get("aws.region"),
            (String) operand.get("aws.accessKeyId"),
            (String) operand.get("aws.secretAccessKey"),
            (String) operand.get("aws.roleArn"));
      }
      
      // Validate that at least one provider is configured
      if (azure == null && gcp == null && aws == null) {
        throw new IllegalArgumentException("At least one cloud provider must be configured");
      }
      
      final CloudGovernanceConfig config = new CloudGovernanceConfig(
          null, azure, gcp, aws, true, 15);
      return new CloudGovernanceSchema(config);
    } catch (Exception e) {
      throw new RuntimeException("Error creating Cloud Governance schema", e);
    }
  }
  
  private List<String> parseList(String value) {
    if (value == null || value.trim().isEmpty()) {
      return Arrays.asList();
    }
    return Arrays.asList(value.split(","));
  }
}