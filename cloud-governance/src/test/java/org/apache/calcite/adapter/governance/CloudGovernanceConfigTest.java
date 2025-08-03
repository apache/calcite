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

import org.apache.calcite.adapter.governance.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit tests for {@link CloudGovernanceConfig}.
 */
@Category(UnitTest.class)
public class CloudGovernanceConfigTest {

  @Test
  public void testAzureConfig() {
    CloudGovernanceConfig.AzureConfig azureConfig = new CloudGovernanceConfig.AzureConfig(
        "test-tenant-id", "test-client-id", "test-client-secret", 
        Arrays.asList("sub1", "sub2", "sub3"));
    
    assertThat(azureConfig.tenantId, is("test-tenant-id"));
    assertThat(azureConfig.clientId, is("test-client-id"));
    assertThat(azureConfig.clientSecret, is("test-client-secret"));
    assertThat(azureConfig.subscriptionIds.size(), is(3));
    assertThat(azureConfig.subscriptionIds.get(0), is("sub1"));
  }

  @Test
  public void testGCPConfig() {
    CloudGovernanceConfig.GCPConfig gcpConfig = new CloudGovernanceConfig.GCPConfig(
        Arrays.asList("project-1", "project-2"), "/path/to/service-account.json");
    
    assertThat(gcpConfig.credentialsPath, is("/path/to/service-account.json"));
    assertThat(gcpConfig.projectIds.size(), is(2));
    assertThat(gcpConfig.projectIds.get(0), is("project-1"));
  }

  @Test
  public void testAWSConfig() {
    CloudGovernanceConfig.AWSConfig awsConfig = new CloudGovernanceConfig.AWSConfig(
        Arrays.asList("111111111111", "222222222222"), "us-west-2", 
        "AKIATEST123456789", "test-secret-access-key", 
        "arn:aws:iam::{account-id}:role/CrossAccountRole");
    
    assertThat(awsConfig.accessKeyId, is("AKIATEST123456789"));
    assertThat(awsConfig.secretAccessKey, is("test-secret-access-key"));
    assertThat(awsConfig.region, is("us-west-2"));
    assertThat(awsConfig.accountIds.size(), is(2));
    assertThat(awsConfig.roleArn, is("arn:aws:iam::{account-id}:role/CrossAccountRole"));
  }

  @Test
  public void testFullCloudGovernanceConfig() {
    CloudGovernanceConfig.AzureConfig azure = new CloudGovernanceConfig.AzureConfig(
        "azure-tenant", "azure-client", "azure-secret", 
        Arrays.asList("azure-sub1", "azure-sub2"));
    
    CloudGovernanceConfig.GCPConfig gcp = new CloudGovernanceConfig.GCPConfig(
        Arrays.asList("gcp-project1", "gcp-project2"), "/gcp/credentials.json");
    
    CloudGovernanceConfig.AWSConfig aws = new CloudGovernanceConfig.AWSConfig(
        Arrays.asList("aws-account1", "aws-account2"), "us-east-1", 
        "aws-key", "aws-secret", null);
    
    CloudGovernanceConfig config = new CloudGovernanceConfig(
        Arrays.asList("azure", "gcp", "aws"), azure, gcp, aws, true, 15);
    
    // Verify all configs are set
    assertThat(config.azure, is(notNullValue()));
    assertThat(config.gcp, is(notNullValue()));
    assertThat(config.aws, is(notNullValue()));
    
    assertThat(config.azure.tenantId, is("azure-tenant"));
    assertThat(config.gcp.credentialsPath, is("/gcp/credentials.json"));
    assertThat(config.aws.accessKeyId, is("aws-key"));
  }

  @Test
  public void testEmptySubscriptionLists() {
    CloudGovernanceConfig.AzureConfig azure = new CloudGovernanceConfig.AzureConfig(
        "tenant", "client", "secret", Collections.emptyList());
    
    CloudGovernanceConfig.GCPConfig gcp = new CloudGovernanceConfig.GCPConfig(
        Collections.emptyList(), "/path/to/creds.json");
    
    CloudGovernanceConfig.AWSConfig aws = new CloudGovernanceConfig.AWSConfig(
        Collections.emptyList(), "us-east-1", "key", "secret", null);
    
    assertThat(azure.subscriptionIds.isEmpty(), is(true));
    assertThat(gcp.projectIds.isEmpty(), is(true));
    assertThat(aws.accountIds.isEmpty(), is(true));
  }

  @Test
  public void testSingleSubscriptionLists() {
    CloudGovernanceConfig.AzureConfig azure = new CloudGovernanceConfig.AzureConfig(
        "tenant", "client", "secret", Collections.singletonList("single-azure-sub"));
    
    CloudGovernanceConfig.GCPConfig gcp = new CloudGovernanceConfig.GCPConfig(
        Collections.singletonList("single-gcp-project"), "/path/to/creds.json");
    
    CloudGovernanceConfig.AWSConfig aws = new CloudGovernanceConfig.AWSConfig(
        Collections.singletonList("single-aws-account"), "us-east-1", 
        "key", "secret", null);
    
    assertThat(azure.subscriptionIds.size(), is(1));
    assertThat(gcp.projectIds.size(), is(1));
    assertThat(aws.accountIds.size(), is(1));
    
    assertThat(azure.subscriptionIds.get(0), is("single-azure-sub"));
    assertThat(gcp.projectIds.get(0), is("single-gcp-project"));
    assertThat(aws.accountIds.get(0), is("single-aws-account"));
  }

  @Test
  public void testConfigDefaults() {
    CloudGovernanceConfig config = new CloudGovernanceConfig(
        null, null, null, null, null, null);
    
    // Check defaults
    assertThat(config.providers.size(), is(3));
    assertThat(config.cacheEnabled, is(true));
    assertThat(config.cacheTtlMinutes, is(15));
  }
}