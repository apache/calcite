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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

/**
 * Unit tests for {@link CloudOpsConfig}.
 */
@Tag("unit")
public class CloudOpsConfigTest {

  @Test public void testAzureConfig() {
    CloudOpsConfig.AzureConfig azureConfig =
        new CloudOpsConfig.AzureConfig("test-tenant-id", "test-client-id", "test-client-secret",
        Arrays.asList("sub1", "sub2", "sub3"));

    assertThat(azureConfig.tenantId, is("test-tenant-id"));
    assertThat(azureConfig.clientId, is("test-client-id"));
    assertThat(azureConfig.clientSecret, is("test-client-secret"));
    assertThat(azureConfig.subscriptionIds, hasSize(3));
    assertThat(azureConfig.subscriptionIds.get(0), is("sub1"));
  }

  @Test public void testGCPConfig() {
    CloudOpsConfig.GCPConfig gcpConfig =
        new CloudOpsConfig.GCPConfig(Arrays.asList("project-1", "project-2"), "/path/to/service-account.json");

    assertThat(gcpConfig.credentialsPath, is("/path/to/service-account.json"));
    assertThat(gcpConfig.projectIds, hasSize(2));
    assertThat(gcpConfig.projectIds.get(0), is("project-1"));
  }

  @Test public void testAWSConfig() {
    CloudOpsConfig.AWSConfig awsConfig =
        new CloudOpsConfig.AWSConfig(Arrays.asList("111111111111", "222222222222"), "us-west-2",
        "AKIATEST123456789", "test-secret-access-key",
        "arn:aws:iam::{account-id}:role/CrossAccountRole");

    assertThat(awsConfig.accessKeyId, is("AKIATEST123456789"));
    assertThat(awsConfig.secretAccessKey, is("test-secret-access-key"));
    assertThat(awsConfig.region, is("us-west-2"));
    assertThat(awsConfig.accountIds, hasSize(2));
    assertThat(awsConfig.roleArn, is("arn:aws:iam::{account-id}:role/CrossAccountRole"));
  }

  @Test public void testFullCloudOpsConfig() {
    CloudOpsConfig.AzureConfig azure =
        new CloudOpsConfig.AzureConfig("azure-tenant", "azure-client", "azure-secret",
        Arrays.asList("azure-sub1", "azure-sub2"));

    CloudOpsConfig.GCPConfig gcp =
        new CloudOpsConfig.GCPConfig(Arrays.asList("gcp-project1", "gcp-project2"), "/gcp/credentials.json");

    CloudOpsConfig.AWSConfig aws =
        new CloudOpsConfig.AWSConfig(Arrays.asList("aws-account1", "aws-account2"), "us-east-1",
        "aws-key", "aws-secret", null);

    CloudOpsConfig config =
        new CloudOpsConfig(Arrays.asList("azure", "gcp", "aws"), azure, gcp, aws, true, 15, false);

    // Verify all configs are set
    assertThat(config.azure, is(notNullValue()));
    assertThat(config.gcp, is(notNullValue()));
    assertThat(config.aws, is(notNullValue()));

    assertThat(config.azure.tenantId, is("azure-tenant"));
    assertThat(config.gcp.credentialsPath, is("/gcp/credentials.json"));
    assertThat(config.aws.accessKeyId, is("aws-key"));
  }

  @Test public void testEmptySubscriptionLists() {
    CloudOpsConfig.AzureConfig azure =
        new CloudOpsConfig.AzureConfig("tenant", "client", "secret", Collections.emptyList());

    CloudOpsConfig.GCPConfig gcp =
        new CloudOpsConfig.GCPConfig(Collections.emptyList(), "/path/to/creds.json");

    CloudOpsConfig.AWSConfig aws =
        new CloudOpsConfig.AWSConfig(Collections.emptyList(), "us-east-1", "key", "secret", null);

    assertThat(azure.subscriptionIds.isEmpty(), is(true));
    assertThat(gcp.projectIds.isEmpty(), is(true));
    assertThat(aws.accountIds.isEmpty(), is(true));
  }

  @Test public void testSingleSubscriptionLists() {
    CloudOpsConfig.AzureConfig azure =
        new CloudOpsConfig.AzureConfig("tenant", "client", "secret", Collections.singletonList("single-azure-sub"));

    CloudOpsConfig.GCPConfig gcp =
        new CloudOpsConfig.GCPConfig(Collections.singletonList("single-gcp-project"), "/path/to/creds.json");

    CloudOpsConfig.AWSConfig aws =
        new CloudOpsConfig.AWSConfig(Collections.singletonList("single-aws-account"), "us-east-1",
        "key", "secret", null);

    assertThat(azure.subscriptionIds, hasSize(1));
    assertThat(gcp.projectIds, hasSize(1));
    assertThat(aws.accountIds, hasSize(1));

    assertThat(azure.subscriptionIds.get(0), is("single-azure-sub"));
    assertThat(gcp.projectIds.get(0), is("single-gcp-project"));
    assertThat(aws.accountIds.get(0), is("single-aws-account"));
  }

  @Test public void testConfigDefaults() {
    CloudOpsConfig config =
        new CloudOpsConfig(null, null, null, null, null, null, null);

    // Check defaults
    assertThat(config.providers, hasSize(3));
    assertThat(config.cacheEnabled, is(true));
    assertThat(config.cacheTtlMinutes, is(5));
    assertThat(config.cacheDebugMode, is(false));
  }
}
