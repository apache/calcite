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
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit tests for {@link CloudGovernanceSchemaFactory}.
 */
@Category(UnitTest.class)
public class CloudGovernanceSchemaFactoryTest {

  @Test
  public void testCreateSchema() {
    CloudGovernanceSchemaFactory factory = new CloudGovernanceSchemaFactory();
    SchemaPlus parentSchema = Frameworks.createRootSchema(false);
    String name = "cloud_governance";
    
    Map<String, Object> operands = new HashMap<>();
    operands.put("azure.tenantId", "test-tenant");
    operands.put("azure.clientId", "test-client");
    operands.put("azure.clientSecret", "test-secret");
    operands.put("azure.subscriptionIds", "sub1,sub2");
    
    operands.put("gcp.credentialsPath", "/path/to/credentials.json");
    operands.put("gcp.projectIds", "project1,project2");
    
    operands.put("aws.accessKeyId", "test-key");
    operands.put("aws.secretAccessKey", "test-secret");
    operands.put("aws.region", "us-east-1");
    operands.put("aws.accountIds", "account1,account2");
    
    Schema schema = factory.create(parentSchema, name, operands);
    
    assertThat(schema, is(notNullValue()));
    assertThat(schema, instanceOf(CloudGovernanceSchema.class));
  }

  @Test
  public void testCreateSchemaWithMinimalConfig() {
    CloudGovernanceSchemaFactory factory = new CloudGovernanceSchemaFactory();
    SchemaPlus parentSchema = Frameworks.createRootSchema(false);
    String name = "cloud_governance";
    
    // Test with minimal Azure-only config
    Map<String, Object> operands = new HashMap<>();
    operands.put("azure.tenantId", "test-tenant");
    operands.put("azure.clientId", "test-client");
    operands.put("azure.clientSecret", "test-secret");
    operands.put("azure.subscriptionIds", "sub1");
    
    Schema schema = factory.create(parentSchema, name, operands);
    
    assertThat(schema, is(notNullValue()));
    assertThat(schema, instanceOf(CloudGovernanceSchema.class));
  }

  @Test(expected = RuntimeException.class)
  public void testCreateSchemaWithMissingConfig() {
    CloudGovernanceSchemaFactory factory = new CloudGovernanceSchemaFactory();
    SchemaPlus parentSchema = Frameworks.createRootSchema(false);
    String name = "cloud_governance";
    
    // Empty operands should throw exception
    Map<String, Object> operands = new HashMap<>();
    
    factory.create(parentSchema, name, operands);
  }

  @Test
  public void testCreateSchemaWithMultipleProviders() {
    CloudGovernanceSchemaFactory factory = new CloudGovernanceSchemaFactory();
    SchemaPlus parentSchema = Frameworks.createRootSchema(false);
    String name = "cloud_governance";
    
    Map<String, Object> operands = new HashMap<>();
    
    // Azure config
    operands.put("azure.tenantId", "test-tenant");
    operands.put("azure.clientId", "test-client");
    operands.put("azure.clientSecret", "test-secret");
    operands.put("azure.subscriptionIds", "sub1,sub2,sub3");
    
    // GCP config
    operands.put("gcp.credentialsPath", "/path/to/gcp-credentials.json");
    operands.put("gcp.projectIds", "gcp-project1,gcp-project2");
    
    // AWS config
    operands.put("aws.accessKeyId", "aws-access-key");
    operands.put("aws.secretAccessKey", "aws-secret-key");
    operands.put("aws.region", "us-west-2");
    operands.put("aws.accountIds", "111111111111,222222222222");
    operands.put("aws.roleArn", "arn:aws:iam::{account-id}:role/CrossAccountRole");
    
    Schema schema = factory.create(parentSchema, name, operands);
    
    assertThat(schema, is(notNullValue()));
    assertThat(schema, instanceOf(CloudGovernanceSchema.class));
    
    CloudGovernanceSchema governanceSchema = (CloudGovernanceSchema) schema;
    assertThat(governanceSchema.getTableMap().size(), is(7)); // All 7 tables should be available
  }
}