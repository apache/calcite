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

import org.apache.calcite.adapter.governance.categories.PerformanceTest;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Performance tests for Cloud Governance adapter.
 * 
 * These tests measure execution time, memory usage, and throughput
 * under various load conditions.
 */
@Category(PerformanceTest.class)
public class CloudGovernancePerformanceTest {

  @Test
  public void testSchemaCreationPerformance() {
    CloudGovernanceConfig config = createTestConfig();
    
    // Measure schema creation time
    long startTime = System.currentTimeMillis();
    
    for (int i = 0; i < 1000; i++) {
      CloudGovernanceSchema schema = new CloudGovernanceSchema(config);
      assertThat(schema != null, is(true));
    }
    
    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;
    
    System.out.println("Created 1000 schemas in " + duration + "ms");
    
    // Performance assertion - should create 1000 schemas in less than 1 second
    assertThat("Schema creation should be fast", duration < 1000, is(true));
  }

  @Test
  public void testTableRowTypeCreationPerformance() {
    CloudGovernanceConfig config = createTestConfig();
    KubernetesClustersTable table = new KubernetesClustersTable(config);
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(
        org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    
    // Measure row type creation time
    long startTime = System.currentTimeMillis();
    
    for (int i = 0; i < 10000; i++) {
      RelDataType rowType = table.getRowType(typeFactory);
      assertThat(rowType.getFieldCount() > 0, is(true));
    }
    
    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;
    
    System.out.println("Created 10000 row types in " + duration + "ms");
    
    // Performance assertion - should create 10000 row types in less than 2 seconds
    assertThat("Row type creation should be fast", duration < 2000, is(true));
  }

  @Test
  public void testConfigurationPerformance() {
    // Measure configuration object creation time
    long startTime = System.currentTimeMillis();
    
    for (int i = 0; i < 100000; i++) {
      CloudGovernanceConfig config = createTestConfig();
      assertThat(config != null, is(true));
    }
    
    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;
    
    System.out.println("Created 100000 configurations in " + duration + "ms");
    
    // Performance assertion - should create 100000 configs in less than 3 seconds
    assertThat("Configuration creation should be fast", duration < 3000, is(true));
  }

  @Test
  public void testMemoryUsage() {
    // Force garbage collection to get baseline
    System.gc();
    long beforeMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    
    // Create many objects to test memory usage
    CloudGovernanceConfig[] configs = new CloudGovernanceConfig[10000];
    for (int i = 0; i < configs.length; i++) {
      configs[i] = createTestConfig();
    }
    
    // Measure memory after allocation
    long afterMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    long memoryUsed = afterMemory - beforeMemory;
    
    System.out.println("Memory used for 10000 configurations: " + (memoryUsed / 1024) + " KB");
    
    // Performance assertion - should use less than 50MB for 10000 configs
    assertThat("Memory usage should be reasonable", memoryUsed < 50 * 1024 * 1024, is(true));
    
    // Keep reference to prevent GC during test
    assertThat(configs.length, is(10000));
  }

  private CloudGovernanceConfig createTestConfig() {
    CloudGovernanceConfig.AzureConfig azure = new CloudGovernanceConfig.AzureConfig(
        "test-tenant", "test-client", "test-secret", Arrays.asList("sub1", "sub2"));
    
    CloudGovernanceConfig.GCPConfig gcp = new CloudGovernanceConfig.GCPConfig(
        Arrays.asList("project1", "project2"), "/path/to/credentials.json");
    
    CloudGovernanceConfig.AWSConfig aws = new CloudGovernanceConfig.AWSConfig(
        Arrays.asList("account1", "account2"), "us-east-1", "test-key", "test-secret", null);
    
    return new CloudGovernanceConfig(
        Arrays.asList("azure", "gcp", "aws"), azure, gcp, aws, true, 15);
  }
}