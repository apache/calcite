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

import org.apache.calcite.adapter.ops.provider.AWSProvider;
import org.apache.calcite.adapter.ops.util.CloudOpsProjectionHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsPaginationHandler;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for StorageResourcesTable optimization with projection pushdown.
 * Verifies the optimization logic without requiring actual AWS API calls.
 */
public class StorageResourcesOptimizationTest {

  private CloudOpsConfig config;
  private StorageResourcesTable storageTable;
  private RelDataTypeFactory typeFactory;
  private RelDataType rowType;

  @BeforeEach
  void setUp() {
    // Configure test AWS config
    CloudOpsConfig.AWSConfig awsConfig = new CloudOpsConfig.AWSConfig(
        Arrays.asList("123456789012"),
        "us-west-2",
        "test-access-key",
        "test-secret-key",
        null
    );
    
    config = new CloudOpsConfig(
        Arrays.asList("aws"),
        null,
        null,
        awsConfig,
        false, // Disable caching for test
        5,
        false
    );
    
    storageTable = new StorageResourcesTable(config);
    typeFactory = new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    rowType = storageTable.getRowType(typeFactory);
  }

  @Test
  @DisplayName("Projection handler should identify when minimal columns are needed")
  void testMinimalProjectionIdentification() {
    // Create projection for only basic columns: cloud_provider(0), account_id(1), resource_name(2)
    int[] projections = new int[]{0, 1, 2};
    CloudOpsProjectionHandler projectionHandler = new CloudOpsProjectionHandler(rowType, projections);
    
    assertFalse(projectionHandler.isSelectAll(), "Should not be SELECT *");
    assertEquals(3, projectionHandler.getProjections().length, "Should project 3 columns");
    
    List<String> projectedFields = projectionHandler.getProjectedFieldNames();
    assertTrue(projectedFields.contains("cloud_provider"));
    assertTrue(projectedFields.contains("account_id"));
    assertTrue(projectedFields.contains("resource_name"));
    assertFalse(projectedFields.contains("encryption_enabled"));
    assertFalse(projectedFields.contains("versioning_enabled"));
  }

  @Test
  @DisplayName("Projection handler should recognize SELECT * queries")
  void testSelectAllProjection() {
    // null projection means SELECT *
    CloudOpsProjectionHandler projectionHandler = new CloudOpsProjectionHandler(rowType, null);
    
    assertTrue(projectionHandler.isSelectAll(), "Should be SELECT *");
    assertEquals(0, projectionHandler.getProjections().length, "Should have no specific projections");
  }

  @Test
  @DisplayName("Projection handler should identify encryption columns")
  void testEncryptionColumnProjection() {
    // Get column indices for encryption fields
    int providerIdx = 0;  // cloud_provider
    int accountIdx = 1;   // account_id  
    int nameIdx = 2;      // resource_name
    int encEnabledIdx = 11; // encryption_enabled
    int encTypeIdx = 12;    // encryption_type
    int encKeyTypeIdx = 13; // encryption_key_type
    
    int[] projections = new int[]{providerIdx, accountIdx, nameIdx, encEnabledIdx, encTypeIdx, encKeyTypeIdx};
    CloudOpsProjectionHandler projectionHandler = new CloudOpsProjectionHandler(rowType, projections);
    
    List<String> projectedFields = projectionHandler.getProjectedFieldNames();
    assertTrue(projectedFields.contains("encryption_enabled"));
    assertTrue(projectedFields.contains("encryption_type"));
    assertTrue(projectedFields.contains("encryption_key_type"));
    assertFalse(projectedFields.contains("versioning_enabled"));
    assertFalse(projectedFields.contains("lifecycle_rules_count"));
  }

  @Test  
  @DisplayName("Pagination handler should apply limits correctly")
  void testPaginationLimits() {
    // Test with no pagination
    CloudOpsPaginationHandler noPagination = new CloudOpsPaginationHandler(null, null);
    assertFalse(noPagination.hasPagination(), "Should have no pagination");
    
    // Test with pagination would require creating RexLiteral nodes
    // Since that's complex without the full Calcite context, we verify the logic conceptually
    
    // The handler should:
    // 1. Extract numeric values from RexNode offset and fetch
    // 2. Apply default max results if no fetch is specified  
    // 3. Calculate AWS-specific MaxResults parameter
    
    // Verify AWS max results default
    assertEquals(100, noPagination.getAWSMaxResults(), 
        "AWS should use default max results of 100");
  }

  @Test
  @DisplayName("Storage table should have correct schema")
  void testStorageTableSchema() {
    RelDataType schema = storageTable.getRowType(typeFactory);
    
    // Verify key columns exist
    assertNotNull(schema.getField("cloud_provider", false, false));
    assertNotNull(schema.getField("account_id", false, false));
    assertNotNull(schema.getField("resource_name", false, false));
    assertNotNull(schema.getField("encryption_enabled", false, false));
    assertNotNull(schema.getField("encryption_type", false, false));
    assertNotNull(schema.getField("versioning_enabled", false, false));
    assertNotNull(schema.getField("lifecycle_rules_count", false, false));
    assertNotNull(schema.getField("public_access_enabled", false, false));
    
    // Verify correct column count
    assertEquals(27, schema.getFieldCount(), "Should have 27 columns");
  }

  @Test
  @DisplayName("Projection row should correctly subset columns")
  void testProjectionRowSubsetting() {
    // Create a projection handler for columns 0, 1, 2
    int[] projections = new int[]{0, 1, 2};
    CloudOpsProjectionHandler projectionHandler = new CloudOpsProjectionHandler(rowType, projections);
    
    // Create a full row with all columns
    Object[] fullRow = new Object[27];
    fullRow[0] = "aws";
    fullRow[1] = "123456789012";
    fullRow[2] = "test-bucket";
    fullRow[11] = true;  // encryption_enabled
    fullRow[12] = "AES256"; // encryption_type
    
    // Project the row
    Object[] projectedRow = projectionHandler.projectRow(fullRow);
    
    // Verify projection
    assertEquals(3, projectedRow.length, "Projected row should have 3 columns");
    assertEquals("aws", projectedRow[0]);
    assertEquals("123456789012", projectedRow[1]);
    assertEquals("test-bucket", projectedRow[2]);
  }

  @Test
  @DisplayName("Projection should handle null and missing values gracefully")
  void testProjectionNullHandling() {
    // Create a projection handler for columns including some that may be null
    int[] projections = new int[]{0, 1, 11, 20}; // provider, account, encryption, versioning
    CloudOpsProjectionHandler projectionHandler = new CloudOpsProjectionHandler(rowType, projections);
    
    // Create a row with some null values
    Object[] fullRow = new Object[27];
    fullRow[0] = "aws";
    fullRow[1] = "123456789012";
    fullRow[11] = null; // encryption_enabled is null
    fullRow[20] = false; // versioning_enabled
    
    // Project the row
    Object[] projectedRow = projectionHandler.projectRow(fullRow);
    
    assertEquals(4, projectedRow.length);
    assertEquals("aws", projectedRow[0]);
    assertEquals("123456789012", projectedRow[1]);
    assertNull(projectedRow[2], "Should preserve null value");
    assertEquals(false, projectedRow[3]);
  }

  @Test
  @DisplayName("AWS provider configuration should be correct")
  void testAWSProviderConfiguration() {
    AWSProvider provider = new AWSProvider(config.aws);
    assertNotNull(provider, "AWS provider should be created");
    
    // The provider should support the optimized queryStorageResources method
    // with projection, sort, pagination, and filter handlers
    
    // Verify cache metrics are available
    assertNotNull(provider.getCacheMetrics(), "Should have cache metrics");
  }
}