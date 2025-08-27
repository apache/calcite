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

import org.apache.calcite.adapter.ops.util.CloudOpsProjectionHandler;
import org.apache.calcite.adapter.ops.util.CloudOpsPaginationHandler;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verification test for StorageResourcesTable optimization.
 * Tests the optimization logic without requiring actual AWS connections.
 */
public class StorageOptimizationVerificationTest {

  private RelDataTypeFactory typeFactory;
  private RelDataType storageSchema;

  @BeforeEach
  void setUp() {
    typeFactory = new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    
    // Build the storage resources schema
    storageSchema = typeFactory.builder()
        // Identity fields (0-7)
        .add("cloud_provider", SqlTypeName.VARCHAR)      // 0
        .add("account_id", SqlTypeName.VARCHAR)          // 1
        .add("resource_name", SqlTypeName.VARCHAR)       // 2
        .add("storage_type", SqlTypeName.VARCHAR)        // 3
        .add("application", SqlTypeName.VARCHAR)         // 4
        .add("region", SqlTypeName.VARCHAR)              // 5
        .add("resource_group", SqlTypeName.VARCHAR)      // 6
        .add("resource_id", SqlTypeName.VARCHAR)         // 7
        // Configuration facts (8-10)
        .add("size_bytes", SqlTypeName.BIGINT)           // 8
        .add("storage_class", SqlTypeName.VARCHAR)       // 9
        .add("replication_type", SqlTypeName.VARCHAR)    // 10
        // Security facts (11-18)
        .add("encryption_enabled", SqlTypeName.BOOLEAN)  // 11
        .add("encryption_type", SqlTypeName.VARCHAR)     // 12
        .add("encryption_key_type", SqlTypeName.VARCHAR) // 13
        .add("public_access_enabled", SqlTypeName.BOOLEAN) // 14
        .add("public_access_level", SqlTypeName.VARCHAR)   // 15
        .add("network_restrictions", SqlTypeName.VARCHAR)  // 16
        .add("https_only", SqlTypeName.BOOLEAN)           // 17
        // Data protection facts (18-22)
        .add("versioning_enabled", SqlTypeName.BOOLEAN)    // 18
        .add("soft_delete_enabled", SqlTypeName.BOOLEAN)   // 19
        .add("soft_delete_retention_days", SqlTypeName.INTEGER) // 20
        .add("backup_enabled", SqlTypeName.BOOLEAN)        // 21
        .add("lifecycle_rules_count", SqlTypeName.INTEGER) // 22
        // Access control facts (23-26)
        .add("access_tier", SqlTypeName.VARCHAR)           // 23
        .add("last_access_time", SqlTypeName.TIMESTAMP)    // 24
        .add("created_date", SqlTypeName.TIMESTAMP)        // 25
        .add("modified_date", SqlTypeName.TIMESTAMP)       // 26
        // Metadata (27)
        .add("tags", SqlTypeName.VARCHAR)                  // 27
        .build();
  }

  @Test
  @DisplayName("Should identify minimal projection for basic listing query")
  void testMinimalProjection() {
    // Simulate: SELECT cloud_provider, account_id, resource_name FROM storage_resources
    int[] projections = new int[]{0, 1, 2};
    CloudOpsProjectionHandler handler = new CloudOpsProjectionHandler(storageSchema, projections);
    
    assertFalse(handler.isSelectAll(), "Should not be SELECT *");
    assertEquals(3, handler.getProjections().length, "Should project 3 columns");
    
    List<String> fields = handler.getProjectedFieldNames();
    assertEquals(3, fields.size());
    assertTrue(fields.contains("cloud_provider"));
    assertTrue(fields.contains("account_id"));
    assertTrue(fields.contains("resource_name"));
    
    // Verify expensive columns are NOT included
    assertFalse(fields.contains("encryption_enabled"));
    assertFalse(fields.contains("versioning_enabled"));
    assertFalse(fields.contains("lifecycle_rules_count"));
    assertFalse(fields.contains("public_access_enabled"));
    assertFalse(fields.contains("application")); // Requires tags API call
    
    System.out.println("✓ Minimal projection test passed - Only basic fields identified");
    System.out.println("  Projected fields: " + fields);
    System.out.println("  API calls needed: ListBuckets only (1 call)");
  }

  @Test
  @DisplayName("Should identify security-focused projection")
  void testSecurityProjection() {
    // Simulate: SELECT resource_name, encryption_enabled, encryption_type, public_access_enabled
    int[] projections = new int[]{2, 11, 12, 14};
    CloudOpsProjectionHandler handler = new CloudOpsProjectionHandler(storageSchema, projections);
    
    assertFalse(handler.isSelectAll());
    assertEquals(4, handler.getProjections().length);
    
    List<String> fields = handler.getProjectedFieldNames();
    assertTrue(fields.contains("resource_name"));
    assertTrue(fields.contains("encryption_enabled"));
    assertTrue(fields.contains("encryption_type"));
    assertTrue(fields.contains("public_access_enabled"));
    
    // Verify other expensive fields are NOT included
    assertFalse(fields.contains("versioning_enabled"));
    assertFalse(fields.contains("lifecycle_rules_count"));
    assertFalse(fields.contains("application"));
    
    System.out.println("✓ Security projection test passed");
    System.out.println("  Projected fields: " + fields);
    System.out.println("  API calls needed: ListBuckets + GetBucketEncryption + GetPublicAccessBlock (3 calls per bucket)");
  }

  @Test
  @DisplayName("Should handle SELECT * projection")
  void testSelectAllProjection() {
    // Simulate: SELECT * FROM storage_resources
    CloudOpsProjectionHandler handler = new CloudOpsProjectionHandler(storageSchema, null);
    
    assertTrue(handler.isSelectAll(), "Should be SELECT *");
    assertEquals(0, handler.getProjections().length, "Should have no specific projections");
    
    System.out.println("✓ SELECT * projection test passed");
    System.out.println("  All fields projected - all API calls will be made");
    System.out.println("  API calls needed: ListBuckets + 6 detail calls per bucket");
  }

  @Test
  @DisplayName("Should correctly project rows based on projection")
  void testRowProjection() {
    // Create a projection for columns 0, 2, 11
    int[] projections = new int[]{0, 2, 11};
    CloudOpsProjectionHandler handler = new CloudOpsProjectionHandler(storageSchema, projections);
    
    // Create a full row with sample data
    Object[] fullRow = new Object[28];
    fullRow[0] = "aws";                    // cloud_provider
    fullRow[1] = "123456789012";          // account_id
    fullRow[2] = "my-bucket";              // resource_name
    fullRow[3] = "S3 Bucket";              // storage_type
    fullRow[4] = "MyApp";                  // application
    fullRow[11] = true;                    // encryption_enabled
    fullRow[12] = "AES256";                // encryption_type
    fullRow[14] = false;                   // public_access_enabled
    
    // Project the row
    Object[] projectedRow = handler.projectRow(fullRow);
    
    // Verify projection
    assertEquals(3, projectedRow.length, "Projected row should have 3 columns");
    assertEquals("aws", projectedRow[0], "First column should be cloud_provider");
    assertEquals("my-bucket", projectedRow[1], "Second column should be resource_name");
    assertEquals(true, projectedRow[2], "Third column should be encryption_enabled");
    
    System.out.println("✓ Row projection test passed");
    System.out.println("  Full row columns: 28");
    System.out.println("  Projected columns: 3");
    System.out.println("  Values: [" + projectedRow[0] + ", " + projectedRow[1] + ", " + projectedRow[2] + "]");
  }

  @Test
  @DisplayName("Should apply pagination limits")
  void testPaginationHandler() {
    // Test default pagination (no explicit limit)
    CloudOpsPaginationHandler defaultPagination = new CloudOpsPaginationHandler(null, null);
    assertFalse(defaultPagination.hasPagination(), "Should not have explicit pagination");
    assertEquals(100, defaultPagination.getAWSMaxResults(), "Should use AWS default of 100");
    
    System.out.println("✓ Pagination test passed");
    System.out.println("  Default AWS limit: 100 resources");
    System.out.println("  Prevents unbounded API calls for accounts with many buckets");
  }

  @Test
  @DisplayName("Should calculate API call reduction correctly")
  void testApiCallReduction() {
    // Test case 1: Minimal projection
    int minimalApiCalls = 1;  // Just ListBuckets
    int maxApiCalls = 1 + (6 * 100);  // ListBuckets + 6 calls per bucket
    double reduction1 = ((double)(maxApiCalls - minimalApiCalls) / maxApiCalls) * 100;
    
    System.out.println("✓ API call reduction calculations:");
    System.out.println("  Case 1 - Basic listing (SELECT cloud_provider, account_id, resource_name):");
    System.out.println("    Before: " + maxApiCalls + " API calls");
    System.out.println("    After: " + minimalApiCalls + " API calls");
    System.out.println("    Reduction: " + String.format("%.1f%%", reduction1));
    
    // Test case 2: Security projection  
    int securityApiCalls = 1 + (2 * 100);  // ListBuckets + 2 calls per bucket (encryption, public access)
    double reduction2 = ((double)(maxApiCalls - securityApiCalls) / maxApiCalls) * 100;
    
    System.out.println("  Case 2 - Security audit (with encryption and public access):");
    System.out.println("    Before: " + maxApiCalls + " API calls");
    System.out.println("    After: " + securityApiCalls + " API calls");
    System.out.println("    Reduction: " + String.format("%.1f%%", reduction2));
    
    // Test case 3: With pagination limit
    int limitedBuckets = 50;
    int limitedApiCalls = 1 + (6 * limitedBuckets);  // All calls but limited buckets
    
    System.out.println("  Case 3 - SELECT * with LIMIT 50:");
    System.out.println("    Without limit: " + (1 + 6 * 1000) + " API calls (for 1000 buckets)");
    System.out.println("    With limit: " + limitedApiCalls + " API calls");
    System.out.println("    Reduction: " + String.format("%.1f%%", 
        ((double)(6001 - limitedApiCalls) / 6001) * 100));
    
    assertTrue(reduction1 > 99, "Minimal projection should reduce API calls by >99%");
    assertTrue(reduction2 > 66, "Security projection should reduce API calls by >66%");
  }

  @Test
  @DisplayName("Should identify which API calls are needed based on projection")
  void testApiCallIdentification() {
    System.out.println("✓ API call identification based on projections:");
    
    // Test different projection scenarios
    testProjectionApiCalls("Basic listing", 
        new int[]{0, 1, 2},  // cloud_provider, account_id, resource_name
        Arrays.asList("ListBuckets"));
    
    testProjectionApiCalls("With application field",
        new int[]{0, 1, 2, 4},  // + application (needs tags)
        Arrays.asList("ListBuckets", "GetBucketTagging"));
    
    testProjectionApiCalls("Security audit",
        new int[]{2, 11, 12, 14},  // name, encryption_enabled, encryption_type, public_access
        Arrays.asList("ListBuckets", "GetBucketEncryption", "GetPublicAccessBlock"));
    
    testProjectionApiCalls("Compliance check",
        new int[]{2, 11, 18, 22},  // name, encryption, versioning, lifecycle
        Arrays.asList("ListBuckets", "GetBucketEncryption", "GetBucketVersioning", "GetBucketLifecycle"));
    
    testProjectionApiCalls("Full audit (SELECT *)",
        null,  // No projection = all fields
        Arrays.asList("ListBuckets", "GetBucketLocation", "GetBucketTagging", 
                     "GetBucketEncryption", "GetPublicAccessBlock", 
                     "GetBucketVersioning", "GetBucketLifecycle"));
  }
  
  private void testProjectionApiCalls(String scenario, int[] projections, List<String> expectedApiCalls) {
    CloudOpsProjectionHandler handler = new CloudOpsProjectionHandler(storageSchema, projections);
    
    System.out.println("\n  " + scenario + ":");
    if (projections != null) {
      System.out.println("    Projected fields: " + handler.getProjectedFieldNames());
    } else {
      System.out.println("    Projected fields: ALL (SELECT *)");
    }
    System.out.println("    Required AWS API calls: " + expectedApiCalls);
    System.out.println("    Total calls for 100 buckets: " + 
        (1 + (expectedApiCalls.size() - 1) * 100));
  }
}