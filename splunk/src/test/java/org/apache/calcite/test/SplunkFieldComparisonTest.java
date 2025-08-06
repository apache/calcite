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
package org.apache.calcite.test;

import org.apache.calcite.adapter.splunk.CimModelBuilder;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.*;

/**
 * Compare field definitions between dynamically discovered models and hardcoded CIM models.
 * Run with: -Dcalcite.test.splunk=true
 */
@Tag("integration")
class SplunkFieldComparisonTest {


  @Test void compareFieldDefinitions() throws Exception {
    System.out.println("\n=== Comparing Field Definitions Between Dynamic and Hardcoded Models ===");

    // Models to compare
    String[] modelsToCompare = {"authentication", "web", "network_traffic"};

    // Get field info from dynamic discovery
    Properties info = new Properties();
    info.put("model", Sources.of(SplunkFieldComparisonTest.class.getResource("/test-splunk-model.json")).file().getAbsolutePath());

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info)) {
      DatabaseMetaData metaData = conn.getMetaData();

      for (String modelName : modelsToCompare) {
        System.out.println("\n\n=== Model: " + modelName.toUpperCase() + " ===");

        // Get fields from dynamic discovery
        Map<String, String> discoveredFields = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        try (ResultSet rs = metaData.getColumns(null, "splunk", modelName, null)) {
          while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME");
            String typeName = rs.getString("TYPE_NAME");
            int nullable = rs.getInt("NULLABLE");
            discoveredFields.put(columnName, typeName + (nullable == 1 ? " NULL" : " NOT NULL"));
          }
        }

        // Get fields from hardcoded model
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        CimModelBuilder.CimSchemaResult result = CimModelBuilder.buildCimSchemaWithMapping(typeFactory, modelName);
        RelDataType schema = result.getSchema();
        Map<String, String> fieldMapping = result.getFieldMapping();
        String searchString = result.getSearchString();

        Map<String, String> hardcodedFields = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (RelDataTypeField field : schema.getFieldList()) {
          String fieldName = field.getName();
          SqlTypeName sqlType = field.getType().getSqlTypeName();
          boolean nullable = field.getType().isNullable();
          hardcodedFields.put(fieldName, sqlType.getName() + (nullable ? " NULL" : " NOT NULL"));
        }

        // Compare
        System.out.println("\nSearch String (hardcoded): " + searchString);
        System.out.println("\nField Mappings (hardcoded): " + fieldMapping.size() + " mappings");

        System.out.println("\nHardcoded Fields (" + hardcodedFields.size() + "):");
        for (Map.Entry<String, String> entry : hardcodedFields.entrySet()) {
          System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }

        System.out.println("\nDiscovered Fields (" + discoveredFields.size() + "):");
        for (Map.Entry<String, String> entry : discoveredFields.entrySet()) {
          System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }

        // Find differences
        Set<String> onlyInHardcoded = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        onlyInHardcoded.addAll(hardcodedFields.keySet());
        onlyInHardcoded.removeAll(discoveredFields.keySet());

        Set<String> onlyInDiscovered = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        onlyInDiscovered.addAll(discoveredFields.keySet());
        onlyInDiscovered.removeAll(hardcodedFields.keySet());

        Set<String> commonFields = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        commonFields.addAll(hardcodedFields.keySet());
        commonFields.retainAll(discoveredFields.keySet());

        System.out.println("\nFields only in hardcoded (" + onlyInHardcoded.size() + "):");
        for (String field : onlyInHardcoded) {
          System.out.println("  - " + field + " (" + hardcodedFields.get(field) + ")");
        }

        System.out.println("\nFields only in discovered (" + onlyInDiscovered.size() + "):");
        for (String field : onlyInDiscovered) {
          System.out.println("  - " + field + " (" + discoveredFields.get(field) + ")");
        }

        System.out.println("\nType mismatches in common fields:");
        int mismatches = 0;
        for (String field : commonFields) {
          String hardcodedType = hardcodedFields.get(field);
          String discoveredType = discoveredFields.get(field);
          if (!hardcodedType.equals(discoveredType)) {
            System.out.println("  - " + field + ": hardcoded=" + hardcodedType + ", discovered=" + discoveredType);
            mismatches++;
          }
        }
        if (mismatches == 0) {
          System.out.println("  (none - all common fields have matching types)");
        }

        System.out.println("\nSummary for " + modelName + ":");
        System.out.println("  Common fields: " + commonFields.size());
        System.out.println("  Only in hardcoded: " + onlyInHardcoded.size());
        System.out.println("  Only in discovered: " + onlyInDiscovered.size());
        System.out.println("  Type mismatches: " + mismatches);
      }
    }
  }
}
