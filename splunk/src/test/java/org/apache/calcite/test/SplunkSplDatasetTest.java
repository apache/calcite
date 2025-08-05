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

import org.apache.calcite.adapter.splunk.DataModelDiscovery;
import org.apache.calcite.adapter.splunk.SplunkTable;
import org.apache.calcite.adapter.splunk.search.SplunkConnectionImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * Test that shows the actual SPL dataset names discovered from data models.
 */
@Tag("integration")
class SplunkSplDatasetTest {
  public static final String SPLUNK_URL = "https://kentest.xyz:8089";
  public static final String SPLUNK_USER = "admin";
  public static final String SPLUNK_PASSWORD = "admin123";

  @Test void testSplDatasetNames() throws Exception {
    // Create connection
    SplunkConnectionImpl connection = new SplunkConnectionImpl(SPLUNK_URL, SPLUNK_USER, SPLUNK_PASSWORD, true);

    // Create discovery instance
    DataModelDiscovery discovery = new DataModelDiscovery(connection, "Splunk_SA_CIM", 0);

    // Get discovered tables
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    Map<String, Table> tables = discovery.discoverDataModels(typeFactory, null);

    System.out.println("=== Data Models and Their SPL Dataset Names ===\n");
    System.out.println("Data Model Name          SPL Dataset Name         Search String");
    System.out.println("---------------          ----------------         -------------");

    for (Map.Entry<String, Table> entry : tables.entrySet()) {
      String tableName = entry.getKey();
      Table table = entry.getValue();

      if (table instanceof SplunkTable) {
        SplunkTable splunkTable = (SplunkTable) table;

        // Use reflection to get the search string which contains the dataset name
        Field searchStringField = SplunkTable.class.getDeclaredField("searchString");
        searchStringField.setAccessible(true);
        String searchString = (String) searchStringField.get(splunkTable);

        // Parse the search string to extract data model and dataset names
        // Format: "| datamodel {DataModelName} {DatasetName} search"
        String[] parts = searchString.split("\\s+");
        if (parts.length >= 4 && "datamodel".equals(parts[1])) {
          String dataModelName = parts[2];
          String datasetName = parts[3];

          System.out.printf("%-24s %-24s %s%n", dataModelName, datasetName, searchString);
        } else {
          System.out.printf("%-24s %-24s %s%n", tableName, "Unknown", searchString);
        }
      }
    }

    System.out.println("\n=== SPL Usage Examples ===\n");
    System.out.println("Each dataset can be queried directly in SPL using:");
    System.out.println("| datamodel Authentication Authentication search");
    System.out.println("| datamodel Network_Traffic All_Traffic search");
    System.out.println("| datamodel Web Web search");
    System.out.println("| datamodel Malware Malware_Attacks search");
    System.out.println("\nThe adapter translates SQL queries to these datamodel commands automatically.");

    // Connection cleanup handled automatically
  }
}
