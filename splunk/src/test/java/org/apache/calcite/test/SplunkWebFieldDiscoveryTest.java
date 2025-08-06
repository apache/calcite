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
import org.apache.calcite.adapter.splunk.search.SplunkConnectionImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.net.URL;
import java.util.Map;
import java.util.Properties;

/**
 * Test to show exactly what fields are discovered for the web data model.
 */
@Tag("integration")
public class SplunkWebFieldDiscoveryTest {


  @Test public void testWebModelFieldDiscovery() throws Exception {
    Properties props = new Properties();

    // Load from local-properties.settings file
    File propsFile = new File("local-properties.settings");
    if (!propsFile.exists()) {
      propsFile = new File("../local-properties.settings");
    }

    if (!propsFile.exists()) {
      System.out.println("Skipping test: Could not find local-properties.settings");
      return;
    }

    try (FileInputStream fis = new FileInputStream(propsFile)) {
      props.load(fis);
    }

    String urlStr = props.getProperty("splunk.url");
    String username = props.getProperty("splunk.username");
    String password = props.getProperty("splunk.password");

    if (urlStr == null || username == null || password == null) {
      System.out.println("Skipping test: Required Splunk connection properties not found");
      return;
    }

    URL url = URI.create(urlStr).toURL();
    boolean disableSsl = "true".equals(props.getProperty("splunk.ssl.insecure"));

    // Create connection
    SplunkConnectionImpl connection = new SplunkConnectionImpl(url, username, password, disableSsl);

    // Create data model discovery with no cache
    DataModelDiscovery discovery = new DataModelDiscovery(connection, "Splunk_SA_CIM", 0);

    // Discover data models, filtering to just "web"
    SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    Map<String, Table> tables = discovery.discoverDataModels(typeFactory, "web");

    System.out.println("=== Web Data Model Field Discovery ===\n");
    System.out.println("Number of tables discovered: " + tables.size());

    if (tables.containsKey("web")) {
      Table webTable = tables.get("web");
      RelDataType rowType = webTable.getRowType(typeFactory);

      System.out.println("\nFields in 'web' data model:");
      System.out.println("Field Name                Type              Nullable");
      System.out.println("---------                ----              --------");

      for (RelDataTypeField field : rowType.getFieldList()) {
        String fieldName = field.getName();
        String typeName = field.getType().getSqlTypeName().toString();
        boolean nullable = field.getType().isNullable();

        System.out.printf("%-25s %-17s %s%n", fieldName, typeName, nullable ? "YES" : "NO");
      }

      System.out.println("\nTotal fields: " + rowType.getFieldCount());

      // Check for specific fields
      boolean hasStatus = rowType.getField("status", false, false) != null;
      boolean hasBytes = rowType.getField("bytes", false, false) != null;
      boolean hasMethod = rowType.getField("method", false, false) != null;
      boolean hasUriPath = rowType.getField("uri_path", false, false) != null;

      System.out.println("\nField presence check:");
      System.out.println("  status: " + hasStatus);
      System.out.println("  bytes: " + hasBytes);
      System.out.println("  method: " + hasMethod);
      System.out.println("  uri_path: " + hasUriPath);
    } else {
      System.out.println("Web table not found in discovered tables!");
      System.out.println("Available tables: " + tables.keySet());
    }
  }
}
