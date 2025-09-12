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
package org.apache.calcite.adapter.govdata.sec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test that Calcite can find and use SecSchemaFactory.
 */
@Tag("unit")
public class CalciteSchemaFactoryTest {

  @Test public void testFactoryIsFoundByCalcite() throws Exception {
    // Create a simple model JSON that uses SecSchemaFactory
    String modelJson = "{\n"
  +
        "  \"version\": \"1.0\",\n"
  +
        "  \"defaultSchema\": \"TEST\",\n"
  +
        "  \"schemas\": [{\n"
  +
        "    \"name\": \"TEST\",\n"
  +
        "    \"type\": \"custom\",\n"
  +
        "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\",\n"
  +
        "    \"operand\": {\n"
  +
        "      \"directory\": \"/tmp/test-sec-" + System.currentTimeMillis() + "\",\n"
  +
        "      \"autoDownload\": false\n"
  +
        "    }\n"
  +
        "  }]\n"
  +
        "}";

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("inline", modelJson);

    // This should cause Calcite to load SecSchemaFactory
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props)) {
      assertNotNull(conn);
      System.err.println("Connection created successfully");

      // Try to get metadata to force schema loading
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT * FROM information_schema.tables")) {
        System.err.println("Query executed, tables found:");
        while (rs.next()) {
          System.err.println("  - " + rs.getString("table_name"));
        }
      }
    }
  }
}
