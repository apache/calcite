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
package org.apache.calcite.adapter.file;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;

/**
 * Tests for custom separator support in CSV adapter.
 */
class CsvSeparatorTest {
  @Test void testPipeSeparatorViaJdbc() throws Exception {
    String path = new java.io.File("src/test/resources/pipe_separated.csv").getAbsolutePath().replace("\\", "\\\\");
    Properties info = new Properties();
    String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'TEST',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'TEST',\n"
        + "      type: 'map',\n"
        + "      tables: [\n"
        + "        {\n"
        + "          name: 'PIPE_SEPARATED',\n"
        + "          type: 'custom',\n"
        + "          factory: 'org.apache.calcite.adapter.file.CsvTableFactory',\n"
        + "          operand: {\n"
        + "            file: '" + path + "',\n"
        + "            separator: '\\t'\n"
        + "          }\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    info.put("model", "inline:" + model);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery("select * from \"PIPE_SEPARATED\"")) {

      ResultSetMetaData metaData = resultSet.getMetaData();
      int columnCount = metaData.getColumnCount();

      System.out.println("DEBUG: Detected Column Count: " + columnCount);
      for (int i = 1; i <= columnCount; i++) {
        System.out.println("DEBUG: Column " + i + ": " + metaData.getColumnName(i));
      }

    }
  }
}
