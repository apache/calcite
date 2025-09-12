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
package org.apache.calcite.adapter.sec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Check what data is actually in the parquet files.
 */
@Tag("integration")
public class CheckParquetDataTest {

  @Test void checkActualData() throws Exception {
    String modelPath = CheckParquetDataTest.class.getResource("/test-aapl-msft-model.json").getPath();

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelPath;

    try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
         Statement stmt = conn.createStatement()) {

      // Query to see actual data
      String query =
        "SELECT * " +
        "FROM sec.financial_line_items " +
        "WHERE cik = '0000320193' " +
        "  AND filing_type = '10K' " +
        "  AND year = 2022 " +
        "LIMIT 5";

      System.out.println("Executing: " + query);

      try (ResultSet rs = stmt.executeQuery(query)) {
        ResultSetMetaData meta = rs.getMetaData();

        // Print column names
        System.out.println("\nColumns in financial_line_items:");
        for (int i = 1; i <= meta.getColumnCount(); i++) {
          System.out.println("  " + i + ". " + meta.getColumnName(i) + " (" + meta.getColumnTypeName(i) + ")");
        }

        // Print data
        System.out.println("\nSample data:");
        int row = 0;
        while (rs.next()) {
          System.out.println("Row " + (++row) + ":");
          for (int i = 1; i <= meta.getColumnCount(); i++) {
            String value = rs.getString(i);
            if (value != null && value.length() > 50) {
              value = value.substring(0, 47) + "...";
            }
            System.out.println("  " + meta.getColumnName(i) + " = " + value);
          }
          System.out.println();
        }

        if (row == 0) {
          System.out.println("No data found!");
        } else {
          System.out.println("Found " + row + " rows");
        }
      }
    }
  }
}
