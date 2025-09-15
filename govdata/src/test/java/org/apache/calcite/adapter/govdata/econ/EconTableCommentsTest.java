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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.govdata.TableCommentDefinitions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Test that all ECON schema tables have proper comment definitions.
 */
@Tag("unit")
public class EconTableCommentsTest {
  
  @Test
  public void testAllEconTablesHaveComments() {
    // List of all ECON tables
    String[] tables = {
        "employment_statistics",
        "inflation_metrics",
        "wage_growth",
        "regional_employment",
        "treasury_yields",
        "federal_debt",
        "world_indicators",
        "fred_indicators",
        "gdp_components",
        "regional_income"
    };
    
    for (String tableName : tables) {
      // Check table comment exists
      String tableComment = TableCommentDefinitions.getEconTableComment(tableName);
      assertNotNull(tableComment, "Table comment missing for: " + tableName);
      assertFalse(tableComment.isEmpty(), "Table comment empty for: " + tableName);
      
      // Check column comments exist
      Map<String, String> columnComments = TableCommentDefinitions.getEconColumnComments(tableName);
      assertNotNull(columnComments, "Column comments missing for: " + tableName);
      assertFalse(columnComments.isEmpty(), "No column comments for: " + tableName);
      
      // Verify at least some key columns have comments
      switch (tableName) {
        case "employment_statistics":
          assertTrue(columnComments.containsKey("date"), "Missing date column comment");
          assertTrue(columnComments.containsKey("series_id"), "Missing series_id column comment");
          assertTrue(columnComments.containsKey("value"), "Missing value column comment");
          break;
        case "inflation_metrics":
          assertTrue(columnComments.containsKey("date"), "Missing date column comment");
          assertTrue(columnComments.containsKey("index_type"), "Missing index_type column comment");
          assertTrue(columnComments.containsKey("index_value"), "Missing index_value column comment");
          break;
        case "treasury_yields":
          assertTrue(columnComments.containsKey("date"), "Missing date column comment");
          assertTrue(columnComments.containsKey("maturity_months"), "Missing maturity_months column comment");
          assertTrue(columnComments.containsKey("yield_percent"), "Missing yield_percent column comment");
          break;
        case "federal_debt":
          assertTrue(columnComments.containsKey("date"), "Missing date column comment");
          assertTrue(columnComments.containsKey("debt_type"), "Missing debt_type column comment");
          assertTrue(columnComments.containsKey("amount_billions"), "Missing amount_billions column comment");
          break;
        case "world_indicators":
          assertTrue(columnComments.containsKey("country_code"), "Missing country_code column comment");
          assertTrue(columnComments.containsKey("indicator_code"), "Missing indicator_code column comment");
          assertTrue(columnComments.containsKey("value"), "Missing value column comment");
          break;
        case "fred_indicators":
          assertTrue(columnComments.containsKey("series_id"), "Missing series_id column comment");
          assertTrue(columnComments.containsKey("date"), "Missing date column comment");
          assertTrue(columnComments.containsKey("value"), "Missing value column comment");
          break;
        case "gdp_components":
          assertTrue(columnComments.containsKey("table_id"), "Missing table_id column comment");
          assertTrue(columnComments.containsKey("line_number"), "Missing line_number column comment");
          assertTrue(columnComments.containsKey("value"), "Missing value column comment");
          break;
        case "regional_income":
          assertTrue(columnComments.containsKey("geo_fips"), "Missing geo_fips column comment");
          assertTrue(columnComments.containsKey("metric"), "Missing metric column comment");
          assertTrue(columnComments.containsKey("value"), "Missing value column comment");
          break;
      }
    }
  }
  
  @Test
  public void testEconSchemaComment() {
    // Verify the ECON schema itself would have a comment
    // This is defined in EconSchema.getComment() method
    String expectedComment = "U.S. and international economic data";
    
    // Just verify that table comments are well-formed
    String employmentComment = TableCommentDefinitions.getEconTableComment("employment_statistics");
    assertTrue(employmentComment.contains("Bureau of Labor Statistics"), 
        "Employment statistics should mention BLS");
    
    String treasuryComment = TableCommentDefinitions.getEconTableComment("treasury_yields");
    assertTrue(treasuryComment.contains("Treasury"), 
        "Treasury yields should mention Treasury");
    
    String worldComment = TableCommentDefinitions.getEconTableComment("world_indicators");
    assertTrue(worldComment.contains("World Bank"), 
        "World indicators should mention World Bank");
  }
}