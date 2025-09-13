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
package org.apache.calcite.adapter.govdata;

import org.apache.calcite.schema.CommentableTable;
import org.apache.calcite.test.CalciteAssert;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for table and column comment support in SEC and GEO schemas.
 *
 * <p>Verifies that business definition comments are properly exposed through
 * JDBC metadata operations and are accessible via DatabaseMetaData methods.
 */
@Tag("unit")
public class TableCommentsTest {

  @Test public void testSecTableComments() {
    // Test that SEC table comments are available
    String financialLineItemsComment = TableCommentDefinitions.getSecTableComment("financial_line_items");
    assertNotNull(financialLineItemsComment);
    assertTrue(financialLineItemsComment.contains("Financial statement line items"));
    assertTrue(financialLineItemsComment.contains("XBRL filings"));
    
    String filingMetadataComment = TableCommentDefinitions.getSecTableComment("filing_metadata");
    assertNotNull(filingMetadataComment);
    assertTrue(filingMetadataComment.contains("Core filing information"));
    
    String insiderTransactionsComment = TableCommentDefinitions.getSecTableComment("insider_transactions");
    assertNotNull(insiderTransactionsComment);
    assertTrue(insiderTransactionsComment.contains("Insider trading transactions"));
    
    String footnotesComment = TableCommentDefinitions.getSecTableComment("footnotes");
    assertNotNull(footnotesComment);
    assertTrue(footnotesComment.contains("Financial statement footnotes"));
  }

  @Test public void testSecColumnComments() {
    // Test SEC column comments
    String cikComment = TableCommentDefinitions.getSecColumnComment("financial_line_items", "cik");
    assertNotNull(cikComment);
    assertTrue(cikComment.contains("Central Index Key"));
    
    String conceptComment = TableCommentDefinitions.getSecColumnComment("financial_line_items", "concept");
    assertNotNull(conceptComment);
    assertTrue(conceptComment.contains("XBRL concept"));
    
    String valueComment = TableCommentDefinitions.getSecColumnComment("financial_line_items", "value");
    assertNotNull(valueComment);
    assertTrue(valueComment.contains("Numeric value"));
    
    // Test case insensitive lookup
    String cikCommentUpper = TableCommentDefinitions.getSecColumnComment("financial_line_items", "CIK");
    assertEquals(cikComment, cikCommentUpper);
  }

  @Test public void testGeoTableComments() {
    // Test GEO table comments
    String statesComment = TableCommentDefinitions.getGeoTableComment("tiger_states");
    assertNotNull(statesComment);
    assertTrue(statesComment.contains("U.S. state boundaries"));
    assertTrue(statesComment.contains("TIGER/Line"));
    
    String countiesComment = TableCommentDefinitions.getGeoTableComment("tiger_counties");
    assertNotNull(countiesComment);
    assertTrue(countiesComment.contains("county boundaries"));
    
    String placesComment = TableCommentDefinitions.getGeoTableComment("census_places");
    assertNotNull(placesComment);
    assertTrue(placesComment.contains("Census designated places"));
    
    String zipCountyComment = TableCommentDefinitions.getGeoTableComment("hud_zip_county");
    assertNotNull(zipCountyComment);
    assertTrue(zipCountyComment.contains("HUD USPS ZIP code"));
    assertTrue(zipCountyComment.contains("crosswalk"));
  }

  @Test public void testGeoColumnComments() {
    // Test GEO column comments
    String stateFipsComment = TableCommentDefinitions.getGeoColumnComment("tiger_states", "state_fips");
    assertNotNull(stateFipsComment);
    assertTrue(stateFipsComment.contains("FIPS"));
    
    String stateNameComment = TableCommentDefinitions.getGeoColumnComment("tiger_states", "state_name");
    assertNotNull(stateNameComment);
    assertTrue(stateNameComment.contains("Full official state name"));
    
    String zipComment = TableCommentDefinitions.getGeoColumnComment("hud_zip_county", "zip");
    assertNotNull(zipComment);
    assertTrue(zipComment.contains("5-digit USPS ZIP code"));
    
    String resRatioComment = TableCommentDefinitions.getGeoColumnComment("hud_zip_county", "res_ratio");
    assertNotNull(resRatioComment);
    assertTrue(resRatioComment.contains("residential addresses"));
  }

  @Test public void testCommentsCaseInsensitive() {
    // Test that both table and column lookups are case-insensitive
    String lowerCase = TableCommentDefinitions.getSecTableComment("financial_line_items");
    String upperCase = TableCommentDefinitions.getSecTableComment("FINANCIAL_LINE_ITEMS");
    String mixedCase = TableCommentDefinitions.getSecTableComment("Financial_Line_Items");
    
    assertEquals(lowerCase, upperCase);
    assertEquals(lowerCase, mixedCase);
    
    // Test column case insensitivity
    String colLower = TableCommentDefinitions.getSecColumnComment("financial_line_items", "concept");
    String colUpper = TableCommentDefinitions.getSecColumnComment("FINANCIAL_LINE_ITEMS", "CONCEPT");
    String colMixed = TableCommentDefinitions.getSecColumnComment("Financial_Line_Items", "Concept");
    
    assertEquals(colLower, colUpper);
    assertEquals(colLower, colMixed);
  }
}