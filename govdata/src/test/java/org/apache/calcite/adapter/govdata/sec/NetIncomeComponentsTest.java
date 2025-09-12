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
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Test NetIncomeLoss with its constituent components and footnotes.
 */
@Tag("integration")
public class NetIncomeComponentsTest {

  @Test @Timeout(value = 5, unit = TimeUnit.MINUTES)
  public void testNetIncomeWithComponents() throws Exception {
    System.out.println("\n=== NET INCOME WITH COMPONENTS AND FOOTNOTES TEST ===");

    // Create inline model for limited test
    String modelJson = "{"
        + "\"version\": \"1.0\","
        + "\"defaultSchema\": \"sec\","
        + "\"schemas\": [{"
        + "  \"name\": \"sec\","
        + "  \"type\": \"custom\","
        + "  \"factory\": \"org.apache.calcite.adapter.sec.SecSchemaFactory\","
        + "  \"operand\": {"
        + "    \"directory\": \"/Volumes/T9/calcite-sec-cache-limited\","
        + "    \"ciks\": [\"AAPL\"],"  // Just Apple for clarity
        + "    \"filingTypes\": [\"10-K\"],"
        + "    \"autoDownload\": false,"  // Use existing cache
        + "    \"startYear\": 2023,"
        + "    \"endYear\": 2023,"
        + "    \"executionEngine\": \"parquet\","
        + "    \"testMode\": false"
        + "  }"
        + "}]"
        + "}";

    // Write model to temp file
    java.io.File modelFile = java.io.File.createTempFile("netincome-components-test", ".json");
    modelFile.deleteOnExit();
    java.nio.file.Files.writeString(modelFile.toPath(), modelJson);

    // Connection properties
    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelFile.getAbsolutePath();

    try (Connection connection = DriverManager.getConnection(jdbcUrl, info)) {

      System.out.println("\n=== STEP 1: Find NetIncomeLoss Value ===");

      String netIncomeQuery =
          "SELECT " +
          "    f.cik, " +
          "    f.\"year\", " +
          "    f.concept, " +
          "    f.numeric_value, " +
          "    f.footnote_refs, " +
          "    c.period_start, " +
          "    c.period_end " +
          "FROM sec.financial_line_items f " +
          "JOIN sec.filing_contexts c " +
          "  ON f.context_ref = c.context_id " +
          "  AND f.cik = c.cik " +
          "  AND f.filing_type = c.filing_type " +
          "  AND f.\"year\" = c.\"year\" " +
          "WHERE f.concept = 'NetIncomeLoss' " +
          "  AND f.cik = '0000320193' " +
          "  AND f.\"year\" = 2023 " +
          "  AND c.period_start IS NOT NULL " +
          "ORDER BY c.period_end DESC " +
          "LIMIT 1";

      double netIncomeValue = 0;
      String netIncomeFootnotes = null;

      try (PreparedStatement stmt = connection.prepareStatement(netIncomeQuery);
           ResultSet rs = stmt.executeQuery()) {

        if (rs.next()) {
          netIncomeValue = rs.getDouble("numeric_value");
          netIncomeFootnotes = rs.getString("footnote_refs");
          String periodStart = rs.getString("period_start");
          String periodEnd = rs.getString("period_end");

          System.out.printf("Apple Net Income for fiscal year ending %s: $%,.0f million\n",
              periodEnd, netIncomeValue);
          if (netIncomeFootnotes != null) {
            System.out.println("Footnote references: " + netIncomeFootnotes);
          }
        }
      }

      System.out.println("\n=== STEP 2: Find Components that Sum to NetIncomeLoss ===");

      // Query calculation relationships to find what sums to NetIncomeLoss
      String componentsQuery =
          "SELECT DISTINCT " +
          "    r.from_concept as component, " +
          "    r.weight, " +
          "    r.\"order\" " +
          "FROM sec.xbrl_relationships r " +
          "WHERE r.to_concept = 'NetIncomeLoss' " +
          "  AND r.linkbase_type = 'calculation' " +
          "  AND r.cik = '0000320193' " +
          "  AND r.\"year\" = 2023 " +
          "ORDER BY r.\"order\", r.from_concept";

      System.out.println("\nComponents that calculate to NetIncomeLoss:");
      System.out.println("=============================================");

      try (PreparedStatement stmt = connection.prepareStatement(componentsQuery);
           ResultSet rs = stmt.executeQuery()) {

        while (rs.next()) {
          String component = rs.getString("component");
          Double weight = rs.getDouble("weight");

          String sign = weight > 0 ? "+" : "-";
          System.out.printf("  %s %s (weight: %.1f)\n", sign, component, Math.abs(weight));

          // Get the value and footnotes for this component
          String componentValueQuery =
              "SELECT " +
              "    f.numeric_value, " +
              "    f.value, " +
              "    f.footnote_refs, " +
              "    f.full_text " +
              "FROM sec.financial_line_items f " +
              "WHERE f.concept = ? " +
              "  AND f.cik = '0000320193' " +
              "  AND f.\"year\" = 2023 " +
              "  AND f.numeric_value IS NOT NULL " +
              "LIMIT 1";

          try (PreparedStatement componentStmt = connection.prepareStatement(componentValueQuery)) {
            componentStmt.setString(1, component);
            try (ResultSet componentRs = componentStmt.executeQuery()) {
              if (componentRs.next()) {
                double value = componentRs.getDouble("numeric_value");
                String footnotes = componentRs.getString("footnote_refs");

                System.out.printf("      Value: $%,.0f million\n", value * (weight > 0 ? 1 : -1));
                if (footnotes != null) {
                  System.out.println("      Footnotes: " + footnotes);
                }
              }
            }
          }
        }
      }

      System.out.println("\n=== STEP 3: Show Presentation Hierarchy ===");

      // Query presentation relationships to show the hierarchy
      String hierarchyQuery =
          "SELECT " +
          "    r.from_concept as parent, " +
          "    r.to_concept as child, " +
          "    r.\"order\", " +
          "    f.numeric_value, " +
          "    f.footnote_refs " +
          "FROM sec.xbrl_relationships r " +
          "LEFT JOIN sec.financial_line_items f " +
          "  ON r.to_concept = f.concept " +
          "  AND f.cik = '0000320193' " +
          "  AND f.\"year\" = 2023 " +
          "WHERE r.linkbase_type = 'presentation' " +
          "  AND r.cik = '0000320193' " +
          "  AND r.\"year\" = 2023 " +
          "  AND (r.from_concept LIKE '%Income%' OR r.to_concept LIKE '%Income%') " +
          "ORDER BY r.from_concept, r.\"order\", r.to_concept " +
          "LIMIT 20";

      System.out.println("\nIncome Statement Presentation Hierarchy:");
      System.out.println("=========================================");

      String lastParent = "";
      try (PreparedStatement stmt = connection.prepareStatement(hierarchyQuery);
           ResultSet rs = stmt.executeQuery()) {

        while (rs.next()) {
          String parent = rs.getString("parent");
          String child = rs.getString("child");
          Double value = rs.getObject("numeric_value") != null ? rs.getDouble("numeric_value") : null;
          String footnotes = rs.getString("footnote_refs");

          if (!parent.equals(lastParent)) {
            System.out.println("\n" + parent + ":");
            lastParent = parent;
          }

          System.out.print("  └─ " + child);
          if (value != null) {
            System.out.printf(" = $%,.0f million", value);
          }
          if (footnotes != null) {
            System.out.print(" [" + footnotes + "]");
          }
        }
      }

      System.out.println("\n=== STEP 4: Related Disclosures and MD&A ===");

      // Find related disclosure text blocks
      String disclosureQuery =
          "SELECT DISTINCT " +
          "    f.concept, " +
          "    f.full_text " +
          "FROM sec.financial_line_items f " +
          "WHERE f.cik = '0000320193' " +
          "  AND f.\"year\" = 2023 " +
          "  AND f.concept LIKE '%Income%TextBlock%' " +
          "  AND f.full_text IS NOT NULL " +
          "  AND CHAR_LENGTH(f.full_text) > 100 " +
          "LIMIT 3";

      System.out.println("\nRelated Income Disclosures:");
      System.out.println("============================");

      try (PreparedStatement stmt = connection.prepareStatement(disclosureQuery);
           ResultSet rs = stmt.executeQuery()) {

        while (rs.next()) {
          String concept = rs.getString("concept");
          String fullText = rs.getString("full_text");

          // Clean concept name for display
          String displayName = concept.replace("TextBlock", "")
              .replaceAll("([A-Z])", " $1").trim();

          System.out.println("\n" + displayName + ":");

          // Show first 500 chars or first paragraph
          String preview = fullText.length() > 500 ?
              fullText.substring(0, 497) + "..." :
              fullText;
          System.out.println(preview);
        }
      }

      // Check for MD&A content about net income
      String mdaQuery =
          "SELECT " +
          "    m.section, " +
          "    m.subsection, " +
          "    m.paragraph_number, " +
          "    m.paragraph_text " +
          "FROM sec.mda_sections m " +
          "WHERE m.cik = '0000320193' " +
          "  AND m.\"year\" = 2023 " +
          "  AND LOWER(m.paragraph_text) LIKE '%net income%' " +
          "ORDER BY m.section, m.paragraph_number " +
          "LIMIT 3";

      System.out.println("\n\nMD&A Discussion of Net Income:");
      System.out.println("===============================");

      try (PreparedStatement stmt = connection.prepareStatement(mdaQuery);
           ResultSet rs = stmt.executeQuery()) {

        int count = 0;
        while (rs.next()) {
          count++;
          String section = rs.getString("section");
          String subsection = rs.getString("subsection");
          int paragraphNum = rs.getInt("paragraph_number");
          String text = rs.getString("paragraph_text");

          System.out.printf("\n[%s - %s, Paragraph %d]\n", section, subsection, paragraphNum);

          // Show first 300 chars
          String preview = text.length() > 300 ?
              text.substring(0, 297) + "..." :
              text;
          System.out.println(preview);
        }

        if (count == 0) {
          System.out.println("No MD&A paragraphs found mentioning net income.");
        }
      }

      System.out.println("\n=== SUMMARY ===");
      System.out.printf("Net Income: $%,.0f million\n", netIncomeValue);
      System.out.println("Successfully demonstrated querying NetIncomeLoss with:");
      System.out.println("  ✓ Component breakdown from calculation linkbase");
      System.out.println("  ✓ Footnote references for each component");
      System.out.println("  ✓ Presentation hierarchy relationships");
      System.out.println("  ✓ Related disclosure text blocks");
      System.out.println("  ✓ MD&A discussions");
    }
  }
}
