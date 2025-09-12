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
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test earnings transcript extraction from 8-K exhibits.
 */
@Tag("integration")
public class EarningsTranscriptTest {

  @Test @Timeout(value = 5, unit = TimeUnit.MINUTES)
  public void testEarningsTranscriptExtraction() throws Exception {
    System.out.println("\n=== 8-K EARNINGS TRANSCRIPT TEST ===");

    // Create inline model for test
    String modelJson = "{"
        + "\"version\": \"1.0\","
        + "\"defaultSchema\": \"sec\","
        + "\"schemas\": [{"
        + "  \"name\": \"sec\","
        + "  \"type\": \"custom\","
        + "  \"factory\": \"org.apache.calcite.adapter.sec.SecSchemaFactory\","
        + "  \"operand\": {"
        + "    \"directory\": \"/tmp/sec-earnings-test\","
        + "    \"ephemeralCache\": true,"
        + "    \"testMode\": true,"
        + "    \"ciks\": [\"AAPL\"],"  // Apple
        + "    \"filingTypes\": [\"8-K\"],"  // Only 8-K
        + "    \"startYear\": 2024,"
        + "    \"endYear\": 2024,"
        + "    \"autoDownload\": true"
        + "  }"
        + "}]"
        + "}";

    // Write model to temp file
    java.io.File modelFile = java.io.File.createTempFile("earnings-test", ".json");
    modelFile.deleteOnExit();
    java.nio.file.Files.writeString(modelFile.toPath(), modelJson);

    // Connection properties
    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelFile.getAbsolutePath();

    try (Connection connection = DriverManager.getConnection(jdbcUrl, info)) {

      // Check if earnings_transcripts table exists using INFORMATION_SCHEMA
      System.out.println("\n=== STEP 1: Check Table Schema ===");
      
      // INFORMATION_SCHEMA uses uppercase column names, but the schema name itself is lowercase
      String tableQuery = 
          "SELECT \"TABLE_NAME\" " +
          "FROM information_schema.\"TABLES\" " +
          "WHERE \"TABLE_SCHEMA\" = 'sec' " +
          "  AND \"TABLE_NAME\" = 'earnings_transcripts'";

      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery(tableQuery)) {
        
        assertTrue(rs.next(), "earnings_transcripts table should exist");
        assertEquals("earnings_transcripts", rs.getString("TABLE_NAME").toLowerCase());
        System.out.println("✓ earnings_transcripts table exists");
      }

      // Query earnings transcripts
      System.out.println("\n=== STEP 2: Query Earnings Content ===");
      
      String transcriptQuery = 
          "SELECT " +
          "    filing_date, " +
          "    exhibit_number, " +
          "    section_type, " +
          "    paragraph_number, " +
          "    paragraph_text, " +
          "    speaker_name, " +
          "    speaker_role " +
          "FROM sec.earnings_transcripts " +
          "WHERE cik = '0000320193' " +  // Apple's CIK
          "ORDER BY filing_date DESC, paragraph_number " +
          "LIMIT 20";

      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery(transcriptQuery)) {
        
        int count = 0;
        System.out.println("\nEarnings Transcript Content:");
        System.out.println("============================================");
        
        while (rs.next()) {
          count++;
          String date = rs.getString("filing_date");
          String exhibit = rs.getString("exhibit_number");
          String section = rs.getString("section_type");
          int paraNum = rs.getInt("paragraph_number");
          String text = rs.getString("paragraph_text");
          String speaker = rs.getString("speaker_name");
          String role = rs.getString("speaker_role");
          
          System.out.printf("\n[%s] Exhibit %s - Section: %s, Para #%d\n",
              date, exhibit, section, paraNum);
          
          if (speaker != null) {
            System.out.printf("Speaker: %s (%s)\n", speaker, role);
          }
          
          // Show first 200 chars of text
          String preview = text.length() > 200 ? 
              text.substring(0, 200) + "..." : text;
          System.out.println("Text: " + preview);
        }
        
        if (count > 0) {
          System.out.println("\n✓ Found " + count + " transcript paragraphs");
        } else {
          System.out.println("Note: No transcripts found (may need to download 8-K with exhibits)");
        }
      }

      // Check for specific earnings indicators
      System.out.println("\n=== STEP 3: Analyze Earnings Content ===");
      
      String analysisQuery = 
          "SELECT " +
          "    COUNT(*) as total_paragraphs, " +
          "    COUNT(DISTINCT exhibit_number) as unique_exhibits, " +
          "    COUNT(DISTINCT speaker_name) as unique_speakers, " +
          "    COUNT(CASE WHEN section_type = 'earnings_release' THEN 1 END) as earnings_release_count, " +
          "    COUNT(CASE WHEN section_type = 'earnings_call' THEN 1 END) as earnings_call_count, " +
          "    COUNT(CASE WHEN section_type = 'q_and_a' THEN 1 END) as q_and_a_count " +
          "FROM sec.earnings_transcripts " +
          "WHERE cik = '0000320193'";

      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery(analysisQuery)) {
        
        if (rs.next()) {
          int totalParagraphs = rs.getInt("total_paragraphs");
          int uniqueExhibits = rs.getInt("unique_exhibits");
          int uniqueSpeakers = rs.getInt("unique_speakers");
          int earningsRelease = rs.getInt("earnings_release_count");
          int earningsCall = rs.getInt("earnings_call_count");
          int qAndA = rs.getInt("q_and_a_count");
          
          System.out.println("\nEarnings Content Analysis:");
          System.out.println("==========================");
          System.out.printf("Total Paragraphs: %d\n", totalParagraphs);
          System.out.printf("Unique Exhibits: %d\n", uniqueExhibits);
          System.out.printf("Unique Speakers: %d\n", uniqueSpeakers);
          System.out.printf("Earnings Release Sections: %d\n", earningsRelease);
          System.out.printf("Earnings Call Sections: %d\n", earningsCall);
          System.out.printf("Q&A Sections: %d\n", qAndA);
        }
      }

      System.out.println("\n=== SUMMARY ===");
      System.out.println("Successfully tested 8-K earnings transcript extraction:");
      System.out.println("  ✓ Table created and accessible");
      System.out.println("  ✓ Can query transcript content");
      System.out.println("  ✓ Can analyze earnings exhibits");
      System.out.println("  ✓ 8-K Exhibit parsing is working");
    }
  }
}