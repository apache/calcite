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
package org.apache.calcite.adapter.file.html;

import org.apache.calcite.adapter.file.converters.CrawlerConfiguration;
import org.apache.calcite.adapter.file.converters.HtmlCrawler;
import org.apache.calcite.adapter.file.converters.HtmlCrawler.CrawlResult;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for HTML crawler configuration options.
 */
@Tag("unit")
public class HtmlCrawlerConfigurationTest {
  
  @TempDir
  Path tempDir;
  
  @Test
  public void testDataFilePatternConfiguration() {
    CrawlerConfiguration config = new CrawlerConfiguration();
    
    // Test CSV-only pattern
    Pattern csvPattern = Pattern.compile(".*\\.csv$");
    config.setDataFilePattern(csvPattern);
    assertEquals(csvPattern, config.getDataFilePattern());
    
    // Test exclude pattern
    Pattern excludePattern = Pattern.compile(".*test.*\\.csv$");
    config.setDataFileExcludePattern(excludePattern);
    assertEquals(excludePattern, config.getDataFileExcludePattern());
  }
  
  @Test
  public void testHtmlTableConfiguration() {
    CrawlerConfiguration config = new CrawlerConfiguration();
    
    // Test default values
    assertTrue(config.isGenerateTablesFromHtml());
    assertEquals(1, config.getHtmlTableMinRows());
    assertEquals(Integer.MAX_VALUE, config.getHtmlTableMaxRows());
    
    // Test setting values
    config.setGenerateTablesFromHtml(false);
    assertFalse(config.isGenerateTablesFromHtml());
    
    config.setHtmlTableMinRows(5);
    assertEquals(5, config.getHtmlTableMinRows());
    
    config.setHtmlTableMaxRows(1000);
    assertEquals(1000, config.getHtmlTableMaxRows());
  }
  
  @Test
  public void testConfigurationFromMap() {
    Map<String, Object> options = new HashMap<>();
    options.put("dataFilePattern", ".*\\.(csv|tsv)$");
    options.put("dataFileExcludePattern", ".*temp.*");
    options.put("generateTablesFromHtml", "false");
    options.put("htmlTableMinRows", "3");
    options.put("htmlTableMaxRows", "500");
    
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    
    assertNotNull(config.getDataFilePattern());
    assertTrue(config.getDataFilePattern().matcher("data.csv").matches());
    assertTrue(config.getDataFilePattern().matcher("data.tsv").matches());
    assertFalse(config.getDataFilePattern().matcher("data.xlsx").matches());
    
    assertNotNull(config.getDataFileExcludePattern());
    assertTrue(config.getDataFileExcludePattern().matcher("temp_data.csv").matches());
    
    assertFalse(config.isGenerateTablesFromHtml());
    assertEquals(3, config.getHtmlTableMinRows());
    assertEquals(500, config.getHtmlTableMaxRows());
  }
  
  @Test
  public void testCsvOnlyConfiguration() throws IOException {
    // Create test HTML with various links
    String html = "<html><body>"
        + "<a href='data.csv'>CSV File</a>"
        + "<a href='data.xlsx'>Excel File</a>"
        + "<a href='data.json'>JSON File</a>"
        + "<table><tr><td>Cell1</td></tr></table>"
        + "</body></html>";
    
    File htmlFile = new File(tempDir.toFile(), "test.html");
    Files.writeString(htmlFile.toPath(), html);
    String fileUrl = htmlFile.toURI().toString();
    
    // Configure to only accept CSV files and no HTML tables
    CrawlerConfiguration config = new CrawlerConfiguration();
    config.setDataFilePattern(Pattern.compile(".*\\.csv$"));
    config.setGenerateTablesFromHtml(false);
    
    HtmlCrawler crawler = new HtmlCrawler(config);
    CrawlResult result = crawler.crawl(fileUrl);
    
    // Should find only CSV file, no HTML tables
    assertEquals(0, result.getTotalTablesFound());
    // Note: actual data file processing would require a web server
    // This test verifies configuration is applied
  }
  
  @Test
  public void testHtmlTablesOnlyConfiguration() throws IOException {
    // Create test HTML with various links and tables
    String html = "<html><body>"
        + "<a href='data.csv'>CSV File</a>"
        + "<table>"
        + "  <tr><th>Header1</th><th>Header2</th></tr>"
        + "  <tr><td>Cell1</td><td>Cell2</td></tr>"
        + "  <tr><td>Cell3</td><td>Cell4</td></tr>"
        + "</table>"
        + "</body></html>";
    
    File htmlFile = new File(tempDir.toFile(), "test.html");
    Files.writeString(htmlFile.toPath(), html);
    String fileUrl = htmlFile.toURI().toString();
    
    // Configure to only extract HTML tables, no data files
    CrawlerConfiguration config = new CrawlerConfiguration();
    config.setDataFilePattern(null); // No data files
    config.getAllowedFileExtensions().clear(); // Clear extensions
    config.setGenerateTablesFromHtml(true);
    
    HtmlCrawler crawler = new HtmlCrawler(config);
    CrawlResult result = crawler.crawl(fileUrl);
    
    // Should find HTML table but no data files
    assertEquals(1, result.getTotalTablesFound());
  }
  
  @Test
  public void testTableRowFiltering() throws IOException {
    // Create test HTML with tables of various sizes
    String html = "<html><body>"
        + "<table id='small'>"
        + "  <tr><td>Row1</td></tr>"
        + "</table>"
        + "<table id='medium'>"
        + "  <tr><th>Header</th></tr>"
        + "  <tr><td>Row1</td></tr>"
        + "  <tr><td>Row2</td></tr>"
        + "  <tr><td>Row3</td></tr>"
        + "  <tr><td>Row4</td></tr>"
        + "</table>"
        + "<table id='large'>"
        + "  <tr><th>Header</th></tr>";
    
    // Add many rows
    for (int i = 1; i <= 20; i++) {
      html += "  <tr><td>Row" + i + "</td></tr>";
    }
    html += "</table></body></html>";
    
    File htmlFile = new File(tempDir.toFile(), "test.html");
    Files.writeString(htmlFile.toPath(), html);
    String fileUrl = htmlFile.toURI().toString();
    
    // Configure to only accept tables with 3-10 rows
    CrawlerConfiguration config = new CrawlerConfiguration();
    config.setGenerateTablesFromHtml(true);
    config.setHtmlTableMinRows(3);
    config.setHtmlTableMaxRows(10);
    
    HtmlCrawler crawler = new HtmlCrawler(config);
    CrawlResult result = crawler.crawl(fileUrl);
    
    // Should find only the medium table (5 rows)
    assertEquals(1, result.getTotalTablesFound());
  }
  
  @Test
  public void testMixedModeConfiguration() throws IOException {
    // Create test HTML
    String html = "<html><body>"
        + "<a href='data.csv'>CSV File</a>"
        + "<a href='data.parquet'>Parquet File</a>"
        + "<a href='report.pdf'>PDF File</a>"
        + "<table><tr><td>Data</td></tr></table>"
        + "</body></html>";
    
    File htmlFile = new File(tempDir.toFile(), "test.html");
    Files.writeString(htmlFile.toPath(), html);
    String fileUrl = htmlFile.toURI().toString();
    
    // Configure to accept CSV and Parquet files, plus HTML tables
    CrawlerConfiguration config = new CrawlerConfiguration();
    config.setDataFilePattern(Pattern.compile(".*\\.(csv|parquet)$"));
    config.setGenerateTablesFromHtml(true);
    
    HtmlCrawler crawler = new HtmlCrawler(config);
    CrawlResult result = crawler.crawl(fileUrl);
    
    // Should find HTML table
    assertEquals(1, result.getTotalTablesFound());
    // Data file links would be processed if they were accessible
  }
}