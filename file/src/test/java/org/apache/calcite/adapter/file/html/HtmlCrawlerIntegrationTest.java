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
import org.apache.calcite.adapter.file.converters.HtmlToJsonConverter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.io.TempDir;
import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for HTML crawler with HtmlToJsonConverter.
 * These tests demonstrate real-world usage scenarios.
 */
@Tag("integration")
public class HtmlCrawlerIntegrationTest {
  
  @TempDir
  Path tempDir;
  
  /**
   * Test complete workflow: crawl, discover tables, convert to JSON.
   */
  @Test
  @Disabled("Requires internet access - integration test")
  public void testCompleteWorkflow() throws Exception {
    File outputDir = tempDir.resolve("output").toFile();
    outputDir.mkdirs();
    
    // Configure crawler
    CrawlerConfiguration config = new CrawlerConfiguration();
    config.setEnabled(true);
    config.setMaxDepth(1);
    config.setMaxPages(3);
    config.setRequestDelay(Duration.ofSeconds(1));
    
    // Test with a simple Wikipedia page
    String startUrl = "https://en.wikipedia.org/wiki/List_of_countries_by_population_(United_Nations)";
    
    // Perform crawl and conversion
    Map<String, List<File>> results = HtmlToJsonConverter.convertWithCrawling(
        startUrl, outputDir, config, "TO_LOWER");
    
    assertNotNull(results);
    assertFalse(results.isEmpty(), "Should have generated some JSON files");
    
    // Print results
    System.out.println("Complete Workflow Results:");
    for (Map.Entry<String, List<File>> entry : results.entrySet()) {
      System.out.println("From URL: " + entry.getKey());
      for (File jsonFile : entry.getValue()) {
        System.out.println("  Generated: " + jsonFile.getName() + " (" + jsonFile.length() + " bytes)");
      }
    }
    
    // Verify JSON files were created
    File[] jsonFiles = outputDir.listFiles((dir, name) -> name.endsWith(".json"));
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.length > 0, "Should have created JSON files");
  }
  
  /**
   * Test crawling with data file discovery.
   * Uses a site that has downloadable data files.
   */
  @Test
  @Disabled("Requires internet access - integration test")
  public void testDataFileCrawling() throws Exception {
    File outputDir = tempDir.resolve("data_files").toFile();
    outputDir.mkdirs();
    
    CrawlerConfiguration config = new CrawlerConfiguration();
    config.setEnabled(true);
    config.setMaxDepth(2);
    config.setMaxPages(10);
    
    // Look for CSV and Excel files
    config.addAllowedFileExtension("csv");
    config.addAllowedFileExtension("xlsx");
    config.setLinkPattern(Pattern.compile(".*\\.(csv|xlsx|xls)$"));
    
    // Size limits
    config.setMaxDataFileSize(10L * 1024 * 1024); // 10MB max
    
    // Try with data.gov or similar
    String startUrl = "https://catalog.data.gov/dataset?res_format=CSV";
    
    Map<String, List<File>> results = HtmlToJsonConverter.convertWithCrawling(
        startUrl, outputDir, config);
    
    System.out.println("Data File Crawling Results:");
    System.out.println("  Total sources processed: " + results.size());
    
    // Count data files vs HTML tables
    int dataFiles = 0;
    int htmlTables = 0;
    for (Map.Entry<String, List<File>> entry : results.entrySet()) {
      String url = entry.getKey();
      if (url.toLowerCase().matches(".*\\.(csv|xlsx|xls)$")) {
        dataFiles += entry.getValue().size();
      } else {
        htmlTables += entry.getValue().size();
      }
    }
    
    System.out.println("  Data files converted: " + dataFiles);
    System.out.println("  HTML tables converted: " + htmlTables);
  }
  
  /**
   * Test domain-restricted crawling.
   * Ensures crawler respects domain boundaries.
   */
  @Test
  @Disabled("Requires internet access - integration test")
  public void testDomainRestrictedCrawling() throws Exception {
    File outputDir = tempDir.resolve("domain_restricted").toFile();
    outputDir.mkdirs();
    
    CrawlerConfiguration config = new CrawlerConfiguration();
    config.setEnabled(true);
    config.setMaxDepth(2);
    config.setMaxPages(5);
    config.setFollowExternalLinks(false);
    
    // Only stay within wikipedia.org
    config.addAllowedDomain("en.wikipedia.org");
    
    String startUrl = "https://en.wikipedia.org/wiki/Economy";
    
    Map<String, List<File>> results = HtmlToJsonConverter.convertWithCrawling(
        startUrl, outputDir, config);
    
    // Verify all URLs are from wikipedia
    for (String url : results.keySet()) {
      assertTrue(url.contains("wikipedia.org"), 
          "All URLs should be from wikipedia.org, but found: " + url);
    }
    
    System.out.println("Domain-restricted crawl successful. All " + results.size() + 
                      " URLs are from wikipedia.org");
  }
  
  /**
   * Test performance with caching.
   * Crawls the same site twice to demonstrate cache effectiveness.
   */
  @Test
  @Disabled("Requires internet access - integration test")
  public void testCachingPerformance() throws Exception {
    File outputDir = tempDir.resolve("cache_test").toFile();
    outputDir.mkdirs();
    
    CrawlerConfiguration config = new CrawlerConfiguration();
    config.setEnabled(true);
    config.setMaxDepth(1);
    config.setMaxPages(3);
    config.setHtmlCacheTTL(Duration.ofMinutes(5)); // 5 minute cache
    
    String startUrl = "https://en.wikipedia.org/wiki/Database";
    
    // First crawl - populates cache
    long startTime1 = System.currentTimeMillis();
    Map<String, List<File>> results1 = HtmlToJsonConverter.convertWithCrawling(
        startUrl, outputDir, config);
    long time1 = System.currentTimeMillis() - startTime1;
    
    // Second crawl - should use cache
    long startTime2 = System.currentTimeMillis();
    Map<String, List<File>> results2 = HtmlToJsonConverter.convertWithCrawling(
        startUrl, outputDir, config);
    long time2 = System.currentTimeMillis() - startTime2;
    
    System.out.println("Caching Performance Test:");
    System.out.println("  First crawl: " + time1 + "ms (" + results1.size() + " sources)");
    System.out.println("  Second crawl: " + time2 + "ms (" + results2.size() + " sources)");
    System.out.println("  Speed improvement: " + (time1 - time2) + "ms");
    
    // Second crawl should be significantly faster due to caching
    assertTrue(time2 < time1, "Second crawl should be faster due to caching");
  }
  
  /**
   * Test safety limits (max pages, max depth, content size).
   */
  @Test
  @Disabled("Requires internet access - integration test")
  public void testSafetyLimits() throws Exception {
    File outputDir = tempDir.resolve("safety_test").toFile();
    outputDir.mkdirs();
    
    CrawlerConfiguration config = new CrawlerConfiguration();
    config.setEnabled(true);
    config.setMaxDepth(10); // Very deep
    config.setMaxPages(2); // But only 2 pages total
    config.setMaxHtmlSize(1024 * 1024); // 1MB HTML limit
    config.setMaxDataFileSize(5 * 1024 * 1024); // 5MB data file limit
    
    String startUrl = "https://en.wikipedia.org/wiki/Main_Page";
    
    Map<String, List<File>> results = HtmlToJsonConverter.convertWithCrawling(
        startUrl, outputDir, config);
    
    // Should have stopped at 2 pages despite deep depth setting
    assertTrue(results.size() <= 2, "Should respect max pages limit");
    
    System.out.println("Safety limits test passed. Crawled " + results.size() + 
                      " pages (max was 2)");
  }
}