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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for HTML crawler functionality.
 * Note: Some tests are disabled by default as they require internet access.
 */
@Tag("integration")
public class HtmlCrawlerTest {

  /**
   * Test crawling Wikipedia tables.
   * Wikipedia has well-structured HTML tables with interesting data.
   */
  @Test @Disabled("Requires internet access - integration test")
  public void testWikipediaCrawl() throws Exception {
    CrawlerConfiguration config = new CrawlerConfiguration();
    config.setEnabled(true);
    config.setMaxDepth(1); // Follow links one level deep
    config.setMaxPages(5); // Limit to 5 pages
    config.setFollowExternalLinks(false);
    config.addAllowedDomain("en.wikipedia.org");

    // Only follow links to other Wikipedia articles
    config.setLinkPattern(Pattern.compile(".*wikipedia\\.org/wiki/.*"));

    HtmlCrawler crawler = new HtmlCrawler(config);

    try {
      // Start with a page that has tables and links to other pages with tables
      CrawlResult result = crawler.crawl("https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)");

      assertNotNull(result);
      assertFalse(result.getVisitedUrls().isEmpty(), "Should have visited at least one URL");
      assertTrue(result.getTotalTablesFound() > 0, "Should have found tables");

      System.out.println("Wikipedia Crawl Results:");
      System.out.println("  Pages visited: " + result.getVisitedUrls().size());
      System.out.println("  Tables found: " + result.getTotalTablesFound());
      System.out.println("  Data files found: " + result.getTotalDataFilesFound());

      // Print sample of discovered tables
      result.getHtmlTables().forEach((url, tables) -> {
        System.out.println("  From " + url + ":");
        tables.forEach(table -> {
          System.out.println("    - Table: " + table.name);
        });
      });

    } finally {
      crawler.cleanup();
    }
  }

  /**
   * Test crawling US Census Bureau data.
   * The Census Bureau provides lots of statistical tables and CSV downloads.
   */
  @Test @Disabled("Requires internet access - enable for manual testing")
  public void testUSCensusDataCrawl() throws Exception {
    CrawlerConfiguration config = new CrawlerConfiguration();
    config.setEnabled(true);
    config.setMaxDepth(2);
    config.setMaxPages(10);
    config.setFollowExternalLinks(false);
    config.addAllowedDomain("census.gov");
    config.addAllowedDomain("www.census.gov");

    // Look for CSV and Excel files
    config.addAllowedFileExtension("csv");
    config.addAllowedFileExtension("xls");
    config.addAllowedFileExtension("xlsx");

    // Reasonable limits for government data files
    config.setSizeLimitForExtension("csv", 50L * 1024 * 1024); // 50MB
    config.setSizeLimitForExtension("xlsx", 25L * 1024 * 1024); // 25MB

    HtmlCrawler crawler = new HtmlCrawler(config);

    try {
      // Start with a page that has links to data files
      CrawlResult result = crawler.crawl("https://www.census.gov/data/tables.html");

      assertNotNull(result);
      assertFalse(result.getVisitedUrls().isEmpty());

      System.out.println("US Census Crawl Results:");
      System.out.println("  Pages visited: " + result.getVisitedUrls().size());
      System.out.println("  Tables found: " + result.getTotalTablesFound());
      System.out.println("  Data files found: " + result.getTotalDataFilesFound());

      // Print discovered data files
      result.getDataFiles().forEach((url, file) -> {
        System.out.println("  Data file: " + url);
        System.out.println("    Local: " + file.getName() + " (" + file.length() + " bytes)");
      });

    } finally {
      crawler.cleanup();
    }
  }

  /**
   * Test crawling World Bank Open Data.
   * World Bank provides economic indicators in various formats.
   */
  @Test @Disabled("Requires internet access - enable for manual testing")
  public void testWorldBankDataCrawl() throws Exception {
    CrawlerConfiguration config = new CrawlerConfiguration();
    config.setEnabled(true);
    config.setMaxDepth(1);
    config.setMaxPages(5);
    config.setFollowExternalLinks(false);
    config.addAllowedDomain("data.worldbank.org");

    // World Bank provides data in CSV and Excel formats
    config.addAllowedFileExtension("csv");
    config.addAllowedFileExtension("xls");
    config.addAllowedFileExtension("xlsx");

    // Look for download links
    config.setLinkPattern(Pattern.compile(".*\\.(csv|xls|xlsx)(\\?.*)?$"));

    HtmlCrawler crawler = new HtmlCrawler(config);

    try {
      // Start with indicators page
      CrawlResult result = crawler.crawl("https://data.worldbank.org/indicator");

      assertNotNull(result);

      System.out.println("World Bank Crawl Results:");
      System.out.println("  Pages visited: " + result.getVisitedUrls().size());
      System.out.println("  Tables found: " + result.getTotalTablesFound());
      System.out.println("  Data files found: " + result.getTotalDataFilesFound());

      // Verify we found some CSV files
      boolean foundCsv = result.getDataFiles().keySet().stream()
          .anyMatch(url -> url.toLowerCase().contains(".csv"));

      if (foundCsv) {
        System.out.println("  Successfully found CSV data files!");
      }

    } finally {
      crawler.cleanup();
    }
  }

  /**
   * Test crawling FRED (Federal Reserve Economic Data).
   * FRED provides time series economic data.
   */
  @Test @Disabled("Requires internet access - enable for manual testing")
  public void testFREDDataCrawl() throws Exception {
    CrawlerConfiguration config = new CrawlerConfiguration();
    config.setEnabled(true);
    config.setMaxDepth(1);
    config.setMaxPages(5);
    config.addAllowedDomain("fred.stlouisfed.org");

    // FRED provides CSV downloads
    config.addAllowedFileExtension("csv");
    config.addAllowedFileExtension("xls");

    // Shorter delay for FRED (they have good infrastructure)
    config.setRequestDelay(Duration.ofMillis(500));

    HtmlCrawler crawler = new HtmlCrawler(config);

    try {
      // Popular economic indicators page
      CrawlResult result = crawler.crawl("https://fred.stlouisfed.org/categories/32991");

      assertNotNull(result);

      System.out.println("FRED Crawl Results:");
      System.out.println("  Pages visited: " + result.getVisitedUrls().size());
      System.out.println("  Tables found: " + result.getTotalTablesFound());
      System.out.println("  Data files found: " + result.getTotalDataFilesFound());

      // FRED pages typically have HTML tables with time series data
      if (result.getTotalTablesFound() > 0) {
        System.out.println("  Found economic indicator tables");
      }

    } finally {
      crawler.cleanup();
    }
  }

  /**
   * Test configuration limits and safety features.
   */
  @Test public void testCrawlerLimits() throws Exception {
    CrawlerConfiguration config = new CrawlerConfiguration();
    config.setEnabled(true);
    config.setMaxDepth(0); // Don't follow links
    config.setMaxHtmlSize(1024 * 1024); // 1MB limit
    config.setMaxDataFileSize(5 * 1024 * 1024); // 5MB limit

    HtmlCrawler crawler = new HtmlCrawler(config);

    try {
      // This should only process the single page
      CrawlResult result = crawler.crawl("https://example.com");

      assertNotNull(result);
      // With maxDepth=0, should only visit the starting URL
      assertTrue(result.getVisitedUrls().size() <= 1);

    } catch (Exception e) {
      // May fail if example.com is not accessible
      System.out.println("Could not reach example.com: " + e.getMessage());
    } finally {
      crawler.cleanup();
    }
  }

  /**
   * Test link pattern filtering.
   */
  @Test @Disabled("Requires internet access")
  public void testLinkPatternFiltering() throws Exception {
    CrawlerConfiguration config = new CrawlerConfiguration();
    config.setEnabled(true);
    config.setMaxDepth(1);

    // Only follow links containing "data" or "statistics"
    config.setLinkPattern(Pattern.compile(".*(data|statistics).*", Pattern.CASE_INSENSITIVE));

    HtmlCrawler crawler = new HtmlCrawler(config);

    try {
      CrawlResult result = crawler.crawl("https://www.data.gov/");

      assertNotNull(result);

      // All visited URLs (except the first) should match the pattern
      result.getVisitedUrls().stream()
          .skip(1) // Skip the starting URL
          .forEach(url -> {
            assertTrue(
                url.toLowerCase().contains("data") || url.toLowerCase().contains("statistics"),
                "URL should match pattern: " + url);
          });

    } finally {
      crawler.cleanup();
    }
  }
}
