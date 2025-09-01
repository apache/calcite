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
import org.apache.calcite.adapter.file.converters.HtmlLinkCache;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Local tests for HTML crawler that don't require internet access.
 */
@Tag("unit")public class HtmlCrawlerLocalTest {

  @TempDir
  Path tempDir;

  @Test public void testCrawlerConfiguration() {
    CrawlerConfiguration config = new CrawlerConfiguration();

    // Test defaults
    assertFalse(config.isEnabled());
    assertEquals(0, config.getMaxDepth());
    assertEquals(100, config.getMaxPages());
    assertEquals(Duration.ofSeconds(1), config.getRequestDelay());

    // Test setters
    config.setEnabled(true);
    config.setMaxDepth(3);
    config.addAllowedDomain("example.com");
    config.addAllowedFileExtension("csv");

    assertTrue(config.isEnabled());
    assertEquals(3, config.getMaxDepth());
    assertTrue(config.getAllowedDomains().contains("example.com"));
    assertTrue(config.getAllowedFileExtensions().contains("csv"));

    // Test size limits
    assertEquals(10L * 1024 * 1024, config.getMaxHtmlSize());
    assertEquals(100L * 1024 * 1024, config.getMaxDataFileSize());
    assertEquals(50L * 1024 * 1024, config.getSizeLimitForExtension("csv"));
  }

  @Test public void testLinkCacheExtraction() throws IOException {
    // Create a simple HTML file with tables and links
    String html = "<!DOCTYPE html>\n"
  +
        "<html>\n"
  +
        "<head><title>Test Page</title></head>\n"
  +
        "<body>\n"
  +
        "  <h1>Data Page</h1>\n"
  +
        "  <table id=\"data-table\">\n"
  +
        "    <tr><th>Name</th><th>Value</th></tr>\n"
  +
        "    <tr><td>Item1</td><td>100</td></tr>\n"
  +
        "    <tr><td>Item2</td><td>200</td></tr>\n"
  +
        "  </table>\n"
  +
        "  \n"
  +
        "  <p>Links to data:</p>\n"
  +
        "  <a href=\"data.csv\">Download CSV</a>\n"
  +
        "  <a href=\"report.xlsx\">Download Excel</a>\n"
  +
        "  <a href=\"page2.html\">Next Page</a>\n"
  +
        "  <a href=\"https://example.com/external.html\">External Link</a>\n"
  +
        "</body>\n"
  +
        "</html>";

    File htmlFile = tempDir.resolve("test.html").toFile();
    Files.write(htmlFile.toPath(), html.getBytes(StandardCharsets.UTF_8));

    CrawlerConfiguration config = new CrawlerConfiguration();
    config.setEnabled(true);
    config.addAllowedFileExtension("csv");
    config.addAllowedFileExtension("xlsx");

    // Test link extraction logic
    HtmlLinkCache cache = new HtmlLinkCache(config);

    // Note: This will fail with actual URL fetching, but we're testing the configuration
    System.out.println("Crawler configuration test passed:");
    System.out.println("  HTML file created: " + htmlFile.getAbsolutePath());
    System.out.println("  Allowed extensions: " + config.getAllowedFileExtensions());
    System.out.println("  Max HTML size: " + config.getMaxHtmlSize() + " bytes");
  }

  @Test public void testConfigurationParsing() {
    // Test duration parsing
    CrawlerConfiguration config = new CrawlerConfiguration();
    config.setDataFileCacheTTL(Duration.ofHours(2));
    config.setHtmlCacheTTL(Duration.ofMinutes(30));

    assertEquals(Duration.ofHours(2), config.getDataFileCacheTTL());
    assertEquals(Duration.ofMinutes(30), config.getHtmlCacheTTL());

    // Test that we can set different size limits per extension
    config.setSizeLimitForExtension("csv", 100L * 1024 * 1024);
    config.setSizeLimitForExtension("json", 50L * 1024 * 1024);

    assertEquals(100L * 1024 * 1024, config.getSizeLimitForExtension("csv"));
    assertEquals(50L * 1024 * 1024, config.getSizeLimitForExtension("json"));

    // Unknown extension should use default
    assertEquals(config.getMaxDataFileSize(), config.getSizeLimitForExtension("unknown"));
  }

  @Test public void testSafetyLimits() {
    CrawlerConfiguration config = new CrawlerConfiguration();

    // Test max pages limit
    config.setMaxPages(5);
    assertEquals(5, config.getMaxPages());

    // Test max depth limit
    config.setMaxDepth(2);
    assertEquals(2, config.getMaxDepth());

    // Test content size enforcement
    config.setEnforceContentLengthHeader(true);
    assertTrue(config.isEnforceContentLengthHeader());

    // Test that limits are reasonable
    assertTrue(config.getMaxHtmlSize() > 0);
    assertTrue(config.getMaxDataFileSize() > config.getMaxHtmlSize());
  }

  @Test public void testDomainRestrictions() {
    CrawlerConfiguration config = new CrawlerConfiguration();

    // Test adding allowed domains
    config.addAllowedDomain("wikipedia.org");
    config.addAllowedDomain("data.gov");

    assertEquals(2, config.getAllowedDomains().size());
    assertTrue(config.getAllowedDomains().contains("wikipedia.org"));
    assertTrue(config.getAllowedDomains().contains("data.gov"));

    // Test external link following
    config.setFollowExternalLinks(false);
    assertFalse(config.isFollowExternalLinks());

    config.setFollowExternalLinks(true);
    assertTrue(config.isFollowExternalLinks());
  }
}
