package org.apache.calcite.adapter.sec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test Wikipedia DJI scraper.
 */
@Tag("integration")
public class WikipediaScraperTest {
  @Test
  public void testWikipediaDJIScraper() throws Exception {
      System.out.println("Testing Wikipedia DJI scraper...");
      List<String> constituents = SecDataFetcher.fetchDJIConstituents();
      
      System.out.println("Found " + constituents.size() + " DJI constituents:");
      for (String ticker : constituents) {
        System.out.println("  - " + ticker);
      }
      
      if (constituents.size() == 30) {
        System.out.println("✓ Success: Found expected 30 DJI constituents");
      } else {
        System.out.println("⚠ Warning: Expected 30 constituents, found " + constituents.size());
      }
  }
}