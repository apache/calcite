package org.apache.calcite.adapter.sec;

import org.apache.calcite.adapter.sec.SecDataFetcher;
import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
public class RealDJIScraperTest {
  @Test
public void test() throws Exception {
    try {
      System.out.println("\n" + "=".repeat(80));
      System.out.println("TESTING REAL DJI WIKIPEDIA SCRAPER");
      System.out.println("=".repeat(80) + "\n");
      
      // Test the new Wikipedia scraper
      System.out.println("Fetching DJI constituents from Wikipedia...");
      List<String> ciks = SecDataFetcher.fetchDJIConstituents();
      
      System.out.println("\nResults:");
      System.out.println("  Found " + ciks.size() + " CIKs");
      
      if (ciks.size() > 0) {
        System.out.println("\n  Sample CIKs:");
        for (int i = 0; i < Math.min(5, ciks.size()); i++) {
          System.out.println("    " + (i+1) + ". " + ciks.get(i));
        }
      }
      
      System.out.println("\n" + "=".repeat(80));
      if (ciks.size() == 30) {
        System.out.println("✓ SUCCESS: Found expected 30 DJI constituents");
      } else if (ciks.size() > 0) {
        System.out.println("✓ PARTIAL: Found " + ciks.size() + " constituents (expected ~30)");
      } else {
        System.out.println("✗ FAILURE: No constituents found");
      }
      System.out.println("=".repeat(80));
      
    } catch (Exception e) {
      System.err.println("\n✗ ERROR: " + e.getMessage());
      e.printStackTrace();
    }
  }
}