package org.apache.calcite.adapter.sec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real DJI download test.
 */
@Tag("integration")
public class RunRealDJIDownload {
  @Test
  public void testRealDJIDownload() throws Exception {
    System.out.println("\n" + "=".repeat(80));
    System.out.println("REAL DJI 5-YEAR DOWNLOAD");
    System.out.println("=".repeat(80) + "\n");
    
    String dataDir = "/Volumes/T9/sec-data/dji-5year";
    File targetDir = new File(dataDir);
    targetDir.mkdirs();
    
    System.out.println("Target directory: " + dataDir);
    System.out.println("Fetching DJI constituents...");
    
    // Get current DJI companies
    List<String> djiCiks = SecDataFetcher.fetchDJIConstituents();
    System.out.println("Found " + djiCiks.size() + " DJI companies\n");
    
    if (djiCiks.isEmpty()) {
      System.err.println("No DJI constituents found!");
      return;
    }
    
    // For testing, just do first 3 companies, 1 year
    int maxCompanies = Math.min(3, djiCiks.size());
    System.out.println("Downloading filings for first " + maxCompanies + " companies (2024 only for test)...\n");
    
    for (int i = 0; i < maxCompanies; i++) {
      String cik = djiCiks.get(i);
      System.out.println("Downloading CIK " + cik + " (" + (i+1) + "/" + maxCompanies + ")...");
      
      Map<String, Object> config = new HashMap<>();
      config.put("ciks", cik);
      config.put("startYear", 2024);
      config.put("endYear", 2024);
      config.put("forms", Arrays.asList("10-K", "10-Q"));
      config.put("maxFilingsPerCompany", 5);
      config.put("realData", true);
      config.put("downloadDelay", 100);
      
      EdgarDownloader downloader = new EdgarDownloader(config, targetDir);
      List<File> files = downloader.downloadFilings();
      System.out.println("  Downloaded " + files.size() + " files");
      
      Thread.sleep(500); // Be nice to SEC servers
    }
    
    // Check results
    File secDir = new File(targetDir, "sec");
    if (secDir.exists()) {
      File[] xmlFiles = secDir.listFiles((dir, name) -> name.endsWith(".xml"));
      if (xmlFiles != null && xmlFiles.length > 0) {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("SUCCESS! Downloaded " + xmlFiles.length + " XBRL files to:");
        System.out.println(secDir.getAbsolutePath());
        System.out.println("\nSample files:");
        for (int i = 0; i < Math.min(5, xmlFiles.length); i++) {
          System.out.printf("  - %s (%,d bytes)\n", xmlFiles[i].getName(), xmlFiles[i].length());
        }
        System.out.println("=".repeat(80));
      }
    } else {
      System.err.println("ERROR: No files downloaded to " + secDir);
    }
  }
}