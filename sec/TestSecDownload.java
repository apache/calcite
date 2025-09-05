import org.apache.calcite.adapter.sec.*;
import java.io.File;
import java.util.*;

public class TestSecDownload {
  public static void main(String[] args) throws Exception {
    System.out.println("Testing SEC XBRL Download Logic");
    
    // Create test directory
    String testDir = "build/test-data/TestSecDownload_" + System.currentTimeMillis();
    new File(testDir).mkdirs();
    
    // Create minimal operand for testing
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", testDir);
    operand.put("ciks", Arrays.asList("0000320193")); // Apple
    operand.put("filingTypes", Arrays.asList("10-K", "10-Q"));
    operand.put("startYear", 2023);
    operand.put("endYear", 2024);
    operand.put("autoDownload", true);
    
    // Test the download logic directly
    SecSchemaFactory factory = new SecSchemaFactory();
    
    try {
      System.out.println("Creating SEC schema with download...");
      factory.create(null, "TEST_SEC", operand);
      
      // Check what was downloaded
      File baseDir = new File(testDir);
      File secRaw = new File(baseDir, "sec-raw");
      File secParquet = new File(baseDir, "sec-parquet");
      
      System.out.println("sec-raw exists: " + secRaw.exists());
      System.out.println("sec-parquet exists: " + secParquet.exists());
      
      if (secRaw.exists()) {
        File[] cikDirs = secRaw.listFiles();
        if (cikDirs != null) {
          for (File cikDir : cikDirs) {
            System.out.println("CIK directory: " + cikDir.getName());
            File[] files = cikDir.listFiles();
            if (files != null) {
              for (File file : files) {
                System.out.println("  - " + file.getName() + " (size: " + file.length() + ")");
              }
            }
          }
        }
      }
      
      if (secParquet.exists()) {
        File[] parquetFiles = secParquet.listFiles();
        if (parquetFiles != null) {
          for (File file : parquetFiles) {
            System.out.println("Parquet file: " + file.getName() + " (size: " + file.length() + ")");
          }
        }
      }
      
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace();
    }
  }
}