import java.sql.*;
import java.util.Properties;

/**
 * Example: Connect to SEC adapter and download DJI company filings for the last 5 years.
 * 
 * This example shows how to:
 * 1. Load the DJI 5-year model configuration
 * 2. Connect to the SEC adapter
 * 3. Query financial data from all Dow 30 companies
 */
public class ConnectToDJI5Year {
  
  public static void main(String[] args) throws Exception {
    System.out.println("\n" + "=".repeat(80));
    System.out.println("CONNECTING TO DJI 5-YEAR SEC DATA");
    System.out.println("=".repeat(80) + "\n");
    
    // Use the model file directly
    String modelPath = "/Users/kennethstott/calcite/sec/src/main/resources/dji-5year-model.json";
    
    Properties info = new Properties();
    info.put("model", modelPath);
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");
    
    // Register the Calcite driver
    Class.forName("org.apache.calcite.jdbc.Driver");
    
    // Connect to the SEC adapter
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info)) {
      System.out.println("✓ Connected to SEC adapter\n");
      
      // Example 1: Get revenue for all DJI companies in 2023
      System.out.println("Example 1: DJI Company Revenues in 2023");
      System.out.println("-".repeat(60));
      
      String revenueQuery = 
        "SELECT company_name, " +
        "       SUM(CAST(value AS DOUBLE)) / 1000000000 AS revenue_billions " +
        "FROM sec_dji.financial_line_items " +
        "WHERE concept_name LIKE '%Revenue%' " +
        "  AND filing_date >= '2023-01-01' " +
        "  AND filing_date <= '2023-12-31' " +
        "  AND filing_type = '10-K' " +
        "GROUP BY company_name " +
        "ORDER BY revenue_billions DESC " +
        "LIMIT 10";
      
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(revenueQuery)) {
        
        while (rs.next()) {
          System.out.printf("%-30s: $%.1fB\n",
            rs.getString("company_name"),
            rs.getDouble("revenue_billions"));
        }
      }
      
      // Example 2: Track Apple's quarterly performance
      System.out.println("\nExample 2: Apple Quarterly Net Income (2022-2024)");
      System.out.println("-".repeat(60));
      
      String appleQuery = 
        "SELECT filing_date, " +
        "       SUM(CAST(value AS DOUBLE)) / 1000000 AS net_income_millions " +
        "FROM sec_dji.financial_line_items " +
        "WHERE cik = '0000320193' " +  // Apple's CIK
        "  AND concept_name LIKE '%NetIncome%' " +
        "  AND filing_type = '10-Q' " +
        "  AND filing_date >= '2022-01-01' " +
        "GROUP BY filing_date " +
        "ORDER BY filing_date DESC " +
        "LIMIT 8";
      
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(appleQuery)) {
        
        while (rs.next()) {
          System.out.printf("%s: $%,.0fM\n",
            rs.getString("filing_date"),
            rs.getDouble("net_income_millions"));
        }
      }
      
      // Example 3: Compare tech giants in DJI
      System.out.println("\nExample 3: Tech Giants Comparison (Latest 10-K)");
      System.out.println("-".repeat(60));
      
      String techQuery = 
        "SELECT company_name, " +
        "       concept_name, " +
        "       CAST(value AS DOUBLE) / 1000000000 AS value_billions " +
        "FROM sec_dji.financial_line_items " +
        "WHERE company_name IN ('Apple Inc', 'Microsoft Corporation', " +
        "                        'Intel Corporation', 'Cisco Systems Inc') " +
        "  AND concept_name IN ('Revenues', 'NetIncome', 'Assets') " +
        "  AND filing_type = '10-K' " +
        "  AND filing_date >= '2023-01-01' " +
        "ORDER BY company_name, concept_name";
      
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(techQuery)) {
        
        String lastCompany = "";
        while (rs.next()) {
          String company = rs.getString("company_name");
          if (!company.equals(lastCompany)) {
            if (!lastCompany.isEmpty()) System.out.println();
            System.out.println(company + ":");
            lastCompany = company;
          }
          System.out.printf("  %-15s: $%.1fB\n",
            rs.getString("concept_name"),
            rs.getDouble("value_billions"));
        }
      }
      
      System.out.println("\n" + "=".repeat(80));
      System.out.println("✓ Successfully queried DJI 5-year SEC data");
      System.out.println("=".repeat(80));
    }
  }
}