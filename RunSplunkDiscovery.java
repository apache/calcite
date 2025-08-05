import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Standalone demo to show Splunk data model discovery.
 */
public class RunSplunkDiscovery {
  
  public static void main(String[] args) throws Exception {
    String url = "jdbc:splunk:" +
        "url='https://kentest.xyz:8089';" +
        "user='admin';" +
        "password='admin123';" +
        "disableSslValidation='true';" +
        "app='Splunk_SA_CIM';" +
        "datamodelCacheTtl=0";

    System.out.println("Connecting to Splunk...");
    
    try (Connection conn = DriverManager.getConnection(url)) {
      System.out.println("=== Data Model Discovery Results ===\n");
      
      // Query all discovered tables using metadata
      try (Statement stmt = conn.createStatement()) {
        String query = "SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname = 'splunk' ORDER BY tablename";
        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.println("Discovered Data Models (Tables):");
          System.out.println("Schema\t\tTable Name");
          System.out.println("------\t\t----------");
          
          while (rs.next()) {
            String schema = rs.getString("schemaname");
            String table = rs.getString("tablename");
            System.out.printf("%-15s %s%n", schema, table);
          }
        }
      }
      
      System.out.println("\n=== Data Model Details ===\n");
      
      // Get detailed information about each data model
      try (Statement stmt = conn.createStatement()) {
        String tablesQuery = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'splunk' ORDER BY table_name";
        try (ResultSet tablesRs = stmt.executeQuery(tablesQuery)) {
          
          while (tablesRs.next()) {
            String tableName = tablesRs.getString("table_name");
            System.out.println("Data Model: " + tableName);
            System.out.println("Columns:");
            System.out.println("Name\t\t\tType\t\tNullable");
            System.out.println("----\t\t\t----\t\t--------");
            
            // Get column information for this table
            String columnsQuery = "SELECT column_name, data_type, is_nullable " +
                "FROM information_schema.columns " +
                "WHERE table_schema = 'splunk' AND table_name = '" + tableName + "' " +
                "ORDER BY ordinal_position";
            
            try (Statement colStmt = conn.createStatement();
                 ResultSet colRs = colStmt.executeQuery(columnsQuery)) {
              
              while (colRs.next()) {
                String colName = colRs.getString("column_name");
                String dataType = colRs.getString("data_type");
                String nullable = colRs.getString("is_nullable");
                System.out.printf("%-23s %-15s %s%n", colName, dataType, nullable);
              }
            }
            
            System.out.println("\n" + "=".repeat(80) + "\n");
          }
        }
      }
      
      System.out.println("\n=== Splunk-Specific Metadata ===\n");
      
      // Query Splunk indexes
      try (Statement stmt = conn.createStatement()) {
        String query = "SELECT index_name, is_internal FROM pg_catalog.splunk_indexes ORDER BY index_name";
        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.println("Splunk Indexes:");
          System.out.println("Index Name\t\tInternal");
          System.out.println("----------\t\t--------");
          
          while (rs.next()) {
            String indexName = rs.getString("index_name");
            boolean isInternal = rs.getBoolean("is_internal");
            System.out.printf("%-23s %s%n", indexName, isInternal ? "Yes" : "No");
          }
        }
      }
      
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace();
    }
  }
}