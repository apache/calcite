package temp;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.DatabaseMetaData;
import java.util.Properties;

/**
 * Test to discover what tables are available with LINQ4J engine
 */
@Tag("temp")
public class TableDiscoveryTest {

    @Test
    void discoverAllTablesWithLinq4j() throws Exception {
        System.out.println("=== TABLE DISCOVERY TEST WITH LINQ4J ENGINE ===");
        
        // Force LINQ4J engine
        System.setProperty("CALCITE_FILE_ENGINE_TYPE", "LINQ4J");
        
        Properties info = new Properties();
        info.setProperty("lex", "ORACLE");
        info.setProperty("unquotedCasing", "TO_LOWER");
        
        String modelPath = "src/test/resources/smart.json";
        String jdbcUrl = "jdbc:calcite:model=" + modelPath;
        
        System.out.println("JDBC URL: " + jdbcUrl);
        System.out.println("Engine: " + System.getProperty("CALCITE_FILE_ENGINE_TYPE"));
        
        try (Connection connection = DriverManager.getConnection(jdbcUrl, info)) {
            System.out.println("✅ Connection established");
            
            DatabaseMetaData metaData = connection.getMetaData();
            
            // List all schemas
            System.out.println("\n=== ALL SCHEMAS ===");
            int schemaCount = 0;
            try (ResultSet schemas = metaData.getSchemas()) {
                while (schemas.next()) {
                    String schemaName = schemas.getString("TABLE_SCHEM");
                    String catalog = schemas.getString("TABLE_CATALOG");
                    System.out.println("Schema: " + schemaName + " (catalog: " + catalog + ")");
                    schemaCount++;
                }
            }
            System.out.println("Total schemas found: " + schemaCount);
            
            // List all tables across all schemas
            System.out.println("\n=== ALL TABLES (ALL SCHEMAS) ===");
            int tableCount = 0;
            try (ResultSet tables = metaData.getTables(null, null, null, null)) {
                while (tables.next()) {
                    String catalog = tables.getString("TABLE_CAT");
                    String schema = tables.getString("TABLE_SCHEM");
                    String table = tables.getString("TABLE_NAME");
                    String type = tables.getString("TABLE_TYPE");
                    System.out.println("Table: " + 
                        (catalog != null ? catalog + "." : "") + 
                        (schema != null ? schema + "." : "") + 
                        table + " (type: " + type + ")");
                    tableCount++;
                }
            }
            System.out.println("Total tables found: " + tableCount);
            
            // Specifically look for tables in SALES schema
            System.out.println("\n=== TABLES IN SALES SCHEMA ===");
            int salesTableCount = 0;
            try (ResultSet salesTables = metaData.getTables(null, "SALES", null, null)) {
                while (salesTables.next()) {
                    String table = salesTables.getString("TABLE_NAME");
                    String type = salesTables.getString("TABLE_TYPE");
                    System.out.println("SALES table: " + table + " (type: " + type + ")");
                    salesTableCount++;
                }
            }
            System.out.println("Total tables in SALES schema: " + salesTableCount);
            
            // Try different case variations for schema name
            System.out.println("\n=== CASE VARIATIONS CHECK ===");
            String[] schemaCases = {"SALES", "sales", "Sales"};
            for (String schemaCase : schemaCases) {
                System.out.println("Checking schema: " + schemaCase);
                try (ResultSet tables = metaData.getTables(null, schemaCase, null, null)) {
                    int count = 0;
                    while (tables.next()) {
                        String table = tables.getString("TABLE_NAME");
                        System.out.println("  - " + table);
                        count++;
                    }
                    System.out.println("  Found " + count + " tables");
                }
            }
            
        } catch (Exception e) {
            System.out.println("❌ Failed: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }
}