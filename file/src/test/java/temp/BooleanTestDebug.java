package temp;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.DatabaseMetaData;
import java.util.Properties;

/**
 * Debug test for boolean handling in LINQ4J engine
 */
@Tag("temp")
public class BooleanTestDebug {

    @Test
    void debugBooleanTest() throws Exception {
        System.out.println("=== BOOLEAN TEST DEBUG WITH LINQ4J ENGINE ===");
        
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
            
            // List all schemas
            System.out.println("\n=== SCHEMAS ===");
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet schemas = metaData.getSchemas()) {
                while (schemas.next()) {
                    String schemaName = schemas.getString("TABLE_SCHEM");
                    System.out.println("Schema: " + schemaName);
                }
            }
            
            // List all tables
            System.out.println("\n=== TABLES ===");
            try (ResultSet tables = metaData.getTables(null, null, null, null)) {
                while (tables.next()) {
                    String schema = tables.getString("TABLE_SCHEM");
                    String table = tables.getString("TABLE_NAME");
                    String type = tables.getString("TABLE_TYPE");
                    System.out.println("Table: " + schema + "." + table + " (type: " + type + ")");
                }
            }
            
            // List columns for SALES.emps table
            System.out.println("\n=== SALES.EMPS COLUMNS ===");
            try (ResultSet columns = metaData.getColumns(null, "SALES", "emps", null)) {
                while (columns.next()) {
                    String columnName = columns.getString("COLUMN_NAME");
                    String dataType = columns.getString("TYPE_NAME");
                    int sqlType = columns.getInt("DATA_TYPE");
                    System.out.println("Column: " + columnName + " -> " + dataType + " (SQL type: " + sqlType + ")");
                }
            }
            
            // Try to query the table structure first
            System.out.println("\n=== QUERY STRUCTURE ===");
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM \"SALES\".emps LIMIT 1")) {
                
                ResultSetMetaData rsmd = rs.getMetaData();
                int columnCount = rsmd.getColumnCount();
                System.out.println("Columns found: " + columnCount);
                
                for (int i = 1; i <= columnCount; i++) {
                    String name = rsmd.getColumnName(i);
                    String type = rsmd.getColumnTypeName(i);
                    int sqlType = rsmd.getColumnType(i);
                    System.out.println("Column " + i + ": " + name + " -> " + type + " (SQL type: " + sqlType + ")");
                }
                
                // Show one row of data
                if (rs.next()) {
                    System.out.println("\n=== SAMPLE DATA ROW ===");
                    for (int i = 1; i <= columnCount; i++) {
                        Object value = rs.getObject(i);
                        System.out.println(rsmd.getColumnName(i) + " = " + value + " (class: " + 
                            (value != null ? value.getClass().getSimpleName() : "null") + ")");
                    }
                }
            }
            
            // Now try the actual boolean query
            System.out.println("\n=== BOOLEAN QUERY TEST ===");
            String sql = "SELECT \"empno\", \"slacker\" FROM \"SALES\".emps WHERE \"slacker\"";
            System.out.println("Query: " + sql);
            
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                
                System.out.println("Query executed successfully!");
                
                while (rs.next()) {
                    Object empno = rs.getObject("empno");
                    Object slacker = rs.getObject("slacker");
                    System.out.println("empno=" + empno + "; slacker=" + slacker);
                }
            } catch (Exception e) {
                System.out.println("❌ Query failed: " + e.getMessage());
                e.printStackTrace();
            }
            
        } catch (Exception e) {
            System.out.println("❌ Connection or setup failed: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }
}