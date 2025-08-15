import java.io.*;
import java.sql.*;
import java.util.*;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.adapter.file.FileSchemaFactory;

public class TestCsvRefresh {
    public static void main(String[] args) throws Exception {
        File tempDir = new File("/tmp/csv_refresh_test");
        tempDir.mkdirs();
        
        // Create initial CSV file
        File csvFile = new File(tempDir, "data.csv");
        try (FileWriter writer = new FileWriter(csvFile)) {
            writer.write("id:int,name:string\n");
            writer.write("1,Alice\n");
        }
        
        // Create schema with refresh interval
        Map<String, Object> operand = new HashMap<>();
        operand.put("directory", tempDir.toString());
        operand.put("refreshInterval", "1 second");
        operand.put("executionEngine", "parquet");
        
        Properties connectionProps = new Properties();
        connectionProps.setProperty("lex", "ORACLE");
        connectionProps.setProperty("unquotedCasing", "TO_LOWER");
        connectionProps.setProperty("caseSensitive", "false");
        
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", connectionProps);
             CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {
            
            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            SchemaPlus fileSchema = rootSchema.add("test", 
                FileSchemaFactory.INSTANCE.create(rootSchema, "test", operand));
            
            // Query initial data
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM test.data")) {
                rs.next();
                System.out.println("Initial: id=" + rs.getInt("id") + ", name=" + rs.getString("name"));
            }
            
            // Update CSV file
            Thread.sleep(1100);
            try (FileWriter writer = new FileWriter(csvFile)) {
                writer.write("id:int,name:string\n");
                writer.write("10,Bob\n");
            }
            
            // Wait for refresh
            Thread.sleep(1100);
            
            // Query updated data
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM test.data WHERE id > 0")) {
                rs.next();
                System.out.println("After refresh: id=" + rs.getInt("id") + ", name=" + rs.getString("name"));
            }
            
            System.out.println("CSV refresh test completed successfully!");
        }
    }
}