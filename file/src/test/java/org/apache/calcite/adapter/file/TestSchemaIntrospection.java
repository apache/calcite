package org.apache.calcite.adapter.file;

import org.junit.jupiter.api.Test;
import java.sql.*;
import java.util.Properties;

public class TestSchemaIntrospection {
  
  @Test
  void introspectCsvInferSchema() throws Exception {
    Properties info = new Properties();
    String engineType = "DUCKDB";
    String modelJson = buildModelJson(engineType);
    info.put("model", "inline:" + modelJson);
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");
    
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info)) {
      DatabaseMetaData metaData = conn.getMetaData();
      
      System.out.println("=== Introspecting csv_infer schema ===\n");
      
      // List all schemas
      System.out.println("Available schemas:");
      try (ResultSet schemas = metaData.getSchemas()) {
        while (schemas.next()) {
          System.out.println("  - " + schemas.getString("TABLE_SCHEM"));
        }
      }
      
      System.out.println("\nTables in csv_infer schema:");
      try (ResultSet tables = metaData.getTables(null, "csv_infer", "%", new String[]{"TABLE"})) {
        while (tables.next()) {
          String table = tables.getString("TABLE_NAME");
          System.out.println("  Table: " + table);
        }
      }
      
      // Also try with uppercase
      System.out.println("\nTables in CSV_INFER schema:");
      try (ResultSet tables = metaData.getTables(null, "CSV_INFER", "%", new String[]{"TABLE"})) {
        while (tables.next()) {
          String table = tables.getString("TABLE_NAME");
          System.out.println("  Table: " + table);
        }
      }
      
      // Try SQL query
      System.out.println("\nUsing INFORMATION_SCHEMA:");
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT table_schema, table_name FROM TABLE(INFORMATION_SCHEMA.TABLES())")) {
        while (rs.next()) {
          String schema = rs.getString("TABLE_SCHEMA");
          String table = rs.getString("TABLE_NAME");
          if (schema.toLowerCase().contains("csv")) {
            System.out.println("  Schema: " + schema + ", Table: " + table);
          }
        }
      }
    }
  }
  
  private static String buildModelJson(String engineType) {
    String resourceDir = CsvTypeInferenceTest.class.getResource("/csv-type-inference").getFile();
    
    StringBuilder model = new StringBuilder();
    model.append("{\n");
    model.append("  \"version\": \"1.0\",\n");
    model.append("  \"defaultSchema\": \"csv_infer\",\n");
    model.append("  \"schemas\": [\n");
    
    model.append("    {\n");
    model.append("      \"name\": \"csv_infer\",\n");
    model.append("      \"type\": \"custom\",\n");
    model.append("      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n");
    model.append("      \"operand\": {\n");
    model.append("        \"directory\": \"").append(resourceDir).append("\",\n");
    if (engineType != null && !engineType.isEmpty()) {
      model.append("        \"executionEngine\": \"").append(engineType).append("\",\n");
    }
    model.append("        \"csvTypeInference\": {\n");
    model.append("          \"enabled\": true,\n");
    model.append("          \"samplingRate\": 1.0,\n");
    model.append("          \"maxSampleRows\": 100,\n");
    model.append("          \"confidenceThreshold\": 0.9,\n");
    model.append("          \"makeAllNullable\": true,\n");
    model.append("          \"inferDates\": true,\n");
    model.append("          \"inferTimes\": true,\n");
    model.append("          \"inferTimestamps\": true\n");
    model.append("        }\n");
    model.append("      }\n");
    model.append("    }\n");
    
    model.append("  ]\n");
    model.append("}\n");
    
    return model.toString();
  }
}