import java.sql.*;

public class TestDirectRead {
  public static void main(String[] args) throws Exception {
    System.setProperty("calcite.file.engine.type", "PARQUET");
    Class.forName("org.apache.calcite.adapter.file.FileJdbcDriver");
    
    String model = "{"
      + "  \"version\": \"1.0\","
      + "  \"defaultSchema\": \"test\","
      + "  \"schemas\": [{"
      + "    \"name\": \"test\","
      + "    \"type\": \"custom\","
      + "    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\","
      + "    \"operand\": {"
      + "      \"executionEngine\": \"PARQUET\","
      + "      \"directory\": \"build/resources/test/csv-type-inference\","
      + "      \"csvTypeInference\": {"
      + "        \"enabled\": true,"
      + "        \"blankStringsAsNull\": false"
      + "      }"
      + "    }"
      + "  }]"
      + "}";
    
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT * FROM \"test\".\"blank_strings\" ORDER BY \"id\"")) {
      
      // Check row 2 - should have empty string for name
      rs.next(); // row 1
      rs.next(); // row 2
      String name = rs.getString("name");
      System.out.println("Row 2 name value: '" + name + "'");
      System.out.println("Row 2 name is null: " + (name == null));
      System.out.println("Row 2 name is empty: " + (name != null && name.isEmpty()));
    }
  }
}
