import java.sql.*;
import java.util.Properties;

public class debug_table_names {
    public static void main(String[] args) throws Exception {
        Properties info = new Properties();
        
        String resourceDir = "/Users/kennethstott/ndc-calcite/calcite-rs-jni/calcite/file/src/test/resources/csv-type-inference";
        
        String modelJson = "{\n" +
            "  \"version\": \"1.0\",\n" +
            "  \"defaultSchema\": \"csv_blank_test\",\n" +
            "  \"schemas\": [\n" +
            "    {\n" +
            "      \"name\": \"csv_blank_test\",\n" +
            "      \"type\": \"custom\",\n" +
            "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n" +
            "      \"operand\": {\n" +
            "        \"directory\": \"" + resourceDir + "\",\n" +
            "        \"executionEngine\": \"PARQUET\",\n" +
            "        \"csvTypeInference\": {\n" +
            "          \"enabled\": true,\n" +
            "          \"samplingRate\": 1.0,\n" +
            "          \"maxSampleRows\": 100,\n" +
            "          \"makeAllNullable\": true,\n" +
            "          \"blankStringsAsNull\": false\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}";
        
        info.put("model", "inline:" + modelJson);
        info.put("lex", "ORACLE");
        info.put("unquotedCasing", "TO_LOWER");

        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet tables = metaData.getTables(null, "csv_blank_test", null, null);
            
            System.out.println("Tables found in csv_blank_test schema:");
            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                String tableType = tables.getString("TABLE_TYPE");
                System.out.println("  - " + tableName + " (" + tableType + ")");
            }
        }
    }
}