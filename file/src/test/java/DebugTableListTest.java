/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;

public class DebugTableListTest {
    public static void main(String[] args) throws Exception {
        System.setProperty("CALCITE_FILE_ENGINE_TYPE", "PARQUET");

        Properties info = new Properties();

        String resourceDir = DebugTableListTest.class.getResource("/csv-type-inference").getFile();

        String modelJson = "{\n"
            + "  \"version\": \"1.0\",\n"
            + "  \"defaultSchema\": \"csv_blank_test\",\n"
            + "  \"schemas\": [\n"
            + "    {\n"
            + "      \"name\": \"csv_blank_test\",\n"
            + "      \"type\": \"custom\",\n"
            + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
            + "      \"operand\": {\n"
            + "        \"directory\": \"" + resourceDir + "\",\n"
            + "        \"executionEngine\": \"PARQUET\",\n"
            + "        \"csvTypeInference\": {\n"
            + "          \"enabled\": true,\n"
            + "          \"samplingRate\": 1.0,\n"
            + "          \"maxSampleRows\": 100,\n"
            + "          \"makeAllNullable\": true,\n"
            + "          \"blankStringsAsNull\": false\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  ]\n"
            + "}\n";

        info.put("model", "inline:" + modelJson);
        info.put("lex", "ORACLE");
        info.put("unquotedCasing", "TO_LOWER");

        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
            DatabaseMetaData metaData = connection.getMetaData();

            System.out.println("=== SCHEMAS ===");
            ResultSet schemas = metaData.getSchemas();
            while (schemas.next()) {
                System.out.println("Schema: " + schemas.getString("TABLE_SCHEM"));
            }
            schemas.close();

            System.out.println("\n=== TABLES IN csv_blank_test ===");
            ResultSet tables = metaData.getTables(null, "csv_blank_test", "%", null);
            while (tables.next()) {
                System.out.println("Table: " + tables.getString("TABLE_NAME") + " (" + tables.getString("TABLE_TYPE") + ")");
            }
            tables.close();

            System.out.println("\n=== ALL TABLES ===");
            ResultSet allTables = metaData.getTables(null, null, "%", null);
            while (allTables.next()) {
                System.out.println("Schema: " + allTables.getString("TABLE_SCHEM") + ", Table: " + allTables.getString("TABLE_NAME"));
            }
            allTables.close();
        }
    }
}
