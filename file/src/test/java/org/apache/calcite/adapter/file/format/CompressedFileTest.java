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
package org.apache.calcite.adapter.file.format;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.zip.GZIPOutputStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for compressed file support in the file adapter.
 */
public class CompressedFileTest {

  @TempDir
  public File tempDir;

  @Test void testGzippedCsvFile() throws Exception {
    // Create a gzipped CSV file
    File gzFile = new File(tempDir, "employees.csv.gz");
    createGzippedCsv(gzFile);

    String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'COMPRESSED',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'COMPRESSED',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        tableNameCasing: 'LOWER',\n"
        + "        columnNameCasing: 'LOWER'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
         Statement stmt = conn.createStatement()) {

      // Test SELECT * from gzipped file
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM \"employees\" ORDER BY \"empno\"")) {
        assertTrue(rs.next());
        assertEquals(100, rs.getInt("EMPNO"));
        assertEquals("Fred", rs.getString("NAME"));
        assertEquals(10, rs.getInt("DEPTNO"));
        assertEquals("M", rs.getString("GENDER"));
        assertEquals("New York", rs.getString("CITY"));
        assertEquals(30, rs.getInt("AGE"));
        assertEquals(true, rs.getBoolean("SLACKER"));

        assertTrue(rs.next());
        assertEquals(110, rs.getInt("EMPNO"));
        assertEquals("Eric", rs.getString("NAME"));
        assertEquals(20, rs.getInt("DEPTNO"));

        assertTrue(rs.next());
        assertEquals(120, rs.getInt("EMPNO"));
        assertEquals("Wilma", rs.getString("NAME"));

        assertTrue(rs.next());
        assertEquals(130, rs.getInt("EMPNO"));
        assertEquals("Alice", rs.getString("NAME"));

        assertThat(rs.next(), is(false));
      }

      // Test aggregation on gzipped file
      try (ResultSet rs =
          stmt.executeQuery("SELECT COUNT(*) as cnt, AVG(\"age\") as avg_age FROM \"employees\"")) {
        assertTrue(rs.next());
        assertEquals(4L, rs.getLong("cnt"));
        assertEquals(40.0, rs.getDouble("avg_age"), 0.001);
        assertThat(rs.next(), is(false));
      }
    }
  }

  @Test void testGzippedJsonFile() throws Exception {
    // Create a gzipped JSON file
    File gzFile = new File(tempDir, "products.json.gz");
    createGzippedJson(gzFile);

    String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'COMPRESSED',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'COMPRESSED',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        tableNameCasing: 'LOWER',\n"
        + "        columnNameCasing: 'LOWER'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
         Statement stmt = conn.createStatement()) {

      // Test SELECT from gzipped JSON
      try (ResultSet rs =
          stmt.executeQuery("SELECT * FROM \"products\" WHERE \"price\" > 20 ORDER BY \"id\"")) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("id"));
        assertEquals("Laptop", rs.getString("name"));
        assertEquals(999.99, rs.getDouble("price"), 0.001);

        assertTrue(rs.next());
        assertEquals(3, rs.getInt("id"));
        assertEquals("Monitor", rs.getString("name"));
        assertEquals(299.99, rs.getDouble("price"), 0.001);

        assertThat(rs.next(), is(false));
      }
    }
  }

  @Test void testGzippedWithParquetEngine() throws Exception {
    File gzFile = new File(tempDir, "data.csv.gz");
    createGzippedCsv(gzFile);

    // Test with PARQUET execution engine
    String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'COMPRESSED',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'COMPRESSED',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        executionEngine: 'parquet',\n"
        + "        tableNameCasing: 'LOWER',\n"
        + "        columnNameCasing: 'LOWER'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
         Statement stmt = conn.createStatement()) {

      // Should work with PARQUET engine via auto-conversion
      try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM \"data\"")) {
        assertTrue(rs.next());
        assertEquals(4L, rs.getLong("cnt"));
      }

      // Check that Parquet cache was created
      File cacheDir = new File(tempDir, ".parquet_cache");
      assertTrue(cacheDir.exists());
    }
  }

  @Test void testMultipleCompressedFiles() throws Exception {
    // Create multiple compressed files of different types
    createGzippedCsv(new File(tempDir, "employees.csv.gz"));
    createGzippedJsonForJoin(new File(tempDir, "products.json.gz"));

    String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'COMPRESSED',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'COMPRESSED',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        glob: '*.gz',\n"
        + "        tableNameCasing: 'LOWER',\n"
        + "        columnNameCasing: 'LOWER'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
         Statement stmt = conn.createStatement()) {

      // Test join between compressed CSV and JSON
      try (ResultSet rs =
          stmt.executeQuery("SELECT e.\"name\", p.\"name\" as product "
          + "FROM \"employees\" e, \"products\" p "
          + "WHERE e.\"empno\" = p.\"id\" + 98 "
          + "ORDER BY e.\"empno\"")) {
        assertTrue(rs.next());
        assertEquals("Fred", rs.getString("NAME"));
        assertEquals("Laptop", rs.getString("product"));

        assertTrue(rs.next());
        assertEquals("Eric", rs.getString("NAME"));
        assertEquals("Monitor", rs.getString("product"));

        assertThat(rs.next(), is(false));
      }
    }
  }

  private void createGzippedCsv(File file) throws IOException {
    String csv = "EMPNO:int,NAME:string,DEPTNO:int,GENDER:string,CITY:string,AGE:int,SLACKER:boolean\n"
        + "100,Fred,10,M,New York,30,true\n"
        + "110,Eric,20,M,San Francisco,40,true\n"
        + "120,Wilma,20,F,Los Angeles,50,false\n"
        + "130,Alice,30,F,Vancouver,40,false\n";

    try (FileOutputStream fos = new FileOutputStream(file);
         GZIPOutputStream gzos = new GZIPOutputStream(fos);
         OutputStreamWriter writer = new OutputStreamWriter(gzos, StandardCharsets.UTF_8)) {
      writer.write(csv);
    }
  }

  private void createGzippedJson(File file) throws IOException {
    String json = "[\n"
        + "  {\"id\": 1, \"name\": \"Widget\", \"price\": 19.99, \"inStock\": true},\n"
        + "  {\"id\": 2, \"name\": \"Laptop\", \"price\": 999.99, \"inStock\": false},\n"
        + "  {\"id\": 3, \"name\": \"Monitor\", \"price\": 299.99, \"inStock\": true}\n"
        + "]\n";

    try (FileOutputStream fos = new FileOutputStream(file);
         GZIPOutputStream gzos = new GZIPOutputStream(fos);
         OutputStreamWriter writer = new OutputStreamWriter(gzos, StandardCharsets.UTF_8)) {
      writer.write(json);
    }
  }

  private void createGzippedJsonForJoin(File file) throws IOException {
    String json = "[\n"
        + "  {\"id\": 1, \"name\": \"Widget\", \"price\": 19.99, \"inStock\": true},\n"
        + "  {\"id\": 2, \"name\": \"Laptop\", \"price\": 999.99, \"inStock\": false},\n"
        + "  {\"id\": 12, \"name\": \"Monitor\", \"price\": 299.99, \"inStock\": true}\n"
        + "]\n";

    try (FileOutputStream fos = new FileOutputStream(file);
         GZIPOutputStream gzos = new GZIPOutputStream(fos);
         OutputStreamWriter writer = new OutputStreamWriter(gzos, StandardCharsets.UTF_8)) {
      writer.write(json);
    }
  }
}
