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

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for Arrow file format support in the file adapter.
 */
@SuppressWarnings("deprecation")
public class ArrowFileTest {

  @TempDir
  public File tempDir;

  private static BufferAllocator allocator;

  @BeforeAll
  public static void setUp() {
    allocator = new RootAllocator();
  }

  @Test void testBasicArrowFile() throws Exception {
    // Create a simple Arrow file
    File arrowFile = new File(tempDir, "test.arrow");
    createSampleArrowFile(arrowFile);

    // Create model
    String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'ARROW',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'ARROW',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        executionEngine: 'arrow',\n"
        + "        tableNameCasing: 'LOWER',\n"
        + "        columnNameCasing: 'LOWER'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
         Statement stmt = conn.createStatement()) {

      // Test SELECT *
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM \"test\" ORDER BY \"id\"")) {
        assertTrue(rs.next());
        assertEquals(1L, rs.getLong("id"));
        assertEquals("Alice", rs.getString("name"));
        assertEquals(30.5, rs.getDouble("score"), 0.001);

        assertTrue(rs.next());
        assertEquals(2L, rs.getLong("id"));
        assertEquals("Bob", rs.getString("name"));
        assertEquals(25.0, rs.getDouble("score"), 0.001);

        assertTrue(rs.next());
        assertEquals(3L, rs.getLong("id"));
        assertEquals("Charlie", rs.getString("name"));
        assertEquals(35.7, rs.getDouble("score"), 0.001);

        assertThat(rs.next(), is(false));
      }

      // Test with WHERE clause
      try (ResultSet rs = stmt.executeQuery("SELECT \"name\" FROM \"test\" WHERE \"score\" > 30")) {
        assertTrue(rs.next());
        assertEquals("Alice", rs.getString(1));

        assertTrue(rs.next());
        assertEquals("Charlie", rs.getString(1));

        assertThat(rs.next(), is(false));
      }

      // Test aggregation
      try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*), AVG(\"score\") FROM \"test\"")) {
        assertTrue(rs.next());
        assertEquals(3L, rs.getLong(1));
        assertEquals(30.4, rs.getDouble(2), 0.001);
        assertThat(rs.next(), is(false));
      }
    }
  }

  @Test void testArrowWithParquetEngine() throws Exception {
    // Create Arrow file
    File arrowFile = new File(tempDir, "data.arrow");
    createSampleArrowFile(arrowFile);

    // Test with PARQUET execution engine (should auto-convert)
    String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'ARROW',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'ARROW',\n"
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
      try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"data\"")) {
        assertTrue(rs.next());
        assertEquals(3L, rs.getLong(1));
      }

      // Check that Parquet cache was created
      File cacheDir = new File(tempDir, ".parquet_cache");
      assertTrue(cacheDir.exists());
      File[] parquetFiles = cacheDir.listFiles((dir, name) -> name.endsWith(".parquet"));
      assertThat(parquetFiles, arrayWithSize(1));
    }
  }

  @Test void testArrowGlobPattern() throws Exception {
    // Create multiple Arrow files
    createSampleArrowFile(new File(tempDir, "data1.arrow"));
    createSampleArrowFile(new File(tempDir, "data2.arrow"));
    createSampleArrowFile(new File(tempDir, "other.txt")); // Should be ignored

    String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'ARROW',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'ARROW',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "',\n"
        + "        glob: '*.arrow',\n"
        + "        executionEngine: 'arrow',\n"
        + "        tableNameCasing: 'LOWER',\n"
        + "        columnNameCasing: 'LOWER'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
         Statement stmt = conn.createStatement()) {

      // Should see both arrow files as tables
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("ARROW");
      assertThat(schema.getTableNames(), hasSize(2));
      assertTrue(schema.getTableNames().contains("data1"));
      assertTrue(schema.getTableNames().contains("data2"));
    }
  }

  @Test void testArrowWithNullValues() throws Exception {
    File arrowFile = new File(tempDir, "nulls.arrow");
    createArrowFileWithNulls(arrowFile);

    String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'ARROW',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'ARROW',\n"
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

      try (ResultSet rs = stmt.executeQuery("SELECT * FROM \"nulls\" ORDER BY \"id\"")) {
        assertTrue(rs.next());
        assertEquals(1L, rs.getLong("id"));
        assertEquals("Alice", rs.getString("name"));
        assertThat(rs.wasNull(), is(false));

        assertTrue(rs.next());
        assertEquals(2L, rs.getLong("id"));
        assertThat(rs.getString("name"), is((String) null));
        assertThat(rs.wasNull(), is(true));

        assertTrue(rs.next());
        assertEquals(3L, rs.getLong("id"));
        assertEquals("Charlie", rs.getString("name"));
        assertThat(rs.wasNull(), is(false));

        assertThat(rs.next(), is(false));
      }
    }
  }

  private void createSampleArrowFile(File file) throws IOException {
    Schema schema =
        new Schema(
            Arrays.asList(new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
        new Field(
            "score", FieldType.nullable(
                new ArrowType.FloatingPoint(
            org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)), null)));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
         FileOutputStream fos = new FileOutputStream(file);
         ArrowFileWriter writer = new ArrowFileWriter(root, null, fos.getChannel())) {

      BigIntVector idVector = (BigIntVector) root.getVector("id");
      VarCharVector nameVector = (VarCharVector) root.getVector("name");
      Float8Vector scoreVector = (Float8Vector) root.getVector("score");

      // Write batch of data
      idVector.allocateNew(3);
      nameVector.allocateNew();
      scoreVector.allocateNew(3);

      idVector.set(0, 1L);
      nameVector.set(0, "Alice".getBytes(StandardCharsets.UTF_8));
      scoreVector.set(0, 30.5);

      idVector.set(1, 2L);
      nameVector.set(1, "Bob".getBytes(StandardCharsets.UTF_8));
      scoreVector.set(1, 25.0);

      idVector.set(2, 3L);
      nameVector.set(2, "Charlie".getBytes(StandardCharsets.UTF_8));
      scoreVector.set(2, 35.7);

      root.setRowCount(3);

      writer.start();
      writer.writeBatch();
      writer.end();
    }
  }

  private void createArrowFileWithNulls(File file) throws IOException {
    Schema schema =
        new Schema(
            Arrays.asList(new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
         FileOutputStream fos = new FileOutputStream(file);
         ArrowFileWriter writer = new ArrowFileWriter(root, null, fos.getChannel())) {

      BigIntVector idVector = (BigIntVector) root.getVector("id");
      VarCharVector nameVector = (VarCharVector) root.getVector("name");

      idVector.allocateNew(3);
      nameVector.allocateNew();

      idVector.set(0, 1L);
      nameVector.set(0, "Alice".getBytes(StandardCharsets.UTF_8));

      idVector.set(1, 2L);
      // nameVector not set for index 1 - will be null

      idVector.set(2, 3L);
      nameVector.set(2, "Charlie".getBytes(StandardCharsets.UTF_8));

      idVector.setValueCount(3);
      nameVector.setValueCount(3);
      root.setRowCount(3);

      writer.start();
      writer.writeBatch();
      writer.end();
    }
  }
}
