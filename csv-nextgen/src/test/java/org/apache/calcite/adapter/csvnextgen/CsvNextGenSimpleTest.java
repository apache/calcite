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
package org.apache.calcite.adapter.csvnextgen;

import org.apache.calcite.test.CalciteAssert;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Test cases for simplified CsvNextGen adapter.
 * Demonstrates basic functionality before adding pluggable execution engines.
 */
public class CsvNextGenSimpleTest {

  @Test public void testBasicCsvQuery(@TempDir File tempDir) throws IOException {
    // Create test CSV file
    File csvFile = new File(tempDir, "test.csv");
    try (FileWriter writer = new FileWriter(csvFile, StandardCharsets.UTF_8)) {
      writer.write("id,name,value\n");
      writer.write("1,Alice,100\n");
      writer.write("2,Bob,200\n");
      writer.write("3,Charlie,300\n");
    }

    String model = "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"csvnextgen\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"csvnextgen\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.csvnextgen.CsvNextGenSchemaFactorySimple\",\n"
        + "      \"operand\": {\n"
        + "        \"directory\": \"" + tempDir.getAbsolutePath() + "\",\n"
        + "        \"engine\": \"linq4j\",\n"
        + "        \"batchSize\": 100,\n"
        + "        \"header\": true\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    CalciteAssert.model(model)
        .query("SELECT * FROM test")
        .returns("id=1; name=Alice; value=100\n"
            + "id=2; name=Bob; value=200\n"
            + "id=3; name=Charlie; value=300\n");
  }

  @Test public void testTsvSupport(@TempDir File tempDir) throws IOException {
    // Create test TSV file
    File tsvFile = new File(tempDir, "test.tsv");
    try (FileWriter writer = new FileWriter(tsvFile, StandardCharsets.UTF_8)) {
      writer.write("id\tname\tvalue\n");
      writer.write("1\tAlice\t100\n");
      writer.write("2\tBob\t200\n");
    }

    String model = "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"csvnextgen\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"csvnextgen\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.csvnextgen.CsvNextGenSchemaFactorySimple\",\n"
        + "      \"operand\": {\n"
        + "        \"directory\": \"" + tempDir.getAbsolutePath() + "\",\n"
        + "        \"engine\": \"linq4j\",\n"
        + "        \"header\": true\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    CalciteAssert.model(model)
        .query("SELECT * FROM test")
        .returns("id=1; name=Alice; value=100\n"
            + "id=2; name=Bob; value=200\n");
  }
}
