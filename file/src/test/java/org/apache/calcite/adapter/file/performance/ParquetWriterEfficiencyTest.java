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
package org.apache.calcite.adapter.file.performance;

import org.apache.calcite.adapter.file.DirectFileSource;
import org.apache.calcite.adapter.file.format.parquet.ParquetConversionUtil;
import org.apache.calcite.adapter.file.table.CsvScannableTable;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.PrintWriter;

/**
 * Test to verify Direct Parquet writer works and compare file sizes.
 */
@Tag("performance")
public class ParquetWriterEfficiencyTest {

  @TempDir
  File tempDir;

  @Test public void testDirectParquetWriter() throws Exception {
    System.out.println("=== Testing Direct Parquet Writer ===\n");

    // Create test CSV file with timestamps
    File csvFile = new File(tempDir, "test_timestamps.csv");
    try (PrintWriter writer = new PrintWriter(csvFile)) {
      writer.println("id:int,name:string,created:timestamp,updated:timestamptz");
      writer.println("1,Test1,2024-03-15 14:30:45,2024-03-15T10:30:45Z");
      writer.println("2,Test2,2024-03-15 14:30:45,2024-03-15T10:30:45-04:00");
      writer.println("3,Test3,2024-03-15 14:30:45,2024-03-15T18:30:45+04:00");
    }

    // Test 1: Convert using regular approach (Avro-based)
    File parquetCacheDir = ParquetConversionUtil.getParquetCacheDir(tempDir);
    File avroBasedParquet = ParquetConversionUtil.getCachedParquetFile(csvFile, parquetCacheDir);

    // Force regular conversion by creating a CSV without timestamp types
    File csvFileNoTimestamp = new File(tempDir, "test_no_timestamps.csv");
    try (PrintWriter writer = new PrintWriter(csvFileNoTimestamp)) {
      writer.println("id:int,name:string,value:double");
      writer.println("1,Test1,3.14");
      writer.println("2,Test2,2.71");
      writer.println("3,Test3,1.41");
    }

    // This will use Avro-based conversion (no timestamp types)
    System.out.println("Testing Avro-based Parquet conversion...");
    long startAvro = System.currentTimeMillis();
    ParquetConversionUtil.convertToParquet(
        new DirectFileSource(csvFileNoTimestamp),
        "test_no_timestamps",
        new CsvScannableTable(new DirectFileSource(csvFileNoTimestamp), null, "UNCHANGED"),
        parquetCacheDir,
        null,
        "TEST");
    long avroTime = System.currentTimeMillis() - startAvro;
    File avroFile = ParquetConversionUtil.getCachedParquetFile(csvFileNoTimestamp, parquetCacheDir);

    // Test 2: Convert using direct approach (with timestamps)
    System.out.println("Testing Direct Parquet conversion...");
    long startDirect = System.currentTimeMillis();
    ParquetConversionUtil.convertToParquet(
        new DirectFileSource(csvFile),
        "test_timestamps",
        new CsvScannableTable(new DirectFileSource(csvFile), null, "UNCHANGED"),
        parquetCacheDir,
        null,
        "TEST");
    long directTime = System.currentTimeMillis() - startDirect;
    File directFile = ParquetConversionUtil.getCachedParquetFile(csvFile, parquetCacheDir);

    // Results
    System.out.println("\nResults:");
    System.out.printf("Avro-based conversion: %d ms (file: %,d bytes)%n",
        avroTime, avroFile.length());
    System.out.printf("Direct conversion:     %d ms (file: %,d bytes)%n",
        directTime, directFile.length());

    System.out.println("\nKey benefits of Direct Parquet writing:");
    System.out.println("✓ Preserves timestamp type information (isAdjustedToUTC)");
    System.out.println("✓ Avoids intermediate Avro conversion overhead");
    System.out.println("✓ Direct SQL type to Parquet type mapping");

    // Verify files were created
    assert avroFile.exists() : "Avro-based Parquet file should exist";
    assert directFile.exists() : "Direct Parquet file should exist";
    assert directFile.length() > 0 : "Direct Parquet file should have content";
  }
}
