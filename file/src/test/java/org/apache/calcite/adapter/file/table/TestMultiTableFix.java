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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.converters.MultiTableExcelToJsonConverter;
import org.junit.jupiter.api.Test;

import java.io.File;

/**
 * Test the multi-table conversion fix.
 */
public class TestMultiTableFix {

  @Test public void testConversion() throws Exception {
    File testDir = new File("src/test/resources/testdata");
    File excelFile = new File(testDir, "lots_of_tables.xlsx");

    // Delete existing JSON files
    for (File f : testDir.listFiles()) {
      if (f.getName().endsWith(".json") && f.getName().startsWith("LotsOfTables")) {
        f.delete();
      }
    }

    // Run the converter
    MultiTableExcelToJsonConverter.convertFileToJson(excelFile, true);

    // Check generated files
    System.out.println("\nGenerated JSON files:");
    for (File f : testDir.listFiles()) {
      if (f.getName().endsWith(".json") && f.getName().startsWith("LotsOfTables")) {
        System.out.println("  " + f.getName());
      }
    }
  }
}
