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

import org.apache.calcite.schema.Table;

import java.io.File;
import java.util.Map;

/**
 * Debug test for recursive directory scanning.
 */
public class RecursiveDebugTest {

  public static void main(String[] args) {
    // Test with the actual nested directory
    String testDir = "/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/nested";
    File baseDir = new File(testDir);

    System.out.println("Testing recursive scanning on: " + baseDir.getAbsolutePath());
    System.out.println("Directory exists: " + baseDir.exists());
    System.out.println("Is directory: " + baseDir.isDirectory());

    // Create FileSchema with recursive enabled
    FileSchema schema =
        new FileSchema(null, "test", baseDir, null, new ExecutionEngineConfig(), true, null, null);

    // Get table map which triggers the scanning
    Map<String, Table> tables = schema.getTableMap();

    System.out.println("\nTables found: " + tables.size());
    for (String tableName : tables.keySet()) {
      System.out.println("  - " + tableName);
    }

    // Also test the driver's expected directory
    System.out.println("\n\nTesting with flat structure:");
    listFilesRecursively(baseDir, "");
  }

  private static void listFilesRecursively(File dir, String indent) {
    File[] files = dir.listFiles();
    if (files != null) {
      for (File file : files) {
        System.out.println(indent + (file.isDirectory() ? "[DIR] " : "[FILE] ") + file.getName());
        if (file.isDirectory()) {
          listFilesRecursively(file, indent + "  ");
        }
      }
    }
  }
}
