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
package org.apache.calcite.adapter.file.materialized;

import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;

import java.util.Locale;

/**
 * Utility methods for materialized view handling.
 */
public class MaterializedViewUtil {

  private MaterializedViewUtil() {
    // Prevent instantiation
  }

  /**
   * Gets the appropriate file extension for a materialized view based on the execution engine.
   *
   * @param engineType The execution engine type
   * @return The file extension to use (without the dot)
   */
  public static String getFileExtension(String engineType) {
    if (engineType == null) {
      return "csv";
    }

    switch (engineType.toUpperCase(Locale.ROOT)) {
    case "PARQUET":
      return "parquet";
    case "ARROW":
      return "arrow";
    case "VECTORIZED":
      // Vectorized engine can use either CSV or Arrow format
      // Default to CSV for simplicity
      return "csv";
    case "LINQ4J":
    default:
      return "csv";
    }
  }

  /**
   * Gets the filename for a materialized view with the appropriate extension.
   *
   * @param tableName The name of the materialized view
   * @param engineConfig The execution engine configuration
   * @return The filename with extension
   */
  public static String getMaterializedViewFilename(String tableName,
      ExecutionEngineConfig engineConfig) {
    String extension = getFileExtension(engineConfig.getEngineType().name());
    return tableName + "." + extension;
  }

  /**
   * Checks if a file is a materialized view based on its extension and engine type.
   *
   * @param filename The filename to check
   * @param engineType The execution engine type
   * @return true if the file could be a materialized view for this engine
   */
  public static boolean isMaterializedViewFile(String filename, String engineType) {
    String expectedExtension = getFileExtension(engineType);
    return filename.endsWith("." + expectedExtension);
  }
}
