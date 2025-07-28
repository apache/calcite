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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects and extracts partition information from file paths.
 */
public class PartitionDetector {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionDetector.class);

  // Pattern for Hive-style partitions: key=value
  private static final Pattern HIVE_PARTITION_PATTERN =
      Pattern.compile("([^/=]+)=([^/]+)");

  /**
   * Partition information extracted from a file path.
   */
  public static class PartitionInfo {
    private final Map<String, String> partitionValues;
    private final List<String> partitionColumns;
    private final boolean isHiveStyle;

    public PartitionInfo(Map<String, String> partitionValues,
                         List<String> partitionColumns,
                         boolean isHiveStyle) {
      this.partitionValues = partitionValues;
      this.partitionColumns = partitionColumns;
      this.isHiveStyle = isHiveStyle;
    }

    public Map<String, String> getPartitionValues() {
      return partitionValues;
    }

    public List<String> getPartitionColumns() {
      return partitionColumns;
    }

    public boolean isHiveStyle() {
      return isHiveStyle;
    }
  }

  /**
   * Auto-detects partition scheme from file paths.
   *
   * @param filePaths List of file paths to analyze
   * @return Detected partition information, or null if no consistent scheme found
   */
  public static PartitionInfo detectPartitionScheme(List<String> filePaths) {
    if (filePaths == null || filePaths.isEmpty()) {
      return null;
    }

    // Check if files use Hive-style partitioning
    boolean allHiveStyle = true;
    List<String> commonColumns = null;

    for (String filePath : filePaths) {
      PartitionInfo info = extractHivePartitions(filePath);
      if (info == null || info.getPartitionColumns().isEmpty()) {
        allHiveStyle = false;
        break;
      }

      if (commonColumns == null) {
        commonColumns = new ArrayList<>(info.getPartitionColumns());
      } else {
        // Verify same partition columns across all files
        if (!commonColumns.equals(info.getPartitionColumns())) {
          LOGGER.warn("Inconsistent partition columns detected. Expected: {}, Found: {}",
              commonColumns, info.getPartitionColumns());
          allHiveStyle = false;
          break;
        }
      }
    }

    if (allHiveStyle && commonColumns != null) {
      LOGGER.info("Detected Hive-style partitioning with columns: {}", commonColumns);
      return new PartitionInfo(new LinkedHashMap<>(), commonColumns, true);
    }

    LOGGER.info("No Hive-style partitioning detected for {} files", filePaths.size());
    return null;
  }

  /**
   * Extracts Hive-style partition information from a file path.
   *
   * @param filePath File path to analyze
   * @return Partition information if Hive-style, null otherwise
   */
  public static PartitionInfo extractHivePartitions(String filePath) {
    Map<String, String> partitionValues = new LinkedHashMap<>();
    List<String> partitionColumns = new ArrayList<>();

    Path path = Paths.get(filePath);
    Path parent = path.getParent();

    while (parent != null && parent.getFileName() != null) {
      String dirName = parent.getFileName().toString();
      Matcher matcher = HIVE_PARTITION_PATTERN.matcher(dirName);

      if (matcher.matches()) {
        String key = matcher.group(1);
        String value = matcher.group(2);
        // Insert at beginning to maintain correct order
        partitionValues.put(key, value);
        partitionColumns.add(0, key);
      }

      parent = parent.getParent();
    }

    if (partitionColumns.isEmpty()) {
      return null;
    }

    return new PartitionInfo(partitionValues, partitionColumns, true);
  }

  /**
   * Extracts partition values based on directory structure.
   *
   * @param filePath File path to analyze
   * @param columnNames Names to assign to directory levels
   * @return Partition information
   */
  public static PartitionInfo extractDirectoryPartitions(String filePath,
                                                         List<String> columnNames) {
    if (columnNames == null || columnNames.isEmpty()) {
      return null;
    }

    Map<String, String> partitionValues = new LinkedHashMap<>();
    Path path = Paths.get(filePath);
    Path parent = path.getParent();

    List<String> dirValues = new ArrayList<>();
    while (parent != null && parent.getFileName() != null && dirValues.size() < columnNames.size()) {
      dirValues.add(0, parent.getFileName().toString());
      parent = parent.getParent();
    }

    // Match directory values with column names from the end
    int startIdx = Math.max(0, dirValues.size() - columnNames.size());
    for (int i = 0; i < columnNames.size() && (i + startIdx) < dirValues.size(); i++) {
      partitionValues.put(columnNames.get(i), dirValues.get(i + startIdx));
    }

    return new PartitionInfo(partitionValues, columnNames, false);
  }

  /**
   * Extracts partition values using a custom regex pattern.
   *
   * @param filePath File path to analyze
   * @param regex Regular expression pattern
   * @param columnMappings Column mappings for regex groups
   * @return Partition information
   */
  public static PartitionInfo extractCustomPartitions(String filePath, String regex,
                                                      List<PartitionedTableConfig.ColumnMapping> columnMappings) {
    if (regex == null || columnMappings == null) {
      return null;
    }

    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(filePath);

    if (!matcher.find()) {
      return null;
    }

    Map<String, String> partitionValues = new LinkedHashMap<>();
    List<String> partitionColumns = new ArrayList<>();

    for (PartitionedTableConfig.ColumnMapping mapping : columnMappings) {
      try {
        String value = matcher.group(mapping.getGroup());
        partitionValues.put(mapping.getName(), value);
        partitionColumns.add(mapping.getName());
      } catch (Exception e) {
        LOGGER.warn("Failed to extract partition value for column: {}", mapping.getName());
      }
    }

    return new PartitionInfo(partitionValues, partitionColumns, false);
  }
}
