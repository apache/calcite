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

import java.util.List;
import java.util.Map;

/**
 * Configuration for a partitioned table that spans multiple files.
 */
public class PartitionedTableConfig {
  private final String name;
  private final String pattern;
  private final String type;
  private final PartitionConfig partitions;

  public PartitionedTableConfig(String name, String pattern, String type,
                                PartitionConfig partitions) {
    this.name = name;
    this.pattern = pattern;
    this.type = type != null ? type : "partitioned";
    this.partitions = partitions;
  }

  public String getName() {
    return name;
  }

  public String getPattern() {
    return pattern;
  }

  public String getType() {
    return type;
  }

  public PartitionConfig getPartitions() {
    return partitions;
  }

  /**
   * Configuration for partition scheme.
   */
  public static class PartitionConfig {
    private final String style;
    private final List<String> columns;
    private final List<ColumnDefinition> columnDefinitions;
    private final String regex;
    private final List<ColumnMapping> columnMappings;

    public PartitionConfig(String style, List<String> columns,
                           List<ColumnDefinition> columnDefinitions,
                           String regex, List<ColumnMapping> columnMappings) {
      this.style = style != null ? style : "auto";
      this.columns = columns;
      this.columnDefinitions = columnDefinitions;
      this.regex = regex;
      this.columnMappings = columnMappings;
    }

    public String getStyle() {
      return style;
    }

    public List<String> getColumns() {
      return columns;
    }

    public List<ColumnDefinition> getColumnDefinitions() {
      return columnDefinitions;
    }

    public String getRegex() {
      return regex;
    }

    public List<ColumnMapping> getColumnMappings() {
      return columnMappings;
    }
  }

  /**
   * Definition of a partition column with name and type.
   */
  public static class ColumnDefinition {
    private final String name;
    private final String type;

    public ColumnDefinition(String name, String type) {
      this.name = name;
      this.type = type != null ? type : "VARCHAR";
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }
  }

  /**
   * Maps a regex group to a column.
   */
  public static class ColumnMapping {
    private final String name;
    private final int group;
    private final String type;

    public ColumnMapping(String name, int group, String type) {
      this.name = name;
      this.group = group;
      this.type = type;
    }

    public String getName() {
      return name;
    }

    public int getGroup() {
      return group;
    }

    public String getType() {
      return type;
    }
  }

  /**
   * Creates a PartitionedTableConfig from a map (JSON deserialization).
   */
  @SuppressWarnings("unchecked")
  public static PartitionedTableConfig fromMap(Map<String, Object> map) {
    String name = (String) map.get("name");
    String pattern = (String) map.get("pattern");
    String type = (String) map.get("type");

    PartitionConfig partitionConfig = null;
    Map<String, Object> partitionsMap = (Map<String, Object>) map.get("partitions");
    if (partitionsMap != null) {
      String style = (String) partitionsMap.get("style");

      // Handle both simple string columns and object-based column definitions
      List<String> simpleColumns = null;
      List<ColumnDefinition> columnDefinitions = null;

      Object columnsObj = partitionsMap.get("columns");
      if (columnsObj instanceof List) {
        List<?> columnsList = (List<?>) columnsObj;
        if (!columnsList.isEmpty()) {
          Object firstElem = columnsList.get(0);
          if (firstElem instanceof String) {
            // Simple string array: ["year", "month", "day"]
            simpleColumns = (List<String>) columnsObj;
          } else if (firstElem instanceof Map) {
            // Object array: [{"name": "year", "type": "INTEGER"}, ...]
            columnDefinitions = ((List<Map<String, Object>>) columnsObj).stream()
                .map(
                    m -> new ColumnDefinition(
                    (String) m.get("name"),
                    (String) m.get("type")))
                .collect(java.util.stream.Collectors.toList());
            // Also create simple columns list for backward compatibility
            simpleColumns = columnDefinitions.stream()
                .map(ColumnDefinition::getName)
                .collect(java.util.stream.Collectors.toList());
          }
        }
      }

      String regex = (String) partitionsMap.get("regex");

      List<ColumnMapping> columnMappings = null;
      List<Map<String, Object>> mappingsList =
          (List<Map<String, Object>>) partitionsMap.get("columnMappings");
      if (mappingsList != null) {
        columnMappings = mappingsList.stream()
            .map(
                m -> new ColumnMapping(
                (String) m.get("name"),
                ((Number) m.get("group")).intValue(),
                (String) m.get("type")))
            .collect(java.util.stream.Collectors.toList());
      }

      partitionConfig =
          new PartitionConfig(style, simpleColumns,
              columnDefinitions, regex, columnMappings);
    }

    return new PartitionedTableConfig(name, pattern, type, partitionConfig);
  }
}
