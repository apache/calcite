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
package org.apache.calcite.adapter.file.schema;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Format-aware schema resolver that handles different file formats with
 * appropriate strategies for schema evolution and unioning.
 */
public class FormatAwareSchemaResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(FormatAwareSchemaResolver.class);

  private final RelDataTypeFactory typeFactory;
  private final ObjectMapper jsonMapper;

  public FormatAwareSchemaResolver(RelDataTypeFactory typeFactory) {
    this.typeFactory = typeFactory;
    this.jsonMapper = new ObjectMapper();
  }

  /**
   * Resolves schema from a list of files using format-aware strategies.
   */
  public RelDataType resolveSchema(List<File> files, SchemaStrategy strategy) {
    if (files.isEmpty()) {
      return typeFactory.builder().build();
    }

    // Group files by format
    Map<String, List<File>> filesByFormat = files.stream()
        .collect(Collectors.groupingBy(this::detectFormat));

    LOGGER.debug("Detected formats: {}", filesByFormat.keySet());

    if (filesByFormat.size() == 1) {
      // Single format - use format-specific resolution
      String format = filesByFormat.keySet().iterator().next();
      return resolveSchemaForFormat(files, format, strategy);
    } else {
      // Mixed formats - use priority-based resolution
      return resolveMixedFormatSchema(filesByFormat, strategy);
    }
  }

  /**
   * Detects file format based on extension and content.
   */
  private String detectFormat(File file) {
    String name = file.getName().toLowerCase();
    if (name.endsWith(".parquet")) {
      return "parquet";
    } else if (name.endsWith(".csv")) {
      return "csv";
    } else if (name.endsWith(".json")) {
      return "json";
    } else {
      // Try to detect by content
      return detectFormatByContent(file);
    }
  }

  private String detectFormatByContent(File file) {
    try {
      // Simple content-based detection
      byte[] header = new byte[4];
      try (java.io.FileInputStream fis = new java.io.FileInputStream(file)) {
        int bytesRead = fis.read(header);
        if (bytesRead >= 4) {
          // Check for Parquet magic number
          if (header[0] == 'P' && header[1] == 'A' && header[2] == 'R' && header[3] == '1') {
            return "parquet";
          }
          // Check for JSON (starts with { or [)
          if (header[0] == '{' || header[0] == '[') {
            return "json";
          }
        }
      }
      // Default to CSV
      return "csv";
    } catch (IOException e) {
      LOGGER.warn("Failed to detect format for file: {}, defaulting to csv", file.getName());
      return "csv";
    }
  }

  /**
   * Resolves schema for a single file format.
   */
  private RelDataType resolveSchemaForFormat(List<File> files, String format, SchemaStrategy strategy) {
    switch (format) {
      case "parquet":
        return resolveParquetSchema(files, strategy);
      case "csv":
        return resolveCsvSchema(files, strategy);
      case "json":
        return resolveJsonSchema(files, strategy);
      default:
        LOGGER.warn("Unknown format: {}, using generic resolution", format);
        return resolveGenericSchema(files);
    }
  }

  /**
   * Resolves Parquet schema using strategy-specific approach.
   */
  private RelDataType resolveParquetSchema(List<File> files, SchemaStrategy strategy) {
    SchemaStrategy.ParquetStrategy parquetStrategy = strategy.getParquetStrategy();
    LOGGER.info("Resolving Parquet schema for {} files using strategy: {}", files.size(), parquetStrategy);

    switch (parquetStrategy) {
      case LATEST_SCHEMA_WINS:
        return resolveParquetLatestSchemaWins(files);
      case UNION_ALL_COLUMNS:
        return resolveParquetUnionAllColumns(files);
      case UNION_WITH_PROMOTION:
        return resolveParquetUnionWithPromotion(files);
      case LATEST_FILE:
        return resolveParquetLatestFile(files);
      case FIRST_FILE:
        return resolveParquetFirstFile(files);
      case INTERSECTION_ONLY:
        return resolveParquetIntersectionOnly(files);
      default:
        LOGGER.warn("Unknown Parquet strategy: {}, using LATEST_SCHEMA_WINS", parquetStrategy);
        return resolveParquetLatestSchemaWins(files);
    }
  }

  private RelDataType resolveParquetLatestSchemaWins(List<File> files) {
    LOGGER.debug("Using LATEST_SCHEMA_WINS strategy for Parquet files");

    // Simply use the latest file's schema (latest schema wins)
    return resolveParquetLatestFile(files);
  }

  private RelDataType resolveParquetUnionAllColumns(List<File> files) {
    LOGGER.debug("Using UNION_ALL_COLUMNS strategy for Parquet files");

    // Sort files by modification date (newest first)
    List<File> sortedFiles = files.stream()
        .sorted((f1, f2) -> Long.compare(f2.lastModified(), f1.lastModified()))
        .collect(Collectors.toList());

    // Collect ALL columns from ALL files with their latest type definitions
    Map<String, ColumnInfo> columnRegistry = new LinkedHashMap<>();

    for (File file : sortedFiles) {
      try {
        MessageType parquetSchema = getParquetSchema(file);
        Date fileDate = new Date(file.lastModified());

        // Process each column in the Parquet schema
        for (org.apache.parquet.schema.Type field : parquetSchema.getFields()) {
          String columnName = field.getName();
          ColumnInfo existing = columnRegistry.get(columnName);

          if (existing == null || fileDate.after(existing.lastSeen)) {
            // Use most recent definition of this column (latest type wins)
            RelDataType calciteType = convertParquetTypeToCalcite(field);
            columnRegistry.put(columnName, new ColumnInfo(calciteType, fileDate, file.getName()));

            LOGGER.debug("Column '{}' type {} from latest file {} ({})",
                        columnName, calciteType, file.getName(), fileDate);
          }
        }
      } catch (Exception e) {
        LOGGER.error("Failed to read Parquet schema from file: {}", file.getName(), e);
      }
    }

    // Build union schema with ALL columns
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (Map.Entry<String, ColumnInfo> entry : columnRegistry.entrySet()) {
      builder.add(entry.getKey(), entry.getValue().dataType);
    }

    RelDataType result = builder.build();
    LOGGER.info("Created Parquet UNION_ALL_COLUMNS schema with {} columns from {} files",
               result.getFieldCount(), files.size());
    return result;
  }

  private RelDataType resolveParquetLatestFile(List<File> files) {
    LOGGER.debug("Using LATEST_FILE strategy for Parquet files");

    File latestFile = files.stream()
        .max(Comparator.comparing(File::lastModified))
        .orElse(files.get(0));

    LOGGER.info("Using Parquet schema from latest file: {} ({})",
               latestFile.getName(), new Date(latestFile.lastModified()));

    try {
      MessageType parquetSchema = getParquetSchema(latestFile);
      RelDataTypeFactory.Builder builder = typeFactory.builder();

      for (org.apache.parquet.schema.Type field : parquetSchema.getFields()) {
        RelDataType calciteType = convertParquetTypeToCalcite(field);
        builder.add(field.getName(), calciteType);
      }

      return builder.build();
    } catch (Exception e) {
      LOGGER.error("Failed to read schema from latest Parquet file: {}", latestFile.getName(), e);
      return typeFactory.builder().build();
    }
  }

  private RelDataType resolveParquetFirstFile(List<File> files) {
    LOGGER.debug("Using FIRST_FILE strategy for Parquet files");
    File firstFile = files.get(0);
    LOGGER.info("Using Parquet schema from first file: {}", firstFile.getName());

    try {
      MessageType parquetSchema = getParquetSchema(firstFile);
      RelDataTypeFactory.Builder builder = typeFactory.builder();

      for (org.apache.parquet.schema.Type field : parquetSchema.getFields()) {
        RelDataType calciteType = convertParquetTypeToCalcite(field);
        builder.add(field.getName(), calciteType);
      }

      return builder.build();
    } catch (Exception e) {
      LOGGER.error("Failed to read schema from first Parquet file: {}", firstFile.getName(), e);
      return typeFactory.builder().build();
    }
  }

  private RelDataType resolveParquetUnionWithPromotion(List<File> files) {
    LOGGER.debug("Using UNION_WITH_PROMOTION strategy for Parquet files");
    // TODO: Implement SQL type promotion rules
    // For now, fall back to UNION_ALL_COLUMNS
    LOGGER.warn("UNION_WITH_PROMOTION not fully implemented, using UNION_ALL_COLUMNS");
    return resolveParquetUnionAllColumns(files);
  }

  private RelDataType resolveParquetIntersectionOnly(List<File> files) {
    LOGGER.debug("Using INTERSECTION_ONLY strategy for Parquet files");
    // TODO: Implement intersection logic
    // For now, fall back to LATEST_FILE
    LOGGER.warn("INTERSECTION_ONLY not fully implemented, using LATEST_FILE");
    return resolveParquetLatestFile(files);
  }

  /**
   * Resolves CSV schema using conservative consensus approach.
   */
  private RelDataType resolveCsvSchema(List<File> files, SchemaStrategy strategy) {
    LOGGER.info("Resolving CSV schema for {} files using richest-file strategy", files.size());

    // Find the file with the most columns (richest schema)
    File richestFile = files.stream()
        .max(Comparator.comparing(this::getCsvColumnCount))
        .orElse(files.get(0));

    LOGGER.info("Using CSV schema from richest file: {} ({} columns)",
               richestFile.getName(), getCsvColumnCount(richestFile));

    // Infer schema from the richest file
    RelDataType baseSchema = inferCsvSchema(richestFile);

    // Validate other files are compatible
    for (File file : files) {
      if (!file.equals(richestFile)) {
        validateCsvCompatibility(file, baseSchema);
      }
    }

    return baseSchema;
  }

  /**
   * Resolves JSON schema using latest-wins approach.
   */
  private RelDataType resolveJsonSchema(List<File> files, SchemaStrategy strategy) {
    LOGGER.info("Resolving JSON schema for {} files using latest-file strategy", files.size());

    // Sort by modification date and use the latest
    File latestFile = files.stream()
        .max(Comparator.comparing(File::lastModified))
        .orElse(files.get(0));

    LOGGER.info("Using JSON schema from latest file: {} ({})",
               latestFile.getName(), new Date(latestFile.lastModified()));

    RelDataType schema = inferJsonSchema(latestFile);

    // Validate other files for compatibility warnings
    for (File file : files) {
      if (!file.equals(latestFile)) {
        validateJsonCompatibility(file, schema);
      }
    }

    return schema;
  }

  /**
   * Resolves mixed format schema using format priority.
   */
  private RelDataType resolveMixedFormatSchema(Map<String, List<File>> filesByFormat, SchemaStrategy strategy) {
    LOGGER.warn("Mixed formats detected: {}. Using priority-based resolution.", filesByFormat.keySet());

    // Format priority: Parquet > CSV > JSON
    String[] formatPriority = {"parquet", "csv", "json"};

    for (String format : formatPriority) {
      if (filesByFormat.containsKey(format)) {
        LOGGER.info("Using {} files as schema authority for mixed format glob", format);
        return resolveSchemaForFormat(filesByFormat.get(format), format, strategy);
      }
    }

    // Fallback to first available format
    String fallbackFormat = filesByFormat.keySet().iterator().next();
    LOGGER.info("Using fallback format: {}", fallbackFormat);
    return resolveSchemaForFormat(filesByFormat.get(fallbackFormat), fallbackFormat, strategy);
  }

  /**
   * Generic schema resolution fallback.
   */
  private RelDataType resolveGenericSchema(List<File> files) {
    LOGGER.warn("Using generic schema resolution for {} files", files.size());
    // Very basic fallback - try to infer as CSV
    return inferCsvSchema(files.get(0));
  }

  // Helper methods for format-specific operations

  private MessageType getParquetSchema(File file) throws IOException {
    HadoopInputFile inputFile =
        HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(file.toURI()), new org.apache.hadoop.conf.Configuration());
    try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
      return reader.getFooter().getFileMetaData().getSchema();
    }
  }

  private RelDataType convertParquetTypeToCalcite(org.apache.parquet.schema.Type parquetType) {
    // Basic Parquet to Calcite type conversion
    // This is a simplified implementation - in production, use proper conversion utilities
    switch (parquetType.asPrimitiveType().getPrimitiveTypeName()) {
      case BOOLEAN:
        return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
      case INT32:
        return typeFactory.createSqlType(SqlTypeName.INTEGER);
      case INT64:
        return typeFactory.createSqlType(SqlTypeName.BIGINT);
      case FLOAT:
        return typeFactory.createSqlType(SqlTypeName.REAL);
      case DOUBLE:
        return typeFactory.createSqlType(SqlTypeName.DOUBLE);
      case BINARY:
        return typeFactory.createSqlType(SqlTypeName.VARBINARY);
      default:
        // Default to VARCHAR for unknown types
        return typeFactory.createSqlType(SqlTypeName.VARCHAR);
    }
  }

  private int getCsvColumnCount(File file) {
    try {
      // Simple CSV column count by reading first line
      try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(file))) {
        String firstLine = reader.readLine();
        if (firstLine != null) {
          // Simple comma counting (not perfect, but works for basic CSV)
          return firstLine.split(",").length;
        }
      }
      return 0;
    } catch (Exception e) {
      LOGGER.warn("Failed to count CSV columns in file: {}", file.getName());
      return 0;
    }
  }

  private RelDataType inferCsvSchema(File file) {
    try {
      // Simple CSV schema inference by reading first line
      try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(file))) {
        String firstLine = reader.readLine();
        if (firstLine != null) {
          String[] columns = firstLine.split(",");
          RelDataTypeFactory.Builder builder = typeFactory.builder();

          for (int i = 0; i < columns.length; i++) {
            String columnName = columns[i].trim().replaceAll("\"", "");
            if (columnName.isEmpty()) {
              columnName = "col_" + i;
            }
            // Default all columns to VARCHAR for simplicity
            builder.add(columnName, typeFactory.createSqlType(SqlTypeName.VARCHAR));
          }

          return builder.build();
        }
      }
      return typeFactory.builder().build();
    } catch (Exception e) {
      LOGGER.error("Failed to infer CSV schema from file: {}", file.getName(), e);
      return typeFactory.builder().build();
    }
  }

  private RelDataType inferJsonSchema(File file) {
    try {
      JsonNode rootNode = jsonMapper.readTree(file);
      return inferJsonSchemaFromNode(rootNode, typeFactory);
    } catch (Exception e) {
      LOGGER.error("Failed to infer JSON schema from file: {}", file.getName(), e);
      return typeFactory.builder().build();
    }
  }

  private RelDataType inferJsonSchemaFromNode(JsonNode node, RelDataTypeFactory typeFactory) {
    RelDataTypeFactory.Builder builder = typeFactory.builder();

    if (node.isArray() && node.size() > 0) {
      // Use first array element as schema template
      node = node.get(0);
    }

    if (node.isObject()) {
      Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        String fieldName = field.getKey();
        JsonNode fieldValue = field.getValue();

        RelDataType fieldType = inferJsonFieldType(fieldValue);
        builder.add(fieldName, fieldType);
      }
    }

    return builder.build();
  }

  private RelDataType inferJsonFieldType(JsonNode node) {
    if (node.isNull()) {
      return typeFactory.createSqlType(SqlTypeName.VARCHAR); // Default to VARCHAR for nulls
    } else if (node.isBoolean()) {
      return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    } else if (node.isInt()) {
      return typeFactory.createSqlType(SqlTypeName.INTEGER);
    } else if (node.isLong()) {
      return typeFactory.createSqlType(SqlTypeName.BIGINT);
    } else if (node.isDouble() || node.isFloat()) {
      return typeFactory.createSqlType(SqlTypeName.DOUBLE);
    } else if (node.isTextual()) {
      return typeFactory.createSqlType(SqlTypeName.VARCHAR);
    } else if (node.isArray()) {
      return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.VARCHAR), -1);
    } else if (node.isObject()) {
      return typeFactory.createSqlType(SqlTypeName.VARCHAR); // Serialize objects as JSON strings
    } else {
      return typeFactory.createSqlType(SqlTypeName.VARCHAR); // Default fallback
    }
  }

  private void validateCsvCompatibility(File file, RelDataType expectedSchema) {
    try {
      RelDataType fileSchema = inferCsvSchema(file);
      if (fileSchema.getFieldCount() != expectedSchema.getFieldCount()) {
        LOGGER.warn("CSV file {} has {} columns, expected {} columns",
                   file.getName(), fileSchema.getFieldCount(), expectedSchema.getFieldCount());
      }

      // Check column names and types
      for (int i = 0; i < Math.min(fileSchema.getFieldCount(), expectedSchema.getFieldCount()); i++) {
        RelDataTypeField fileField = fileSchema.getFieldList().get(i);
        RelDataTypeField expectedField = expectedSchema.getFieldList().get(i);

        if (!fileField.getName().equals(expectedField.getName())) {
          LOGGER.warn("CSV file {} column {} name mismatch: '{}' vs expected '{}'",
                     file.getName(), i, fileField.getName(), expectedField.getName());
        }

        if (!fileField.getType().getSqlTypeName().equals(expectedField.getType().getSqlTypeName())) {
          LOGGER.warn("CSV file {} column '{}' type mismatch: {} vs expected {}",
                     file.getName(), fileField.getName(),
                     fileField.getType().getSqlTypeName(), expectedField.getType().getSqlTypeName());
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to validate CSV compatibility for file: {}", file.getName(), e);
    }
  }

  private void validateJsonCompatibility(File file, RelDataType expectedSchema) {
    try {
      RelDataType fileSchema = inferJsonSchema(file);
      Set<String> expectedColumns = new HashSet<>(expectedSchema.getFieldNames());
      Set<String> fileColumns = new HashSet<>(fileSchema.getFieldNames());

      Set<String> missingColumns = new HashSet<>(expectedColumns);
      missingColumns.removeAll(fileColumns);

      Set<String> extraColumns = new HashSet<>(fileColumns);
      extraColumns.removeAll(expectedColumns);

      if (!missingColumns.isEmpty()) {
        LOGGER.warn("JSON file {} missing expected columns: {}", file.getName(), missingColumns);
      }

      if (!extraColumns.isEmpty()) {
        LOGGER.debug("JSON file {} has extra columns: {}", file.getName(), extraColumns);
      }
    } catch (Exception e) {
      LOGGER.error("Failed to validate JSON compatibility for file: {}", file.getName(), e);
    }
  }

  /**
   * Internal class to track column information.
   */
  private static class ColumnInfo {
    final RelDataType dataType;
    final Date lastSeen;
    final String originFile;

    ColumnInfo(RelDataType dataType, Date lastSeen, String originFile) {
      this.dataType = dataType;
      this.lastSeen = lastSeen;
      this.originFile = originFile;
    }
  }
}
