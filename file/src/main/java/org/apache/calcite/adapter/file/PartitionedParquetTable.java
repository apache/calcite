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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Table implementation for partitioned Parquet datasets.
 * Represents multiple Parquet files as a single logical table.
 */
public class PartitionedParquetTable extends AbstractTable implements ScannableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionedParquetTable.class);

  private final List<String> filePaths;
  private final PartitionDetector.PartitionInfo partitionInfo;
  private final ExecutionEngineConfig engineConfig;
  private RelProtoDataType protoRowType;
  private List<String> partitionColumns;
  private Map<String, String> partitionColumnTypes;
  private String customRegex;
  private List<PartitionedTableConfig.ColumnMapping> columnMappings;

  public PartitionedParquetTable(List<String> filePaths,
                                 PartitionDetector.PartitionInfo partitionInfo,
                                 ExecutionEngineConfig engineConfig) {
    this(filePaths, partitionInfo, engineConfig, null);
  }

  public PartitionedParquetTable(List<String> filePaths,
                                 PartitionDetector.PartitionInfo partitionInfo,
                                 ExecutionEngineConfig engineConfig,
                                 Map<String, String> partitionColumnTypes) {
    this(filePaths, partitionInfo, engineConfig, partitionColumnTypes, null, null);
  }

  public PartitionedParquetTable(List<String> filePaths,
                                 PartitionDetector.PartitionInfo partitionInfo,
                                 ExecutionEngineConfig engineConfig,
                                 Map<String, String> partitionColumnTypes,
                                 String customRegex,
                                 List<PartitionedTableConfig.ColumnMapping> columnMappings) {
    this.filePaths = filePaths;
    this.partitionInfo = partitionInfo;
    this.engineConfig = engineConfig;
    this.partitionColumnTypes = partitionColumnTypes;
    this.customRegex = customRegex;
    this.columnMappings = columnMappings;

    // Initialize partition columns
    if (partitionInfo != null && partitionInfo.getPartitionColumns() != null) {
      this.partitionColumns = partitionInfo.getPartitionColumns();
    } else {
      this.partitionColumns = new ArrayList<>();
    }
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (protoRowType != null) {
      return protoRowType.apply(typeFactory);
    }

    // Get schema from first Parquet file
    if (filePaths.isEmpty()) {
      return typeFactory.builder().build();
    }

    try {
      String firstFile = filePaths.get(0);
      RelDataType fileSchema = getParquetSchema(firstFile, typeFactory);

      // Add partition columns to schema
      RelDataTypeFactory.Builder builder = typeFactory.builder();

      // Add all fields from Parquet file
      fileSchema.getFieldList().forEach(field ->
          builder.add(field.getName(), field.getType()));

      // Add partition columns
      for (String partCol : partitionColumns) {
        if (!containsField(fileSchema, partCol)) {
          // Use specified type or default to VARCHAR
          SqlTypeName sqlType = SqlTypeName.VARCHAR;
          if (partitionColumnTypes != null && partitionColumnTypes.containsKey(partCol)) {
            String typeStr = partitionColumnTypes.get(partCol);
            try {
              sqlType = SqlTypeName.valueOf(typeStr.toUpperCase(java.util.Locale.ROOT));
            } catch (IllegalArgumentException e) {
              LOGGER.warn("Unknown type '{}' for partition column '{}', defaulting to VARCHAR",
                          typeStr, partCol);
            }
          }
          builder.add(partCol, typeFactory.createSqlType(sqlType));
        }
      }

      return builder.build();

    } catch (Exception e) {
      LOGGER.error("Failed to get schema from Parquet files", e);
      return typeFactory.builder().build();
    }
  }

  private boolean containsField(RelDataType rowType, String fieldName) {
    return rowType.getFieldList().stream()
        .anyMatch(field -> field.getName().equalsIgnoreCase(fieldName));
  }

  private RelDataType getParquetSchema(String filePath, RelDataTypeFactory typeFactory)
      throws IOException {
    Configuration conf = new Configuration();
    Path hadoopPath = new Path(filePath);
    InputFile inputFile = HadoopInputFile.fromPath(hadoopPath, conf);

    // Use the non-deprecated method
    ParquetMetadata metadata;
    try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
      metadata = reader.getFooter();
    }

    MessageType messageType = metadata.getFileMetaData().getSchema();

    // Map Parquet types to SQL types properly
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    messageType.getFields().forEach(field -> {
      SqlTypeName sqlType = mapParquetTypeToSqlType(field);
      builder.add(field.getName(), typeFactory.createSqlType(sqlType));
    });

    return builder.build();
  }

  private SqlTypeName mapParquetTypeToSqlType(org.apache.parquet.schema.Type parquetType) {
    if (!parquetType.isPrimitive()) {
      // Complex types default to VARCHAR
      return SqlTypeName.VARCHAR;
    }

    org.apache.parquet.schema.PrimitiveType primitiveType = parquetType.asPrimitiveType();
    org.apache.parquet.schema.LogicalTypeAnnotation logicalType =
        primitiveType.getLogicalTypeAnnotation();

    switch (primitiveType.getPrimitiveTypeName()) {
    case INT32:
      // Check for logical types
      if (logicalType
          instanceof org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
        org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation intType =
            (org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation)
                logicalType;
        if (intType.getBitWidth() <= 8) {
          return SqlTypeName.TINYINT;
        } else if (intType.getBitWidth() <= 16) {
          return SqlTypeName.SMALLINT;
        }
      }
      return SqlTypeName.INTEGER;
    case INT64:
      return SqlTypeName.BIGINT;
    case FLOAT:
      return SqlTypeName.REAL;
    case DOUBLE:
      return SqlTypeName.DOUBLE;
    case BOOLEAN:
      return SqlTypeName.BOOLEAN;
    case BINARY:
    case FIXED_LEN_BYTE_ARRAY:
      // Check if it's a string
      if (logicalType
          instanceof org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
        return SqlTypeName.VARCHAR;
      }
      // Check using ConvertedType instead of deprecated OriginalType
      org.apache.parquet.schema.Type.Repetition repetition = primitiveType.getRepetition();
      if (primitiveType.getId() != null
          && primitiveType.getName() != null
          && primitiveType.getPrimitiveTypeName()
          == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY) {
        // Most BINARY types in Parquet files are strings unless explicitly marked otherwise
        return SqlTypeName.VARCHAR;
      }
      return SqlTypeName.VARBINARY;
    case INT96:
      // INT96 is typically used for timestamps
      return SqlTypeName.TIMESTAMP;
    default:
      return SqlTypeName.VARCHAR;
    }
  }

  @Override public Enumerable<Object[]> scan(DataContext root) {
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    final JavaTypeFactory typeFactory = root.getTypeFactory();

    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        return new PartitionedParquetEnumerator(
            filePaths, partitionInfo, partitionColumns, partitionColumnTypes,
            cancelFlag, engineConfig.getMemoryThreshold(), customRegex, columnMappings);
      }
    };
  }

  /**
   * Enumerator that reads from multiple Parquet files with partition support.
   */
  private static class PartitionedParquetEnumerator implements Enumerator<Object[]> {
    private final List<String> filePaths;
    private final PartitionDetector.PartitionInfo globalPartitionInfo;
    private final List<String> partitionColumns;
    private final Map<String, String> partitionColumnTypes;
    private final AtomicBoolean cancelFlag;
    private final long memoryThreshold;
    private final String customRegex;
    private final List<PartitionedTableConfig.ColumnMapping> columnMappings;

    private Iterator<String> fileIterator;
    private String currentFile;
    private ParquetReader<GenericRecord> currentReader;
    private GenericRecord currentRecord;
    private Object[] currentRow;
    private Map<String, String> currentPartitionValues;
    private Schema recordSchema;
    private int fileFieldCount;

    PartitionedParquetEnumerator(List<String> filePaths,
                                 PartitionDetector.PartitionInfo partitionInfo,
                                 List<String> partitionColumns,
                                 Map<String, String> partitionColumnTypes,
                                 AtomicBoolean cancelFlag,
                                 long memoryThreshold,
                                 String customRegex,
                                 List<PartitionedTableConfig.ColumnMapping> columnMappings) {
      this.filePaths = filePaths;
      this.globalPartitionInfo = partitionInfo;
      this.partitionColumns = partitionColumns;
      this.partitionColumnTypes = partitionColumnTypes;
      this.cancelFlag = cancelFlag;
      this.memoryThreshold = memoryThreshold;
      this.customRegex = customRegex;
      this.columnMappings = columnMappings;
      this.fileIterator = filePaths.iterator();
    }

    @Override public Object[] current() {
      return currentRow;
    }

    @Override public boolean moveNext() {
      if (cancelFlag != null && cancelFlag.get()) {
        return false;
      }

      try {
        // Try to read next record from current file
        if (currentReader != null) {
          currentRecord = currentReader.read();
          if (currentRecord != null) {
            currentRow = convertToRow(currentRecord);
            return true;
          }
        }

        // Current file exhausted, move to next file
        while (fileIterator.hasNext()) {
          closeCurrentReader();
          currentFile = fileIterator.next();

          if (!openNextFile()) {
            continue;
          }

          currentRecord = currentReader.read();
          if (currentRecord != null) {
            currentRow = convertToRow(currentRecord);
            return true;
          }
        }

        // No more files
        return false;

      } catch (Exception e) {
        throw new RuntimeException("Error reading partitioned Parquet data", e);
      }
    }

    private boolean openNextFile() throws IOException {
      Path hadoopPath = new Path(currentFile);
      Configuration conf = new Configuration();

      // Extract partition values for this file
      currentPartitionValues = extractPartitionValues(currentFile);

      try {
        @SuppressWarnings("deprecation")
        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(hadoopPath)
            .withConf(conf)
            .build();
        currentReader = reader;

        // Read first record to get schema
        GenericRecord firstRecord = currentReader.read();
        if (firstRecord != null) {
          recordSchema = firstRecord.getSchema();
          fileFieldCount = recordSchema.getFields().size();
          // Put the record back by creating a new reader
          closeCurrentReader();
          @SuppressWarnings("deprecation")
          ParquetReader<GenericRecord> newReader =
              AvroParquetReader.<GenericRecord>builder(hadoopPath)
              .withConf(conf)
              .build();
          currentReader = newReader;
          return true;
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to open Parquet file: {}", currentFile, e);
      }

      return false;
    }

    private Map<String, String> extractPartitionValues(String filePath) {
      if (globalPartitionInfo == null) {
        return new LinkedHashMap<>();
      }

      PartitionDetector.PartitionInfo filePartitionInfo = null;

      if (globalPartitionInfo.isHiveStyle()) {
        filePartitionInfo =
            PartitionDetector.extractHivePartitions(filePath);
      } else if (customRegex != null && columnMappings != null) {
        // Custom regex partitions
        filePartitionInfo =
            PartitionDetector.extractCustomPartitions(filePath, customRegex,
                columnMappings);
      } else if (partitionColumns != null && !partitionColumns.isEmpty()) {
        filePartitionInfo =
            PartitionDetector.extractDirectoryPartitions(filePath,
                partitionColumns);
      }

      return filePartitionInfo != null
          ? filePartitionInfo.getPartitionValues()
          : new LinkedHashMap<>();
    }

    private Object[] convertToRow(GenericRecord record) {
      // Create array with space for file fields + partition fields
      Object[] row = new Object[fileFieldCount + partitionColumns.size()];

      // Copy file data
      for (int i = 0; i < fileFieldCount; i++) {
        Object value = record.get(i);
        // Convert Avro UTF8 to String
        if (value != null && value.getClass().getName().equals("org.apache.avro.util.Utf8")) {
          value = value.toString();
        }
        row[i] = value;
      }

      // Add partition values with type conversion
      for (int i = 0; i < partitionColumns.size(); i++) {
        String partCol = partitionColumns.get(i);
        String strValue = currentPartitionValues.get(partCol);

        // Convert based on specified type
        Object value = strValue;
        if (partitionColumnTypes != null && partitionColumnTypes.containsKey(partCol)) {
          String typeStr = partitionColumnTypes.get(partCol);
          try {
            SqlTypeName sqlType = SqlTypeName.valueOf(typeStr.toUpperCase(java.util.Locale.ROOT));
            value = convertPartitionValue(strValue, sqlType);
          } catch (Exception e) {
            // Keep as string if conversion fails
            LOGGER.debug("Failed to convert partition value '{}' to type {}", strValue, typeStr);
          }
        }

        row[fileFieldCount + i] = value;
      }

      return row;
    }

    private Object convertPartitionValue(String strValue, SqlTypeName sqlType) {
      if (strValue == null) {
        return null;
      }

      switch (sqlType) {
      case INTEGER:
        return Integer.parseInt(strValue);
      case BIGINT:
        return Long.parseLong(strValue);
      case SMALLINT:
        return Short.parseShort(strValue);
      case TINYINT:
        return Byte.parseByte(strValue);
      case DOUBLE:
        return Double.parseDouble(strValue);
      case FLOAT:
      case REAL:
        return Float.parseFloat(strValue);
      case DECIMAL:
        return new java.math.BigDecimal(strValue);
      case BOOLEAN:
        return Boolean.parseBoolean(strValue);
      case DATE:
        // Simple date parsing for yyyy-MM-dd format
        if (strValue.matches("\\d{4}-\\d{2}-\\d{2}")) {
          return java.sql.Date.valueOf(strValue);
        }
        return strValue;
      case VARCHAR:
      case CHAR:
      default:
        return strValue;
      }
    }

    private void closeCurrentReader() {
      if (currentReader != null) {
        try {
          currentReader.close();
        } catch (IOException e) {
          // Ignore
        }
        currentReader = null;
      }
    }

    @Override public void reset() {
      closeCurrentReader();
      fileIterator = filePaths.iterator();
      currentFile = null;
      currentRecord = null;
      currentRow = null;
    }

    @Override public void close() {
      closeCurrentReader();
    }
  }
}
