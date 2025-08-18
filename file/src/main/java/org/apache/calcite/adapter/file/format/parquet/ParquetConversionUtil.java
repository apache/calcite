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
package org.apache.calcite.adapter.file.format.parquet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.calcite.adapter.file.format.csv.CsvTypeInferrer;
import org.apache.calcite.adapter.file.table.CsvTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Source;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import java.io.File;
import java.math.BigDecimal;

/**
 * Utility class for converting various file formats to Parquet.
 */
public class ParquetConversionUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetConversionUtil.class);

  private ParquetConversionUtil() {
    // Utility class should not be instantiated
  }


  /**
   * Get the cache directory for Parquet conversions.
   */
  public static File getParquetCacheDir(File baseDirectory) {
    return getParquetCacheDir(baseDirectory, null);
  }

  /**
   * Get the cache directory for Parquet conversions with optional custom directory.
   */
  public static File getParquetCacheDir(File baseDirectory, String customCacheDir) {
    return getParquetCacheDir(baseDirectory, customCacheDir, null);
  }

  /**
   * Get the cache directory for Parquet conversions with optional custom directory and schema name.
   * @param baseDirectory The base directory for data files
   * @param customCacheDir Optional custom cache directory path
   * @param schemaName Optional schema name for schema-specific caching
   */
  public static File getParquetCacheDir(File baseDirectory, String customCacheDir, String schemaName) {
    File cacheDir;
    if (customCacheDir != null && !customCacheDir.isEmpty()) {
      // If custom cache dir is specified, append schema name for separation
      if (schemaName != null && !schemaName.isEmpty()) {
        cacheDir = new File(customCacheDir, "schema_" + schemaName);
      } else {
        cacheDir = new File(customCacheDir);
      }
    } else {
      // Use default cache directory with optional schema suffix
      String cacheDirName = schemaName != null && !schemaName.isEmpty() 
          ? ".parquet_cache_" + schemaName 
          : ".parquet_cache";
      cacheDir = new File(baseDirectory, cacheDirName);
    }
    if (!cacheDir.exists()) {
      cacheDir.mkdirs();
    }
    return cacheDir;
  }

  /**
   * Get the cached Parquet file for a source file.
   */
  public static File getCachedParquetFile(File sourceFile, File cacheDir) {
    return getCachedParquetFile(sourceFile, cacheDir, false);
  }

  /**
   * Get the cached Parquet file for a source file with optional type inference suffix.
   */
  public static File getCachedParquetFile(File sourceFile, File cacheDir, boolean typeInferenceEnabled) {
    String baseName = sourceFile.getName();
    // Remove existing extensions
    int lastDot = baseName.lastIndexOf('.');
    if (lastDot > 0) {
      baseName = baseName.substring(0, lastDot);
    }
    // Add suffix to distinguish files created with different type inference settings
    String suffix = typeInferenceEnabled ? ".inferred" : "";
    return new File(cacheDir, baseName + suffix + ".parquet");
  }

  /**
   * Check if a Parquet conversion is needed based on file timestamps.
   */
  public static boolean needsConversion(File sourceFile, File parquetFile) {
    if (!parquetFile.exists()) {
      return true;
    }
    // Check if source file is newer than the cached Parquet file
    // Add a small buffer (1 second) to handle filesystem timestamp precision issues
    return sourceFile.lastModified() > (parquetFile.lastModified() + 1000);
  }

  /**
   * Convert a source file to Parquet by querying it through Calcite.
   */
  public static File convertToParquet(Source source, String tableName, Table table,
      File cacheDir, SchemaPlus parentSchema, String schemaName) throws Exception {

    File sourceFile = new File(source.path());
    
    // Check if type inference is enabled for CSV tables
    boolean typeInferenceEnabled = false;
    if (table instanceof CsvTable) {
      CsvTypeInferrer.TypeInferenceConfig config = ((CsvTable) table).getTypeInferenceConfig();
      typeInferenceEnabled = config != null && config.isEnabled();
    }

    // Use concurrent cache for thread-safe conversion with type inference suffix and schema name
    return ConcurrentParquetCache.convertWithLocking(sourceFile, cacheDir, typeInferenceEnabled, schemaName, tempFile -> {
      performConversion(source, tableName, table, tempFile, parentSchema, schemaName);
    });
  }


  /**
   * Perform the actual conversion to a temporary file.
   */
  private static void performConversion(Source source, String tableName, Table table,
      File targetFile, SchemaPlus parentSchema, String schemaName) throws Exception {

    // Use direct conversion only to preserve original schema structure
    if (!tryDirectConversion(table, targetFile, schemaName)) {
      throw new RuntimeException("Table does not support direct scanning: " + tableName + 
          ". Only ScannableTable implementations are supported for Parquet conversion.");
    }
  }

  /**
   * Try to convert the table directly by reading from its scan() method
   * to preserve original column names and types.
   */
  private static boolean tryDirectConversion(Table table, File targetFile, String schemaName) throws Exception {
    org.apache.calcite.schema.ScannableTable scannableTable;
    
    if (table instanceof org.apache.calcite.schema.ScannableTable) {
      scannableTable = (org.apache.calcite.schema.ScannableTable) table;
    } else if (table instanceof org.apache.calcite.schema.TranslatableTable) {
      // Create an adapter wrapper for TranslatableTable to make it scannable
      scannableTable = new TranslatableTableAdapter((org.apache.calcite.schema.TranslatableTable) table, schemaName);
    } else {
      return false;
    }
    
    // Create a minimal DataContext for scanning
    org.apache.calcite.DataContext dataContext = new org.apache.calcite.DataContext() {
      @Override public SchemaPlus getRootSchema() { return null; }
      @Override public JavaTypeFactory getTypeFactory() { 
        return new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
      }
      @Override public Object get(String name) { 
        if ("spark".equals(name)) return false;
        return null; 
      }
      @Override public org.apache.calcite.linq4j.QueryProvider getQueryProvider() {
        return null;
      }
    };

    // Get the row type to understand the schema
    JavaTypeFactory typeFactory = (JavaTypeFactory) dataContext.getTypeFactory();
    org.apache.calcite.rel.type.RelDataType rowType = table.getRowType(typeFactory);
    
    // Scan the table directly
    org.apache.calcite.linq4j.Enumerable<Object[]> enumerable = scannableTable.scan(dataContext);
    
    // Convert to Parquet using direct writer
    convertEnumerableToParquetDirect(enumerable, rowType, targetFile, typeFactory);
    
    return true;
  }

  /**
   * Convert an enumerable directly to Parquet using native Parquet writers.
   * This preserves the original schema structure and column names exactly.
   */
  private static void convertEnumerableToParquetDirect(org.apache.calcite.linq4j.Enumerable<Object[]> enumerable,
      org.apache.calcite.rel.type.RelDataType rowType, File targetFile, JavaTypeFactory typeFactory) throws Exception {
    
    List<org.apache.calcite.rel.type.RelDataTypeField> fields = rowType.getFieldList();
    
    // Build Parquet schema from the original RelDataType (preserves original column names and types)
    List<org.apache.parquet.schema.Type> parquetFields = new ArrayList<>();
    for (org.apache.calcite.rel.type.RelDataTypeField field : fields) {
      String fieldName = field.getName(); // Preserve original field name exactly
      org.apache.calcite.sql.type.SqlTypeName sqlType = field.getType().getSqlTypeName();
      
      // Map Calcite types to Parquet types directly
      org.apache.parquet.schema.Type parquetField = createParquetFieldFromCalciteType(fieldName, sqlType, field);
      parquetFields.add(parquetField);
    }

    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("record", parquetFields);

    // Delete existing file if it exists
    if (targetFile.exists()) {
      targetFile.delete();
    }

    // Write to Parquet using direct writer
    org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(targetFile.getAbsolutePath());
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    conf.set("parquet.enable.vectorized.reader", "true");
    
    org.apache.parquet.hadoop.example.GroupWriteSupport.setSchema(schema, conf);

    // Create ParquetWriter
    try (org.apache.parquet.hadoop.ParquetWriter<org.apache.parquet.example.data.Group> writer = 
        createParquetWriter(hadoopPath, schema, conf)) {

      org.apache.parquet.example.data.simple.SimpleGroupFactory groupFactory = 
          new org.apache.parquet.example.data.simple.SimpleGroupFactory(schema);

      // Write all rows
      for (Object[] row : enumerable) {
        org.apache.parquet.example.data.Group group = groupFactory.newGroup();

        for (int i = 0; i < fields.size() && i < row.length; i++) {
          org.apache.calcite.rel.type.RelDataTypeField field = fields.get(i);
          String fieldName = field.getName();
          Object value = row[i];
          
          if (value != null) {
            org.apache.calcite.sql.type.SqlTypeName sqlType = field.getType().getSqlTypeName();
            // Add value directly to group
            addValueToParquetGroup(group, fieldName, value, sqlType);
          }
          // Skip null values - Parquet handles nulls through repetition levels
        }

        writer.write(group);
      }
    }
  }

  /**
   * Create a Parquet field from a Calcite type, preserving original type information.
   */
  private static org.apache.parquet.schema.Type createParquetFieldFromCalciteType(String fieldName, 
      org.apache.calcite.sql.type.SqlTypeName sqlType, org.apache.calcite.rel.type.RelDataTypeField field) {
    
    org.apache.parquet.schema.Type.Repetition repetition = field.getType().isNullable() 
        ? org.apache.parquet.schema.Type.Repetition.OPTIONAL 
        : org.apache.parquet.schema.Type.Repetition.REQUIRED;

    switch (sqlType) {
      case BOOLEAN:
        return org.apache.parquet.schema.Types.primitive(
            org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition).named(fieldName);

      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return org.apache.parquet.schema.Types.primitive(
            org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32, repetition).named(fieldName);

      case BIGINT:
        return org.apache.parquet.schema.Types.primitive(
            org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64, repetition).named(fieldName);

      case FLOAT:
      case REAL:
        return org.apache.parquet.schema.Types.primitive(
            org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT, repetition).named(fieldName);

      case DOUBLE:
        return org.apache.parquet.schema.Types.primitive(
            org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE, repetition).named(fieldName);

      case DECIMAL:
        int precision = field.getType().getPrecision();
        int scale = field.getType().getScale();
        if (precision <= 0) precision = 38;
        return org.apache.parquet.schema.Types.primitive(
            org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY, repetition)
            .as(org.apache.parquet.schema.LogicalTypeAnnotation.decimalType(scale, precision))
            .named(fieldName);

      case DATE:
        return org.apache.parquet.schema.Types.primitive(
            org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32, repetition)
            .as(org.apache.parquet.schema.LogicalTypeAnnotation.dateType())
            .named(fieldName);

      case TIME:
        return org.apache.parquet.schema.Types.primitive(
            org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32, repetition)
            .as(org.apache.parquet.schema.LogicalTypeAnnotation.timeType(true, 
                org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named(fieldName);

      case TIMESTAMP:
        return org.apache.parquet.schema.Types.primitive(
            org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64, repetition)
            .as(org.apache.parquet.schema.LogicalTypeAnnotation.timestampType(true, 
                org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named(fieldName);

      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return org.apache.parquet.schema.Types.primitive(
            org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64, repetition)
            .as(org.apache.parquet.schema.LogicalTypeAnnotation.timestampType(true, 
                org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named(fieldName);

      default:
        // Default to string for VARCHAR, CHAR, and unknown types
        return org.apache.parquet.schema.Types.primitive(
            org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY, repetition)
            .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType())
            .named(fieldName);
    }
  }

  /**
   * Create a custom ParquetWriter for Group objects using builder pattern.
   */
  private static org.apache.parquet.hadoop.ParquetWriter<org.apache.parquet.example.data.Group> createParquetWriter(
      org.apache.hadoop.fs.Path path, org.apache.parquet.schema.MessageType schema, 
      org.apache.hadoop.conf.Configuration conf) throws Exception {
    
    return new SimpleParquetWriter.Builder(path)
        .withSchema(schema)
        .withConf(conf)
        .withCompressionCodec(org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY)
        .withWriterVersion(org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0)
        .withPageSize(org.apache.parquet.column.ParquetProperties.DEFAULT_PAGE_SIZE)
        .withDictionaryEncoding(true)
        .build();
  }

  /**
   * Simple ParquetWriter implementation for Group objects.
   */
  @SuppressWarnings("deprecation")
  private static class SimpleParquetWriter extends org.apache.parquet.hadoop.ParquetWriter<org.apache.parquet.example.data.Group> {
    
    public SimpleParquetWriter(org.apache.hadoop.fs.Path path, 
                               org.apache.parquet.hadoop.example.GroupWriteSupport writeSupport,
                               org.apache.parquet.hadoop.metadata.CompressionCodecName compressionCodecName,
                               int blockSize, int pageSize, boolean enableDictionary,
                               boolean enableValidation, 
                               org.apache.parquet.column.ParquetProperties.WriterVersion writerVersion,
                               org.apache.hadoop.conf.Configuration conf) throws java.io.IOException {
      super(path, writeSupport, compressionCodecName, blockSize, pageSize,
          pageSize, enableDictionary, enableValidation, writerVersion, conf);
    }

    public static Builder builder(org.apache.hadoop.fs.Path path) {
      return new Builder(path);
    }

    public static class Builder extends org.apache.parquet.hadoop.ParquetWriter.Builder<org.apache.parquet.example.data.Group, Builder> {
      private org.apache.parquet.schema.MessageType schema = null;

      private Builder(org.apache.hadoop.fs.Path path) {
        super(path);
      }

      public Builder withSchema(org.apache.parquet.schema.MessageType schema) {
        this.schema = schema;
        return this;
      }

      @Override protected Builder self() {
        return this;
      }

      @Override @SuppressWarnings("deprecation")
      protected org.apache.parquet.hadoop.example.GroupWriteSupport getWriteSupport(org.apache.hadoop.conf.Configuration conf) {
        org.apache.parquet.hadoop.example.GroupWriteSupport.setSchema(schema, conf);
        return new org.apache.parquet.hadoop.example.GroupWriteSupport();
      }
    }
  }

  /**
   * Add a value to a Parquet group with proper type conversion.
   */
  private static void addValueToParquetGroup(org.apache.parquet.example.data.Group group, 
      String fieldName, Object value, org.apache.calcite.sql.type.SqlTypeName sqlType) {
    
    if (value == null) return;

    switch (sqlType) {
      case BOOLEAN:
        if (value instanceof Boolean) {
          group.append(fieldName, (Boolean) value);
        } else {
          group.append(fieldName, Boolean.parseBoolean(value.toString()));
        }
        break;

      case TINYINT:
      case SMALLINT:
      case INTEGER:
        if (value instanceof Number) {
          group.append(fieldName, ((Number) value).intValue());
        } else {
          group.append(fieldName, Integer.parseInt(value.toString()));
        }
        break;

      case BIGINT:
        if (value instanceof Number) {
          group.append(fieldName, ((Number) value).longValue());
        } else {
          group.append(fieldName, Long.parseLong(value.toString()));
        }
        break;

      case FLOAT:
      case REAL:
        if (value instanceof Number) {
          group.append(fieldName, ((Number) value).floatValue());
        } else {
          group.append(fieldName, Float.parseFloat(value.toString()));
        }
        break;

      case DOUBLE:
        if (value instanceof Number) {
          group.append(fieldName, ((Number) value).doubleValue());
        } else {
          group.append(fieldName, Double.parseDouble(value.toString()));
        }
        break;

      case DECIMAL:
        if (value instanceof BigDecimal) {
          BigDecimal decimal = (BigDecimal) value;
          group.append(fieldName, org.apache.parquet.io.api.Binary.fromConstantByteArray(
              decimal.unscaledValue().toByteArray()));
        } else {
          BigDecimal decimal = new BigDecimal(value.toString());
          group.append(fieldName, org.apache.parquet.io.api.Binary.fromConstantByteArray(
              decimal.unscaledValue().toByteArray()));
        }
        break;

      case DATE:
        if (value instanceof java.sql.Date) {
          // Convert to days since epoch (1970-01-01) in UTC
          // Do NOT use toLocalDate() as it may apply timezone offset
          long millis = ((java.sql.Date) value).getTime();
          int daysSinceEpoch = (int) (millis / (24L * 60 * 60 * 1000));
          group.append(fieldName, daysSinceEpoch);
        } else if (value instanceof java.time.LocalDate) {
          group.append(fieldName, (int) ((java.time.LocalDate) value).toEpochDay());
        } else if (value instanceof Integer) {
          // Already in days since epoch format
          group.append(fieldName, (Integer) value);
        } else {
          // Try to parse as date string
          try {
            java.time.LocalDate localDate = java.time.LocalDate.parse(value.toString());
            group.append(fieldName, (int) localDate.toEpochDay());
          } catch (Exception e) {
            // Fall back to string representation
            group.append(fieldName, value.toString());
          }
        }
        break;

      case TIME:
        if (value instanceof java.time.LocalTime) {
          // LocalTime is the preferred representation for TIME values
          java.time.LocalTime localTime = (java.time.LocalTime) value;
          int millisSinceMidnight = (int) (localTime.toNanoOfDay() / 1_000_000L);
          group.append(fieldName, millisSinceMidnight);
        } else if (value instanceof java.sql.Time) {
          // Legacy java.sql.Time support - convert via LocalTime
          java.sql.Time time = (java.sql.Time) value;
          java.time.LocalTime localTime = time.toLocalTime();
          int millisSinceMidnight = (int) (localTime.toNanoOfDay() / 1_000_000L);
          group.append(fieldName, millisSinceMidnight);
        } else if (value instanceof Integer) {
          // Already in milliseconds since midnight format
          group.append(fieldName, (Integer) value);
        } else {
          // Try to parse time string
          String timeStr = value.toString();
          if (timeStr.matches("\\d{2}:\\d{2}:\\d{2}")) {
            java.time.LocalTime localTime = java.time.LocalTime.parse(timeStr);
            int millisSinceMidnight = (int) (localTime.toNanoOfDay() / 1_000_000L);
            group.append(fieldName, millisSinceMidnight);
          } else {
            group.append(fieldName, value.toString());
          }
        }
        break;

      case TIMESTAMP:
        if (value instanceof java.sql.Timestamp) {
          // Store as milliseconds since epoch in UTC (timezone-naive)
          long tsValue = ((java.sql.Timestamp) value).getTime();
          LOGGER.debug("Writing TIMESTAMP to Parquet: {} for field {}", tsValue, fieldName);
          group.append(fieldName, tsValue);
        } else if (value instanceof java.util.Date) {
          long tsValue = ((java.util.Date) value).getTime();
          LOGGER.debug("Writing Date as TIMESTAMP to Parquet: {} for field {}", tsValue, fieldName);
          group.append(fieldName, tsValue);
        } else if (value instanceof Long) {
          // Already in milliseconds since epoch format
          group.append(fieldName, (Long) value);
        } else if (value instanceof org.apache.calcite.adapter.file.temporal.LocalTimestamp) {
          // LocalTimestamp stores milliseconds since epoch in UTC
          org.apache.calcite.adapter.file.temporal.LocalTimestamp localTs = 
              (org.apache.calcite.adapter.file.temporal.LocalTimestamp) value;
          group.append(fieldName, localTs.getTime());
        } else {
          // Parse timestamp string as UTC
          String timestampStr = value.toString();
          if (timestampStr.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.*")) {
            try {
              java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
              // Parse as UTC - TIMESTAMP WITHOUT TIME ZONE should store UTC time
              sdf.setTimeZone(java.util.TimeZone.getTimeZone("UTC"));
              java.util.Date parsedDate = sdf.parse(timestampStr.substring(0, 19));
              group.append(fieldName, parsedDate.getTime());
            } catch (java.text.ParseException e) {
              group.append(fieldName, value.toString());
            }
          } else {
            group.append(fieldName, value.toString());
          }
        }
        break;

      default:
        // Default to string for all other types
        group.append(fieldName, value.toString());
        break;
    }
  }

  /**
   * Adapter that wraps a TranslatableTable to make it behave like a ScannableTable.
   * This allows direct conversion of TranslatableTable implementations by executing
   * their SQL translation and scanning the results.
   */
  private static class TranslatableTableAdapter extends org.apache.calcite.schema.impl.AbstractTable implements org.apache.calcite.schema.ScannableTable {
    private final org.apache.calcite.schema.TranslatableTable translatableTable;
    private final String schemaName;

    public TranslatableTableAdapter(org.apache.calcite.schema.TranslatableTable translatableTable, String schemaName) {
      this.translatableTable = translatableTable;
      this.schemaName = schemaName;
    }

    @Override
    public org.apache.calcite.rel.type.RelDataType getRowType(org.apache.calcite.rel.type.RelDataTypeFactory typeFactory) {
      return translatableTable.getRowType(typeFactory);
    }

    @Override
    public org.apache.calcite.linq4j.Enumerable<Object[]> scan(org.apache.calcite.DataContext root) {
      try {
        // Create a temporary Calcite connection to execute the translation
        java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:calcite:");
        org.apache.calcite.jdbc.CalciteConnection calciteConn = conn.unwrap(org.apache.calcite.jdbc.CalciteConnection.class);
        
        SchemaPlus rootSchema = calciteConn.getRootSchema();
        
        // Create a temporary schema with just this table
        String tempTableName = "temp_table_" + System.currentTimeMillis();
        SchemaPlus tempSchema = rootSchema.add("TEMP_SCAN", new org.apache.calcite.schema.impl.AbstractSchema() {
          @Override
          protected java.util.Map<String, org.apache.calcite.schema.Table> getTableMap() {
            return java.util.Collections.singletonMap(tempTableName, translatableTable);
          }
        });

        // Execute a SELECT * query to get all data
        try (java.sql.Statement stmt = conn.createStatement();
             java.sql.ResultSet rs = stmt.executeQuery("SELECT * FROM TEMP_SCAN.\"" + tempTableName + "\"")) {

          // Convert ResultSet to Enumerable<Object[]>
          java.util.List<Object[]> rows = new java.util.ArrayList<>();
          org.apache.calcite.rel.type.RelDataType rowType = getRowType(root.getTypeFactory());
          int columnCount = rowType.getFieldCount();
          
          while (rs.next()) {
            Object[] row = new Object[columnCount];
            for (int i = 0; i < columnCount; i++) {
              row[i] = rs.getObject(i + 1);
            }
            rows.add(row);
          }
          
          conn.close();
          return org.apache.calcite.linq4j.Linq4j.asEnumerable(rows);
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to scan TranslatableTable: " + e.getMessage(), e);
      }
    }
  }

}
