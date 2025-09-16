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

import org.apache.calcite.adapter.file.format.csv.CsvTypeInferrer;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFile;
import org.apache.calcite.adapter.file.table.CsvTable;
import org.apache.calcite.adapter.file.util.NullEquivalents;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
      // The baseDirectory should already be .aperio/<schema> from FileSchema
      // So we just need to add .parquet_cache subdirectory
      cacheDir = new File(baseDirectory, ".parquet_cache");
    }
    if (!cacheDir.exists()) {
      cacheDir.mkdirs();
    }
    return cacheDir;
  }

  /**
   * Get the cached Parquet file location for a given source file.
   */
  public static File getCachedParquetFile(File sourceFile, File cacheDir, boolean typeInferenceEnabled, String casing) {
    String baseName = sourceFile.getName();
    if (baseName.contains(".")) {
      int lastDot = baseName.lastIndexOf('.');
      if (lastDot > 0) {
        baseName = baseName.substring(0, lastDot);
      }
    }
    baseName = org.apache.calcite.adapter.file.converters.ConverterUtils.sanitizeIdentifier(baseName);
    return new File(cacheDir, baseName + ".parquet");
  }

  /**
   * Check if a path represents an S3 location.
   */
  private static boolean isS3Path(String path) {
    return path != null && path.startsWith("s3://");
  }

  /**
   * Helper method to detect if a path is an S3 URI and convert it to S3A format for Hadoop.
   */
  private static String getHadoopPath(String path) {
    if (path != null && path.startsWith("s3://")) {
      return path.replace("s3://", "s3a://");
    }
    return path;
  }

  /**
   * Configure Hadoop Configuration for S3A access using AWS SDK credentials chain.
   */
  private static void configureS3Access(org.apache.hadoop.conf.Configuration conf) {
    // Use AWS SDK default credential provider chain (same as S3StorageProvider)
    conf.set("fs.s3a.aws.credentials.provider", 
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
    
    // Try to get region from AWS SDK default chain
    try {
      com.amazonaws.regions.DefaultAwsRegionProviderChain regionProvider = 
          new com.amazonaws.regions.DefaultAwsRegionProviderChain();
      String region = regionProvider.getRegion();
      if (region != null) {
        conf.set("fs.s3a.endpoint.region", region);
        LOGGER.debug("Configured S3A region: {}", region);
      }
    } catch (Exception e) {
      LOGGER.debug("Could not auto-detect AWS region, using default: {}", e.getMessage());
      // Fallback to us-west-1 (same as S3StorageProvider)
      conf.set("fs.s3a.endpoint.region", "us-west-1");
    }
    
    // Performance optimizations for Parquet writing
    conf.set("fs.s3a.multipart.size", "64M");
    conf.set("fs.s3a.multipart.threshold", "128M"); 
    conf.set("fs.s3a.fast.upload", "true");
    
    LOGGER.info("Configured Hadoop for S3A access using AWS credentials chain");
  }

  /**
   * Convert source to Parquet using StorageProviderFile abstraction.
   * This method supports both local filesystem and S3 storage.
   */
  public static StorageProviderFile convertToParquet(Source source, String tableName, Table table,
      StorageProviderFile cacheDirFile, SchemaPlus parentSchema, String schemaName, String casing,
      StorageProvider storageProvider) throws Exception {

    StorageProviderFile sourceFile = StorageProviderFile.create(source.path(), storageProvider);

    // Check if type inference is enabled for CSV tables
    boolean typeInferenceEnabled = false;
    if (table instanceof CsvTable) {
      CsvTypeInferrer.TypeInferenceConfig config = ((CsvTable) table).getTypeInferenceConfig();
      typeInferenceEnabled = config != null && config.isEnabled();
    }

    // Generate Parquet file name from source
    String baseName = sourceFile.getName();
    if (baseName.contains(".")) {
      int lastDot = baseName.lastIndexOf('.');
      if (lastDot > 0) {
        baseName = baseName.substring(0, lastDot);
      }
    }
    baseName = org.apache.calcite.adapter.file.converters.ConverterUtils.sanitizeIdentifier(baseName);
    
    // Create target Parquet file path
    String targetPath = cacheDirFile.getPath();
    if (!targetPath.endsWith("/")) {
      targetPath += "/";
    }
    targetPath += baseName + ".parquet";
    
    StorageProviderFile parquetFile = StorageProviderFile.create(targetPath, storageProvider);
    
    // Check if conversion is needed
    if (!needsConversion(sourceFile, parquetFile)) {
      LOGGER.debug("Parquet file is up to date, skipping conversion: {}", parquetFile.getPath());
      return parquetFile;
    }
    
    LOGGER.info("Converting {} to Parquet at: {}", sourceFile.getPath(), parquetFile.getPath());
    
    if (parquetFile.isLocal()) {
      // Use existing local file system approach with locking
      File localCacheDir = cacheDirFile.getFile();
      File localSourceFile = sourceFile.getFile();
      File result = ConcurrentParquetCache.convertWithLocking(localSourceFile, localCacheDir, 
          typeInferenceEnabled, schemaName, casing, tempFile -> {
        performConversion(source, tableName, table, tempFile, parentSchema, schemaName);
      });
      return StorageProviderFile.create(result.getAbsolutePath(), storageProvider);
    } else {
      // Use S3/remote storage approach
      performRemoteConversion(source, tableName, table, parquetFile, parentSchema, schemaName);
      return parquetFile;
    }
  }

  /**
   * Legacy method for backward compatibility - delegates to StorageProviderFile version.
   */
  public static File convertToParquet(Source source, String tableName, Table table,
      File cacheDir, SchemaPlus parentSchema, String schemaName, String casing) throws Exception {

    // Create StorageProviderFile instances for local files
    StorageProviderFile cacheDirFile = StorageProviderFile.create(cacheDir.getAbsolutePath(), 
        new org.apache.calcite.adapter.file.storage.LocalFileStorageProvider());
    
    StorageProviderFile result = convertToParquet(source, tableName, table, cacheDirFile, 
        parentSchema, schemaName, casing, new org.apache.calcite.adapter.file.storage.LocalFileStorageProvider());
    
    return result.getFile();
  }


  /**
   * Check if conversion is needed by comparing source and target timestamps.
   */
  private static boolean needsConversion(StorageProviderFile sourceFile, StorageProviderFile parquetFile) throws Exception {
    try {
      if (!parquetFile.exists()) {
        LOGGER.debug("Target Parquet file does not exist, conversion needed");
        return true;
      }
      
      long sourceTime = sourceFile.lastModified();
      long parquetTime = parquetFile.lastModified();
      
      boolean needsConversion = sourceTime > parquetTime;
      LOGGER.debug("Source time: {}, Parquet time: {}, needs conversion: {}", 
          sourceTime, parquetTime, needsConversion);
      
      return needsConversion;
    } catch (Exception e) {
      LOGGER.warn("Error checking conversion timestamps, assuming conversion needed: {}", e.getMessage());
      return true;
    }
  }

  /**
   * Check if conversion is needed by comparing source and target File timestamps (backward compatibility).
   */
  public static boolean needsConversion(File sourceFile, File parquetFile) {
    try {
      if (!parquetFile.exists()) {
        LOGGER.debug("Target Parquet file does not exist, conversion needed");
        return true;
      }
      
      long sourceTime = sourceFile.lastModified();
      long parquetTime = parquetFile.lastModified();
      
      boolean needsConversion = sourceTime > parquetTime;
      LOGGER.debug("Source time: {}, Parquet time: {}, needs conversion: {}", 
          sourceTime, parquetTime, needsConversion);
      
      return needsConversion;
    } catch (Exception e) {
      LOGGER.warn("Error checking conversion timestamps, assuming conversion needed: {}", e.getMessage());
      return true;
    }
  }

  /**
   * Perform conversion to remote storage (S3, etc.).
   */
  private static void performRemoteConversion(Source source, String tableName, Table table,
      StorageProviderFile targetFile, SchemaPlus parentSchema, String schemaName) throws Exception {
    
    LOGGER.debug("Starting remote Parquet conversion for table: {}", tableName);
    
    // For remote storage, we'll perform the conversion locally first, then upload
    java.io.File tempFile = java.io.File.createTempFile("parquet-conv-", ".parquet");
    try {
      // Use direct conversion to temp file
      if (!tryDirectConversion(table, tempFile, schemaName)) {
        throw new RuntimeException("Table does not support direct scanning: " + tableName +
            ". Only ScannableTable implementations are supported for Parquet conversion.");
      }
      
      // Upload temp file to target location
      try (java.io.InputStream input = java.nio.file.Files.newInputStream(tempFile.toPath())) {
        targetFile.writeInputStream(input);
      }
      
      LOGGER.info("Remote Parquet conversion completed: {}", targetFile.getPath());
    } finally {
      if (tempFile.exists()) {
        tempFile.delete();
      }
    }
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

    // Check if we need to handle blankStringsAsNull for CSV tables
    boolean blankStringsAsNull = false;
    if (table instanceof CsvTable) {
      CsvTypeInferrer.TypeInferenceConfig config = ((CsvTable) table).getTypeInferenceConfig();
      if (config != null) {
        blankStringsAsNull = config.isBlankStringsAsNull();
        LOGGER.info("ParquetConversionUtil: CsvTable with config, blankStringsAsNull={}, enabled={}",
            blankStringsAsNull, config.isEnabled());
      } else {
        // Default to true when type inference is not configured (disabled)
        blankStringsAsNull = true;
        LOGGER.info("ParquetConversionUtil: CsvTable with null config, defaulting blankStringsAsNull=true");
      }
    } else {
      LOGGER.info("ParquetConversionUtil: Non-CsvTable type: {}", table.getClass().getName());
    }

    if (table instanceof org.apache.calcite.schema.ScannableTable) {
      scannableTable = (org.apache.calcite.schema.ScannableTable) table;
    } else if (table instanceof org.apache.calcite.schema.TranslatableTable) {
      // Create an adapter wrapper for TranslatableTable to make it scannable
      scannableTable = new TranslatableTableAdapter((org.apache.calcite.schema.TranslatableTable) table, schemaName);
    } else {
      return false;
    }

    // Create a minimal DataContext for scanning
    final java.util.concurrent.atomic.AtomicBoolean cancelFlag = new java.util.concurrent.atomic.AtomicBoolean(false);
    org.apache.calcite.DataContext dataContext = new org.apache.calcite.DataContext() {
      @Override public SchemaPlus getRootSchema() { return null; }
      @Override public JavaTypeFactory getTypeFactory() {
        return new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
      }
      @Override public Object get(String name) {
        if ("spark".equals(name)) return false;
        if (org.apache.calcite.DataContext.Variable.CANCEL_FLAG.camelName.equals(name)) {
          return cancelFlag;
        }
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
    convertEnumerableToParquetDirect(enumerable, rowType, targetFile, typeFactory, blankStringsAsNull);

    return true;
  }

  /**
   * Convert an enumerable directly to Parquet using native Parquet writers.
   * This preserves the original schema structure and column names exactly.
   */
  private static void convertEnumerableToParquetDirect(org.apache.calcite.linq4j.Enumerable<Object[]> enumerable,
      org.apache.calcite.rel.type.RelDataType rowType, File targetFile, JavaTypeFactory typeFactory,
      boolean blankStringsAsNull) throws Exception {

    List<org.apache.calcite.rel.type.RelDataTypeField> fields = rowType.getFieldList();

    // Build Parquet schema from the original RelDataType (preserves original column names and types)
    List<org.apache.parquet.schema.Type> parquetFields = new ArrayList<>();
    for (org.apache.calcite.rel.type.RelDataTypeField field : fields) {
      String fieldName = field.getName(); // Preserve original field name exactly
      org.apache.calcite.sql.type.SqlTypeName sqlType = field.getType().getSqlTypeName();

      LOGGER.debug("Processing field: {} with type: {}", fieldName, sqlType);
      // Map Calcite types to Parquet types directly
      try {
        org.apache.parquet.schema.Type parquetField = createParquetFieldFromCalciteType(fieldName, sqlType, field);
        parquetFields.add(parquetField);
        LOGGER.debug("Successfully created Parquet field for: {}", fieldName);
      } catch (Exception e) {
        LOGGER.error("Failed to create Parquet field for: {} with type: {}", fieldName, sqlType, e);
        throw e;
      }
    }

    org.apache.parquet.schema.MessageType schema;
    try {
      schema = new org.apache.parquet.schema.MessageType("record", parquetFields);
      LOGGER.debug("Successfully created MessageType schema with {} fields", parquetFields.size());
      LOGGER.debug("=== Parquet Schema Details ===");
      for (org.apache.parquet.schema.Type field : schema.getFields()) {
        LOGGER.debug("  Field: {} | Type: {} | Repetition: {} | LogicalType: {}",
                    field.getName(),
                    field.asPrimitiveType().getPrimitiveTypeName(),
                    field.getRepetition(),
                    field.getLogicalTypeAnnotation());
      }
      LOGGER.debug("Full schema: {}", schema);
    } catch (Exception e) {
      LOGGER.error("Failed to create MessageType schema: {}", e.getMessage(), e);
      throw e;
    }

    // Determine the output path and configure Hadoop accordingly
    String outputPath;
    String targetFilePath = targetFile.getAbsolutePath();
    
    // Check if we're working with an S3 path (cache directory could be S3)
    if (isS3Path(targetFilePath)) {
      outputPath = getHadoopPath(targetFilePath);
      LOGGER.info("Writing Parquet file to S3: {}", outputPath);
    } else {
      // Delete existing local file if it exists
      if (targetFile.exists()) {
        targetFile.delete();
      }
      outputPath = targetFilePath;
      LOGGER.debug("Writing Parquet file locally: {}", outputPath);
    }

    // Write to Parquet using direct writer
    org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(outputPath);
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    conf.set("parquet.enable.vectorized.reader", "true");
    
    // Configure S3A access if needed
    if (isS3Path(targetFilePath)) {
      configureS3Access(conf);
    }

    org.apache.parquet.hadoop.example.GroupWriteSupport.setSchema(schema, conf);

    // Create ParquetWriter
    try (org.apache.parquet.hadoop.ParquetWriter<org.apache.parquet.example.data.Group> writer =
        createParquetWriter(hadoopPath, schema, conf)) {

      LOGGER.debug("Created Parquet writer successfully for schema: {}", schema);
      org.apache.parquet.example.data.simple.SimpleGroupFactory groupFactory;
      try {
        groupFactory = new org.apache.parquet.example.data.simple.SimpleGroupFactory(schema);
        LOGGER.debug("Created SimpleGroupFactory successfully");
      } catch (Exception e) {
        LOGGER.error("Failed to create SimpleGroupFactory: {}", e.getMessage(), e);
        throw e;
      }

      // Write all rows
      for (Object[] row : enumerable) {
        org.apache.parquet.example.data.Group group;
        try {
          group = groupFactory.newGroup();
          LOGGER.debug("Created new group successfully");
        } catch (Exception e) {
          LOGGER.error("Failed to create new group: {}", e.getMessage(), e);
          throw e;
        }

        for (int i = 0; i < fields.size() && i < row.length; i++) {
          org.apache.calcite.rel.type.RelDataTypeField field = fields.get(i);
          String fieldName = field.getName();
          Object value = row[i];

          LOGGER.debug("Processing field {}: {} = {} (type: {})", i, fieldName, value, field.getType().getSqlTypeName());

          // For VARCHAR/CHAR fields, always preserve empty strings
          // blankStringsAsNull should only apply to non-string types
          org.apache.calcite.sql.type.SqlTypeName sqlType = field.getType().getSqlTypeName();
          if (value != null && (sqlType == org.apache.calcite.sql.type.SqlTypeName.VARCHAR ||
               sqlType == org.apache.calcite.sql.type.SqlTypeName.CHAR) && value instanceof String) {
            String stringValue = (String) value;
            LOGGER.debug("Processing field {}: value='{}', blankStringsAsNull={}, isEmpty={}, trimIsEmpty={}",
                fieldName, stringValue, blankStringsAsNull, stringValue.isEmpty(), stringValue.trim().isEmpty());
            // Always preserve empty strings for VARCHAR/CHAR fields
            // blankStringsAsNull only affects non-string types
            LOGGER.debug("Preserving string value as-is for field: {} (blankStringsAsNull does not apply to strings)", fieldName);
          }

          if (value != null) {
            // Add value directly to group
            try {
              addValueToParquetGroup(group, fieldName, value, sqlType, blankStringsAsNull);
              LOGGER.debug("Successfully added value for field: {}", fieldName);
            } catch (Exception e) {
              LOGGER.error("Failed to add value for field {}: {}", fieldName, e.getMessage(), e);
              throw e;
            }
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

    // Force VARCHAR/CHAR fields to be OPTIONAL (nullable) to avoid DuckDB buffer issues
    org.apache.parquet.schema.Type.Repetition repetition;
    if (sqlType == org.apache.calcite.sql.type.SqlTypeName.VARCHAR ||
        sqlType == org.apache.calcite.sql.type.SqlTypeName.CHAR) {
      repetition = org.apache.parquet.schema.Type.Repetition.OPTIONAL;
      LOGGER.debug("FORCING VARCHAR/CHAR field '{}' to be OPTIONAL for DuckDB compatibility", fieldName);
    } else {
      repetition = field.getType().isNullable()
          ? org.apache.parquet.schema.Type.Repetition.OPTIONAL
          : org.apache.parquet.schema.Type.Repetition.REQUIRED;
    }

    LOGGER.debug("Creating Parquet field '{}': sqlType={}, isNullable={}, repetition={}",
                fieldName, sqlType, field.getType().isNullable(), repetition);

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
        // Add a logical type annotation to distinguish from timestamp INT64 fields
        return org.apache.parquet.schema.Types.primitive(
            org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64, repetition)
            .as(org.apache.parquet.schema.LogicalTypeAnnotation.intType(64, true))
            .named(fieldName);

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
        // TIME is just time of day, no timezone adjustment needed
        return org.apache.parquet.schema.Types.primitive(
            org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32, repetition)
            .as(
                org.apache.parquet.schema.LogicalTypeAnnotation.timeType(false,
                org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named(fieldName);

      case TIMESTAMP:
        // Use INT64 with timestamp logical type annotation
        // For TIMESTAMP WITHOUT TIME ZONE, we use isAdjustedToUTC=false
        // This tells DuckDB this is a naive timestamp (no timezone information)
        LOGGER.debug("Creating TIMESTAMP field '{}' with isAdjustedToUTC=false (timezone-naive)", fieldName);
        return org.apache.parquet.schema.Types.primitive(
            org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64, repetition)
            .as(
                org.apache.parquet.schema.LogicalTypeAnnotation.timestampType(false,
                org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named(fieldName);

      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        // Use INT64 with timestamp logical type annotation (milliseconds, adjusted to UTC)
        // For TIMESTAMP WITH TIME ZONE, we use isAdjustedToUTC=true
        // This tells DuckDB this timestamp has timezone information and is stored in UTC
        LOGGER.debug("Creating TIMESTAMP_WITH_LOCAL_TIME_ZONE field '{}' with isAdjustedToUTC=true (timezone-aware)", fieldName);
        return org.apache.parquet.schema.Types.primitive(
            org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64, repetition)
            .as(
                org.apache.parquet.schema.LogicalTypeAnnotation.timestampType(true,
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
   * Check if a string value represents a null value using default null equivalents.
   */
  private static boolean isNullRepresentation(String value) {
    return NullEquivalents.isNullRepresentation(value);
  }

  /**
   * Check if a string value represents a null value using specified null equivalents.
   */
  private static boolean isNullRepresentation(String value, Set<String> nullEquivalents) {
    return NullEquivalents.isNullRepresentation(value, nullEquivalents);
  }

  /**
   * WTFAdd a value to a Parquet group with proper type conversion.
   */
  private static void addValueToParquetGroup(org.apache.parquet.example.data.Group group,
      String fieldName, Object value, org.apache.calcite.sql.type.SqlTypeName sqlType, boolean blankStringsAsNull) {

    LOGGER.info("[addValueToParquetGroup] Called with field={}, value={}, sqlType={}, blankStringsAsNull={}",
                fieldName, (value == null ? "null" : "'" + value + "'"), sqlType, blankStringsAsNull);

    if (value == null) {
      // For null values, skip (Parquet handles null via absence)
      LOGGER.debug("[addValueToParquetGroup] Value is null, skipping field: {}", fieldName);
      return;
    }

    // For VARCHAR/CHAR fields, empty strings are valid values, not nulls
    // Only check for null representations for non-string SQL types
    if (value instanceof String &&
        sqlType != org.apache.calcite.sql.type.SqlTypeName.VARCHAR &&
        sqlType != org.apache.calcite.sql.type.SqlTypeName.CHAR) {
      // For non-string types (numbers, dates), check if the string represents null
      if (isNullRepresentation((String) value)) {
        // It's a null representation for a non-string type, skip
        return;
      }
    }

    // For VARCHAR/CHAR fields, always preserve empty strings regardless of blankStringsAsNull
    // The blankStringsAsNull setting only applies to non-string types
    LOGGER.info("[addValueToParquetGroup] Checking if value is String: {}, sqlType: {}",
                value instanceof String, sqlType);
    if (value instanceof String &&
        (sqlType == org.apache.calcite.sql.type.SqlTypeName.VARCHAR ||
         sqlType == org.apache.calcite.sql.type.SqlTypeName.CHAR)) {
      String strValue = (String) value;
      LOGGER.info("[addValueToParquetGroup] VARCHAR field '{}': value='{}', length={}, isEmpty={}",
                  fieldName, strValue, strValue.length(), strValue.isEmpty());
      if (strValue.isEmpty()) {
        // Always preserve empty strings for VARCHAR/CHAR fields
        LOGGER.info("[addValueToParquetGroup] Preserving empty string for field '{}' (blankStringsAsNull does not apply to strings)", fieldName);
        group.append(fieldName, "");
        LOGGER.info("[addValueToParquetGroup] Successfully appended empty string to field '{}'", fieldName);
        return;
      }
    }

    LOGGER.debug("Adding value to Parquet group: field={}, value={}, sqlType={}, valueClass={}",
                fieldName, value, sqlType, value.getClass().getName());

    // Check if field exists in the schema to prevent "is not primitive" errors
    try {
      org.apache.parquet.schema.Type fieldType = group.getType().getType(fieldName);
      if (fieldType == null) {
        LOGGER.warn("Field '{}' not found in Parquet schema, skipping", fieldName);
        return;
      }
      LOGGER.debug("Found field type in schema: {}", fieldType);
    } catch (Exception e) {
      LOGGER.warn("Field '{}' not found in Parquet schema: {}, skipping", fieldName, e.getMessage());
      return;
    }

    switch (sqlType) {
      case BOOLEAN:
        if (value instanceof Boolean) {
          group.append(fieldName, (Boolean) value);
        } else {
          String strValue = value.toString();
          // For non-string types, check again for null representations
          if (isNullRepresentation(strValue)) {
            return;
          }
          try {
            group.append(fieldName, Boolean.parseBoolean(strValue));
          } catch (Exception e) {
            LOGGER.error("Failed to parse boolean value '{}' for field '{}': {}", strValue, fieldName, e.getMessage());
            // Don't add value (treat as null)
            return;
          }
        }
        break;

      case TINYINT:
      case SMALLINT:
      case INTEGER:
        if (value instanceof Number) {
          group.append(fieldName, ((Number) value).intValue());
        } else {
          String strValue = value.toString();
          // For non-string types, check again for null representations
          if (isNullRepresentation(strValue)) {
            return;
          }
          try {
            group.append(fieldName, Integer.parseInt(strValue));
          } catch (NumberFormatException e) {
            LOGGER.error("Failed to parse integer value '{}' for field '{}': {}", strValue, fieldName, e.getMessage());
            // Don't add value (treat as null)
            return;
          }
        }
        break;

      case BIGINT:
        if (value instanceof Number) {
          group.append(fieldName, ((Number) value).longValue());
        } else {
          String strValue = value.toString();
          // For non-string types, check again for null representations
          if (isNullRepresentation(strValue)) {
            return;
          }
          try {
            group.append(fieldName, Long.parseLong(strValue));
          } catch (NumberFormatException e) {
            LOGGER.error("Failed to parse long value '{}' for field '{}': {}", strValue, fieldName, e.getMessage());
            // Don't add value (treat as null)
            return;
          }
        }
        break;

      case FLOAT:
      case REAL:
        if (value instanceof Number) {
          group.append(fieldName, ((Number) value).floatValue());
        } else {
          String strValue = value.toString();
          // For non-string types, check again for null representations
          if (isNullRepresentation(strValue)) {
            return;
          }
          try {
            group.append(fieldName, Float.parseFloat(strValue));
          } catch (NumberFormatException e) {
            LOGGER.error("Failed to parse float value '{}' for field '{}': {}", strValue, fieldName, e.getMessage());
            // Don't add value (treat as null)
            return;
          }
        }
        break;

      case DOUBLE:
        if (value instanceof Number) {
          group.append(fieldName, ((Number) value).doubleValue());
        } else {
          String strValue = value.toString();
          // For non-string types, check again for null representations
          if (isNullRepresentation(strValue)) {
            return;
          }
          try {
            group.append(fieldName, Double.parseDouble(strValue));
          } catch (NumberFormatException e) {
            LOGGER.error("Failed to parse double value '{}' for field '{}': {}", strValue, fieldName, e.getMessage());
            // Don't add value (treat as null)
            return;
          }
        }
        break;

      case DECIMAL:
        if (value instanceof BigDecimal) {
          BigDecimal decimal = (BigDecimal) value;
          group.append(
              fieldName, org.apache.parquet.io.api.Binary.fromConstantByteArray(
              decimal.unscaledValue().toByteArray()));
        } else {
          String strValue = value.toString();
          // For non-string types, check again for null representations
          if (isNullRepresentation(strValue)) {
            return;
          }
          try {
            BigDecimal decimal = new BigDecimal(strValue);
            group.append(
                fieldName, org.apache.parquet.io.api.Binary.fromConstantByteArray(
                decimal.unscaledValue().toByteArray()));
          } catch (NumberFormatException e) {
            LOGGER.error("Failed to parse decimal value '{}' for field '{}': {}", strValue, fieldName, e.getMessage());
            // Don't add value (treat as null)
            return;
          }
        }
        break;

      case DATE:
        if (value instanceof java.sql.Date) {
          // Convert to days since epoch (1970-01-01) in UTC
          // Use Instant from millis with UTC zone to avoid timezone issues
          java.sql.Date sqlDate = (java.sql.Date) value;
          java.time.Instant instant = java.time.Instant.ofEpochMilli(sqlDate.getTime());
          java.time.LocalDate localDate = instant.atZone(java.time.ZoneOffset.UTC).toLocalDate();
          int daysSinceEpoch = (int) localDate.toEpochDay();
          group.append(fieldName, daysSinceEpoch);
        } else if (value instanceof java.time.LocalDate) {
          group.append(fieldName, (int) ((java.time.LocalDate) value).toEpochDay());
        } else if (value instanceof Integer) {
          // Already in days since epoch format
          group.append(fieldName, (Integer) value);
        } else {
          String strValue = value.toString();
          // For non-string types, check again for null representations
          if (isNullRepresentation(strValue)) {
            return;
          }
          // Try to parse as date string
          try {
            java.time.LocalDate localDate = java.time.LocalDate.parse(strValue);
            group.append(fieldName, (int) localDate.toEpochDay());
          } catch (Exception e) {
            LOGGER.error("Failed to parse date value '{}' for field '{}': {}", strValue, fieldName, e.getMessage());
            // Don't add value (treat as null)
            return;
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
        // For TIMESTAMP WITHOUT TIME ZONE, the value from CsvEnumerator is adjusted for local timezone
        // We need to convert it back to UTC for storage since we're using isAdjustedToUTC=true
        int fieldIndexTs = group.getType().getFieldIndex(fieldName);

        if (value instanceof java.sql.Timestamp) {
          // The timestamp from CsvEnumerator has timezone adjustment applied
          java.sql.Timestamp ts = (java.sql.Timestamp) value;
          long adjustedMillis = ts.getTime();

          // Get the timezone offset for this timestamp
          java.util.TimeZone tz = java.util.TimeZone.getDefault();
          long offset = tz.getOffset(adjustedMillis);

          // Add the offset to get back to UTC (offset is negative for US timezones)
          // For example, if the value is 838972862000 (04:01:02 EDT),
          // we add the -4 hour offset (which subtracts 4 hours) to get 838958462000 (00:01:02 UTC)
          long utcMillis = adjustedMillis + offset;

          LOGGER.debug("TIMESTAMP storage: field={}, input value={}, adjusted millis={}, offset={}, storing UTC millis={}",
                      fieldName, value, adjustedMillis, offset, utcMillis);
          group.add(fieldIndexTs, utcMillis);
        } else if (value instanceof java.time.LocalDateTime) {
          // Convert LocalDateTime to UTC millis
          long millis = ((java.time.LocalDateTime) value)
              .atZone(java.time.ZoneOffset.UTC)
              .toInstant()
              .toEpochMilli();
          group.add(fieldIndexTs, millis);
        } else if (value instanceof Long) {
          group.add(fieldIndexTs, (Long) value);
        } else if (value != null) {
          // Try to parse as timestamp string
          try {
            java.sql.Timestamp ts = java.sql.Timestamp.valueOf(value.toString());
            group.add(fieldIndexTs, ts.getTime());
          } catch (Exception e) {
            LOGGER.warn("Failed to parse timestamp value: {}", value);
          }
        }
        break;

      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        // Get the field index to use group.add() instead of group.append()
        // This avoids the Parquet library bug with group.append() for TIMESTAMP fields
        int fieldIndex = group.getType().getFieldIndex(fieldName);

        if (value instanceof java.sql.Timestamp) {
          // For timezone-aware timestamps, store the actual epoch millis
          long millis = ((java.sql.Timestamp) value).getTime();
          group.add(fieldIndex, millis);
        } else if (value instanceof java.time.LocalDateTime) {
          // Convert to milliseconds since epoch in UTC
          long millis = ((java.time.LocalDateTime) value).atZone(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();
          group.add(fieldIndex, millis);
        } else if (value instanceof java.time.Instant) {
          group.add(fieldIndex, ((java.time.Instant) value).toEpochMilli());
        } else if (value instanceof Number) {
          // Already in milliseconds since epoch format
          group.add(fieldIndex, ((Number) value).longValue());
        } else {
          String strValue = value.toString();
          // For non-string types, check again for null representations
          if (isNullRepresentation(strValue)) {
            return;
          }
          // Use the same type converter as LINQ4J engine
          try {
            org.apache.calcite.adapter.file.format.csv.CsvTypeConverter converter =
                new org.apache.calcite.adapter.file.format.csv.CsvTypeConverter(null, null, true);
            Object converted = converter.convert(strValue, org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP);
            if (converted != null) {
              long millis = (Long) converted;
              group.add(fieldIndex, millis);
            }
            // If converted is null, don't add value (treat as null)
          } catch (Exception e) {
            LOGGER.error("Failed to parse timestamp value '{}' for field '{}': {}", strValue, fieldName, e.getMessage());
            // Don't add value (treat as null)
            return;
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

    @Override public org.apache.calcite.rel.type.RelDataType getRowType(org.apache.calcite.rel.type.RelDataTypeFactory typeFactory) {
      return translatableTable.getRowType(typeFactory);
    }

    @Override public org.apache.calcite.linq4j.Enumerable<Object[]> scan(org.apache.calcite.DataContext root) {
      try {
        // Create a temporary Calcite connection to execute the translation
        java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:calcite:");
        org.apache.calcite.jdbc.CalciteConnection calciteConn = conn.unwrap(org.apache.calcite.jdbc.CalciteConnection.class);

        SchemaPlus rootSchema = calciteConn.getRootSchema();

        // Create a temporary schema with just this table
        String tempTableName = "temp_table_" + System.currentTimeMillis();
        SchemaPlus tempSchema = rootSchema.add("TEMP_SCAN", new org.apache.calcite.schema.impl.AbstractSchema() {
          @Override protected java.util.Map<String, org.apache.calcite.schema.Table> getTableMap() {
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
              // Check field type to handle VARCHAR/CHAR specially
              org.apache.calcite.rel.type.RelDataTypeField field = rowType.getFieldList().get(i);
              org.apache.calcite.sql.type.SqlTypeName sqlType = field.getType().getSqlTypeName();

              if (sqlType == org.apache.calcite.sql.type.SqlTypeName.VARCHAR ||
                  sqlType == org.apache.calcite.sql.type.SqlTypeName.CHAR) {
                // For VARCHAR/CHAR, use getString to preserve empty strings
                String strValue = rs.getString(i + 1);
                // getString returns null for SQL NULL, but empty string for empty string
                row[i] = strValue;
                LOGGER.debug("TranslatableTableAdapter: Column {} ({}): value='{}' (null={})",
                            i, field.getName(), strValue, (strValue == null));
              } else {
                // For other types, use getObject
                row[i] = rs.getObject(i + 1);
              }
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
