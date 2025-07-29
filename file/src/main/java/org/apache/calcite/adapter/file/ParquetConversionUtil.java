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

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Source;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collections;
import java.util.Map;

/**
 * Utility class for converting various file formats to Parquet.
 */
public class ParquetConversionUtil {

  /**
   * Get the cache directory for Parquet conversions.
   */
  public static File getParquetCacheDir(File baseDirectory) {
    File cacheDir = new File(baseDirectory, ".parquet_cache");
    if (!cacheDir.exists()) {
      cacheDir.mkdirs();
    }
    return cacheDir;
  }

  /**
   * Get the cached Parquet file for a source file.
   */
  public static File getCachedParquetFile(File sourceFile, File cacheDir) {
    String baseName = sourceFile.getName();
    // Remove existing extensions
    int lastDot = baseName.lastIndexOf('.');
    if (lastDot > 0) {
      baseName = baseName.substring(0, lastDot);
    }
    return new File(cacheDir, baseName + ".parquet");
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
    
    // Use concurrent cache for thread-safe conversion
    return ConcurrentParquetCache.convertWithLocking(sourceFile, cacheDir, (tempFile) -> {
      performConversion(source, tableName, table, tempFile, parentSchema, schemaName);
    });
  }
  
  /**
   * Perform the actual conversion to a temporary file.
   */
  private static void performConversion(Source source, String tableName, Table table,
      File targetFile, SchemaPlus parentSchema, String schemaName) throws Exception {
    
    System.out.println("Converting " + tableName + " to Parquet format...");

    // Create a temporary schema with just this table
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConn.getRootSchema();
      SchemaPlus tempSchema = rootSchema.add("TEMP_CONVERT", new AbstractSchema() {
        @Override protected Map<String, Table> getTableMap() {
          return Collections.singletonMap(tableName, table);
        }
      });

      // Query the table to get all data
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT * FROM TEMP_CONVERT.\"" + tableName + "\"")) {

        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();

        // Build Avro schema from ResultSet metadata
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record(tableName + "_record")
            .namespace("org.apache.calcite.adapter.file.cache")
            .fields();

        for (int i = 1; i <= columnCount; i++) {
          String columnName = rsmd.getColumnName(i);
          int sqlType = rsmd.getColumnType(i);

          // Add nullable types to handle null values
          switch (sqlType) {
            case Types.INTEGER:
            case Types.BIGINT:
              fieldAssembler.name(columnName).type().nullable().longType().noDefault();
              break;
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.DECIMAL:
              fieldAssembler.name(columnName).type().nullable().doubleType().noDefault();
              break;
            case Types.BOOLEAN:
              fieldAssembler.name(columnName).type().nullable().booleanType().noDefault();
              break;
            default:
              fieldAssembler.name(columnName).type().nullable().stringType().noDefault();
              break;
          }
        }

        Schema avroSchema = fieldAssembler.endRecord();

        // Delete existing file if it exists
        if (targetFile.exists()) {
          targetFile.delete();
        }

        // Write to Parquet
        Path hadoopPath = new Path(targetFile.getAbsolutePath());
        Configuration conf = new Configuration();

        @SuppressWarnings("deprecation")
        ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath)
            .withConf(conf)
            .withSchema(avroSchema)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE)
            .build();

        try {
          int rowCount = 0;
          while (rs.next()) {
            GenericRecord record = new GenericData.Record(avroSchema);

            for (int i = 1; i <= columnCount; i++) {
              String columnName = rsmd.getColumnName(i);
              int sqlType = rsmd.getColumnType(i);

              // Get value and check for nulls
              Object value = rs.getObject(i);
              if (value != null && !rs.wasNull()) {
                switch (sqlType) {
                  case Types.INTEGER:
                  case Types.BIGINT:
                    record.put(columnName, rs.getLong(i));
                    break;
                  case Types.FLOAT:
                  case Types.DOUBLE:
                  case Types.DECIMAL:
                    record.put(columnName, rs.getDouble(i));
                    break;
                  case Types.BOOLEAN:
                    record.put(columnName, rs.getBoolean(i));
                    break;
                  default:
                    record.put(columnName, rs.getString(i));
                    break;
                }
              }
            }

            writer.write(record);
            rowCount++;
          }

          System.out.println("Converted " + rowCount + " rows from " + tableName + " to Parquet");

        } finally {
          writer.close();
        }
      }
    }

    // Set the modification time to match the source file
    File sourceFile = new File(source.path());
    targetFile.setLastModified(sourceFile.lastModified());
  }
}
