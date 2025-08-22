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

import org.apache.calcite.adapter.file.converters.ConverterUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/**
 * Direct Parquet writer that preserves timestamp type information.
 * This writer skips the Avro conversion step and writes directly to Parquet,
 * allowing us to properly set the isAdjustedToUTC flag for timestamps.
 */
public class DirectParquetWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(DirectParquetWriter.class);
  

  private DirectParquetWriter() {
    // Utility class
  }

  /**
   * Write ResultSet directly to Parquet file.
   */
  public static void writeResultSetToParquet(ResultSet rs, Path outputPath) throws SQLException, IOException {
    ResultSetMetaData rsmd = rs.getMetaData();
    int columnCount = rsmd.getColumnCount();

    // Build Parquet schema from ResultSet metadata
    List<Type> fields = new ArrayList<>();
    for (int i = 1; i <= columnCount; i++) {
      String columnName = ConverterUtils.sanitizeIdentifier(rsmd.getColumnName(i));
      int sqlType = rsmd.getColumnType(i);
      Type field = createParquetField(columnName, sqlType, i, rsmd);
      fields.add(field);
    }

    MessageType schema = new MessageType("record", fields);

    // Configure Parquet writer
    Configuration conf = new Configuration();
    
    // Enable vectorized reading for better performance
    conf.set("parquet.enable.vectorized.reader", "true");
    
    GroupWriteSupport.setSchema(schema, conf);

    // Create writer
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputPath)
        .withConf(conf)
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
        .withPageSize(ParquetProperties.DEFAULT_PAGE_SIZE)
        .withDictionaryEncoding(true)
        .build()) {

      SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

      // Write all rows
      while (rs.next()) {
        Group group = groupFactory.newGroup();

        for (int i = 1; i <= columnCount; i++) {
          String columnName = ConverterUtils.sanitizeIdentifier(rsmd.getColumnName(i));
          int sqlType = rsmd.getColumnType(i);

          // Check for null values
          Object value = rs.getObject(i);
          if (value == null || rs.wasNull()) {
            // Skip null values - Parquet handles nulls through repetition levels
            continue;
          }

          // Add value to group based on SQL type
          addValueToGroup(group, columnName, sqlType, rs, i);
        }

        writer.write(group);
      }
    }
  }

  private static Type createParquetField(String name, int sqlType, int index, ResultSetMetaData rsmd)
      throws SQLException {
    Type.Repetition repetition = rsmd.isNullable(index) == ResultSetMetaData.columnNullable
        ? OPTIONAL : REQUIRED;

    switch (sqlType) {
      case java.sql.Types.BOOLEAN:
        return org.apache.parquet.schema.Types.primitive(BOOLEAN, repetition).named(name);

      case java.sql.Types.TINYINT:
      case java.sql.Types.SMALLINT:
      case java.sql.Types.INTEGER:
        return org.apache.parquet.schema.Types.primitive(INT32, repetition).named(name);

      case java.sql.Types.BIGINT:
        return org.apache.parquet.schema.Types.primitive(INT64, repetition).named(name);

      case java.sql.Types.FLOAT:
      case java.sql.Types.REAL:
        return org.apache.parquet.schema.Types.primitive(FLOAT, repetition).named(name);

      case java.sql.Types.DOUBLE:
        return org.apache.parquet.schema.Types.primitive(DOUBLE, repetition).named(name);

      case java.sql.Types.DECIMAL:
      case java.sql.Types.NUMERIC:
        int precision = rsmd.getPrecision(index);
        int scale = rsmd.getScale(index);
        if (precision <= 0) {
          precision = 38; // Default precision
        }
        return org.apache.parquet.schema.Types.primitive(BINARY, repetition)
            .as(LogicalTypeAnnotation.decimalType(scale, precision))
            .named(name);

      case java.sql.Types.DATE:
        return org.apache.parquet.schema.Types.primitive(INT32, repetition)
            .as(LogicalTypeAnnotation.dateType())
            .named(name);

      case java.sql.Types.TIME:
        // TIME is just time of day, no timezone adjustment needed
        return org.apache.parquet.schema.Types.primitive(INT32, repetition)
            .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named(name);

      case java.sql.Types.TIMESTAMP:
        // Timezone-naive timestamp - isAdjustedToUTC = true
        return org.apache.parquet.schema.Types.primitive(INT64, repetition)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named(name);

      case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
        // Timezone-aware timestamp - isAdjustedToUTC = true
        return org.apache.parquet.schema.Types.primitive(INT64, repetition)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named(name);

      default:
        // Default to string for unknown types
        return org.apache.parquet.schema.Types.primitive(BINARY, repetition)
            .as(LogicalTypeAnnotation.stringType())
            .named(name);
    }
  }

  private static void addValueToGroup(Group group, String columnName, int sqlType,
      ResultSet rs, int index) throws SQLException {
    switch (sqlType) {
      case java.sql.Types.BOOLEAN:
        group.append(columnName, rs.getBoolean(index));
        break;

      case java.sql.Types.TINYINT:
      case java.sql.Types.SMALLINT:
      case java.sql.Types.INTEGER:
        group.append(columnName, rs.getInt(index));
        break;

      case java.sql.Types.BIGINT:
        group.append(columnName, rs.getLong(index));
        break;

      case java.sql.Types.FLOAT:
      case java.sql.Types.REAL:
        group.append(columnName, rs.getFloat(index));
        break;

      case java.sql.Types.DOUBLE:
        group.append(columnName, rs.getDouble(index));
        break;

      case java.sql.Types.DECIMAL:
      case java.sql.Types.NUMERIC:
        BigDecimal decimal = rs.getBigDecimal(index);
        if (decimal != null) {
          group.append(
              columnName, org.apache.parquet.io.api.Binary.fromConstantByteArray(
              decimal.unscaledValue().toByteArray()));
        }
        break;

      case java.sql.Types.DATE:
        java.sql.Date date = rs.getDate(index);
        if (date != null) {
          // DATE type should never involve timezones
          // Convert to LocalDate using UTC instant to avoid timezone issues
          java.time.Instant instant = java.time.Instant.ofEpochMilli(date.getTime());
          java.time.LocalDate localDate = instant.atZone(java.time.ZoneOffset.UTC).toLocalDate();
          int daysSinceEpoch = (int) localDate.toEpochDay();
          
          LOGGER.debug("DATE storage: column={}, date={}, daysSinceEpoch={}", 
                      columnName, date, daysSinceEpoch);
          
          group.append(columnName, daysSinceEpoch);
        }
        break;

      case java.sql.Types.TIME:
        // Get the value as an object first to handle both Time and LocalTime
        Object timeValue = rs.getObject(index);
        if (timeValue != null) {
          if (timeValue instanceof java.time.LocalTime) {
            // LocalTime is the preferred representation
            java.time.LocalTime localTime = (java.time.LocalTime) timeValue;
            int millisSinceMidnight = (int) (localTime.toNanoOfDay() / 1_000_000L);
            group.append(columnName, millisSinceMidnight);
          } else if (timeValue instanceof java.sql.Time) {
            // Legacy Time support - convert via LocalTime
            java.sql.Time time = (java.sql.Time) timeValue;
            java.time.LocalTime localTime = time.toLocalTime();
            int millisSinceMidnight = (int) (localTime.toNanoOfDay() / 1_000_000L);
            group.append(columnName, millisSinceMidnight);
          }
        }
        break;

      case java.sql.Types.TIMESTAMP:
        // For TIMESTAMP WITHOUT TIME ZONE (wall clock time):
        // The CsvEnumerator already provides the correct UTC value for the wall clock time.
        // For example, "1996-08-02 00:01:02" becomes 838958462000 (UTC representation).
        // We store this value directly in parquet.
        java.sql.Timestamp timestamp = rs.getTimestamp(index);
        if (timestamp != null) {
          // The timestamp from CsvEnumerator is already the UTC representation
          // of the wall clock time that we need to store
          long utcMillis = timestamp.getTime();
          
          LOGGER.debug("TIMESTAMP storage: column={}, storing UTC value={}, as timestamp={}", 
                      columnName, utcMillis, new java.sql.Timestamp(utcMillis));
          
          group.append(columnName, utcMillis);
        }
        break;
        
      case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
        // For timezone-aware timestamps, use the standard getTimestamp
        java.sql.Timestamp timestampTz = rs.getTimestamp(index);
        if (timestampTz != null) {
          group.append(columnName, timestampTz.getTime());
        }
        break;

      default:
        String stringValue = rs.getString(index);
        if (stringValue != null) {
          group.append(columnName, stringValue);
        }
        // If stringValue is null, don't write anything (Parquet will handle it as NULL)
        break;
    }
  }

  /**
   * Custom ParquetWriter builder for Group objects.
   */
  @SuppressWarnings("deprecation")
  private static class ExampleParquetWriter extends ParquetWriter<Group> {
    public ExampleParquetWriter(Path path, GroupWriteSupport writeSupport,
                                CompressionCodecName compressionCodecName,
                                int blockSize, int pageSize, boolean enableDictionary,
                                boolean enableValidation, ParquetProperties.WriterVersion writerVersion,
                                Configuration conf) throws IOException {
      super(path, writeSupport, compressionCodecName, blockSize, pageSize,
          pageSize, enableDictionary, enableValidation, writerVersion, conf);
    }

    public static Builder builder(Path path) {
      return new Builder(path);
    }

    public static class Builder extends ParquetWriter.Builder<Group, Builder> {
      private MessageType schema = null;

      private Builder(Path path) {
        super(path);
      }

      public Builder withSchema(MessageType schema) {
        this.schema = schema;
        return this;
      }

      @Override protected Builder self() {
        return this;
      }

      @Override @SuppressWarnings("deprecation")
      protected GroupWriteSupport getWriteSupport(Configuration conf) {
        GroupWriteSupport.setSchema(schema, conf);
        return new GroupWriteSupport();
      }
    }
  }
}
