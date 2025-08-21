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
package org.apache.calcite.adapter.file.execution.parquet;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating Enumerable instances from Parquet files.
 */
@SuppressWarnings("deprecation")
public class ParquetEnumerableFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetEnumerableFactory.class);

  private ParquetEnumerableFactory() {
    // Utility class should not be instantiated
  }

  /**
   * Creates an Enumerable that reads from a Parquet file.
   * This method is called via reflection from generated code.
   */
  public static Enumerable<Object[]> enumerable(String filePath) {
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        // Check if vectorized reading is enabled
        Configuration conf = new Configuration();
        String systemProperty = System.getProperty("parquet.enable.vectorized.reader", "false");
        conf.set("parquet.enable.vectorized.reader", systemProperty);
        boolean useVectorized = "true".equals(systemProperty);
        
        if (useVectorized) {
          LOGGER.debug("Using batch reader for {} (batches rows for better I/O efficiency)", filePath);
          return new VectorizedParquetEnumerator(filePath);
        } else {
          LOGGER.debug("Using record-by-record reader for {}", filePath);
          return new ParquetEnumerator(filePath);
        }
      }
    };
  }

  /**
   * Creates an Enumerable that reads from a Parquet file with null filtering.
   * This method is called via reflection from generated code when filters are present.
   */
  public static Enumerable<Object[]> enumerableWithFilters(String filePath, boolean[] nullFilters) {
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        // Check if vectorized reading is enabled
        Configuration conf = new Configuration();
        String systemProperty = System.getProperty("parquet.enable.vectorized.reader", "false");
        conf.set("parquet.enable.vectorized.reader", systemProperty);
        boolean useVectorized = "true".equals(systemProperty);
        
        if (useVectorized) {
          LOGGER.debug("Using batch filtered reader for {} (batches rows for better I/O efficiency)", filePath);
          return new VectorizedFilteredParquetEnumerator(filePath, nullFilters);
        } else {
          LOGGER.debug("Using record-by-record filtered reader for {}", filePath);
          return new FilteredParquetEnumerator(filePath, nullFilters);
        }
      }
    };
  }

  /**
   * Creates an Enumerable that reads from a Parquet file with TIME filtering.
   * This method filters out rows with null TIME values at the Parquet level.
   */
  public static Enumerable<Object[]> enumerableWithTimeFiltering(String filePath) {
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        return new TimeFilteredParquetEnumerator(filePath);
      }
    };
  }

  /**
   * Enumerator that reads from Parquet files.
   */
  private static class ParquetEnumerator implements Enumerator<Object[]> {
    private final String filePath;
    private ParquetReader<GenericRecord> reader;
    private GenericRecord current;
    private Object[] currentRow;
    private boolean finished = false;
    private Map<String, Boolean> timestampAdjustedMap = new HashMap<>();

    ParquetEnumerator(String filePath) {
      this.filePath = filePath;
      initReader();
    }

    private void initReader() {
      try {
        Path hadoopPath = new Path(filePath);
        Configuration conf = new Configuration();
        
        // Enable vectorized reading for better performance
        conf.set("parquet.enable.vectorized.reader", "true");

        // Read Parquet schema to get timestamp metadata
        try (ParquetFileReader fileReader = ParquetFileReader.open(conf, hadoopPath)) {
          ParquetMetadata metadata = fileReader.getFooter();
          MessageType schema = metadata.getFileMetaData().getSchema();
          
          // Build map of field names to isAdjustedToUTC flags
          for (Type field : schema.getFields()) {
            if (field.isPrimitive()) {
              LogicalTypeAnnotation logicalType = field.getLogicalTypeAnnotation();
              if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
                LogicalTypeAnnotation.TimestampLogicalTypeAnnotation tsType = 
                    (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalType;
                timestampAdjustedMap.put(field.getName(), tsType.isAdjustedToUTC());
                LOGGER.debug("[ParquetEnumerator] Field {} has isAdjustedToUTC={}", 
                    field.getName(), tsType.isAdjustedToUTC());
              }
            }
          }
        }

        @SuppressWarnings("deprecation")
        ParquetReader<GenericRecord> tempReader =
            AvroParquetReader.<GenericRecord>builder(hadoopPath)
            .withConf(conf)
            .build();
        reader = tempReader;

      } catch (IOException e) {
        throw new RuntimeException("Failed to initialize Parquet reader for " + filePath, e);
      }
    }

    @Override public Object[] current() {
      return currentRow;
    }

    @Override public boolean moveNext() {
      if (finished) {
        return false;
      }

      try {
        current = reader.read();
        if (current == null) {
          finished = true;
          return false;
        }

        // Convert GenericRecord to Object[]
        int fieldCount = current.getSchema().getFields().size();
        currentRow = new Object[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
          Object value = current.get(i);

          // Get the field schema to check for logical types
          Schema.Field field = current.getSchema().getFields().get(i);
          Schema fieldSchema = field.schema();

          // Handle union types (nullable fields)
          if (fieldSchema.getType() == Schema.Type.UNION) {
            for (Schema unionType : fieldSchema.getTypes()) {
              if (unionType.getType() != Schema.Type.NULL) {
                fieldSchema = unionType;
                break;
              }
            }
          }

          // Convert based on logical type
          LogicalType logicalType = fieldSchema.getLogicalType();
          if (logicalType != null) {
            String logicalTypeName = logicalType.getName();
            if ("date".equals(logicalTypeName)) {
              if (value instanceof Integer) {
                // Keep as Integer to match LINQ4J engine behavior
                // CsvEnumerator returns Integer for DATE type, not java.sql.Date
                // This is days since epoch - no conversion needed
                // value is already an Integer with days since epoch
              } else if (value == null) {
                // Keep null values as null
                value = null;
              } else {
                // Log unexpected value type
                LOGGER.debug("[ParquetEnumerableFactory] Unexpected DATE value type: {}", 
                    (value != null ? value.getClass().getName() : "null"));
              }
            } else if ("time-millis".equals(logicalTypeName)) {
              if (value instanceof Integer) {
                // Keep as integer - milliseconds since midnight
                // This is the correct representation for TIME values
              } else if (value == null) {
                // Handle null times
                value = null;
              }
            } else if ("timestamp-millis".equals(logicalTypeName)) {
              if (value instanceof Long) {
                long milliseconds = (Long) value;
                String fieldName = field.name();
                
                // For Parquet engine, return long values for timestamps to avoid casting issues
                // in ORDER BY and WHERE clauses. The timestamp display formatting is handled
                // by the result set processing.
                value = milliseconds;
              } else if (value == null) {
                // Handle null timestamps
                value = null;
              }
            }
          } else if (value != null && value.getClass().getName().equals("org.apache.avro.util.Utf8")) {
            // Convert Avro UTF8 to String
            value = value.toString();
          }

          currentRow[i] = value;
        }

        return true;
      } catch (IOException e) {
        throw new RuntimeException("Error reading Parquet file", e);
      }
    }

    @Override public void reset() {
      close();
      finished = false;
      initReader();
    }

    @Override public void close() {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          // Ignore
        }
        reader = null;
      }
    }
  }

  /**
   * Filtered enumerator that can skip rows with null values in specified columns.
   */
  private static class FilteredParquetEnumerator implements Enumerator<Object[]> {
    private final String filePath;
    private final boolean[] nullFilters;
    private ParquetReader<GenericRecord> reader;
    private GenericRecord current;
    private Object[] currentRow;
    private boolean finished = false;
    private Map<String, Boolean> timestampAdjustedMap = new HashMap<>();

    FilteredParquetEnumerator(String filePath, boolean[] nullFilters) {
      this.filePath = filePath;
      this.nullFilters = nullFilters;
      initReader();
    }

    private void initReader() {
      try {
        Path hadoopPath = new Path(filePath);
        Configuration conf = new Configuration();
        
        // Enable vectorized reading for better performance
        conf.set("parquet.enable.vectorized.reader", "true");

        // Read Parquet schema to get timestamp metadata
        try (ParquetFileReader fileReader = ParquetFileReader.open(conf, hadoopPath)) {
          ParquetMetadata metadata = fileReader.getFooter();
          MessageType schema = metadata.getFileMetaData().getSchema();
          
          // Build map of field names to isAdjustedToUTC flags
          for (Type field : schema.getFields()) {
            if (field.isPrimitive()) {
              LogicalTypeAnnotation logicalType = field.getLogicalTypeAnnotation();
              if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
                LogicalTypeAnnotation.TimestampLogicalTypeAnnotation tsType = 
                    (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalType;
                timestampAdjustedMap.put(field.getName(), tsType.isAdjustedToUTC());
                LOGGER.debug("[FilteredParquetEnumerator] Field {} has isAdjustedToUTC={}", 
                    field.getName(), tsType.isAdjustedToUTC());
              }
            }
          }
        }

        @SuppressWarnings("deprecation")
        ParquetReader<GenericRecord> tempReader =
            AvroParquetReader.<GenericRecord>builder(hadoopPath)
            .withConf(conf)
            .build();
        reader = tempReader;

      } catch (IOException e) {
        throw new RuntimeException("Failed to initialize Parquet reader for " + filePath, e);
      }
    }

    @Override public Object[] current() {
      return currentRow;
    }

    @Override public boolean moveNext() {
      if (finished) {
        return false;
      }

      try {
        while (true) {
          current = reader.read();
          if (current == null) {
            finished = true;
            return false;
          }

          // Convert GenericRecord to Object[] (same logic as ParquetEnumerator)
          int fieldCount = current.getSchema().getFields().size();
          currentRow = new Object[fieldCount];
          for (int i = 0; i < fieldCount; i++) {
            Object value = current.get(i);

            // Get the field schema to check for logical types
            Schema.Field field = current.getSchema().getFields().get(i);
            Schema fieldSchema = field.schema();

            // Handle union types (nullable fields)
            if (fieldSchema.getType() == Schema.Type.UNION) {
              for (Schema unionType : fieldSchema.getTypes()) {
                if (unionType.getType() != Schema.Type.NULL) {
                  fieldSchema = unionType;
                  break;
                }
              }
            }

            // Convert based on logical type
            LogicalType logicalType = fieldSchema.getLogicalType();
            if (logicalType != null) {
              String logicalTypeName = logicalType.getName();
              if ("date".equals(logicalTypeName)) {
                if (value instanceof Integer) {
                  // Keep as Integer to match LINQ4J engine behavior
                } else if (value == null) {
                  value = null;
                } else {
                  LOGGER.debug("[FilteredParquetEnumerator] Unexpected DATE value type: {}", 
                      (value != null ? value.getClass().getName() : "null"));
                }
              } else if ("time-millis".equals(logicalTypeName)) {
                if (value instanceof Integer) {
                  // Keep as integer - milliseconds since midnight
                  // This is the correct representation for TIME values
                } else if (value == null) {
                  value = null;
                }
              } else if ("timestamp-millis".equals(logicalTypeName)) {
                if (value instanceof Long) {
                  long milliseconds = (Long) value;
                  String fieldName = field.name();
                  
                  // For Parquet engine, return long values for timestamps to avoid casting issues
                  // in ORDER BY and WHERE clauses. The timestamp display formatting is handled
                  // by the result set processing.
                  value = milliseconds;
                } else if (value == null) {
                  value = null;
                }
              }
            } else if (value != null && value.getClass().getName().equals("org.apache.avro.util.Utf8")) {
              // Convert Avro UTF8 to String
              String stringValue = value.toString();
              // Just use the string value as-is
              value = stringValue;
            }

            currentRow[i] = value;
          }

          // Apply null filters - skip rows that have null values in filtered columns
          boolean shouldSkip = false;
          for (int i = 0; i < nullFilters.length && i < currentRow.length; i++) {
            if (nullFilters[i] && currentRow[i] == null) {
              shouldSkip = true;
              break;
            }
          }

          if (!shouldSkip) {
            return true; // Found a valid row
          }

          // Continue to next row if this one should be skipped
        }
      } catch (IOException e) {
        throw new RuntimeException("Error reading Parquet file", e);
      }
    }

    @Override public void reset() {
      close();
      finished = false;
      initReader();
    }

    @Override public void close() {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          // Ignore
        }
        reader = null;
      }
    }
  }

  /**
   * Time-filtered enumerator that skips rows with null TIME values.
   */
  private static class TimeFilteredParquetEnumerator implements Enumerator<Object[]> {
    private final String filePath;
    private ParquetReader<GenericRecord> reader;
    private GenericRecord current;
    private Object[] currentRow;
    private boolean finished = false;
    private Map<String, Boolean> timestampAdjustedMap = new HashMap<>();

    TimeFilteredParquetEnumerator(String filePath) {
      this.filePath = filePath;
      initReader();
    }

    private void initReader() {
      try {
        Path hadoopPath = new Path(filePath);
        Configuration conf = new Configuration();
        
        // Enable vectorized reading for better performance
        conf.set("parquet.enable.vectorized.reader", "true");

        // Read Parquet schema to get timestamp metadata
        try (ParquetFileReader fileReader = ParquetFileReader.open(conf, hadoopPath)) {
          ParquetMetadata metadata = fileReader.getFooter();
          MessageType schema = metadata.getFileMetaData().getSchema();
          
          // Build map of field names to isAdjustedToUTC flags
          for (Type field : schema.getFields()) {
            if (field.isPrimitive()) {
              LogicalTypeAnnotation logicalType = field.getLogicalTypeAnnotation();
              if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
                LogicalTypeAnnotation.TimestampLogicalTypeAnnotation tsType = 
                    (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalType;
                timestampAdjustedMap.put(field.getName(), tsType.isAdjustedToUTC());
                LOGGER.debug("[TimeFilteredParquetEnumerator] Field {} has isAdjustedToUTC={}", 
                    field.getName(), tsType.isAdjustedToUTC());
              }
            }
          }
        }

        @SuppressWarnings("deprecation")
        ParquetReader<GenericRecord> tempReader =
            AvroParquetReader.<GenericRecord>builder(hadoopPath)
            .withConf(conf)
            .build();
        reader = tempReader;

      } catch (IOException e) {
        throw new RuntimeException("Failed to initialize Parquet reader for " + filePath, e);
      }
    }

    @Override public Object[] current() {
      return currentRow;
    }

    @Override public boolean moveNext() {
      if (finished) {
        return false;
      }

      try {
        while (true) {
          current = reader.read();
          if (current == null) {
            finished = true;
            return false;
          }

          // Convert GenericRecord to Object[]
          int fieldCount = current.getSchema().getFields().size();
          currentRow = new Object[fieldCount];
          boolean shouldSkipRow = false;

          for (int i = 0; i < fieldCount; i++) {
            Object value = current.get(i);

            // Get the field schema to check for logical types
            Schema.Field field = current.getSchema().getFields().get(i);
            Schema fieldSchema = field.schema();

            // Handle union types (nullable fields)
            if (fieldSchema.getType() == Schema.Type.UNION) {
              for (Schema unionType : fieldSchema.getTypes()) {
                if (unionType.getType() != Schema.Type.NULL) {
                  fieldSchema = unionType;
                  break;
                }
              }
            }

            // Convert based on logical type
            LogicalType logicalType = fieldSchema.getLogicalType();
            if (logicalType != null) {
              String logicalTypeName = logicalType.getName();
              if ("date".equals(logicalTypeName)) {
                // Keep as Integer to match LINQ4J engine behavior
              } else if ("time-millis".equals(logicalTypeName)) {
                // Skip rows with null TIME values - this is the key filter
                if (value == null) {
                  shouldSkipRow = true;
                  break;
                } else if (value instanceof Integer) {
                  // Keep as integer - milliseconds since midnight
                  // This is the correct representation for TIME values
                }
              } else if ("timestamp-millis".equals(logicalTypeName)) {
                if (value instanceof Long) {
                  long milliseconds = (Long) value;
                  String fieldName = field.name();
                  
                  // For Parquet engine, return long values for timestamps to avoid casting issues
                  // in ORDER BY and WHERE clauses. The timestamp display formatting is handled
                  // by the result set processing.
                  value = milliseconds;
                }
              }
            } else if (value != null && value.getClass().getName().equals("org.apache.avro.util.Utf8")) {
              // Convert Avro UTF8 to String
              String stringValue = value.toString();
              // Just use the string value as-is
              value = stringValue;
            }

            currentRow[i] = value;
          }

          // Skip this row if it has null TIME values
          if (shouldSkipRow) {
            continue; // Read next row
          }

          return true; // Return this row
        }
      } catch (IOException e) {
        throw new RuntimeException("Error reading Parquet file", e);
      }
    }

    @Override public void reset() {
      close();
      finished = false;
      initReader();
    }

    @Override public void close() {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          // Ignore
        }
        reader = null;
      }
    }
  }
  
  /**
   * Vectorized enumerator that reads from Parquet files in batches.
   */
  private static class VectorizedParquetEnumerator implements Enumerator<Object[]> {
    private final String filePath;
    private VectorizedParquetReader reader;
    private List<Object[]> currentBatch;
    private int batchPosition = 0;
    private Object[] currentRow;
    private boolean finished = false;
    
    VectorizedParquetEnumerator(String filePath) {
      this.filePath = filePath;
      initReader();
    }
    
    private void initReader() {
      try {
        reader = new VectorizedParquetReader(filePath);
        loadNextBatch();
      } catch (IOException e) {
        throw new RuntimeException("Failed to initialize vectorized Parquet reader for " + filePath, e);
      }
    }
    
    private void loadNextBatch() throws IOException {
      currentBatch = reader.readBatch();
      batchPosition = 0;
      
      if (currentBatch == null || currentBatch.isEmpty()) {
        finished = true;
        currentBatch = null;
      } else {
        LOGGER.trace("Loaded batch with {} rows", currentBatch.size());
      }
    }
    
    @Override 
    public Object[] current() {
      return currentRow;
    }
    
    @Override 
    public boolean moveNext() {
      if (finished) {
        return false;
      }
      
      try {
        // Check if we need to load the next batch
        if (currentBatch == null || batchPosition >= currentBatch.size()) {
          loadNextBatch();
          if (finished) {
            return false;
          }
        }
        
        // Get the next row from the current batch
        currentRow = currentBatch.get(batchPosition);
        batchPosition++;
        return true;
        
      } catch (IOException e) {
        throw new RuntimeException("Error reading vectorized Parquet data", e);
      }
    }
    
    @Override 
    public void reset() {
      close();
      finished = false;
      initReader();
    }
    
    @Override 
    public void close() {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          // Ignore
        }
        reader = null;
      }
      currentBatch = null;
    }
  }
  
  /**
   * Vectorized filtered enumerator that reads from Parquet files in batches with null filtering.
   */
  private static class VectorizedFilteredParquetEnumerator implements Enumerator<Object[]> {
    private final String filePath;
    private final boolean[] nullFilters;
    private VectorizedParquetReader reader;
    private List<Object[]> currentBatch;
    private int batchPosition = 0;
    private Object[] currentRow;
    private boolean finished = false;
    
    VectorizedFilteredParquetEnumerator(String filePath, boolean[] nullFilters) {
      this.filePath = filePath;
      this.nullFilters = nullFilters;
      initReader();
    }
    
    private void initReader() {
      try {
        reader = new VectorizedParquetReader(filePath);
        loadNextBatch();
      } catch (IOException e) {
        throw new RuntimeException("Failed to initialize vectorized Parquet reader for " + filePath, e);
      }
    }
    
    private void loadNextBatch() throws IOException {
      currentBatch = reader.readBatch();
      batchPosition = 0;
      
      if (currentBatch == null || currentBatch.isEmpty()) {
        finished = true;
        currentBatch = null;
      } else {
        LOGGER.trace("Loaded batch with {} rows for filtering", currentBatch.size());
      }
    }
    
    @Override 
    public Object[] current() {
      return currentRow;
    }
    
    @Override 
    public boolean moveNext() {
      if (finished) {
        return false;
      }
      
      try {
        while (true) {
          // Check if we need to load the next batch
          if (currentBatch == null || batchPosition >= currentBatch.size()) {
            loadNextBatch();
            if (finished) {
              return false;
            }
          }
          
          // Get the next row from the current batch
          Object[] row = currentBatch.get(batchPosition);
          batchPosition++;
          
          // Apply null filters - skip rows that have null values in filtered columns
          boolean shouldSkip = false;
          for (int i = 0; i < nullFilters.length && i < row.length; i++) {
            if (nullFilters[i] && row[i] == null) {
              shouldSkip = true;
              break;
            }
          }
          
          if (!shouldSkip) {
            currentRow = row;
            return true; // Found a valid row
          }
          // Continue to next row if this one should be skipped
        }
        
      } catch (IOException e) {
        throw new RuntimeException("Error reading vectorized Parquet data", e);
      }
    }
    
    @Override 
    public void reset() {
      close();
      finished = false;
      initReader();
    }
    
    @Override 
    public void close() {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          // Ignore
        }
        reader = null;
      }
      currentBatch = null;
    }
  }
}
