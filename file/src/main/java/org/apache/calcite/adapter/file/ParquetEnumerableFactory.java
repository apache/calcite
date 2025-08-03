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

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;

/**
 * Factory for creating Enumerable instances from Parquet files.
 */
public class ParquetEnumerableFactory {

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
        return new ParquetEnumerator(filePath);
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
        return new FilteredParquetEnumerator(filePath, nullFilters);
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

    ParquetEnumerator(String filePath) {
      this.filePath = filePath;
      initReader();
    }

    private void initReader() {
      try {
        Path hadoopPath = new Path(filePath);
        Configuration conf = new Configuration();

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
                System.out.println("[ParquetEnumerableFactory] Unexpected DATE value type: " 
                    + (value != null ? value.getClass().getName() : "null"));
              }
            } else if ("time-millis".equals(logicalTypeName)) {
              if (value instanceof Integer) {
                // Convert milliseconds since midnight to Time
                // Keep as integer (milliseconds since midnight) for Calcite compatibility
                // The integer value will be properly handled by Calcite's runtime
                // No conversion needed - Parquet time-millis is already in the correct format
              } else if (value == null) {
                // Handle null times
                value = null;
              }
            } else if ("timestamp-millis".equals(logicalTypeName)) {
              if (value instanceof Long) {
                // Use LocalTimestamp wrapper to ensure SQL:2003 compliant string representation
                // This ensures TIMESTAMP WITHOUT TIME ZONE returns local time strings
                long milliseconds = (Long) value;
                value = new LocalTimestamp(milliseconds);
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

    FilteredParquetEnumerator(String filePath, boolean[] nullFilters) {
      this.filePath = filePath;
      this.nullFilters = nullFilters;
      initReader();
    }

    private void initReader() {
      try {
        Path hadoopPath = new Path(filePath);
        Configuration conf = new Configuration();

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
                  System.out.println("[FilteredParquetEnumerator] Unexpected DATE value type: " 
                      + (value != null ? value.getClass().getName() : "null"));
                }
              } else if ("time-millis".equals(logicalTypeName)) {
                if (value instanceof Integer) {
                  // Keep as integer (milliseconds since midnight) for Calcite compatibility
                } else if (value == null) {
                  value = null;
                }
              } else if ("timestamp-millis".equals(logicalTypeName)) {
                if (value instanceof Long) {
                  long milliseconds = (Long) value;
                  value = new LocalTimestamp(milliseconds);
                } else if (value == null) {
                  value = null;
                }
              }
            } else if (value != null && value.getClass().getName().equals("org.apache.avro.util.Utf8")) {
              // Convert Avro UTF8 to String
              value = value.toString();
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

    TimeFilteredParquetEnumerator(String filePath) {
      this.filePath = filePath;
      initReader();
    }

    private void initReader() {
      try {
        Path hadoopPath = new Path(filePath);
        Configuration conf = new Configuration();

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
                }
              } else if ("timestamp-millis".equals(logicalTypeName)) {
                if (value instanceof Long) {
                  long milliseconds = (Long) value;
                  value = new LocalTimestamp(milliseconds);
                }
              }
            } else if (value != null && value.getClass().getName().equals("org.apache.avro.util.Utf8")) {
              // Convert Avro UTF8 to String
              value = value.toString();
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
}
