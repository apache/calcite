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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Batch Parquet reader that reads data in batches for improved I/O efficiency.
 * This reader processes entire row groups and returns batches of rows.
 * Note: This is not true vectorized/columnar processing, but row-wise batching.
 */
public class VectorizedParquetReader implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(VectorizedParquetReader.class);

  // Use computed optimal batch size as fallback
  private static final int DEFAULT_BATCH_SIZE = ParquetConfig.computeOptimalBatchSize();

  private final ParquetFileReader reader;
  private final MessageType schema;
  private final ParquetMetadata metadata;
  private final int batchSize;
  private final List<ColumnDescriptor> columns;
  private final Map<String, Boolean> timestampAdjustedMap;

  private PageReadStore currentRowGroup;
  private int currentRowGroupIndex = 0;
  private int currentRowInGroup = 0;
  private long totalRowsRead = 0;
  private final long totalRowCount;

  // Batch data storage - pre-allocate for efficiency
  private final List<Object[]> batchData;
  private int batchPosition = 0;
  private boolean hasMoreData = true;

  public VectorizedParquetReader(String filePath) throws IOException {
    this(filePath, DEFAULT_BATCH_SIZE);
  }

  @SuppressWarnings("deprecation")
  public VectorizedParquetReader(String filePath, int batchSize) throws IOException {
    // Use configured batch size or system property
    ParquetConfig config = ParquetConfig.fromSystemProperties();
    int effectiveBatchSize = (batchSize > 0) ? batchSize : config.getBatchSize();
    Path hadoopPath = new Path(filePath);
    Configuration conf = new Configuration();
    conf.set("parquet.enable.vectorized.reader", "true");

    this.reader = ParquetFileReader.open(conf, hadoopPath);
    this.metadata = reader.getFooter();
    this.schema = metadata.getFileMetaData().getSchema();
    this.batchSize = effectiveBatchSize;
    this.columns = schema.getColumns();
    this.timestampAdjustedMap = new HashMap<>();
    this.batchData = new ArrayList<>(effectiveBatchSize); // Pre-allocated capacity

    // Calculate total row count
    long count = 0;
    for (int i = 0; i < metadata.getBlocks().size(); i++) {
      count += metadata.getBlocks().get(i).getRowCount();
    }
    this.totalRowCount = count;

    // Build timestamp metadata map
    buildTimestampMetadata();

    LOGGER.debug("Initialized VectorizedParquetReader for {} with batch size {}, total rows: {}",
                filePath, effectiveBatchSize, totalRowCount);
  }

  private void buildTimestampMetadata() {
    for (Type field : schema.getFields()) {
      if (field.isPrimitive()) {
        LogicalTypeAnnotation logicalType = field.getLogicalTypeAnnotation();
        if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
          LogicalTypeAnnotation.TimestampLogicalTypeAnnotation tsType =
              (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalType;
          timestampAdjustedMap.put(field.getName(), tsType.isAdjustedToUTC());
          LOGGER.debug("Field {} has isAdjustedToUTC={}",
              field.getName(), tsType.isAdjustedToUTC());
        }
      }
    }
  }

  /**
   * Read the next batch of rows.
   * @return List of Object[] representing rows, or null if no more data
   */
  public List<Object[]> readBatch() throws IOException {
    if (!hasMoreData) {
      return null;
    }

    batchData.clear();
    int rowsInBatch = 0;

    while (rowsInBatch < batchSize && hasMoreData) {
      // Load next row group if needed
      if (currentRowGroup == null || currentRowInGroup >= currentRowGroup.getRowCount()) {
        if (!loadNextRowGroup()) {
          hasMoreData = false;
          break;
        }
      }

      // Read rows from current row group
      int rowsToRead =
          Math.min(batchSize - rowsInBatch,
          (int)(currentRowGroup.getRowCount() - currentRowInGroup));

      // Use columnar reading for better performance
      readRowsColumnar(rowsToRead);

      rowsInBatch += rowsToRead;
      currentRowInGroup += rowsToRead;
      totalRowsRead += rowsToRead;
    }

    if (batchData.isEmpty()) {
      return null;
    }

    LOGGER.trace("Read batch of {} rows, total read: {}/{}",
                batchData.size(), totalRowsRead, totalRowCount);

    return new ArrayList<>(batchData);
  }

  private boolean loadNextRowGroup() throws IOException {
    if (currentRowGroupIndex >= metadata.getBlocks().size()) {
      return false;
    }

    currentRowGroup = reader.readNextRowGroup();
    if (currentRowGroup == null) {
      return false;
    }

    currentRowGroupIndex++;
    currentRowInGroup = 0;

    LOGGER.trace("Loaded row group {} with {} rows",
                currentRowGroupIndex - 1, currentRowGroup.getRowCount());

    return true;
  }

  private void readRowsColumnar(int numRows) throws IOException {
    // Create column readers for each column
    MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
    VectorizedGroupMaterializer materializer = new VectorizedGroupMaterializer();
    RecordReader<VectorizedGroup> recordReader =
        columnIO.getRecordReader(currentRowGroup, materializer);

    // Read rows in a more efficient batch - avoid individual read() calls
    // Pre-allocate array to avoid repeated ArrayList growth
    Object[][] rowBatch = new Object[numRows][];
    int actualRowsRead = 0;

    // Read all rows at once if possible
    for (int i = 0; i < numRows; i++) {
      VectorizedGroup group = recordReader.read();
      if (group == null) {
        break;
      }
      rowBatch[actualRowsRead] = group.toArray();
      actualRowsRead++;
    }

    // Add only the rows we actually read
    for (int i = 0; i < actualRowsRead; i++) {
      batchData.add(rowBatch[i]);
    }
  }

  /**
   * Check if there are more rows to read.
   */
  public boolean hasNext() {
    return hasMoreData || !batchData.isEmpty();
  }

  /**
   * Get the total number of rows in the file.
   */
  public long getTotalRowCount() {
    return totalRowCount;
  }

  /**
   * Get the number of rows read so far.
   */
  public long getRowsRead() {
    return totalRowsRead;
  }

  @Override public void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
  }

  /**
   * Internal class to materialize groups in vectorized manner.
   */
  private class VectorizedGroupMaterializer extends RecordMaterializer<VectorizedGroup> {
    private final GroupConverter converter;
    private VectorizedGroup currentGroup;

    VectorizedGroupMaterializer() {
      this.converter = new GroupConverter() {
        @Override public void start() {
          currentGroup = new VectorizedGroup(schema.getFieldCount());
        }

        @Override public void end() {
          // Group is complete
        }

        @Override public Converter getConverter(int fieldIndex) {
          Type field = schema.getFields().get(fieldIndex);
          return new VectorizedFieldConverter(fieldIndex, field);
        }
      };
    }

    @Override public VectorizedGroup getCurrentRecord() {
      return currentGroup;
    }

    @Override public GroupConverter getRootConverter() {
      return converter;
    }

    /**
     * Converter for individual fields.
     */
    class VectorizedFieldConverter extends PrimitiveConverter {
      private final int fieldIndex;
      private final Type fieldType;
      private final LogicalTypeAnnotation logicalType;

      VectorizedFieldConverter(int fieldIndex, Type fieldType) {
        this.fieldIndex = fieldIndex;
        this.fieldType = fieldType;
        this.logicalType = fieldType.getLogicalTypeAnnotation();
      }

      @Override public void addBinary(Binary value) {
        if (logicalType != null && "decimal".equals(logicalType.toString())) {
          // Handle decimal as bytes
          currentGroup.setValue(fieldIndex, value.getBytes());
        } else {
          // Handle as string
          String stringValue = value.toStringUsingUTF8();

          // Check for EMPTY sentinel and convert back to empty string
          if ("\uFFFF".equals(stringValue)) {
            // Debug logging for name and status fields
            if (fieldIndex == 1 || fieldIndex == 2) {
              LOGGER.info("[VectorizedParquetReader] Field {} (index {}): EMPTY sentinel detected, converting to empty string",
                          fieldType.getName(), fieldIndex);
            }
            currentGroup.setValue(fieldIndex, "");
          } else {
            // Debug logging for name and status fields
            if (fieldIndex == 1 || fieldIndex == 2) {
              LOGGER.info("[VectorizedParquetReader] Field {} (index {}): Binary value='{}', length={}, isEmpty={}",
                          fieldType.getName(), fieldIndex, stringValue, stringValue.length(), stringValue.isEmpty());
            }
            currentGroup.setValue(fieldIndex, stringValue);
          }
        }
      }

      @Override public void addBoolean(boolean value) {
        currentGroup.setValue(fieldIndex, value);
      }

      @Override public void addDouble(double value) {
        currentGroup.setValue(fieldIndex, value);
      }

      @Override public void addFloat(float value) {
        currentGroup.setValue(fieldIndex, (double) value);
      }

      @Override public void addInt(int value) {
        if (logicalType instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
          // Keep as integer (days since epoch)
          currentGroup.setValue(fieldIndex, value);
        } else if (logicalType instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
          // Keep as integer (milliseconds since midnight)
          currentGroup.setValue(fieldIndex, value);
        } else {
          currentGroup.setValue(fieldIndex, (long) value);
        }
      }

      @Override public void addLong(long value) {
        if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
          // Check if timestamp is adjusted to UTC
          String fieldName = fieldType.getName();
          Boolean isAdjustedToUTC = timestampAdjustedMap.get(fieldName);

          LOGGER.debug("Reading TIMESTAMP from Parquet: {} for field {} (isAdjustedToUTC={})",
                      value, fieldName, isAdjustedToUTC);

          // For Parquet engine, return long values for timestamps to avoid casting issues
          // in ORDER BY and WHERE clauses. The timestamp display formatting is handled
          // by the result set processing.
          currentGroup.setValue(fieldIndex, value);
        } else {
          currentGroup.setValue(fieldIndex, value);
        }
      }
    }
  }

  /**
   * Internal representation of a vectorized group (row).
   */
  private static class VectorizedGroup {
    private final Object[] values;

    VectorizedGroup(int fieldCount) {
      this.values = new Object[fieldCount];
    }

    void setValue(int index, Object value) {
      values[index] = value;
    }

    Object[] toArray() {
      return values.clone();
    }
  }
}
