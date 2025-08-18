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
package org.apache.calcite.adapter.file.table;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.file.format.parquet.ParquetConversionUtil;
import org.apache.calcite.adapter.file.execution.parquet.VectorizedParquetReader;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Table that reads data from Parquet files using the ScannableTable interface.
 * This avoids the complexity of the Arrow adapter and works directly with
 * the file adapter's execution model.
 */
@SuppressWarnings("deprecation")
public class ParquetScannableTable extends AbstractTable implements ScannableTable, FilterableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetScannableTable.class);

  private final File parquetFile;
  private RelDataType rowType;

  public ParquetScannableTable(File parquetFile) {
    this.parquetFile = parquetFile;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (rowType == null) {
      rowType = deriveRowType(typeFactory);
    }
    return rowType;
  }

  private RelDataType deriveRowType(RelDataTypeFactory typeFactory) {
    try {
      Path hadoopPath = new Path(parquetFile.getAbsolutePath());
      Configuration conf = new Configuration();
      
      // Enable vectorized reading for better performance
      conf.set("parquet.enable.vectorized.reader", "true");

      // Read Parquet schema
      @SuppressWarnings("deprecation")
      ParquetMetadata metadata = ParquetFileReader.readFooter(conf, hadoopPath);
      MessageType messageType = metadata.getFileMetaData().getSchema();

      // Convert Parquet schema to Calcite schema
      List<String> names = new ArrayList<>();
      List<RelDataType> types = new ArrayList<>();
      List<Type> parquetFields = messageType.getFields();

      for (Type field : parquetFields) {
        SqlTypeName sqlType = convertParquetTypeToSql(field);
        // Use the field name directly - no sanitization in direct conversion
        String fieldName = field.getName();

        names.add(fieldName);
        // All Parquet fields should be nullable
        types.add(typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(sqlType), true));
      }

      return typeFactory.createStructType(Pair.zip(names, types));

    } catch (IOException e) {
      throw new RuntimeException("Failed to read Parquet schema", e);
    }
  }

  private SqlTypeName convertParquetTypeToSql(Type parquetType) {
    // Check for logical types first
    LogicalTypeAnnotation logicalType = parquetType.getLogicalTypeAnnotation();

    // Debug output
    String fieldName = parquetType.getName();
    if (fieldName.equals("JOINEDAT") || fieldName.equals("JOINTIME") || fieldName.equals("JOINTIMES")) {
      LOGGER.debug("DEBUG: Field {} - LogicalType: {}, PrimitiveType: {}", fieldName, logicalType, parquetType.asPrimitiveType());
    }

    if (logicalType != null) {
      if (logicalType instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
        return SqlTypeName.DATE;
      } else if (logicalType instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
        return SqlTypeName.TIME;
      } else if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
        return SqlTypeName.TIMESTAMP;
      } else if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
        return SqlTypeName.DECIMAL;
      }
    }

    // Fall back to primitive type mapping
    switch (parquetType.asPrimitiveType().getPrimitiveTypeName()) {
    case INT32:
      return SqlTypeName.INTEGER;
    case INT64:
      return SqlTypeName.BIGINT;
    case FLOAT:
      return SqlTypeName.FLOAT;
    case DOUBLE:
      return SqlTypeName.DOUBLE;
    case BOOLEAN:
      return SqlTypeName.BOOLEAN;
    case BINARY:
    default:
      return SqlTypeName.VARCHAR;
    }
  }

  @Override public Enumerable<Object[]> scan(DataContext root) {
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        // Check if vectorized reading is enabled
        Configuration conf = new Configuration();
        String systemProperty = System.getProperty("parquet.enable.vectorized.reader", "false");
        conf.set("parquet.enable.vectorized.reader", systemProperty);
        boolean useVectorized = "true".equals(systemProperty);
        
        if (useVectorized) {
          LOGGER.debug("Using vectorized reader for scan");
          return new VectorizedParquetEnumerator(cancelFlag);
        } else {
          LOGGER.debug("Using record-by-record reader for scan");
          return new ParquetReaderEnumerator(cancelFlag);
        }
      }
    };
  }

  @Override public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);

    // Extract filter conditions
    final boolean[] nullFilters = new boolean[getRowType(root.getTypeFactory()).getFieldCount()];

    // Process filters and identify which columns have IS NOT NULL conditions
    filters.removeIf(filter -> processFilter(filter, nullFilters));

    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        // Check if vectorized reading is enabled
        Configuration conf = new Configuration();
        String systemProperty = System.getProperty("parquet.enable.vectorized.reader", "false");
        conf.set("parquet.enable.vectorized.reader", systemProperty);
        boolean useVectorized = "true".equals(systemProperty);
        
        if (useVectorized) {
          LOGGER.debug("Using vectorized filtered reader for scan");
          return new VectorizedFilteredParquetEnumerator(cancelFlag, nullFilters);
        } else {
          LOGGER.debug("Using record-by-record filtered reader for scan");
          return new FilteredParquetReaderEnumerator(cancelFlag, nullFilters);
        }
      }
    };
  }

  /**
   * Process a filter condition and extract IS NOT NULL predicates.
   * @param filter The filter condition to process
   * @param nullFilters Array to track which columns have IS NOT NULL filters
   * @return true if the filter was successfully processed and can be removed from the list
   */
  private boolean processFilter(RexNode filter, boolean[] nullFilters) {
    if (filter.isA(SqlKind.AND)) {
      // Process all operands of AND
      RexCall call = (RexCall) filter;
      boolean allProcessed = true;
      for (RexNode operand : call.getOperands()) {
        if (!processFilter(operand, nullFilters)) {
          allProcessed = false;
        }
      }
      return allProcessed;
    } else if (filter.isA(SqlKind.IS_NOT_NULL)) {
      // Handle IS NOT NULL condition
      RexCall call = (RexCall) filter;
      RexNode operand = call.getOperands().get(0);
      if (operand instanceof RexInputRef) {
        int index = ((RexInputRef) operand).getIndex();
        nullFilters[index] = true;
        return true; // This filter can be removed as we handle it here
      }
    }
    return false; // Filter not processed, keep it for further processing
  }

  /**
   * Enumerator that reads from Parquet files.
   */
  private class ParquetReaderEnumerator implements Enumerator<Object[]> {
    private final AtomicBoolean cancelFlag;
    private ParquetReader<GenericRecord> reader;
    private GenericRecord current;
    private Object[] currentRow;
    private boolean finished = false;

    ParquetReaderEnumerator(AtomicBoolean cancelFlag) {
      this.cancelFlag = cancelFlag;
      initReader();
    }

    private void initReader() {
      try {
        Path hadoopPath = new Path(parquetFile.getAbsolutePath());
        Configuration conf = new Configuration();
        
        // Enable vectorized reading for better performance
        conf.set("parquet.enable.vectorized.reader", "true");

        @SuppressWarnings("deprecation")
        ParquetReader<GenericRecord> tempReader =
            AvroParquetReader.<GenericRecord>builder(hadoopPath)
            .withConf(conf)
            .build();
        reader = tempReader;

      } catch (IOException e) {
        throw new RuntimeException("Failed to initialize Parquet reader", e);
      }
    }

    @Override public Object[] current() {
      return currentRow;
    }

    @Override public boolean moveNext() {
      if (finished || cancelFlag.get()) {
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
          currentRow[i] = current.get(i);
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
  private class FilteredParquetReaderEnumerator implements Enumerator<Object[]> {
    private final AtomicBoolean cancelFlag;
    private final boolean[] nullFilters;
    private ParquetReader<GenericRecord> reader;
    private GenericRecord current;
    private Object[] currentRow;
    private boolean finished = false;

    FilteredParquetReaderEnumerator(AtomicBoolean cancelFlag, boolean[] nullFilters) {
      this.cancelFlag = cancelFlag;
      this.nullFilters = nullFilters;
      initReader();
    }

    private void initReader() {
      try {
        Path hadoopPath = new Path(parquetFile.getAbsolutePath());
        Configuration conf = new Configuration();
        
        // Enable vectorized reading for better performance
        conf.set("parquet.enable.vectorized.reader", "true");

        @SuppressWarnings("deprecation")
        ParquetReader<GenericRecord> tempReader =
            AvroParquetReader.<GenericRecord>builder(hadoopPath)
            .withConf(conf)
            .build();
        reader = tempReader;

      } catch (IOException e) {
        throw new RuntimeException("Failed to initialize Parquet reader", e);
      }
    }

    @Override public Object[] current() {
      return currentRow;
    }

    @Override public boolean moveNext() {
      if (finished || cancelFlag.get()) {
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
          for (int i = 0; i < fieldCount; i++) {
            currentRow[i] = current.get(i);
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
   * Vectorized enumerator that reads from Parquet files in batches.
   */
  private class VectorizedParquetEnumerator implements Enumerator<Object[]> {
    private final AtomicBoolean cancelFlag;
    private VectorizedParquetReader reader;
    private List<Object[]> currentBatch;
    private Iterator<Object[]> batchIterator;
    private Object[] currentRow;
    private boolean finished = false;
    
    VectorizedParquetEnumerator(AtomicBoolean cancelFlag) {
      this.cancelFlag = cancelFlag;
      initReader();
    }
    
    private void initReader() {
      try {
        String filePath = parquetFile.getAbsolutePath();
        reader = new VectorizedParquetReader(filePath);
        loadNextBatch();
      } catch (IOException e) {
        throw new RuntimeException("Failed to initialize vectorized Parquet reader", e);
      }
    }
    
    private void loadNextBatch() throws IOException {
      if (cancelFlag.get()) {
        finished = true;
        return;
      }
      
      currentBatch = reader.readBatch();
      
      if (currentBatch == null || currentBatch.isEmpty()) {
        finished = true;
        batchIterator = null;
      } else {
        batchIterator = currentBatch.iterator();
        LOGGER.trace("Loaded batch with {} rows", currentBatch.size());
      }
    }
    
    @Override 
    public Object[] current() {
      return currentRow;
    }
    
    @Override 
    public boolean moveNext() {
      if (finished || cancelFlag.get()) {
        return false;
      }
      
      try {
        // Check if we need to load the next batch
        if (batchIterator == null || !batchIterator.hasNext()) {
          loadNextBatch();
          if (finished) {
            return false;
          }
        }
        
        // Get the next row from the current batch
        currentRow = batchIterator.next();
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
      batchIterator = null;
    }
  }
  
  /**
   * Vectorized filtered enumerator that reads from Parquet files in batches with null filtering.
   */
  private class VectorizedFilteredParquetEnumerator implements Enumerator<Object[]> {
    private final AtomicBoolean cancelFlag;
    private final boolean[] nullFilters;
    private VectorizedParquetReader reader;
    private List<Object[]> currentBatch;
    private Iterator<Object[]> batchIterator;
    private Object[] currentRow;
    private boolean finished = false;
    
    VectorizedFilteredParquetEnumerator(AtomicBoolean cancelFlag, boolean[] nullFilters) {
      this.cancelFlag = cancelFlag;
      this.nullFilters = nullFilters;
      initReader();
    }
    
    private void initReader() {
      try {
        String filePath = parquetFile.getAbsolutePath();
        reader = new VectorizedParquetReader(filePath);
        loadNextBatch();
      } catch (IOException e) {
        throw new RuntimeException("Failed to initialize vectorized Parquet reader", e);
      }
    }
    
    private void loadNextBatch() throws IOException {
      if (cancelFlag.get()) {
        finished = true;
        return;
      }
      
      currentBatch = reader.readBatch();
      
      if (currentBatch == null || currentBatch.isEmpty()) {
        finished = true;
        batchIterator = null;
      } else {
        batchIterator = currentBatch.iterator();
        LOGGER.trace("Loaded batch with {} rows for filtering", currentBatch.size());
      }
    }
    
    @Override 
    public Object[] current() {
      return currentRow;
    }
    
    @Override 
    public boolean moveNext() {
      if (finished || cancelFlag.get()) {
        return false;
      }
      
      try {
        while (true) {
          // Check if we need to load the next batch
          if (batchIterator == null || !batchIterator.hasNext()) {
            loadNextBatch();
            if (finished) {
              return false;
            }
          }
          
          // Get the next row from the current batch
          Object[] row = batchIterator.next();
          
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
      batchIterator = null;
    }
  }
}
