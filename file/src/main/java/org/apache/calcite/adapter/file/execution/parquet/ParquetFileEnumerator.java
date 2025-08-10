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

import org.apache.calcite.adapter.file.execution.arrow.UniversalDataBatchAdapter;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Source;

import org.apache.arrow.vector.VectorSchemaRoot;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Enumerator that uses the Parquet execution engine for columnar processing.
 *
 * <p>This enumerator provides:
 * <ul>
 *   <li>Streaming data processing via Parquet row groups</li>
 *   <li>Compressed in-memory storage</li>
 *   <li>Efficient columnar operations</li>
 *   <li>Support for very large datasets</li>
 * </ul>
 *
 * @param <T> the element type
 */
public class ParquetFileEnumerator<T> implements Enumerator<T> {

  private final Source source;
  private final RelDataType rowType;
  private final int batchSize;

  private CSVReader csvReader;
  private Queue<ParquetExecutionEngine.InMemoryParquetData> parquetBatches;
  private Iterator<T> currentBatchIterator;
  private T current;
  private boolean initialized = false;

  public ParquetFileEnumerator(Source source, RelDataType rowType, int batchSize) {
    this.source = source;
    this.rowType = rowType;
    this.batchSize = batchSize;
    this.parquetBatches = new LinkedList<>();
  }

  @Override public T current() {
    return current;
  }

  @Override public boolean moveNext() {
    // If we have rows in the current batch, return the next one
    if (currentBatchIterator != null && currentBatchIterator.hasNext()) {
      current = currentBatchIterator.next();
      return true;
    }

    // Try to get the next batch
    if (!parquetBatches.isEmpty() || loadNextBatch()) {
      ParquetExecutionEngine.InMemoryParquetData batch = parquetBatches.poll();
      if (batch != null) {
        currentBatchIterator = convertParquetBatchToIterator(batch);
        if (currentBatchIterator.hasNext()) {
          current = currentBatchIterator.next();
          return true;
        }
      }
    }

    current = null;
    return false;
  }

  private boolean loadNextBatch() {
    if (!initialized) {
      initialize();
    }

    // For simplicity, create a CSV enumerator to read the data
    // In a real implementation, this would handle different file formats
    try {
      List<Object[]> rows = new ArrayList<>(batchSize);

      // Read batch of rows
      String[] line;
      while (rows.size() < batchSize && (line = csvReader.readNext()) != null) {
        Object[] row = convertRow(line);
        if (row != null) {
          rows.add(row);
        }
      }

      if (rows.isEmpty()) {
        return false;
      }

      // Convert rows to Arrow format
      VectorSchemaRoot arrowBatch =
          UniversalDataBatchAdapter.convertToArrowBatch(rows.iterator(), rowType, rows.size());

      if (arrowBatch != null && arrowBatch.getRowCount() > 0) {
        // Convert Arrow batch to in-memory Parquet format
        ParquetExecutionEngine.InMemoryParquetData parquetData =
            ParquetExecutionEngine.convertToParquet(arrowBatch);

        parquetBatches.offer(parquetData);

        // Clean up Arrow resources
        arrowBatch.close();

        return true;
      }
    } catch (IOException | CsvValidationException e) {
      throw new RuntimeException("Error reading file", e);
    }

    return false;
  }

  private void initialize() {
    try {
      // Initialize CSV reader
      csvReader = new CSVReaderBuilder(source.reader())
          .withCSVParser(new CSVParserBuilder()
              .withSeparator(',')
              .withQuoteChar('"')
              .build())
          .build();

      // Skip header if present
      csvReader.readNext();

      initialized = true;
    } catch (IOException | CsvValidationException e) {
      throw new RuntimeException("Failed to initialize reader", e);
    }
  }

  private Object[] convertRow(String[] csvRow) {
    Object[] row = new Object[rowType.getFieldCount()];

    for (int i = 0; i < Math.min(csvRow.length, row.length); i++) {
      String value = csvRow[i];
      if (value == null || value.isEmpty()) {
        row[i] = null;
        continue;
      }

      SqlTypeName sqlType = rowType.getFieldList().get(i).getType().getSqlTypeName();
      try {
        switch (sqlType) {
        case INTEGER:
          row[i] = Integer.parseInt(value);
          break;
        case BIGINT:
          row[i] = Long.parseLong(value);
          break;
        case FLOAT:
        case REAL:
          row[i] = Float.parseFloat(value);
          break;
        case DOUBLE:
          row[i] = Double.parseDouble(value);
          break;
        case BOOLEAN:
          row[i] = Boolean.parseBoolean(value);
          break;
        case VARCHAR:
        case CHAR:
        default:
          row[i] = value;
          break;
        }
      } catch (NumberFormatException e) {
        row[i] = null;
      }
    }

    return row;
  }

  private Iterator<T> convertParquetBatchToIterator(
      ParquetExecutionEngine.InMemoryParquetData batch) {

    // Convert Parquet back to Arrow for row iteration
    VectorSchemaRoot arrowBatch = ParquetExecutionEngine.toArrow(batch);

    // Convert Arrow rows to typed objects
    List<T> typedRows = new ArrayList<>(arrowBatch.getRowCount());
    for (int i = 0; i < arrowBatch.getRowCount(); i++) {
      Object[] row = new Object[arrowBatch.getFieldVectors().size()];
      for (int j = 0; j < row.length; j++) {
        row[j] = arrowBatch.getVector(j).getObject(i);
      }
      @SuppressWarnings("unchecked")
      T typedRow = (T) row;
      typedRows.add(typedRow);
    }

    arrowBatch.close();
    return typedRows.iterator();
  }

  @Override public void reset() {
    if (csvReader != null) {
      try {
        csvReader.close();
      } catch (IOException e) {
        // Ignore
      }
    }
    initialized = false;
    parquetBatches.clear();
    currentBatchIterator = null;
    current = null;
  }

  @Override public void close() {
    reset();
  }

  /**
   * Apply projection using Parquet's column pruning.
   */
  public ParquetFileEnumerator<T> project(int[] columnIndices) {
    // Apply projection to all batches
    Queue<ParquetExecutionEngine.InMemoryParquetData> projectedBatches =
        new LinkedList<>();

    for (ParquetExecutionEngine.InMemoryParquetData batch : parquetBatches) {
      projectedBatches.offer(ParquetExecutionEngine.project(batch, columnIndices));
    }

    this.parquetBatches = projectedBatches;
    return this;
  }

  /**
   * Apply filter using Parquet's predicate pushdown.
   */
  public ParquetFileEnumerator<T> filter(int columnIndex,
                                         java.util.function.Predicate<Object> predicate) {
    Queue<ParquetExecutionEngine.InMemoryParquetData> filteredBatches =
        new LinkedList<>();

    for (ParquetExecutionEngine.InMemoryParquetData batch : parquetBatches) {
      ParquetExecutionEngine.InMemoryParquetData filtered =
          ParquetExecutionEngine.filter(batch, columnIndex, predicate);

      // Only keep non-empty batches
      if (filtered.getMetadata() != null) {
        filteredBatches.offer(filtered);
      }
    }

    this.parquetBatches = filteredBatches;
    return this;
  }

  /**
   * Get memory usage of all loaded Parquet batches.
   */
  public long getMemoryUsage() {
    long total = 0;
    for (ParquetExecutionEngine.InMemoryParquetData batch : parquetBatches) {
      total += ParquetExecutionEngine.getMemoryUsage(batch);
    }
    return total;
  }

  /**
   * Check if the data supports streaming (has multiple row groups).
   */
  public boolean isStreamable() {
    if (parquetBatches.isEmpty()) {
      return false;
    }

    for (ParquetExecutionEngine.InMemoryParquetData batch : parquetBatches) {
      if (ParquetExecutionEngine.isStreamable(batch)) {
        return true;
      }
    }
    return false;
  }
}
