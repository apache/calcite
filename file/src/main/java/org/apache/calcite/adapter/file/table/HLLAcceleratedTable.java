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
import org.apache.calcite.adapter.file.statistics.HyperLogLogSketch;
import org.apache.calcite.adapter.file.statistics.StatisticsCache;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Source;

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Table implementation that pre-computes and caches HLL sketches for all columns.
 * This enables instant COUNT(DISTINCT) queries without scanning data.
 */
public class HLLAcceleratedTable extends AbstractTable implements ScannableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(HLLAcceleratedTable.class);

  private final Source source;
  private final String tableName;
  private final RelDataType rowType;
  private final Map<String, HyperLogLogSketch> columnSketches = new ConcurrentHashMap<>();
  private final Map<String, Long> exactCounts = new ConcurrentHashMap<>();
  private boolean sketchesBuilt = false;

  public HLLAcceleratedTable(Source source, String tableName, RelDataType rowType) {
    this.source = source;
    this.tableName = tableName;
    this.rowType = rowType;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return rowType;
  }

  /**
   * Build HLL sketches for all columns by scanning the data once.
   * This is called lazily on first access.
   */
  private synchronized void buildSketchesIfNeeded() {
    if (sketchesBuilt) {
      return;
    }

    LOGGER.debug("Building HLL sketches for table: {}", tableName);
    long startTime = System.currentTimeMillis();

    try {
      // Initialize sketches for each column
      List<String> columnNames = rowType.getFieldNames();
      for (String column : columnNames) {
        columnSketches.put(column, new HyperLogLogSketch(14)); // 14-bit precision
        exactCounts.put(column, 0L);
      }

      // Scan the Parquet file and build sketches
      if (source.path().endsWith(".parquet")) {
        buildFromParquet();
      }

      // Save sketches to cache
      saveSketchesToCache();

      sketchesBuilt = true;
      long elapsed = System.currentTimeMillis() - startTime;
      LOGGER.debug("Built HLL sketches in {}ms", elapsed);

      // Print sketch estimates
      for (Map.Entry<String, HyperLogLogSketch> entry : columnSketches.entrySet()) {
        LOGGER.debug("  Column {}: ~{} distinct values", entry.getKey(), entry.getValue().getEstimate());
      }

    } catch (Exception e) {
      LOGGER.error("Failed to build HLL sketches", e);
    }
  }

  private void buildFromParquet() throws IOException {
    File file = new File(source.path());
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(file.getAbsolutePath());

    @SuppressWarnings("deprecation")
    ParquetFileReader reader =
        ParquetFileReader.open(new org.apache.hadoop.conf.Configuration(), path);
    try {

      ParquetMetadata metadata = reader.getFooter();
      MessageType schema = metadata.getFileMetaData().getSchema();

      PageReadStore pages;
      while ((pages = reader.readNextRowGroup()) != null) {
        long rows = pages.getRowCount();
        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
        RecordReader recordReader =
            columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

        for (int i = 0; i < rows; i++) {
          SimpleGroup group = (SimpleGroup) recordReader.read();

          // Add each field value to its corresponding HLL sketch
          for (int j = 0; j < group.getType().getFieldCount(); j++) {
            String fieldName = group.getType().getFieldName(j);
            HyperLogLogSketch sketch = columnSketches.get(fieldName);

            if (sketch != null && group.getFieldRepetitionCount(j) > 0) {
              Object value = group.getValueToString(j, 0);
              if (value != null) {
                sketch.add(value);
              }
            }
          }
        }
      }
    } finally {
      reader.close();
    }
  }

  private void saveSketchesToCache() {
    String cacheDir = System.getProperty("calcite.file.statistics.cache.directory");
    if (cacheDir == null) {
      return;
    }

    File dir = new File(cacheDir);
    if (!dir.exists()) {
      dir.mkdirs();
    }

    for (Map.Entry<String, HyperLogLogSketch> entry : columnSketches.entrySet()) {
      File sketchFile = new File(dir, tableName + "_" + entry.getKey() + ".hll");
      try {
        StatisticsCache.saveHLLSketch(entry.getValue(), sketchFile);
      } catch (Exception e) {
        LOGGER.error("Failed to save HLL sketch: {}", sketchFile.getName(), e);
      }
    }
  }

  /**
   * Get the pre-computed distinct count for a column.
   * This returns instantly without scanning data.
   */
  public long getDistinctCount(String columnName) {
    buildSketchesIfNeeded();
    HyperLogLogSketch sketch = columnSketches.get(columnName);
    return sketch != null ? sketch.getEstimate() : -1;
  }

  /**
   * Check if we have pre-computed HLL sketch for a column.
   */
  public boolean hasHLLSketch(String columnName) {
    buildSketchesIfNeeded();
    return columnSketches.containsKey(columnName);
  }

  @Override public Enumerable<Object[]> scan(DataContext root) {
    // For COUNT(DISTINCT) queries, return pre-computed results
    final String queryType = (String) root.get("query.type");
    if ("count_distinct".equals(queryType)) {
      final String columnName = (String) root.get("query.column");
      if (columnName != null && hasHLLSketch(columnName)) {
        final long distinctCount = getDistinctCount(columnName);

        return new AbstractEnumerable<Object[]>() {
          @Override public Enumerator<Object[]> enumerator() {
            return new Enumerator<Object[]>() {
              private boolean hasNext = true;

              @Override public Object[] current() {
                return new Object[] { distinctCount };
              }

              @Override public boolean moveNext() {
                if (hasNext) {
                  hasNext = false;
                  return true;
                }
                return false;
              }

              @Override public void reset() {
                hasNext = true;
              }

              @Override public void close() {
              }
            };
          }
        };
      }
    }

    // Fall back to regular scanning for non-optimized queries
    // This would delegate to the original table implementation
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        throw new UnsupportedOperationException("Regular scanning not implemented");
      }
    };
  }
}
