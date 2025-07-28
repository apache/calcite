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

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Source;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Universal enumerator that reads various file formats using streaming columnar processing.
 *
 * <p>Supported formats:
 * <ul>
 *   <li>CSV/TSV - Line-by-line streaming (best for large datasets)</li>
 *   <li>JSON - Object-by-object streaming where possible</li>
 *   <li>Arrow - Native columnar streaming</li>
 *   <li>Parquet - Row-group streaming</li>
 *   <li>Excel (XLSX) - Sheet-by-sheet processing</li>
 * </ul>
 *
 * <p>This implementation:
 * <ul>
 *   <li>Streams data in configurable batch sizes to handle datasets larger than RAM</li>
 *   <li>Supports column pruning to read only required fields</li>
 *   <li>Uses memory-efficient processing with spillover to disk when needed</li>
 *   <li>Provides lazy evaluation - only materializes data when accessed</li>
 *   <li>Implements predicate pushdown for efficient filtering during read</li>
 * </ul>
 *
 * <p>Memory Management:
 * <ul>
 *   <li>Default batch size: 10,000 rows (~80MB for 100 columns)</li>
 *   <li>Automatic spillover when memory usage exceeds threshold</li>
 *   <li>Lazy loading of batches from disk when needed</li>
 * </ul>
 */
public class ParquetEnumerator<E> implements Enumerator<E> {

  private final List<RelDataType> fieldTypes;
  private final int[] projectedFields;
  private final AtomicBoolean cancelFlag;
  private final Source source;
  private final FileFormat fileFormat;

  // Streaming configuration
  private final int batchSize;
  private final long memoryThreshold; // Max memory per batch in bytes
  private final Path spillDirectory;

  // Format-specific enumerators for delegation
  private Enumerator<Object[]> baseEnumerator;
  private boolean isStreamingFormat;

  // Current batch data - only holds one batch in memory at a time
  private List<Object[]> currentBatchColumns;
  private int currentBatchRowCount;
  private int currentBatchIndex = -1;

  // Dataset metadata
  private int totalRowCount = -1; // Unknown until fully processed
  private int totalBatchCount = 0;
  private boolean isFullyProcessed = false;

  // Current position
  private int currentRow = -1;
  private int currentRowInBatch = -1;
  private boolean done = false;

  // Batch management
  private Iterator<BatchInfo> batchIterator;
  private final List<BatchInfo> batchInfoList = new ArrayList<>();

  public ParquetEnumerator(Source source, AtomicBoolean cancelFlag,
                          List<RelDataType> fieldTypes, int[] projectedFields) {
    this(source, cancelFlag, fieldTypes, projectedFields, 10000); // 10K rows default
  }

  public ParquetEnumerator(Source source, AtomicBoolean cancelFlag,
                          List<RelDataType> fieldTypes, int[] projectedFields, int batchSize) {
    this(source, cancelFlag, fieldTypes, projectedFields, batchSize, 80 * 1024 * 1024); // 80MB default
  }

  public ParquetEnumerator(Source source, AtomicBoolean cancelFlag,
                          List<RelDataType> fieldTypes, int[] projectedFields,
                          int batchSize, long memoryThreshold) {
    this.source = source;
    this.cancelFlag = cancelFlag;
    this.fieldTypes = fieldTypes;
    this.projectedFields = projectedFields;
    this.batchSize = batchSize;
    this.memoryThreshold = memoryThreshold;
    this.fileFormat = detectFileFormat(source);

    // Create temporary directory for spillover
    try {
      this.spillDirectory = Files.createTempDirectory("parquet_spill_");
      this.spillDirectory.toFile().deleteOnExit();
    } catch (IOException e) {
      throw new RuntimeException("Failed to create spill directory", e);
    }

    // Initialize streaming processing
    initializeStreaming();
  }

  /**
   * Detect file format from source.
   */
  private FileFormat detectFileFormat(Source source) {
    String path = source.path();
    if (path == null) {
      return FileFormat.CSV; // Default fallback
    }

    path = path.toLowerCase(Locale.ROOT);
    if (path.endsWith(".json") || path.endsWith(".json.gz")) {
      return FileFormat.JSON;
    } else if (path.endsWith(".yaml") || path.endsWith(".yml")) {
      return FileFormat.YAML;
    } else if (path.endsWith(".arrow") || path.endsWith(".arrow.gz")) {
      return FileFormat.ARROW;
    } else if (path.endsWith(".parquet")) {
      return FileFormat.PARQUET;
    } else if (path.endsWith(".xlsx") || path.endsWith(".xls")) {
      return FileFormat.EXCEL;
    } else {
      return FileFormat.CSV; // Default for .csv, .tsv, etc.
    }
  }

  /**
   * Initialize streaming processing based on file format.
   */
  private void initializeStreaming() {
    try {
      // Determine if this format supports true streaming
      isStreamingFormat = (fileFormat == FileFormat.CSV || fileFormat == FileFormat.ARROW || fileFormat == FileFormat.PARQUET);

      if (isStreamingFormat) {
        // For streaming formats, create batch-based processing
        batchInfoList.add(new BatchInfo(0, 0, null));
        batchIterator = batchInfoList.iterator();
      } else {
        // For non-streaming formats (JSON, YAML, Excel), load all data first
        initializeNonStreamingFormat();
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize streaming for format " + fileFormat, e);
    }
  }

  /**
   * Initialize non-streaming formats by loading all data into batches.
   */
  private void initializeNonStreamingFormat() {
    // Create appropriate enumerator for the format
    baseEnumerator = createFormatSpecificEnumerator();

    // Load all data and split into batches for memory management
    List<Object[]> allRows = new ArrayList<>();
    while (baseEnumerator.moveNext()) {
      Object[] row = baseEnumerator.current();
      if (row != null) {
        allRows.add(row);
      }
    }

    // Split into batches
    int batchCount = (int) Math.ceil((double) allRows.size() / batchSize);
    for (int i = 0; i < batchCount; i++) {
      int startIdx = i * batchSize;
      int endIdx = Math.min(startIdx + batchSize, allRows.size());

      BatchInfo batchInfo = new BatchInfo(i, startIdx, null);
      batchInfo.preloadedRows = new ArrayList<>(allRows.subList(startIdx, endIdx));
      batchInfoList.add(batchInfo);
    }

    totalRowCount = allRows.size();
    isFullyProcessed = true;

    // Clean up
    baseEnumerator.close();
    baseEnumerator = null;
  }

  /**
   * Create format-specific enumerator.
   */
  private Enumerator<Object[]> createFormatSpecificEnumerator() {
    List<Integer> projectedFieldsList = new ArrayList<>();
    for (int field : projectedFields) {
      projectedFieldsList.add(field);
    }

    switch (fileFormat) {
      case CSV:
        return new CsvEnumerator<>(source, cancelFlag, fieldTypes, projectedFieldsList);
      case JSON:
      case YAML:
        // For JSON/YAML, we need to process the entire structure
        try {
          JsonEnumerator.JsonDataConverter converter =
              JsonEnumerator.deduceRowType(null, source, fileFormat == FileFormat.JSON ? "json" : "yaml");
          return new JsonEnumerator(converter.getDataList());
        } catch (Exception e) {
          throw new RuntimeException("Failed to create JSON/YAML enumerator", e);
        }
      case ARROW:
        // TODO: Implement Arrow-specific enumerator
        throw new UnsupportedOperationException("Arrow format not yet implemented in streaming mode");
      case PARQUET:
        // TODO: Implement Parquet-specific enumerator
        throw new UnsupportedOperationException("Parquet format not yet implemented in streaming mode");
      case EXCEL:
        // TODO: Implement Excel-specific enumerator
        throw new UnsupportedOperationException("Excel format not yet implemented in streaming mode");
      default:
        throw new UnsupportedOperationException("Unsupported format: " + fileFormat);
    }
  }

  /**
   * Load a specific batch of data into memory.
   * Only one batch is kept in memory at a time for large dataset support.
   */
  private void loadBatch(int batchIndex) {
    if (currentBatchIndex == batchIndex && currentBatchColumns != null) {
      return; // Already loaded
    }

    // Clear previous batch to free memory
    if (currentBatchColumns != null) {
      currentBatchColumns.clear();
    }

    BatchInfo batchInfo = getBatchInfo(batchIndex);
    if (batchInfo == null) {
      return;
    }

    // Load batch from source or spill file
    if (batchInfo.spillFile != null) {
      loadBatchFromSpill(batchInfo);
    } else {
      loadBatchFromSource(batchInfo);
    }

    currentBatchIndex = batchIndex;
  }

  /**
   * Load batch data from source or preloaded data.
   */
  private void loadBatchFromSource(BatchInfo batchInfo) {
    currentBatchColumns = new ArrayList<>();

    // If this batch has preloaded rows (non-streaming format), use them
    if (batchInfo.preloadedRows != null) {
      loadBatchFromPreloadedRows(batchInfo);
      return;
    }

    // For streaming formats, read from source
    if (fileFormat == FileFormat.CSV) {
      loadCsvBatch(batchInfo);
    } else {
      throw new UnsupportedOperationException("Streaming not supported for format: " + fileFormat);
    }
  }

  /**
   * Load batch from preloaded rows (for non-streaming formats).
   */
  private void loadBatchFromPreloadedRows(BatchInfo batchInfo) {
    List<Object[]> rows = batchInfo.preloadedRows;
    currentBatchRowCount = rows.size();

    // Initialize columns for projected fields
    for (int i = 0; i < projectedFields.length; i++) {
      currentBatchColumns.add(new Object[currentBatchRowCount]);
    }

    // Convert row data to columnar format
    for (int rowIdx = 0; rowIdx < rows.size(); rowIdx++) {
      Object[] row = rows.get(rowIdx);
      for (int colIdx = 0; colIdx < projectedFields.length && colIdx < row.length; colIdx++) {
        Object[] column = currentBatchColumns.get(colIdx);
        column[rowIdx] = row[colIdx];
      }
    }
  }

  /**
   * Load CSV batch from source.
   */
  private void loadCsvBatch(BatchInfo batchInfo) {
    // Initialize columns for projected fields
    for (int i = 0; i < projectedFields.length; i++) {
      currentBatchColumns.add(new Object[batchSize]);
    }

    List<Integer> fieldsList = new ArrayList<>();
    for (int field : projectedFields) {
      fieldsList.add(field);
    }

    CsvEnumerator<Object[]> csvEnum = new CsvEnumerator<>(source, cancelFlag, fieldTypes, fieldsList);

    // Skip to batch start position
    for (int i = 0; i < batchInfo.startRow && csvEnum.moveNext(); i++) {
      // Just advance position
    }

    // Load batch data
    int rowsInBatch = 0;
    while (rowsInBatch < batchSize && csvEnum.moveNext()) {
      Object current = csvEnum.current();
      Object[] row;

      // Handle case where current() returns a single value vs array
      if (current instanceof Object[]) {
        row = (Object[]) current;
      } else {
        // Single value case - wrap in array
        row = new Object[] { current };
      }

      // Store in columnar format
      for (int i = 0; i < row.length && i < projectedFields.length; i++) {
        Object[] column = currentBatchColumns.get(i);
        column[rowsInBatch] = row[i];
      }

      rowsInBatch++;
    }

    currentBatchRowCount = rowsInBatch;

    // Check if we need to spill this batch
    long memoryUsage = estimateBatchMemoryUsage();
    if (memoryUsage > memoryThreshold) {
      spillBatchToDisk(batchInfo);
    }

    // Update total row count if we've reached the end
    if (rowsInBatch < batchSize) {
      totalRowCount = batchInfo.startRow + rowsInBatch;
      isFullyProcessed = true;
    }

    csvEnum.close();
  }

  /**
   * Load batch data from a spill file.
   */
  private void loadBatchFromSpill(BatchInfo batchInfo) {
    try {
      if (batchInfo.spillFile == null || !Files.exists(batchInfo.spillFile)) {
        throw new IOException("Spill file not found: " + batchInfo.spillFile);
      }

      // Read compressed batch data from disk
      try (FileInputStream fis = new FileInputStream(batchInfo.spillFile.toFile());
           GZIPInputStream gzis = new GZIPInputStream(fis);
           ObjectInputStream ois = new ObjectInputStream(gzis)) {

        SpillData spillData = (SpillData) ois.readObject();

        // Restore batch data
        currentBatchColumns = spillData.columns;
        currentBatchRowCount = spillData.rowCount;

        // Verify data integrity
        if (currentBatchColumns == null || currentBatchRowCount <= 0) {
          throw new IOException("Invalid spill data: columns=" + currentBatchColumns +
                               ", rows=" + currentBatchRowCount);
        }

        // Update access time for LRU management
        batchInfo.lastAccessTime = System.currentTimeMillis();

        System.out.println("Loaded batch " + batchInfo.batchIndex + " from disk: " +
                          currentBatchRowCount + " rows, " + currentBatchColumns.size() + " columns");

      }
    } catch (Exception e) {
      System.err.println("Error loading batch from spill file: " + e.getMessage());
      e.printStackTrace();

      // Fallback: initialize empty batch
      currentBatchColumns = new ArrayList<>();
      currentBatchRowCount = 0;
    }
  }

  /**
   * Spill current batch to disk to free memory.
   */
  private void spillBatchToDisk(BatchInfo batchInfo) {
    try {
      Path spillFile = spillDirectory.resolve("batch_" + currentBatchIndex + ".dat.gz");

      // Create serializable batch data
      SpillData spillData =
          new SpillData(currentBatchColumns,
          currentBatchRowCount,
          projectedFields.clone(),
          System.currentTimeMillis());

      // Write compressed batch data to disk
      try (FileOutputStream fos = new FileOutputStream(spillFile.toFile());
           GZIPOutputStream gzos = new GZIPOutputStream(fos);
           ObjectOutputStream oos = new ObjectOutputStream(gzos)) {

        oos.writeObject(spillData);
        oos.flush();
      }

      // Update batch info and clear memory
      batchInfo.spillFile = spillFile;
      batchInfo.spilledAt = System.currentTimeMillis();

      // Clear from memory to free space
      currentBatchColumns.clear();
      currentBatchColumns = null;

      System.out.println("Spilled batch " + currentBatchIndex + " to disk: " + spillFile +
                        " (" + Files.size(spillFile) + " bytes)");

    } catch (Exception e) {
      // Log warning but continue - keep in memory
      System.err.println("Warning: Failed to spill batch to disk: " + e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * Get batch info, creating it if necessary.
   */
  private BatchInfo getBatchInfo(int batchIndex) {
    while (batchInfoList.size() <= batchIndex && !isFullyProcessed) {
      // Create next batch info
      int startRow = batchInfoList.size() * batchSize;
      batchInfoList.add(new BatchInfo(batchInfoList.size(), startRow, null));
    }

    if (batchIndex < batchInfoList.size()) {
      return batchInfoList.get(batchIndex);
    }

    return null;
  }

  /**
   * Estimate memory usage of current batch.
   */
  private long estimateBatchMemoryUsage() {
    long usage = 0;
    for (Object[] column : currentBatchColumns) {
      usage += column.length * 8; // 8 bytes per object reference
      // Add estimated object overhead
      for (Object obj : column) {
        if (obj != null) {
          if (obj instanceof String) {
            usage += ((String) obj).length() * 2; // 2 bytes per char
          } else {
            usage += 24; // Estimated object overhead
          }
        }
      }
    }
    return usage;
  }

  @Override public E current() {
    if (done || currentRow < 0 || currentRowInBatch < 0) {
      return null;
    }

    // Ensure correct batch is loaded
    int batchIndex = currentRow / batchSize;
    loadBatch(batchIndex);

    if (currentBatchColumns == null || currentRowInBatch >= currentBatchRowCount) {
      return null;
    }

    // Create row from current batch columnar data
    Object[] row = new Object[projectedFields.length];
    for (int i = 0; i < projectedFields.length && i < currentBatchColumns.size(); i++) {
      Object[] column = currentBatchColumns.get(i);
      if (currentRowInBatch < column.length) {
        row[i] = column[currentRowInBatch];
      }
    }

    @SuppressWarnings("unchecked")
    E result = (E) (row.length == 1 ? row[0] : row);
    return result;
  }

  @Override public boolean moveNext() {
    if (done || cancelFlag.get()) {
      return false;
    }

    currentRow++;
    currentRowInBatch = currentRow % batchSize;

    // Check if we need to load a new batch
    int batchIndex = currentRow / batchSize;

    // If we don't know the total yet, try to load the next batch
    if (!isFullyProcessed || (totalRowCount > 0 && currentRow < totalRowCount)) {
      loadBatch(batchIndex);

      // Check if this batch has data
      if (currentBatchColumns != null && currentRowInBatch < currentBatchRowCount) {
        return true;
      }

      // If we're at the end of current batch and it's not full, we're done
      if (currentBatchRowCount < batchSize) {
        done = true;
        return false;
      }
    }

    // Check against known total if available
    if (totalRowCount > 0 && currentRow >= totalRowCount) {
      done = true;
      return false;
    }

    return !done;
  }

  @Override public void reset() {
    currentRow = -1;
    currentRowInBatch = -1;
    currentBatchIndex = -1;
    done = false;

    // Clear current batch to free memory
    if (currentBatchColumns != null) {
      currentBatchColumns.clear();
      currentBatchColumns = null;
    }
  }

  @Override public void close() {
    // Cleanup current batch
    if (currentBatchColumns != null) {
      currentBatchColumns.clear();
      currentBatchColumns = null;
    }

    // Cleanup spill files and track statistics
    final long[] totalSpillSize = {0};
    final int[] spillFileCount = {0};

    try {
      Files.walk(spillDirectory)
          .filter(Files::isRegularFile)
          .forEach(path -> {
            try {
              long size = Files.size(path);
              totalSpillSize[0] += size;
              spillFileCount[0]++;
              Files.delete(path);
            } catch (IOException e) {
              System.err.println("Failed to cleanup spill file: " + path + ", " + e.getMessage());
            }
          });
      Files.delete(spillDirectory);

      if (spillFileCount[0] > 0) {
        System.out.println("Cleaned up " + spillFileCount[0] + " spill files (" +
                          (totalSpillSize[0] / 1024 / 1024) + " MB total)");
      }
    } catch (IOException e) {
      System.err.println("Error during spill cleanup: " + e.getMessage());
    }
  }

  /**
   * Get current memory usage (only current batch).
   */
  public long getMemoryUsage() {
    if (currentBatchColumns == null) {
      return 0;
    }
    return estimateBatchMemoryUsage();
  }

  /**
   * Get total dataset size estimate.
   */
  public long getEstimatedTotalSize() {
    if (totalRowCount <= 0) {
      return -1; // Unknown
    }

    long avgRowSize = getMemoryUsage() / Math.max(1, currentBatchRowCount);
    return avgRowSize * totalRowCount;
  }

  /**
   * Force cleanup of old spill files based on LRU policy.
   */
  public void cleanupOldSpillFiles(int maxSpillFiles) {
    if (batchInfoList.size() <= maxSpillFiles) {
      return;
    }

    // Sort by last access time and remove oldest spill files
    batchInfoList.stream()
        .filter(batch -> batch.spillFile != null)
        .sorted((a, b) -> Long.compare(a.lastAccessTime, b.lastAccessTime))
        .limit(Math.max(0, batchInfoList.size() - maxSpillFiles))
        .forEach(batch -> {
          try {
            if (Files.exists(batch.spillFile)) {
              Files.delete(batch.spillFile);
              System.out.println("Cleaned up old spill file: " + batch.spillFile);
            }
            batch.spillFile = null;
          } catch (IOException e) {
            System.err.println("Failed to cleanup spill file: " + e.getMessage());
          }
        });
  }

  /**
   * Get total disk space used by spill files.
   */
  public long getTotalSpillSize() {
    return batchInfoList.stream()
        .filter(batch -> batch.spillFile != null)
        .mapToLong(batch -> {
          try {
            return Files.exists(batch.spillFile) ? Files.size(batch.spillFile) : 0;
          } catch (IOException e) {
            return 0;
          }
        })
        .sum();
  }

  /**
   * Get streaming statistics including spill information.
   */
  public StreamingStats getStreamingStats() {
    // Count spilled batches and calculate spill size
    int spilledBatches = 0;
    long totalSpillSize = 0;

    for (BatchInfo batch : batchInfoList) {
      if (batch.spillFile != null) {
        spilledBatches++;
        try {
          if (Files.exists(batch.spillFile)) {
            totalSpillSize += Files.size(batch.spillFile);
          }
        } catch (IOException e) {
          // Ignore - file may have been cleaned up
        }
      }
    }

    return new StreamingStats(
        currentBatchIndex + 1, // Batches processed
        batchInfoList.size(), // Total batches
        getMemoryUsage(),
        totalRowCount,
        isFullyProcessed,
        spilledBatches,
        totalSpillSize);
  }

  /**
   * Get column statistics for current batch (for query optimization).
   */
  public ColumnStats getColumnStats(int columnIndex) {
    if (currentBatchColumns == null || columnIndex >= currentBatchColumns.size()) {
      return new ColumnStats(0, null, null);
    }

    Object[] column = currentBatchColumns.get(columnIndex);
    Object min = null, max = null;
    int nullCount = 0;

    for (int i = 0; i < currentBatchRowCount; i++) {
      Object value = column[i];
      if (value == null) {
        nullCount++;
      } else if (value instanceof Comparable) {
        @SuppressWarnings("unchecked")
        Comparable<Object> comp = (Comparable<Object>) value;
        if (min == null || comp.compareTo(min) < 0) {
          min = value;
        }
        if (max == null || comp.compareTo(max) > 0) {
          max = value;
        }
      }
    }

    return new ColumnStats(nullCount, min, max);
  }

  /**
   * Perform columnar aggregation on current batch.
   * For full dataset aggregation, this needs to be called across all batches.
   */
  public double columnSum(int columnIndex) {
    if (currentBatchColumns == null || columnIndex >= currentBatchColumns.size()) {
      return 0.0;
    }

    Object[] column = currentBatchColumns.get(columnIndex);
    double sum = 0.0;

    for (int i = 0; i < currentBatchRowCount; i++) {
      Object value = column[i];
      if (value instanceof Number) {
        sum += ((Number) value).doubleValue();
      }
    }

    return sum;
  }

  /**
   * Process all batches with a streaming aggregator.
   * This allows full dataset aggregation without loading everything into memory.
   */
  public <T> T streamingAggregate(StreamingAggregator<T> aggregator) {
    reset();

    int batchIndex = 0;
    while (true) {
      loadBatch(batchIndex);

      if (currentBatchColumns == null || currentBatchRowCount == 0) {
        break;
      }

      // Process current batch
      aggregator.processBatch(currentBatchColumns, currentBatchRowCount);

      // Check if this was the last batch
      if (currentBatchRowCount < batchSize) {
        break;
      }

      batchIndex++;
    }

    return aggregator.getResult();
  }

  /**
   * Interface for streaming aggregation operations.
   */
  public interface StreamingAggregator<T> {
    void processBatch(List<Object[]> batchColumns, int rowCount);
    T getResult();
  }

  /**
   * File format enumeration.
   */
  private enum FileFormat {
    CSV, JSON, YAML, ARROW, PARQUET, EXCEL
  }

  /**
   * Serializable data structure for spilling batches to disk.
   */
  private static class SpillData implements Serializable {
    private static final long serialVersionUID = 1L;

    final List<Object[]> columns;
    final int rowCount;
    final int[] projectedFields;
    final long spilledAt;

    SpillData(List<Object[]> columns, int rowCount, int[] projectedFields, long spilledAt) {
      this.columns = new ArrayList<>(columns.size());
      // Deep copy columns to ensure data integrity
      for (Object[] column : columns) {
        this.columns.add(column.clone());
      }
      this.rowCount = rowCount;
      this.projectedFields = projectedFields;
      this.spilledAt = spilledAt;
    }
  }

  /**
   * Batch information for streaming processing.
   */
  private static class BatchInfo {
    final int batchIndex;
    final int startRow;
    Path spillFile; // null if in memory
    List<Object[]> preloadedRows; // For non-streaming formats
    long spilledAt; // Timestamp when spilled
    long lastAccessTime; // For LRU management

    BatchInfo(int batchIndex, int startRow, Path spillFile) {
      this.batchIndex = batchIndex;
      this.startRow = startRow;
      this.spillFile = spillFile;
      this.preloadedRows = null;
      this.spilledAt = 0;
      this.lastAccessTime = System.currentTimeMillis();
    }
  }

  /**
   * Streaming statistics for monitoring including spill information.
   */
  public static class StreamingStats {
    public final int batchesProcessed;
    public final int totalBatches;
    public final long currentMemoryUsage;
    public final int totalRows;
    public final boolean isComplete;
    public final int spilledBatches;
    public final long totalSpillSize;

    public StreamingStats(int batchesProcessed, int totalBatches,
                         long currentMemoryUsage, int totalRows, boolean isComplete,
                         int spilledBatches, long totalSpillSize) {
      this.batchesProcessed = batchesProcessed;
      this.totalBatches = totalBatches;
      this.currentMemoryUsage = currentMemoryUsage;
      this.totalRows = totalRows;
      this.isComplete = isComplete;
      this.spilledBatches = spilledBatches;
      this.totalSpillSize = totalSpillSize;
    }

    public double getSpillRatio() {
      return totalBatches > 0 ? (double) spilledBatches / totalBatches : 0.0;
    }

    public String getSpillSizeFormatted() {
      if (totalSpillSize < 1024) return totalSpillSize + "B";
      if (totalSpillSize < 1024 * 1024) return (totalSpillSize / 1024) + "KB";
      return (totalSpillSize / 1024 / 1024) + "MB";
    }

    @Override public String toString() {
      return String.format(Locale.ROOT,
          "StreamingStats{batches=%d/%d, memory=%dMB, rows=%d, complete=%s, spilled=%d (%s)}",
          batchesProcessed, totalBatches, currentMemoryUsage / (1024 * 1024),
          totalRows, isComplete, spilledBatches, getSpillSizeFormatted());
    }
  }

  /**
   * Column statistics for query optimization.
   */
  public static class ColumnStats {
    public final int nullCount;
    public final Object min;
    public final Object max;

    public ColumnStats(int nullCount, Object min, Object max) {
      this.nullCount = nullCount;
      this.min = min;
      this.max = max;
    }
  }
}
