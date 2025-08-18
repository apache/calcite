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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration for Parquet reading operations.
 * Controls batch size and other Parquet-specific optimizations.
 */
public class ParquetConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetConfig.class);
  
  private final int batchSize;
  private final boolean enableVectorizedReader;
  
  private static final boolean DEFAULT_VECTORIZED_ENABLED = false;
  
  // Compute default batch size dynamically
  private static final int DEFAULT_BATCH_SIZE = computeOptimalBatchSize();
  
  public ParquetConfig(int batchSize, boolean enableVectorizedReader) {
    this.batchSize = batchSize;
    this.enableVectorizedReader = enableVectorizedReader;
  }
  
  /**
   * Get the batch size for reading Parquet files.
   * 
   * <h3>Trade-offs:</h3>
   * <ul>
   *   <li><b>Too small (< 1000):</b> High I/O overhead, frequent row group loading, 
   *       poor CPU cache utilization, many small object allocations</li>
   *   <li><b>Too large (> 10000):</b> High memory usage, potential OOM, 
   *       longer GC pauses, reduced parallelism opportunities</li>
   *   <li><b>Optimal (2000-8000):</b> Good balance of memory usage and I/O efficiency</li>
   * </ul>
   * 
   * @return batch size for row reading
   */
  public int getBatchSize() {
    return batchSize;
  }
  
  /**
   * Whether to use the batch reader instead of single-row reader.
   * The "vectorized" reader actually performs row-wise batching for better I/O efficiency.
   */
  public boolean isVectorizedReaderEnabled() {
    return enableVectorizedReader;
  }
  
  /**
   * Create configuration from system properties with adaptive batch sizing.
   */
  public static ParquetConfig fromSystemProperties() {
    String batchSizeStr = System.getProperty("calcite.file.parquet.batch.size");
    int batchSize;
    
    if (batchSizeStr != null) {
      batchSize = Integer.parseInt(batchSizeStr);
    } else {
      // Compute optimal batch size if not specified
      batchSize = computeOptimalBatchSize();
    }
    
    String vectorizedStr = System.getProperty("parquet.enable.vectorized.reader", 
                                              String.valueOf(DEFAULT_VECTORIZED_ENABLED));
    boolean vectorizedEnabled = Boolean.parseBoolean(vectorizedStr);
    
    return new ParquetConfig(batchSize, vectorizedEnabled);
  }
  
  /**
   * Compute optimal batch size based on available memory and system characteristics.
   * 
   * <h3>Factors considered:</h3>
   * <ul>
   *   <li><b>Available heap memory:</b> Larger heap allows larger batches</li>
   *   <li><b>CPU cores:</b> More cores can handle larger batches in parallel</li>
   *   <li><b>Expected row size:</b> Estimated at ~100 bytes per row for typical data</li>
   *   <li><b>GC pressure:</b> Keep batches under 1MB to avoid allocation pressure</li>
   * </ul>
   * 
   * @return optimal batch size (clamped to reasonable bounds)
   */
  public static int computeOptimalBatchSize() {
    Runtime runtime = Runtime.getRuntime();
    long maxHeapBytes = runtime.maxMemory();
    long availableBytes = runtime.freeMemory();
    int availableCores = runtime.availableProcessors();
    
    // Estimate memory per batch (assuming ~100 bytes per row average)
    int estimatedBytesPerRow = 100;
    int maxBatchSizeByMemory = (int) Math.min(
        maxHeapBytes / (100 * estimatedBytesPerRow), // Use 1% of max heap
        1_000_000 / estimatedBytesPerRow             // Cap at 1MB batches
    );
    
    // Scale with CPU cores (more cores can handle larger batches)
    int cpuScaledBatch = Math.max(1024, availableCores * 512);
    
    // Take minimum of memory and CPU constraints
    int computedBatchSize = Math.min(maxBatchSizeByMemory, cpuScaledBatch);
    
    // Clamp to reasonable bounds
    int optimalBatchSize = Math.max(1024,        // Minimum batch size
                                   Math.min(computedBatchSize, 16384)); // Maximum batch size
    
    // Log the computed batch size for debugging
    System.getProperty("calcite.debug.batch.computation", "false").equals("true");
    boolean debugMode = "true".equals(System.getProperty("calcite.debug.batch.computation", "false"));
    if (debugMode) {
      LOGGER.debug("Batch size computation: maxHeap={}MB, cores={}, computed={}, optimal={}",
                       maxHeapBytes / (1024 * 1024), availableCores, computedBatchSize, optimalBatchSize);
    }
    
    return optimalBatchSize;
  }
  
  /**
   * Default configuration with computed optimal batch size and vectorized reading disabled.
   */
  public static final ParquetConfig DEFAULT = new ParquetConfig(DEFAULT_BATCH_SIZE, DEFAULT_VECTORIZED_ENABLED);
  
  @Override
  public String toString() {
    return String.format("ParquetConfig{batchSize=%d, vectorizedReader=%s}", 
                         batchSize, enableVectorizedReader);
  }
}