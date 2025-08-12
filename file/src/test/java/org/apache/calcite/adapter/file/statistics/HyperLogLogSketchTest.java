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
package org.apache.calcite.adapter.file.statistics;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Low-level tests for HyperLogLog sketch accuracy and functionality.
 * These tests validate the core HLL algorithm implementation.
 */
@Tag("unit")public class HyperLogLogSketchTest {

  @Test
  @DisplayName("HLL should provide accurate cardinality estimates for small datasets")
  void testSmallDatasetAccuracy() {
    HyperLogLogSketch hll = new HyperLogLogSketch(12); // Standard precision
    
    // Add 100 distinct values
    Set<String> actualValues = new HashSet<>();
    for (int i = 0; i < 100; i++) {
      String value = "value_" + i;
      hll.addString(value);
      actualValues.add(value);
    }
    
    long estimate = hll.getEstimate();
    int actual = actualValues.size();
    
    // For small datasets, HLL should be within 20% of actual
    double errorRate = Math.abs(estimate - actual) / (double) actual;
    assertTrue(errorRate < 0.20, 
        String.format("HLL estimate %d too far from actual %d (error: %.2f%%)", 
                      estimate, actual, errorRate * 100));
  }

  @Test
  @DisplayName("HLL should provide accurate cardinality estimates for medium datasets")
  void testMediumDatasetAccuracy() {
    HyperLogLogSketch hll = new HyperLogLogSketch(12);
    
    // Add 10,000 distinct values
    Set<String> actualValues = new HashSet<>();
    for (int i = 0; i < 10000; i++) {
      String value = "user_" + i;
      hll.addString(value);
      actualValues.add(value);
    }
    
    long estimate = hll.getEstimate();
    int actual = actualValues.size();
    
    // For medium datasets, HLL should be within 5% of actual
    double errorRate = Math.abs(estimate - actual) / (double) actual;
    assertTrue(errorRate < 0.05, 
        String.format("HLL estimate %d too far from actual %d (error: %.2f%%)", 
                      estimate, actual, errorRate * 100));
  }

  @Test
  @DisplayName("HLL should provide accurate cardinality estimates for large datasets")
  void testLargeDatasetAccuracy() {
    HyperLogLogSketch hll = new HyperLogLogSketch(14); // Higher precision for large datasets
    
    // Add 1,000,000 distinct values (simulated with good hash distribution)
    Random random = new Random(12345); // Fixed seed for reproducible results
    Set<Integer> actualValues = new HashSet<>();
    
    for (int i = 0; i < 1000000; i++) {
      int value = random.nextInt(Integer.MAX_VALUE);
      hll.addNumber(value);
      actualValues.add(value);
    }
    
    long estimate = hll.getEstimate();
    int actual = actualValues.size();
    
    // For large datasets, HLL should be within 2% of actual
    double errorRate = Math.abs(estimate - actual) / (double) actual;
    assertTrue(errorRate < 0.02, 
        String.format("HLL estimate %d too far from actual %d (error: %.2f%%)", 
                      estimate, actual, errorRate * 100));
  }

  @Test
  @DisplayName("HLL should handle duplicate values correctly")
  void testDuplicateValues() {
    HyperLogLogSketch hll = new HyperLogLogSketch();
    
    // Add the same values multiple times
    for (int i = 0; i < 1000; i++) {
      hll.addString("duplicate_1");
      hll.addString("duplicate_2");
      hll.addString("duplicate_3");
    }
    
    long estimate = hll.getEstimate();
    
    // Should estimate close to 3 distinct values despite 3000 additions
    assertTrue(estimate <= 10, 
        String.format("HLL should estimate ~3 distinct values, got %d", estimate));
    assertTrue(estimate >= 1, 
        "HLL should estimate at least 1 distinct value");
  }

  @Test
  @DisplayName("HLL should handle null values gracefully")
  void testNullValues() {
    HyperLogLogSketch hll = new HyperLogLogSketch();
    
    // Add some nulls mixed with real values
    hll.add(null);
    hll.addString(null);
    hll.addNumber(null);
    
    for (int i = 0; i < 100; i++) {
      hll.addString("value_" + i);
      hll.add(null); // Nulls should be ignored
    }
    
    long estimate = hll.getEstimate();
    
    // Should estimate close to 100 (nulls ignored)
    assertTrue(estimate >= 80 && estimate <= 120, 
        String.format("HLL should estimate ~100 values, got %d", estimate));
  }

  @Test
  @DisplayName("HLL precision should affect accuracy and memory usage")
  void testPrecisionImpact() {
    // Test different precision levels
    int[] precisions = {8, 10, 12, 14};
    int numValues = 50000;
    
    for (int precision : precisions) {
      HyperLogLogSketch hll = new HyperLogLogSketch(precision);
      
      // Add known number of distinct values
      for (int i = 0; i < numValues; i++) {
        hll.addString("precision_test_" + i);
      }
      
      long estimate = hll.getEstimate();
      double errorRate = Math.abs(estimate - numValues) / (double) numValues;
      int memoryUsage = hll.getMemoryUsage();
      
      // Higher precision should generally be more accurate
      // and use more memory
      System.out.printf("Precision %d: estimate=%d, actual=%d, error=%.3f%%, memory=%dB%n", 
                       precision, estimate, numValues, errorRate * 100, memoryUsage);
      
      // All precisions should be reasonably accurate
      assertTrue(errorRate < 0.10, 
          String.format("Precision %d error rate %.3f%% too high", precision, errorRate * 100));
      
      // Memory usage should increase with precision
      int expectedBuckets = 1 << precision;
      assertTrue(memoryUsage >= expectedBuckets, 
          String.format("Memory usage %d should be at least %d buckets", memoryUsage, expectedBuckets));
    }
  }

  @Test
  @DisplayName("HLL should support merging sketches")
  void testSketchMerging() {
    HyperLogLogSketch hll1 = new HyperLogLogSketch(12);
    HyperLogLogSketch hll2 = new HyperLogLogSketch(12);
    HyperLogLogSketch hllCombined = new HyperLogLogSketch(12);
    
    // Add different values to each sketch
    for (int i = 0; i < 1000; i++) {
      hll1.addString("set1_" + i);
      hllCombined.addString("set1_" + i);
    }
    
    for (int i = 0; i < 1000; i++) {
      hll2.addString("set2_" + i);
      hllCombined.addString("set2_" + i);
    }
    
    // Merge hll2 into hll1
    hll1.merge(hll2);
    
    long mergedEstimate = hll1.getEstimate();
    long combinedEstimate = hllCombined.getEstimate();
    
    // Merged result should be similar to combined result
    double difference = Math.abs(mergedEstimate - combinedEstimate) / (double) combinedEstimate;
    assertTrue(difference < 0.10, 
        String.format("Merged estimate %d should be close to combined estimate %d", 
                      mergedEstimate, combinedEstimate));
    
    // Should estimate close to 2000 total distinct values
    assertTrue(mergedEstimate >= 1600 && mergedEstimate <= 2400, 
        String.format("Merged HLL should estimate ~2000 values, got %d", mergedEstimate));
  }

  @Test
  @DisplayName("HLL should handle different data types correctly")
  void testDataTypeHandling() {
    HyperLogLogSketch hll = new HyperLogLogSketch();
    
    // Add different types of values
    for (int i = 0; i < 1000; i++) {
      hll.addString("string_" + i);
      hll.addNumber(i);
      hll.addNumber((long) i);
      hll.addNumber((double) i / 100.0);
      hll.add(i % 2 == 0); // Boolean values
    }
    
    long estimate = hll.getEstimate();
    
    // Should handle mixed types and estimate reasonable cardinality
    // Actual distinct count is complex due to type mixing, but should be substantial
    assertTrue(estimate >= 1900, 
        String.format("HLL should estimate substantial cardinality for mixed types, got %d", estimate));
  }

  @Test
  @DisplayName("HLL serialization should preserve sketch state")
  void testSerialization() {
    HyperLogLogSketch original = new HyperLogLogSketch(10);
    
    // Add some values
    for (int i = 0; i < 5000; i++) {
      original.addString("serialization_test_" + i);
    }
    
    long originalEstimate = original.getEstimate();
    
    // Create new sketch from serialized data
    HyperLogLogSketch restored = new HyperLogLogSketch(
        original.getPrecision(), 
        original.getBuckets()
    );
    
    long restoredEstimate = restored.getEstimate();
    
    // Estimates should be identical
    assertEquals(originalEstimate, restoredEstimate, 
        "Restored HLL should have same estimate as original");
    
    // Precision should be preserved
    assertEquals(original.getPrecision(), restored.getPrecision(), 
        "Restored HLL should have same precision");
    
    // Bucket count should be preserved
    assertEquals(original.getNumBuckets(), restored.getNumBuckets(), 
        "Restored HLL should have same bucket count");
  }

  @Test
  @DisplayName("HLL should reject invalid precision values")
  void testInvalidPrecision() {
    // Test boundary conditions
    assertThrows(IllegalArgumentException.class, 
        () -> new HyperLogLogSketch(3), // Too low
        "HLL should reject precision < 4");
    
    assertThrows(IllegalArgumentException.class, 
        () -> new HyperLogLogSketch(17), // Too high  
        "HLL should reject precision > 16");
    
    // Valid boundary values should work
    assertDoesNotThrow(() -> new HyperLogLogSketch(4), "Precision 4 should be valid");
    assertDoesNotThrow(() -> new HyperLogLogSketch(16), "Precision 16 should be valid");
  }

  @Test
  @DisplayName("HLL performance should be reasonable for large datasets")
  void testPerformanceCharacteristics() {
    HyperLogLogSketch hll = new HyperLogLogSketch(12);
    
    long startTime = System.nanoTime();
    
    // Add 100,000 values
    for (int i = 0; i < 100000; i++) {
      hll.addString("performance_test_" + i);
    }
    
    long addTime = System.nanoTime() - startTime;
    
    startTime = System.nanoTime();
    long estimate = hll.getEstimate();
    long estimateTime = System.nanoTime() - startTime;
    
    // Performance should be reasonable
    assertTrue(addTime < 5_000_000_000L, // 5 seconds max for 100K adds
        String.format("Adding 100K values took too long: %.2f seconds", addTime / 1e9));
    
    assertTrue(estimateTime < 100_000_000L, // 100ms max for estimate
        String.format("Estimate calculation took too long: %.2f ms", estimateTime / 1e6));
    
    // Memory usage should be reasonable
    int memoryUsage = hll.getMemoryUsage();
    assertTrue(memoryUsage < 10_000, // Should be under 10KB for precision 12
        String.format("Memory usage %d bytes seems too high", memoryUsage));
    
    // Estimate should still be accurate
    double errorRate = Math.abs(estimate - 100000) / 100000.0;
    assertTrue(errorRate < 0.05, 
        String.format("Large dataset estimate accuracy degraded: %.2f%% error", errorRate * 100));
  }
}