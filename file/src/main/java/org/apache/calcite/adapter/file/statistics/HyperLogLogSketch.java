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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * HyperLogLog implementation for cardinality estimation.
 * 
 * <p>This implementation provides approximate distinct count estimation
 * with configurable precision. Used by Aperio-db for intelligent
 * query optimization and join ordering.
 * 
 * <p>Based on the HyperLogLog algorithm described in:
 * "HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm"
 * by Philippe Flajolet, et al.
 */
public class HyperLogLogSketch implements Serializable {
  private static final long serialVersionUID = 1L;
  
  private final int precision;
  private final int numBuckets;
  private final byte[] buckets;
  private final double alpha;
  private transient MessageDigest hasher;

  /**
   * Create a new HyperLogLog sketch with default precision (14 bits).
   * This gives ~0.8% standard error with 16KB memory usage.
   */
  public HyperLogLogSketch() {
    this(14);
  }

  /**
   * Create a new HyperLogLog sketch with specified precision.
   * 
   * @param precision Number of bits for bucketing (4-16 recommended)
   *                 Higher precision = better accuracy but more memory
   */
  public HyperLogLogSketch(int precision) {
    if (precision < 4 || precision > 16) {
      throw new IllegalArgumentException("Precision must be between 4 and 16");
    }
    
    this.precision = precision;
    this.numBuckets = 1 << precision; // 2^precision
    this.buckets = new byte[numBuckets];
    this.alpha = calculateAlpha(numBuckets);
    initHasher();
  }

  /**
   * Create HyperLogLog sketch from existing bucket data (for deserialization).
   */
  public HyperLogLogSketch(int precision, byte[] buckets) {
    this.precision = precision;
    this.numBuckets = 1 << precision;
    this.buckets = Arrays.copyOf(buckets, buckets.length);
    this.alpha = calculateAlpha(numBuckets);
    initHasher();
  }

  private void initHasher() {
    try {
      this.hasher = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("MD5 algorithm not available", e);
    }
  }

  /**
   * Add a value to the HyperLogLog sketch.
   * 
   * @param value The value to add (will be hashed)
   */
  public void add(Object value) {
    if (value == null) {
      return; // Skip null values
    }
    
    byte[] hash = hashValue(value);
    long hashLong = ByteBuffer.wrap(hash).getLong();
    
    // Use first 'precision' bits for bucket selection
    int bucket = (int) (hashLong >>> (64 - precision)) & ((1 << precision) - 1);
    
    // Count leading zeros in remaining bits + 1
    long remainingBits = hashLong << precision;
    int leadingZeros = Long.numberOfLeadingZeros(remainingBits) + 1;
    
    // Update bucket with maximum leading zeros seen
    buckets[bucket] = (byte) Math.max(buckets[bucket], Math.min(leadingZeros, 255));
  }

  /**
   * Add a string value to the sketch.
   */
  public void addString(String value) {
    add(value);
  }

  /**
   * Add a numeric value to the sketch.
   */
  public void addNumber(Number value) {
    add(value);
  }

  /**
   * Get the estimated cardinality (distinct count).
   */
  public long getEstimate() {
    double rawEstimate = alpha * numBuckets * numBuckets / sumOfPowersOfTwo();
    
    // Apply bias correction and range adjustments
    if (rawEstimate <= 2.5 * numBuckets) {
      // Small range correction
      int zeros = countZeroBuckets();
      if (zeros != 0) {
        return Math.round(numBuckets * Math.log((double) numBuckets / zeros));
      }
    } else if (rawEstimate <= (1.0 / 30.0) * (1L << 32)) {
      // Intermediate range - no correction
      return Math.round(rawEstimate);
    } else {
      // Large range correction
      return Math.round(-1L * (1L << 32) * Math.log(1.0 - rawEstimate / (1L << 32)));
    }
    
    return Math.round(rawEstimate);
  }

  /**
   * Merge another HyperLogLog sketch into this one.
   * Both sketches must have the same precision.
   */
  public void merge(HyperLogLogSketch other) {
    if (other.precision != this.precision) {
      throw new IllegalArgumentException("Cannot merge sketches with different precision");
    }
    
    for (int i = 0; i < numBuckets; i++) {
      buckets[i] = (byte) Math.max(buckets[i], other.buckets[i]);
    }
  }

  /**
   * Get the raw bucket data for serialization.
   */
  public byte[] getBuckets() {
    return Arrays.copyOf(buckets, buckets.length);
  }

  /**
   * Get the precision (number of buckets = 2^precision).
   */
  public int getPrecision() {
    return precision;
  }

  /**
   * Get the number of buckets.
   */
  public int getNumBuckets() {
    return numBuckets;
  }

  /**
   * Get estimated memory usage in bytes.
   */
  public int getMemoryUsage() {
    return numBuckets + 64; // bucket array + object overhead
  }

  private byte[] hashValue(Object value) {
    hasher.reset();
    
    if (value instanceof String) {
      hasher.update(((String) value).getBytes(StandardCharsets.UTF_8));
    } else if (value instanceof Number) {
      hasher.update(ByteBuffer.allocate(8).putLong(((Number) value).longValue()).array());
    } else {
      hasher.update(value.toString().getBytes(StandardCharsets.UTF_8));
    }
    
    return hasher.digest();
  }

  private double sumOfPowersOfTwo() {
    double sum = 0.0;
    for (byte bucket : buckets) {
      sum += Math.pow(2.0, -bucket);
    }
    return sum;
  }

  private int countZeroBuckets() {
    int count = 0;
    for (byte bucket : buckets) {
      if (bucket == 0) {
        count++;
      }
    }
    return count;
  }

  private double calculateAlpha(int m) {
    switch (m) {
      case 16:
        return 0.673;
      case 32:
        return 0.697;
      case 64:
        return 0.709;
      default:
        return 0.7213 / (1.0 + 1.079 / m);
    }
  }

  @Override
  public String toString() {
    return String.format("HyperLogLog{precision=%d, buckets=%d, estimate=%d, memory=%dB}",
        precision, numBuckets, getEstimate(), getMemoryUsage());
  }
}