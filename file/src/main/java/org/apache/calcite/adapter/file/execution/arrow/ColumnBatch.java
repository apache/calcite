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
package org.apache.calcite.adapter.file.execution.arrow;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.nio.charset.StandardCharsets;
import java.util.function.Predicate;

/**
 * Column batch abstraction for true vectorized processing.
 *
 * <p>This class provides type-specific column access with primitive arrays
 * for optimal CPU cache utilization and SIMD operations.
 *
 * <p>Test bed implementation for Phase 3 columnar batch processing.
 */
public class ColumnBatch implements AutoCloseable {
  private final VectorSchemaRoot vectorSchemaRoot;
  private final int rowCount;

  public ColumnBatch(VectorSchemaRoot vectorSchemaRoot) {
    this.vectorSchemaRoot = vectorSchemaRoot;
    this.rowCount = vectorSchemaRoot.getRowCount();
  }

  /**
   * Get row count in this batch.
   */
  public int getRowCount() {
    return rowCount;
  }

  /**
   * Get column count in this batch.
   */
  public int getColumnCount() {
    return vectorSchemaRoot.getFieldVectors().size();
  }

  /**
   * Get integer column reader for vectorized operations.
   */
  public IntColumnReader getIntColumn(int columnIndex) {
    if (columnIndex >= getColumnCount()) {
      throw new IndexOutOfBoundsException("Column index: " + columnIndex);
    }

    org.apache.arrow.vector.FieldVector vector = vectorSchemaRoot.getVector(columnIndex);
    if (!(vector instanceof IntVector)) {
      throw new IllegalArgumentException("Column " + columnIndex + " is not an integer column");
    }

    return new IntColumnReader((IntVector) vector);
  }

  /**
   * Get long column reader for vectorized operations.
   */
  public LongColumnReader getLongColumn(int columnIndex) {
    if (columnIndex >= getColumnCount()) {
      throw new IndexOutOfBoundsException("Column index: " + columnIndex);
    }

    org.apache.arrow.vector.FieldVector vector = vectorSchemaRoot.getVector(columnIndex);
    if (!(vector instanceof BigIntVector)) {
      throw new IllegalArgumentException("Column " + columnIndex + " is not a long column");
    }

    return new LongColumnReader((BigIntVector) vector);
  }

  /**
   * Get double column reader for vectorized operations.
   */
  public DoubleColumnReader getDoubleColumn(int columnIndex) {
    if (columnIndex >= getColumnCount()) {
      throw new IndexOutOfBoundsException("Column index: " + columnIndex);
    }

    org.apache.arrow.vector.FieldVector vector = vectorSchemaRoot.getVector(columnIndex);
    if (!(vector instanceof Float8Vector)) {
      throw new IllegalArgumentException("Column " + columnIndex + " is not a double column");
    }

    return new DoubleColumnReader((Float8Vector) vector);
  }

  /**
   * Get boolean column reader for vectorized operations.
   */
  public BooleanColumnReader getBooleanColumn(int columnIndex) {
    if (columnIndex >= getColumnCount()) {
      throw new IndexOutOfBoundsException("Column index: " + columnIndex);
    }

    org.apache.arrow.vector.FieldVector vector = vectorSchemaRoot.getVector(columnIndex);
    if (!(vector instanceof BitVector)) {
      throw new IllegalArgumentException("Column " + columnIndex + " is not a boolean column");
    }

    return new BooleanColumnReader((BitVector) vector);
  }

  /**
   * Get string column reader for vectorized operations.
   */
  public StringColumnReader getStringColumn(int columnIndex) {
    if (columnIndex >= getColumnCount()) {
      throw new IndexOutOfBoundsException("Column index: " + columnIndex);
    }

    org.apache.arrow.vector.FieldVector vector = vectorSchemaRoot.getVector(columnIndex);
    if (!(vector instanceof VarCharVector)) {
      throw new IllegalArgumentException("Column " + columnIndex + " is not a string column");
    }

    return new StringColumnReader((VarCharVector) vector);
  }

  /**
   * Convert back to row format when needed by Calcite.
   */
  public Object[][] toRowFormat() {
    Object[][] rows = new Object[rowCount][getColumnCount()];

    for (int col = 0; col < getColumnCount(); col++) {
      org.apache.arrow.vector.FieldVector vector = vectorSchemaRoot.getVector(col);
      for (int row = 0; row < rowCount; row++) {
        if (vector.isNull(row)) {
          rows[row][col] = null;
        } else {
          rows[row][col] = vector.getObject(row);
        }
      }
    }

    return rows;
  }

  @Override public void close() {
    if (vectorSchemaRoot != null) {
      vectorSchemaRoot.close();
    }
  }

  /**
   * Type-specific column reader for integers with vectorized operations.
   */
  public static class IntColumnReader {
    private final IntVector vector;

    IntColumnReader(IntVector vector) {
      this.vector = vector;
    }

    public int get(int index) {
      return vector.get(index);
    }

    public boolean isNull(int index) {
      return vector.isNull(index);
    }

    /**
     * Vectorized sum operation - JVM can auto-vectorize this loop.
     */
    public long sum() {
      long sum = 0;
      int valueCount = vector.getValueCount();

      for (int i = 0; i < valueCount; i++) {
        if (!vector.isNull(i)) {
          sum += vector.get(i);
        }
      }
      return sum;
    }

    /**
     * Vectorized filter operation.
     */
    public boolean[] filter(Predicate<Integer> predicate) {
      int valueCount = vector.getValueCount();
      boolean[] selection = new boolean[valueCount];

      for (int i = 0; i < valueCount; i++) {
        if (!vector.isNull(i)) {
          selection[i] = predicate.test(vector.get(i));
        } else {
          selection[i] = false;
        }
      }
      return selection;
    }

    /**
     * Get direct access to Arrow's internal buffer for zero-copy operations.
     * WARNING: This is a zero-copy view - modifications affect the original vector.
     */
    public org.apache.arrow.memory.ArrowBuf getDataBuffer() {
      return vector.getDataBuffer();
    }

    /**
     * Get validity buffer for null checking without copying.
     * Returns null if all values are non-null.
     */
    public org.apache.arrow.memory.ArrowBuf getValidityBuffer() {
      return vector.getValidityBuffer();
    }

    /**
     * Get all values as primitive array for maximum performance.
     * NOTE: This creates a copy. Use getDataBuffer() for zero-copy access.
     */
    public int[] getValues() {
      int valueCount = vector.getValueCount();
      int[] values = new int[valueCount];

      for (int i = 0; i < valueCount; i++) {
        values[i] = vector.isNull(i) ? 0 : vector.get(i);
      }
      return values;
    }

    /**
     * Zero-copy vectorized sum using direct buffer access.
     * This is significantly faster than the copying approach.
     */
    public long sumZeroCopy() {
      org.apache.arrow.memory.ArrowBuf dataBuffer = getDataBuffer();

      int valueCount = vector.getValueCount();
      long sum = 0;

      // Use Arrow's built-in null checking instead of accessing validity buffer directly
      for (int i = 0; i < valueCount; i++) {
        if (!vector.isNull(i)) {
          sum += dataBuffer.getInt(i * 4L); // 4 bytes per int
        }
      }

      return sum;
    }
  }

  /**
   * Type-specific column reader for longs with vectorized operations.
   */
  public static class LongColumnReader {
    private final BigIntVector vector;

    LongColumnReader(BigIntVector vector) {
      this.vector = vector;
    }

    public long get(int index) {
      return vector.get(index);
    }

    public boolean isNull(int index) {
      return vector.isNull(index);
    }

    /**
     * Vectorized sum operation.
     */
    public long sum() {
      long sum = 0;
      int valueCount = vector.getValueCount();

      for (int i = 0; i < valueCount; i++) {
        if (!vector.isNull(i)) {
          sum += vector.get(i);
        }
      }
      return sum;
    }

    /**
     * Vectorized filter operation.
     */
    public boolean[] filter(Predicate<Long> predicate) {
      int valueCount = vector.getValueCount();
      boolean[] selection = new boolean[valueCount];

      for (int i = 0; i < valueCount; i++) {
        if (!vector.isNull(i)) {
          selection[i] = predicate.test(vector.get(i));
        } else {
          selection[i] = false;
        }
      }
      return selection;
    }
  }

  /**
   * Type-specific column reader for doubles with vectorized operations.
   */
  public static class DoubleColumnReader {
    private final Float8Vector vector;

    DoubleColumnReader(Float8Vector vector) {
      this.vector = vector;
    }

    public double get(int index) {
      return vector.get(index);
    }

    public boolean isNull(int index) {
      return vector.isNull(index);
    }

    /**
     * Vectorized sum operation.
     */
    public double sum() {
      double sum = 0.0;
      int valueCount = vector.getValueCount();

      for (int i = 0; i < valueCount; i++) {
        if (!vector.isNull(i)) {
          sum += vector.get(i);
        }
      }
      return sum;
    }

    /**
     * Vectorized min/max operation.
     */
    public double[] minMax() {
      double min = Double.MAX_VALUE;
      double max = Double.MIN_VALUE;
      boolean hasValues = false;
      int valueCount = vector.getValueCount();

      for (int i = 0; i < valueCount; i++) {
        if (!vector.isNull(i)) {
          double value = vector.get(i);
          min = Math.min(min, value);
          max = Math.max(max, value);
          hasValues = true;
        }
      }

      return hasValues ? new double[]{min, max} : new double[]{0, 0};
    }

    /**
     * Vectorized filter operation.
     */
    public boolean[] filter(Predicate<Double> predicate) {
      int valueCount = vector.getValueCount();
      boolean[] selection = new boolean[valueCount];

      for (int i = 0; i < valueCount; i++) {
        if (!vector.isNull(i)) {
          selection[i] = predicate.test(vector.get(i));
        } else {
          selection[i] = false;
        }
      }
      return selection;
    }
  }

  /**
   * Type-specific column reader for booleans with vectorized operations.
   */
  public static class BooleanColumnReader {
    private final BitVector vector;

    BooleanColumnReader(BitVector vector) {
      this.vector = vector;
    }

    public boolean get(int index) {
      return vector.get(index) == 1;
    }

    public boolean isNull(int index) {
      return vector.isNull(index);
    }

    /**
     * Vectorized count operation.
     */
    public int countTrue() {
      int count = 0;
      int valueCount = vector.getValueCount();

      for (int i = 0; i < valueCount; i++) {
        if (!vector.isNull(i) && vector.get(i) == 1) {
          count++;
        }
      }
      return count;
    }
  }

  /**
   * Type-specific column reader for strings with vectorized operations.
   */
  public static class StringColumnReader {
    private final VarCharVector vector;

    StringColumnReader(VarCharVector vector) {
      this.vector = vector;
    }

    public String get(int index) {
      if (vector.isNull(index)) {
        return null;
      }
      byte[] bytes = vector.get(index);
      return new String(bytes, StandardCharsets.UTF_8);
    }

    public boolean isNull(int index) {
      return vector.isNull(index);
    }

    /**
     * Vectorized filter operation for strings.
     */
    public boolean[] filter(Predicate<String> predicate) {
      int valueCount = vector.getValueCount();
      boolean[] selection = new boolean[valueCount];

      for (int i = 0; i < valueCount; i++) {
        if (!vector.isNull(i)) {
          String value = get(i);
          selection[i] = predicate.test(value);
        } else {
          selection[i] = false;
        }
      }
      return selection;
    }
  }
}
