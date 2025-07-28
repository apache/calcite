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

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Vectorized execution engine for file processing using Apache Arrow.
 *
 * <p>This engine provides true columnar vectorized operations including:
 * <ul>
 *   <li>Projection - selecting specific columns</li>
 *   <li>Filtering - applying predicates using SIMD-style operations</li>
 *   <li>Aggregation - sum, min, max with chunked processing</li>
 * </ul>
 */
public final class VectorizedArrowExecutionEngine {

  private VectorizedArrowExecutionEngine() {
    // Utility class
  }
  private static final RootAllocator ALLOCATOR = new RootAllocator(Long.MAX_VALUE);

  /**
   * Projects specific columns from the input batch.
   * This is a zero-copy operation when projecting entire columns.
   */
  public static VectorSchemaRoot project(VectorSchemaRoot input, int[] columnIndices) {
    List<Field> projectedFields = new ArrayList<>();
    for (int idx : columnIndices) {
      projectedFields.add(input.getSchema().getFields().get(idx));
    }

    Schema projectedSchema = new Schema(projectedFields);
    VectorSchemaRoot output = VectorSchemaRoot.create(projectedSchema, ALLOCATOR);
    output.allocateNew();

    // Copy vectors for projected columns
    for (int i = 0; i < columnIndices.length; i++) {
      input.getVector(columnIndices[i]).makeTransferPair(output.getVector(i)).transfer();
    }

    output.setRowCount(input.getRowCount());
    return output;
  }

  /**
   * Filters rows based on a predicate using vectorized operations.
   */
  public static VectorSchemaRoot filter(VectorSchemaRoot input, int columnIndex,
      Predicate<Object> predicate) {

    int rowCount = input.getRowCount();
    BitVector selectionVector = new BitVector("selection", ALLOCATOR);
    selectionVector.allocateNew(rowCount);

    // Build selection vector using chunked processing
    for (int i = 0; i < rowCount; i += 8) {
      int endIdx = Math.min(i + 8, rowCount);

      for (int j = i; j < endIdx; j++) {
        Object value = input.getVector(columnIndex).getObject(j);
        if (predicate.test(value)) {
          selectionVector.set(j, 1);
        }
      }
    }

    // Count selected rows
    int selectedCount = 0;
    for (int i = 0; i < rowCount; i++) {
      if (selectionVector.get(i) == 1) {
        selectedCount++;
      }
    }

    // Create output with selected rows
    VectorSchemaRoot output = VectorSchemaRoot.create(input.getSchema(), ALLOCATOR);
    output.allocateNew();

    // Copy selected rows
    int outputIdx = 0;
    for (int i = 0; i < rowCount; i++) {
      if (selectionVector.get(i) == 1) {
        for (int col = 0; col < input.getFieldVectors().size(); col++) {
          copyValue(input.getVector(col), i, output.getVector(col), outputIdx);
        }
        outputIdx++;
      }
    }

    output.setRowCount(selectedCount);
    selectionVector.close();
    return output;
  }

  /**
   * Performs sum aggregation on a numeric column using vectorized operations.
   */
  public static double aggregateSum(VectorSchemaRoot input, int columnIndex) {
    org.apache.arrow.vector.FieldVector vector = input.getVector(columnIndex);
    int rowCount = input.getRowCount();
    double sum = 0.0;

    if (vector instanceof Float8Vector) {
      Float8Vector doubleVector = (Float8Vector) vector;

      // Vectorized summation - process in chunks for better cache efficiency
      for (int i = 0; i < rowCount; i += 8) {
        int endIdx = Math.min(i + 8, rowCount);
        double chunkSum = 0.0;

        // Unrolled loop for better performance
        for (int j = i; j < endIdx; j++) {
          if (!doubleVector.isNull(j)) {
            chunkSum += doubleVector.get(j);
          }
        }
        sum += chunkSum;
      }
    } else if (vector instanceof IntVector) {
      IntVector intVector = (IntVector) vector;

      for (int i = 0; i < rowCount; i += 8) {
        int endIdx = Math.min(i + 8, rowCount);
        long chunkSum = 0;

        for (int j = i; j < endIdx; j++) {
          if (!intVector.isNull(j)) {
            chunkSum += intVector.get(j);
          }
        }
        sum += chunkSum;
      }
    } else if (vector instanceof BigIntVector) {
      BigIntVector bigIntVector = (BigIntVector) vector;

      for (int i = 0; i < rowCount; i += 8) {
        int endIdx = Math.min(i + 8, rowCount);
        long chunkSum = 0;

        for (int j = i; j < endIdx; j++) {
          if (!bigIntVector.isNull(j)) {
            chunkSum += bigIntVector.get(j);
          }
        }
        sum += chunkSum;
      }
    }

    return sum;
  }

  /**
   * Performs min/max aggregation on a numeric column.
   */
  public static double[] aggregateMinMax(VectorSchemaRoot input, int columnIndex) {
    org.apache.arrow.vector.FieldVector vector = input.getVector(columnIndex);
    int rowCount = input.getRowCount();
    double min = Double.MAX_VALUE;
    double max = Double.MIN_VALUE;
    boolean hasValues = false;

    if (vector instanceof Float8Vector) {
      Float8Vector doubleVector = (Float8Vector) vector;

      for (int i = 0; i < rowCount; i++) {
        if (!doubleVector.isNull(i)) {
          double value = doubleVector.get(i);
          min = Math.min(min, value);
          max = Math.max(max, value);
          hasValues = true;
        }
      }
    } else if (vector instanceof IntVector) {
      IntVector intVector = (IntVector) vector;

      for (int i = 0; i < rowCount; i++) {
        if (!intVector.isNull(i)) {
          int value = intVector.get(i);
          min = Math.min(min, value);
          max = Math.max(max, value);
          hasValues = true;
        }
      }
    }

    return hasValues ? new double[]{min, max} : new double[]{0, 0};
  }

  /**
   * Copies a value from one vector to another.
   */
  private static void copyValue(org.apache.arrow.vector.FieldVector from, int fromIndex,
      org.apache.arrow.vector.FieldVector to, int toIndex) {

    if (from.isNull(fromIndex)) {
      to.setNull(toIndex);
      return;
    }

    if (from instanceof IntVector) {
      ((IntVector) to).set(toIndex, ((IntVector) from).get(fromIndex));
    } else if (from instanceof BigIntVector) {
      ((BigIntVector) to).set(toIndex, ((BigIntVector) from).get(fromIndex));
    } else if (from instanceof Float8Vector) {
      ((Float8Vector) to).set(toIndex, ((Float8Vector) from).get(fromIndex));
    } else if (from instanceof BitVector) {
      ((BitVector) to).set(toIndex, ((BitVector) from).get(fromIndex));
    } else if (from instanceof VarCharVector) {
      byte[] bytes = ((VarCharVector) from).get(fromIndex);
      ((VarCharVector) to).set(toIndex, bytes);
    } else {
      // For other types, use generic object copy
      Object value = from.getObject(fromIndex);
      if (value != null && to instanceof VarCharVector) {
        // Convert to string for generic types
        ((VarCharVector) to).set(toIndex, value.toString().getBytes(StandardCharsets.UTF_8));
      }
    }
  }
}
