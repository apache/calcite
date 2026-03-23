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
package org.apache.calcite.adapter.arrow;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Enumerator that reads from a collection of Arrow value-vectors.
 */
abstract class AbstractArrowEnumerator implements Enumerator<Object> {
  protected final ArrowFileReader arrowFileReader;
  protected final List<Integer> fields;
  protected final List<ValueVector> valueVectors;
  protected int currRowIndex;
  protected int rowCount;

  AbstractArrowEnumerator(ArrowFileReader arrowFileReader, ImmutableIntList fields) {
    this.arrowFileReader = arrowFileReader;
    this.fields = fields;
    this.valueVectors = new ArrayList<>(fields.size());
    this.currRowIndex = -1;
  }

  abstract void evaluateOperator(ArrowRecordBatch arrowRecordBatch);

  protected void loadNextArrowBatch() {
    try {
      final VectorSchemaRoot vsr = arrowFileReader.getVectorSchemaRoot();
      for (int i : fields) {
        this.valueVectors.add(vsr.getVector(i));
      }
      this.rowCount = vsr.getRowCount();
      VectorUnloader vectorUnloader = new VectorUnloader(vsr);
      ArrowRecordBatch arrowRecordBatch = vectorUnloader.getRecordBatch();
      evaluateOperator(arrowRecordBatch);
    } catch (IOException e) {
      throw Util.toUnchecked(e);
    }
  }

  @Override public Object current() {
    if (fields.size() == 1) {
      return getValue(this.valueVectors.get(0), currRowIndex);
    }
    Object[] current = new Object[valueVectors.size()];
    for (int i = 0; i < valueVectors.size(); i++) {
      ValueVector vector = this.valueVectors.get(i);
      current[i] = getValue(vector, currRowIndex);
    }
    return current;
  }

  /** Extracts a value from a vector at the given index.
   *
   * <p>For {@link TimeStampVector}, converts the raw value to
   * milliseconds since epoch, which is the representation used by
   * Calcite's Enumerable runtime for TIMESTAMP types. */
  private static Object getValue(ValueVector vector, int index) {
    if (vector instanceof TimeStampVector) {
      if (vector.isNull(index)) {
        return null;
      }
      final TimeStampVector tsVector = (TimeStampVector) vector;
      final long rawValue = tsVector.get(index);
      final ArrowType.Timestamp tsType =
          (ArrowType.Timestamp) vector.getField().getType();
      return toMillis(rawValue, tsType.getUnit());
    }
    return vector.getObject(index);
  }

  /** Converts a raw timestamp value to milliseconds since epoch.
   *
   * <p>Note: for {@link TimeUnit#MICROSECOND} and {@link TimeUnit#NANOSECOND},
   * this conversion is lossy because sub-millisecond precision is truncated. */
  private static long toMillis(long rawValue, TimeUnit unit) {
    switch (unit) {
    case SECOND:
      return rawValue * 1000L;
    case MILLISECOND:
      return rawValue;
    case MICROSECOND:
      return rawValue / 1000L;
    case NANOSECOND:
      return rawValue / 1_000_000L;
    default:
      throw new IllegalArgumentException("Unsupported TimeUnit: " + unit);
    }
  }

  @Override public void reset() {
    throw new UnsupportedOperationException();
  }
}
