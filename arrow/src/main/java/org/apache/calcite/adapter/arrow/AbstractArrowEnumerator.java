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

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

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
      return this.valueVectors.get(0).getObject(currRowIndex);
    }
    Object[] current = new Object[valueVectors.size()];
    for (int i = 0; i < valueVectors.size(); i++) {
      ValueVector vector = this.valueVectors.get(i);
      current[i] = vector.getObject(currRowIndex);
    }
    return current;
  }

  @Override public void reset() {
    throw new UnsupportedOperationException();
  }
}
