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

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.types.pojo.Field;

import org.apache.calcite.linq4j.Enumerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ArrowEnumerator implements Enumerator<Object> {
  private final int[] fields;
  private ArrowFileReader arrowFileReader;
  private VectorSchemaRoot vectorSchemaRoot;
  private int vectorIndex = -1;

  public ArrowEnumerator(int[] fields, ArrowFileReader arrowFileReader) {
    this.fields = fields;
    this.arrowFileReader = arrowFileReader;
    try {
      if (arrowFileReader.loadNextBatch()) {
        vectorSchemaRoot = getProjectedVectorSchemaRoot(arrowFileReader.getVectorSchemaRoot());
      }
      else {
        throw new IllegalStateException();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public VectorSchemaRoot getProjectedVectorSchemaRoot(VectorSchemaRoot vectorSchemaRoot) {
    final int[] projected = fields;
    VectorSchemaRoot projectedVectorSchemaRoot;
    List<FieldVector> fieldVectors = new ArrayList<>();
    List<Field> fields = new ArrayList<>();
    for (int value : projected) {
      FieldVector fieldVector = vectorSchemaRoot.getFieldVectors().get(value);
      fieldVectors.add(fieldVector);
      fields.add(fieldVector.getField());
    }
    projectedVectorSchemaRoot = new VectorSchemaRoot(fields, fieldVectors, vectorSchemaRoot.getRowCount());
    return projectedVectorSchemaRoot;
  }

  public static int[] identityList(int n) {
    int[] integers = new int[n];
    for (int i = 0; i < n; i++) {
      integers[i] = i;
    }
    return integers;
  }

  public Object current() {
    int fieldSize = vectorSchemaRoot.getFieldVectors().size();
    Object[] current = new Object[fieldSize];
    for (int i = 0; i < fieldSize; i++) {
      FieldVector vector = vectorSchemaRoot.getFieldVectors().get(i);
      current[i] = vector.getObject(vectorIndex);
    }
    return current;
  }

  public boolean moveNext() {
    if (vectorIndex >= (vectorSchemaRoot.getRowCount() - 1)) {
      boolean hasBatch = false;
      try {
        hasBatch = arrowFileReader.loadNextBatch();
      } catch (IOException e) {}

      if (hasBatch) {
        vectorIndex = 0;
        return true;
      } else {
        return false;
      }
    }
    vectorIndex++;
    return true;
  }

  public void reset() {
  }

  public void close() {
  }
}
