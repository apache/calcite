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
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;

import org.apache.arrow.vector.types.pojo.Field;

import org.apache.calcite.linq4j.Enumerator;

import java.util.ArrayList;
import java.util.List;

public class ArrowEnumerator implements Enumerator<Object> {
  private final int[] fields;
  private final VectorSchemaRoot[] vectorSchemaRoots;
  private int rootIndex = 0;
  private int vectorIndex = -1;
  private int selectionVectorIndex = 0;
  private UInt4Vector selectionVector;

  public ArrowEnumerator(VectorSchemaRoot[] vectorSchemaRoots, int[] fields, UInt4Vector selectionVector) {
    this.vectorSchemaRoots = vectorSchemaRoots;
    this.fields = fields;
    this.selectionVector = selectionVector;
  }

  public VectorSchemaRoot[] getVectorSchemaRoots() {
    final int[] projected = this.fields;
    int rootSize = vectorSchemaRoots.length;
    VectorSchemaRoot[] vectorSchemaRoots = new VectorSchemaRoot[rootSize];
    for (int i = 0; i < rootSize; i++) {
      List<FieldVector> fieldVectors = new ArrayList<>();
      List<Field> fields = new ArrayList<>();
      for (int value : projected) {
        FieldVector fieldVector = this.vectorSchemaRoots[i].getFieldVectors().get(value);
        fieldVectors.add(fieldVector);
        fields.add(fieldVector.getField());
      }
      vectorSchemaRoots[i] = new VectorSchemaRoot(fields, fieldVectors, this.vectorSchemaRoots[i].getRowCount());
    }
    return vectorSchemaRoots;
  }

  public static int[] identityList(int n) {
    int[] integers = new int[n];
    for (int i = 0; i < n; i++) {
      integers[i] = i;
    }
    return integers;
  }

  public Object current() {
    int fieldSize = vectorSchemaRoots[rootIndex].getFieldVectors().size();
    Object[] current = new Object[fieldSize];
    for (int i = 0; i < fieldSize; i++) {
      FieldVector vector = vectorSchemaRoots[rootIndex].getFieldVectors().get(i);
      current[i] = vector.getObject(vectorIndex);
    }
    return current;
  }

  public boolean moveNext() {
    UInt4Vector selectionVector = this.selectionVector;
    if (selectionVector.getValueCount() > 0) {
      if (selectionVectorIndex >= selectionVector.getValueCount()) {
        return false;
      }

      int index = (int) selectionVector.getObject(selectionVectorIndex++);
      rootIndex = index & 0xffff0000;
      vectorIndex = index & 0x0000ffff;

    } else {
      if (vectorIndex >= (vectorSchemaRoots[rootIndex].getRowCount() - 1)) {
        if (rootIndex >= (vectorSchemaRoots.length - 1)) {
          return false;
        }
        rootIndex++;
        vectorIndex = 0;
      } else {
        vectorIndex++;
      }
    }
    return true;
  }

  public void reset() {
  }

  public void close() {
  }
}
