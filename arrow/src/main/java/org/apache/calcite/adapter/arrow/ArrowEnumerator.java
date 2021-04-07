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

import org.apache.arrow.gandiva.evaluator.Filter;
import org.apache.arrow.gandiva.evaluator.Projector;
import org.apache.arrow.gandiva.evaluator.SelectionVector;
import org.apache.arrow.gandiva.evaluator.SelectionVectorInt16;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ArrowEnumerator.
 */
public class ArrowEnumerator implements Enumerator<Object> {
  private final BufferAllocator allocator;
  private final ArrowFileReader arrowFileReader;
  private final Projector projector;
  private final Filter filter;
  private int rowIndex = -1;
  private List<ValueVector> valueVectors;
  private final int[] fields;
  private final int numFields;
  private int rowSize;
  private ArrowBuf buf;
  private SelectionVector selectionVector;
  private int selectionVectorIndex = 0;

  public ArrowEnumerator(Projector projector, Filter filter, int[] fields,
                         ArrowFileReader arrowFileReader) {
    this.allocator = new RootAllocator(Long.MAX_VALUE);

    this.projector = projector;
    this.filter = filter;
    this.arrowFileReader = arrowFileReader;
    this.fields = fields;
    this.numFields = fields.length;
    this.valueVectors = new ArrayList<ValueVector>(numFields);

    try {
      if (arrowFileReader.loadNextBatch()) {
        loadNextArrowBatch();
      } else {
        throw new IllegalStateException();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static int[] identityList(int n) {
    int[] integers = new int[n];
    for (int i = 0; i < n; i++) {
      integers[i] = i;
    }
    return integers;
  }

  public Object current() {
    if (numFields == 1) {
      return this.valueVectors.get(0).getObject(rowIndex);
    }
    Object[] current = new Object[numFields];
    for (int i = 0; i < numFields; i++) {
      ValueVector vector = this.valueVectors.get(i);
      current[i] = vector.getObject(rowIndex);
    }
    return current;
  }

  public boolean moveNext() {
    if (projector != null) {
      if (rowIndex >= this.rowSize - 1) {
        boolean hasNextBatch = false;
        try {
          hasNextBatch = arrowFileReader.loadNextBatch();
        } catch (IOException e) {
          e.printStackTrace();
        }
        if (hasNextBatch) {
          rowIndex = 0;
          this.valueVectors = new ArrayList<ValueVector>(numFields);
          loadNextArrowBatch();
          return true;
        } else {
          return false;
        }
      } else {
        rowIndex++;
        return true;
      }
    } else {
      if (selectionVectorIndex >= this.selectionVector.getRecordCount()) {
        boolean hasNextBatch = false;
        try {
          hasNextBatch = arrowFileReader.loadNextBatch();
        } catch (IOException e) {
          e.printStackTrace();
        }
        if (hasNextBatch) {
          selectionVectorIndex = 0;
          this.valueVectors = new ArrayList<ValueVector>(numFields);
          loadNextArrowBatch();
          if (selectionVectorIndex >= this.selectionVector.getRecordCount()) {
            return false;
          }
          rowIndex = selectionVector.getIndex(selectionVectorIndex++);
          return true;
        } else {
          return false;
        }
      } else {
        rowIndex = selectionVector.getIndex(selectionVectorIndex++);
        return true;
      }
    }
  }

  private void loadNextArrowBatch() {
    try {
      VectorSchemaRoot vsr = arrowFileReader.getVectorSchemaRoot();
      for (int i : fields) {
        this.valueVectors.add(vsr.getVector(i));
      }
      this.rowSize = vsr.getRowCount();
      VectorUnloader vectorUnloader = new VectorUnloader(vsr);
      ArrowRecordBatch arrowRecordBatch = vectorUnloader.getRecordBatch();
      if (projector != null) {
        projector.evaluate(arrowRecordBatch, valueVectors);
      }
      if (filter != null) {
        this.buf = this.allocator.buffer(rowSize * 2);
        this.selectionVector = new SelectionVectorInt16(buf);
        filter.evaluate(arrowRecordBatch, selectionVector);
        this.selectionVector = selectionVector;
      }
    } catch (IOException | GandivaException e) {
      e.printStackTrace();
    }
  }

  public void reset() {
    throw new UnsupportedOperationException();
  }

  public void close() {
    try {
      if (projector != null) {
        projector.close();
      }
      if (filter != null) {
        buf.close();
        allocator.close();
        filter.close();
      }
    } catch (GandivaException e) {
    }
  }
}
