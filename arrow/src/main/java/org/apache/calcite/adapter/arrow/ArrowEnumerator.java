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

import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;

import org.apache.calcite.linq4j.Enumerator;

import org.apache.arrow.gandiva.evaluator.Projector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ArrowEnumerator implements Enumerator<Object> {
  private final ArrowFileReader arrowFileReader;
  private VectorSchemaRoot vectorSchemaRoot;
  private Projector projector;
  private int rowIndex = -1;
  private List<ValueVector> valueVectors;
  private int numFields;

  public ArrowEnumerator(Projector projector, int numFields, ArrowFileReader arrowFileReader) {
    this.projector = projector;
    this.arrowFileReader = arrowFileReader;
    this.valueVectors = new ArrayList<ValueVector>(numFields);
    this.numFields = numFields;

    try {
      if (arrowFileReader.loadNextBatch()) {
        VectorSchemaRoot vsr = arrowFileReader.getVectorSchemaRoot();
        for (FieldVector fv : vsr.getFieldVectors()) {
          this.valueVectors.add(fv);
        }

        VectorUnloader vectorUnloader = new VectorUnloader(vsr);
        ArrowRecordBatch arrowRecordBatch = vectorUnloader.getRecordBatch();
        projector.evaluate(arrowRecordBatch, valueVectors);
      } else {
        throw new IllegalStateException();
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (GandivaException e) {
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
    Object[] current = new Object[numFields];
    for (int i = 0; i < numFields; i++) {
      ValueVector vector = this.valueVectors.get(i);
      current[i] = vector.getObject(rowIndex);
    }
    return current;
  }

  public boolean moveNext() {
    if (rowIndex >= this.valueVectors.size() - 1) {
      // TODO: Move to next batch if possible
      return false;
    } else {
      rowIndex++;
    }
    return true;
  }

  public void reset() {
  }

  public void close() {
  }
}
