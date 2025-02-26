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

import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import org.apache.arrow.gandiva.evaluator.Projector;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import java.io.IOException;

/**
 * Enumerator that reads from a projected collection of Arrow value-vectors.
 */
class ArrowProjectEnumerator extends AbstractArrowEnumerator {
  private final Projector projector;

  ArrowProjectEnumerator(ArrowFileReader arrowFileReader, ImmutableIntList fields,
      Projector projector) {
    super(arrowFileReader, fields);
    this.projector = projector;
  }

  @Override protected void evaluateOperator(ArrowRecordBatch arrowRecordBatch) {
    try {
      projector.evaluate(arrowRecordBatch, valueVectors);
    } catch (GandivaException e) {
      throw Util.toUnchecked(e);
    }
  }

  @Override public boolean moveNext() {
    if (currRowIndex >= rowCount - 1) {
      final boolean hasNextBatch;
      try {
        hasNextBatch = arrowFileReader.loadNextBatch();
      } catch (IOException e) {
        throw Util.toUnchecked(e);
      }
      if (hasNextBatch) {
        currRowIndex = 0;
        this.valueVectors.clear();
        loadNextArrowBatch();
      }
      return hasNextBatch;
    } else {
      currRowIndex++;
      return true;
    }
  }

  @Override public void close() {
    try {
      projector.close();
    } catch (GandivaException e) {
      throw Util.toUnchecked(e);
    }
  }
}
