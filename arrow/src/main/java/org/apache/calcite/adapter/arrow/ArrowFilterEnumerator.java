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

import org.apache.arrow.gandiva.evaluator.Filter;
import org.apache.arrow.gandiva.evaluator.SelectionVector;
import org.apache.arrow.gandiva.evaluator.SelectionVectorInt16;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * Enumerator that reads from a filtered collection of Arrow value-vectors.
 */
class ArrowFilterEnumerator extends AbstractArrowEnumerator {
  private final BufferAllocator allocator;
  private final Filter filter;
  private @Nullable ArrowBuf buf;
  private @Nullable SelectionVector selectionVector;
  private int selectionVectorIndex;

  ArrowFilterEnumerator(ArrowFileReader arrowFileReader, ImmutableIntList fields, Filter filter) {
    super(arrowFileReader, fields);
    this.allocator = new RootAllocator(Long.MAX_VALUE);
    this.filter = filter;
  }

  @Override void evaluateOperator(ArrowRecordBatch arrowRecordBatch) {
    try {
      this.buf = this.allocator.buffer((long) rowCount * 2);
      this.selectionVector = new SelectionVectorInt16(buf);
      filter.evaluate(arrowRecordBatch, selectionVector);
    } catch (GandivaException e) {
      throw Util.toUnchecked(e);
    }
  }

  @Override public boolean moveNext() {
    if (selectionVector == null
        || selectionVectorIndex >= selectionVector.getRecordCount()) {
      boolean hasNextBatch;
      while (true) {
        try {
          hasNextBatch = arrowFileReader.loadNextBatch();
        } catch (IOException e) {
          throw Util.toUnchecked(e);
        }
        if (hasNextBatch) {
          selectionVectorIndex = 0;
          this.valueVectors.clear();
          loadNextArrowBatch();
          requireNonNull(selectionVector, "selectionVector");
          if (selectionVectorIndex >= selectionVector.getRecordCount()) {
            // the "filtered" batch is empty, but there may be more batches to fetch
            continue;
          }
          currRowIndex = selectionVector.getIndex(selectionVectorIndex++);
        }
        return hasNextBatch;
      }
    } else {
      currRowIndex = selectionVector.getIndex(selectionVectorIndex++);
      return true;
    }
  }

  @Override public void close() {
    try {
      if (buf != null) {
        buf.close();
      }
      filter.close();
    } catch (GandivaException e) {
      throw Util.toUnchecked(e);
    }
  }
}
