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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;

class ArrowFilterEnumerator extends AbstractArrowEnumerator {
  private final BufferAllocator allocator;
  private final Object filter; // Can be either Arrow Filter or Parquet FilterPredicate
  private final Object sourceReader; // Can be either ArrowFileReader or ParquetReader<?>
  private ArrowBuf buf;
  private SelectionVector selectionVector;
  private int selectionVectorIndex;
  private Object current; // For Parquet records

  ArrowFilterEnumerator(Object sourceReader, ImmutableIntList fields, Object filter) {
    super(sourceReader instanceof ArrowFileReader ? (ArrowFileReader) sourceReader : null, fields);
    this.sourceReader = sourceReader;
    this.allocator = new RootAllocator(Long.MAX_VALUE);
    this.filter = filter;
  }

  @Override void evaluateOperator(ArrowRecordBatch arrowRecordBatch) {
    try {
      if (sourceReader instanceof ArrowFileReader) {
        this.buf = this.allocator.buffer((long) arrowRecordBatch.getLength() * 2);
        this.selectionVector = new SelectionVectorInt16(buf);
        ((Filter) filter).evaluate(arrowRecordBatch, selectionVector);
      } else {
        // For Parquet, filtering is typically done during reading, so we don't need to do anything here
      }
    } catch (GandivaException e) {
      throw Util.toUnchecked(e);
    }
  }

  private boolean loadNextBatch() throws IOException {
    if (sourceReader instanceof ArrowFileReader) {
      ArrowFileReader arrowReader = (ArrowFileReader) sourceReader;
      boolean hasNextBatch = arrowReader.loadNextBatch();
      if (hasNextBatch) {
        VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
        ArrowRecordBatch recordBatch = ArrowUtils.fromStructsToArrowRecordBatch(root, allocator);
        evaluateOperator(recordBatch);
      }
      return hasNextBatch;
    }
    return false;
  }

  @Override public boolean moveNext() {
    try {
      if (sourceReader instanceof ArrowFileReader) {
        return moveNextArrow();
      } else if (sourceReader instanceof ParquetReader) {
        return moveNextParquet();
      } else {
        throw new IllegalStateException("Unsupported reader type");
      }
    } catch (IOException e) {
      throw Util.toUnchecked(e);
    }
  }

  private boolean moveNextArrow() throws IOException {
    if (selectionVector == null || selectionVectorIndex >= selectionVector.getRecordCount()) {
      boolean hasNextBatch = loadNextBatch();
      if (hasNextBatch) {
        selectionVectorIndex = 0;
        if (selectionVector.getRecordCount() == 0) {
          return moveNextArrow(); // Skip empty batches
        }
        currRowIndex = selectionVector.getIndex(selectionVectorIndex++);
      }
      return hasNextBatch;
    } else {
      currRowIndex = selectionVector.getIndex(selectionVectorIndex++);
      return true;
    }
  }

  private boolean moveNextParquet() throws IOException {
    ParquetReader<?> parquetReader = (ParquetReader<?>) sourceReader;
    Object record = parquetReader.read();
    if (record != null) {
      current = record;
      return true;
    }
    return false;
  }

  @Override public void close() {
    try {
      if (buf != null) {
        buf.close();
      }
      if (filter instanceof Filter) {
        ((Filter) filter).close();
      }
      if (sourceReader instanceof ArrowFileReader) {
        ((ArrowFileReader) sourceReader).close();
      } else if (sourceReader instanceof ParquetReader) {
        ((ParquetReader<?>) sourceReader).close();
      }
    } catch (IOException | GandivaException e) {
      throw Util.toUnchecked(e);
    }
  }

  @Override public Object current() {
    if (sourceReader instanceof ParquetReader) {
      return current;
    }
    return super.current();
  }
}
