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

import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.gandiva.evaluator.Projector;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;

/**
 * Enumerator that reads from a projected collection of Arrow or Parquet columns.
 */
class ArrowProjectEnumerator extends AbstractArrowEnumerator {
    private final Object projector; // Can be either Arrow Projector or Parquet ReadSupport
    private final Object sourceReader; // Can be either ArrowFileReader or ParquetReader<?>
    private final BufferAllocator allocator;
    private Object current; // For Parquet records

    ArrowProjectEnumerator(Object sourceReader, ImmutableIntList fields, Object projector) {
        super(sourceReader, fields);
        this.sourceReader = sourceReader;
        this.projector = projector;
        this.allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    void evaluateOperator(ArrowRecordBatch arrowRecordBatch) {
        try {
            if (sourceReader instanceof ArrowFileReader) {
                ((Projector) projector).evaluate(arrowRecordBatch, valueVectors);
            } else {
                // For Parquet, projection is typically done during reading, so we don't need to do anything here
            }
        } catch (GandivaException e) {
            throw Util.toUnchecked(e);
        }
    }

    @Override
    public boolean moveNext() {
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
      if (currRowIndex >= rowCount - 1) {
        ArrowFileReader arrowFileReader = (ArrowFileReader) sourceReader;
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

    private boolean moveNextParquet() throws IOException {
      return true;
    }

    @Override
    public void close() {
        try {
            if (projector instanceof Projector) {
                ((Projector) projector).close();
            }
            if (sourceReader instanceof ArrowFileReader) {
                ((ArrowFileReader) sourceReader).close();
            } else if (sourceReader instanceof ParquetReader) {
                ((ParquetReader<?>) sourceReader).close();
            }
            allocator.close();
        } catch (IOException | GandivaException e) {
            throw Util.toUnchecked(e);
        }
    }

    @Override
    public Object current() {
        if (sourceReader instanceof ParquetReader) {
            return current;
        }
        return super.current();
    }
}
