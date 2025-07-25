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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility methods for working with Apache Arrow data structures.
 */
public final class ArrowUtils {
  private ArrowUtils() {
    // Prevent instantiation
  }

  public static ArrowRecordBatch fromStructsToArrowRecordBatch(VectorSchemaRoot root,
      BufferAllocator allocator) {
    List<ArrowFieldNode> nodes = new ArrayList<>();
    List<ArrowBuf> buffers = new ArrayList<>();

    for (ValueVector vector : root.getFieldVectors()) {
      nodes.add(new ArrowFieldNode(vector.getValueCount(), vector.getNullCount()));
      for (ArrowBuf buf : vector.getBuffers(false)) {
        ArrowBuf copyBuf = allocator.buffer(buf.capacity());
        copyBuf.setBytes(0, buf, 0, buf.capacity());
        buffers.add(copyBuf);
      }
    }

    return new ArrowRecordBatch(root.getRowCount(), nodes, buffers);
  }
}
