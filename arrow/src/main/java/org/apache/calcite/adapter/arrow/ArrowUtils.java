package org.apache.calcite.adapter.arrow;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.memory.ArrowBuf;

import java.util.ArrayList;
import java.util.List;

public class ArrowUtils {

  public static ArrowRecordBatch fromStructsToArrowRecordBatch(VectorSchemaRoot root, BufferAllocator allocator) {
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
