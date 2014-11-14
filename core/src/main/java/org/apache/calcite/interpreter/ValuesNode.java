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
package net.hydromatic.optiq.impl.interpreter;

import org.eigenbase.rel.ValuesRelBase;
import org.eigenbase.rex.RexLiteral;

import java.util.List;

/**
 * Interpreter node that implements a {@link ValuesRelBase}.
 */
public class ValuesNode implements Node {
  private final Sink sink;
  private final ValuesRelBase rel;
  private final int fieldCount;

  public ValuesNode(Interpreter interpreter, ValuesRelBase rel) {
    this.rel = rel;
    this.sink = interpreter.sink(rel);
    this.fieldCount = rel.getRowType().getFieldCount();
  }

  public void run() throws InterruptedException {
    for (List<RexLiteral> list : rel.getTuples()) {
      final Object[] values = new Object[fieldCount];
      for (int i = 0; i < list.size(); i++) {
        RexLiteral rexLiteral = list.get(i);
        values[i] = rexLiteral.getValue();
      }
      sink.send(new Row(values));
    }
    sink.end();
  }
}

// End ValuesNode.java
