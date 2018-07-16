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
package org.apache.calcite.interpreter;

import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * Interpreter node that implements a
 * {@link org.apache.calcite.rel.core.Values}.
 */
public class ValuesNode implements Node {
  private final Sink sink;
  private final int fieldCount;
  private final ImmutableList<Row> rows;

  public ValuesNode(Compiler compiler, Values rel) {
    this.sink = compiler.sink(rel);
    this.fieldCount = rel.getRowType().getFieldCount();
    this.rows = createRows(compiler, rel.getTuples());
  }

  private ImmutableList<Row> createRows(Compiler compiler,
      ImmutableList<ImmutableList<RexLiteral>> tuples) {
    final List<RexNode> nodes = new ArrayList<>();
    for (ImmutableList<RexLiteral> tuple : tuples) {
      nodes.addAll(tuple);
    }
    final Scalar scalar = compiler.compile(nodes, null);
    final Object[] values = new Object[nodes.size()];
    final Context context = compiler.createContext();
    scalar.execute(context, values);
    final ImmutableList.Builder<Row> rows = ImmutableList.builder();
    Object[] subValues = new Object[fieldCount];
    for (int i = 0; i < values.length; i += fieldCount) {
      System.arraycopy(values, i, subValues, 0, fieldCount);
      rows.add(Row.asCopy(subValues));
    }
    return rows.build();
  }

  public void run() throws InterruptedException {
    for (Row row : rows) {
      sink.send(row);
    }
    sink.end();
  }
}

// End ValuesNode.java
