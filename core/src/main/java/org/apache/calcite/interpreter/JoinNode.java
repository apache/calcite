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

import org.apache.calcite.rel.core.Join;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * Interpreter node that implements a
 * {@link org.apache.calcite.rel.core.Join}.
 */
public class JoinNode implements Node {
  private final Source leftSource;
  private final Source rightSource;
  private final Sink sink;
  private final Join rel;
  private final Scalar condition;
  private final Context context;

  public JoinNode(Compiler compiler, Join rel) {
    this.leftSource = compiler.source(rel, 0);
    this.rightSource = compiler.source(rel, 1);
    this.sink = compiler.sink(rel);
    this.condition = compiler.compile(ImmutableList.of(rel.getCondition()),
        compiler.combinedRowType(rel.getInputs()));
    this.rel = rel;
    this.context = compiler.createContext();

  }

  public void run() throws InterruptedException {
    List<Row> rightList = null;
    final int leftCount = rel.getLeft().getRowType().getFieldCount();
    final int rightCount = rel.getRight().getRowType().getFieldCount();
    context.values = new Object[rel.getRowType().getFieldCount()];
    Row left;
    Row right;
    while ((left = leftSource.receive()) != null) {
      System.arraycopy(left.getValues(), 0, context.values, 0, leftCount);
      if (rightList == null) {
        rightList = new ArrayList<>();
        while ((right = rightSource.receive()) != null) {
          rightList.add(right);
        }
      }
      for (Row right2 : rightList) {
        System.arraycopy(right2.getValues(), 0, context.values, leftCount,
            rightCount);
        final Boolean execute = (Boolean) condition.execute(context);
        if (execute != null && execute) {
          sink.send(Row.asCopy(context.values));
        }
      }
    }
  }
}

// End JoinNode.java
