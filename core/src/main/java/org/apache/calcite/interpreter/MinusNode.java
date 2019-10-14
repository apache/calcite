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

import org.apache.calcite.rel.core.Minus;

import com.google.common.collect.HashMultiset;

import java.util.Collection;
import java.util.HashSet;

/**
 * Interpreter node that implements a
 * {@link org.apache.calcite.rel.core.Minus}.
 */
public class MinusNode implements Node {
  private final Source leftSource;
  private final Source rightSource;
  private final Sink sink;
  private final Minus minus;

  public MinusNode(Compiler compiler, Minus minus) {
    leftSource = compiler.source(minus, 0);
    rightSource = compiler.source(minus, 1);
    sink = compiler.sink(minus);
    this.minus = minus;
  }

  @Override public void run() throws InterruptedException {
    Collection<Row> rows = minus.all ? HashMultiset.create() : new HashSet<>();
    Row row = null;
    while ((row = leftSource.receive()) != null) {
      rows.add(row);
    }
    while ((row = rightSource.receive()) != null) {
      rows.remove(row);
    }
    for (Row resultRow: rows) {
      sink.send(resultRow);
    }
  }
}

// End MinusNode.java
