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

import org.apache.calcite.rel.core.Intersect;

import java.util.HashSet;
import java.util.Set;

/**
 * Interpreter node that implements a
 * {@link org.apache.calcite.rel.core.Intersect}.
 */
public class IntersectNode implements Node {
  private final Source leftSource;
  private final Source rightSource;
  private final Sink sink;
  private final Intersect intersect;

  public IntersectNode(Compiler compiler, Intersect intersect) {
    leftSource = compiler.source(intersect, 0);
    rightSource = compiler.source(intersect, 1);
    sink = compiler.sink(intersect);
    this.intersect = intersect;
  }

  @Override public void run() throws InterruptedException {
    Set<Row> rows = new HashSet<>();
    Set<Row> outputRows = new HashSet<>();
    Row row = null;
    while ((row = rightSource.receive()) != null) {
      rows.add(row);
    }
    while ((row = leftSource.receive()) != null) {
      if (!rows.contains(row)) {
        continue;
      }
      if (intersect.all || outputRows.add(row)) {
        sink.send(row);
      }
    }
  }
}

// End IntersectNode.java
