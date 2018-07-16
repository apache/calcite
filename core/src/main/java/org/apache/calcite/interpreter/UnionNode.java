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

import org.apache.calcite.rel.core.Union;

import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.Set;

/**
 * Interpreter node that implements a
 * {@link org.apache.calcite.rel.core.Union}.
 */
public class UnionNode implements Node {
  private final ImmutableList<Source> sources;
  private final Sink sink;
  private final Union rel;

  public UnionNode(Compiler compiler, Union rel) {
    ImmutableList.Builder<Source> builder = ImmutableList.builder();
    for (int i = 0; i < rel.getInputs().size(); i++) {
      builder.add(compiler.source(rel, i));
    }
    this.sources = builder.build();
    this.sink = compiler.sink(rel);
    this.rel = rel;
  }

  public void run() throws InterruptedException {
    final Set<Row> rows = rel.all ? null : new HashSet<>();
    for (Source source : sources) {
      Row row;
      while ((row = source.receive()) != null) {
        if (rows == null || rows.add(row)) {
          sink.send(row);
        }
      }
    }
  }
}

// End UnionNode.java
