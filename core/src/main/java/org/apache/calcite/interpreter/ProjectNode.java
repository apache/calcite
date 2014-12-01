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

import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.ImmutableList;

/**
 * Interpreter node that implements a
 * {@link org.apache.calcite.rel.logical.LogicalFilter}.
 */
public class ProjectNode implements Node {
  private final ImmutableList<Scalar> projects;
  private final Source source;
  private final Sink sink;
  private final Context context;

  public ProjectNode(Interpreter interpreter, Project rel) {
    ImmutableList.Builder<Scalar> builder = ImmutableList.builder();
    for (RexNode node : rel.getProjects()) {
      builder.add(interpreter.compile(node));
    }
    this.projects = builder.build();
    this.source = interpreter.source(rel, 0);
    this.sink = interpreter.sink(rel);
    this.context = interpreter.createContext();
  }

  public void run() throws InterruptedException {
    Row row;
    while ((row = source.receive()) != null) {
      context.values = row.getValues();
      Object[] values = new Object[projects.size()];
      for (int i = 0; i < projects.size(); i++) {
        Scalar scalar = projects.get(i);
        values[i] = scalar.execute(context);
      }
      sink.send(new Row(values));
    }
  }
}

// End ProjectNode.java
