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

import org.eigenbase.rel.*;

/**
 * Helper methods for {@link Node} and implementations for core relational
 * expressions.
 */
public class Nodes {
  /** Extension to
   * {@link net.hydromatic.optiq.impl.interpreter.Interpreter.Compiler}
   * that knows how to handle the core logical
   * {@link org.eigenbase.rel.RelNode}s. */
  public static class CoreCompiler extends Interpreter.Compiler {
    CoreCompiler(Interpreter interpreter) {
      super(interpreter);
    }

    public void visit(FilterRelBase filter) {
      node = new FilterNode(interpreter, filter);
    }

    public void visit(ProjectRelBase project) {
      node = new ProjectNode(interpreter, project);
    }

    public void visit(ValuesRelBase value) {
      node = new ValuesNode(interpreter, value);
    }

    public void visit(TableAccessRelBase scan) {
      node = new ScanNode(interpreter, scan);
    }

    public void visit(SortRel sort) {
      node = new SortNode(interpreter, sort);
    }
  }
}

// End Node.java
