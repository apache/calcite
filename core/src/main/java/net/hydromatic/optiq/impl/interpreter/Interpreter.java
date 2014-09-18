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

import net.hydromatic.linq4j.AbstractEnumerable;
import net.hydromatic.linq4j.Enumerator;

import net.hydromatic.optiq.DataContext;

import org.eigenbase.rel.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.ReflectUtil;
import org.eigenbase.util.ReflectiveVisitDispatcher;
import org.eigenbase.util.ReflectiveVisitor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.*;

/**
 * Interpreter.
 *
 * <p>Contains the context for interpreting relational expressions. In
 * particular it holds working state while the data flow graph is being
 * assembled.</p>
 */
public class Interpreter extends AbstractEnumerable<Row> {
  final Map<RelNode, NodeInfo> nodes = Maps.newLinkedHashMap();
  private final DataContext dataContext;
  private final RelNode rootRel;

  public Interpreter(DataContext dataContext, RelNode rootRel) {
    this.dataContext = dataContext;
    this.rootRel = rootRel;
    Compiler compiler = new Nodes.CoreCompiler(this);
    compiler.visit(rootRel, 0, null);
  }

  public Enumerator<Row> enumerator() {
    start();
    final ArrayDeque<Row> queue = nodes.get(rootRel).sink.list;
    return new Enumerator<Row>() {
      Row row;

      public Row current() {
        return row;
      }

      public boolean moveNext() {
        try {
          row = queue.removeFirst();
        } catch (NoSuchElementException e) {
          return false;
        }
        return true;
      }

      public void reset() {
        row = null;
      }

      public void close() {
        Interpreter.this.close();
      }
    };
  }

  private void start() {
    // We rely on the nodes being ordered leaves first.
    for (Map.Entry<RelNode, NodeInfo> entry : nodes.entrySet()) {
      final NodeInfo nodeInfo = entry.getValue();
      try {
        nodeInfo.node.run();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private void close() {
    // TODO:
  }

  /** Compiles an expression to an executable form. */
  public Scalar compile(final RexNode node) {
    if (node instanceof RexCall) {
      final RexCall call = (RexCall) node;
      final ImmutableList.Builder<Scalar> list = ImmutableList.builder();
      for (RexNode operand : call.getOperands()) {
        list.add(compile(operand));
      }
      final ImmutableList<Scalar> scalars = list.build();
      return new Scalar() {
        public Object execute(final Context context) {
          final List<Object> args;
          final Comparable o0;
          final Comparable o1;
          switch (call.getKind()) {
          case LESS_THAN:
            args = lazyArgs(context);
            o0 = (Comparable) args.get(0);
            o1 = (Comparable) args.get(1);
            return o0 == null || o1 == null ? null : o0.compareTo(o1) < 0;
          case GREATER_THAN:
            args = lazyArgs(context);
            o0 = (Comparable) args.get(0);
            o1 = (Comparable) args.get(1);
            return o0 == null || o1 == null ? null : o0.compareTo(o1) > 0;
          default:
            throw new AssertionError("unknown expression " + call);
          }
        }

        private List<Object> lazyArgs(final Context context) {
          return new AbstractList<Object>() {
            @Override public Object get(int index) {
              return scalars.get(index).execute(context);
            }

            @Override public int size() {
              return scalars.size();
            }
          };
        }
      };
    }
    return new Scalar() {
      public Object execute(Context context) {
        switch (node.getKind()) {
        case LITERAL:
          return ((RexLiteral) node).getValue();
        case INPUT_REF:
          return context.values[((RexInputRef) node).getIndex()];
        default:
          throw new RuntimeException("unknown expression type " + node);
        }
      }
    };
  }

  public Source source(RelNode rel, int ordinal) {
    final RelNode input = rel.getInput(ordinal);
    final NodeInfo x = nodes.get(input);
    if (x == null) {
      throw new AssertionError("should be registered: " + rel);
    }
    return new ListSource(x.sink);
  }

  public Sink sink(RelNode rel) {
    final ArrayDeque<Row> queue = new ArrayDeque<Row>(1);
    final ListSink sink = new ListSink(queue);
    final NodeInfo nodeInfo = new NodeInfo(rel, sink);
    nodes.put(rel, nodeInfo);
    return sink;
  }

  public Context createContext() {
    return new Context();
  }

  public DataContext getDataContext() {
    return dataContext;
  }

  /** Information about a node registered in the data flow graph. */
  private static class NodeInfo {
    final RelNode rel;
    final ListSink sink;
    Node node;

    public NodeInfo(RelNode rel, ListSink sink) {
      this.rel = rel;
      this.sink = sink;
    }
  }

  /** Implementation of {@link Sink} using a {@link java.util.ArrayDeque}. */
  private static class ListSink implements Sink {
    final ArrayDeque<Row> list;

    private ListSink(ArrayDeque<Row> list) {
      this.list = list;
    }

    public void send(Row row) throws InterruptedException {
      list.add(row);
    }

    public void end() throws InterruptedException {
    }
  }

  /** Implementation of {@link Source} using a {@link java.util.ArrayDeque}. */
  private static class ListSource implements Source {
    private final ArrayDeque<Row> list;

    public ListSource(ListSink sink) {
      this.list = sink.list;
    }

    public Row receive() {
      try {
        return list.remove();
      } catch (NoSuchElementException e) {
        return null;
      }
    }
  }

  /**
   * Walks over a tree of {@link org.eigenbase.rel.RelNode} and, for each,
   * creates a {@link net.hydromatic.optiq.impl.interpreter.Node} that can be
   * executed in the interpreter.
   *
   * <p>The compiler looks for methods of the form "visit(XxxRel)".
   * A "visit" method must create an appropriate {@link Node} and put it into
   * the {@link #node} field.
   *
   * <p>If you wish to handle more kinds of relational expressions, add extra
   * "visit" methods in this or a sub-class, and they will be found and called
   * via reflection.
   */
  public static class Compiler extends RelVisitor implements ReflectiveVisitor {
    private final ReflectiveVisitDispatcher<Compiler, RelNode> dispatcher =
        ReflectUtil.createDispatcher(Compiler.class, RelNode.class);
    protected final Interpreter interpreter;
    protected Node node;

    private static final String VISIT_METHOD_NAME = "visit";

    Compiler(Interpreter interpreter) {
      this.interpreter = interpreter;
    }

    @Override public void visit(RelNode p, int ordinal, RelNode parent) {
      // rewrite children first (from left to right)
      super.visit(p, ordinal, parent);

      node = null;
      boolean found = dispatcher.invokeVisitor(this, p, VISIT_METHOD_NAME);
      if (!found) {
        // Probably need to add a visit(XxxRel) method to CoreCompiler.
        throw new AssertionError("interpreter: no implementation for "
            + p.getClass());
      }
      final NodeInfo nodeInfo = interpreter.nodes.get(p);
      assert nodeInfo != null;
      nodeInfo.node = node;
    }
  }
}

// End Interpreter.java
