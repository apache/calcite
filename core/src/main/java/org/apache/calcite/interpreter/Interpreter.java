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
import net.hydromatic.optiq.prepare.OptiqPrepareImpl;

import org.eigenbase.rel.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.ReflectUtil;
import org.eigenbase.util.ReflectiveVisitDispatcher;
import org.eigenbase.util.ReflectiveVisitor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.math.BigDecimal;
import java.util.*;

/**
 * Interpreter.
 *
 * <p>Contains the context for interpreting relational expressions. In
 * particular it holds working state while the data flow graph is being
 * assembled.</p>
 */
public class Interpreter extends AbstractEnumerable<Object[]> {
  final Map<RelNode, NodeInfo> nodes = Maps.newLinkedHashMap();
  private final DataContext dataContext;
  private final RelNode rootRel;
  private final Map<RelNode, List<RelNode>> relInputs = Maps.newHashMap();

  public Interpreter(DataContext dataContext, RelNode rootRel) {
    this.dataContext = dataContext;
    Compiler compiler = new Nodes.CoreCompiler(this);
    this.rootRel = compiler.visitRoot(rootRel);
  }

  public Enumerator<Object[]> enumerator() {
    start();
    final ArrayDeque<Row> queue = nodes.get(rootRel).sink.list;
    return new Enumerator<Object[]>() {
      Row row;

      public Object[] current() {
        return row.getValues();
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
          Comparable o0;
          Comparable o1;
          switch (call.getKind()) {
          case LESS_THAN:
          case LESS_THAN_OR_EQUAL:
          case GREATER_THAN:
          case GREATER_THAN_OR_EQUAL:
          case EQUALS:
          case NOT_EQUALS:
            args = lazyArgs(context);
            o0 = (Comparable) args.get(0);
            if (o0 == null) {
              return null;
            }
            o1 = (Comparable) args.get(1);
            if (o1 == null) {
              return null;
            }
            if (o0 instanceof BigDecimal) {
              if (o1 instanceof Double || o1 instanceof Float) {
                o1 = new BigDecimal(((Number) o1).doubleValue());
              } else {
                o1 = new BigDecimal(((Number) o1).longValue());
              }
            }
            if (o1 instanceof BigDecimal) {
              if (o0 instanceof Double || o0 instanceof Float) {
                o0 = new BigDecimal(((Number) o0).doubleValue());
              } else {
                o0 = new BigDecimal(((Number) o0).longValue());
              }
            }
            final int c = o0.compareTo(o1);
            switch (call.getKind()) {
            case LESS_THAN:
              return c < 0;
            case LESS_THAN_OR_EQUAL:
              return c <= 0;
            case GREATER_THAN:
              return c > 0;
            case GREATER_THAN_OR_EQUAL:
              return c >= 0;
            case EQUALS:
              return c == 0;
            case NOT_EQUALS:
              return c != 0;
            default:
              throw new AssertionError("unknown expression " + call);
            }
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
    final RelNode input = getInput(rel, ordinal);
    final NodeInfo x = nodes.get(input);
    if (x == null) {
      throw new AssertionError("should be registered: " + rel);
    }
    return new ListSource(x.sink);
  }

  private RelNode getInput(RelNode rel, int ordinal) {
    final List<RelNode> inputs = relInputs.get(rel);
    if (inputs != null) {
      return inputs.get(ordinal);
    }
    return rel.getInput(ordinal);
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
    protected RelNode rootRel;
    protected RelNode rel;
    protected Node node;

    private static final String REWRITE_METHOD_NAME = "rewrite";
    private static final String VISIT_METHOD_NAME = "visit";

    Compiler(Interpreter interpreter) {
      this.interpreter = interpreter;
    }

    public RelNode visitRoot(RelNode p) {
      rootRel = p;
      visit(p, 0, null);
      return rootRel;
    }

    @Override public void visit(RelNode p, int ordinal, RelNode parent) {
      for (;;) {
        rel = null;
        boolean found = dispatcher.invokeVisitor(this, p, REWRITE_METHOD_NAME);
        if (!found) {
          throw new AssertionError(
              "interpreter: no implementation for rewrite");
        }
        if (rel == null) {
          break;
        }
        if (OptiqPrepareImpl.DEBUG) {
          System.out.println("Interpreter: rewrite " + p + " to " + rel);
        }
        p = rel;
        if (parent != null) {
          List<RelNode> inputs = interpreter.relInputs.get(parent);
          if (inputs == null) {
            inputs = Lists.newArrayList(parent.getInputs());
            interpreter.relInputs.put(parent, inputs);
          }
          inputs.set(ordinal, p);
        } else {
          rootRel = p;
        }
      }

      // rewrite children first (from left to right)
      final List<RelNode> inputs = interpreter.relInputs.get(p);
      if (inputs != null) {
        for (int i = 0; i < inputs.size(); i++) {
          RelNode input = inputs.get(i);
          visit(input, i, p);
        }
      } else {
        p.childrenAccept(this);
      }

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

    /** Fallback rewrite method.
     *
     * <p>Overriding methods (each with a different sub-class of {@link RelNode}
     * as its argument type) sets the {@link #rel} field if intends to
     * rewrite. */
    public void rewrite(RelNode r) {
    }
  }
}

// End Interpreter.java
