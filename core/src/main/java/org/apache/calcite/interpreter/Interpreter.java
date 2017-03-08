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

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.TransformedEnumerator;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.rules.CalcSplitRule;
import org.apache.calcite.rel.rules.FilterTableScanRule;
import org.apache.calcite.rel.rules.ProjectTableScanRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitDispatcher;
import org.apache.calcite.util.ReflectiveVisitor;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Interpreter.
 *
 * <p>Contains the context for interpreting relational expressions. In
 * particular it holds working state while the data flow graph is being
 * assembled.
 */
public class Interpreter extends AbstractEnumerable<Object[]>
    implements AutoCloseable {
  final Map<RelNode, NodeInfo> nodes = Maps.newLinkedHashMap();
  private final DataContext dataContext;
  private final RelNode rootRel;
  private final Map<RelNode, List<RelNode>> relInputs = Maps.newHashMap();
  protected final ScalarCompiler scalarCompiler;

  /** Creates an Interpreter. */
  public Interpreter(DataContext dataContext, RelNode rootRel) {
    this.dataContext = Preconditions.checkNotNull(dataContext);
    this.scalarCompiler =
        new JaninoRexCompiler(rootRel.getCluster().getRexBuilder());
    final RelNode rel = optimize(rootRel);
    final Compiler compiler = new Nodes.CoreCompiler(this);
    this.rootRel = compiler.visitRoot(rel);
  }

  private RelNode optimize(RelNode rootRel) {
    final HepProgram hepProgram = new HepProgramBuilder()
        .addRuleInstance(CalcSplitRule.INSTANCE)
        .addRuleInstance(FilterTableScanRule.INSTANCE)
        .addRuleInstance(FilterTableScanRule.INTERPRETER)
        .addRuleInstance(ProjectTableScanRule.INSTANCE)
        .addRuleInstance(ProjectTableScanRule.INTERPRETER).build();
    final HepPlanner planner = new HepPlanner(hepProgram);
    planner.setRoot(rootRel);
    rootRel = planner.findBestExp();
    return rootRel;
  }

  public Enumerator<Object[]> enumerator() {
    start();
    final NodeInfo nodeInfo = nodes.get(rootRel);
    final Enumerator<Row> rows;
    if (nodeInfo.rowEnumerable != null) {
      rows = nodeInfo.rowEnumerable.enumerator();
    } else {
      final ArrayDeque<Row> queue = ((ListSink) nodeInfo.sink).list;
      rows = Linq4j.iterableEnumerator(queue);
    }

    return new TransformedEnumerator<Row, Object[]>(rows) {
      protected Object[] transform(Row row) {
        return row.getValues();
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

  public void close() {
  }

  /** Compiles an expression to an executable form. */
  public Scalar compile(List<RexNode> nodes, RelDataType inputRowType) {
    if (inputRowType == null) {
      inputRowType = dataContext.getTypeFactory().builder().build();
    }
    return scalarCompiler.compile(nodes, inputRowType);
  }

  RelDataType combinedRowType(List<RelNode> inputs) {
    final RelDataTypeFactory.FieldInfoBuilder builder =
        dataContext.getTypeFactory().builder();
    for (RelNode input : inputs) {
      builder.addAll(input.getRowType().getFieldList());
    }
    return builder.build();
  }

  /** Not used. */
  private class FooCompiler implements ScalarCompiler {
    public Scalar compile(List<RexNode> nodes, RelDataType inputRowType) {
      final RexNode node = nodes.get(0);
      if (node instanceof RexCall) {
        final RexCall call = (RexCall) node;
        final Scalar argScalar = compile(call.getOperands(), inputRowType);
        return new Scalar() {
          final Object[] args = new Object[call.getOperands().size()];

          public void execute(final Context context, Object[] results) {
            results[0] = execute(context);
          }

          public Object execute(Context context) {
            Comparable o0;
            Comparable o1;
            switch (call.getKind()) {
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case EQUALS:
            case NOT_EQUALS:
              argScalar.execute(context, args);
              o0 = (Comparable) args[0];
              if (o0 == null) {
                return null;
              }
              o1 = (Comparable) args[1];
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
              if (call.getOperator() == SqlStdOperatorTable.UPPER) {
                argScalar.execute(context, args);
                String s0 = (String) args[0];
                if (s0 == null) {
                  return null;
                }
                return s0.toUpperCase(Locale.ROOT);
              }
              if (call.getOperator() == SqlStdOperatorTable.SUBSTRING) {
                argScalar.execute(context, args);
                String s0 = (String) args[0];
                Number i1 = (Number) args[1];
                Number i2 = (Number) args[2];
                if (s0 == null || i1 == null || i2 == null) {
                  return null;
                }
                return s0.substring(i1.intValue() - 1,
                    i1.intValue() - 1 + i2.intValue());
              }
              throw new AssertionError("unknown expression " + call);
            }
          }
        };
      }
      return new Scalar() {
        public void execute(Context context, Object[] results) {
          results[0] = execute(context);
        }

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
  }

  public Source source(RelNode rel, int ordinal) {
    final RelNode input = getInput(rel, ordinal);
    final NodeInfo nodeInfo = nodes.get(input);
    if (nodeInfo == null) {
      throw new AssertionError("should be registered: " + rel);
    }
    if (nodeInfo.rowEnumerable != null) {
      return new EnumeratorSource(nodeInfo.rowEnumerable.enumerator());
    }
    Sink sink = nodeInfo.sink;
    if (sink instanceof ListSink) {
      return new ListSource((ListSink) nodeInfo.sink);
    }
    throw new IllegalStateException(
      "Got a sink " + sink + " to which there is no match source type!");
  }

  private RelNode getInput(RelNode rel, int ordinal) {
    final List<RelNode> inputs = relInputs.get(rel);
    if (inputs != null) {
      return inputs.get(ordinal);
    }
    return rel.getInput(ordinal);
  }

  /**
   * Creates a Sink for a relational expression to write into.
   *
   * <p>This method is generally called from the constructor of a {@link Node}.
   * But a constructor could instead call
   * {@link #enumerable(RelNode, Enumerable)}.
   *
   * @param rel Relational expression
   * @return Sink
   */
  public Sink sink(RelNode rel) {
    final ArrayDeque<Row> queue = new ArrayDeque<>(1);
    final Sink sink = new ListSink(queue);
    NodeInfo nodeInfo = new NodeInfo(rel, sink, null);
    nodes.put(rel, nodeInfo);
    return sink;
  }

  /** Tells the interpreter that a given relational expression wishes to
   * give its output as an enumerable.
   *
   * <p>This is as opposed to the norm, where a relational expression calls
   * {@link #sink(RelNode)}, then its {@link Node#run()} method writes into that
   * sink.
   *
   * @param rel Relational expression
   * @param rowEnumerable Contents of relational expression
   */
  public void enumerable(RelNode rel, Enumerable<Row> rowEnumerable) {
    NodeInfo nodeInfo = new NodeInfo(rel, null, rowEnumerable);
    nodes.put(rel, nodeInfo);
  }

  public Context createContext() {
    return new Context(dataContext);
  }

  public DataContext getDataContext() {
    return dataContext;
  }

  /** Information about a node registered in the data flow graph. */
  private static class NodeInfo {
    final RelNode rel;
    final Sink sink;
    final Enumerable<Row> rowEnumerable;
    Node node;

    public NodeInfo(RelNode rel, Sink sink, Enumerable<Row> rowEnumerable) {
      this.rel = rel;
      this.sink = sink;
      this.rowEnumerable = rowEnumerable;
      assert (sink != null) != (rowEnumerable != null) : "one or the other";
    }
  }

  /**
   * A {@link Source} that is just backed by an {@link Enumerator}. The {@link Enumerator} is closed
   * when it is finished or by calling {@link #close()}.
   */
  private static class EnumeratorSource implements Source {
    private final Enumerator<Row> enumerator;

    public EnumeratorSource(final Enumerator<Row> enumerator) {
      this.enumerator = Preconditions.checkNotNull(enumerator);
    }

    @Override public Row receive() {
      if (enumerator.moveNext()) {
        return enumerator.current();
      }
      // close the enumerator once we have gone through everything
      enumerator.close();
      return null;
    }

    @Override public void close() {
      enumerator.close();
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

    @SuppressWarnings("deprecation")
    @Override public void setSourceEnumerable(Enumerable<Row> enumerable)
        throws InterruptedException {
      // just copy over the source into the local list
      final Enumerator<Row> enumerator = enumerable.enumerator();
      while (enumerator.moveNext()) {
        this.send(enumerator.current());
      }
      enumerator.close();
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

    @Override public void close() {
      // noop
    }
  }

  /**
   * Walks over a tree of {@link org.apache.calcite.rel.RelNode} and, for each,
   * creates a {@link org.apache.calcite.interpreter.Node} that can be
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
        if (CalcitePrepareImpl.DEBUG) {
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
        if (p instanceof InterpretableRel) {
          InterpretableRel interpretableRel = (InterpretableRel) p;
          node = interpretableRel.implement(
              new InterpretableRel.InterpreterImplementor(interpreter, null,
                  null));
        } else {
          // Probably need to add a visit(XxxRel) method to CoreCompiler.
          throw new AssertionError("interpreter: no implementation for "
              + p.getClass());
        }
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

  /** Converts a list of expressions to a scalar that can compute their
   * values. */
  interface ScalarCompiler {
    Scalar compile(List<RexNode> nodes, RelDataType inputRowType);
  }
}

// End Interpreter.java
