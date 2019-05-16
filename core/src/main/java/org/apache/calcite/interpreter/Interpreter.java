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
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.TransformedEnumerator;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
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
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitDispatcher;
import org.apache.calcite.util.ReflectiveVisitor;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Interpreter.
 *
 * <p>Contains the context for interpreting relational expressions. In
 * particular it holds working state while the data flow graph is being
 * assembled.
 */
public class Interpreter extends AbstractEnumerable<Object[]>
    implements AutoCloseable {
  private final Map<RelNode, NodeInfo> nodes;
  private final DataContext dataContext;
  private final RelNode rootRel;

  /** Creates an Interpreter. */
  public Interpreter(DataContext dataContext, RelNode rootRel) {
    this.dataContext = Objects.requireNonNull(dataContext);
    final RelNode rel = optimize(rootRel);
    final CompilerImpl compiler =
        new Nodes.CoreCompiler(this, rootRel.getCluster());
    Pair<RelNode, Map<RelNode, NodeInfo>> pair = compiler.visitRoot(rel);
    this.rootRel = pair.left;
    this.nodes = ImmutableMap.copyOf(pair.right);
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
      final ArrayDeque<Row> queue =
          Iterables.getOnlyElement(nodeInfo.sinks.values()).list;
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
            return ((RexLiteral) node).getValueAs(Comparable.class);
          case INPUT_REF:
            return context.values[((RexInputRef) node).getIndex()];
          default:
            throw new RuntimeException("unknown expression type " + node);
          }
        }
      };
    }
  }

  /** Information about a node registered in the data flow graph. */
  private static class NodeInfo {
    final RelNode rel;
    final Map<Edge, ListSink> sinks = new LinkedHashMap<>();
    final Enumerable<Row> rowEnumerable;
    Node node;

    NodeInfo(RelNode rel, Enumerable<Row> rowEnumerable) {
      this.rel = rel;
      this.rowEnumerable = rowEnumerable;
    }
  }

  /**
   * A {@link Source} that is just backed by an {@link Enumerator}. The {@link Enumerator} is closed
   * when it is finished or by calling {@link #close()}.
   */
  private static class EnumeratorSource implements Source {
    private final Enumerator<Row> enumerator;

    EnumeratorSource(final Enumerator<Row> enumerator) {
      this.enumerator = Objects.requireNonNull(enumerator);
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
    private Iterator<Row> iterator = null;

    ListSource(ArrayDeque<Row> list) {
      this.list = list;
    }

    public Row receive() {
      try {
        if (iterator == null) {
          iterator = list.iterator();
        }
        return iterator.next();
      } catch (NoSuchElementException e) {
        iterator = null;
        return null;
      }
    }

    @Override public void close() {
      // noop
    }
  }

  /** Implementation of {@link Sink} using a {@link java.util.ArrayDeque}. */
  private static class DuplicatingSink implements Sink {
    private List<ArrayDeque<Row>> queues;

    private DuplicatingSink(List<ArrayDeque<Row>> queues) {
      this.queues = ImmutableList.copyOf(queues);
    }

    public void send(Row row) throws InterruptedException {
      for (ArrayDeque<Row> queue : queues) {
        queue.add(row);
      }
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
  static class CompilerImpl extends RelVisitor
      implements Compiler, ReflectiveVisitor {
    final ScalarCompiler scalarCompiler;
    private final ReflectiveVisitDispatcher<CompilerImpl, RelNode> dispatcher =
        ReflectUtil.createDispatcher(CompilerImpl.class, RelNode.class);
    protected final Interpreter interpreter;
    protected RelNode rootRel;
    protected RelNode rel;
    protected Node node;
    final Map<RelNode, NodeInfo> nodes = new LinkedHashMap<>();
    final Map<RelNode, List<RelNode>> relInputs = new HashMap<>();
    final Multimap<RelNode, Edge> outEdges = LinkedHashMultimap.create();

    private static final String REWRITE_METHOD_NAME = "rewrite";
    private static final String VISIT_METHOD_NAME = "visit";

    CompilerImpl(Interpreter interpreter, RelOptCluster cluster) {
      this.interpreter = interpreter;
      this.scalarCompiler = new JaninoRexCompiler(cluster.getRexBuilder());
    }

    /** Visits the tree, starting from the root {@code p}. */
    Pair<RelNode, Map<RelNode, NodeInfo>> visitRoot(RelNode p) {
      rootRel = p;
      visit(p, 0, null);
      return Pair.of(rootRel, nodes);
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
        if (CalciteSystemProperty.DEBUG.value()) {
          System.out.println("Interpreter: rewrite " + p + " to " + rel);
        }
        p = rel;
        if (parent != null) {
          List<RelNode> inputs = relInputs.get(parent);
          if (inputs == null) {
            inputs = Lists.newArrayList(parent.getInputs());
            relInputs.put(parent, inputs);
          }
          inputs.set(ordinal, p);
        } else {
          rootRel = p;
        }
      }

      // rewrite children first (from left to right)
      final List<RelNode> inputs = relInputs.get(p);
      for (Ord<RelNode> input : Ord.zip(Util.first(inputs, p.getInputs()))) {
        outEdges.put(input.e, new Edge(p, input.i));
      }
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
              new InterpretableRel.InterpreterImplementor(this, null, null));
        } else {
          // Probably need to add a visit(XxxRel) method to CoreCompiler.
          throw new AssertionError("interpreter: no implementation for "
              + p.getClass());
        }
      }
      final NodeInfo nodeInfo = nodes.get(p);
      assert nodeInfo != null;
      nodeInfo.node = node;
      if (inputs != null) {
        for (int i = 0; i < inputs.size(); i++) {
          final RelNode input = inputs.get(i);
          visit(input, i, p);
        }
      }
    }

    /** Fallback rewrite method.
     *
     * <p>Overriding methods (each with a different sub-class of {@link RelNode}
     * as its argument type) sets the {@link #rel} field if intends to
     * rewrite. */
    public void rewrite(RelNode r) {
    }

    public Scalar compile(List<RexNode> nodes, RelDataType inputRowType) {
      if (inputRowType == null) {
        inputRowType = interpreter.dataContext.getTypeFactory().builder()
            .build();
      }
      return scalarCompiler.compile(nodes, inputRowType);
    }

    public RelDataType combinedRowType(List<RelNode> inputs) {
      final RelDataTypeFactory.Builder builder =
          interpreter.dataContext.getTypeFactory().builder();
      for (RelNode input : inputs) {
        builder.addAll(input.getRowType().getFieldList());
      }
      return builder.build();
    }

    public Source source(RelNode rel, int ordinal) {
      final RelNode input = getInput(rel, ordinal);
      final Edge edge = new Edge(rel, ordinal);
      final Collection<Edge> edges = outEdges.get(input);
      final NodeInfo nodeInfo = nodes.get(input);
      if (nodeInfo == null) {
        throw new AssertionError("should be registered: " + rel);
      }
      if (nodeInfo.rowEnumerable != null) {
        return new EnumeratorSource(nodeInfo.rowEnumerable.enumerator());
      }
      assert nodeInfo.sinks.size() == edges.size();
      final ListSink sink = nodeInfo.sinks.get(edge);
      if (sink != null) {
        return new ListSource(sink.list);
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

    public Sink sink(RelNode rel) {
      final Collection<Edge> edges = outEdges.get(rel);
      final Collection<Edge> edges2 = edges.isEmpty()
          ? ImmutableList.of(new Edge(null, 0))
          : edges;
      NodeInfo nodeInfo = nodes.get(rel);
      if (nodeInfo == null) {
        nodeInfo = new NodeInfo(rel, null);
        nodes.put(rel, nodeInfo);
        for (Edge edge : edges2) {
          nodeInfo.sinks.put(edge, new ListSink(new ArrayDeque<>()));
        }
      }
      if (edges.size() == 1) {
        return Iterables.getOnlyElement(nodeInfo.sinks.values());
      } else {
        final List<ArrayDeque<Row>> queues = new ArrayList<>();
        for (ListSink sink : nodeInfo.sinks.values()) {
          queues.add(sink.list);
        }
        return new DuplicatingSink(queues);
      }
    }

    public void enumerable(RelNode rel, Enumerable<Row> rowEnumerable) {
      NodeInfo nodeInfo = new NodeInfo(rel, rowEnumerable);
      nodes.put(rel, nodeInfo);
    }

    public Context createContext() {
      return new Context(getDataContext());
    }

    public DataContext getDataContext() {
      return interpreter.dataContext;
    }
  }

  /** Edge between a {@link RelNode} and one of its inputs. */
  static class Edge extends Pair<RelNode, Integer> {
    Edge(RelNode parent, int ordinal) {
      super(parent, ordinal);
    }
  }

  /** Converts a list of expressions to a scalar that can compute their
   * values. */
  interface ScalarCompiler {
    Scalar compile(List<RexNode> nodes, RelDataType inputRowType);
  }
}

// End Interpreter.java
