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
package org.apache.calcite.materialize;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.util.CompositeList;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.graph.AttributedDirectedGraph;
import org.apache.calcite.util.graph.CycleDetector;
import org.apache.calcite.util.graph.DefaultEdge;
import org.apache.calcite.util.graph.TopologicalOrderIterator;
import org.apache.calcite.util.mapping.IntPair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Algorithm that suggests a set of lattices.
 */
public class LatticeSuggester {
  final LatticeSpace space;

  private static final HepProgram PROGRAM =
      new HepProgramBuilder()
          .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
          .addRuleInstance(CoreRules.JOIN_CONDITION_PUSH)
          .build();

  /** Lattices, indexed by digest. Uses LinkedHashMap for determinacy. */
  final Map<String, Lattice> latticeMap = new LinkedHashMap<>();

  /** Lattices that have been made obsolete. Key is the obsolete lattice, value
   * is the lattice that superseded it. */
  private final Map<Lattice, Lattice> obsoleteLatticeMap = new HashMap<>();

  /** Whether to try to extend an existing lattice when adding a lattice. */
  private final boolean evolve;

  /** Creates a LatticeSuggester. */
  public LatticeSuggester(FrameworkConfig config) {
    this.evolve = config.isEvolveLattice();
    space = new LatticeSpace(config.getStatisticProvider());
  }

  /** Returns the minimal set of lattices necessary to cover all of the queries
   * seen. Any lattices that are subsumed by other lattices are not included. */
  public Set<Lattice> getLatticeSet() {
    final Set<Lattice> set = new LinkedHashSet<>(latticeMap.values());
    set.removeAll(obsoleteLatticeMap.keySet());
    return ImmutableSet.copyOf(set);
  }

  /** Converts a column reference to an expression. */
  public RexNode toRex(LatticeTable table, int column) {
    final List<RelDataTypeField> fieldList =
        table.t.getRowType().getFieldList();
    if (column < fieldList.size()) {
      return new RexInputRef(column, fieldList.get(column).getType());
    } else {
      return requireNonNull(space.tableExpressions.get(table),
          () -> "space.tableExpressions.get(table) is null for " + table)
          .get(column - fieldList.size());
    }
  }

  /** Adds a query.
   *
   * <p>It may fit within an existing lattice (or lattices). Or it may need a
   * new lattice, or an extension to an existing lattice.
   *
   * @param r Relational expression for a query
   *
   * @return A list of join graphs: usually 1; more if the query contains a
   * cartesian product; zero if the query graph is cyclic
   */
  public List<Lattice> addQuery(RelNode r) {
    // Push filters into joins and towards leaves
    final HepPlanner planner =
        new HepPlanner(PROGRAM, null, true, null, RelOptCostImpl.FACTORY);
    planner.setRoot(r);
    final RelNode r2 = planner.findBestExp();

    final Query q = new Query(space);
    final List<Frame> frameList = new ArrayList<>();
    frames(frameList, q, r2);
    final List<Lattice> lattices = new ArrayList<>();
    frameList.forEach(frame -> addFrame(q, frame, lattices));
    return ImmutableList.copyOf(lattices);
  }

  private void addFrame(Query q, Frame frame, List<Lattice> lattices) {
    final AttributedDirectedGraph<TableRef, StepRef> g =
        AttributedDirectedGraph.create(new StepRef.Factory());
    final Multimap<Pair<TableRef, TableRef>, IntPair> map =
        LinkedListMultimap.create();
    for (TableRef tableRef : frame.tableRefs) {
      g.addVertex(tableRef);
    }
    for (Hop hop : frame.hops) {
      map.put(Pair.of(hop.source.tableRef(), hop.target.tableRef()),
          IntPair.of(hop.source.col(space), hop.target.col(space)));
    }
    for (Map.Entry<Pair<TableRef, TableRef>, Collection<IntPair>> e
        : map.asMap().entrySet()) {
      final TableRef source = e.getKey().left;
      final TableRef target = e.getKey().right;
      final StepRef stepRef =
          q.stepRef(source, target, ImmutableList.copyOf(e.getValue()));
      g.addVertex(stepRef.source());
      g.addVertex(stepRef.target());
      g.addEdge(stepRef.source(), stepRef.target(), stepRef.step,
          stepRef.ordinalInQuery);
    }

    // If the join graph is cyclic, we can't use it.
    final Set<TableRef> cycles = new CycleDetector<>(g).findCycles();
    if (!cycles.isEmpty()) {
      return;
    }

    // Translate the query graph to mutable nodes
    final IdentityHashMap<TableRef, @Nullable MutableNode> nodes = new IdentityHashMap<>();
    final Map<List, MutableNode> nodesByParent = new HashMap<>();
    final List<MutableNode> rootNodes = new ArrayList<>();
    for (TableRef tableRef : TopologicalOrderIterator.of(g)) {
      final List<StepRef> edges = g.getInwardEdges(tableRef);
      final MutableNode node;
      switch (edges.size()) {
      case 0:
        node = new MutableNode(tableRef.table);
        rootNodes.add(node);
        break;
      case 1:
        final StepRef edge = edges.get(0);
        final MutableNode parent = nodes.get(edge.source());
        final List key =
            FlatLists.of(parent, tableRef.table, edge.step.keys);
        final MutableNode existingNode = nodesByParent.get(key);
        if (existingNode == null) {
          node = new MutableNode(tableRef.table, parent, edge.step);
          nodesByParent.put(key, node);
        } else {
          node = existingNode;
        }
        break;
      default:
        for (StepRef edge2 : edges) {
          final MutableNode parent2 = nodes.get(edge2.source());
          requireNonNull(
              parent2,
              () -> "parent for " + edge2.source());
          final MutableNode node2 =
              new MutableNode(tableRef.table, parent2, edge2.step);
          parent2.children.add(node2);
        }
        node = null;
        break;
      }
      nodes.put(tableRef, node);
    }

    // Transcribe the hierarchy of mutable nodes to immutable nodes
    for (MutableNode rootNode : rootNodes) {
      if (rootNode.isCyclic()) {
        continue;
      }
      final CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
      final Lattice.Builder latticeBuilder =
          new Lattice.Builder(space, rootSchema, rootNode);

      final List<MutableNode> flatNodes = new ArrayList<>();
      rootNode.flatten(flatNodes);

      for (MutableMeasure measure : frame.measures) {
        for (ColRef arg : measure.arguments) {
          if (arg == null) {
            // Cannot handle expressions, e.g. "sum(x + 1)" yet
            return;
          }
        }
        latticeBuilder.addMeasure(
            new Lattice.Measure(measure.aggregate, measure.distinct,
                measure.name,
                Util.transform(measure.arguments, colRef -> {
                  final Lattice.Column column;
                  if (colRef instanceof BaseColRef) {
                    final BaseColRef baseColRef = (BaseColRef) colRef;
                    final MutableNode node = nodes.get(baseColRef.t);
                    final int table = flatNodes.indexOf(node);
                    column = latticeBuilder.column(table, baseColRef.c);
                  } else if (colRef instanceof DerivedColRef) {
                    final DerivedColRef derivedColRef =
                        (DerivedColRef) colRef;
                    final String alias = deriveAlias(measure, derivedColRef);
                    column =
                        latticeBuilder.expression(derivedColRef.e, alias,
                            derivedColRef.tableAliases());
                  } else {
                    throw new AssertionError("expression in measure");
                  }
                  latticeBuilder.use(column, true);
                  return column;
                })));
      }

      for (int i = 0; i < frame.columnCount; i++) {
        final ColRef c = frame.column(i);
        if (c instanceof DerivedColRef) {
          final DerivedColRef derivedColRef = (DerivedColRef) c;
          final Lattice.Column expression =
              latticeBuilder.expression(derivedColRef.e,
                  derivedColRef.alias, derivedColRef.tableAliases());
          latticeBuilder.use(expression, false);
        }
      }

      final Lattice lattice0 = latticeBuilder.build();
      final Lattice lattice1 = findMatch(lattice0, rootNode);
      lattices.add(lattice1);
    }
  }

  /** Derives the alias of an expression that is the argument to a measure.
   *
   * <p>For example, if the measure is called "sum_profit" and the aggregate
   * function is "sum", returns "profit".
   */
  private static String deriveAlias(MutableMeasure measure,
      DerivedColRef derivedColRef) {
    if (!derivedColRef.alias.contains("$")) {
      // User specified an alias. Use that.
      return derivedColRef.alias;
    }
    String alias = requireNonNull(measure.name, "measure.name");
    if (alias.contains("$")) {
      // User did not specify an alias for the aggregate function, and it got a
      // system-generated name like 'EXPR$2'. Don't try to derive anything from
      // it.
      return derivedColRef.alias;
    }
    final String aggUpper =
        measure.aggregate.getName().toUpperCase(Locale.ROOT);
    final String aliasUpper = alias.toUpperCase(Locale.ROOT);
    if (aliasUpper.startsWith(aggUpper + "_")) {
      // Convert "sum_profit" to "profit"
      return alias.substring((aggUpper + "_").length());
    } else if (aliasUpper.startsWith(aggUpper)) {
      // Convert "sumprofit" to "profit"
      return alias.substring(aggUpper.length());
    } else if (aliasUpper.endsWith("_" + aggUpper)) {
      // Convert "profit_sum" to "profit"
      return alias.substring(0, alias.length() - ("_" + aggUpper).length());
    } else if (aliasUpper.endsWith(aggUpper)) {
      // Convert "profitsum" to "profit"
      return alias.substring(0, alias.length() - aggUpper.length());
    } else {
      return alias;
    }
  }

  /** Returns the best match for a lattice. If no match, registers the lattice
   * and returns it. Never returns null. */
  private Lattice findMatch(final Lattice lattice, MutableNode mutableNode) {
    final Lattice lattice1 = latticeMap.get(lattice.toString());
    if (lattice1 != null) {
      // Exact match for an existing lattice
      return lattice1;
    }

    if (evolve) {
      // No exact match. Scan existing lattices for a sub-set.
      int bestMatchQuality = 0;
      Lattice bestMatch = null;
      for (Lattice lattice2 : latticeMap.values()) {
        int q = matchQuality(lattice2, lattice);
        if (q > bestMatchQuality) {
          bestMatch = lattice2;
          bestMatchQuality = q;
        } else if (q == bestMatchQuality
            && bestMatch != null
            && !lattice2.rootNode.paths.equals(bestMatch.rootNode.paths)
            && lattice2.rootNode.paths.containsAll(bestMatch.rootNode.paths)) {
          bestMatch = lattice2;
        }
      }

      if (bestMatch != null) {
        // Fix up the best batch
        for (Path path
            : minus(bestMatch.rootNode.paths, lattice.rootNode.paths)) {
          // TODO: assign alias based on node in bestMatch
          mutableNode.addPath(path, null);
        }
        final CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
        final Lattice.Builder builder =
            new Lattice.Builder(space, rootSchema, mutableNode);
        copyMeasures(builder, bestMatch);
        copyMeasures(builder, lattice);
        final Lattice lattice2 = builder.build();
        latticeMap.remove(bestMatch.toString());
        obsoleteLatticeMap.put(bestMatch, lattice2);
        latticeMap.put(lattice2.toString(), lattice2);
        return lattice2;
      }
    }

    // No suitable existing lattice. Register this one.
    latticeMap.put(lattice.toString(), lattice);
    return lattice;
  }

  /** Copies measures and column usages from an existing lattice into a builder,
   * using a mapper to translate old-to-new columns, so that the new lattice can
   * inherit from the old. */
  private static void copyMeasures(Lattice.Builder builder, Lattice lattice) {
    final Function<Lattice.Column, Lattice.Column> mapper =
        (Lattice.Column c) -> {
          if (c instanceof Lattice.BaseColumn) {
            Lattice.BaseColumn baseColumn = (Lattice.BaseColumn) c;
            Pair<Path, Integer> p = lattice.columnToPathOffset(baseColumn);
            return builder.pathOffsetToColumn(p.left, p.right);
          } else {
            final Lattice.DerivedColumn derivedColumn = (Lattice.DerivedColumn) c;
            return builder.expression(derivedColumn.e, derivedColumn.alias,
                derivedColumn.tables);
          }
        };
    for (Lattice.Measure measure : lattice.defaultMeasures) {
      builder.addMeasure(measure.copy(mapper));
    }
    for (Map.Entry<Integer, Boolean> entry : lattice.columnUses.entries()) {
      final Lattice.Column column = lattice.columns.get(entry.getKey());
      builder.use(mapper.apply(column), entry.getValue());
    }
  }

  private static int matchQuality(Lattice lattice, Lattice target) {
    if (!lattice.rootNode.table.equals(target.rootNode.table)) {
      return 0;
    }
    if (lattice.rootNode.paths.equals(target.rootNode.paths)) {
      return 3;
    }
    if (lattice.rootNode.paths.containsAll(target.rootNode.paths)) {
      return 2;
    }
    return 1;
  }

  private static <E> Set<E> minus(Collection<E> c, Collection<E> c2) {
    final LinkedHashSet<E> c3 = new LinkedHashSet<>(c);
    c3.removeAll(c2);
    return c3;
  }

  private static void frames(List<Frame> frames, final Query q, RelNode r) {
    if (r instanceof SetOp) {
      r.getInputs().forEach(input -> frames(frames, q, input));
    } else {
      final Frame frame = frame(q, r);
      if (frame != null) {
        frames.add(frame);
      }
    }
  }

  private static @Nullable Frame frame(final Query q, RelNode r) {
    if (r instanceof Sort) {
      final Sort sort = (Sort) r;
      return frame(q, sort.getInput());
    } else if (r instanceof Filter) {
      final Filter filter = (Filter) r;
      return frame(q, filter.getInput());
    } else if (r instanceof Aggregate) {
      final Aggregate aggregate = (Aggregate) r;
      final Frame h = frame(q, aggregate.getInput());
      if (h == null) {
        return null;
      }
      final List<MutableMeasure> measures = new ArrayList<>();
      for (AggregateCall call : aggregate.getAggCallList()) {
        String name =
            SqlValidatorUtil.uniquify(call.name, q.usedNames, SqlValidatorUtil.ATTEMPT_SUGGESTER);
        measures.add(
            new MutableMeasure(call.getAggregation(), call.isDistinct(),
                Util.<Integer, @Nullable ColRef>transform(call.getArgList(), h::column),
                name));
      }
      final int fieldCount = r.getRowType().getFieldCount();
      return new Frame(fieldCount, h.hops, measures, ImmutableList.of(h)) {
        @Override @Nullable ColRef column(int offset) {
          if (offset < aggregate.getGroupSet().cardinality()) {
            return h.column(aggregate.getGroupSet().nth(offset));
          }
          return null; // an aggregate function; no direct mapping
        }
      };
    } else if (r instanceof Project) {
      final Project project = (Project) r;
      final Frame h = frame(q, project.getInput());
      if (h == null) {
        return null;
      }
      final int fieldCount = r.getRowType().getFieldCount();
      return new Frame(fieldCount, h.hops, h.measures, ImmutableList.of(h)) {
        final List<@Nullable ColRef> columns;

        {
          final ImmutableNullableList.Builder<@Nullable ColRef> columnBuilder =
              ImmutableNullableList.builder();
          for (Pair<RexNode, String> p : project.getNamedProjects()) {
            @SuppressWarnings("method.invocation.invalid")
            ColRef colRef = toColRef(p.left, p.right);
            columnBuilder.add(colRef);
          }
          columns = columnBuilder.build();
        }

        @Override @Nullable ColRef column(int offset) {
          return columns.get(offset);
        }

        /** Converts an expression to a base or derived column reference.
         * The alias is optional, but if the derived column reference becomes
         * a dimension or measure, the alias will be used to choose a name. */
        private @Nullable ColRef toColRef(RexNode e, String alias) {
          if (e instanceof RexInputRef) {
            return h.column(((RexInputRef) e).getIndex());
          }

          final ImmutableBitSet bits = RelOptUtil.InputFinder.bits(e);
          final ImmutableList.Builder<TableRef> tableRefs =
              ImmutableList.builder();
          int c = 0; // offset within lattice of first column in a table
          for (TableRef tableRef : h.tableRefs) {
            final int prev = c;
            c += tableRef.table.t.getRowType().getFieldCount();
            if (bits.intersects(ImmutableBitSet.range(prev, c))) {
              tableRefs.add(tableRef);
            }
          }
          final List<TableRef> tableRefList = tableRefs.build();
          switch (tableRefList.size()) {
          case 1:
            return new SingleTableDerivedColRef(tableRefList.get(0), e, alias);
          default:
            return new DerivedColRef(tableRefList, e, alias);
          }
        }
      };
    } else if (r instanceof Join) {
      final Join join = (Join) r;
      final int leftCount = join.getLeft().getRowType().getFieldCount();
      final Frame left = frame(q, join.getLeft());
      final Frame right = frame(q, join.getRight());
      if (left == null || right == null) {
        return null;
      }
      final ImmutableList.Builder<Hop> builder = ImmutableList.builder();
      builder.addAll(left.hops);
      for (IntPair p : join.analyzeCondition().pairs()) {
        final ColRef source = left.column(p.source);
        final ColRef target = right.column(p.target);
        assert source instanceof SingleTableColRef;
        assert target instanceof SingleTableColRef;
        builder.add(
            new Hop((SingleTableColRef) source,
                (SingleTableColRef) target));
      }
      builder.addAll(right.hops);
      final int fieldCount = r.getRowType().getFieldCount();
      return new Frame(fieldCount, builder.build(),
          CompositeList.of(left.measures, right.measures),
          ImmutableList.of(left, right)) {
        @Override @Nullable ColRef column(int offset) {
          if (offset < leftCount) {
            return left.column(offset);
          } else {
            return right.column(offset - leftCount);
          }
        }
      };
    } else if (r instanceof TableScan) {
      final TableScan scan = (TableScan) r;
      final TableRef tableRef = q.tableRef(scan);
      final int fieldCount = r.getRowType().getFieldCount();
      return new Frame(fieldCount, ImmutableList.of(),
          ImmutableList.of(), ImmutableSet.of(tableRef)) {
        @Override ColRef column(int offset) {
          if (offset >= scan.getTable().getRowType().getFieldCount()) {
            throw new IndexOutOfBoundsException("field " + offset
                + " out of range in " + scan.getTable().getRowType());
          }
          return new BaseColRef(tableRef, offset);
        }
      };
    } else {
      return null;
    }
  }

  /** Holds state for a particular query graph. In particular table and step
   * references count from zero each query. */
  private static class Query {
    final LatticeSpace space;
    final Map<Integer, TableRef> tableRefs = new HashMap<>();
    int stepRefCount = 0;
    final Set<String> usedNames = new HashSet<>();

    Query(LatticeSpace space) {
      this.space = space;
    }

    TableRef tableRef(TableScan scan) {
      final TableRef r = tableRefs.get(scan.getId());
      if (r != null) {
        return r;
      }
      final LatticeTable t = space.register(scan.getTable());
      final TableRef r2 = new TableRef(t, tableRefs.size());
      tableRefs.put(scan.getId(), r2);
      return r2;
    }

    StepRef stepRef(TableRef source, TableRef target, List<IntPair> keys) {
      keys = LatticeSpace.sortUnique(keys);
      final Step h = Step.create(source.table, target.table, keys, space);
      if (h.isBackwards(space.statisticProvider)) {
        final List<IntPair> keys1 = LatticeSpace.swap(h.keys);
        final Step h2 = space.addEdge(h.target(), h.source(), keys1);
        return new StepRef(target, source, h2, stepRefCount++);
      } else {
        final Step h2 = space.addEdge(h.source(), h.target(), h.keys);
        return new StepRef(source, target, h2, stepRefCount++);
      }
    }
  }

  /** Information about the parent of fields from a relational expression. */
  abstract static class Frame {
    final List<Hop> hops;
    final List<MutableMeasure> measures;
    final Set<TableRef> tableRefs;
    final int columnCount;

    Frame(int columnCount, List<Hop> hops, List<MutableMeasure> measures,
          Collection<TableRef> tableRefs) {
      this.hops = ImmutableList.copyOf(hops);
      this.measures = ImmutableList.copyOf(measures);
      this.tableRefs = ImmutableSet.copyOf(tableRefs);
      this.columnCount = columnCount;
    }

    Frame(int columnCount, List<Hop> hops, List<MutableMeasure> measures,
          List<Frame> inputs) {
      this(columnCount, hops, measures, collectTableRefs(inputs, hops));
    }

    abstract @Nullable ColRef column(int offset);

    @Override public String toString() {
      return "Frame(" + hops + ")";
    }

    static Set<TableRef> collectTableRefs(List<Frame> inputs, List<Hop> hops) {
      final LinkedHashSet<TableRef> set = new LinkedHashSet<>();
      for (Hop hop : hops) {
        set.add(hop.source.tableRef());
        set.add(hop.target.tableRef());
      }
      for (Frame frame : inputs) {
        set.addAll(frame.tableRefs);
      }
      return set;
    }
  }

  /** Use of a table within a query. A table can be used more than once. */
  private static class TableRef {
    final LatticeTable table;
    private final int ordinalInQuery;

    private TableRef(LatticeTable table, int ordinalInQuery) {
      this.table = requireNonNull(table, "table");
      this.ordinalInQuery = ordinalInQuery;
    }

    @Override public int hashCode() {
      return ordinalInQuery;
    }

    @Override public boolean equals(@Nullable Object obj) {
      return this == obj
          || obj instanceof TableRef
          && ordinalInQuery == ((TableRef) obj).ordinalInQuery;
    }

    @Override public String toString() {
      return table + ":" + ordinalInQuery;
    }
  }

  /** Use of a step within a query. A step can be used more than once. */
  private static class StepRef extends DefaultEdge {
    final Step step;
    private final int ordinalInQuery;

    StepRef(TableRef source, TableRef target, Step step, int ordinalInQuery) {
      super(source, target);
      this.step = requireNonNull(step, "step");
      this.ordinalInQuery = ordinalInQuery;
    }

    @Override public int hashCode() {
      return ordinalInQuery;
    }

    @Override public boolean equals(@Nullable Object obj) {
      return this == obj
          || obj instanceof StepRef
          && ((StepRef) obj).ordinalInQuery == ordinalInQuery;
    }

    @Override public String toString() {
      return "StepRef(" + source + ", " + target + "," + step.keyString + "):"
          + ordinalInQuery;
    }

    TableRef source() {
      return (TableRef) source;
    }

    TableRef target() {
      return (TableRef) target;
    }

    /** Creates {@link StepRef} instances. */
    private static class Factory
        implements AttributedDirectedGraph.AttributedEdgeFactory<
            TableRef, StepRef> {
      @Override public StepRef createEdge(TableRef source, TableRef target) {
        throw new UnsupportedOperationException();
      }

      @Override public StepRef createEdge(TableRef source, TableRef target,
            Object... attributes) {
        final Step step = (Step) attributes[0];
        final Integer ordinalInQuery = (Integer) attributes[1];
        return new StepRef(source, target, step, ordinalInQuery);
      }
    }
  }

  /** A hop is a join condition. One or more hops between the same source and
   * target combine to form a {@link Step}.
   *
   * <p>The tables are registered but the step is not. After we have gathered
   * several join conditions we may discover that the keys are composite: e.g.
   *
   * <blockquote>
   *   <pre>
   *     x.a = y.a
   *     AND x.b = z.b
   *     AND x.c = y.c
   *   </pre>
   * </blockquote>
   *
   * <p>has 3 semi-hops:
   *
   * <ul>
   *   <li>x.a = y.a
   *   <li>x.b = z.b
   *   <li>x.c = y.c
   * </ul>
   *
   * <p>which turn into 2 steps, the first of which is composite:
   *
   * <ul>
   *   <li>x.[a, c] = y.[a, c]
   *   <li>x.b = z.b
   * </ul>
   */
  private static class Hop {
    final SingleTableColRef source;
    final SingleTableColRef target;

    private Hop(SingleTableColRef source, SingleTableColRef target) {
      this.source = source;
      this.target = target;
    }
  }

  /** Column reference. */
  private abstract static class ColRef {
  }

  /** Column reference that is within a single table. */
  private interface SingleTableColRef {
    TableRef tableRef();

    int col(LatticeSpace space);
  }

  /** Reference to a base column. */
  private static class BaseColRef extends ColRef implements SingleTableColRef {
    final TableRef t;
    final int c;

    private BaseColRef(TableRef t, int c) {
      this.t = t;
      this.c = c;
    }

    @Override public TableRef tableRef() {
      return t;
    }

    @Override public int col(LatticeSpace space) {
      return c;
    }
  }

  /** Reference to a derived column (that is, an expression). */
  private static class DerivedColRef extends ColRef {
    final List<TableRef> tableRefs;
    final RexNode e;
    final String alias;

    DerivedColRef(Iterable<TableRef> tableRefs, RexNode e, String alias) {
      this.tableRefs = ImmutableList.copyOf(tableRefs);
      this.e = e;
      this.alias = alias;
    }

    List<String> tableAliases() {
      return Util.transform(tableRefs, tableRef -> tableRef.table.alias);
    }
  }

  /** Variant of {@link DerivedColRef} where all referenced expressions are in
   * the same table. */
  private static class SingleTableDerivedColRef extends DerivedColRef
      implements SingleTableColRef {
    SingleTableDerivedColRef(TableRef tableRef, RexNode e, String alias) {
      super(ImmutableList.of(tableRef), e, alias);
    }

    @Override public TableRef tableRef() {
      return tableRefs.get(0);
    }

    @Override public int col(LatticeSpace space) {
      return space.registerExpression(tableRef().table, e);
    }
  }

  /** An aggregate call. Becomes a measure in the final lattice. */
  private static class MutableMeasure {
    final SqlAggFunction aggregate;
    final boolean distinct;
    final List<? extends @Nullable ColRef> arguments;
    final @Nullable String name;

    private MutableMeasure(SqlAggFunction aggregate, boolean distinct,
        List<? extends @Nullable ColRef> arguments, @Nullable String name) {
      this.aggregate = aggregate;
      this.arguments = arguments;
      this.distinct = distinct;
      this.name = name;
    }
  }

}
