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

import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Utilities;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.MaterializedViewTable;
import org.apache.calcite.schema.impl.StarTable;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.graph.DefaultDirectedGraph;
import org.apache.calcite.util.graph.DefaultEdge;
import org.apache.calcite.util.graph.DirectedGraph;
import org.apache.calcite.util.graph.TopologicalOrderIterator;
import org.apache.calcite.util.mapping.IntPair;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Structure that allows materialized views based upon a star schema to be
 * recognized and recommended.
 */
public class Lattice {
  public final CalciteSchema rootSchema;
  public final ImmutableList<Node> nodes;
  public final ImmutableList<Column> columns;
  public final boolean auto;
  public final boolean algorithm;
  public final long algorithmMaxMillis;
  public final double rowCountEstimate;
  public final ImmutableList<Measure> defaultMeasures;
  public final ImmutableList<Tile> tiles;
  public final ImmutableList<String> uniqueColumnNames;
  public final LatticeStatisticProvider statisticProvider;

  private Lattice(CalciteSchema rootSchema, ImmutableList<Node> nodes,
      boolean auto, boolean algorithm, long algorithmMaxMillis,
      LatticeStatisticProvider.Factory statisticProviderFactory,
      Double rowCountEstimate, ImmutableList<Column> columns,
      ImmutableList<Measure> defaultMeasures, ImmutableList<Tile> tiles) {
    this.rootSchema = rootSchema;
    this.nodes = Objects.requireNonNull(nodes);
    this.columns = Objects.requireNonNull(columns);
    this.auto = auto;
    this.algorithm = algorithm;
    this.algorithmMaxMillis = algorithmMaxMillis;
    this.defaultMeasures = Objects.requireNonNull(defaultMeasures);
    this.tiles = Objects.requireNonNull(tiles);

    // Validate that nodes form a tree; each node except the first references
    // a predecessor.
    for (int i = 0; i < nodes.size(); i++) {
      Node node = nodes.get(i);
      if (i == 0) {
        assert node.parent == null;
      } else {
        assert nodes.subList(0, i).contains(node.parent);
      }
    }

    uniqueColumnNames =
        ImmutableList.copyOf(
            SqlValidatorUtil.uniquify(
                Lists.transform(columns, input -> input.alias), true));
    if (rowCountEstimate == null) {
      // We could improve this when we fix
      // [CALCITE-429] Add statistics SPI for lattice optimization algorithm
      rowCountEstimate = 1000d;
    }
    Preconditions.checkArgument(rowCountEstimate > 0d);
    this.rowCountEstimate = rowCountEstimate;
    this.statisticProvider =
        Objects.requireNonNull(statisticProviderFactory.apply(this));
  }

  /** Creates a Lattice. */
  public static Lattice create(CalciteSchema schema, String sql, boolean auto) {
    return builder(schema, sql).auto(auto).build();
  }

  private static void populateAliases(SqlNode from, List<String> aliases,
      String current) {
    if (from instanceof SqlJoin) {
      SqlJoin join = (SqlJoin) from;
      populateAliases(join.getLeft(), aliases, null);
      populateAliases(join.getRight(), aliases, null);
    } else if (from.getKind() == SqlKind.AS) {
      populateAliases(SqlUtil.stripAs(from), aliases,
          SqlValidatorUtil.getAlias(from, -1));
    } else {
      if (current == null) {
        current = SqlValidatorUtil.getAlias(from, -1);
      }
      aliases.add(current);
    }
  }

  private static boolean populate(List<RelNode> nodes, List<int[][]> tempLinks,
      RelNode rel) {
    if (nodes.isEmpty() && rel instanceof LogicalProject) {
      return populate(nodes, tempLinks, ((LogicalProject) rel).getInput());
    }
    if (rel instanceof TableScan) {
      nodes.add(rel);
      return true;
    }
    if (rel instanceof LogicalJoin) {
      LogicalJoin join = (LogicalJoin) rel;
      if (join.getJoinType() != JoinRelType.INNER) {
        throw new RuntimeException("only inner join allowed, but got "
            + join.getJoinType());
      }
      populate(nodes, tempLinks, join.getLeft());
      populate(nodes, tempLinks, join.getRight());
      for (RexNode rex : RelOptUtil.conjunctions(join.getCondition())) {
        tempLinks.add(grab(nodes, rex));
      }
      return true;
    }
    throw new RuntimeException("Invalid node type "
        + rel.getClass().getSimpleName() + " in lattice query");
  }

  /** Converts an "t1.c1 = t2.c2" expression into two (input, field) pairs. */
  private static int[][] grab(List<RelNode> leaves, RexNode rex) {
    switch (rex.getKind()) {
    case EQUALS:
      break;
    default:
      throw new AssertionError("only equi-join allowed");
    }
    final List<RexNode> operands = ((RexCall) rex).getOperands();
    return new int[][] {
        inputField(leaves, operands.get(0)),
        inputField(leaves, operands.get(1))};
  }

  /** Converts an expression into an (input, field) pair. */
  private static int[] inputField(List<RelNode> leaves, RexNode rex) {
    if (!(rex instanceof RexInputRef)) {
      throw new RuntimeException("only equi-join of columns allowed: " + rex);
    }
    RexInputRef ref = (RexInputRef) rex;
    int start = 0;
    for (int i = 0; i < leaves.size(); i++) {
      final RelNode leaf = leaves.get(i);
      final int end = start + leaf.getRowType().getFieldCount();
      if (ref.getIndex() < end) {
        return new int[] {i, ref.getIndex() - start};
      }
      start = end;
    }
    throw new AssertionError("input not found");
  }

  /** Generates a SQL query to populate a tile of the lattice specified by a
   * given set of columns and measures. */
  public String sql(ImmutableBitSet groupSet, List<Measure> aggCallList) {
    return sql(groupSet, true, aggCallList);
  }

  /** Generates a SQL query to populate a tile of the lattice specified by a
   * given set of columns and measures, optionally grouping. */
  public String sql(ImmutableBitSet groupSet, boolean group,
      List<Measure> aggCallList) {
    final List<Node> usedNodes = new ArrayList<>();
    if (group) {
      final ImmutableBitSet.Builder columnSetBuilder = groupSet.rebuild();
      for (Measure call : aggCallList) {
        for (Column arg : call.args) {
          columnSetBuilder.set(arg.ordinal);
        }
      }
      final ImmutableBitSet columnSet = columnSetBuilder.build();

      // Figure out which nodes are needed. Use a node if its columns are used
      // or if has a child whose columns are used.
      for (Node node : nodes) {
        if (ImmutableBitSet.range(node.startCol, node.endCol)
            .intersects(columnSet)) {
          use(usedNodes, node);
        }
      }
      if (usedNodes.isEmpty()) {
        usedNodes.add(nodes.get(0));
      }
    } else {
      usedNodes.addAll(nodes);
    }

    final SqlDialect dialect = SqlDialect.DatabaseProduct.CALCITE.getDialect();
    final StringBuilder buf = new StringBuilder("SELECT ");
    final StringBuilder groupBuf = new StringBuilder("\nGROUP BY ");
    int k = 0;
    final Set<String> columnNames = new HashSet<>();
    if (groupSet != null) {
      for (int i : groupSet) {
        if (k++ > 0) {
          buf.append(", ");
          groupBuf.append(", ");
        }
        final Column column = columns.get(i);
        dialect.quoteIdentifier(buf, column.identifiers());
        dialect.quoteIdentifier(groupBuf, column.identifiers());
        final String fieldName = uniqueColumnNames.get(i);
        columnNames.add(fieldName);
        if (!column.alias.equals(fieldName)) {
          buf.append(" AS ");
          dialect.quoteIdentifier(buf, fieldName);
        }
      }
      if (groupSet.isEmpty()) {
        groupBuf.append("()");
      }
      int m = 0;
      for (Measure measure : aggCallList) {
        if (k++ > 0) {
          buf.append(", ");
        }
        buf.append(measure.agg.getName())
            .append("(");
        if (measure.args.isEmpty()) {
          buf.append("*");
        } else {
          int z = 0;
          for (Column arg : measure.args) {
            if (z++ > 0) {
              buf.append(", ");
            }
            dialect.quoteIdentifier(buf, arg.identifiers());
          }
        }
        buf.append(") AS ");
        String measureName;
        while (!columnNames.add(measureName = "m" + m)) {
          ++m;
        }
        dialect.quoteIdentifier(buf, measureName);
      }
    } else {
      buf.append("*");
    }
    buf.append("\nFROM ");
    for (Node node : usedNodes) {
      if (node.parent != null) {
        buf.append("\nJOIN ");
      }
      dialect.quoteIdentifier(buf, node.scan.getTable().getQualifiedName());
      buf.append(" AS ");
      dialect.quoteIdentifier(buf, node.alias);
      if (node.parent != null) {
        buf.append(" ON ");
        k = 0;
        for (IntPair pair : node.link) {
          if (k++ > 0) {
            buf.append(" AND ");
          }
          final Column left = columns.get(node.parent.startCol + pair.source);
          dialect.quoteIdentifier(buf, left.identifiers());
          buf.append(" = ");
          final Column right = columns.get(node.startCol + pair.target);
          dialect.quoteIdentifier(buf, right.identifiers());
        }
      }
    }
    if (CalcitePrepareImpl.DEBUG) {
      System.out.println("Lattice SQL:\n"
          + buf);
    }
    if (group) {
      buf.append(groupBuf);
    }
    return buf.toString();
  }

  /** Returns a SQL query that counts the number of distinct values of the
   * attributes given in {@code groupSet}. */
  public String countSql(ImmutableBitSet groupSet) {
    return "select count(*) as c from ("
        + sql(groupSet, ImmutableList.of())
        + ")";
  }

  private static void use(List<Node> usedNodes, Node node) {
    if (!usedNodes.contains(node)) {
      if (node.parent != null) {
        use(usedNodes, node.parent);
      }
      usedNodes.add(node);
    }
  }

  public StarTable createStarTable() {
    final List<Table> tables = new ArrayList<>();
    for (Node node : nodes) {
      tables.add(node.scan.getTable().unwrap(Table.class));
    }
    return StarTable.of(this, tables);
  }

  public static Builder builder(CalciteSchema calciteSchema, String sql) {
    return new Builder(calciteSchema, sql);
  }

  public List<Measure> toMeasures(List<AggregateCall> aggCallList) {
    return Lists.transform(aggCallList,
        call -> new Measure(call.getAggregation(),
        Lists.transform(call.getArgList(), columns::get)));
  }

  public Iterable<? extends Tile> computeTiles() {
    if (!algorithm) {
      return tiles;
    }
    return new TileSuggester(this).tiles();
  }

  /** Returns an estimate of the number of rows in the un-aggregated star. */
  public double getFactRowCount() {
    return rowCountEstimate;
  }

  /** Returns an estimate of the number of rows in the tile with the given
   * dimensions. */
  public double getRowCount(List<Column> columns) {
    return statisticProvider.cardinality(columns);
  }

  /** Returns an estimate of the number of rows in the tile with the given
   * dimensions. */
  public static double getRowCount(double factCount, double... columnCounts) {
    return getRowCount(factCount, Primitive.asList(columnCounts));
  }

  /** Returns an estimate of the number of rows in the tile with the given
   * dimensions. */
  public static double getRowCount(double factCount,
      List<Double> columnCounts) {
    // The expected number of distinct values when choosing p values
    // with replacement from n integers is n . (1 - ((n - 1) / n) ^ p).
    //
    // If we have several uniformly distributed attributes A1 ... Am
    // with N1 ... Nm distinct values, they behave as one uniformly
    // distributed attribute with N1 * ... * Nm distinct values.
    double n = 1d;
    for (Double columnCount : columnCounts) {
      if (columnCount > 1d) {
        n *= columnCount;
      }
    }
    final double a = (n - 1d) / n;
    if (a == 1d) {
      // A under-flows if nn is large.
      return factCount;
    }
    final double v = n * (1d - Math.pow(a, factCount));
    // Cap at fact-row-count, because numerical artifacts can cause it
    // to go a few % over.
    return Math.min(v, factCount);
  }

  /** Source relation of a lattice.
   *
   * <p>Relations form a tree; all relations except the root relation
   * (the fact table) have precisely one parent and an equi-join
   * condition on one or more pairs of columns linking to it. */
  public static class Node {
    public final TableScan scan;
    public final Node parent;
    public final ImmutableList<IntPair> link;
    public final int startCol;
    public final int endCol;
    public final String alias;

    public Node(TableScan scan, Node parent, List<IntPair> link,
        int startCol, int endCol, String alias) {
      this.scan = Objects.requireNonNull(scan);
      this.parent = parent;
      this.link = link == null ? null : ImmutableList.copyOf(link);
      assert (parent == null) == (link == null);
      assert startCol >= 0;
      assert endCol > startCol;
      this.startCol = startCol;
      this.endCol = endCol;
      this.alias = alias;
    }
  }

  /** Edge in the temporary graph. */
  private static class Edge extends DefaultEdge {
    public static final DirectedGraph.EdgeFactory<RelNode, Edge> FACTORY =
        Edge::new;

    final List<IntPair> pairs = new ArrayList<>();

    Edge(RelNode source, RelNode target) {
      super(source, target);
    }

    public RelNode getTarget() {
      return (RelNode) target;
    }

    public RelNode getSource() {
      return (RelNode) source;
    }
  }

  /** Measure in a lattice. */
  public static class Measure implements Comparable<Measure> {
    public final SqlAggFunction agg;
    public final ImmutableList<Column> args;

    public Measure(SqlAggFunction agg, Iterable<Column> args) {
      this.agg = Objects.requireNonNull(agg);
      this.args = ImmutableList.copyOf(args);
    }

    public int compareTo(Measure measure) {
      int c = agg.getName().compareTo(measure.agg.getName());
      if (c != 0) {
        return c;
      }
      return compare(args, measure.args);
    }

    @Override public String toString() {
      return "Measure: [agg: " + agg + ", args: " + args + "]";
    }

    @Override public int hashCode() {
      return Objects.hash(agg, args);
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof Measure
          && this.agg.equals(((Measure) obj).agg)
          && this.args.equals(((Measure) obj).args);
    }

    /** Returns the set of distinct argument ordinals. */
    public ImmutableBitSet argBitSet() {
      final ImmutableBitSet.Builder bitSet = ImmutableBitSet.builder();
      for (Column arg : args) {
        bitSet.set(arg.ordinal);
      }
      return bitSet.build();
    }

    /** Returns a list of argument ordinals. */
    public List<Integer> argOrdinals() {
      return Lists.transform(args, input -> input.ordinal);
    }

    private static int compare(List<Column> list0, List<Column> list1) {
      final int size = Math.min(list0.size(), list1.size());
      for (int i = 0; i < size; i++) {
        final int o0 = list0.get(i).ordinal;
        final int o1 = list1.get(i).ordinal;
        final int c = Utilities.compare(o0, o1);
        if (c != 0) {
          return c;
        }
      }
      return Utilities.compare(list0.size(), list1.size());
    }
  }

  /** Column in a lattice. Columns are identified by table alias and
   * column name, and may have an additional alias that is unique
   * within the entire lattice. */
  public static class Column implements Comparable<Column> {
    public final int ordinal;
    public final String table;
    public final String column;
    public final String alias;

    private Column(int ordinal, String table, String column, String alias) {
      this.ordinal = ordinal;
      this.table = Objects.requireNonNull(table);
      this.column = Objects.requireNonNull(column);
      this.alias = Objects.requireNonNull(alias);
    }

    /** Converts a list of columns to a bit set of their ordinals. */
    static ImmutableBitSet toBitSet(List<Column> columns) {
      final ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
      for (Column column : columns) {
        builder.set(column.ordinal);
      }
      return builder.build();
    }

    public int compareTo(Column column) {
      return Utilities.compare(ordinal, column.ordinal);
    }

    @Override public int hashCode() {
      return ordinal;
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof Column
          && this.ordinal == ((Column) obj).ordinal;
    }

    @Override public String toString() {
      return identifiers().toString();
    }

    public List<String> identifiers() {
      return ImmutableList.of(table, column);
    }
  }

  /** Lattice builder. */
  public static class Builder {
    private final List<Node> nodes = new ArrayList<>();
    private final ImmutableList<Column> columns;
    private final ImmutableListMultimap<String, Column> columnsByAlias;
    private final ImmutableList.Builder<Measure> defaultMeasureListBuilder =
        ImmutableList.builder();
    private final ImmutableList.Builder<Tile> tileListBuilder =
        ImmutableList.builder();
    private final CalciteSchema rootSchema;
    private boolean algorithm = false;
    private long algorithmMaxMillis = -1;
    private boolean auto = true;
    private Double rowCountEstimate;
    private String statisticProvider;

    public Builder(CalciteSchema schema, String sql) {
      this.rootSchema = Objects.requireNonNull(schema.root());
      Preconditions.checkArgument(rootSchema.isRoot(), "must be root schema");
      CalcitePrepare.ConvertResult parsed =
          Schemas.convert(MaterializedViewTable.MATERIALIZATION_CONNECTION,
              schema, schema.path(null), sql);

      // Walk the join tree.
      List<RelNode> relNodes = new ArrayList<>();
      List<int[][]> tempLinks = new ArrayList<>();
      populate(relNodes, tempLinks, parsed.root.rel);

      // Get aliases.
      List<String> aliases = new ArrayList<>();
      populateAliases(((SqlSelect) parsed.sqlNode).getFrom(), aliases, null);

      // Build a graph.
      final DirectedGraph<RelNode, Edge> graph =
          DefaultDirectedGraph.create(Edge.FACTORY);
      for (RelNode node : relNodes) {
        graph.addVertex(node);
      }
      for (int[][] tempLink : tempLinks) {
        final RelNode source = relNodes.get(tempLink[0][0]);
        final RelNode target = relNodes.get(tempLink[1][0]);
        Edge edge = graph.getEdge(source, target);
        if (edge == null) {
          edge = graph.addEdge(source, target);
        }
        edge.pairs.add(IntPair.of(tempLink[0][1], tempLink[1][1]));
      }

      // Convert the graph into a tree of nodes, each connected to a parent and
      // with a join condition to that parent.
      Node previous = null;
      final Map<RelNode, Node> map = new IdentityHashMap<>();
      int previousColumn = 0;
      for (RelNode relNode : TopologicalOrderIterator.of(graph)) {
        final List<Edge> edges = graph.getInwardEdges(relNode);
        Node node;
        final int column = previousColumn
            + relNode.getRowType().getFieldCount();
        if (previous == null) {
          if (!edges.isEmpty()) {
            throw new RuntimeException("root node must not have relationships: "
                + relNode);
          }
          node = new Node((TableScan) relNode, null, null,
              previousColumn, column, aliases.get(nodes.size()));
        } else {
          if (edges.size() != 1) {
            throw new RuntimeException(
                "child node must have precisely one parent: " + relNode);
          }
          final Edge edge = edges.get(0);
          node = new Node((TableScan) relNode,
              map.get(edge.getSource()), edge.pairs, previousColumn, column,
              aliases.get(nodes.size()));
        }
        nodes.add(node);
        map.put(relNode, node);
        previous = node;
        previousColumn = column;
      }

      final ImmutableList.Builder<Column> builder = ImmutableList.builder();
      final ImmutableListMultimap.Builder<String, Column> aliasBuilder =
          ImmutableListMultimap.builder();
      int c = 0;
      for (Node node : nodes) {
        if (node.scan != null) {
          for (String name : node.scan.getRowType().getFieldNames()) {
            final Column column = new Column(c++, node.alias, name, name);
            builder.add(column);
            aliasBuilder.put(column.alias, column);
          }
        }
      }
      columns = builder.build();
      columnsByAlias = aliasBuilder.build();
    }

    /** Sets the "auto" attribute (default true). */
    public Builder auto(boolean auto) {
      this.auto = auto;
      return this;
    }

    /** Sets the "algorithm" attribute (default false). */
    public Builder algorithm(boolean algorithm) {
      this.algorithm = algorithm;
      return this;
    }

    /** Sets the "algorithmMaxMillis" attribute (default -1). */
    public Builder algorithmMaxMillis(long algorithmMaxMillis) {
      this.algorithmMaxMillis = algorithmMaxMillis;
      return this;
    }

    /** Sets the "rowCountEstimate" attribute (default null). */
    public Builder rowCountEstimate(double rowCountEstimate) {
      this.rowCountEstimate = rowCountEstimate;
      return this;
    }

    /** Sets the "statisticProvider" attribute.
     *
     * <p>If not set, the lattice will use {@link Lattices#CACHED_SQL}. */
    public Builder statisticProvider(String statisticProvider) {
      this.statisticProvider = statisticProvider;
      return this;
    }

    /** Builds a lattice. */
    public Lattice build() {
      LatticeStatisticProvider.Factory statisticProvider =
          this.statisticProvider != null
              ? AvaticaUtils.instantiatePlugin(
                  LatticeStatisticProvider.Factory.class,
                  this.statisticProvider)
              : Lattices.CACHED_SQL;
      Preconditions.checkArgument(rootSchema.isRoot(), "must be root schema");
      return new Lattice(rootSchema, ImmutableList.copyOf(nodes), auto,
          algorithm, algorithmMaxMillis, statisticProvider, rowCountEstimate,
          columns, defaultMeasureListBuilder.build(), tileListBuilder.build());
    }

    /** Resolves the arguments of a
     * {@link org.apache.calcite.model.JsonMeasure}. They must either be null,
     * a string, or a list of strings. Throws if the structure is invalid, or if
     * any of the columns do not exist in the lattice. */
    public ImmutableList<Column> resolveArgs(Object args) {
      if (args == null) {
        return ImmutableList.of();
      } else if (args instanceof String) {
        return ImmutableList.of(resolveColumnByAlias((String) args));
      } else if (args instanceof List) {
        final ImmutableList.Builder<Column> builder = ImmutableList.builder();
        for (Object o : (List) args) {
          if (o instanceof String) {
            builder.add(resolveColumnByAlias((String) o));
          } else {
            throw new RuntimeException(
                "Measure arguments must be a string or a list of strings; argument: "
                    + o);
          }
        }
        return builder.build();
      } else {
        throw new RuntimeException(
            "Measure arguments must be a string or a list of strings");
      }
    }

    /** Looks up a column in this lattice by alias. The alias must be unique
     * within the lattice.
     */
    private Column resolveColumnByAlias(String name) {
      final ImmutableList<Column> list = columnsByAlias.get(name);
      if (list == null || list.size() == 0) {
        throw new RuntimeException("Unknown lattice column '" + name + "'");
      } else if (list.size() == 1) {
        return list.get(0);
      } else {
        throw new RuntimeException("Lattice column alias '" + name
            + "' is not unique");
      }
    }

    public Column resolveColumn(Object name) {
      if (name instanceof String) {
        return resolveColumnByAlias((String) name);
      }
      if (name instanceof List) {
        List list = (List) name;
        switch (list.size()) {
        case 1:
          final Object alias = list.get(0);
          if (alias instanceof String) {
            return resolveColumnByAlias((String) alias);
          }
          break;
        case 2:
          final Object table = list.get(0);
          final Object column = list.get(1);
          if (table instanceof String && column instanceof String) {
            return resolveQualifiedColumn((String) table, (String) column);
          }
          break;
        }
      }
      throw new RuntimeException(
          "Lattice column reference must be a string or a list of 1 or 2 strings; column: "
              + name);
    }

    private Column resolveQualifiedColumn(String table, String column) {
      for (Column column1 : columns) {
        if (column1.table.equals(table)
            && column1.column.equals(column)) {
          return column1;
        }
      }
      throw new RuntimeException("Unknown lattice column [" + table + ", "
          + column + "]");
    }

    public Measure resolveMeasure(String aggName, Object args) {
      final SqlAggFunction agg = resolveAgg(aggName);
      final ImmutableList<Column> list = resolveArgs(args);
      return new Measure(agg, list);
    }

    private SqlAggFunction resolveAgg(String aggName) {
      if (aggName.equalsIgnoreCase("count")) {
        return SqlStdOperatorTable.COUNT;
      } else if (aggName.equalsIgnoreCase("sum")) {
        return SqlStdOperatorTable.SUM;
      } else {
        throw new RuntimeException("Unknown lattice aggregate function "
            + aggName);
      }
    }

    public void addMeasure(Measure measure) {
      defaultMeasureListBuilder.add(measure);
    }

    public void addTile(Tile tile) {
      tileListBuilder.add(tile);
    }
  }

  /** Materialized aggregate within a lattice. */
  public static class Tile {
    public final ImmutableList<Measure> measures;
    public final ImmutableList<Column> dimensions;
    public final ImmutableBitSet bitSet;

    public Tile(ImmutableList<Measure> measures,
        ImmutableList<Column> dimensions) {
      this.measures = Objects.requireNonNull(measures);
      this.dimensions = Objects.requireNonNull(dimensions);
      assert Ordering.natural().isStrictlyOrdered(dimensions);
      assert Ordering.natural().isStrictlyOrdered(measures);
      bitSet = Column.toBitSet(dimensions);
    }

    public static TileBuilder builder() {
      return new TileBuilder();
    }

    public ImmutableBitSet bitSet() {
      return bitSet;
    }
  }

  /** Tile builder. */
  public static class TileBuilder {
    private final List<Measure> measureBuilder = new ArrayList<>();
    private final List<Column> dimensionListBuilder = new ArrayList<>();

    public Tile build() {
      return new Tile(
          Ordering.natural().immutableSortedCopy(measureBuilder),
          Ordering.natural().immutableSortedCopy(dimensionListBuilder));
    }

    public void addMeasure(Measure measure) {
      measureBuilder.add(measure);
    }

    public void addDimension(Column column) {
      dimensionListBuilder.add(column);
    }
  }
}

// End Lattice.java
