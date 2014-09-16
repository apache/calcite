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
package net.hydromatic.optiq.materialize;

import net.hydromatic.optiq.Schemas;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.MaterializedViewTable;
import net.hydromatic.optiq.impl.StarTable;
import net.hydromatic.optiq.jdbc.OptiqPrepare;
import net.hydromatic.optiq.jdbc.OptiqSchema;
import net.hydromatic.optiq.prepare.OptiqPrepareImpl;
import net.hydromatic.optiq.runtime.Utilities;
import net.hydromatic.optiq.util.BitSets;
import net.hydromatic.optiq.util.graph.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.rex.*;
import org.eigenbase.sql.SqlAggFunction;
import org.eigenbase.sql.SqlDialect;
import org.eigenbase.sql.SqlJoin;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlSelect;
import org.eigenbase.sql.SqlUtil;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.validate.SqlValidatorUtil;
import org.eigenbase.util.Util;
import org.eigenbase.util.mapping.IntPair;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;

import java.util.*;

/**
 * Structure that allows materialized views based upon a star schema to be
 * recognized and recommended.
 */
public class Lattice {
  private static final Function<Column, String> GET_ALIAS =
      new Function<Column, String>() {
        public String apply(Column input) {
          return input.alias;
        }
      };

  private static final Function<Column, Integer> GET_ORDINAL =
      new Function<Column, Integer>() {
        public Integer apply(Column input) {
          return input.ordinal;
        }
      };

  public final ImmutableList<Node> nodes;
  public final ImmutableList<Column> columns;
  public final boolean auto;
  public final ImmutableList<Measure> defaultMeasures;
  public final ImmutableList<Tile> tiles;
  public final ImmutableList<String> uniqueColumnNames;

  private final Function<Integer, Column> toColumnFunction =
      new Function<Integer, Column>() {
        public Column apply(Integer input) {
          return columns.get(input);
        }
      };
  private final Function<AggregateCall, Measure> toMeasureFunction =
      new Function<AggregateCall, Measure>() {
        public Measure apply(AggregateCall input) {
          return new Measure(input.getAggregation(),
              Lists.transform(input.getArgList(), toColumnFunction));
        }
      };

  private Lattice(ImmutableList<Node> nodes, boolean auto,
      ImmutableList<Column> columns,
      ImmutableList<Measure> defaultMeasures, ImmutableList<Tile> tiles) {
    this.nodes = Preconditions.checkNotNull(nodes);
    this.columns = Preconditions.checkNotNull(columns);
    this.auto = auto;
    this.defaultMeasures = Preconditions.checkNotNull(defaultMeasures);
    this.tiles = Preconditions.checkNotNull(tiles);

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

    List<String> nameList = Lists.newArrayList();
    for (Column column : columns) {
      nameList.add(column.alias);
    }
    uniqueColumnNames =
        ImmutableList.copyOf(
            SqlValidatorUtil.uniquify(Lists.transform(columns, GET_ALIAS)));
  }

  /** Creates a Lattice. */
  public static Lattice create(OptiqSchema schema, String sql, boolean auto) {
    return builder(schema, sql, auto).build();
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
    if (nodes.isEmpty() && rel instanceof ProjectRel) {
      return populate(nodes, tempLinks, ((ProjectRel) rel).getChild());
    }
    if (rel instanceof TableAccessRelBase) {
      nodes.add(rel);
      return true;
    }
    if (rel instanceof JoinRel) {
      JoinRel join = (JoinRel) rel;
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

  public String sql(BitSet groupSet, List<Measure> aggCallList) {
    final BitSet columnSet = (BitSet) groupSet.clone();
    for (Measure call : aggCallList) {
      for (Column arg : call.args) {
        columnSet.set(arg.ordinal);
      }
    }
    // Figure out which nodes are needed. Use a node if its columns are used
    // or if has a child whose columns are used.
    List<Node> usedNodes = Lists.newArrayList();
    for (Node node : nodes) {
      if (BitSets.range(node.startCol, node.endCol).intersects(columnSet)) {
        use(usedNodes, node);
      }
    }
    final SqlDialect dialect = SqlDialect.DatabaseProduct.OPTIQ.getDialect();
    final StringBuilder buf = new StringBuilder("SELECT ");
    final StringBuilder groupBuf = new StringBuilder("\nGROUP BY ");
    int k = 0;
    final Set<String> columnNames = Sets.newHashSet();
    for (int i : BitSets.toIter(groupSet)) {
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
    if (OptiqPrepareImpl.DEBUG) {
      System.out.println("Lattice SQL:\n" + buf);
    }
    buf.append(groupBuf);
    return buf.toString();
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
    final List<Table> tables = Lists.newArrayList();
    for (Node node : nodes) {
      tables.add(node.scan.getTable().unwrap(Table.class));
    }
    return StarTable.of(this, tables);
  }

  public static Builder builder(OptiqSchema optiqSchema, String sql,
      boolean auto) {
    return new Builder(optiqSchema, sql, auto);
  }

  public List<Measure> toMeasures(List<AggregateCall> aggCallList) {
    return Lists.transform(aggCallList, toMeasureFunction);
  }

  /** Source relation of a lattice.
   *
   * <p>Relations form a tree; all relations except the root relation
   * (the fact table) have precisely one parent and an equi-join
   * condition on one or more pairs of columns linking to it. */
  public static class Node {
    public final TableAccessRelBase scan;
    public final Node parent;
    public final ImmutableList<IntPair> link;
    public final int startCol;
    public final int endCol;
    public final String alias;

    public Node(TableAccessRelBase scan, Node parent, List<IntPair> link,
        int startCol, int endCol, String alias) {
      this.scan = Preconditions.checkNotNull(scan);
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
        new DirectedGraph.EdgeFactory<RelNode, Edge>() {
          public Edge createEdge(RelNode source, RelNode target) {
            return new Edge(source, target);
          }
        };

    final List<IntPair> pairs = Lists.newArrayList();

    public Edge(RelNode source, RelNode target) {
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
    public final Aggregation agg;
    public final ImmutableList<Column> args;

    public Measure(Aggregation agg, Iterable<Column> args) {
      this.agg = Preconditions.checkNotNull(agg);
      this.args = ImmutableList.copyOf(args);
    }

    public int compareTo(Measure measure) {
      int c = agg.getName().compareTo(measure.agg.getName());
      if (c != 0) {
        return c;
      }
      return compare(args, measure.args);
    }

    @Override public int hashCode() {
      return Util.hashV(agg, args);
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof Measure
          && this.agg == ((Measure) obj).agg
          && this.args.equals(((Measure) obj).args);
    }

    /** Returns the set of distinct argument ordinals. */
    public BitSet argBitSet() {
      final BitSet bitSet = new BitSet();
      for (Column arg : args) {
        bitSet.set(arg.ordinal);
      }
      return bitSet;
    }

    /** Returns a list of argument ordinals. */
    public List<Integer> argOrdinals() {
      return Lists.transform(args, GET_ORDINAL);
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
      this.table = Preconditions.checkNotNull(table);
      this.column = Preconditions.checkNotNull(column);
      this.alias = Preconditions.checkNotNull(alias);
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

    public List<String> identifiers() {
      return ImmutableList.of(table, column);
    }
  }

  /** Lattice builder. */
  public static class Builder {
    private final boolean auto;
    private final List<Node> nodes = Lists.newArrayList();
    private final ImmutableList<Column> columns;
    private final ImmutableListMultimap<String, Column> columnsByAlias;
    private final ImmutableList.Builder<Measure> defaultMeasureListBuilder =
        ImmutableList.builder();
    private final ImmutableList.Builder<Tile> tileListBuilder =
        ImmutableList.builder();

    public Builder(OptiqSchema schema, String sql, boolean auto) {
      this.auto = auto;

      OptiqPrepare.ConvertResult parsed =
          Schemas.convert(MaterializedViewTable.MATERIALIZATION_CONNECTION,
              schema, schema.path(null), sql);

      // Walk the join tree.
      List<RelNode> relNodes = Lists.newArrayList();
      List<int[][]> tempLinks = Lists.newArrayList();
      populate(relNodes, tempLinks, parsed.relNode);

      // Get aliases.
      List<String> aliases = Lists.newArrayList();
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
      final Map<RelNode, Node> map = Maps.newIdentityHashMap();
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
          node = new Node((TableAccessRelBase) relNode, null, null,
              previousColumn, column, aliases.get(nodes.size()));
        } else {
          if (edges.size() != 1) {
            throw new RuntimeException(
                "child node must have precisely one parent: " + relNode);
          }
          final Edge edge = edges.get(0);
          node = new Node((TableAccessRelBase) relNode,
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

    public Lattice build() {
      return new Lattice(ImmutableList.copyOf(nodes), auto, columns,
          defaultMeasureListBuilder.build(), tileListBuilder.build());
    }

    /** Resolves the arguments of a
     * {@link net.hydromatic.optiq.model.JsonMeasure}. They must either be null,
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
    public final BitSet bitSet = new BitSet();

    public Tile(ImmutableList<Measure> measures,
        ImmutableList<Column> dimensions) {
      this.measures = measures;
      this.dimensions = dimensions;
      assert Util.isStrictlySorted(dimensions);
      assert Util.isStrictlySorted(measures);
      for (Column dimension : dimensions) {
        bitSet.set(dimension.ordinal);
      }
    }

    public static TileBuilder builder() {
      return new TileBuilder();
    }

    public BitSet bitSet() {
      return bitSet;
    }
  }

  /** Tile builder. */
  public static class TileBuilder {
    private final ImmutableList.Builder<Measure> measureBuilder =
        ImmutableList.builder();
    private final ImmutableList.Builder<Column> dimensionListBuilder =
        ImmutableList.builder();

    public Tile build() {
      return new Tile(
          ImmutableList.copyOf(Util.sort(measureBuilder.build())),
          ImmutableList.copyOf(Util.sort(dimensionListBuilder.build())));
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
