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
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
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
import org.apache.calcite.statistic.MapSqlStatisticProvider;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.graph.DefaultDirectedGraph;
import org.apache.calcite.util.graph.DefaultEdge;
import org.apache.calcite.util.graph.DirectedGraph;
import org.apache.calcite.util.graph.TopologicalOrderIterator;
import org.apache.calcite.util.mapping.IntPair;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;

import org.checkerframework.checker.initialization.qual.UnknownInitialization;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.RequiresNonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.util.Objects.requireNonNull;

/**
 * Structure that allows materialized views based upon a star schema to be
 * recognized and recommended.
 */
public class Lattice {
  public final CalciteSchema rootSchema;
  public final LatticeRootNode rootNode;
  public final ImmutableList<Column> columns;
  public final boolean auto;
  public final boolean algorithm;
  public final long algorithmMaxMillis;
  public final double rowCountEstimate;
  public final ImmutableList<Measure> defaultMeasures;
  public final ImmutableList<Tile> tiles;
  public final ImmutableListMultimap<Integer, Boolean> columnUses;
  public final LatticeStatisticProvider statisticProvider;

  private Lattice(CalciteSchema rootSchema, LatticeRootNode rootNode,
      boolean auto, boolean algorithm, long algorithmMaxMillis,
      LatticeStatisticProvider.Factory statisticProviderFactory,
      @Nullable Double rowCountEstimate, ImmutableList<Column> columns,
      ImmutableSortedSet<Measure> defaultMeasures, ImmutableList<Tile> tiles,
      ImmutableListMultimap<Integer, Boolean> columnUses) {
    this.rootSchema = rootSchema;
    this.rootNode = requireNonNull(rootNode, "rootNode");
    this.columns = requireNonNull(columns, "columns");
    this.auto = auto;
    this.algorithm = algorithm;
    this.algorithmMaxMillis = algorithmMaxMillis;
    this.defaultMeasures = defaultMeasures.asList(); // unique and sorted
    this.tiles = requireNonNull(tiles, "tiles");
    this.columnUses = columnUses;

    assert isValid(Litmus.THROW);

    if (rowCountEstimate == null) {
      // We could improve this when we fix
      // [CALCITE-429] Add statistics SPI for lattice optimization algorithm
      rowCountEstimate = 1000d;
    }
    Preconditions.checkArgument(rowCountEstimate > 0d);
    this.rowCountEstimate = rowCountEstimate;
    @SuppressWarnings("argument.type.incompatible")
    LatticeStatisticProvider statisticProvider =
        requireNonNull(statisticProviderFactory.apply(this));
    this.statisticProvider = statisticProvider;
  }

  /** Creates a Lattice. */
  public static Lattice create(CalciteSchema schema, String sql, boolean auto) {
    return builder(schema, sql).auto(auto).build();
  }

  @RequiresNonNull({"rootNode", "defaultMeasures", "columns"})
  private boolean isValid(
      @UnknownInitialization Lattice this,
      Litmus litmus) {
    if (!rootNode.isValid(litmus)) {
      return false;
    }
    for (Measure measure : defaultMeasures) {
      for (Column arg : measure.args) {
        if (columns.get(arg.ordinal) != arg) {
          return litmus.fail("measure argument must be a column registered in"
              + " this lattice: {}", measure);
        }
      }
    }
    return litmus.succeed();
  }

  private static void populateAliases(SqlNode from, List<@Nullable String> aliases,
      @Nullable String current) {
    if (from instanceof SqlJoin) {
      SqlJoin join = (SqlJoin) from;
      populateAliases(join.getLeft(), aliases, null);
      populateAliases(join.getRight(), aliases, null);
    } else if (from.getKind() == SqlKind.AS) {
      populateAliases(SqlUtil.stripAs(from), aliases,
          SqlValidatorUtil.alias(from));
    } else {
      if (current == null) {
        current = SqlValidatorUtil.alias(from);
      }
      aliases.add(current);
    }
  }

  private static boolean populate(List<TableScan> nodes, List<int[][]> tempLinks,
      RelNode rel) {
    if (nodes.isEmpty() && rel instanceof LogicalProject) {
      return populate(nodes, tempLinks, ((LogicalProject) rel).getInput());
    }
    if (rel instanceof TableScan) {
      nodes.add((TableScan) rel);
      return true;
    }
    if (rel instanceof LogicalJoin) {
      LogicalJoin join = (LogicalJoin) rel;
      if (join.getJoinType().isOuterJoin()) {
        throw new RuntimeException("only non nulls-generating join allowed, but got "
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
  private static int[][] grab(List<TableScan> leaves, RexNode rex) {
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
  private static int[] inputField(List<TableScan> leaves, RexNode rex) {
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

  @Override public String toString() {
    return rootNode + ":" + defaultMeasures;
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
    final List<LatticeNode> usedNodes = new ArrayList<>();
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
      for (LatticeNode node : rootNode.descendants) {
        if (ImmutableBitSet.range(node.startCol, node.endCol)
            .intersects(columnSet)) {
          node.use(usedNodes);
        }

        if (usedNodes.isEmpty()) {
          usedNodes.add(rootNode);
        }
      }
    } else {
      usedNodes.addAll(rootNode.descendants);
    }

    final SqlDialect dialect = SqlDialect.DatabaseProduct.CALCITE.getDialect();
    final StringBuilder buf = new StringBuilder("SELECT ");
    final StringBuilder groupBuf = new StringBuilder("\nGROUP BY ");
    int k = 0;
    final Set<String> columnNames = new HashSet<>();
    final SqlWriter w = createSqlWriter(dialect, buf, f -> {
      throw new UnsupportedOperationException();
    });
    if (groupSet != null) {
      for (int i : groupSet) {
        if (k++ > 0) {
          buf.append(", ");
          groupBuf.append(", ");
        }
        final Column column = columns.get(i);
        column.toSql(w);
        column.toSql(w.with(groupBuf));
        if (column instanceof BaseColumn) {
          columnNames.add(((BaseColumn) column).column);
        }
        if (!column.alias.equals(column.defaultAlias())) {
          buf.append(" AS ");
          dialect.quoteIdentifier(buf, column.alias);
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
            arg.toSql(w);
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
    for (LatticeNode node : usedNodes) {
      if (node instanceof LatticeChildNode) {
        buf.append("\nJOIN ");
      }
      dialect.quoteIdentifier(buf, node.table.t.getQualifiedName());
      String alias = node.alias;
      if (alias != null) {
        buf.append(" AS ");
        dialect.quoteIdentifier(buf, alias);
      }
      if (node instanceof LatticeChildNode) {
        final LatticeChildNode node1 = (LatticeChildNode) node;
        buf.append(" ON ");
        k = 0;
        for (IntPair pair : node1.link) {
          if (k++ > 0) {
            buf.append(" AND ");
          }
          final Column left = columns.get(node1.parent.startCol + pair.source);
          left.toSql(w);
          buf.append(" = ");
          final Column right = columns.get(node.startCol + pair.target);
          right.toSql(w);
        }
      }
    }
    if (CalciteSystemProperty.DEBUG.value()) {
      System.out.println("Lattice SQL:\n"
          + buf);
    }
    if (group) {
      if (groupSet.isEmpty()) {
        groupBuf.append("()");
      }
      buf.append(groupBuf);
    }
    return buf.toString();
  }

  /** Creates a context to which SQL can be generated. */
  public SqlWriter createSqlWriter(SqlDialect dialect, StringBuilder buf,
      IntFunction<SqlNode> field) {
    return new SqlWriter(this, dialect, buf,
        new SqlImplementor.SimpleContext(dialect, field));
  }

  /** Returns a SQL query that counts the number of distinct values of the
   * attributes given in {@code groupSet}. */
  public String countSql(ImmutableBitSet groupSet) {
    return "select count(*) as c from ("
        + sql(groupSet, ImmutableList.of())
        + ")";
  }

  public StarTable createStarTable() {
    final List<Table> tables = new ArrayList<>();
    for (LatticeNode node : rootNode.descendants) {
      tables.add(node.table.t.unwrapOrThrow(Table.class));
    }
    return StarTable.of(this, tables);
  }

  public static Builder builder(CalciteSchema calciteSchema, String sql) {
    return builder(new LatticeSpace(MapSqlStatisticProvider.INSTANCE),
        calciteSchema, sql);
  }

  static Builder builder(LatticeSpace space, CalciteSchema calciteSchema,
      String sql) {
    return new Builder(space, calciteSchema, sql);
  }

  public List<Measure> toMeasures(List<AggregateCall> aggCallList) {
    return Util.transform(aggCallList, this::toMeasure);
  }

  private Measure toMeasure(AggregateCall aggCall) {
    return new Measure(aggCall.getAggregation(), aggCall.isDistinct(),
        aggCall.name, Util.transform(aggCall.getArgList(), columns::get));
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

  public List<String> uniqueColumnNames() {
    return Util.transform(columns, column -> column.alias);
  }

  Pair<Path, Integer> columnToPathOffset(BaseColumn c) {
    for (Pair<LatticeNode, Path> p
        : Pair.zip(rootNode.descendants, rootNode.paths)) {
      if (Objects.equals(p.left.alias, c.table)) {
        return Pair.of(p.right, c.ordinal - p.left.startCol);
      }
    }
    throw new AssertionError("lattice column not found: " + c);
  }

  /** Returns the set of tables in this lattice. */
  public Set<LatticeTable> tables() {
    return rootNode.descendants.stream().map(n -> n.table)
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  /** Returns the ordinal, within all of the columns in this Lattice, of the
   * first column in the table with a given alias.
   * Returns -1 if the table is not found. */
  public int firstColumn(String tableAlias) {
    for (Column column : columns) {
      if (column instanceof BaseColumn
          && ((BaseColumn) column).table.equals(tableAlias)) {
        return column.ordinal;
      }
    }
    return -1;
  }

  /** Returns whether every use of a column is as an argument to a measure.
   *
   * <p>For example, in the query
   * {@code select sum(x + y), sum(a + b) from t group by x + y}
   * the expression "x + y" is used once as an argument to a measure,
   * and once as a dimension.
   *
   * <p>Therefore, in a lattice created from that one query,
   * {@code isAlwaysMeasure} for the derived column corresponding to "x + y"
   * returns false, and for "a + b" returns true.
   *
   * @param column Column or derived column
   * @return Whether all uses are as arguments to aggregate functions
   */
  public boolean isAlwaysMeasure(Column column) {
    return !columnUses.get(column.ordinal).contains(false);
  }

  /** Edge in the temporary graph. */
  private static class Edge extends DefaultEdge {
    public static final DirectedGraph.EdgeFactory<Vertex, Edge> FACTORY =
        Edge::new;

    final List<IntPair> pairs = new ArrayList<>();

    Edge(Vertex source, Vertex target) {
      super(source, target);
    }

    Vertex getTarget() {
      return (Vertex) target;
    }

    Vertex getSource() {
      return (Vertex) source;
    }
  }

  /** Vertex in the temporary graph. */
  private static class Vertex {
    final LatticeTable table;
    final @Nullable String alias;

    private Vertex(LatticeTable table, @Nullable String alias) {
      this.table = table;
      this.alias = alias;
    }
  }

  /** A measure within a {@link Lattice}.
   *
   * <p>It is immutable.
   *
   * <p>Examples: SUM(products.weight), COUNT() (means "COUNT(*")),
   * COUNT(DISTINCT customer.id).
   */
  public static class Measure implements Comparable<Measure> {
    public final SqlAggFunction agg;
    public final boolean distinct;
    public final @Nullable String name;
    public final ImmutableList<Column> args;
    public final String digest;

    public Measure(SqlAggFunction agg, boolean distinct, @Nullable String name,
        Iterable<Column> args) {
      this.agg = requireNonNull(agg, "agg");
      this.distinct = distinct;
      this.name = name;
      this.args = ImmutableList.copyOf(args);

      final StringBuilder b = new StringBuilder()
          .append(agg)
          .append(distinct ? "(DISTINCT " : "(");
      for (Ord<Column> arg : Ord.zip(this.args)) {
        if (arg.i > 0) {
          b.append(", ");
        }
        if (arg.e instanceof BaseColumn) {
          b.append(((BaseColumn) arg.e).table);
          b.append('.');
          b.append(((BaseColumn) arg.e).column);
        } else {
          b.append(arg.e.alias);
        }
      }
      b.append(')');
      this.digest = b.toString();
    }

    @Override public int compareTo(Measure measure) {
      int c = compare(args, measure.args);
      if (c == 0) {
        c = agg.getName().compareTo(measure.agg.getName());
        if (c == 0) {
          c = Boolean.compare(distinct, measure.distinct);
        }
      }
      return c;
    }

    @Override public String toString() {
      return digest;
    }

    @Override public int hashCode() {
      return Objects.hash(agg, args);
    }

    @Override public boolean equals(@Nullable Object obj) {
      return obj == this
          || obj instanceof Measure
          && this.agg.equals(((Measure) obj).agg)
          && this.args.equals(((Measure) obj).args)
          && this.distinct == ((Measure) obj).distinct;
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
      return Util.transform(args, column -> column.ordinal);
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

    /** Copies this measure, mapping its arguments using a given function. */
    Measure copy(Function<Column, Column> mapper) {
      return new Measure(agg, distinct, name, Util.transform(args, mapper));
    }
  }

  /** Column in a lattice. May be an a base column or an expression,
   * and may have an additional alias that is unique
   * within the entire lattice. */
  public abstract static class Column implements Comparable<Column> {
    /** Ordinal of the column within the lattice. */
    public final int ordinal;
    /** Alias of the column, unique within the lattice. Derived from the column
     * name, automatically disambiguated if necessary. */
    public final String alias;

    private Column(int ordinal, String alias) {
      this.ordinal = ordinal;
      this.alias = requireNonNull(alias, "alias");
    }

    /** Converts a list of columns to a bit set of their ordinals. */
    static ImmutableBitSet toBitSet(List<Column> columns) {
      final ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
      for (Column column : columns) {
        builder.set(column.ordinal);
      }
      return builder.build();
    }

    @Override public int compareTo(Column column) {
      return Utilities.compare(ordinal, column.ordinal);
    }

    @Override public int hashCode() {
      return ordinal;
    }

    @Override public boolean equals(@Nullable Object obj) {
      return obj == this
          || obj instanceof Column
          && this.ordinal == ((Column) obj).ordinal;
    }

    public abstract void toSql(SqlWriter writer);

    /** The alias that SQL would give to this expression. */
    public abstract @Nullable String defaultAlias();
  }

  /** Column in a lattice. Columns are identified by table alias and
   * column name, and may have an additional alias that is unique
   * within the entire lattice. */
  public static class BaseColumn extends Column {
    /** Alias of the table reference that the column belongs to. */
    public final String table;

    /** Name of the column. Unique within the table reference, but not
     * necessarily within the lattice. */
    public final String column;

    private BaseColumn(int ordinal, String table, String column, String alias) {
      super(ordinal, alias);
      this.table = requireNonNull(table, "table");
      this.column = requireNonNull(column, "column");
    }

    @Override public String toString() {
      return identifiers().toString();
    }

    public List<String> identifiers() {
      return ImmutableList.of(table, column);
    }

    @Override public void toSql(SqlWriter writer) {
      writer.dialect.quoteIdentifier(writer.buf, identifiers());
    }

    @Override public String defaultAlias() {
      return column;
    }
  }

  /** Column in a lattice that is based upon a SQL expression. */
  public static class DerivedColumn extends Column {
    public final RexNode e;
    final List<String> tables;

    private DerivedColumn(int ordinal, String alias, RexNode e,
        List<String> tables) {
      super(ordinal, alias);
      this.e = e;
      this.tables = ImmutableList.copyOf(tables);
    }

    @Override public String toString() {
      return Arrays.toString(new Object[] {e, alias});
    }

    @Override public void toSql(SqlWriter writer) {
      writer.write(e);
    }

    @Override public @Nullable String defaultAlias() {
      // there is no default alias for an expression
      return null;
    }
  }

  /** The information necessary to convert a column to SQL. */
  public static class SqlWriter {
    public final Lattice lattice;
    public final StringBuilder buf;
    public final SqlDialect dialect;
    private final SqlImplementor.SimpleContext context;

    SqlWriter(Lattice lattice, SqlDialect dialect, StringBuilder buf,
        SqlImplementor.SimpleContext context) {
      this.lattice = lattice;
      this.context = context;
      this.buf = buf;
      this.dialect = dialect;
    }

    /** Re-binds this writer to a different {@link StringBuilder}. */
    public SqlWriter with(StringBuilder buf) {
      return new SqlWriter(lattice, dialect, buf, context);
    }

    /** Writes an expression. */
    public SqlWriter write(RexNode e) {
      final SqlNode node = context.toSql(null, e);
      buf.append(node.toSqlString(dialect));
      return this;
    }
  }

  /** Lattice builder. */
  public static class Builder {
    private final LatticeRootNode rootNode;
    private final ImmutableList<BaseColumn> baseColumns;
    private final ImmutableListMultimap<String, Column> columnsByAlias;
    private final NavigableSet<Measure> defaultMeasureSet =
        new TreeSet<>();
    private final ImmutableList.Builder<Tile> tileListBuilder =
        ImmutableList.builder();
    private final Multimap<Integer, Boolean> columnUses =
        LinkedHashMultimap.create();
    private final CalciteSchema rootSchema;
    private boolean algorithm = false;
    private long algorithmMaxMillis = -1;
    private boolean auto = true;
    private @MonotonicNonNull Double rowCountEstimate;
    private @Nullable String statisticProvider;
    private final Map<String, DerivedColumn> derivedColumnsByName =
        new LinkedHashMap<>();

    public Builder(LatticeSpace space, CalciteSchema schema, String sql) {
      this.rootSchema = requireNonNull(schema.root());
      Preconditions.checkArgument(rootSchema.isRoot(), "must be root schema");
      CalcitePrepare.ConvertResult parsed =
          Schemas.convert(MaterializedViewTable.MATERIALIZATION_CONNECTION,
              schema, schema.path(null), sql);

      // Walk the join tree.
      List<TableScan> relNodes = new ArrayList<>();
      List<int[][]> tempLinks = new ArrayList<>();
      populate(relNodes, tempLinks, parsed.root.rel);

      // Get aliases.
      List<@Nullable String> aliases = new ArrayList<>();
      SqlNode from = ((SqlSelect) parsed.sqlNode).getFrom();
      assert from != null : "from must not be null";
      populateAliases(from, aliases, null);

      // Build a graph.
      final DirectedGraph<Vertex, Edge> graph =
          DefaultDirectedGraph.create(Edge.FACTORY);
      final List<Vertex> vertices = new ArrayList<>();
      for (Pair<TableScan, @Nullable String> p : Pair.zip(relNodes, aliases)) {
        final LatticeTable table = space.register(p.left.getTable());
        final Vertex vertex = new Vertex(table, p.right);
        graph.addVertex(vertex);
        vertices.add(vertex);
      }
      for (int[][] tempLink : tempLinks) {
        final Vertex source = vertices.get(tempLink[0][0]);
        final Vertex target = vertices.get(tempLink[1][0]);
        Edge edge = graph.getEdge(source, target);
        if (edge == null) {
          edge = castNonNull(graph.addEdge(source, target));
        }
        edge.pairs.add(IntPair.of(tempLink[0][1], tempLink[1][1]));
      }

      // Convert the graph into a tree of nodes, each connected to a parent and
      // with a join condition to that parent.
      MutableNode root = null;
      final IdentityHashMap<LatticeTable, MutableNode> map = new IdentityHashMap<>();
      for (Vertex vertex : TopologicalOrderIterator.of(graph)) {
        final List<Edge> edges = graph.getInwardEdges(vertex);
        MutableNode node;
        if (root == null) {
          if (!edges.isEmpty()) {
            throw new RuntimeException("root node must not have relationships: "
                + vertex);
          }
          root = node = new MutableNode(vertex.table);
          node.alias = vertex.alias;
        } else {
          if (edges.size() != 1) {
            throw new RuntimeException(
                "child node must have precisely one parent: " + vertex);
          }
          final Edge edge = edges.get(0);
          final MutableNode parent = map.get(edge.getSource().table);
          final Step step =
              Step.create(edge.getSource().table,
                  edge.getTarget().table, edge.pairs, space);
          node = new MutableNode(vertex.table, parent, step);
          node.alias = vertex.alias;
        }
        map.put(vertex.table, node);
      }
      assert root != null;
      final Fixer fixer = new Fixer();
      fixer.fixUp(root);
      baseColumns = fixer.columnList.build();
      columnsByAlias = fixer.columnAliasList.build();
      rootNode = new LatticeRootNode(space, root);
    }

    /** Creates a Builder based upon a mutable node. */
    Builder(LatticeSpace space, CalciteSchema schema,
        MutableNode mutableNode) {
      this.rootSchema = schema;

      final Fixer fixer = new Fixer();
      fixer.fixUp(mutableNode);

      final LatticeRootNode node0 = new LatticeRootNode(space, mutableNode);
      final LatticeRootNode node1 = space.nodeMap.get(node0.digest);
      final LatticeRootNode node;
      if (node1 != null) {
        node = node1;
      } else {
        node = node0;
        space.nodeMap.put(node0.digest, node0);
      }

      this.rootNode = node;
      baseColumns = fixer.columnList.build();
      columnsByAlias = fixer.columnAliasList.build();
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
    public Builder statisticProvider(@Nullable String statisticProvider) {
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
      final ImmutableList.Builder<Column> columnBuilder =
          ImmutableList.<Column>builder()
          .addAll(baseColumns)
          .addAll(derivedColumnsByName.values());
      return new Lattice(rootSchema, rootNode, auto,
          algorithm, algorithmMaxMillis, statisticProvider, rowCountEstimate,
          columnBuilder.build(), ImmutableSortedSet.copyOf(defaultMeasureSet),
          tileListBuilder.build(), ImmutableListMultimap.copyOf(columnUses));
    }

    /** Resolves the arguments of a
     * {@link org.apache.calcite.model.JsonMeasure}. They must either be null,
     * a string, or a list of strings. Throws if the structure is invalid, or if
     * any of the columns do not exist in the lattice. */
    public ImmutableList<Column> resolveArgs(@Nullable Object args) {
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
        default:
          break;
        }
      }
      throw new RuntimeException(
          "Lattice column reference must be a string or a list of 1 or 2 strings; column: "
              + name);
    }

    private Column resolveQualifiedColumn(String table, String column) {
      for (BaseColumn column1 : baseColumns) {
        if (column1.table.equals(table)
            && column1.column.equals(column)) {
          return column1;
        }
      }
      throw new RuntimeException("Unknown lattice column [" + table + ", "
          + column + "]");
    }

    public Measure resolveMeasure(String aggName, boolean distinct,
        @Nullable Object args) {
      final SqlAggFunction agg = resolveAgg(aggName);
      final ImmutableList<Column> list = resolveArgs(args);
      return new Measure(agg, distinct, aggName, list);
    }

    private static SqlAggFunction resolveAgg(String aggName) {
      if (aggName.equalsIgnoreCase("count")) {
        return SqlStdOperatorTable.COUNT;
      } else if (aggName.equalsIgnoreCase("sum")) {
        return SqlStdOperatorTable.SUM;
      } else {
        throw new RuntimeException("Unknown lattice aggregate function "
            + aggName);
      }
    }

    /** Adds a measure, if it does not already exist.
     * Returns false if an identical measure already exists. */
    public boolean addMeasure(Measure measure) {
      return defaultMeasureSet.add(measure);
    }

    public void addTile(Tile tile) {
      tileListBuilder.add(tile);
    }

    public Column column(int table, int column) {
      int i = 0;
      for (LatticeNode descendant : rootNode.descendants) {
        if (table-- == 0) {
          break;
        }
        i += descendant.table.t.getRowType().getFieldCount();
      }
      return baseColumns.get(i + column);
    }

    Column pathOffsetToColumn(Path path, int offset) {
      final int i = rootNode.paths.indexOf(path);
      final LatticeNode node = rootNode.descendants.get(i);
      final int c = node.startCol + offset;
      if (c >= node.endCol) {
        throw new AssertionError();
      }
      return baseColumns.get(c);
    }

    /** Adds a lattice column based on a SQL expression,
     * or returns a column based on the same expression seen previously. */
    public Column expression(RexNode e, String alias,
        List<String> tableAliases) {
      return derivedColumnsByName.computeIfAbsent(e.toString(), k -> {
        final int derivedOrdinal = derivedColumnsByName.size();
        final int ordinal = baseColumns.size() + derivedOrdinal;
        return new DerivedColumn(ordinal,
            Util.first(alias, "e$" + derivedOrdinal), e, tableAliases);
      });
    }

    /** Records a use of a column.
     *
     * @param column Column
     * @param measure Whether this use is as an argument to a measure;
     *                e.g. "sum(x + y)" is a measure use of the expression
     *                "x + y"; "group by x + y" is not
     */
    public void use(Column column, boolean measure) {
      columnUses.put(column.ordinal, measure);
    }

    /** Work space for fixing up a tree of mutable nodes. */
    private static class Fixer {
      final Set<String> aliases = new HashSet<>();
      final Set<String> columnAliases = new HashSet<>();
      final Set<MutableNode> seen = new HashSet<>();
      final ImmutableList.Builder<BaseColumn> columnList =
          ImmutableList.builder();
      final ImmutableListMultimap.Builder<String, Column> columnAliasList =
          ImmutableListMultimap.builder();
      int c;

      void fixUp(MutableNode node) {
        if (!seen.add(node)) {
          throw new IllegalArgumentException("cyclic query graph");
        }
        if (node.alias == null) {
          node.alias = Util.last(node.table.t.getQualifiedName());
        }
        node.alias = SqlValidatorUtil.uniquify(node.alias, aliases,
            SqlValidatorUtil.ATTEMPT_SUGGESTER);
        node.startCol = c;
        for (String name : node.table.t.getRowType().getFieldNames()) {
          final String alias = SqlValidatorUtil.uniquify(name,
              columnAliases, SqlValidatorUtil.ATTEMPT_SUGGESTER);
          final BaseColumn column =
              new BaseColumn(c++, castNonNull(node.alias), name, alias);
          columnList.add(column);
          columnAliasList.put(name, column); // name before it is made unique
        }
        node.endCol = c;

        assert MutableNode.ORDERING.isStrictlyOrdered(node.children)
            : node.children;
        for (MutableNode child : node.children) {
          fixUp(child);
        }
      }
    }
  }

  /** Materialized aggregate within a lattice. */
  public static class Tile {
    public final ImmutableList<Measure> measures;
    public final ImmutableList<Column> dimensions;
    public final ImmutableBitSet bitSet;

    public Tile(ImmutableList<Measure> measures,
        ImmutableList<Column> dimensions) {
      this.measures = requireNonNull(measures, "measures");
      this.dimensions = requireNonNull(dimensions, "dimensions");
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
