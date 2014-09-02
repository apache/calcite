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
import net.hydromatic.optiq.util.graph.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.rex.*;
import org.eigenbase.sql.SqlJoin;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlSelect;
import org.eigenbase.sql.SqlUtil;
import org.eigenbase.sql.validate.SqlValidatorUtil;
import org.eigenbase.util.mapping.IntPair;

import com.google.common.base.Preconditions;
import com.google.common.collect.*;

import java.util.*;

/**
 * Structure that allows materialized views based upon a star schema to be
 * recognized and recommended.
 */
public class Lattice {
  public final ImmutableList<Node> nodes;
  public final ImmutableList<List<String>> columns;

  private Lattice(List<Node> nodes) {
    this.nodes = ImmutableList.copyOf(nodes);

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

    final ImmutableList.Builder<List<String>> builder = ImmutableList.builder();
    for (Node node : nodes) {
      if (node.scan != null) {
        for (String name : node.scan.getRowType().getFieldNames()) {
          builder.add(ImmutableList.of(node.alias, name));
        }
      }
    }
    columns = builder.build();
  }

  /** Creates a Lattice. */
  public static Lattice create(OptiqSchema schema, String sql) {
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
    List<Node> nodes = Lists.newArrayList();
    Node previous = null;
    final Map<RelNode, Node> map = Maps.newIdentityHashMap();
    int previousColumn = 0;
    for (RelNode relNode : TopologicalOrderIterator.of(graph)) {
      final List<Edge> edges = graph.getInwardEdges(relNode);
      Node node;
      final int column = previousColumn + relNode.getRowType().getFieldCount();
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
        node = new Node((TableAccessRelBase) relNode, map.get(edge.getSource()),
            edge.pairs, previousColumn, column, aliases.get(nodes.size()));
      }
      nodes.add(node);
      map.put(relNode, node);
      previous = node;
      previousColumn = column;
    }
    return new Lattice(nodes);
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

  public StarTable createStarTable() {
    final List<Table> tables = Lists.newArrayList();
    for (Node node : nodes) {
      tables.add(node.scan.getTable().unwrap(Table.class));
    }
    return StarTable.of(this, tables);
  }

  public List<String> getColumn(int i) {
    return columns.get(i);
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
}

// End Lattice.java
