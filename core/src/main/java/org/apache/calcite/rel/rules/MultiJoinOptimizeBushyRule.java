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
package org.eigenbase.rel.rules;

import java.io.PrintWriter;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.RelMdUtil;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.Pair;
import org.eigenbase.util.Util;
import org.eigenbase.util.mapping.Mappings;

import net.hydromatic.optiq.prepare.OptiqPrepareImpl;
import net.hydromatic.optiq.util.BitSets;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

/**
 * Planner rule that finds an approximately optimal ordering for join operators
 * using a heuristic algorithm.
 *
 * <p>It is triggered by the pattern {@link ProjectRel} ({@link MultiJoinRel}).
 *
 * <p>It is similar to {@link org.eigenbase.rel.rules.LoptOptimizeJoinRule}.
 * {@code LoptOptimizeJoinRule} is only capable of producing left-deep joins;
 * this rule is capable of producing bushy joins.
 *
 * <p>TODO:
 * <ol>
 *   <li>Join conditions that touch 1 factor.
 *   <li>Join conditions that touch 3 factors.
 *   <li>More than 1 join conditions that touch the same pair of factors,
 *       e.g. {@code t0.c1 = t1.c1 and t1.c2 = t0.c3}
 * </ol>
 */
public class OptimizeBushyJoinRule extends RelOptRule {
  public static final OptimizeBushyJoinRule INSTANCE =
      new OptimizeBushyJoinRule(RelFactories.DEFAULT_JOIN_FACTORY,
          RelFactories.DEFAULT_PROJECT_FACTORY);

  private final RelFactories.JoinFactory joinFactory;
  private final RelFactories.ProjectFactory projectFactory;
  private final PrintWriter pw =
      OptiqPrepareImpl.DEBUG ? new PrintWriter(System.out, true) : null;

  /** Creates an OptimizeBushyJoinRule. */
  public OptimizeBushyJoinRule(RelFactories.JoinFactory joinFactory,
      RelFactories.ProjectFactory projectFactory) {
    super(operand(MultiJoinRel.class, any()));
    this.joinFactory = joinFactory;
    this.projectFactory = projectFactory;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final MultiJoinRel multiJoinRel = call.rel(0);
    final RexBuilder rexBuilder = multiJoinRel.getCluster().getRexBuilder();

    final LoptMultiJoin multiJoin = new LoptMultiJoin(multiJoinRel);

    final List<Vertex> vertexes = Lists.newArrayList();
    int x = 0;
    for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
      final RelNode rel = multiJoin.getJoinFactor(i);
      double cost = RelMetadataQuery.getRowCount(rel);
      vertexes.add(new LeafVertex(i, rel, cost, x));
      x += rel.getRowType().getFieldCount();
    }
    assert x == multiJoin.getNumTotalFields();

    final List<LoptMultiJoin.Edge> unusedEdges = Lists.newArrayList();
    for (RexNode node : multiJoin.getJoinFilters()) {
      unusedEdges.add(multiJoin.createEdge(node));
    }

    // Comparator that chooses the best edge. A "good edge" is one that has
    // a large difference in the number of rows on LHS and RHS.
    final Comparator<LoptMultiJoin.Edge> edgeComparator =
        new Comparator<LoptMultiJoin.Edge>() {
          public int compare(LoptMultiJoin.Edge e0, LoptMultiJoin.Edge e1) {
            return Double.compare(rowCountDiff(e0), rowCountDiff(e1));
          }

          private double rowCountDiff(LoptMultiJoin.Edge edge) {
            assert edge.factors.cardinality() == 2 : edge.factors;
            final int factor0 = edge.factors.nextSetBit(0);
            final int factor1 = edge.factors.nextSetBit(factor0 + 1);
            return Math.abs(vertexes.get(factor0).cost
                - vertexes.get(factor1).cost);
          }
        };

    final List<LoptMultiJoin.Edge> usedEdges = Lists.newArrayList();
    for (;;) {
      final int edgeOrdinal = chooseBestEdge(unusedEdges, edgeComparator);
      if (pw != null) {
        trace(vertexes, unusedEdges, usedEdges, edgeOrdinal, pw);
      }
      final int[] factors;
      if (edgeOrdinal == -1) {
        // No more edges. Are there any un-joined vertexes?
        final Vertex lastVertex = Util.last(vertexes);
        final int z =
            BitSets.previousClearBit(lastVertex.factors, lastVertex.id - 1);
        if (z < 0) {
          break;
        }
        factors = new int[] {z, lastVertex.id};
      } else {
        final LoptMultiJoin.Edge bestEdge = unusedEdges.get(edgeOrdinal);

        // For now, assume that the edge is between precisely two factors.
        // 1-factor conditions have probably been pushed down,
        // and 3-or-more-factor conditions are advanced. (TODO:)
        // Therefore, for now, the factors that are merged are exactly the
        // factors on this edge.
        assert bestEdge.factors.cardinality() == 2;
        factors = BitSets.toArray(bestEdge.factors);
      }

      // Determine which factor is to be on the LHS of the join.
      final int majorFactor;
      final int minorFactor;
      if (vertexes.get(factors[0]).cost <= vertexes.get(factors[1]).cost) {
        majorFactor = factors[0];
        minorFactor = factors[1];
      } else {
        majorFactor = factors[1];
        minorFactor = factors[0];
      }
      final Vertex majorVertex = vertexes.get(majorFactor);
      final Vertex minorVertex = vertexes.get(minorFactor);

      // Find the join conditions. All conditions whose factors are now all in
      // the join can now be used.
      final BitSet newFactors =
          BitSets.union(majorVertex.factors, minorVertex.factors);
      final List<RexNode> conditions = Lists.newArrayList();
      final Iterator<LoptMultiJoin.Edge> edgeIterator = unusedEdges.iterator();
      while (edgeIterator.hasNext()) {
        LoptMultiJoin.Edge edge = edgeIterator.next();
        if (BitSets.contains(newFactors, edge.factors)) {
          conditions.add(edge.condition);
          edgeIterator.remove();
          usedEdges.add(edge);
        }
      }

      final int v = vertexes.size();
      double cost =
          majorVertex.cost
          * minorVertex.cost
          * RelMdUtil.guessSelectivity(
              RexUtil.composeConjunction(rexBuilder, conditions, false));
      newFactors.set(v);
      final Vertex newVertex =
          new JoinVertex(v, majorFactor, minorFactor, newFactors,
              cost, ImmutableList.copyOf(conditions));
      vertexes.add(newVertex);

      // Re-compute selectivity of edges above the one just chosen.
      // Suppose that we just chose the edge between "product" (10k rows) and
      // "product_class" (10 rows).
      // Both of those vertices are now replaced by a new vertex "P-PC".
      // This vertex has fewer rows (1k rows) -- a fact that is critical to
      // decisions made later. (Hence "greedy" algorithm not "simple".)
      // The adjacent edges are modified.
      final BitSet merged = BitSets.of(minorFactor, majorFactor);
      for (int i = 0; i < unusedEdges.size(); i++) {
        final LoptMultiJoin.Edge edge = unusedEdges.get(i);
        if (edge.factors.intersects(merged)) {
          BitSet newEdgeFactors = (BitSet) edge.factors.clone();
          newEdgeFactors.andNot(newFactors);
          newEdgeFactors.set(v);
          assert newEdgeFactors.cardinality() == 2;
          final LoptMultiJoin.Edge newEdge =
              new LoptMultiJoin.Edge(edge.condition, newEdgeFactors,
                  edge.columns);
          unusedEdges.set(i, newEdge);
        }
      }
    }

    // We have a winner!
    List<Pair<RelNode, Mappings.TargetMapping>> relNodes = Lists.newArrayList();
    for (Vertex vertex : vertexes) {
      if (vertex instanceof LeafVertex) {
        LeafVertex leafVertex = (LeafVertex) vertex;
        final Mappings.TargetMapping mapping =
            Mappings.offsetSource(
                Mappings.createIdentity(
                    leafVertex.rel.getRowType().getFieldCount()),
                leafVertex.fieldOffset,
                multiJoin.getNumTotalFields());
        relNodes.add(Pair.of(leafVertex.rel, mapping));
      } else {
        JoinVertex joinVertex = (JoinVertex) vertex;
        final Pair<RelNode, Mappings.TargetMapping> leftPair =
            relNodes.get(joinVertex.leftFactor);
        RelNode left = leftPair.left;
        final Mappings.TargetMapping leftMapping = leftPair.right;
        final Pair<RelNode, Mappings.TargetMapping> rightPair =
            relNodes.get(joinVertex.rightFactor);
        RelNode right = rightPair.left;
        final Mappings.TargetMapping rightMapping = rightPair.right;
        final Mappings.TargetMapping mapping =
            Mappings.merge(leftMapping,
                Mappings.offsetTarget(rightMapping,
                    left.getRowType().getFieldCount()));
        if (pw != null) {
          pw.println("left: " + leftMapping);
          pw.println("right: " + rightMapping);
          pw.println("combined: " + mapping);
          pw.println();
        }
        final RexVisitor<RexNode> shuttle =
            new RexPermuteInputsShuttle(mapping, left, right);
        final RexNode condition =
            RexUtil.composeConjunction(rexBuilder, joinVertex.conditions,
                false);
        relNodes.add(
            Pair.of((RelNode)
                joinFactory.createJoin(left, right, condition.accept(shuttle),
                    JoinRelType.INNER, ImmutableSet.<String>of(), false),
                mapping));
      }
      if (pw != null) {
        pw.println(Util.last(relNodes));
      }
    }

    final Pair<RelNode, Mappings.TargetMapping> top = Util.last(relNodes);
    final RelNode project =
        RelOptUtil.createProject(projectFactory, top.left,
            Mappings.asList(top.right));

    call.transformTo(project);
  }

  private void trace(List<Vertex> vertexes,
      List<LoptMultiJoin.Edge> unusedEdges, List<LoptMultiJoin.Edge> usedEdges,
      int edgeOrdinal, PrintWriter pw) {
    pw.println("bestEdge: " + edgeOrdinal);
    pw.println("vertexes:");
    for (Vertex vertex : vertexes) {
      pw.println(vertex);
    }
    pw.println("unused edges:");
    for (LoptMultiJoin.Edge edge : unusedEdges) {
      pw.println(edge);
    }
    pw.println("edges:");
    for (LoptMultiJoin.Edge edge : usedEdges) {
      pw.println(edge);
    }
    pw.println();
    pw.flush();
  }

  int chooseBestEdge(List<LoptMultiJoin.Edge> edges,
      Comparator<LoptMultiJoin.Edge> comparator) {
    return minPos(edges, comparator);
  }

  /** Returns the index within a list at which compares least according to a
   * comparator.
   *
   * <p>In the case of a tie, returns the earliest such element.</p>
   *
   * <p>If the list is empty, returns -1.</p>
   */
  static <E> int minPos(List<E> list, Comparator<E> fn) {
    if (list.isEmpty()) {
      return -1;
    }
    E eBest = list.get(0);
    int iBest = 0;
    for (int i = 1; i < list.size(); i++) {
      E e = list.get(i);
      if (fn.compare(e, eBest) < 0) {
        eBest = e;
        iBest = i;
      }
    }
    return iBest;
  }

  /** Participant in a join (relation or join). */
  abstract static class Vertex {
    final int id;

    protected final BitSet factors;
    final double cost;

    Vertex(int id, BitSet factors, double cost) {
      this.id = id;
      this.factors = factors;
      this.cost = cost;
    }
  }

  /** Relation participating in a join. */
  static class LeafVertex extends Vertex {
    private final RelNode rel;
    final int fieldOffset;

    LeafVertex(int id, RelNode rel, double cost, int fieldOffset) {
      super(id, BitSets.of(id), cost);
      this.rel = rel;
      this.fieldOffset = fieldOffset;
    }

    @Override public String toString() {
      return "LeafVertex(id: " + id
          + ", cost: " + Util.human(cost)
          + ", factors: " + factors
          + ", fieldOffset: " + fieldOffset
          + ")";
    }
  }

  /** Participant in a join which is itself a join. */
  static class JoinVertex extends Vertex {
    private final int leftFactor;
    private final int rightFactor;
    /** Zero or more join conditions. All are in terms of the original input
     * columns (not in terms of the outputs of left and right input factors). */
    final ImmutableList<RexNode> conditions;

    JoinVertex(int id, int leftFactor, int rightFactor,
        BitSet factors, double cost, ImmutableList<RexNode> conditions) {
      super(id, factors, cost);
      this.leftFactor = leftFactor;
      this.rightFactor = rightFactor;
      this.conditions = Preconditions.checkNotNull(conditions);
    }

    @Override public String toString() {
      return "JoinVertex(id: " + id
          + ", cost: " + Util.human(cost)
          + ", factors: " + factors
          + ", leftFactor: " + leftFactor
          + ", rightFactor: " + rightFactor
          + ")";
    }
  }
}

// End OptimizeBushyJoinRule.java
