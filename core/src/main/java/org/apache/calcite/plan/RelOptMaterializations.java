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
package org.apache.calcite.plan;

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.graph.DefaultDirectedGraph;
import org.apache.calcite.util.graph.DefaultEdge;
import org.apache.calcite.util.graph.DirectedGraph;
import org.apache.calcite.util.graph.Graphs;
import org.apache.calcite.util.graph.TopologicalOrderIterator;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Utility methods for using
 * materialized views and lattices for queries.
 */
public abstract class RelOptMaterializations {

  /**
   * Returns a list of RelNode transformed from all possible combination of
   * materialized view uses. Big queries will likely have more than one
   * transformed RelNode, e.g., (t1 group by c1) join (t2 group by c2).
   * @param rel               the original RelNode
   * @param materializations  the materialized view list
   * @return the list of transformed RelNode together with their corresponding
   *         materialized views used in the transformation.
   */
  public static List<Pair<RelNode, List<RelOptMaterialization>>> useMaterializedViews(
      final RelNode rel, List<RelOptMaterialization> materializations) {
    final List<RelOptMaterialization> applicableMaterializations =
        getApplicableMaterializations(rel, materializations);
    final List<Pair<RelNode, List<RelOptMaterialization>>> applied =
        new ArrayList<>();
    applied.add(Pair.of(rel, ImmutableList.of()));
    for (RelOptMaterialization m : applicableMaterializations) {
      int count = applied.size();
      for (int i = 0; i < count; i++) {
        Pair<RelNode, List<RelOptMaterialization>> current = applied.get(i);
        List<RelNode> sub = substitute(current.left, m);
        if (!sub.isEmpty()) {
          ImmutableList.Builder<RelOptMaterialization> builder =
              ImmutableList.builder();
          builder.addAll(current.right);
          builder.add(m);
          List<RelOptMaterialization> uses = builder.build();
          for (RelNode rel2 : sub) {
            applied.add(Pair.of(rel2, uses));
          }
        }
      }
    }

    return applied.subList(1, applied.size());
  }

  /**
   * Returns a list of RelNode transformed from all possible lattice uses.
   * @param rel       the original RelNode
   * @param lattices  the lattice list
   * @return the list of transformed RelNode together with their corresponding
   *         lattice used in the transformation.
   */
  public static List<Pair<RelNode, RelOptLattice>> useLattices(
      final RelNode rel, List<RelOptLattice> lattices) {
    final Set<RelOptTable> queryTables = RelOptUtil.findTables(rel);
    // Use a lattice if the query uses at least the central (fact) table of the
    // lattice.
    final List<Pair<RelNode, RelOptLattice>> latticeUses = new ArrayList<>();
    final Set<List<String>> queryTableNames =
        Sets.newHashSet(
            Iterables.transform(queryTables, RelOptTable::getQualifiedName));
    // Remember leaf-join form of root so we convert at most once.
    final Supplier<RelNode> leafJoinRoot =
        Suppliers.memoize(() -> RelOptMaterialization.toLeafJoinForm(rel))::get;
    for (RelOptLattice lattice : lattices) {
      if (queryTableNames.contains(lattice.rootTable().getQualifiedName())) {
        RelNode rel2 = lattice.rewrite(leafJoinRoot.get());
        if (rel2 != null) {
          if (CalciteSystemProperty.DEBUG.value()) {
            System.out.println("use lattice:\n"
                + RelOptUtil.toString(rel2));
          }
          latticeUses.add(Pair.of(rel2, lattice));
        }
      }
    }

    return latticeUses;
  }

  /**
   * Returns a list of materializations that can potentially be used by the query.
   */
  public static List<RelOptMaterialization> getApplicableMaterializations(
      RelNode rel, List<RelOptMaterialization> materializations) {
    DirectedGraph<List<String>, DefaultEdge> usesGraph =
        DefaultDirectedGraph.create();
    final Map<List<String>, RelOptMaterialization> qnameMap = new HashMap<>();
    for (RelOptMaterialization materialization : materializations) {
      // If materialization is a tile in a lattice, we will deal with it shortly.
      if (materialization.qualifiedTableName != null
          && materialization.starTable == null) {
        final List<String> qname = materialization.qualifiedTableName;
        qnameMap.put(qname, materialization);
        for (RelOptTable usedTable
            : RelOptUtil.findTables(materialization.queryRel)) {
          usesGraph.addVertex(qname);
          usesGraph.addVertex(usedTable.getQualifiedName());
          usesGraph.addEdge(usedTable.getQualifiedName(), qname);
        }
      }
    }

    // Use a materialization if uses at least one of the tables are used by
    // the query. (Simple rule that includes some materializations we won't
    // actually use.)
    // For example, given materializations:
    //   T = Emps Join Depts
    //   T2 = T Group by C1
    // the graph will contain
    //   (T, Emps), (T, Depts), (T2, T)
    // and therefore we can deduce T2 uses Emps.
    final Graphs.FrozenGraph<List<String>, DefaultEdge> frozenGraph =
        Graphs.makeImmutable(usesGraph);
    final Set<RelOptTable> queryTablesUsed = RelOptUtil.findTables(rel);
    final List<RelOptMaterialization> applicableMaterializations =
        new ArrayList<>();
    for (List<String> qname : TopologicalOrderIterator.of(usesGraph)) {
      RelOptMaterialization materialization = qnameMap.get(qname);
      if (materialization != null
          && usesTable(materialization.qualifiedTableName, queryTablesUsed, frozenGraph)) {
        applicableMaterializations.add(materialization);
      }
    }
    return applicableMaterializations;
  }

  private static List<RelNode> substitute(
      RelNode root, RelOptMaterialization materialization) {
    // First, if the materialization is in terms of a star table, rewrite
    // the query in terms of the star table.
    if (materialization.starTable != null) {
      RelNode newRoot = RelOptMaterialization.tryUseStar(root,
          materialization.starRelOptTable);
      if (newRoot != null) {
        root = newRoot;
      }
    }

    // Push filters to the bottom, and combine projects on top.
    RelNode target = materialization.queryRel;
    HepProgram program =
        new HepProgramBuilder()
            .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
            .addRuleInstance(ProjectMergeRule.INSTANCE)
            .addRuleInstance(ProjectRemoveRule.INSTANCE)
            .build();

    // We must use the same HEP planner for the two optimizations below.
    // Thus different nodes with the same digest will share the same vertex in
    // the plan graph. This is important for the matching process.
    final HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(target);
    target = hepPlanner.findBestExp();

    hepPlanner.setRoot(root);
    root = hepPlanner.findBestExp();

    return new MaterializedViewSubstitutionVisitor(target, root)
            .go(materialization.tableRel);
  }

  /**
   * Returns whether {@code table} uses one or more of the tables in
   * {@code usedTables}.
   */
  private static boolean usesTable(
      List<String> qualifiedName,
      Set<RelOptTable> usedTables,
      Graphs.FrozenGraph<List<String>, DefaultEdge> usesGraph) {
    for (RelOptTable queryTable : usedTables) {
      if (usesGraph.getShortestPath(queryTable.getQualifiedName(), qualifiedName)
          != null) {
        return true;
      }
    }
    return false;
  }
}

// End RelOptMaterializations.java
