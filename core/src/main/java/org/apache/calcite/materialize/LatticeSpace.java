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

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.graph.AttributedDirectedGraph;
import org.apache.calcite.util.mapping.IntPair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/** Space within which lattices exist. */
class LatticeSpace {
  final SqlStatisticProvider statisticProvider;
  private final Map<List<String>, LatticeTable> tableMap = new HashMap<>();
  final AttributedDirectedGraph<LatticeTable, Step> g =
      new AttributedDirectedGraph<>(new Step.Factory(this));
  private final Map<List<String>, String> simpleTableNames = new HashMap<>();
  private final Set<String> simpleNames = new HashSet<>();
  /** Root nodes, indexed by digest. */
  final Map<String, LatticeRootNode> nodeMap = new HashMap<>();
  final Map<ImmutableList<Step>, Path> pathMap = new HashMap<>();
  final Map<LatticeTable, List<RexNode>> tableExpressions = new HashMap<>();

  LatticeSpace(SqlStatisticProvider statisticProvider) {
    this.statisticProvider = Objects.requireNonNull(statisticProvider);
  }

  /** Derives a unique name for a table, qualifying with schema name only if
   * necessary. */
  String simpleName(LatticeTable table) {
    return simpleName(table.t.getQualifiedName());
  }

  String simpleName(RelOptTable table) {
    return simpleName(table.getQualifiedName());
  }

  String simpleName(List<String> table) {
    final String name = simpleTableNames.get(table);
    if (name != null) {
      return name;
    }
    final String name2 = Util.last(table);
    if (simpleNames.add(name2)) {
      simpleTableNames.put(ImmutableList.copyOf(table), name2);
      return name2;
    }
    final String name3 = table.toString();
    simpleTableNames.put(ImmutableList.copyOf(table), name3);
    return name3;
  }

  LatticeTable register(RelOptTable t) {
    final LatticeTable table = tableMap.get(t.getQualifiedName());
    if (table != null) {
      return table;
    }
    final LatticeTable table2 = new LatticeTable(t);
    tableMap.put(t.getQualifiedName(), table2);
    g.addVertex(table2);
    return table2;
  }

  Step addEdge(LatticeTable source, LatticeTable target, List<IntPair> keys) {
    keys = sortUnique(keys);
    final Step step = g.addEdge(source, target, keys);
    if (step != null) {
      return step;
    }
    for (Step step2 : g.getEdges(source, target)) {
      if (step2.keys.equals(keys)) {
        return step2;
      }
    }
    throw new AssertionError("addEdge failed, yet no edge present");
  }

  /** Returns a list of {@link IntPair} that is sorted and unique. */
  static List<IntPair> sortUnique(List<IntPair> keys) {
    if (keys.size() > 1) {
      // list may not be sorted; sort it
      keys = IntPair.ORDERING.immutableSortedCopy(keys);
      if (!IntPair.ORDERING.isStrictlyOrdered(keys)) {
        // list may contain duplicates; sort and eliminate duplicates
        final Set<IntPair> set = new TreeSet<>(IntPair.ORDERING);
        set.addAll(keys);
        keys = ImmutableList.copyOf(set);
      }
    }
    return keys;
  }

  /** Returns a list of {@link IntPair}, transposing source and target fields,
   * and ensuring the result is sorted and unique. */
  static List<IntPair> swap(List<IntPair> keys) {
    return sortUnique(Lists.transform(keys, IntPair.SWAP));
  }

  Path addPath(List<Step> steps) {
    final ImmutableList<Step> key = ImmutableList.copyOf(steps);
    final Path path = pathMap.get(key);
    if (path != null) {
      return path;
    }
    final Path path2 = new Path(key, pathMap.size());
    pathMap.put(key, path2);
    return path2;
  }

  /** Registers an expression as a derived column of a given table.
   *
   * <p>Its ordinal is the number of fields in the row type plus the ordinal
   * of the extended expression. For example, if a table has 10 fields then its
   * derived columns will have ordinals 10, 11, 12 etc. */
  int registerExpression(LatticeTable table, RexNode e) {
    final List<RexNode> expressions =
        tableExpressions.computeIfAbsent(table, t -> new ArrayList<>());
    final int fieldCount = table.t.getRowType().getFieldCount();
    for (int i = 0; i < expressions.size(); i++) {
      if (expressions.get(i).toString().equals(e.toString())) {
        return fieldCount + i;
      }
    }
    final int result = fieldCount + expressions.size();
    expressions.add(e);
    return result;
  }

  /** Returns the name of field {@code field} of {@code table}.
   *
   * <p>If the field is derived (see
   * {@link #registerExpression(LatticeTable, RexNode)}) its name is its
   * {@link RexNode#toString()}. */
  public String fieldName(LatticeTable table, int field) {
    final List<RelDataTypeField> fieldList =
        table.t.getRowType().getFieldList();
    final int fieldCount = fieldList.size();
    if (field < fieldCount) {
      return fieldList.get(field).getName();
    } else {
      return tableExpressions.get(table).get(field - fieldCount).toString();
    }
  }
}

// End LatticeSpace.java
