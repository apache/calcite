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
import org.apache.calcite.util.graph.AttributedDirectedGraph;
import org.apache.calcite.util.graph.DefaultEdge;
import org.apache.calcite.util.mapping.IntPair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import java.util.List;
import java.util.Objects;

/** Edge in the join graph.
 *
 * <p>It is directed: the "parent" must be the "many" side containing the
 * foreign key, and the "target" is the "one" side containing the primary
 * key. For example, EMP &rarr; DEPT.
 *
 * <p>When created via
 * {@link LatticeSpace#addEdge(LatticeTable, LatticeTable, List)}
 * it is unique within the {@link LatticeSpace}. */
class Step extends DefaultEdge {
  final List<IntPair> keys;

  Step(LatticeTable source, LatticeTable target, List<IntPair> keys) {
    super(source, target);
    this.keys = ImmutableList.copyOf(keys);
    assert IntPair.ORDERING.isStrictlyOrdered(keys); // ordered and unique
  }

  @Override public int hashCode() {
    return Objects.hash(source, target, keys);
  }

  @Override public boolean equals(Object obj) {
    return this == obj
        || obj instanceof Step
        && ((Step) obj).source.equals(source)
        && ((Step) obj).target.equals(target)
        && ((Step) obj).keys.equals(keys);
  }

  @Override public String toString() {
    final StringBuilder b = new StringBuilder()
        .append("Step(")
        .append(source)
        .append(", ")
        .append(target)
        .append(",");
    for (IntPair key : keys) {
      b.append(' ')
          .append(source().field(key.source).getName())
          .append(':')
          .append(target().field(key.target).getName());
    }
    return b.append(")")
        .toString();
  }

  LatticeTable source() {
    return (LatticeTable) source;
  }

  LatticeTable target() {
    return (LatticeTable) target;
  }

  boolean isBackwards(SqlStatisticProvider statisticProvider) {
    final RelOptTable sourceTable = source().t;
    final List<Integer> sourceColumns = IntPair.left(keys);
    final RelOptTable targetTable = target().t;
    final List<Integer> targetColumns = IntPair.right(keys);
    final boolean forwardForeignKey =
        statisticProvider.isForeignKey(sourceTable, sourceColumns, targetTable,
            targetColumns)
        && statisticProvider.isKey(targetTable, targetColumns);
    final boolean backwardForeignKey =
        statisticProvider.isForeignKey(targetTable, targetColumns, sourceTable,
            sourceColumns)
        && statisticProvider.isKey(sourceTable, sourceColumns);
    if (backwardForeignKey != forwardForeignKey) {
      return backwardForeignKey;
    }
    // Tie-break if it's a foreign key in neither or both directions
    return compare(sourceTable, sourceColumns, targetTable, targetColumns) < 0;
  }

  /** Arbitrarily compares (table, columns). */
  private static int compare(RelOptTable table1, List<Integer> columns1,
      RelOptTable table2, List<Integer> columns2) {
    int c = Ordering.natural().<String>lexicographical()
        .compare(table1.getQualifiedName(), table2.getQualifiedName());
    if (c == 0) {
      c = Ordering.natural().<Integer>lexicographical()
          .compare(columns1, columns2);
    }
    return c;
  }

  /** Temporary method. We should use (inferred) primary keys to figure out
   * the direction of steps. */
  private double cardinality(SqlStatisticProvider statisticProvider,
      LatticeTable table) {
    return statisticProvider.tableCardinality(table.t);
  }

  /** Creates {@link Step} instances. */
  static class Factory implements AttributedDirectedGraph.AttributedEdgeFactory<
      LatticeTable, Step> {
    public Step createEdge(LatticeTable source, LatticeTable target) {
      throw new UnsupportedOperationException();
    }

    public Step createEdge(LatticeTable source, LatticeTable target,
        Object... attributes) {
      @SuppressWarnings("unchecked") final List<IntPair> keys =
          (List) attributes[0];
      return new Step(source, target, keys);
    }
  }
}

// End Step.java
