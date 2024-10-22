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

import org.checkerframework.checker.initialization.qual.NotOnlyInitialized;
import org.checkerframework.checker.initialization.qual.UnderInitialization;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

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

  /** String representation of {@link #keys}. Computing the string requires a
   * {@link LatticeSpace}, so we pre-compute it before construction. */
  final String keyString;

  private Step(LatticeTable source, LatticeTable target,
      List<IntPair> keys, String keyString) {
    super(source, target);
    this.keys = ImmutableList.copyOf(keys);
    this.keyString = requireNonNull(keyString, "keyString");
    assert IntPair.ORDERING.isStrictlyOrdered(keys); // ordered and unique
  }

  /** Creates a Step. */
  static Step create(LatticeTable source, LatticeTable target,
      List<IntPair> keys, LatticeSpace space) {
    final StringBuilder b = new StringBuilder();
    for (IntPair key : keys) {
      b.append(' ')
          .append(space.fieldName(source, key.source))
          .append(':')
          .append(space.fieldName(target, key.target));
    }
    return new Step(source, target, keys, b.toString());
  }

  @Override public int hashCode() {
    return Objects.hash(source, target, keys);
  }

  @Override public boolean equals(@Nullable Object obj) {
    return this == obj
        || obj instanceof Step
        && ((Step) obj).source.equals(source)
        && ((Step) obj).target.equals(target)
        && ((Step) obj).keys.equals(keys);
  }

  @Override public String toString() {
    return "Step(" + source + ", " + target + "," + keyString + ")";
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
    final boolean noDerivedSourceColumns =
        sourceColumns.stream().allMatch(i ->
            i < sourceTable.getRowType().getFieldCount());
    final boolean noDerivedTargetColumns =
        targetColumns.stream().allMatch(i ->
            i < targetTable.getRowType().getFieldCount());
    final boolean forwardForeignKey = noDerivedSourceColumns
        && noDerivedTargetColumns
        && statisticProvider.isForeignKey(sourceTable, sourceColumns,
            targetTable, targetColumns)
        && statisticProvider.isKey(targetTable, targetColumns);
    final boolean backwardForeignKey = noDerivedSourceColumns
        && noDerivedTargetColumns
        && statisticProvider.isForeignKey(targetTable, targetColumns,
            sourceTable, sourceColumns)
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
  @SuppressWarnings("unused")
  private static double cardinality(SqlStatisticProvider statisticProvider,
      LatticeTable table) {
    return statisticProvider.tableCardinality(table.t);
  }

  /** Creates {@link Step} instances. */
  static class Factory implements AttributedDirectedGraph.AttributedEdgeFactory<
      LatticeTable, Step> {
    private final @NotOnlyInitialized LatticeSpace space;

    @SuppressWarnings("type.argument.type.incompatible")
    Factory(@UnderInitialization LatticeSpace space) {
      this.space = requireNonNull(space, "space");
    }

    @Override public Step createEdge(LatticeTable source, LatticeTable target) {
      throw new UnsupportedOperationException();
    }

    @Override public Step createEdge(LatticeTable source, LatticeTable target,
        Object... attributes) {
      @SuppressWarnings("unchecked") final List<IntPair> keys =
          (List) attributes[0];
      return Step.create(source, target, keys, space);
    }
  }
}
