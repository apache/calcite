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
package org.apache.calcite.profile;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.rel.metadata.NullSentinel;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.PartiallyOrderedSet;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.annotation.Nonnull;

/**
 * Basic implementation of {@link Profiler}.
 */
public class SimpleProfiler implements Profiler {

  public Profile profile(Iterable<List<Comparable>> rows,
      final List<Column> columns, Collection<ImmutableBitSet> initialGroups) {
    Util.discard(initialGroups); // this profiler ignores initial groups
    return new Run(columns).profile(rows);
  }

  /** Returns a measure of how much an actual value differs from expected.
   * The formula is {@code abs(expected - actual) / (expected + actual)}.
   *
   * <p>Examples:<ul>
   *   <li>surprise(e, a) is always between 0 and 1;
   *   <li>surprise(e, a) is 0 if e = a;
   *   <li>surprise(e, 0) is 1 if e &gt; 0;
   *   <li>surprise(0, a) is 1 if a &gt; 0;
   *   <li>surprise(5, 0) is 100%;
   *   <li>surprise(5, 3) is 25%;
   *   <li>surprise(5, 4) is 11%;
   *   <li>surprise(5, 5) is 0%;
   *   <li>surprise(5, 6) is 9%;
   *   <li>surprise(5, 16) is 52%;
   *   <li>surprise(5, 100) is 90%;
   * </ul>
   *
   * @param expected Expected value
   * @param actual Actual value
   * @return Measure of how much expected deviates from actual
   */
  public static double surprise(double expected, double actual) {
    if (expected == actual) {
      return 0d;
    }
    final double sum = expected + actual;
    if (sum <= 0d) {
      return 1d;
    }
    return Math.abs(expected - actual) / sum;
  }

  /** A run of the profiler. */
  static class Run {
    private final List<Column> columns;
    final List<Space> spaces = new ArrayList<>();
    final List<Space> singletonSpaces;
    final List<Statistic> statistics = new ArrayList<>();
    final PartiallyOrderedSet.Ordering<Space> ordering =
        (e1, e2) -> e2.columnOrdinals.contains(e1.columnOrdinals);
    final PartiallyOrderedSet<Space> results =
        new PartiallyOrderedSet<>(ordering);
    final PartiallyOrderedSet<Space> keyResults =
        new PartiallyOrderedSet<>(ordering);
    private final List<ImmutableBitSet> keyOrdinalLists =
        new ArrayList<>();

    Run(final List<Column> columns) {
      for (Ord<Column> column : Ord.zip(columns)) {
        if (column.e.ordinal != column.i) {
          throw new IllegalArgumentException();
        }
      }
      this.columns = columns;
      this.singletonSpaces =
          new ArrayList<>(Collections.nCopies(columns.size(), null));
      for (ImmutableBitSet ordinals
          : ImmutableBitSet.range(columns.size()).powerSet()) {
        final Space space = new Space(ordinals, toColumns(ordinals));
        spaces.add(space);
        if (ordinals.cardinality() == 1) {
          singletonSpaces.set(ordinals.nth(0), space);
        }
      }
    }

    Profile profile(Iterable<List<Comparable>> rows) {
      final List<Comparable> values = new ArrayList<>();
      int rowCount = 0;
      for (final List<Comparable> row : rows) {
        ++rowCount;
      joint:
        for (Space space : spaces) {
          values.clear();
          for (Column column : space.columns) {
            final Comparable value = row.get(column.ordinal);
            values.add(value);
            if (value == NullSentinel.INSTANCE) {
              space.nullCount++;
              continue joint;
            }
          }
          space.values.add(FlatLists.ofComparable(values));
        }
      }

      // Populate unique keys
      // If [x, y] is a key,
      // then [x, y, z] is a key but not intersecting,
      // and [x, y] => [a] is a functional dependency but not interesting,
      // and [x, y, z] is not an interesting distribution.
      final Map<ImmutableBitSet, Distribution> distributions = new HashMap<>();
      for (Space space : spaces) {
        if (space.values.size() == rowCount
            && !containsKey(space.columnOrdinals, false)) {
          // We have discovered a new key.
          // It is not an existing key or a super-set of a key.
          statistics.add(new Unique(space.columns));
          space.unique = true;
          keyOrdinalLists.add(space.columnOrdinals);
        }

        int nonMinimal = 0;
      dependents:
        for (Space s : results.getDescendants(space)) {
          if (s.cardinality() == space.cardinality()) {
            // We have discovered a sub-set that has the same cardinality.
            // The column(s) that are not in common are functionally
            // dependent.
            final ImmutableBitSet dependents =
                space.columnOrdinals.except(s.columnOrdinals);
            for (int i : s.columnOrdinals) {
              final Space s1 = singletonSpaces.get(i);
              final ImmutableBitSet rest = s.columnOrdinals.clear(i);
              for (ImmutableBitSet dependent : s1.dependents) {
                if (rest.contains(dependent)) {
                  // The "key" of this functional dependency is not minimal.
                  // For instance, if we know that
                  //   (a) -> x
                  // then
                  //   (a, b, x) -> y
                  // is not minimal; we could say the same with a smaller key:
                  //   (a, b) -> y
                  ++nonMinimal;
                  continue dependents;
                }
              }
            }
            for (int dependent : dependents) {
              final Space s1 = singletonSpaces.get(dependent);
              for (ImmutableBitSet d : s1.dependents) {
                if (s.columnOrdinals.contains(d)) {
                  ++nonMinimal;
                  continue dependents;
                }
              }
            }
            space.dependencies.or(dependents.toBitSet());
            for (int d : dependents) {
              singletonSpaces.get(d).dependents.add(s.columnOrdinals);
            }
          }
        }

        int nullCount;
        final SortedSet<Comparable> valueSet;
        if (space.columns.size() == 1) {
          nullCount = space.nullCount;
          valueSet = ImmutableSortedSet.copyOf(
              Iterables.transform(space.values, Iterables::getOnlyElement));
        } else {
          nullCount = -1;
          valueSet = null;
        }
        double expectedCardinality;
        final double cardinality = space.cardinality();
        switch (space.columns.size()) {
        case 0:
          expectedCardinality = 1d;
          break;
        case 1:
          expectedCardinality = rowCount;
          break;
        default:
          expectedCardinality = rowCount;
          for (Column column : space.columns) {
            final Distribution d1 =
                distributions.get(ImmutableBitSet.of(column.ordinal));
            final Distribution d2 =
                distributions.get(space.columnOrdinals.clear(column.ordinal));
            final double d =
                Lattice.getRowCount(rowCount, d1.cardinality, d2.cardinality);
            expectedCardinality = Math.min(expectedCardinality, d);
          }
        }
        final boolean minimal = nonMinimal == 0
            && !space.unique
            && !containsKey(space.columnOrdinals, true);
        final Distribution distribution =
            new Distribution(space.columns, valueSet, cardinality, nullCount,
                expectedCardinality, minimal);
        statistics.add(distribution);
        distributions.put(space.columnOrdinals, distribution);

        if (distribution.minimal) {
          results.add(space);
        }
      }

      for (Space s : singletonSpaces) {
        for (ImmutableBitSet dependent : s.dependents) {
          if (!containsKey(dependent, false)
              && !hasNull(dependent)) {
            statistics.add(
                new FunctionalDependency(toColumns(dependent),
                    Iterables.getOnlyElement(s.columns)));
          }
        }
      }
      return new Profile(columns, new RowCount(rowCount),
          Iterables.filter(statistics, FunctionalDependency.class),
          Iterables.filter(statistics, Distribution.class),
          Iterables.filter(statistics, Unique.class));
    }

    /** Returns whether a set of column ordinals
     * matches or contains a unique key.
     * If {@code strict}, it must contain a unique key. */
    private boolean containsKey(ImmutableBitSet ordinals, boolean strict) {
      for (ImmutableBitSet keyOrdinals : keyOrdinalLists) {
        if (ordinals.contains(keyOrdinals)) {
          return !(strict && keyOrdinals.equals(ordinals));
        }
      }
      return false;
    }

    private boolean hasNull(ImmutableBitSet columnOrdinals) {
      for (Integer columnOrdinal : columnOrdinals) {
        if (singletonSpaces.get(columnOrdinal).nullCount > 0) {
          return true;
        }
      }
      return false;
    }

    private ImmutableSortedSet<Column> toColumns(Iterable<Integer> ordinals) {
      return ImmutableSortedSet.copyOf(
          Iterables.transform(ordinals, columns::get));
    }
  }

  /** Work space for a particular combination of columns. */
  static class Space implements Comparable<Space> {
    final ImmutableBitSet columnOrdinals;
    final ImmutableSortedSet<Column> columns;
    int nullCount;
    final SortedSet<FlatLists.ComparableList<Comparable>> values =
        new TreeSet<>();
    boolean unique;
    final BitSet dependencies = new BitSet();
    final Set<ImmutableBitSet> dependents = new HashSet<>();

    Space(ImmutableBitSet columnOrdinals, Iterable<Column> columns) {
      this.columnOrdinals = columnOrdinals;
      this.columns = ImmutableSortedSet.copyOf(columns);
    }

    @Override public int hashCode() {
      return columnOrdinals.hashCode();
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof Space
          && columnOrdinals.equals(((Space) o).columnOrdinals);
    }

    public int compareTo(@Nonnull Space o) {
      return columnOrdinals.equals(o.columnOrdinals) ? 0
          : columnOrdinals.contains(o.columnOrdinals) ? 1
              : -1;
    }

    /** Number of distinct values. Null is counted as a value, if present. */
    public double cardinality() {
      return values.size() + (nullCount > 0 ? 1 : 0);
    }
  }
}

// End SimpleProfiler.java
