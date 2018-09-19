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

import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedSet;
import javax.annotation.Nonnull;

/**
 * Analyzes data sets.
 */
public interface Profiler {
  /** Creates a profile of a data set.
   *
   * @param rows List of rows. Can be iterated over more than once (maybe not
   *             cheaply)
   * @param columns Column definitions
   *
   * @param initialGroups List of combinations of columns that should be
   *                     profiled early, because they may be interesting
   *
   * @return A profile describing relationships within the data set
   */
  Profile profile(Iterable<List<Comparable>> rows, List<Column> columns,
      Collection<ImmutableBitSet> initialGroups);

  /** Column. */
  class Column implements Comparable<Column> {
    public final int ordinal;
    public final String name;

    /** Creates a Column.
     *
     * @param ordinal Unique and contiguous within a particular data set
     * @param name Name of the column
     */
    public Column(int ordinal, String name) {
      this.ordinal = ordinal;
      this.name = name;
    }

    static ImmutableBitSet toOrdinals(Iterable<Column> columns) {
      final ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
      for (Column column : columns) {
        builder.set(column.ordinal);
      }
      return builder.build();
    }

    @Override public int hashCode() {
      return ordinal;
    }

    @Override public boolean equals(Object o) {
      return this == o
          || o instanceof Column
          && ordinal == ((Column) o).ordinal;
    }

    @Override public int compareTo(@Nonnull Column column) {
      return Integer.compare(ordinal, column.ordinal);
    }

    @Override public String toString() {
      return name;
    }
  }

  /** Statistic produced by the profiler. */
  interface Statistic {
    Object toMap(JsonBuilder jsonBuilder);
  }

  /** Whole data set. */
  class RowCount implements Statistic {
    final int rowCount;

    public RowCount(int rowCount) {
      this.rowCount = rowCount;
    }

    public Object toMap(JsonBuilder jsonBuilder) {
      final Map<String, Object> map = jsonBuilder.map();
      map.put("type", "rowCount");
      map.put("rowCount", rowCount);
      return map;
    }
  }

  /** Unique key. */
  class Unique implements Statistic {
    final NavigableSet<Column> columns;

    public Unique(SortedSet<Column> columns) {
      this.columns = ImmutableSortedSet.copyOf(columns);
    }

    public Object toMap(JsonBuilder jsonBuilder) {
      final Map<String, Object> map = jsonBuilder.map();
      map.put("type", "unique");
      map.put("columns", FunctionalDependency.getObjects(jsonBuilder, columns));
      return map;
    }
  }

  /** Functional dependency. */
  class FunctionalDependency implements Statistic {
    final NavigableSet<Column> columns;
    final Column dependentColumn;

    FunctionalDependency(SortedSet<Column> columns, Column dependentColumn) {
      this.columns = ImmutableSortedSet.copyOf(columns);
      this.dependentColumn = dependentColumn;
    }

    public Object toMap(JsonBuilder jsonBuilder) {
      final Map<String, Object> map = jsonBuilder.map();
      map.put("type", "fd");
      map.put("columns", getObjects(jsonBuilder, columns));
      map.put("dependentColumn", dependentColumn.name);
      return map;
    }

    private static List<Object> getObjects(JsonBuilder jsonBuilder,
        NavigableSet<Column> columns) {
      final List<Object> list = jsonBuilder.list();
      for (Column column : columns) {
        list.add(column.name);
      }
      return list;
    }
  }

  /** Value distribution, including cardinality and optionally values, of a
   * column or set of columns. If the set of columns is empty, it describes
   * the number of rows in the entire data set. */
  class Distribution implements Statistic {
    static final MathContext ROUND5 =
        new MathContext(5, RoundingMode.HALF_EVEN);

    static final MathContext ROUND3 =
        new MathContext(3, RoundingMode.HALF_EVEN);

    final NavigableSet<Column> columns;
    final NavigableSet<Comparable> values;
    final double cardinality;
    final int nullCount;
    final double expectedCardinality;
    final boolean minimal;

    /** Creates a Distribution.
     *
     * @param columns Column or columns being described
     * @param values Values of columns, or null if there are too many
     * @param cardinality Number of distinct values
     * @param nullCount Number of rows where this column had a null value;
     * @param expectedCardinality Expected cardinality
     * @param minimal Whether the distribution is not implied by a unique
     *   or functional dependency
     */
    public Distribution(SortedSet<Column> columns, SortedSet<Comparable> values,
        double cardinality, int nullCount, double expectedCardinality,
        boolean minimal) {
      this.columns = ImmutableSortedSet.copyOf(columns);
      this.values = values == null ? null : ImmutableSortedSet.copyOf(values);
      this.cardinality = cardinality;
      this.nullCount = nullCount;
      this.expectedCardinality = expectedCardinality;
      this.minimal = minimal;
    }

    public Object toMap(JsonBuilder jsonBuilder) {
      final Map<String, Object> map = jsonBuilder.map();
      map.put("type", "distribution");
      map.put("columns", FunctionalDependency.getObjects(jsonBuilder, columns));
      if (values != null) {
        List<Object> list = jsonBuilder.list();
        for (Comparable value : values) {
          if (value instanceof java.sql.Date) {
            value = value.toString();
          }
          list.add(value);
        }
        map.put("values", list);
      }
      map.put("cardinality", new BigDecimal(cardinality, ROUND5));
      if (nullCount > 0) {
        map.put("nullCount", nullCount);
      }
      map.put("expectedCardinality",
          new BigDecimal(expectedCardinality, ROUND5));
      map.put("surprise", new BigDecimal(surprise(), ROUND3));
      return map;
    }

    ImmutableBitSet columnOrdinals() {
      return Column.toOrdinals(columns);
    }

    double surprise() {
      return SimpleProfiler.surprise(expectedCardinality, cardinality);
    }
  }

  /** The result of profiling, contains various statistics about the
   * data in a table. */
  class Profile {
    public final RowCount rowCount;
    public final List<FunctionalDependency> functionalDependencyList;
    public final List<Distribution> distributionList;
    public final List<Unique> uniqueList;

    private final Map<ImmutableBitSet, Distribution> distributionMap;
    private final List<Distribution> singletonDistributionList;

    Profile(List<Column> columns, RowCount rowCount,
        Iterable<FunctionalDependency> functionalDependencyList,
        Iterable<Distribution> distributionList, Iterable<Unique> uniqueList) {
      this.rowCount = rowCount;
      this.functionalDependencyList =
          ImmutableList.copyOf(functionalDependencyList);
      this.distributionList = ImmutableList.copyOf(distributionList);
      this.uniqueList = ImmutableList.copyOf(uniqueList);

      final ImmutableMap.Builder<ImmutableBitSet, Distribution> m =
          ImmutableMap.builder();
      for (Distribution distribution : distributionList) {
        m.put(distribution.columnOrdinals(), distribution);
      }
      distributionMap = m.build();

      final ImmutableList.Builder<Distribution> b = ImmutableList.builder();
      for (int i = 0; i < columns.size(); i++) {
        b.add(distributionMap.get(ImmutableBitSet.of(i)));
      }
      singletonDistributionList = b.build();
    }

    public List<Statistic> statistics() {
      return ImmutableList.<Statistic>builder()
          .add(rowCount)
          .addAll(functionalDependencyList)
          .addAll(distributionList)
          .addAll(uniqueList)
          .build();
    }

    public double cardinality(ImmutableBitSet columnOrdinals) {
      final ImmutableBitSet originalOrdinals = columnOrdinals;
      for (;;) {
        final Distribution distribution = distributionMap.get(columnOrdinals);
        if (distribution != null) {
          if (columnOrdinals == originalOrdinals) {
            return distribution.cardinality;
          } else {
            final List<Double> cardinalityList = new ArrayList<>();
            cardinalityList.add(distribution.cardinality);
            for (int ordinal : originalOrdinals.except(columnOrdinals)) {
              final Distribution d = singletonDistributionList.get(ordinal);
              cardinalityList.add(d.cardinality);
            }
            return Lattice.getRowCount(rowCount.rowCount, cardinalityList);
          }
        }
        // Clear the last bit and iterate.
        // Better would be to combine all of our nearest ancestors.
        final List<Integer> list = columnOrdinals.asList();
        columnOrdinals = columnOrdinals.clear(Util.last(list));
      }
    }
  }
}

// End Profiler.java
