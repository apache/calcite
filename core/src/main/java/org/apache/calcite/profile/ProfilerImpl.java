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

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.rel.metadata.NullSentinel;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.PartiallyOrderedSet;
import org.apache.calcite.util.Util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.yahoo.sketches.hll.HllSketch;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;

import static org.apache.calcite.profile.ProfilerImpl.CompositeCollector.OF;

/**
 * Implementation of {@link Profiler} that only investigates "interesting"
 * combinations of columns.
 */
public class ProfilerImpl implements Profiler {
  /** The number of combinations to consider per pass.
   * The number is determined by memory, but a value of 1,000 is typical.
   * You need 2KB memory per sketch, and one sketch for each combination. */
  private final int combinationsPerPass;

  /** The minimum number of combinations considered "interesting". After that,
   * a combination is only considered "interesting" if its surprise is greater
   * than the median surprise. */
  private final int interestingCount;

  /** Whether a successor is considered interesting enough to analyze. */
  private final Predicate<Pair<Space, Column>> predicate;

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a {@code ProfilerImpl}.
   *
   * @param combinationsPerPass Maximum number of columns (or combinations of
   *   columns) to compute each pass
   * @param interestingCount Minimum number of combinations considered
   *   interesting
   * @param predicate Whether a successor is considered interesting enough to
   *   analyze
   */
  ProfilerImpl(int combinationsPerPass,
      int interestingCount, Predicate<Pair<Space, Column>> predicate) {
    Preconditions.checkArgument(combinationsPerPass > 2);
    Preconditions.checkArgument(interestingCount > 2);
    this.combinationsPerPass = combinationsPerPass;
    this.interestingCount = interestingCount;
    this.predicate = predicate;
  }

  public Profile profile(Iterable<List<Comparable>> rows,
      final List<Column> columns, Collection<ImmutableBitSet> initialGroups) {
    return new Run(columns, initialGroups).profile(rows);
  }

  /** A run of the profiler. */
  class Run {
    private final List<Column> columns;
    final PartiallyOrderedSet<ImmutableBitSet> keyPoset =
        new PartiallyOrderedSet<>(
            PartiallyOrderedSet.BIT_SET_INCLUSION_ORDERING);
    final Map<ImmutableBitSet, Distribution> distributions = new HashMap<>();
    /** List of spaces that have one column. */
    final List<Space> singletonSpaces;
    /** Combinations of columns that we have computed but whose successors have
     * not yet been computed. We may add some of those successors to
     * {@link #spaceQueue}. */
    final Queue<Space> doneQueue =
        new PriorityQueue<>(100, (s0, s1) -> {
          // The space with 0 columns is more interesting than
          // any space with 1 column, and so forth.
          // For spaces with 2 or more columns we compare "surprise":
          // how many fewer values did it have than expected?
          int c = Integer.compare(s0.columns.size(), s1.columns.size());
          if (c == 0) {
            c = Double.compare(s0.surprise(), s1.surprise());
          }
          return c;
        });
    final SurpriseQueue surprises;

    /** Combinations of columns that we will compute next pass. */
    final Deque<ImmutableBitSet> spaceQueue = new ArrayDeque<>();
    final List<Unique> uniques = new ArrayList<>();
    final List<FunctionalDependency> functionalDependencies = new ArrayList<>();
    /** Column ordinals that have ever been placed on {@link #spaceQueue}.
     * Ensures that we do not calculate the same combination more than once,
     * even though we generate a column set from multiple parents. */
    final Set<ImmutableBitSet> resultSet = new HashSet<>();
    final PartiallyOrderedSet<Space> results =
        new PartiallyOrderedSet<>((e1, e2) ->
            e2.columnOrdinals.contains(e1.columnOrdinals));
    private final List<ImmutableBitSet> keyOrdinalLists =
        new ArrayList<>();
    private int rowCount;

    /**
     * Creates a Run.
     *
     * @param columns List of columns
     *
     * @param initialGroups List of combinations of columns that should be
     *                     profiled early, because they may be interesting
     */
    Run(final List<Column> columns, Collection<ImmutableBitSet> initialGroups) {
      this.columns = ImmutableList.copyOf(columns);
      for (Ord<Column> column : Ord.zip(columns)) {
        if (column.e.ordinal != column.i) {
          throw new IllegalArgumentException();
        }
      }
      this.singletonSpaces =
          new ArrayList<>(Collections.nCopies(columns.size(), (Space) null));
      if (combinationsPerPass > Math.pow(2D, columns.size())) {
        // There are not many columns. We can compute all combinations in the
        // first pass.
        for (ImmutableBitSet ordinals
            : ImmutableBitSet.range(columns.size()).powerSet()) {
          spaceQueue.add(ordinals);
        }
      } else {
        // We will need to take multiple passes.
        // Pass 0, just put the empty combination on the queue.
        // Next pass, we will do its successors, the singleton combinations.
        spaceQueue.add(ImmutableBitSet.of());
        spaceQueue.addAll(initialGroups);
        if (columns.size() < combinationsPerPass) {
          // There are not very many columns. Compute the singleton
          // groups in pass 0.
          for (Column column : columns) {
            spaceQueue.add(ImmutableBitSet.of(column.ordinal));
          }
        }
      }
      // The surprise queue must have enough room for all singleton groups
      // plus all initial groups.
      surprises = new SurpriseQueue(1 + columns.size() + initialGroups.size(),
          interestingCount);
    }

    Profile profile(Iterable<List<Comparable>> rows) {
      int pass = 0;
      for (;;) {
        final List<Space> spaces = nextBatch(pass);
        if (spaces.isEmpty()) {
          break;
        }
        pass(pass++, spaces, rows);
      }

      for (Space s : singletonSpaces) {
        for (ImmutableBitSet dependent : s.dependents) {
          functionalDependencies.add(
              new FunctionalDependency(toColumns(dependent),
                  Iterables.getOnlyElement(s.columns)));
        }
      }
      return new Profile(columns, new RowCount(rowCount),
          functionalDependencies, distributions.values(), uniques);
    }

    /** Populates {@code spaces} with the next batch.
     * Returns an empty list if done. */
    List<Space> nextBatch(int pass) {
      final List<Space> spaces = new ArrayList<>();
    loop:
      for (;;) {
        if (spaces.size() >= combinationsPerPass) {
          // We have enough for the next pass.
          return spaces;
        }
        // First, see if there is a space we did have room for last pass.
        final ImmutableBitSet ordinals = spaceQueue.poll();
        if (ordinals != null) {
          final Space space = new Space(this, ordinals, toColumns(ordinals));
          spaces.add(space);
          if (ordinals.cardinality() == 1) {
            singletonSpaces.set(ordinals.nth(0), space);
          }
        } else {
          // Next, take a space that was done last time, generate its
          // successors, and add the interesting ones to the space queue.
          for (;;) {
            final Space doneSpace = doneQueue.poll();
            if (doneSpace == null) {
              // There are no more done spaces. We're done.
              return spaces;
            }
            if (doneSpace.columnOrdinals.cardinality() > 4) {
              // Do not generate successors for groups with lots of columns,
              // probably initial groups
              continue;
            }
            for (Column column : columns) {
              if (!doneSpace.columnOrdinals.get(column.ordinal)) {
                if (pass == 0
                    || doneSpace.columnOrdinals.cardinality() == 0
                    || !containsKey(
                        doneSpace.columnOrdinals.set(column.ordinal))
                    && predicate.test(Pair.of(doneSpace, column))) {
                  final ImmutableBitSet nextOrdinals =
                      doneSpace.columnOrdinals.set(column.ordinal);
                  if (resultSet.add(nextOrdinals)) {
                    spaceQueue.add(nextOrdinals);
                  }
                }
              }
            }
            // We've converted at a space into at least one interesting
            // successor.
            if (!spaceQueue.isEmpty()) {
              continue loop;
            }
          }
        }
      }
    }

    private boolean containsKey(ImmutableBitSet ordinals) {
      for (ImmutableBitSet keyOrdinals : keyOrdinalLists) {
        if (ordinals.contains(keyOrdinals)) {
          return true;
        }
      }
      return false;
    }

    void pass(int pass, List<Space> spaces, Iterable<List<Comparable>> rows) {
      if (CalciteSystemProperty.DEBUG.value()) {
        System.out.println("pass: " + pass
            + ", spaces.size: " + spaces.size()
            + ", distributions.size: " + distributions.size());
      }

      for (Space space : spaces) {
        space.collector = Collector.create(space, 1000);
      }

      int rowCount = 0;
      for (final List<Comparable> row : rows) {
        ++rowCount;
        for (Space space : spaces) {
          space.collector.add(row);
        }
      }

      // Populate unique keys.
      // If [x, y] is a key,
      // then [x, y, z] is a non-minimal key (therefore not interesting),
      // and [x, y] => [a] is a functional dependency but not interesting,
      // and [x, y, z] is not an interesting distribution.
      for (Space space : spaces) {
        space.collector.finish();
        space.collector = null;
//        results.add(space);

        int nonMinimal = 0;
      dependents:
        for (Space s : results.getDescendants(space)) {
          if (s.cardinality == space.cardinality) {
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
        if (nonMinimal > 0) {
          continue;
        }
        final String s = space.columns.toString(); // for debug
        Util.discard(s);
        double expectedCardinality =
            expectedCardinality(rowCount, space.columnOrdinals);

        final boolean minimal = nonMinimal == 0
            && !space.unique
            && !containsKey(space.columnOrdinals);
        space.expectedCardinality = expectedCardinality;
        if (minimal) {
          final Distribution distribution =
              new Distribution(space.columns, space.valueSet, space.cardinality,
                  space.nullCount, expectedCardinality, minimal);
          final double surprise = distribution.surprise();
          if (CalciteSystemProperty.DEBUG.value() && surprise > 0.1d) {
            System.out.println(distribution.columnOrdinals()
                + " " + distribution.columns
                + ", cardinality: " + distribution.cardinality
                + ", expected: " + distribution.expectedCardinality
                + ", surprise: " + distribution.surprise());
          }
          if (surprises.offer(surprise)) {
            distributions.put(space.columnOrdinals, distribution);
            keyPoset.add(space.columnOrdinals);
            doneQueue.add(space);
          }
        }

        if (space.cardinality == rowCount) {
          // We have discovered a new key. It is not a super-set of a key.
          uniques.add(new Unique(space.columns));
          keyOrdinalLists.add(space.columnOrdinals);
          space.unique = true;
        }
      }

      if (pass == 0) {
        this.rowCount = rowCount;
      }
    }

    /** Estimates the cardinality of a collection of columns represented by
     * {@code columnOrdinals}, drawing on existing distributions. */
    private double cardinality(double rowCount, ImmutableBitSet columns) {
      final Distribution distribution = distributions.get(columns);
      if (distribution != null) {
        return distribution.cardinality;
      } else {
        return expectedCardinality(rowCount, columns);
      }
    }

    /** Estimates the cardinality of a collection of columns represented by
     * {@code columnOrdinals}, drawing on existing distributions. Does not
     * look in the distribution map for this column set. */
    private double expectedCardinality(double rowCount,
        ImmutableBitSet columns) {
      switch (columns.cardinality()) {
      case 0:
        return 1d;
      case 1:
        return rowCount;
      default:
        double c = rowCount;
        for (ImmutableBitSet bitSet : keyPoset.getParents(columns, true)) {
          if (bitSet.isEmpty()) {
            // If the parent is the empty group (i.e. "GROUP BY ()", the grand
            // total) we cannot improve on the estimate.
            continue;
          }
          final Distribution d1 = distributions.get(bitSet);
          final double c2 = cardinality(rowCount, columns.except(bitSet));
          final double d = Lattice.getRowCount(rowCount, d1.cardinality, c2);
          c = Math.min(c, d);
        }
        for (ImmutableBitSet bitSet : keyPoset.getChildren(columns, true)) {
          final Distribution d1 = distributions.get(bitSet);
          c = Math.min(c, d1.cardinality);
        }
        return c;
      }
    }


    private ImmutableSortedSet<Column> toColumns(Iterable<Integer> ordinals) {
      return ImmutableSortedSet.copyOf(
          Iterables.transform(ordinals, columns::get));
    }
  }

  /** Work space for a particular combination of columns. */
  static class Space {
    private final Run run;
    final ImmutableBitSet columnOrdinals;
    final ImmutableSortedSet<Column> columns;
    boolean unique;
    final BitSet dependencies = new BitSet();
    final Set<ImmutableBitSet> dependents = new HashSet<>();
    double expectedCardinality;
    Collector collector;
    /** Assigned by {@link Collector#finish()}. */
    int nullCount;
    /** Number of distinct values. Null is counted as a value, if present.
     * Assigned by {@link Collector#finish()}. */
    int cardinality;
    /** Assigned by {@link Collector#finish()}. */
    SortedSet<Comparable> valueSet;

    Space(Run run, ImmutableBitSet columnOrdinals, Iterable<Column> columns) {
      this.run = run;
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

    /** Returns the distribution created from this space, or null if no
     * distribution has been registered yet. */
    public Distribution distribution() {
      return run.distributions.get(columnOrdinals);
    }

    double surprise() {
      return SimpleProfiler.surprise(expectedCardinality, cardinality);
    }
  }

  /** Builds a {@link org.apache.calcite.profile.ProfilerImpl}. */
  public static class Builder {
    int combinationsPerPass = 100;
    Predicate<Pair<Space, Column>> predicate = p -> true;

    public ProfilerImpl build() {
      return new ProfilerImpl(combinationsPerPass, 200, predicate);
    }

    public Builder withPassSize(int passSize) {
      this.combinationsPerPass = passSize;
      return this;
    }

    public Builder withMinimumSurprise(double v) {
      predicate =
          spaceColumnPair -> {
            final Space space = spaceColumnPair.left;
            return false;
          };
      return this;
    }
  }

  /** Collects values of a column or columns. */
  abstract static class Collector {
    protected final Space space;

    Collector(Space space) {
      this.space = space;
    }

    abstract void add(List<Comparable> row);
    abstract void finish();

    /** Creates an initial collector of the appropriate kind. */
    public static Collector create(Space space, int sketchThreshold) {
      final List<Integer> columnOrdinalList = space.columnOrdinals.asList();
      if (columnOrdinalList.size() == 1) {
        return new SingletonCollector(space, columnOrdinalList.get(0),
            sketchThreshold);
      } else {
        return new CompositeCollector(space,
            (int[]) Primitive.INT.toArray(columnOrdinalList), sketchThreshold);
      }
    }
  }

  /** Collector that collects values of a single column. */
  static class SingletonCollector extends Collector {
    final SortedSet<Comparable> values = new TreeSet<>();
    final int columnOrdinal;
    final int sketchThreshold;
    int nullCount = 0;

    SingletonCollector(Space space, int columnOrdinal, int sketchThreshold) {
      super(space);
      this.columnOrdinal = columnOrdinal;
      this.sketchThreshold = sketchThreshold;
    }

    public void add(List<Comparable> row) {
      final Comparable v = row.get(columnOrdinal);
      if (v == NullSentinel.INSTANCE) {
        nullCount++;
      } else {
        if (values.add(v) && values.size() == sketchThreshold) {
          // Too many values. Switch to a sketch collector.
          final HllSingletonCollector collector =
              new HllSingletonCollector(space, columnOrdinal);
          for (Comparable value : values) {
            collector.add(value);
          }
          space.collector = collector;
        }
      }
    }

    public void finish() {
      space.nullCount = nullCount;
      space.cardinality = values.size() + (nullCount > 0 ? 1 : 0);
      space.valueSet = values.size() < 20 ? values : null;
    }
  }

  /** Collector that collects two or more column values in a tree set. */
  static class CompositeCollector extends Collector {
    protected static final ImmutableBitSet OF = ImmutableBitSet.of(2, 13);
    final Set<FlatLists.ComparableList> values = new HashSet<>();
    final int[] columnOrdinals;
    final Comparable[] columnValues;
    int nullCount = 0;
    private final int sketchThreshold;

    CompositeCollector(Space space, int[] columnOrdinals, int sketchThreshold) {
      super(space);
      this.columnOrdinals = columnOrdinals;
      this.columnValues = new Comparable[columnOrdinals.length];
      this.sketchThreshold = sketchThreshold;
    }

    public void add(List<Comparable> row) {
      if (space.columnOrdinals.equals(OF)) {
        Util.discard(0);
      }
      int nullCountThisRow = 0;
      for (int i = 0, length = columnOrdinals.length; i < length; i++) {
        final Comparable value = row.get(columnOrdinals[i]);
        if (value == NullSentinel.INSTANCE) {
          if (nullCountThisRow++ == 0) {
            nullCount++;
          }
        }
        columnValues[i] = value;
      }
      //noinspection unchecked
      if (((Set) values).add(FlatLists.copyOf(columnValues))
          && values.size() == sketchThreshold) {
        // Too many values. Switch to a sketch collector.
        final HllCompositeCollector collector =
            new HllCompositeCollector(space, columnOrdinals);
        final List<Comparable> list =
            new ArrayList<>(
                Collections.nCopies(columnOrdinals[columnOrdinals.length - 1]
                        + 1,
                    null));
        for (FlatLists.ComparableList value : this.values) {
          for (int i = 0; i < value.size(); i++) {
            Comparable c = (Comparable) value.get(i);
            list.set(columnOrdinals[i], c);
          }
          collector.add(list);
        }
        space.collector = collector;
      }
    }

    public void finish() {
      // number of input rows (not distinct values)
      // that were null or partially null
      space.nullCount = nullCount;
      space.cardinality = values.size() + (nullCount > 0 ? 1 : 0);
      space.valueSet = null;
    }

  }

  /** Collector that collects two or more column values into a HyperLogLog
   * sketch. */
  abstract static class HllCollector extends Collector {
    final HllSketch sketch;
    int nullCount = 0;

    static final long[] NULL_BITS = {0x9f77d57e93167a16L};

    HllCollector(Space space) {
      super(space);
      this.sketch = HllSketch.builder().build();
    }

    protected void add(Comparable value) {
      if (value == NullSentinel.INSTANCE) {
        sketch.update(NULL_BITS);
      } else if (value instanceof String) {
        sketch.update((String) value);
      } else if (value instanceof Double) {
        sketch.update((Double) value);
      } else if (value instanceof Float) {
        sketch.update((Float) value);
      } else if (value instanceof Long) {
        sketch.update((Long) value);
      } else if (value instanceof Number) {
        sketch.update(((Number) value).longValue());
      } else {
        sketch.update(value.toString());
      }
    }

    public void finish() {
      space.nullCount = nullCount;
      space.cardinality = (int) sketch.getEstimate();
      space.valueSet = null;
    }
  }

  /** Collector that collects one column value into a HyperLogLog sketch. */
  static class HllSingletonCollector extends HllCollector {
    final int columnOrdinal;

    HllSingletonCollector(Space space, int columnOrdinal) {
      super(space);
      this.columnOrdinal = columnOrdinal;
    }

    public void add(List<Comparable> row) {
      final Comparable value = row.get(columnOrdinal);
      if (value == NullSentinel.INSTANCE) {
        nullCount++;
        sketch.update(NULL_BITS);
      } else {
        add(value);
      }
    }
  }

  /** Collector that collects two or more column values into a HyperLogLog
   * sketch. */
  static class HllCompositeCollector extends HllCollector {
    private final int[] columnOrdinals;
    private final ByteBuffer buf = ByteBuffer.allocate(1024);

    HllCompositeCollector(Space space, int[] columnOrdinals) {
      super(space);
      this.columnOrdinals = columnOrdinals;
    }

    public void add(List<Comparable> row) {
      if (space.columnOrdinals.equals(OF)) {
        Util.discard(0);
      }
      int nullCountThisRow = 0;
      buf.clear();
      for (int columnOrdinal : columnOrdinals) {
        final Comparable value = row.get(columnOrdinal);
        if (value == NullSentinel.INSTANCE) {
          if (nullCountThisRow++ == 0) {
            nullCount++;
          }
          buf.put((byte) 0);
        } else if (value instanceof String) {
          buf.put((byte) 1)
              .put(((String) value).getBytes(StandardCharsets.UTF_8));
        } else if (value instanceof Double) {
          buf.put((byte) 2).putDouble((Double) value);
        } else if (value instanceof Float) {
          buf.put((byte) 3).putFloat((Float) value);
        } else if (value instanceof Long) {
          buf.put((byte) 4).putLong((Long) value);
        } else if (value instanceof Integer) {
          buf.put((byte) 5).putInt((Integer) value);
        } else if (value instanceof Boolean) {
          buf.put((Boolean) value ? (byte) 6 : (byte) 7);
        } else {
          buf.put((byte) 8)
              .put(value.toString().getBytes(StandardCharsets.UTF_8));
        }
      }
      sketch.update(Arrays.copyOf(buf.array(), buf.position()));
    }
  }

  /** A priority queue of the last N surprise values. Accepts a new value if
   * the queue is not yet full, or if its value is greater than the median value
   * over the last N. */
  static class SurpriseQueue {
    private final int warmUpCount;
    private final int size;
    int count = 0;
    final Deque<Double> deque = new ArrayDeque<>();
    final PriorityQueue<Double> priorityQueue =
        new PriorityQueue<>(11, Ordering.natural());

    SurpriseQueue(int warmUpCount, int size) {
      this.warmUpCount = warmUpCount;
      this.size = size;
      Preconditions.checkArgument(warmUpCount > 3);
      Preconditions.checkArgument(size > 0);
    }

    @Override public String toString() {
      return "min: " + priorityQueue.peek()
          + ", contents: " + deque.toString();
    }

    boolean isValid() {
      if (CalciteSystemProperty.DEBUG.value()) {
        System.out.println(toString());
      }
      assert deque.size() == priorityQueue.size();
      if (count > size) {
        assert deque.size() == size;
      }
      return true;
    }

    boolean offer(double d) {
      boolean b;
      if (count++ < warmUpCount || d > priorityQueue.peek()) {
        if (priorityQueue.size() >= size) {
          priorityQueue.remove(deque.pop());
        }
        priorityQueue.add(d);
        deque.add(d);
        b = true;
      } else {
        b = false;
      }
      if (CalciteSystemProperty.DEBUG.value()) {
        System.out.println("offer " + d
            + " min " + priorityQueue.peek()
            + " accepted " + b);
      }
      return b;
    }
  }
}

// End ProfilerImpl.java
