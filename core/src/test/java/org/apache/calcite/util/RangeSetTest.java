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
package org.apache.calcite.util;

import org.apache.calcite.linq4j.Ord;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit test for {@link RangeSets} and other utilities relating to Guava
 * {@link Range} and {@link RangeSet}.
 */
@SuppressWarnings("UnstableApiUsage")
class RangeSetTest {
  /** Tests {@link RangeSets#minus(RangeSet, Range)}. */
  @SuppressWarnings("UnstableApiUsage")
  @Test void testRangeSetMinus() {
    final RangeSet<Integer> setNone = ImmutableRangeSet.of();
    final RangeSet<Integer> setAll = setNone.complement();
    final RangeSet<Integer> setGt2 = ImmutableRangeSet.of(Range.greaterThan(2));
    final RangeSet<Integer> setGt1 = ImmutableRangeSet.of(Range.greaterThan(1));
    final RangeSet<Integer> setGe1 = ImmutableRangeSet.of(Range.atLeast(1));
    final RangeSet<Integer> setGt0 = ImmutableRangeSet.of(Range.greaterThan(0));
    final RangeSet<Integer> setComplex =
        ImmutableRangeSet.<Integer>builder()
            .add(Range.closed(0, 2))
            .add(Range.singleton(3))
            .add(Range.greaterThan(5))
            .build();
    assertThat(setComplex.toString(), is("[[0‥2], [3‥3], (5‥+∞)]"));

    assertThat(RangeSets.minus(setAll, Range.singleton(1)).toString(),
        is("[(-∞‥1), (1‥+∞)]"));
    assertThat(RangeSets.minus(setNone, Range.singleton(1)), is(setNone));
    assertThat(RangeSets.minus(setGt2, Range.singleton(1)), is(setGt2));
    assertThat(RangeSets.minus(setGt1, Range.singleton(1)), is(setGt1));
    assertThat(RangeSets.minus(setGe1, Range.singleton(1)), is(setGt1));
    assertThat(RangeSets.minus(setGt0, Range.singleton(1)).toString(),
        is("[(0‥1), (1‥+∞)]"));
    assertThat(RangeSets.minus(setComplex, Range.singleton(1)).toString(),
        is("[[0‥1), (1‥2], [3‥3], (5‥+∞)]"));
    assertThat(RangeSets.minus(setComplex, Range.singleton(2)).toString(),
        is("[[0‥2), [3‥3], (5‥+∞)]"));
    assertThat(RangeSets.minus(setComplex, Range.singleton(3)).toString(),
        is("[[0‥2], (5‥+∞)]"));
    assertThat(RangeSets.minus(setComplex, Range.open(2, 3)).toString(),
        is("[[0‥2], [3‥3], (5‥+∞)]"));
    assertThat(RangeSets.minus(setComplex, Range.closed(2, 3)).toString(),
        is("[[0‥2), (5‥+∞)]"));
    assertThat(RangeSets.minus(setComplex, Range.closed(2, 7)).toString(),
        is("[[0‥2), (7‥+∞)]"));
  }

  /** Tests {@link RangeSets#isPoint(Range)}. */
  @Test void testRangeSetIsPoint() {
    assertThat(RangeSets.isPoint(Range.singleton(0)), is(true));
    assertThat(RangeSets.isPoint(Range.closed(0, 0)), is(true));
    assertThat(RangeSets.isPoint(Range.closed(0, 1)), is(false));
    assertThat(RangeSets.isPoint(Range.openClosed(0, 1)), is(false));

    // The integer range '0 > x and x < 2' contains only one valid integer
    // but it is not a point.
    assertThat(RangeSets.isPoint(Range.open(0, 2)), is(false));

    assertThat(RangeSets.isPoint(Range.lessThan(0)), is(false));
    assertThat(RangeSets.isPoint(Range.atMost(0)), is(false));
    assertThat(RangeSets.isPoint(Range.greaterThan(0)), is(false));
    assertThat(RangeSets.isPoint(Range.atLeast(0)), is(false));
  }

  /** Tests {@link RangeSets#countPoints(RangeSet)}. */
  @Test void testRangeCountPoints() {
    final Fixture f = new Fixture();
    assertThat(RangeSets.countPoints(f.empty), is(0));
    assertThat(RangeSets.countPoints(f.zeroRangeSet), is(1));
    assertThat(RangeSets.countPoints(f.rangeSet), is(1));
    final ImmutableRangeSet<Integer> set =
        ImmutableRangeSet.<Integer>builder()
            .add(Range.singleton(0))
            .add(Range.open(1, 2))
            .add(Range.singleton(3))
            .add(Range.atLeast(4)).build();
    assertThat(RangeSets.countPoints(set), is(2));
    final ImmutableRangeSet<Integer> set2 =
        ImmutableRangeSet.<Integer>builder()
            .add(Range.open(1, 2))
            .add(Range.atLeast(4)).build();
    assertThat(RangeSets.countPoints(set2), is(0));
  }

  /** Tests {@link RangeSets#map} and {@link RangeSets#forEach}. */
  @Test void testRangeMap() {
    final StringBuilder b = new StringBuilder();
    final RangeSets.Handler<Integer, StringBuilder> h =
        new RangeSets.Handler<Integer, StringBuilder>() {
          @Override public StringBuilder all() {
            return b.append("all()");
          }

          @Override public StringBuilder atLeast(Integer lower) {
            return b.append("atLeast(").append(lower).append(")");
          }

          @Override public StringBuilder atMost(Integer upper) {
            return b.append("atMost(").append(upper).append(")");
          }

          @Override public StringBuilder greaterThan(Integer lower) {
            return b.append("greaterThan(").append(lower).append(")");
          }

          @Override public StringBuilder lessThan(Integer upper) {
            return b.append("lessThan(").append(upper).append(")");
          }

          @Override public StringBuilder singleton(Integer value) {
            return b.append("singleton(").append(value).append(")");
          }

          @Override public StringBuilder closed(Integer lower, Integer upper) {
            return b.append("closed(").append(lower).append(", ")
                .append(upper).append(")");
          }

          @Override public StringBuilder closedOpen(Integer lower, Integer upper) {
            return b.append("closedOpen(").append(lower).append(", ")
                .append(upper).append(")");
          }

          @Override public StringBuilder openClosed(Integer lower, Integer upper) {
            return b.append("openClosed(").append(lower).append(", ")
                .append(upper).append(")");
          }

          @Override public StringBuilder open(Integer lower, Integer upper) {
            return b.append("open(").append(lower).append(", ")
                .append(upper).append(")");
          }
        };
    final RangeSets.Consumer<Integer> c =
        new RangeSets.Consumer<Integer>() {
          @Override public void all() {
            b.append("all()");
          }

          @Override public void atLeast(Integer lower) {
            b.append("atLeast(").append(lower).append(")");
          }

          @Override public void atMost(Integer upper) {
            b.append("atMost(").append(upper).append(")");
          }

          @Override public void greaterThan(Integer lower) {
            b.append("greaterThan(").append(lower).append(")");
          }

          @Override public void lessThan(Integer upper) {
            b.append("lessThan(").append(upper).append(")");
          }

          @Override public void singleton(Integer value) {
            b.append("singleton(").append(value).append(")");
          }

          @Override public void closed(Integer lower, Integer upper) {
            b.append("closed(").append(lower).append(", ")
                .append(upper).append(")");
          }

          @Override public void closedOpen(Integer lower, Integer upper) {
            b.append("closedOpen(").append(lower).append(", ")
                .append(upper).append(")");
          }

          @Override public void openClosed(Integer lower, Integer upper) {
            b.append("openClosed(").append(lower).append(", ")
                .append(upper).append(")");
          }

          @Override public void open(Integer lower, Integer upper) {
            b.append("open(").append(lower).append(", ")
                .append(upper).append(")");
          }
        };
    final Fixture f = new Fixture();
    for (Range<Integer> range : f.ranges) {
      RangeSets.map(range, h);
    }
    assertThat(b.toString(), is(f.rangesString));

    b.setLength(0);
    for (Range<Integer> range : f.ranges) {
      RangeSets.forEach(range, c);
    }
    assertThat(b.toString(), is(f.rangesString));

    // Use a smaller set of ranges that does not overlap
    b.setLength(0);
    for (Range<Integer> range : f.disjointRanges) {
      RangeSets.forEach(range, c);
    }
    assertThat(b.toString(), is(f.disjointRangesString));

    // For a RangeSet consisting of disjointRanges the effect is the same,
    // but the ranges are sorted.
    b.setLength(0);
    RangeSets.forEach(f.rangeSet, c);
    assertThat(b.toString(), is(f.disjointRangesSortedString));
  }

  /** Tests that {@link RangeSets#hashCode(RangeSet)} returns the same result
   * as the hashCode of a list of the same ranges. */
  @Test void testRangeSetHashCode() {
    final Fixture f = new Fixture();
    final int h = new ArrayList<>(f.rangeSet.asRanges()).hashCode();
    assertThat(RangeSets.hashCode(f.rangeSet), is(h));
    assertThat(RangeSets.hashCode(f.treeRangeSet), is(h));

    assertThat(RangeSets.hashCode(ImmutableRangeSet.<Integer>of()),
        is(ImmutableList.of().hashCode()));
  }

  /** Tests {@link RangeSets#compare(Range, Range)}. */
  @Test void testRangeCompare() {
    final Fixture f = new Fixture();
    Ord.forEach(f.sortedRanges, (r0, i) ->
        Ord.forEach(f.sortedRanges, (r1, j) -> {
          final String reason = "compare " + r0 + " to " + r1;
          assertThat(reason, RangeSets.compare(r0, r1),
              is(Integer.compare(i, j)));
        }));
  }

  /** Tests {@link RangeSets#compare(RangeSet, RangeSet)}. */
  @Test void testRangeSetCompare() {
    final Fixture f = new Fixture();
    assertThat(RangeSets.compare(f.rangeSet, f.treeRangeSet), is(0));
    assertThat(RangeSets.compare(f.rangeSet, f.rangeSet), is(0));
    assertThat(RangeSets.compare(f.treeRangeSet, f.rangeSet), is(0));

    // empty range set collates before everything
    assertThat(RangeSets.compare(f.empty, f.treeRangeSet), is(-1));
    assertThat(RangeSets.compare(f.treeRangeSet, f.empty), is(1));
    assertThat(RangeSets.compare(f.empty, f.zeroRangeSet), is(-1));
    assertThat(RangeSets.compare(f.zeroRangeSet, f.empty), is(1));

    // removing the first element (if it's not the only element)
    // makes a range set collate later
    final RangeSet<Integer> s2 = TreeRangeSet.create(f.treeRangeSet);
    s2.asRanges().remove(Iterables.getFirst(s2.asRanges(), null));
    assertThat(RangeSets.compare(s2, f.treeRangeSet), is(1));
    assertThat(RangeSets.compare(f.treeRangeSet, s2), is(-1));
    assertThat(RangeSets.compare(f.empty, s2), is(-1));
    assertThat(RangeSets.compare(s2, f.empty), is(1));

    // removing the last element
    // makes a range set collate earlier
    final RangeSet<Integer> s3 = TreeRangeSet.create(f.treeRangeSet);
    s3.asRanges().remove(Iterables.getLast(s3.asRanges(), null));
    assertThat(RangeSets.compare(s3, f.treeRangeSet), is(-1));
    assertThat(RangeSets.compare(f.treeRangeSet, s3), is(1));
  }

  /** Data sets used by various tests. */
  static class Fixture {
    final ImmutableRangeSet<Integer> empty = ImmutableRangeSet.of();

    final List<Range<Integer>> ranges =
        Arrays.asList(Range.all(),
            Range.atMost(3),
            Range.atLeast(4),
            Range.lessThan(5),
            Range.greaterThan(6),
            Range.singleton(7),
            Range.open(8, 9),
            Range.openClosed(10, 11),
            Range.closed(12, 13),
            Range.closedOpen(14, 15));
    final String rangesString = "all()"
        + "atMost(3)"
        + "atLeast(4)"
        + "lessThan(5)"
        + "greaterThan(6)"
        + "singleton(7)"
        + "open(8, 9)"
        + "openClosed(10, 11)"
        + "closed(12, 13)"
        + "closedOpen(14, 15)";

    final List<Range<Integer>> sortedRanges =
        Arrays.asList(
            Range.lessThan(3),
            Range.atMost(3),
            Range.lessThan(5),
            Range.all(),
            Range.greaterThan(4),
            Range.atLeast(4),
            Range.greaterThan(6),
            Range.singleton(7),
            Range.open(8, 9),
            Range.openClosed(8, 9),
            Range.closedOpen(8, 9),
            Range.closed(8, 9),
            Range.openClosed(10, 11),
            Range.closed(12, 13),
            Range.closedOpen(14, 15));

    final List<Range<Integer>> disjointRanges =
        Arrays.asList(Range.lessThan(5),
            Range.greaterThan(16),
            Range.singleton(7),
            Range.open(8, 9),
            Range.openClosed(10, 11),
            Range.closed(12, 13),
            Range.closedOpen(14, 15));

    final String disjointRangesString = "lessThan(5)"
        + "greaterThan(16)"
        + "singleton(7)"
        + "open(8, 9)"
        + "openClosed(10, 11)"
        + "closed(12, 13)"
        + "closedOpen(14, 15)";

    final String disjointRangesSortedString = "lessThan(5)"
        + "singleton(7)"
        + "open(8, 9)"
        + "openClosed(10, 11)"
        + "closed(12, 13)"
        + "closedOpen(14, 15)"
        + "greaterThan(16)";

    final RangeSet<Integer> rangeSet;
    final TreeRangeSet<Integer> treeRangeSet;

    final RangeSet<Integer> zeroRangeSet =
        ImmutableRangeSet.of(Range.singleton(0));

    Fixture() {
      final ImmutableRangeSet.Builder<Integer> builder =
          ImmutableRangeSet.builder();
      disjointRanges.forEach(builder::add);
      rangeSet = builder.build();
      treeRangeSet = TreeRangeSet.create();
      treeRangeSet.addAll(rangeSet);
    }
  }
}
