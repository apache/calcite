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
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.Matchers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.apache.calcite.test.Matchers.isRangeSet;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.assertThrows;

import static java.util.Arrays.asList;

/**
 * Unit test for {@link RangeSets} and other utilities relating to Guava
 * {@link Range} and {@link RangeSet}.
 */
class RangeSetTest {

  /** Tests {@link org.apache.calcite.rel.externalize.RelJson#toJson(Range)}
   * and {@link RelJson#rangeFromJson(List, RelDataType)}. */
  @Test void testRangeSetSerializeDeserialize() {
    RelJson relJson = RelJson.create();
    RelDataType integerType = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.INTEGER);
    RelDataType decimalType = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DECIMAL);
    RelDataType doubleType = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DOUBLE);

    final Range<Integer> integerPoint = Range.singleton(Integer.valueOf(0));
    final Range<BigDecimal> bigDecimalPoint = Range.singleton(BigDecimal.valueOf(0));
    final Range<Double> doublePoint = Range.singleton(Double.valueOf(0));
    final Range<BigDecimal> closedRange1 =
        Range.closed(BigDecimal.valueOf(0), BigDecimal.valueOf(5));
    final Range<BigDecimal> closedRange2 =
        Range.closed(BigDecimal.valueOf(6), BigDecimal.valueOf(10));

    final Range<BigDecimal> gt1 = Range.greaterThan(BigDecimal.valueOf(7));
    final Range<BigDecimal> al1 = Range.atLeast(BigDecimal.valueOf(8));
    final Range<BigDecimal> lt1 = Range.lessThan(BigDecimal.valueOf(4));
    final Range<BigDecimal> am1 = Range.atMost(BigDecimal.valueOf(3));

    // Test serialize/deserialize Range
    //    Integer Point
    //    Deserializes as BigDecimal because Calcite uses BigDecimal for exact numerics
    assertThat(RelJson.rangeFromJson(relJson.toJson(integerPoint), integerType),
        is(bigDecimalPoint));
    //    BigDecimal Point
    assertThat(RelJson.rangeFromJson(relJson.toJson(bigDecimalPoint), decimalType),
        is(bigDecimalPoint));
    //    Double Point
    assertThat(RelJson.rangeFromJson(relJson.toJson(doublePoint), doubleType),
        is(doublePoint));
    //    Closed Range
    assertThat(RelJson.rangeFromJson(relJson.toJson(closedRange1), decimalType),
        is(closedRange1));
    //    Open Range
    assertThat(RelJson.rangeFromJson(relJson.toJson(gt1), decimalType), is(gt1));
    assertThat(RelJson.rangeFromJson(relJson.toJson(al1), decimalType), is(al1));
    assertThat(RelJson.rangeFromJson(relJson.toJson(lt1), decimalType), is(lt1));
    assertThat(RelJson.rangeFromJson(relJson.toJson(am1), decimalType), is(am1));
    // Test closed single RangeSet
    final RangeSet<BigDecimal> closedRangeSet = ImmutableRangeSet.of(closedRange1);
    assertThat(RelJson.rangeSetFromJson(relJson.toJson(closedRangeSet), decimalType),
        is(closedRangeSet));
    // Test complex RangeSets
    final RangeSet<BigDecimal> complexClosedRangeSet1 =
        ImmutableRangeSet.<BigDecimal>builder()
            .add(closedRange1)
            .add(closedRange2)
            .build();
    assertThat(
        RelJson.rangeSetFromJson(relJson.toJson(complexClosedRangeSet1), decimalType),
        is(complexClosedRangeSet1));
    final RangeSet<BigDecimal> complexClosedRangeSet2 =
        ImmutableRangeSet.<BigDecimal>builder()
            .add(gt1)
            .add(am1)
            .build();
    assertThat(RelJson.rangeSetFromJson(relJson.toJson(complexClosedRangeSet2), decimalType),
        is(complexClosedRangeSet2));

    // Test None and All
    final RangeSet<BigDecimal> setNone = ImmutableRangeSet.of();
    final RangeSet<BigDecimal> setAll = setNone.complement();
    assertThat(RelJson.rangeSetFromJson(relJson.toJson(setNone), decimalType), is(setNone));
    assertThat(RelJson.rangeSetFromJson(relJson.toJson(setAll), decimalType), is(setAll));
  }

  /** Tests {@link RangeSets#minus(RangeSet, Range)}. */
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
    assertThat(setComplex, isRangeSet("[[0..2], [3..3], (5..+\u221e)]"));

    assertThat(RangeSets.minus(setAll, Range.singleton(1)),
        isRangeSet("[(-\u221e..1), (1..+\u221e)]"));
    assertThat(RangeSets.minus(setNone, Range.singleton(1)), is(setNone));
    assertThat(RangeSets.minus(setGt2, Range.singleton(1)), is(setGt2));
    assertThat(RangeSets.minus(setGt1, Range.singleton(1)), is(setGt1));
    assertThat(RangeSets.minus(setGe1, Range.singleton(1)), is(setGt1));
    assertThat(RangeSets.minus(setGt0, Range.singleton(1)),
        isRangeSet("[(0..1), (1..+\u221e)]"));
    assertThat(RangeSets.minus(setComplex, Range.singleton(1)),
        isRangeSet("[[0..1), (1..2], [3..3], (5..+\u221e)]"));
    assertThat(RangeSets.minus(setComplex, Range.singleton(2)),
        isRangeSet("[[0..2), [3..3], (5..+\u221e)]"));
    assertThat(RangeSets.minus(setComplex, Range.singleton(3)),
        isRangeSet("[[0..2], (5..+\u221e)]"));
    assertThat(RangeSets.minus(setComplex, Range.open(2, 3)),
        isRangeSet("[[0..2], [3..3], (5..+\u221e)]"));
    assertThat(RangeSets.minus(setComplex, Range.closed(2, 3)),
        isRangeSet("[[0..2), (5..+\u221e)]"));
    assertThat(RangeSets.minus(setComplex, Range.closed(2, 7)),
        isRangeSet("[[0..2), (7..+\u221e)]"));
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

    // Test situation where endpoints of closed range are equal under
    // Comparable.compareTo but not T.equals.
    final BigDecimal one = new BigDecimal("1");
    final BigDecimal onePoint = new BigDecimal("1.0");
    assertThat(RangeSets.isPoint(Range.closed(one, onePoint)), is(true));
  }

  /** Tests ranges with a data type that implements {@link Comparable}
   * but is not consistent with {@code equals}.
   *
   * <p>Per {@link Comparable}:
   *
   * <blockquote>
   * Virtually all Java core classes that implement Comparable have natural
   * orderings that are consistent with equals. One exception is
   * {@link BigDecimal}, whose natural ordering equates {@code BigDecimal}
   * objects with equal numerical values and different representations
   * (such as 4.0 and 4.00). For {@link BigDecimal#equals} to return true,
   * the representation and numerical value of the two {@code BigDecimal}
   * objects must be the same.
   * </blockquote>
   */
  @Test void testNotConsistentWithEquals() {
    final BigDecimal one = new BigDecimal("1");
    final BigDecimal onePoint = new BigDecimal("1.0");
    final BigDecimal two = BigDecimal.valueOf(2);
    assertThat(one.equals(onePoint), is(false));
    assertThat(onePoint.equals(one), is(false));
    assertThat(one.compareTo(onePoint), is(0));
    assertThat(onePoint.equals(one), is(false));
    assertThat(one.equals(BigDecimal.ONE), is(true));
    assertThat(onePoint.equals(BigDecimal.ONE), is(false));
    assertThat(RangeSets.isPoint(Range.closed(one, onePoint)), is(true));

    // Ranges (0, 1], [1.0, 2) merges to [0, 2).
    final Range<BigDecimal> range01 = Range.closed(BigDecimal.ZERO, one);
    final Range<BigDecimal> range01point = Range.closed(BigDecimal.ZERO, onePoint);
    final Range<BigDecimal> range1point2 = Range.closedOpen(onePoint, two);
    final Range<BigDecimal> range12 = Range.closedOpen(one, two);
    assertThrows(IllegalArgumentException.class,
        () -> ImmutableRangeSet.<BigDecimal>builder()
            .add(range01point)
            .add(range12)
            .build());

    assertThat(RangeSets.compare(range01, range12), is(-1));
    assertThat(RangeSets.compare(range12, range01), is(1));
    assertThat(RangeSets.compare(range01, range01point), is(0));
    assertThat(RangeSets.compare(range12, range1point2), is(0));

    // Ranges are merged correctly.
    final ImmutableRangeSet<BigDecimal> rangeSet =
        ImmutableRangeSet.unionOf(asList(range01point, range12));
    final ImmutableRangeSet<BigDecimal> rangeSet2 =
        ImmutableRangeSet.unionOf(asList(range01, range12));
    final ImmutableRangeSet<BigDecimal> rangeSet3 =
        ImmutableRangeSet.unionOf(asList(range01, range1point2));
    assertThat(rangeSet.asRanges(), hasSize(1));
    assertThat(rangeSet, is(rangeSet2));
    assertThat(rangeSet, is(rangeSet3));

    // Check the Consumer mechanism; RangeSets.printer is a Consumer,
    // and it gives the Consumer mechanism a pretty good workout.
    final Function<RangeSet<BigDecimal>, String> f =
        rs -> {
          final StringBuilder buf = new StringBuilder();
          final RangeSets.Consumer<BigDecimal> printer =
              RangeSets.printer(buf, StringBuilder::append);
          RangeSets.forEach(rs, printer);
          return Matchers.sanitizeRangeSet(buf.toString());
        };
    final Function<Range<BigDecimal>, String> f2 =
        r -> f.apply(ImmutableRangeSet.of(r));
    assertThat(f.apply(rangeSet), is("[0..2)"));

    // If a closed range has bounds that are equal, Consumer should treat them
    // as singletons of the lower bound; but not if the bounds compareTo 0.
    assertThat(f2.apply(Range.singleton(onePoint)), is("1.0"));
    assertThat(f2.apply(Range.closed(one, one)), is("1"));
    assertThat(f2.apply(Range.closed(one, onePoint)), is("[1..1.0]"));
    assertThat(f2.apply(Range.closed(onePoint, one)), is("[1.0..1]"));
    assertThat(f2.apply(Range.closed(onePoint, onePoint)), is("1.0"));
    assertThat(f2.apply(Range.closed(onePoint, two)), is("[1.0..2]"));

    // As for Consumer, now for Handler.
    // RangeSets.copy tests Handler pretty thoroughly.
    final Function<RangeSet<BigDecimal>, RangeSet<BigDecimal>> g =
        rs -> RangeSets.copy(rs, v -> v.multiply(two));
    final Function<Range<BigDecimal>, RangeSet<BigDecimal>> g2 =
        r -> g.apply(ImmutableRangeSet.of(r));
    assertThat(g.apply(rangeSet), isRangeSet("[[0..4)]"));

    assertThat(g2.apply(Range.singleton(onePoint)), isRangeSet("[[2.0..2.0]]"));
    assertThat(g2.apply(Range.closed(one, one)), isRangeSet("[[2..2]]"));
    assertThat(g2.apply(Range.closed(one, onePoint)), isRangeSet("[[2..2.0]]"));
    assertThat(g2.apply(Range.closed(onePoint, one)), isRangeSet("[[2.0..2]]"));
    assertThat(g2.apply(Range.closed(onePoint, onePoint)),
        isRangeSet("[[2.0..2.0]]"));
    assertThat(g2.apply(Range.closed(onePoint, two)), isRangeSet("[[2.0..4]]"));
  }

  /** Tests {@link RangeSets#isOpenInterval(RangeSet)}. */
  @Test void testRangeSetIsOpenInterval() {
    final RangeSet<Integer> setGt0 = ImmutableRangeSet.of(Range.greaterThan(0));
    final RangeSet<Integer> setAl0 = ImmutableRangeSet.of(Range.atLeast(0));
    final RangeSet<Integer> setLt0 = ImmutableRangeSet.of(Range.lessThan(0));
    final RangeSet<Integer> setAm0 = ImmutableRangeSet.of(Range.atMost(0));

    assertThat(RangeSets.isOpenInterval(setGt0), is(true));
    assertThat(RangeSets.isOpenInterval(setAl0), is(true));
    assertThat(RangeSets.isOpenInterval(setLt0), is(true));
    assertThat(RangeSets.isOpenInterval(setAm0), is(true));

    final RangeSet<Integer> setNone = ImmutableRangeSet.of();
    final RangeSet<Integer> multiRanges = ImmutableRangeSet.<Integer>builder()
        .add(Range.lessThan(0))
        .add(Range.greaterThan(3))
        .build();

    assertThat(RangeSets.isOpenInterval(setNone), is(false));
    assertThat(RangeSets.isOpenInterval(multiRanges), is(false));

    final RangeSet<Integer> open = ImmutableRangeSet.of(Range.open(0, 3));
    final RangeSet<Integer> closed = ImmutableRangeSet.of(Range.closed(0, 3));
    final RangeSet<Integer> openClosed = ImmutableRangeSet.of(Range.openClosed(0, 3));
    final RangeSet<Integer> closedOpen = ImmutableRangeSet.of(Range.closedOpen(0, 3));

    assertThat(RangeSets.isOpenInterval(open), is(false));
    assertThat(RangeSets.isOpenInterval(closed), is(false));
    assertThat(RangeSets.isOpenInterval(openClosed), is(false));
    assertThat(RangeSets.isOpenInterval(closedOpen), is(false));
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
    final StringBuilder sb = new StringBuilder();
    final RangeSets.Handler<Integer, StringBuilder> h =
        new RangeSets.Handler<Integer, StringBuilder>() {
          @Override public StringBuilder all() {
            return sb.append("all()");
          }

          @Override public StringBuilder atLeast(Integer lower) {
            return sb.append("atLeast(").append(lower).append(")");
          }

          @Override public StringBuilder atMost(Integer upper) {
            return sb.append("atMost(").append(upper).append(")");
          }

          @Override public StringBuilder greaterThan(Integer lower) {
            return sb.append("greaterThan(").append(lower).append(")");
          }

          @Override public StringBuilder lessThan(Integer upper) {
            return sb.append("lessThan(").append(upper).append(")");
          }

          @Override public StringBuilder singleton(Integer value) {
            return sb.append("singleton(").append(value).append(")");
          }

          @Override public StringBuilder closed(Integer lower, Integer upper) {
            return sb.append("closed(").append(lower).append(", ")
                .append(upper).append(")");
          }

          @Override public StringBuilder closedOpen(Integer lower, Integer upper) {
            return sb.append("closedOpen(").append(lower).append(", ")
                .append(upper).append(")");
          }

          @Override public StringBuilder openClosed(Integer lower, Integer upper) {
            return sb.append("openClosed(").append(lower).append(", ")
                .append(upper).append(")");
          }

          @Override public StringBuilder open(Integer lower, Integer upper) {
            return sb.append("open(").append(lower).append(", ")
                .append(upper).append(")");
          }
        };
    final RangeSets.Consumer<Integer> c =
        new RangeSets.Consumer<Integer>() {
          @Override public void all() {
            sb.append("all()");
          }

          @Override public void atLeast(Integer lower) {
            sb.append("atLeast(").append(lower).append(")");
          }

          @Override public void atMost(Integer upper) {
            sb.append("atMost(").append(upper).append(")");
          }

          @Override public void greaterThan(Integer lower) {
            sb.append("greaterThan(").append(lower).append(")");
          }

          @Override public void lessThan(Integer upper) {
            sb.append("lessThan(").append(upper).append(")");
          }

          @Override public void singleton(Integer value) {
            sb.append("singleton(").append(value).append(")");
          }

          @Override public void closed(Integer lower, Integer upper) {
            sb.append("closed(").append(lower).append(", ")
                .append(upper).append(")");
          }

          @Override public void closedOpen(Integer lower, Integer upper) {
            sb.append("closedOpen(").append(lower).append(", ")
                .append(upper).append(")");
          }

          @Override public void openClosed(Integer lower, Integer upper) {
            sb.append("openClosed(").append(lower).append(", ")
                .append(upper).append(")");
          }

          @Override public void open(Integer lower, Integer upper) {
            sb.append("open(").append(lower).append(", ")
                .append(upper).append(")");
          }
        };
    final Fixture f = new Fixture();
    for (Range<Integer> range : f.ranges) {
      RangeSets.map(range, h);
    }
    assertThat(sb, hasToString(f.rangesString));

    sb.setLength(0);
    for (Range<Integer> range : f.ranges) {
      RangeSets.forEach(range, c);
    }
    assertThat(sb, hasToString(f.rangesString));

    // Use a smaller set of ranges that does not overlap
    sb.setLength(0);
    for (Range<Integer> range : f.disjointRanges) {
      RangeSets.forEach(range, c);
    }
    assertThat(sb, hasToString(f.disjointRangesString));

    // For a RangeSet consisting of disjointRanges the effect is the same,
    // but the ranges are sorted.
    sb.setLength(0);
    RangeSets.forEach(f.rangeSet, c);
    assertThat(sb, hasToString(f.disjointRangesSortedString));
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

  /** Tests {@link RangeSets#printer(StringBuilder, BiConsumer)}. */
  @Test void testRangePrint() {
    final Fixture f = new Fixture();

    // RangeSet's native printing; format used a unicode symbol up to 28.2, and
    // ".." 29.0 and later.
    final List<String> list = new ArrayList<>();
    f.ranges.forEach(r -> list.add(r.toString()));
    final String expectedGuava28 = "[(-\u221e\u2025+\u221e), (-\u221e\u20253], "
        + "[4\u2025+\u221e), (-\u221e\u20255), (6\u2025+\u221e), [7\u20257], "
        + "(8\u20259), (10\u202511], [12\u202513], [14\u202515)]";
    final String expectedGuava29 = "[(-\u221e..+\u221e), (-\u221e..3], "
        + "[4..+\u221e), (-\u221e..5), (6..+\u221e), [7..7], "
        + "(8..9), (10..11], [12..13], [14..15)]";
    assertThat(list,
        hasToString(anyOf(is(expectedGuava28), is(expectedGuava29))));
    list.clear();

    final StringBuilder sb = new StringBuilder();
    f.ranges.forEach(r -> {
      RangeSets.forEach(r, RangeSets.printer(sb, StringBuilder::append));
      list.add(sb.toString());
      sb.setLength(0);
    });
    // our format matches Guava's, except points ("7" vs "[7, 7]")
    final String expected2 = "[(-\u221e..+\u221e), (-\u221e..3], "
        + "[4..+\u221e), (-\u221e..5), (6..+\u221e), 7, "
        + "(8..9), (10..11], [12..13], [14..15)]";
    assertThat(list, hasToString(expected2));
    list.clear();
  }

  /** Data sets used by various tests. */
  static class Fixture {
    final ImmutableRangeSet<Integer> empty = ImmutableRangeSet.of();

    final List<Range<Integer>> ranges =
        asList(Range.all(),
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
        asList(
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
        asList(Range.lessThan(5),
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
