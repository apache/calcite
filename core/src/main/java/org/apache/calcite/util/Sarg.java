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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/** Set of values (or ranges) that are the target of a search.
 *
 * <p>The name is derived from <b>S</b>earch <b>arg</b>ument, an ancient
 * concept in database implementation; see Access Path Selection in a Relational
 * Database Management System &mdash; Selinger et al. 1979 or the
 * "<a href="https://blog.acolyer.org/2016/01/04/access-path-selection/">morning
 * paper summary</a>.
 *
 * <p>In RexNode, a Sarg only occur as the right-hand operand in a call to
 * {@link SqlStdOperatorTable#SEARCH}, wrapped in a
 * {@link org.apache.calcite.rex.RexLiteral}. Lifecycle methods:
 *
 * <ul>
 * <li>{@link org.apache.calcite.rex.RexUtil#expandSearch} removes
 *     calls to SEARCH and the included Sarg, converting them to comparisons;
 * <li>{@link org.apache.calcite.rex.RexSimplify} converts complex comparisons
 *     on the same argument into SEARCH calls with an included Sarg;
 * <li>Various {@link org.apache.calcite.tools.RelBuilder} methods,
 *     including {@link org.apache.calcite.tools.RelBuilder#in}
 *     and {@link org.apache.calcite.tools.RelBuilder#between}
 *     call {@link org.apache.calcite.rex.RexBuilder}
 *     methods {@link org.apache.calcite.rex.RexBuilder#makeIn}
 *     and {@link org.apache.calcite.rex.RexBuilder#makeBetween}
 *     that create Sarg instances directly;
 * <li>{@link org.apache.calcite.rel.rel2sql.SqlImplementor} converts
 *     {@link org.apache.calcite.rex.RexCall}s
 *     to SEARCH into {@link org.apache.calcite.sql.SqlNode} AST expressions
 *     such as comparisons, {@code BETWEEN} and {@code IN}.
 * </ul>
 *
 * @param <C> Value type
 *
 * @see SqlStdOperatorTable#SEARCH
 */
public class Sarg<C extends Comparable<C>> implements Comparable<Sarg<C>> {
  public final RangeSet<C> rangeSet;
  public final boolean containsNull;
  public final int pointCount;
  public final RelDataType type;

  /**
   * Handler to transform long ranges back to ranges with the original type.
   *
   * <p>
   * Some special processing is applied to the lower bound,
   * because negative infinity could be replaced with Long.MIN_VALUE when canonizing
   * the range boundaries. This problem does not exist for the positive infinity,
   * because the Guava library gives different implementations for
   * Cut#BelowAll#canonical and Cut#AboveAll#canonical methods.
   * </p>
   */
  private final RangeSets.CopyingHandler<Long, C>
      fromLongHandler = new RangeSets.CopyingHandler<Long, C>() {
        @Override C convert(Long value) {
          return (C) BigDecimal.valueOf(value);
        }

        @Override public Range<C> atLeast(Long lower) {
          if (lower.equals(DiscreteDomain.longs().minValue())) {
            return Range.all();
          }
          return super.atLeast(lower);
        }

        @Override public Range<C> greaterThan(Long lower) {
          if (lower.equals(DiscreteDomain.longs().minValue())) {
            return Range.all();
          }
          return super.greaterThan(lower);
        }

        @Override public Range<C> closed(Long lower, Long upper) {
          if (lower.equals(DiscreteDomain.longs().minValue())) {
            return (Range<C>) Range.atMost(BigDecimal.valueOf(upper));
          }
          return super.closed(lower, upper);
        }

        @Override public Range<C> closedOpen(Long lower, Long upper) {
          if (lower.equals(DiscreteDomain.longs().minValue())) {
            return (Range<C>) Range.lessThan(BigDecimal.valueOf(upper));
          }
          return super.closedOpen(lower, upper);
        }

        @Override public Range<C> openClosed(Long lower, Long upper) {
          if (lower.equals(DiscreteDomain.longs().minValue())) {
            return (Range<C>) Range.atMost(BigDecimal.valueOf(upper));
          }
          return super.openClosed(lower, upper);
        }

        @Override public Range<C> open(Long lower, Long upper) {
          if (lower.equals(DiscreteDomain.longs().minValue())) {
            return (Range<C>) Range.lessThan(BigDecimal.valueOf(upper));
          }
          return super.open(lower, upper);
        }
      };

  static final Range<Long> EMPTY_RANGE = Range.closedOpen(0L, 0L);

  /**
   * Transform a range to one with closed boundaries.
   * This normalizes the range, making subsequent operations easier.
   */
  private final RangeSets.CopyingHandler<Long, Long>
      closeRangeHandler = new RangeSets.CopyingHandler<Long, Long>() {

        @Override Long convert(Long value) {
          return value;
        }

        @Override public Range<Long> greaterThan(Long lower) {
          return Range.atLeast(lower + 1);
        }

        @Override public Range<Long> lessThan(Long upper) {
          return Range.atMost(upper - 1);
        }

        @Override public Range<Long> closedOpen(Long lower, Long upper) {
          if (lower > upper - 1) {
            return EMPTY_RANGE;
          }
          return Range.closed(lower, upper - 1);
        }

        @Override public Range<Long> openClosed(Long lower, Long upper) {
          if (lower + 1 > upper) {
            return EMPTY_RANGE;
          }
          return Range.closed(lower + 1, upper);
        }

        @Override public Range<Long> open(Long lower, Long upper) {
          if (lower + 1 > upper - 1) {
            return EMPTY_RANGE;
          }
          return Range.closed(lower + 1, upper - 1);
        }
      };

  private Sarg(ImmutableRangeSet<C> rangeSet, boolean containsNull, RelDataType type) {
    this.type = type;
    this.rangeSet = simplifyRangeSet(Objects.requireNonNull(rangeSet));
    this.containsNull = containsNull;
    this.pointCount = RangeSets.countPoints(rangeSet);
  }

  /**
   * Creates a search argument.
   * <p>
   *   Please note that we need a type argument here,
   *   because for some discrete types, we can simplify the Sarg by
   *   merging the underling ranges.
   * </p>
   */
  public static <C extends Comparable<C>> Sarg<C> of(boolean containsNull,
      RangeSet<C> rangeSet, RelDataType type) {
    return new Sarg<>(ImmutableRangeSet.copyOf(rangeSet), containsNull, type);
  }

  RangeSet<C> simplifyRangeSet(RangeSet<C> rangeSet) {
    if (!isDiscreteType(type)) {
      // nothing to do for non-discrete types.
      return rangeSet;
    }

    // transform to long ranges, so range merging can be performed
    List<Range<Long>> longRanges =  rangeSet.asRanges().stream()
        .map(r -> RangeSets.copy(r, v -> ((BigDecimal) v).longValue()))
        .map(r -> RangeSets.map(r, closeRangeHandler)).collect(Collectors.toList());

    // calculate cost of the original range set.
    SargCost sc = new SargCost();
    double originalCost = sc.getCost(longRanges);

    // perform range merging
    RangeSet<Long> longRangeSet = TreeRangeSet.create();
    longRanges.forEach(r -> longRangeSet.add(r.canonical(DiscreteDomain.longs())));

    // calculate the cost of the new range set.
    List<Range<Long>> newLongRanges = longRangeSet.asRanges().stream()
        .map(r -> RangeSets.map(r, closeRangeHandler)).collect(Collectors.toList());
    double newCost = sc.getCost(newLongRanges);

    if (originalCost <= newCost) {
      // the new range set is more expensive
      return rangeSet;
    }

    // transform back to ranges of the original type.
    RangeSet<C> newRangeSet = TreeRangeSet.create();
    newLongRanges.forEach(r -> newRangeSet.add(RangeSets.map(r, fromLongHandler)));
    return newRangeSet;
  }

  boolean isDiscreteType(RelDataType type) {
    switch (type.getSqlTypeName()) {
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
      return true;
    default:
      return false;
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Produces a similar result to {@link RangeSet}, but adds ", null"
   * if nulls are matched, and simplifies point ranges. For example,
   * the Sarg that allows the range set
   *
   * <blockquote>{@code [[7&#x2025;7], [9&#x2025;9],
   * (10&#x2025;+&infin;)]}</blockquote>
   *
   * and also null is printed as
   *
   * <blockquote>{@code Sarg[7, 9, (10&#x2025;+&infin;), null]}</blockquote>
   */
  @Override public String toString() {
    final StringBuilder sb = new StringBuilder();
    printTo(sb, StringBuilder::append);
    return sb.toString();
  }

  /** Prints this Sarg to a StringBuilder, using the given printer to deal
   * with each embedded value. */
  public StringBuilder printTo(StringBuilder sb,
      BiConsumer<StringBuilder, C> valuePrinter) {
    sb.append("Sarg[");
    final RangeSets.Consumer<C> printer = RangeSets.printer(sb, valuePrinter);
    Ord.forEach(rangeSet.asRanges(), (r, i) -> {
      if (i > 0) {
        sb.append(", ");
      }
      RangeSets.forEach(r, printer);
    });
    if (containsNull) {
      sb.append(", null");
    }
    return sb.append("]");
  }

  @Override public int compareTo(Sarg<C> o) {
    return RangeSets.compare(rangeSet, o.rangeSet);
  }

  @Override public int hashCode() {
    return RangeSets.hashCode(rangeSet) * 31 + (containsNull ? 2 : 3);
  }

  @Override public boolean equals(Object o) {
    return o == this
        || o instanceof Sarg
        && rangeSet.equals(((Sarg) o).rangeSet)
        && containsNull == ((Sarg) o).containsNull;
  }

  /** Returns whether this Sarg is a collection of 1 or more points (and perhaps
   * an {@code IS NULL} if {@link #containsNull}).
   *
   * <p>Such sargs could be translated as {@code ref = value}
   * or {@code ref IN (value1, ...)}. */
  public boolean isPoints() {
    return pointCount == rangeSet.asRanges().size();
  }

  /** Returns whether this Sarg, when negated, is a collection of 1 or more
   * points (and perhaps an {@code IS NULL} if {@link #containsNull}).
   *
   * <p>Such sargs could be translated as {@code ref <> value}
   * or {@code ref NOT IN (value1, ...)}. */
  public boolean isComplementedPoints() {
    return rangeSet.span().encloses(Range.all())
        && rangeSet.complement().asRanges().stream()
            .allMatch(RangeSets::isPoint);
  }

  /**
   * Utility for evaluating the cost of a Sarg.
   */
  class SargCost {
    /**
     * Cost for a comparison option.
     */
    static final double COMPARISON_COST = 1.0;

    /**
     * Cost of a logical option.
     */
    static final double LOGICAL_OP_COST = 0.2;

    /**
     * Gets the cost of a list of range evaluations combined by logical or.
     */
    double getCost(Collection<Range<Long>> ranges) {
      return (ranges.size() - 1) * LOGICAL_OP_COST
          + ranges.stream().mapToDouble(r -> getCost(r)).sum();
    }

    /**
     * Gets the cost for evaluating a single range.
     */
    double getCost(Range<Long> range) {
      if (range == EMPTY_RANGE) {
        return 0;
      } else if (RangeSets.isPoint(range)) {
        return COMPARISON_COST;
      } else {
        return 2 * COMPARISON_COST + LOGICAL_OP_COST;
      }
    }
  }
}
