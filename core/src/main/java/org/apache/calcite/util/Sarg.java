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
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.function.BiConsumer;

import static java.util.Objects.requireNonNull;

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
@SuppressWarnings("type.argument.type.incompatible")
public class Sarg<C extends Comparable<C>> implements Comparable<Sarg<C>> {
  public final RangeSet<C> rangeSet;
  public final RexUnknownAs nullAs;
  public final int pointCount;

  /** Returns FALSE for all null and not-null values.
   *
   * <p>{@code SEARCH(x, FALSE)} is equivalent to {@code FALSE}. */
  private static final SpecialSarg FALSE =
      new SpecialSarg(ImmutableRangeSet.of(), RexUnknownAs.FALSE,
          "Sarg[FALSE]", 2);

  /** Returns TRUE for all not-null values, FALSE for null.
   *
   * <p>{@code SEARCH(x, IS_NOT_NULL)} is equivalent to
   * {@code x IS NOT NULL}. */
  private static final SpecialSarg IS_NOT_NULL =
      new SpecialSarg(ImmutableRangeSet.of().complement(), RexUnknownAs.FALSE,
          "Sarg[IS NOT NULL]", 3);

  /** Returns FALSE for all not-null values, TRUE for null.
   *
   * <p>{@code SEARCH(x, IS_NULL)} is equivalent to {@code x IS NULL}. */
  private static final SpecialSarg IS_NULL =
      new SpecialSarg(ImmutableRangeSet.of(), RexUnknownAs.TRUE,
          "Sarg[IS NULL]", 4);

  /** Returns TRUE for all null and not-null values.
   *
   * <p>{@code SEARCH(x, TRUE)} is equivalent to {@code TRUE}. */
  private static final SpecialSarg TRUE =
      new SpecialSarg(ImmutableRangeSet.of().complement(), RexUnknownAs.TRUE,
          "Sarg[TRUE]", 5);

  /** Returns FALSE for all not-null values, UNKNOWN for null.
   *
   * <p>{@code SEARCH(x, NOT_EQUAL)} is equivalent to {@code x <> x}. */
  private static final SpecialSarg NOT_EQUAL =
      new SpecialSarg(ImmutableRangeSet.of(), RexUnknownAs.UNKNOWN,
          "Sarg[<>]", 6);

  /** Returns TRUE for all not-null values, UNKNOWN for null.
   *
   * <p>{@code SEARCH(x, EQUAL)} is equivalent to {@code x = x}. */
  private static final SpecialSarg EQUAL =
      new SpecialSarg(ImmutableRangeSet.of().complement(), RexUnknownAs.UNKNOWN,
          "Sarg[=]", 7);

  private Sarg(ImmutableRangeSet<C> rangeSet, RexUnknownAs nullAs) {
    this.rangeSet = requireNonNull(rangeSet, "rangeSet");
    this.nullAs = requireNonNull(nullAs, "nullAs");
    this.pointCount = RangeSets.countPoints(rangeSet);
  }

  @Deprecated // to be removed before 2.0
  public static <C extends Comparable<C>> Sarg<C> of(boolean containsNull,
      RangeSet<C> rangeSet) {
    return of(containsNull ? RexUnknownAs.TRUE : RexUnknownAs.UNKNOWN,
        rangeSet);
  }

  /** Creates a search argument. */
  public static <C extends Comparable<C>> Sarg<C> of(RexUnknownAs nullAs,
      RangeSet<C> rangeSet) {
    if (rangeSet.isEmpty()) {
      switch (nullAs) {
      case FALSE:
        return FALSE;
      case TRUE:
        return IS_NULL;
      default:
        return NOT_EQUAL;
      }
    }
    if (rangeSet.equals(RangeSets.rangeSetAll())) {
      switch (nullAs) {
      case FALSE:
        return IS_NOT_NULL;
      case TRUE:
        return TRUE;
      default:
        return EQUAL;
      }
    }
    return new Sarg<>(ImmutableRangeSet.copyOf(rangeSet), nullAs);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Produces a similar result to {@link RangeSet},
   * but adds "; NULL AS FALSE" or "; NULL AS TRUE" to indicate {@link #nullAs},
   * and simplifies point ranges.
   *
   * <p>For example, the Sarg that allows the range set
   *
   * <blockquote>{@code [[7..7], [9..9], (10..+∞)]}</blockquote>
   *
   * <p>and also null is printed as
   *
   * <blockquote>{@code Sarg[7, 9, (10..+∞); NULL AS TRUE]}</blockquote>
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
    switch (nullAs) {
    case FALSE:
      return sb.append("; NULL AS FALSE]");
    case TRUE:
      return sb.append("; NULL AS TRUE]");
    case UNKNOWN:
      return sb.append("]");
    default:
      throw new AssertionError();
    }
  }

  @Override public int compareTo(Sarg<C> o) {
    return RangeSets.compare(rangeSet, o.rangeSet);
  }

  @Override public int hashCode() {
    return RangeSets.hashCode(rangeSet) * 31 + nullAs.ordinal();
  }

  @Override public boolean equals(@Nullable Object o) {
    return o == this
        || o instanceof Sarg
        && nullAs == ((Sarg) o).nullAs
        && rangeSet.equals(((Sarg) o).rangeSet);
  }

  /** Returns whether this Sarg includes all values (including or not including
   * null). */
  public boolean isAll() {
    return false;
  }

  /** Returns whether this Sarg includes no values (including or not including
   * null). */
  public boolean isNone() {
    return false;
  }

  /** Returns whether this Sarg is a collection of 1 or more
   * points (and perhaps an {@code IS NULL} if
   * {@code nullAs == RexUnknownAs.TRUE}).
   *
   * <p>Such sargs could be translated as {@code ref = value}
   * or {@code ref IN (value1, ...)}. */
  public boolean isPoints() {
    return pointCount == rangeSet.asRanges().size();
  }

  /** Returns whether this Sarg, when negated, is a collection of 1 or more
   * points (and perhaps an {@code IS NULL} if
   * {@code nullAs == RexUnknownAs.TRUE}).
   *
   * <p>Such sargs could be translated as {@code ref <> value}
   * or {@code ref NOT IN (value1, ...)}. */
  public boolean isComplementedPoints() {
    return rangeSet.span().encloses(Range.all())
        && !rangeSet.equals(RangeSets.rangeSetAll())
        && rangeSet.complement().asRanges().stream()
            .allMatch(RangeSets::isPoint);
  }

  /** Returns a measure of the complexity of this expression.
   *
   * <p>It is basically the number of values that need to be checked against
   * (including NULL).
   *
   * <p>Examples:
   * <ul>
   *   <li>{@code x = 1}, {@code x <> 1}, {@code x > 1} have complexity 1
   *   <li>{@code x > 1 or x is null} has complexity 2
   *   <li>{@code x in (2, 4, 6) or x > 20} has complexity 4
   *   <li>{@code x between 3 and 8 or x between 10 and 20} has complexity 2
   * </ul>
   */
  public int complexity() {
    int complexity;
    if (rangeSet.asRanges().size() == 2
        && rangeSet.complement().asRanges().size() == 1
        && RangeSets.isPoint(
            Iterables.getOnlyElement(rangeSet.complement().asRanges()))) {
      // The complement of a point is a range set with two elements.
      // For example, "x <> 1" is "[(-inf, 1), (1, inf)]".
      // We want this to have complexity 1.
      complexity = 1;
    } else {
      complexity = rangeSet.asRanges().size();
    }
    if (nullAs == RexUnknownAs.TRUE) {
      ++complexity;
    }
    return complexity;
  }

  /** Returns a Sarg that matches a value if and only this Sarg does not. */
  public Sarg negate() {
    return Sarg.of(nullAs.negate(), rangeSet.complement());
  }

  /** Sarg whose range is all or none.
   *
   * <p>There are only 6 instances: {all, none} * {true, false, unknown}.
   *
   * @param <C> Value type */
  private static class SpecialSarg<C extends Comparable<C>> extends Sarg<C> {
    final String name;
    final int ordinal;

    SpecialSarg(ImmutableRangeSet<C> rangeSet, RexUnknownAs nullAs, String name,
        int ordinal) {
      super(rangeSet, nullAs);
      this.name = name;
      this.ordinal = ordinal;
      assert rangeSet.isEmpty() == ((ordinal & 1) == 0);
      assert rangeSet.equals(RangeSets.rangeSetAll()) == ((ordinal & 1) == 1);
    }

    @Override public boolean equals(@Nullable Object o) {
      return this == o;
    }

    @Override public int hashCode() {
      return ordinal;
    }

    @Override public boolean isAll() {
      return (ordinal & 1) == 1;
    }

    @Override public boolean isNone() {
      return (ordinal & 1) == 0;
    }

    @Override public int complexity() {
      switch (ordinal) {
      case 2: // Sarg[FALSE]
        return 0; // for backwards compatibility
      case 5: // Sarg[TRUE]
        return 2; // for backwards compatibility
      default:
        return 1;
      }
    }

    @Override public StringBuilder printTo(StringBuilder sb,
        BiConsumer<StringBuilder, C> valuePrinter) {
      return sb.append(name);
    }

    @Override public String toString() {
      return name;
    }
  }
}
