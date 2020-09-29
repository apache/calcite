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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

import java.util.Objects;
import java.util.function.BiConsumer;

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

  private Sarg(ImmutableRangeSet<C> rangeSet, boolean containsNull) {
    this.rangeSet = Objects.requireNonNull(rangeSet);
    this.containsNull = containsNull;
    this.pointCount = RangeSets.countPoints(rangeSet);
  }

  /** Creates a search argument. */
  public static <C extends Comparable<C>> Sarg<C> of(boolean containsNull,
      RangeSet<C> rangeSet) {
    return new Sarg<>(ImmutableRangeSet.copyOf(rangeSet), containsNull);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Produces a similar result to {@link RangeSet}, but adds ", null"
   * if nulls are matched, and simplifies point ranges. For example,
   * the Sarg that allows the range set
   *
   * <blockquote>{@code [[7..7], [9..9], (10..+∞)]}</blockquote>
   *
   * and also null is printed as
   *
   * <blockquote>{@code Sarg[7, 9, (10..+∞), null]}</blockquote>
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
}
