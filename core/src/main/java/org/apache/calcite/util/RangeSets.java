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

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import java.util.Iterator;

/** Utilities for Guava {@link com.google.common.collect.RangeSet}. */
@SuppressWarnings({"UnstableApiUsage"})
public class RangeSets {
  private RangeSets() {}

  private static final ImmutableRangeSet ALL =
      ImmutableRangeSet.of().complement();

  /** Subtracts a range from a range set. */
  public static <C extends Comparable<C>> RangeSet<C> minus(RangeSet<C> rangeSet,
      Range<C> range) {
    final TreeRangeSet<C> mutableRangeSet = TreeRangeSet.create(rangeSet);
    mutableRangeSet.remove(range);
    return mutableRangeSet.equals(rangeSet) ? rangeSet
        : ImmutableRangeSet.copyOf(mutableRangeSet);
  }

  /** Returns the unrestricted range set. */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static <C extends Comparable<C>> RangeSet<C> rangeSetAll() {
    return (RangeSet) ALL;
  }

  /** Compares two range sets. */
  public static <C extends Comparable<C>> int compare(RangeSet<C> s0,
      RangeSet<C> s1) {
    final Iterator<Range<C>> i0 = s0.asRanges().iterator();
    final Iterator<Range<C>> i1 = s1.asRanges().iterator();
    for (;;) {
      final boolean h0 = i0.hasNext();
      final boolean h1 = i1.hasNext();
      if (!h0 || !h1) {
        return Boolean.compare(h0, h1);
      }
      final Range<C> r0 = i0.next();
      final Range<C> r1 = i1.next();
      int c = compare(r0, r1);
      if (c != 0) {
        return c;
      }
    }
  }

  /** Compares two ranges. */
  public static <C extends Comparable<C>> int compare(Range<C> r0,
      Range<C> r1) {
    int c = Boolean.compare(r0.hasLowerBound(), r1.hasLowerBound());
    if (c != 0) {
      return c;
    }
    if (r0.hasLowerBound()) {
      c = r0.lowerBoundType().compareTo(r1.lowerBoundType());
      if (c != 0) {
        return c;
      }
      c = r0.lowerEndpoint().compareTo(r1.lowerEndpoint());
      if (c != 0) {
        return c;
      }
    }
    c = Boolean.compare(r0.hasUpperBound(), r1.hasUpperBound());
    if (c != 0) {
      return c;
    }
    if (r0.hasUpperBound()) {
      c = r0.upperBoundType().compareTo(r1.upperBoundType());
      if (c != 0) {
        return c;
      }
      c = r0.upperEndpoint().compareTo(r1.upperEndpoint());
      if (c != 0) {
        return c;
      }
    }
    return 0;
  }

  /** Computes a hash code for a range set.
   *
   * <p>This method does not compute the same result as
   * {@link RangeSet#hashCode}. That is a poor hash code because it is based
   * upon {@link java.util.Set#hashCode}).
   *
   * <p>The algorithm is based on {@link java.util.List#hashCode()},
   * which is well-defined because {@link RangeSet#asRanges()} is sorted. */
  public static <C extends Comparable<C>> int hashCode(RangeSet<C> rangeSet) {
    int h = 1;
    for (Range<C> r : rangeSet.asRanges()) {
      h = 31 * h + r.hashCode();
    }
    return h;
  }

  /** Returns whether a range is a point. */
  public static <C extends Comparable<C>> boolean isPoint(Range<C> range) {
    return range.hasLowerBound()
        && range.hasUpperBound()
        && range.lowerEndpoint().equals(range.upperEndpoint())
        && !range.isEmpty();
  }
}
