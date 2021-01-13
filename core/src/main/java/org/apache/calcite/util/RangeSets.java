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

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import java.util.Iterator;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

/** Utilities for Guava {@link com.google.common.collect.RangeSet}. */
@SuppressWarnings({"BetaApi", "UnstableApiUsage"})
public class RangeSets {
  private RangeSets() {}

  @SuppressWarnings({"BetaApi", "rawtypes"})
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
      c = r0.lowerEndpoint().compareTo(r1.lowerEndpoint());
      if (c != 0) {
        return c;
      }
      c = r0.lowerBoundType().compareTo(r1.lowerBoundType());
      if (c != 0) {
        return c;
      }
    }
    c = Boolean.compare(r0.hasUpperBound(), r1.hasUpperBound());
    if (c != 0) {
      return -c;
    }
    if (r0.hasUpperBound()) {
      c = r0.upperEndpoint().compareTo(r1.upperEndpoint());
      if (c != 0) {
        return c;
      }
      c = r0.upperBoundType().compareTo(r1.upperBoundType());
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

  /** Returns whether a range set is a single open interval. */
  public static <C extends Comparable<C>> boolean isOpenInterval(RangeSet<C> rangeSet) {
    if (rangeSet.isEmpty()) {
      return false;
    }
    final Set<Range<C>> ranges = rangeSet.asRanges();
    final Range<C> range = ranges.iterator().next();
    return ranges.size() == 1
        && (!range.hasLowerBound() || !range.hasUpperBound());
  }

  /** Returns the number of ranges in a range set that are points.
   *
   * <p>If every range in a range set is a point then it can be converted to a
   * SQL IN list. */
  public static <C extends Comparable<C>> int countPoints(RangeSet<C> rangeSet) {
    int n = 0;
    for (Range<C> range : rangeSet.asRanges()) {
      if (isPoint(range)) {
        ++n;
      }
    }
    return n;
  }

  /** Calls the appropriate handler method for each range in a range set,
   * creating a new range set from the results. */
  public static <C extends Comparable<C>, C2 extends Comparable<C2>>
      RangeSet<C2> map(RangeSet<C> rangeSet, Handler<C, Range<C2>> handler) {
    final ImmutableRangeSet.Builder<C2> builder = ImmutableRangeSet.builder();
    rangeSet.asRanges().forEach(range -> builder.add(map(range, handler)));
    return builder.build();
  }

  /** Calls the appropriate handler method for the type of range. */
  public static <C extends Comparable<C>, R> R map(Range<C> range,
      Handler<C, R> handler) {
    if (range.hasLowerBound() && range.hasUpperBound()) {
      final C lower = range.lowerEndpoint();
      final C upper = range.upperEndpoint();
      if (range.lowerBoundType() == BoundType.OPEN) {
        if (range.upperBoundType() == BoundType.OPEN) {
          return handler.open(lower, upper);
        } else {
          return handler.openClosed(lower, upper);
        }
      } else {
        if (range.upperBoundType() == BoundType.OPEN) {
          return handler.closedOpen(lower, upper);
        } else {
          if (lower.equals(upper)) {
            return handler.singleton(lower);
          } else {
            return handler.closed(lower, upper);
          }
        }
      }
    } else if (range.hasLowerBound()) {
      final C lower = range.lowerEndpoint();
      if (range.lowerBoundType() == BoundType.OPEN) {
        return handler.greaterThan(lower);
      } else {
        return handler.atLeast(lower);
      }
    } else if (range.hasUpperBound()) {
      final C upper = range.upperEndpoint();
      if (range.upperBoundType() == BoundType.OPEN) {
        return handler.lessThan(upper);
      } else {
        return handler.atMost(upper);
      }
    } else {
      return handler.all();
    }
  }

  /** Copies a range set. */
  public static <C extends Comparable<C>, C2 extends Comparable<C2>>
      RangeSet<C2> copy(RangeSet<C> rangeSet, Function<C, C2> map) {
    return map(rangeSet, new CopyingHandler<C, C2>() {
      @Override C2 convert(C c) {
        return map.apply(c);
      }
    });
  }

  /** Copies a range. */
  public static <C extends Comparable<C>, C2 extends Comparable<C2>>
      Range<C2> copy(Range<C> range, Function<C, C2> map) {
    return map(range, new CopyingHandler<C, C2>() {
      @Override C2 convert(C c) {
        return map.apply(c);
      }
    });
  }

  public static <C extends Comparable<C>> void forEach(RangeSet<C> rangeSet,
      Consumer<C> consumer) {
    rangeSet.asRanges().forEach(range -> forEach(range, consumer));
  }

  public static <C extends Comparable<C>> void forEach(Range<C> range,
      Consumer<C> consumer) {
    if (range.hasLowerBound() && range.hasUpperBound()) {
      final C lower = range.lowerEndpoint();
      final C upper = range.upperEndpoint();
      if (range.lowerBoundType() == BoundType.OPEN) {
        if (range.upperBoundType() == BoundType.OPEN) {
          consumer.open(lower, upper);
        } else {
          consumer.openClosed(lower, upper);
        }
      } else {
        if (range.upperBoundType() == BoundType.OPEN) {
          consumer.closedOpen(lower, upper);
        } else {
          if (lower.equals(upper)) {
            consumer.singleton(lower);
          } else {
            consumer.closed(lower, upper);
          }
        }
      }
    } else if (range.hasLowerBound()) {
      final C lower = range.lowerEndpoint();
      if (range.lowerBoundType() == BoundType.OPEN) {
        consumer.greaterThan(lower);
      } else {
        consumer.atLeast(lower);
      }
    } else if (range.hasUpperBound()) {
      final C upper = range.upperEndpoint();
      if (range.upperBoundType() == BoundType.OPEN) {
        consumer.lessThan(upper);
      } else {
        consumer.atMost(upper);
      }
    } else {
      consumer.all();
    }
  }

  /** Creates a consumer that prints values to a {@link StringBuilder}. */
  public static <C extends Comparable<C>> Consumer<C> printer(StringBuilder sb,
      BiConsumer<StringBuilder, C> valuePrinter) {
    return new Printer<>(sb, valuePrinter);
  }

  /** Deconstructor for {@link Range} values.
   *
   * @param <C> Value type
   * @param <R> Return type
   *
   * @see Consumer */
  public interface Handler<C extends Comparable<C>, R> {
    R all();
    R atLeast(C lower);
    R atMost(C upper);
    R greaterThan(C lower);
    R lessThan(C upper);
    R singleton(C value);
    R closed(C lower, C upper);
    R closedOpen(C lower, C upper);
    R openClosed(C lower, C upper);
    R open(C lower, C upper);
  }

  /** Consumer of {@link Range} values.
   *
   * @param <C> Value type
   *
   * @see Handler */
  public interface Consumer<C extends Comparable<C>> {
    void all();
    void atLeast(C lower);
    void atMost(C upper);
    void greaterThan(C lower);
    void lessThan(C upper);
    void singleton(C value);
    void closed(C lower, C upper);
    void closedOpen(C lower, C upper);
    void openClosed(C lower, C upper);
    void open(C lower, C upper);
  }

  /** Handler that converts a Range into another Range of the same type,
   * applying a mapping function to the range's bound(s).
   *
   * @param <C> Value type
   * @param <C2> Output value type */
  private abstract static
      class CopyingHandler<C extends Comparable<C>, C2 extends Comparable<C2>>
      implements RangeSets.Handler<C, Range<C2>> {
    abstract C2 convert(C c);

    @Override public Range<C2> all() {
      return Range.all();
    }

    @Override public Range<C2> atLeast(C lower) {
      return Range.atLeast(convert(lower));
    }

    @Override public Range<C2> atMost(C upper) {
      return Range.atMost(convert(upper));
    }

    @Override public Range<C2> greaterThan(C lower) {
      return Range.greaterThan(convert(lower));
    }

    @Override public Range<C2> lessThan(C upper) {
      return Range.lessThan(convert(upper));
    }

    @Override public Range<C2> singleton(C value) {
      return Range.singleton(convert(value));
    }

    @Override public Range<C2> closed(C lower, C upper) {
      return Range.closed(convert(lower), convert(upper));
    }

    @Override public Range<C2> closedOpen(C lower, C upper) {
      return Range.closedOpen(convert(lower), convert(upper));
    }

    @Override public Range<C2> openClosed(C lower, C upper) {
      return Range.openClosed(convert(lower), convert(upper));
    }

    @Override public Range<C2> open(C lower, C upper) {
      return Range.open(convert(lower), convert(upper));
    }
  }

  /** Converts any type of range to a string, using a given value printer.
   *
   * @param <C> Value type */
  static class Printer<C extends Comparable<C>> implements Consumer<C> {
    private final StringBuilder sb;
    private final BiConsumer<StringBuilder, C> valuePrinter;

    Printer(StringBuilder sb, BiConsumer<StringBuilder, C> valuePrinter) {
      this.sb = sb;
      this.valuePrinter = valuePrinter;
    }

    @Override public void all() {
      sb.append("(-\u221e..+\u221e)");
    }

    @Override public void atLeast(C lower) {
      sb.append('[');
      valuePrinter.accept(sb, lower);
      sb.append("..+\u221e)");
    }

    @Override public void atMost(C upper) {
      sb.append("(-\u221e..");
      valuePrinter.accept(sb, upper);
      sb.append("]");
    }

    @Override public void greaterThan(C lower) {
      sb.append('(');
      valuePrinter.accept(sb, lower);
      sb.append("..+\u221e)");
    }

    @Override public void lessThan(C upper) {
      sb.append("(-\u221e..");
      valuePrinter.accept(sb, upper);
      sb.append(")");
    }

    @Override public void singleton(C value) {
      valuePrinter.accept(sb, value);
    }

    @Override public void closed(C lower, C upper) {
      sb.append('[');
      valuePrinter.accept(sb, lower);
      sb.append("..");
      valuePrinter.accept(sb, upper);
      sb.append(']');
    }

    @Override public void closedOpen(C lower, C upper) {
      sb.append('[');
      valuePrinter.accept(sb, lower);
      sb.append("..");
      valuePrinter.accept(sb, upper);
      sb.append(')');
    }

    @Override public void openClosed(C lower, C upper) {
      sb.append('(');
      valuePrinter.accept(sb, lower);
      sb.append("..");
      valuePrinter.accept(sb, upper);
      sb.append(']');
    }

    @Override public void open(C lower, C upper) {
      sb.append('(');
      valuePrinter.accept(sb, lower);
      sb.append("..");
      valuePrinter.accept(sb, upper);
      sb.append(')');
    }
  }
}
