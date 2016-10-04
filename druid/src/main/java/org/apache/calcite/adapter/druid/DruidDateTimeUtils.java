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
package org.apache.calcite.adapter.druid;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.base.Function;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;

import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;

import org.slf4j.Logger;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Utilities for generating intervals from RexNode.
 */
@SuppressWarnings({"rawtypes", "unchecked" })
public class DruidDateTimeUtils {

  protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  private static final Pattern TIMESTAMP_PATTERN =
      Pattern.compile("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]"
          + " [0-9][0-9]:[0-9][0-9]:[0-9][0-9]");

  private DruidDateTimeUtils() {
  }

  /**
   * Generates a list of {@link Interval}s equivalent to a given
   * expression. Assumes that all the predicates in the input
   * reference a single column: the timestamp column.
   */
  public static List<Interval> createInterval(RelDataType type, RexNode e) {
    final List<Range> ranges = extractRanges(type, e, false);
    if (ranges == null) {
      // We did not succeed, bail out
      return null;
    }
    final TreeRangeSet condensedRanges = TreeRangeSet.create();
    for (Range r : ranges) {
      condensedRanges.add(r);
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Inferred ranges on interval : " + condensedRanges);
    }
    return toInterval(ImmutableList.<Range>copyOf(condensedRanges.asRanges()));
  }

  protected static List<Interval> toInterval(List<Range> ranges) {
    List<Interval> intervals = Lists.transform(ranges, new Function<Range, Interval>() {
      @Override public Interval apply(Range range) {
        if (!range.hasLowerBound() && !range.hasUpperBound()) {
          return DruidTable.DEFAULT_INTERVAL;
        }
        long start = range.hasLowerBound() ? toLong(range.lowerEndpoint())
            : DruidTable.DEFAULT_INTERVAL.getStartMillis();
        long end = range.hasUpperBound() ? toLong(range.upperEndpoint())
            : DruidTable.DEFAULT_INTERVAL.getEndMillis();
        if (range.hasLowerBound() && range.lowerBoundType() == BoundType.OPEN) {
          start++;
        }
        if (range.hasUpperBound() && range.upperBoundType() == BoundType.CLOSED) {
          end++;
        }
        return new Interval(start, end, ISOChronology.getInstanceUTC());
      }
    });
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Converted time ranges " + ranges + " to interval " + intervals);
    }
    return intervals;
  }

  protected static List<Range> extractRanges(RelDataType type, RexNode node,
      boolean withNot) {
    switch (node.getKind()) {
    case EQUALS:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    case BETWEEN:
    case IN:
      return leafToRanges(type, (RexCall) node, withNot);

    case NOT:
      return extractRanges(type, ((RexCall) node).getOperands().get(0), !withNot);

    case OR: {
      RexCall call = (RexCall) node;
      List<Range> intervals = Lists.newArrayList();
      for (RexNode child : call.getOperands()) {
        List<Range> extracted = extractRanges(type, child, withNot);
        if (extracted != null) {
          intervals.addAll(extracted);
        }
      }
      return intervals;
    }

    case AND: {
      RexCall call = (RexCall) node;
      List<Range> ranges = new ArrayList<>();
      for (RexNode child : call.getOperands()) {
        List<Range> extractedRanges = extractRanges(type, child, false);
        if (extractedRanges == null || extractedRanges.isEmpty()) {
          // We could not extract, we bail out
          return null;
        }
        if (ranges.isEmpty()) {
          ranges.addAll(extractedRanges);
          continue;
        }
        List<Range> overlapped = Lists.newArrayList();
        for (Range current : ranges) {
          for (Range interval : extractedRanges) {
            if (current.isConnected(interval)) {
              overlapped.add(current.intersection(interval));
            }
          }
        }
        ranges = overlapped;
      }
      return ranges;
    }

    default:
      return null;
    }
  }

  protected static List<Range> leafToRanges(RelDataType type, RexCall call,
      boolean withNot) {
    switch (call.getKind()) {
    case EQUALS:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    {
      RexLiteral literal = null;
      if (call.getOperands().get(0) instanceof RexInputRef
          && call.getOperands().get(1) instanceof RexLiteral) {
        literal = extractLiteral(call.getOperands().get(1));
      } else if (call.getOperands().get(0) instanceof RexInputRef
          && call.getOperands().get(1).getKind() == SqlKind.CAST) {
        literal = extractLiteral(call.getOperands().get(1));
      } else if (call.getOperands().get(1) instanceof RexInputRef
          && call.getOperands().get(0) instanceof RexLiteral) {
        literal = extractLiteral(call.getOperands().get(0));
      } else if (call.getOperands().get(1) instanceof RexInputRef
          && call.getOperands().get(0).getKind() == SqlKind.CAST) {
        literal = extractLiteral(call.getOperands().get(0));
      }
      if (literal == null) {
        return null;
      }
      Comparable value = literalToType(literal, type);
      if (value == null) {
        return null;
      }
      switch (call.getKind()) {
      case LESS_THAN:
        return Arrays.<Range>asList(withNot ? Range.atLeast(value) : Range.lessThan(value));
      case LESS_THAN_OR_EQUAL:
        return Arrays.<Range>asList(withNot ? Range.greaterThan(value) : Range.atMost(value));
      case GREATER_THAN:
        return Arrays.<Range>asList(withNot ? Range.atMost(value) : Range.greaterThan(value));
      case GREATER_THAN_OR_EQUAL:
        return Arrays.<Range>asList(withNot ? Range.lessThan(value) : Range.atLeast(value));
      default:
        if (!withNot) {
          return Arrays.<Range>asList(Range.closed(value, value));
        }
        return Arrays.<Range>asList(Range.lessThan(value), Range.greaterThan(value));
      }
    }
    case BETWEEN:
    {
      RexLiteral literal1 = extractLiteral(call.getOperands().get(2));
      if (literal1 == null) {
        return null;
      }
      RexLiteral literal2 = extractLiteral(call.getOperands().get(3));
      if (literal2 == null) {
        return null;
      }
      Comparable value1 = literalToType(literal1, type);
      Comparable value2 = literalToType(literal2, type);
      if (value1 == null || value2 == null) {
        return null;
      }
      boolean inverted = value1.compareTo(value2) > 0;
      if (!withNot) {
        return Arrays.<Range>asList(
            inverted ? Range.closed(value2, value1) : Range.closed(value1, value2));
      }
      return Arrays.<Range>asList(Range.lessThan(inverted ? value2 : value1),
          Range.greaterThan(inverted ? value1 : value2));
    }
    case IN:
    {
      List<Range> ranges = Lists.newArrayList();
      for (int i = 1; i < call.getOperands().size(); i++) {
        RexLiteral literal = extractLiteral(call.getOperands().get(i));
        if (literal == null) {
          return null;
        }
        Comparable element = literalToType(literal, type);
        if (element == null) {
          return null;
        }
        if (withNot) {
          ranges.addAll(
              Arrays.<Range>asList(Range.lessThan(element), Range.greaterThan(element)));
        } else {
          ranges.add(Range.closed(element, element));
        }
      }
      return ranges;
    }
    default:
      return null;
    }
  }

  @SuppressWarnings("incomplete-switch")
  protected static Comparable literalToType(RexLiteral literal, RelDataType type) {
    // Extract
    Object value = null;
    switch (literal.getType().getSqlTypeName()) {
    case DATE:
    case TIME:
    case TIMESTAMP:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_DAY:
      value = literal.getValue();
      break;
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
    case DOUBLE:
    case DECIMAL:
    case FLOAT:
    case REAL:
    case VARCHAR:
    case CHAR:
    case BOOLEAN:
      value = literal.getValue3();
    }
    if (value == null) {
      return null;
    }

    // Convert
    switch (type.getSqlTypeName()) {
    case BIGINT:
      return toLong(value);
    case INTEGER:
      return toInt(value);
    case FLOAT:
      return toFloat(value);
    case DOUBLE:
      return toDouble(value);
    case VARCHAR:
    case CHAR:
      return String.valueOf(value);
    case TIMESTAMP:
      return toTimestamp(value);
    }
    return null;
  }

  private static RexLiteral extractLiteral(RexNode node) {
    RexNode target = node;
    if (node.getKind() == SqlKind.CAST) {
      target = ((RexCall) node).getOperands().get(0);
    }
    if (!(target instanceof RexLiteral)) {
      return null;
    }
    return (RexLiteral) target;
  }

  private static Comparable toTimestamp(Object literal) {
    if (literal instanceof Timestamp) {
      return (Timestamp) literal;
    }
    final Long v = toLong(literal);
    if (v != null) {
      return new Timestamp(v);
    }
    return null;
  }

  private static Long toLong(Object literal) {
    if (literal instanceof Number) {
      return ((Number) literal).longValue();
    }
    if (literal instanceof Date) {
      return ((Date) literal).getTime();
    }
    if (literal instanceof Timestamp) {
      return ((Timestamp) literal).getTime();
    }
    if (literal instanceof Calendar) {
      return ((Calendar) literal).getTime().getTime();
    }
    if (literal instanceof String) {
      final String s = (String) literal;
      try {
        return Long.valueOf(s);
      } catch (NumberFormatException e) {
        // ignore
      }
      if (TIMESTAMP_PATTERN.matcher(s).matches()) {
        return DateTimeUtils.timestampStringToUnixDate(s);
      }
    }
    return null;
  }


  private static Integer toInt(Object literal) {
    if (literal instanceof Number) {
      return ((Number) literal).intValue();
    }
    if (literal instanceof String) {
      try {
        return Integer.valueOf((String) literal);
      } catch (NumberFormatException e) {
        // ignore
      }
    }
    return null;
  }

  private static Float toFloat(Object literal) {
    if (literal instanceof Number) {
      return ((Number) literal).floatValue();
    }
    if (literal instanceof String) {
      try {
        return Float.valueOf((String) literal);
      } catch (NumberFormatException e) {
        // ignore
      }
    }
    return null;
  }

  private static Double toDouble(Object literal) {
    if (literal instanceof Number) {
      return ((Number) literal).doubleValue();
    }
    if (literal instanceof String) {
      try {
        return Double.valueOf((String) literal);
      } catch (NumberFormatException e) {
        // ignore
      }
    }
    return null;
  }

  /**
   * Extract the total time span covered by these intervals. It does not check
   * if the intervals overlap.
   * @param intervals list of intervals
   * @return total time span covered by these intervals
   */
  public static long extractTotalTime(List<Interval> intervals) {
    long totalTime = 0;
    for (Interval interval : intervals) {
      totalTime += interval.getEndMillis() - interval.getStartMillis();
    }
    return totalTime;
  }

  /**
   * Extracts granularity from a call {@code FLOOR(<time> TO <timeunit>)}.
   * Timeunit specifies the granularity. Returns null if it cannot
   * be inferred.
   *
   * @param call the function call
   * @return the granularity, or null if it cannot be inferred
   */
  public static String extractGranularity(RexCall call) {
    if (call.getKind() != SqlKind.FLOOR
        || call.getOperands().size() != 2) {
      return null;
    }
    final RexLiteral flag = (RexLiteral) call.operands.get(1);
    final TimeUnitRange timeUnit = (TimeUnitRange) flag.getValue();
    if (timeUnit == null) {
      return null;
    }
    switch (timeUnit) {
    case YEAR:
    case QUARTER:
    case MONTH:
    case WEEK:
    case DAY:
    case HOUR:
    case MINUTE:
    case SECOND:
      return timeUnit.name();
    default:
      return null;
    }
  }

}

// End DruidDateTimeUtils.java
