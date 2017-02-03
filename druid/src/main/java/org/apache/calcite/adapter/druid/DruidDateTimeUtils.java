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

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.base.Function;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;

import org.slf4j.Logger;

import java.util.ArrayList;
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
   * Generates a list of {@link LocalInterval}s equivalent to a given
   * expression. Assumes that all the predicates in the input
   * reference a single column: the timestamp column.
   */
  public static List<LocalInterval> createInterval(RelDataType type,
      RexNode e) {
    final List<Range<Calendar>> ranges = extractRanges(e, false);
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

  protected static List<LocalInterval> toInterval(List<Range<Calendar>> ranges) {
    List<LocalInterval> intervals = Lists.transform(ranges,
        new Function<Range<Calendar>, LocalInterval>() {
          public LocalInterval apply(Range<Calendar> range) {
            if (!range.hasLowerBound() && !range.hasUpperBound()) {
              return DruidTable.DEFAULT_INTERVAL;
            }
            long start = range.hasLowerBound()
                ? range.lowerEndpoint().getTime().getTime()
                : DruidTable.DEFAULT_INTERVAL.getStartMillis();
            long end = range.hasUpperBound()
                ? range.upperEndpoint().getTime().getTime()
                : DruidTable.DEFAULT_INTERVAL.getEndMillis();
            if (range.hasLowerBound()
                && range.lowerBoundType() == BoundType.OPEN) {
              start++;
            }
            if (range.hasUpperBound()
                && range.upperBoundType() == BoundType.CLOSED) {
              end++;
            }
            return LocalInterval.create(start, end);
          }
        });
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Converted time ranges " + ranges + " to interval " + intervals);
    }
    return intervals;
  }

  protected static List<Range<Calendar>> extractRanges(RexNode node,
      boolean withNot) {
    switch (node.getKind()) {
    case EQUALS:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    case BETWEEN:
    case IN:
      return leafToRanges((RexCall) node, withNot);

    case NOT:
      return extractRanges(((RexCall) node).getOperands().get(0), !withNot);

    case OR: {
      RexCall call = (RexCall) node;
      List<Range<Calendar>> intervals = Lists.newArrayList();
      for (RexNode child : call.getOperands()) {
        List<Range<Calendar>> extracted = extractRanges(child, withNot);
        if (extracted != null) {
          intervals.addAll(extracted);
        }
      }
      return intervals;
    }

    case AND: {
      RexCall call = (RexCall) node;
      List<Range<Calendar>> ranges = new ArrayList<>();
      for (RexNode child : call.getOperands()) {
        List<Range<Calendar>> extractedRanges = extractRanges(child, false);
        if (extractedRanges == null || extractedRanges.isEmpty()) {
          // We could not extract, we bail out
          return null;
        }
        if (ranges.isEmpty()) {
          ranges.addAll(extractedRanges);
          continue;
        }
        List<Range<Calendar>> overlapped = new ArrayList<>();
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

  protected static List<Range<Calendar>> leafToRanges(RexCall call,
      boolean withNot) {
    switch (call.getKind()) {
    case EQUALS:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    {
      final Calendar value;
      if (call.getOperands().get(0) instanceof RexInputRef
          && literalValue(call.getOperands().get(1)) != null) {
        value = literalValue(call.getOperands().get(1));
      } else if (call.getOperands().get(1) instanceof RexInputRef
          && literalValue(call.getOperands().get(0)) != null) {
        value = literalValue(call.getOperands().get(0));
      } else {
        return null;
      }
      switch (call.getKind()) {
      case LESS_THAN:
        return ImmutableList.of(withNot ? Range.atLeast(value) : Range.lessThan(value));
      case LESS_THAN_OR_EQUAL:
        return ImmutableList.of(withNot ? Range.greaterThan(value) : Range.atMost(value));
      case GREATER_THAN:
        return ImmutableList.of(withNot ? Range.atMost(value) : Range.greaterThan(value));
      case GREATER_THAN_OR_EQUAL:
        return ImmutableList.of(withNot ? Range.lessThan(value) : Range.atLeast(value));
      default:
        if (!withNot) {
          return ImmutableList.of(Range.closed(value, value));
        }
        return ImmutableList.of(Range.lessThan(value), Range.greaterThan(value));
      }
    }
    case BETWEEN:
    {
      final Calendar value1;
      final Calendar value2;
      if (literalValue(call.getOperands().get(2)) != null
          && literalValue(call.getOperands().get(3)) != null) {
        value1 = literalValue(call.getOperands().get(2));
        value2 = literalValue(call.getOperands().get(3));
      } else {
        return null;
      }

      boolean inverted = value1.compareTo(value2) > 0;
      if (!withNot) {
        return ImmutableList.of(
            inverted ? Range.closed(value2, value1) : Range.closed(value1, value2));
      }
      return ImmutableList.of(Range.lessThan(inverted ? value2 : value1),
          Range.greaterThan(inverted ? value1 : value2));
    }
    case IN:
    {
      ImmutableList.Builder<Range<Calendar>> ranges = ImmutableList.builder();
      for (RexNode operand : Util.skip(call.operands)) {
        final Calendar element = literalValue(operand);
        if (element == null) {
          return null;
        }
        if (withNot) {
          ranges.add(Range.lessThan(element));
          ranges.add(Range.greaterThan(element));
        } else {
          ranges.add(Range.closed(element, element));
        }
      }
      return ranges.build();
    }
    default:
      return null;
    }
  }

  private static Calendar literalValue(RexNode node) {
    if (node instanceof RexLiteral) {
      return (Calendar) ((RexLiteral) node).getValue();
    }
    return null;
  }

  /**
   * Extracts granularity from a call {@code FLOOR(<time> TO <timeunit>)}.
   * Timeunit specifies the granularity. Returns null if it cannot
   * be inferred.
   *
   * @param call the function call
   * @return the granularity, or null if it cannot be inferred
   */
  public static Granularity extractGranularity(RexCall call) {
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
      return Granularity.YEAR;
    case QUARTER:
      return Granularity.QUARTER;
    case MONTH:
      return Granularity.MONTH;
    case WEEK:
      return Granularity.WEEK;
    case DAY:
      return Granularity.DAY;
    case HOUR:
      return Granularity.HOUR;
    case MINUTE:
      return Granularity.MINUTE;
    case SECOND:
      return Granularity.SECOND;
    default:
      return null;
    }
  }

}

// End DruidDateTimeUtils.java
