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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.TimestampWithTimeZoneString;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.base.Function;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;

import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.chrono.ISOChronology;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import javax.annotation.Nullable;

/**
 * Utilities for generating intervals from RexNode.
 */
@SuppressWarnings({"rawtypes", "unchecked" })
public class DruidDateTimeUtils {

  protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  private DruidDateTimeUtils() {
  }

  /**
   * Generates a list of {@link Interval}s equivalent to a given
   * expression. Assumes that all the predicates in the input
   * reference a single column: the timestamp column.
   */
  @Nullable
  public static List<Interval> createInterval(RexNode e, String timeZone) {
    final List<Range<TimestampString>> ranges =
        extractRanges(e, TimeZone.getTimeZone(timeZone), false);
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
    return toInterval(
        ImmutableList.<Range>copyOf(condensedRanges.asRanges()));
  }

  protected static List<Interval> toInterval(
      List<Range<TimestampString>> ranges) {
    List<Interval> intervals = Lists.transform(ranges,
        new Function<Range<TimestampString>, Interval>() {
          public Interval apply(Range<TimestampString> range) {
            if (!range.hasLowerBound() && !range.hasUpperBound()) {
              return DruidTable.DEFAULT_INTERVAL;
            }
            long start = range.hasLowerBound()
                ? range.lowerEndpoint().getMillisSinceEpoch()
                : DruidTable.DEFAULT_INTERVAL.getStartMillis();
            long end = range.hasUpperBound()
                ? range.upperEndpoint().getMillisSinceEpoch()
                : DruidTable.DEFAULT_INTERVAL.getEndMillis();
            if (range.hasLowerBound()
                && range.lowerBoundType() == BoundType.OPEN) {
              start++;
            }
            if (range.hasUpperBound()
                && range.upperBoundType() == BoundType.CLOSED) {
              end++;
            }
            return new Interval(start, end, ISOChronology.getInstanceUTC());
          }
        });
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Converted time ranges " + ranges + " to interval " + intervals);
    }
    return intervals;
  }

  @Nullable
  protected static List<Range<TimestampString>> extractRanges(RexNode node,
      TimeZone timeZone, boolean withNot) {
    switch (node.getKind()) {
    case EQUALS:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    case BETWEEN:
    case IN:
      return leafToRanges((RexCall) node, timeZone, withNot);

    case NOT:
      return extractRanges(((RexCall) node).getOperands().get(0), timeZone, !withNot);

    case OR: {
      RexCall call = (RexCall) node;
      List<Range<TimestampString>> intervals = Lists.newArrayList();
      for (RexNode child : call.getOperands()) {
        List<Range<TimestampString>> extracted =
            extractRanges(child, timeZone, withNot);
        if (extracted != null) {
          intervals.addAll(extracted);
        }
      }
      return intervals;
    }

    case AND: {
      RexCall call = (RexCall) node;
      List<Range<TimestampString>> ranges = new ArrayList<>();
      for (RexNode child : call.getOperands()) {
        List<Range<TimestampString>> extractedRanges =
            extractRanges(child, timeZone, false);
        if (extractedRanges == null || extractedRanges.isEmpty()) {
          // We could not extract, we bail out
          return null;
        }
        if (ranges.isEmpty()) {
          ranges.addAll(extractedRanges);
          continue;
        }
        List<Range<TimestampString>> overlapped = new ArrayList<>();
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

  @Nullable
  protected static List<Range<TimestampString>> leafToRanges(RexCall call,
      TimeZone timeZone, boolean withNot) {
    switch (call.getKind()) {
    case EQUALS:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    {
      final TimestampString value;
      if (call.getOperands().get(0) instanceof RexInputRef
          && literalValue(call.getOperands().get(1), timeZone) != null) {
        value = literalValue(call.getOperands().get(1), timeZone);
      } else if (call.getOperands().get(1) instanceof RexInputRef
          && literalValue(call.getOperands().get(0), timeZone) != null) {
        value = literalValue(call.getOperands().get(0), timeZone);
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
      final TimestampString value1;
      final TimestampString value2;
      if (literalValue(call.getOperands().get(2), timeZone) != null
          && literalValue(call.getOperands().get(3), timeZone) != null) {
        value1 = literalValue(call.getOperands().get(2), timeZone);
        value2 = literalValue(call.getOperands().get(3), timeZone);
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
      ImmutableList.Builder<Range<TimestampString>> ranges =
          ImmutableList.builder();
      for (RexNode operand : Util.skip(call.operands)) {
        final TimestampString element = literalValue(operand, timeZone);
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

  @Nullable
  protected static TimestampString literalValue(RexNode node, TimeZone timeZone) {
    switch (node.getKind()) {
    case LITERAL:
      switch (((RexLiteral) node).getTypeName()) {
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return ((RexLiteral) node).getValueAs(TimestampString.class);
      case TIMESTAMP:
        // Cast timestamp to timestamp with local time zone
        final TimestampString t = ((RexLiteral) node).getValueAs(TimestampString.class);
        return new TimestampWithTimeZoneString(t.toString() + " " + timeZone.getID())
            .withTimeZone(DateTimeUtils.UTC_ZONE).getLocalTimestampString();
      case DATE:
        // Cast date to timestamp with local time zone
        final DateString d = ((RexLiteral) node).getValueAs(DateString.class);
        return new TimestampWithTimeZoneString(
            TimestampString.fromMillisSinceEpoch(
                d.getMillisSinceEpoch()).toString() + " " + timeZone.getID())
            .withTimeZone(DateTimeUtils.UTC_ZONE).getLocalTimestampString();
      }
      break;
    case CAST:
      // Normally all CASTs are eliminated by now by constant reduction.
      // But when HiveExecutor is used there may be a cast that changes only
      // nullability, from TIMESTAMP NOT NULL literal to TIMESTAMP literal.
      // We can handle that case by traversing the dummy CAST.
      assert node instanceof RexCall;
      final RexCall call = (RexCall) node;
      final RexNode operand = call.getOperands().get(0);
      final RelDataType callType = call.getType();
      final RelDataType operandType = operand.getType();
      if (operand.getKind() == SqlKind.LITERAL
          && callType.getSqlTypeName() == operandType.getSqlTypeName()
          && (callType.getSqlTypeName() == SqlTypeName.DATE
              || callType.getSqlTypeName() == SqlTypeName.TIMESTAMP
              || callType.getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
          && callType.isNullable()
          && !operandType.isNullable()) {
        return literalValue(operand, timeZone);
      }
    }
    return null;
  }

  /**
   * Infers granularity from a time unit.
   * It supports {@code FLOOR(<time> TO <timeunit>)}
   * and {@code EXTRACT(<timeunit> FROM <time>)}.
   * Returns null if it cannot be inferred.
   *
   * @param node the Rex node
   * @return the granularity, or null if it cannot be inferred
   */
  public static Granularity extractGranularity(RexNode node, String timeZone) {
    final int flagIndex;

    if (TimeExtractionFunction.isValidTimeExtract(node)) {
      flagIndex = 0;
    } else if (TimeExtractionFunction.isValidTimeFloor(node)) {
      flagIndex = 1;
    } else {
      // We can only infer granularity from floor and extract.
      return null;
    }
    final RexCall call = (RexCall) node;
    final RexLiteral flag = (RexLiteral) call.operands.get(flagIndex);
    final TimeUnitRange timeUnit = (TimeUnitRange) flag.getValue();
    return Granularities.createGranularity(timeUnit, timeZone);
  }

  /**
   * @param type Druid Granularity  to translate as period of time
   *
   * @return String representing the granularity as ISO8601 Period of Time, null for unknown case.
   */
  @Nullable
  public static String toISOPeriodFormat(Granularity.Type type) {
    switch (type) {
    case SECOND:
      return Period.seconds(1).toString();
    case MINUTE:
      return Period.minutes(1).toString();
    case HOUR:
      return Period.hours(1).toString();
    case DAY:
      return Period.days(1).toString();
    case WEEK:
      return Period.weeks(1).toString();
    case MONTH:
      return Period.months(1).toString();
    case QUARTER:
      return Period.months(3).toString();
    case YEAR:
      return Period.years(1).toString();
    default:
      return null;
    }
  }

  /**
   * Translates Calcite TimeUnitRange to Druid {@link Granularity}
   * @param timeUnit Calcite Time unit to convert
   *
   * @return Druid Granularity or null
   */
  @Nullable
  public static Granularity.Type toDruidGranularity(TimeUnitRange timeUnit) {
    if (timeUnit == null) {
      return null;
    }
    switch (timeUnit) {
    case YEAR:
      return Granularity.Type.YEAR;
    case QUARTER:
      return Granularity.Type.QUARTER;
    case MONTH:
      return Granularity.Type.MONTH;
    case WEEK:
      return Granularity.Type.WEEK;
    case DAY:
      return Granularity.Type.DAY;
    case HOUR:
      return Granularity.Type.HOUR;
    case MINUTE:
      return Granularity.Type.MINUTE;
    case SECOND:
      return Granularity.Type.SECOND;
    default:
      return null;
    }
  }
}

// End DruidDateTimeUtils.java
