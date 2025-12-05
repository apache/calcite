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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.RangeSets;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.chrono.ISOChronology;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

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
  public static @Nullable List<Interval> createInterval(RexNode e) {
    final List<Range<Long>> ranges = extractRanges(e, false);
    if (ranges == null) {
      // We did not succeed, bail out
      return null;
    }
    final TreeRangeSet condensedRanges = TreeRangeSet.create();
    for (Range r : ranges) {
      condensedRanges.add(r);
    }
    LOGGER.debug("Inferred ranges on interval : {}", condensedRanges);
    return toInterval(ImmutableList.<Range<Long>>copyOf(condensedRanges.asRanges()));
  }

  protected static List<Interval> toInterval(
      List<Range<Long>> ranges) {
    List<Interval> intervals = Util.transform(ranges, range -> {
      if (!range.hasLowerBound() && !range.hasUpperBound()) {
        return DruidTable.DEFAULT_INTERVAL;
      }
      long start = range.hasLowerBound()
          ? range.lowerEndpoint().longValue()
          : DruidTable.DEFAULT_INTERVAL.getStartMillis();
      long end = range.hasUpperBound()
          ? range.upperEndpoint().longValue()
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
    });
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Converted time ranges " + ranges + " to interval " + intervals);
    }
    return intervals;
  }

  protected static @Nullable List<Range<Long>> extractRanges(RexNode node, boolean withNot) {
    switch (node.getKind()) {
    case EQUALS:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    case DRUID_IN:
    case SEARCH:
      return leafToRanges((RexCall) node, withNot);

    case NOT:
      return extractRanges(((RexCall) node).getOperands().get(0), !withNot);

    case OR: {
      RexCall call = (RexCall) node;
      List<Range<Long>> intervals = new ArrayList<>();
      for (RexNode child : call.getOperands()) {
        List<Range<Long>> extracted =
            extractRanges(child, withNot);
        if (extracted != null) {
          intervals.addAll(extracted);
        }
      }
      return intervals;
    }

    case AND: {
      RexCall call = (RexCall) node;
      List<Range<Long>> ranges = new ArrayList<>();
      for (RexNode child : call.getOperands()) {
        List<Range<Long>> extractedRanges =
            extractRanges(child, false);
        if (extractedRanges == null || extractedRanges.isEmpty()) {
          // We could not extract, we bail out
          return null;
        }
        if (ranges.isEmpty()) {
          ranges.addAll(extractedRanges);
          continue;
        }
        List<Range<Long>> overlapped = new ArrayList<>();
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

  protected static @Nullable List<Range<Long>> leafToRanges(RexCall call, boolean withNot) {
    final ImmutableList.Builder<Range<Long>> ranges;
    switch (call.getKind()) {
    case EQUALS:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL: {
      final Long value;
      SqlKind kind = call.getKind();
      if (call.getOperands().get(0) instanceof RexInputRef
          && literalValue(call.getOperands().get(1)) != null) {
        value = requireNonNull(literalValue(call.getOperands().get(1)));
      } else if (call.getOperands().get(1) instanceof RexInputRef
          && literalValue(call.getOperands().get(0)) != null) {
        value = requireNonNull(literalValue(call.getOperands().get(0)));
        kind = kind.reverse();
      } else {
        return null;
      }
      switch (kind) {
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
    case BETWEEN: {
      final Long value1;
      final Long value2;
      if (literalValue(call.getOperands().get(2)) != null
          && literalValue(call.getOperands().get(3)) != null) {
        value1 = requireNonNull(literalValue(call.getOperands().get(2)));
        value2 = requireNonNull(literalValue(call.getOperands().get(3)));
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
    case DRUID_IN:
      ranges = ImmutableList.builder();
      for (RexNode operand : Util.skip(call.operands)) {
        final Long element = literalValue(operand);
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

    case SEARCH:
      if (!canTransformSearchToRange(call)) {
        return null;
      }
      final RexLiteral right = (RexLiteral) call.operands.get(1);
      final Sarg<?> sarg = requireNonNull(right.getValueAs(Sarg.class));
      ranges = ImmutableList.builder();
      for (Range range : sarg.rangeSet.asRanges()) {
        Range<Long> range2 = RangeSets.copy(range, DruidDateTimeUtils::toLong);
        if (withNot) {
          ranges.addAll(ImmutableRangeSet.of(range2).complement().asRanges());
        } else {
          ranges.add(range2);
        }
      }
      return ranges.build();

    default:
      return null;
    }
  }

  /**
   * Returns whether the given SEARCH call can be transformed to a Druid range.
   *
   * @param call a SEARCH call
   * @return whether the given SEARCH call can be transformed to a Druid range.
   */
  private static boolean canTransformSearchToRange(RexCall call) {
    assert call.getKind() == SqlKind.SEARCH;
    if (call.getOperands().get(0) instanceof RexInputRef) {
      SqlTypeName literalType = call.operands.get(1).getType().getSqlTypeName();
      switch (literalType) {
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case DATE:
        return true;
      default:
        return false;
      }
    }
    return false;
  }

  private static Long toLong(Comparable comparable) {
    if (comparable instanceof TimestampString) {
      TimestampString timestampString = (TimestampString) comparable;
      return timestampString.getMillisSinceEpoch();
    }
    if (comparable instanceof DateString) {
      DateString dataString = (DateString) comparable;
      return dataString.getMillisSinceEpoch();
    }
    throw new AssertionError("unsupported type: " + comparable.getClass());
  }

  /**
   * Returns the literal value for the given node, assuming it is a literal with
   * datetime type, or a cast that only alters nullability on top of a literal with
   * datetime type.
   */
  protected static @Nullable Long literalValue(RexNode node) {
    switch (node.getKind()) {
    case LITERAL:
      switch (((RexLiteral) node).getTypeName()) {
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        TimestampString tsVal = ((RexLiteral) node).getValueAs(TimestampString.class);
        if (tsVal == null) {
          return null;
        }
        return tsVal.getMillisSinceEpoch();
      case DATE:
        DateString dateVal = ((RexLiteral) node).getValueAs(DateString.class);
        if (dateVal == null) {
          return null;
        }
        return dateVal.getMillisSinceEpoch();
      default:
        break;
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
        return literalValue(operand);
      }
      break;
    default:
      break;
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
  public static @Nullable Granularity extractGranularity(RexNode node, String timeZone) {
    final int valueIndex;
    final int flagIndex;

    if (TimeExtractionFunction.isValidTimeExtract(node)) {
      flagIndex = 0;
      valueIndex = 1;
    } else if (TimeExtractionFunction.isValidTimeFloor(node)) {
      valueIndex = 0;
      flagIndex = 1;
    } else {
      // We can only infer granularity from floor and extract.
      return null;
    }
    final RexCall call = (RexCall) node;
    final RexNode value = call.operands.get(valueIndex);
    final RexLiteral flag = (RexLiteral) call.operands.get(flagIndex);
    final TimeUnitRange timeUnit =
        requireNonNull((TimeUnitRange) flag.getValue());

    final RelDataType valueType = value.getType();
    if (valueType.getSqlTypeName() == SqlTypeName.DATE
        || valueType.getSqlTypeName() == SqlTypeName.TIMESTAMP) {
      // We use 'UTC' for date/timestamp type as Druid needs timezone information
      return Granularities.createGranularity(timeUnit, "UTC");
    } else if (valueType.getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      return Granularities.createGranularity(timeUnit, timeZone);
    }
    // Type not recognized
    return null;
  }

  /**
   * Converts a granularity to ISO period format.
   *
   * @param type Druid Granularity  to translate as period of time
   *
   * @return String representing the granularity as ISO8601 Period of Time; null
   * for unknown case
   */
  public static @Nullable String toISOPeriodFormat(Granularity.Type type) {
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
   * Translates a Calcite {@link TimeUnitRange} to a Druid {@link Granularity}.
   *
   * @param timeUnit Calcite Time unit to convert
   *
   * @return Druid Granularity or null
   */
  public static Granularity.@Nullable Type toDruidGranularity(TimeUnitRange timeUnit) {
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
