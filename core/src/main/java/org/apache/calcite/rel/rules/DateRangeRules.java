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
package org.apache.calcite.rel.rules;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.runtime.PredicateImpl;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Collection of planner rules that convert
 * {@code EXTRACT(timeUnit FROM dateTime) = constant} to
 * {@code dateTime BETWEEN lower AND upper}.
 *
 * <p>The rules allow conversion of queries on time dimension tables, such as
 *
 * <blockquote>SELECT ... FROM sales JOIN time_by_day USING (time_id)
 * WHERE time_by_day.the_year = 1997
 * AND time_by_day.the_month IN (4, 5, 6)</blockquote>
 *
 * <p>into
 *
 * <blockquote>SELECT ... FROM sales JOIN time_by_day USING (time_id)
 * WHERE the_date BETWEEN DATE '2016-04-01' AND DATE '2016-06-30'</blockquote>
 *
 * <p>and is especially useful for Druid, which has a single timestamp column.
 */
public abstract class DateRangeRules {

  private DateRangeRules() {}

  private static final Predicate<Filter> FILTER_PREDICATE =
      new PredicateImpl<Filter>() {
        @Override public boolean test(Filter filter) {
          final ExtractFinder finder = ExtractFinder.THREAD_INSTANCES.get();
          assert finder.timeUnits.isEmpty() : "previous user did not clean up";
          try {
            filter.getCondition().accept(finder);
            return !finder.timeUnits.isEmpty();
          } finally {
            finder.timeUnits.clear();
          }
        }
      };

  public static final RelOptRule FILTER_INSTANCE =
      new FilterDateRangeRule(RelFactories.LOGICAL_BUILDER);

  private static final Map<TimeUnitRange, Integer> TIME_UNIT_CODES =
      ImmutableMap.<TimeUnitRange, Integer>builder()
          .put(TimeUnitRange.YEAR, Calendar.YEAR)
          .put(TimeUnitRange.MONTH, Calendar.MONTH)
          .put(TimeUnitRange.DAY, Calendar.DAY_OF_MONTH)
          .put(TimeUnitRange.HOUR, Calendar.HOUR)
          .put(TimeUnitRange.MINUTE, Calendar.MINUTE)
          .put(TimeUnitRange.SECOND, Calendar.SECOND)
          .put(TimeUnitRange.MILLISECOND, Calendar.MILLISECOND)
          .build();

  private static final Map<TimeUnitRange, TimeUnitRange> TIME_UNIT_PARENTS =
      ImmutableMap.<TimeUnitRange, TimeUnitRange>builder()
          .put(TimeUnitRange.MONTH, TimeUnitRange.YEAR)
          .put(TimeUnitRange.DAY, TimeUnitRange.MONTH)
          .put(TimeUnitRange.HOUR, TimeUnitRange.DAY)
          .put(TimeUnitRange.MINUTE, TimeUnitRange.HOUR)
          .put(TimeUnitRange.SECOND, TimeUnitRange.MINUTE)
          .put(TimeUnitRange.MILLISECOND, TimeUnitRange.SECOND)
          .put(TimeUnitRange.MICROSECOND, TimeUnitRange.SECOND)
          .build();

  /** Tests whether an expression contains one or more calls to the
   * {@code EXTRACT} function, and if so, returns the time units used.
   *
   * <p>The result is an immutable set in natural order. This is important,
   * because code relies on the collection being sorted (so YEAR comes before
   * MONTH before HOUR) and unique. A predicate on MONTH is not useful if there
   * is no predicate on YEAR. Then when we apply the predicate on DAY it doesn't
   * generate hundreds of ranges we'll later throw away. */
  static ImmutableSortedSet<TimeUnitRange> extractTimeUnits(RexNode e) {
    final ExtractFinder finder = ExtractFinder.THREAD_INSTANCES.get();
    try {
      assert finder.timeUnits.isEmpty() : "previous user did not clean up";
      e.accept(finder);
      return ImmutableSortedSet.copyOf(finder.timeUnits);
    } finally {
      finder.timeUnits.clear();
    }
  }

  /** Replaces calls to EXTRACT in an expression. */
  @VisibleForTesting
  public static RexNode replaceTimeUnits(RexBuilder rexBuilder, RexNode e) {
    final ImmutableSortedSet<TimeUnitRange> timeUnits = extractTimeUnits(e);
    if (!timeUnits.contains(TimeUnitRange.YEAR)) {
      // bail out if there is no year extract.
      return e;
    }
    final Map<String, RangeSet<Calendar>> operandRanges = new HashMap<>();
    for (TimeUnitRange timeUnit : timeUnits) {
      e = e.accept(
          new ExtractShuttle(rexBuilder, timeUnit, operandRanges, timeUnits));
    }
    return e;
  }

  /** Rule that converts EXTRACT in a Filter condition into a date range. */
  @SuppressWarnings("WeakerAccess")
  public static class FilterDateRangeRule extends RelOptRule {
    public FilterDateRangeRule(RelBuilderFactory relBuilderFactory) {
      super(operand(Filter.class, null, FILTER_PREDICATE, any()),
          relBuilderFactory, "FilterDateRangeRule");
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Filter filter = call.rel(0);
      final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
      final RexNode condition =
          replaceTimeUnits(rexBuilder, filter.getCondition());
      if (RexUtil.eq(condition, filter.getCondition())) {
        return;
      }
      final RelBuilder relBuilder =
          relBuilderFactory.create(filter.getCluster(), null);
      relBuilder.push(filter.getInput())
          .filter(condition);
      call.transformTo(relBuilder.build());
    }
  }

  /** Visitor that searches for calls to the {@code EXTRACT} function, building
   * a list of distinct time units. */
  private static class ExtractFinder extends RexVisitorImpl {
    private final Set<TimeUnitRange> timeUnits =
        EnumSet.noneOf(TimeUnitRange.class);

    private static final ThreadLocal<ExtractFinder> THREAD_INSTANCES =
        new ThreadLocal<ExtractFinder>() {
          @Override protected ExtractFinder initialValue() {
            return new ExtractFinder();
          }
        };

    private ExtractFinder() {
      super(true);
    }

    @Override public Object visitCall(RexCall call) {
      switch (call.getKind()) {
      case EXTRACT:
        final RexLiteral operand = (RexLiteral) call.getOperands().get(0);
        timeUnits.add((TimeUnitRange) operand.getValue());
      }
      return super.visitCall(call);
    }
  }

  /** Walks over an expression, replacing {@code EXTRACT} with date ranges. */
  @VisibleForTesting
  static class ExtractShuttle extends RexShuttle {
    private final RexBuilder rexBuilder;
    private final TimeUnitRange timeUnit;
    private final Map<String, RangeSet<Calendar>> operandRanges;
    private final Deque<RexCall> calls = new ArrayDeque<>();
    private final ImmutableSortedSet<TimeUnitRange> timeUnitRanges;

    @VisibleForTesting
    ExtractShuttle(RexBuilder rexBuilder, TimeUnitRange timeUnit,
        Map<String, RangeSet<Calendar>> operandRanges,
        ImmutableSortedSet<TimeUnitRange> timeUnitRanges) {
      this.rexBuilder = Preconditions.checkNotNull(rexBuilder);
      this.timeUnit = Preconditions.checkNotNull(timeUnit);
      Bug.upgrade("Change type to Map<RexNode, RangeSet<Calendar>> when"
          + " [CALCITE-1367] is fixed");
      this.operandRanges = Preconditions.checkNotNull(operandRanges);
      this.timeUnitRanges = Preconditions.checkNotNull(timeUnitRanges);
    }

    @Override public RexNode visitCall(RexCall call) {
      switch (call.getKind()) {
      case EQUALS:
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN:
      case LESS_THAN:
        final RexNode op0 = call.operands.get(0);
        final RexNode op1 = call.operands.get(1);
        switch (op0.getKind()) {
        case LITERAL:
          if (isExtractCall(op1) && canRewriteExtract()) {
            return compareExtract(call.getKind().reverse(),
                ((RexCall) op1).getOperands().get(1), (RexLiteral) op0);
          }
        }
        switch (op1.getKind()) {
        case LITERAL:
          if (isExtractCall(op0) && canRewriteExtract()) {
            return compareExtract(call.getKind(),
                ((RexCall) op0).getOperands().get(1), (RexLiteral) op1);
          }
        }
        // fall through
      default:
        calls.push(call);
        try {
          return super.visitCall(call);
        } finally {
          calls.pop();
        }
      }
    }

    private boolean canRewriteExtract() {
      // We rely on timeUnits being sorted (so YEAR comes before MONTH
      // before HOUR) and unique. If we have seen a predicate on YEAR,
      // operandRanges will not be empty. This checks whether we can rewrite
      // the "extract" condition. For example, in the condition
      //
      //   extract(MONTH from time) = someValue
      //   OR extract(YEAR from time) = someValue
      //
      // we cannot rewrite extract on MONTH.
      return timeUnit == TimeUnitRange.YEAR || !operandRanges.isEmpty();
    }

    @Override protected List<RexNode> visitList(List<? extends RexNode> exprs,
        boolean[] update) {
      if (exprs.isEmpty()) {
        return ImmutableList.of(); // a bit more efficient
      }
      switch (calls.peek().getKind()) {
      case AND:
        return super.visitList(exprs, update);
      default:
        if (timeUnit != TimeUnitRange.YEAR) {
          // Already visited for lower TimeUnit ranges in the loop below.
          // Early bail out.
          //noinspection unchecked
          return (List<RexNode>) exprs;
        }
        final Map<String, RangeSet<Calendar>> save =
            ImmutableMap.copyOf(operandRanges);
        final ImmutableList.Builder<RexNode> clonedOperands =
            ImmutableList.builder();
        for (RexNode operand : exprs) {
          RexNode clonedOperand = operand;
          for (TimeUnitRange timeUnit : timeUnitRanges) {
            clonedOperand = clonedOperand.accept(
                new ExtractShuttle(rexBuilder, timeUnit, operandRanges,
                    timeUnitRanges));
          }
          if ((clonedOperand != operand) && (update != null)) {
            update[0] = true;
          }
          clonedOperands.add(clonedOperand);

          // Restore the state. For an operator such as "OR", an argument
          // cannot inherit the previous argument's state.
          operandRanges.clear();
          operandRanges.putAll(save);
        }
        return clonedOperands.build();
      }
    }

    boolean isExtractCall(RexNode e) {
      switch (e.getKind()) {
      case EXTRACT:
        final RexCall call = (RexCall) e;
        final RexLiteral flag = (RexLiteral) call.operands.get(0);
        final TimeUnitRange timeUnit = (TimeUnitRange) flag.getValue();
        return timeUnit == this.timeUnit;
      default:
        return false;
      }
    }

    RexNode compareExtract(SqlKind comparison, RexNode operand,
        RexLiteral literal) {
      RangeSet<Calendar> rangeSet = operandRanges.get(operand.toString());
      if (rangeSet == null) {
        rangeSet = ImmutableRangeSet.<Calendar>of().complement();
      }
      final RangeSet<Calendar> s2 = TreeRangeSet.create();
      // Calendar.MONTH is 0-based
      final int v = ((BigDecimal) literal.getValue()).intValue()
          - (timeUnit == TimeUnitRange.MONTH ? 1 : 0);
      for (Range<Calendar> r : rangeSet.asRanges()) {
        final Calendar c;
        switch (timeUnit) {
        case YEAR:
          c = Util.calendar();
          c.clear();
          c.set(v, Calendar.JANUARY, 1);
          s2.add(range(timeUnit, comparison, c));
          break;
        case MONTH:
        case DAY:
        case HOUR:
        case MINUTE:
        case SECOND:
          if (r.hasLowerBound()) {
            c = (Calendar) r.lowerEndpoint().clone();
            int i = 0;
            while (next(c, timeUnit, v, r, i++ > 0)) {
              s2.add(range(timeUnit, comparison, c));
            }
          }
        }
      }
      // Intersect old range set with new.
      s2.removeAll(rangeSet.complement());
      operandRanges.put(operand.toString(), ImmutableRangeSet.copyOf(s2));
      final List<RexNode> nodes = new ArrayList<>();
      for (Range<Calendar> r : s2.asRanges()) {
        nodes.add(toRex(operand, r));
      }
      return RexUtil.composeDisjunction(rexBuilder, nodes);
    }

    private boolean next(Calendar c, TimeUnitRange timeUnit, int v,
        Range<Calendar> r, boolean strict) {
      final Calendar original = (Calendar) c.clone();
      final int code = TIME_UNIT_CODES.get(timeUnit);
      for (;;) {
        c.set(code, v);
        int v2 = c.get(code);
        if (v2 < v) {
          // E.g. when we set DAY=30 on 2014-02-01, we get 2014-02-30 because
          // February has 28 days.
          continue;
        }
        if (strict && original.compareTo(c) == 0) {
          c.add(TIME_UNIT_CODES.get(TIME_UNIT_PARENTS.get(timeUnit)), 1);
          continue;
        }
        if (!r.contains(c)) {
          return false;
        }
        return true;
      }
    }

    private RexNode toRex(RexNode operand, Range<Calendar> r) {
      final List<RexNode> nodes = new ArrayList<>();
      if (r.hasLowerBound()) {
        final SqlBinaryOperator op = r.lowerBoundType() == BoundType.CLOSED
            ? SqlStdOperatorTable.GREATER_THAN_OR_EQUAL
            : SqlStdOperatorTable.GREATER_THAN;
        nodes.add(
            rexBuilder.makeCall(op, operand,
                dateTimeLiteral(rexBuilder, r.lowerEndpoint(), operand)));
      }
      if (r.hasUpperBound()) {
        final SqlBinaryOperator op = r.upperBoundType() == BoundType.CLOSED
            ? SqlStdOperatorTable.LESS_THAN_OR_EQUAL
            : SqlStdOperatorTable.LESS_THAN;
        nodes.add(
            rexBuilder.makeCall(op, operand,
                dateTimeLiteral(rexBuilder, r.upperEndpoint(), operand)));
      }
      return RexUtil.composeConjunction(rexBuilder, nodes, false);
    }

    private RexLiteral dateTimeLiteral(RexBuilder rexBuilder, Calendar calendar,
        RexNode operand) {
      switch (operand.getType().getSqlTypeName()) {
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        final TimestampString ts = TimestampString.fromCalendarFields(calendar);
        return rexBuilder.makeTimestampLiteral(ts, operand.getType().getPrecision());
      case DATE:
        final DateString d = DateString.fromCalendarFields(calendar);
        return rexBuilder.makeDateLiteral(d);
      default:
        throw Util.unexpected(operand.getType().getSqlTypeName());
      }
    }

    private Range<Calendar> range(TimeUnitRange timeUnit, SqlKind comparison,
        Calendar c) {
      switch (comparison) {
      case EQUALS:
        return Range.closedOpen(round(c, timeUnit, true),
            round(c, timeUnit, false));
      case LESS_THAN:
        return Range.lessThan(round(c, timeUnit, true));
      case LESS_THAN_OR_EQUAL:
        return Range.lessThan(round(c, timeUnit, false));
      case GREATER_THAN:
        return Range.atLeast(round(c, timeUnit, false));
      case GREATER_THAN_OR_EQUAL:
        return Range.atLeast(round(c, timeUnit, true));
      default:
        throw new AssertionError(comparison);
      }
    }

    /** Returns a copy of a calendar, optionally rounded up to the next time
     * unit. */
    private Calendar round(Calendar c, TimeUnitRange timeUnit, boolean down) {
      c = (Calendar) c.clone();
      if (!down) {
        final Integer code = TIME_UNIT_CODES.get(timeUnit);
        final int v = c.get(code);
        c.set(code, v + 1);
      }
      return c;
    }
  }
}

// End DateRangeRules.java
