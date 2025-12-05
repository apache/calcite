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
package org.apache.calcite.rex;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.metadata.NullSentinel;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.RangeSets;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.RangeSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.function.IntPredicate;

/**
 * Evaluates {@link RexNode} expressions.
 *
 * <p>Caveats:
 * <ul>
 *   <li>It uses interpretation, so it is not very efficient.
 *   <li>It is intended for testing, so does not cover very many functions and
 *   operators. (Feel free to contribute more!)
 *   <li>It is not well tested.
 * </ul>
 */
public class RexInterpreter implements RexVisitor<Comparable> {
  private static final NullSentinel N = NullSentinel.INSTANCE;

  public static final EnumSet<SqlKind> SUPPORTED_SQL_KIND =
      EnumSet.of(SqlKind.IS_NOT_DISTINCT_FROM, SqlKind.EQUALS, SqlKind.IS_DISTINCT_FROM,
          SqlKind.NOT_EQUALS, SqlKind.GREATER_THAN, SqlKind.GREATER_THAN_OR_EQUAL,
          SqlKind.LESS_THAN, SqlKind.LESS_THAN_OR_EQUAL, SqlKind.AND, SqlKind.OR,
          SqlKind.NOT, SqlKind.CASE, SqlKind.IS_TRUE, SqlKind.IS_NOT_TRUE,
          SqlKind.IS_FALSE, SqlKind.IS_NOT_FALSE, SqlKind.PLUS_PREFIX,
          SqlKind.MINUS_PREFIX, SqlKind.PLUS, SqlKind.MINUS, SqlKind.TIMES,
          SqlKind.DIVIDE, SqlKind.COALESCE, SqlKind.CEIL,
          SqlKind.FLOOR, SqlKind.EXTRACT);

  private final SqlFunctions.LikeFunction likeFunction =
      new SqlFunctions.LikeFunction();
  private final SqlFunctions.SimilarFunction similarFunction =
      new SqlFunctions.SimilarFunction();
  private final SqlFunctions.SimilarEscapeFunction similarEscapeFunction =
      new SqlFunctions.SimilarEscapeFunction();

  private final Map<RexNode, Comparable> environment;

  /** Creates an interpreter.
   *
   * @param environment Values of certain expressions (usually
   *       {@link RexInputRef}s)
   */
  private RexInterpreter(Map<RexNode, Comparable> environment) {
    this.environment = ImmutableMap.copyOf(environment);
  }

  /** Evaluates an expression in an environment. */
  public static @Nullable Comparable evaluate(RexNode e, Map<RexNode, Comparable> map) {
    final Comparable v = e.accept(new RexInterpreter(map));
    if (false) {
      System.out.println("evaluate " + e + " on " + map + " returns " + v);
    }
    return v;
  }

  private static IllegalArgumentException unbound(RexNode e) {
    return new IllegalArgumentException("unbound: " + e);
  }

  private Comparable getOrUnbound(RexNode e) {
    final Comparable comparable = environment.get(e);
    if (comparable != null) {
      return comparable;
    }
    throw unbound(e);
  }

  @Override public Comparable visitInputRef(RexInputRef inputRef) {
    return getOrUnbound(inputRef);
  }

  @Override public Comparable visitLocalRef(RexLocalRef localRef) {
    throw unbound(localRef);
  }

  @Override public Comparable visitLiteral(RexLiteral literal) {
    return Util.first(literal.getValue4(), N);
  }

  @Override public Comparable visitOver(RexOver over) {
    throw unbound(over);
  }

  @Override public Comparable visitCorrelVariable(RexCorrelVariable correlVariable) {
    return getOrUnbound(correlVariable);
  }

  @Override public Comparable visitDynamicParam(RexDynamicParam dynamicParam) {
    return getOrUnbound(dynamicParam);
  }

  @Override public Comparable visitRangeRef(RexRangeRef rangeRef) {
    throw unbound(rangeRef);
  }

  @Override public Comparable visitFieldAccess(RexFieldAccess fieldAccess) {
    return getOrUnbound(fieldAccess);
  }

  @Override public Comparable visitSubQuery(RexSubQuery subQuery) {
    throw unbound(subQuery);
  }

  @Override public Comparable visitTableInputRef(RexTableInputRef fieldRef) {
    throw unbound(fieldRef);
  }

  @Override public Comparable visitPatternFieldRef(RexPatternFieldRef fieldRef) {
    throw unbound(fieldRef);
  }

  @Override public Comparable visitLambda(RexLambda lambda) {
    throw unbound(lambda);
  }

  @Override public Comparable visitLambdaRef(RexLambdaRef lambdaRef) {
    throw unbound(lambdaRef);
  }

  @Override public Comparable visitNodeAndFieldIndex(RexNodeAndFieldIndex nodeAndFieldIndex) {
    throw unbound(nodeAndFieldIndex);
  }

  @Override public Comparable visitCall(RexCall call) {
    final List<Comparable> values = visitList(call.operands);
    switch (call.getKind()) {
    case IS_NOT_DISTINCT_FROM:
      if (containsNull(values)) {
        return values.get(0).equals(values.get(1));
      }
      // falls through EQUALS
    case EQUALS:
      return compare(values, c -> c == 0);
    case IS_DISTINCT_FROM:
      if (containsNull(values)) {
        return !values.get(0).equals(values.get(1));
      }
      // falls through NOT_EQUALS
    case NOT_EQUALS:
      return compare(values, c -> c != 0);
    case GREATER_THAN:
      return compare(values, c -> c > 0);
    case GREATER_THAN_OR_EQUAL:
      return compare(values, c -> c >= 0);
    case LESS_THAN:
      return compare(values, c -> c < 0);
    case LESS_THAN_OR_EQUAL:
      return compare(values, c -> c <= 0);
    case AND:
      return values.stream().map(Truthy::of).min(Comparator.naturalOrder())
          .get().toComparable();
    case OR:
      return values.stream().map(Truthy::of).max(Comparator.naturalOrder())
          .get().toComparable();
    case NOT:
      return not(values.get(0));
    case CASE:
      return case_(values);
    case IS_TRUE:
      return values.get(0).equals(true);
    case IS_NOT_TRUE:
      return !values.get(0).equals(true);
    case IS_NULL:
      return values.get(0).equals(N);
    case IS_NOT_NULL:
      return !values.get(0).equals(N);
    case IS_FALSE:
      return values.get(0).equals(false);
    case IS_NOT_FALSE:
      return !values.get(0).equals(false);
    case PLUS_PREFIX:
      return values.get(0);
    case MINUS_PREFIX:
      return containsNull(values) ? N
          : number(values.get(0)).negate();
    case PLUS:
      return containsNull(values) ? N
          : number(values.get(0)).add(number(values.get(1)));
    case MINUS:
      return containsNull(values) ? N
          : number(values.get(0)).subtract(number(values.get(1)));
    case TIMES:
      return containsNull(values) ? N
          : number(values.get(0)).multiply(number(values.get(1)));
    case DIVIDE:
      return containsNull(values) ? N
          : number(values.get(0)).divide(number(values.get(1)));
    case CAST:
      return cast(values);
    case COALESCE:
      return coalesce(values);
    case CEIL:
    case FLOOR:
      return ceil(call, values);
    case TRIM:
      return trim(call, values);
    case EXTRACT:
      return extract(values);
    case LIKE:
      return like(values);
    case SIMILAR:
      return similar(values);
    case SEARCH:
      return search(call.operands.get(1).getType().getSqlTypeName(), values);
    default:
      throw unbound(call);
    }
  }

  private static Comparable extract(List<Comparable> values) {
    final Comparable v = values.get(1);
    if (v == N) {
      return N;
    }
    final TimeUnitRange timeUnitRange = (TimeUnitRange) values.get(0);
    final int v2;
    if (v instanceof Long) {
      // TIMESTAMP
      v2 = (int) (((Long) v) / TimeUnit.DAY.multiplier.longValue());
    } else {
      // DATE
      v2 = (Integer) v;
    }
    return DateTimeUtils.unixDateExtract(timeUnitRange, v2);
  }

  private Comparable like(List<Comparable> values) {
    if (containsNull(values)) {
      return N;
    }
    final NlsString value = (NlsString) values.get(0);
    final NlsString pattern = (NlsString) values.get(1);
    switch (values.size()) {
    case 2:
      return likeFunction.like(value.getValue(), pattern.getValue());
    case 3:
      final NlsString escape = (NlsString) values.get(2);
      return likeFunction.like(value.getValue(), pattern.getValue(),
          escape.getValue());
    default:
      throw new AssertionError();
    }
  }

  private Comparable similar(List<Comparable> values) {
    if (containsNull(values)) {
      return N;
    }
    final NlsString value = (NlsString) values.get(0);
    final NlsString pattern = (NlsString) values.get(1);
    switch (values.size()) {
    case 2:
      return similarFunction.similar(value.getValue(), pattern.getValue());
    case 3:
      final NlsString escape = (NlsString) values.get(2);
      return similarEscapeFunction.similar(value.getValue(), pattern.getValue(),
          escape.getValue());
    default:
      throw new AssertionError();
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Comparable search(SqlTypeName typeName, List<Comparable> values) {
    Comparable value = values.get(0);
    final Sarg sarg = (Sarg) values.get(1);
    if (value == N) {
      switch (sarg.nullAs) {
      case FALSE:
        return false;
      case TRUE:
        return true;
      default:
        return N;
      }
    }
    if (SqlTypeName.NUMERIC_TYPES.contains(typeName)) {
      value = number(value);
    }
    return translate(sarg.rangeSet, typeName).contains(value);
  }

  /** Translates the values in a RangeSet from literal format to runtime format.
   * For example the DATE SQL type uses DateString for literals and Integer at
   * runtime. */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private static RangeSet translate(RangeSet rangeSet, SqlTypeName typeName) {
    switch (typeName) {
    case DATE:
      return RangeSets.copy(rangeSet, DateString::getDaysSinceEpoch);
    case TIME:
      return RangeSets.copy(rangeSet, TimeString::getMillisOfDay);
    case TIMESTAMP:
      return RangeSets.copy(rangeSet, TimestampString::getMillisSinceEpoch);
    default:
      if (SqlTypeName.NUMERIC_TYPES.contains(typeName)) {
        return RangeSets.copy(rangeSet, RexInterpreter::number);
      }
      return rangeSet;
    }
  }

  private static Comparable coalesce(List<Comparable> values) {
    for (Comparable value : values) {
      if (value != N) {
        return value;
      }
    }
    return N;
  }

  private static Comparable trim(RexNode call, List<Comparable> values) {
    if (containsNull(values)) {
      return N;
    }
    NlsString trimType = (NlsString) values.get(0);
    NlsString trimed = (NlsString) values.get(1);
    NlsString trimString = (NlsString) values.get(2);
    switch (trimType.getValue()) {
    case "BOTH":
      return trimString.trim(trimed.getValue());
    case "LEADING":
      return trimString.ltrim(trimed.getValue());
    case "TRAILING":
      return trimString.rtrim(trimed.getValue());
    default:
      throw unbound(call);
    }
  }

  private static Comparable ceil(RexCall call, List<Comparable> values) {
    if (values.get(0) == N) {
      return N;
    }
    final Long v = (Long) values.get(0);
    final TimeUnitRange unit = (TimeUnitRange) values.get(1);
    switch (unit) {
    case YEAR:
    case MONTH:
      switch (call.getKind()) {
      case FLOOR:
        return DateTimeUtils.unixTimestampFloor(unit, v);
      default:
        return DateTimeUtils.unixTimestampCeil(unit, v);
      }
    default:
      break;
    }
    final TimeUnitRange subUnit = subUnit(unit);
    for (long v2 = v;;) {
      final int e = DateTimeUtils.unixTimestampExtract(subUnit, v2);
      if (e == 0) {
        return v2;
      }
      v2 -= unit.startUnit.multiplier.longValue();
    }
  }

  private static TimeUnitRange subUnit(TimeUnitRange unit) {
    switch (unit) {
    case QUARTER:
      return TimeUnitRange.MONTH;
    default:
      return TimeUnitRange.DAY;
    }
  }

  private static Comparable cast(List<Comparable> values) {
    if (values.get(0) == N) {
      return N;
    }
    return values.get(0);
  }

  private static Comparable not(Comparable value) {
    if (value.equals(true)) {
      return false;
    } else if (value.equals(false)) {
      return true;
    } else {
      return N;
    }
  }

  private static Comparable case_(List<Comparable> values) {
    final int size;
    final Comparable elseValue;
    if (values.size() % 2 == 0) {
      size = values.size();
      elseValue = N;
    } else {
      size = values.size() - 1;
      elseValue = Util.last(values);
    }
    for (int i = 0; i < size; i += 2) {
      if (values.get(i).equals(true)) {
        return values.get(i + 1);
      }
    }
    return elseValue;
  }

  private static BigDecimal number(Comparable comparable) {
    return comparable instanceof BigDecimal
        ? (BigDecimal) comparable
        : comparable instanceof BigInteger
        ? new BigDecimal((BigInteger) comparable)
            : comparable instanceof Long
                || comparable instanceof Integer
                || comparable instanceof Short
        ? new BigDecimal(((Number) comparable).longValue())
        : new BigDecimal(((Number) comparable).doubleValue());
  }

  private static Comparable compare(List<Comparable> values, IntPredicate p) {
    if (containsNull(values)) {
      return N;
    }
    Comparable v0 = values.get(0);
    Comparable v1 = values.get(1);

    if (v0 instanceof Number && v1 instanceof NlsString) {
      try {
        v1 = new BigDecimal(((NlsString) v1).getValue());
      } catch (NumberFormatException e) {
        return false;
      }
    }
    if (v1 instanceof Number && v0 instanceof NlsString) {
      try {
        v0 = new BigDecimal(((NlsString) v0).getValue());
      } catch (NumberFormatException e) {
        return false;
      }
    }
    if (v0 instanceof Number) {
      v0 = number(v0);
    }
    if (v1 instanceof Number) {
      v1 = number(v1);
    }
    //noinspection unchecked
    final int c = v0.compareTo(v1);
    return p.test(c);
  }

  private static boolean containsNull(List<Comparable> values) {
    for (Comparable value : values) {
      if (value == N) {
        return true;
      }
    }
    return false;
  }

  /** An enum that wraps boolean and unknown values and makes them
   * comparable. */
  enum Truthy {
    // Order is important; AND returns the min, OR returns the max
    FALSE, UNKNOWN, TRUE;

    static Truthy of(Comparable c) {
      return c.equals(true) ? TRUE : c.equals(false) ? FALSE : UNKNOWN;
    }

    Comparable toComparable() {
      switch (this) {
      case TRUE: return true;
      case FALSE: return false;
      case UNKNOWN: return N;
      default:
        throw new AssertionError();
      }
    }
  }
}
