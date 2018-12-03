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
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableMap;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
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
  public static Comparable evaluate(RexNode e, Map<RexNode, Comparable> map) {
    final Comparable v = e.accept(new RexInterpreter(map));
    if (false) {
      System.out.println("evaluate " + e + " on " + map + " returns " + v);
    }
    return v;
  }

  private IllegalArgumentException unbound(RexNode e) {
    return new IllegalArgumentException("unbound: " + e);
  }

  private Comparable getOrUnbound(RexNode e) {
    final Comparable comparable = environment.get(e);
    if (comparable != null) {
      return comparable;
    }
    throw unbound(e);
  }

  public Comparable visitInputRef(RexInputRef inputRef) {
    return getOrUnbound(inputRef);
  }

  public Comparable visitLocalRef(RexLocalRef localRef) {
    throw unbound(localRef);
  }

  public Comparable visitLiteral(RexLiteral literal) {
    return Util.first(literal.getValue4(), N);
  }

  public Comparable visitOver(RexOver over) {
    throw unbound(over);
  }

  public Comparable visitCorrelVariable(RexCorrelVariable correlVariable) {
    return getOrUnbound(correlVariable);
  }

  public Comparable visitDynamicParam(RexDynamicParam dynamicParam) {
    return getOrUnbound(dynamicParam);
  }

  public Comparable visitRangeRef(RexRangeRef rangeRef) {
    throw unbound(rangeRef);
  }

  public Comparable visitFieldAccess(RexFieldAccess fieldAccess) {
    return getOrUnbound(fieldAccess);
  }

  public Comparable visitSubQuery(RexSubQuery subQuery) {
    throw unbound(subQuery);
  }

  public Comparable visitTableInputRef(RexTableInputRef fieldRef) {
    throw unbound(fieldRef);
  }

  public Comparable visitPatternFieldRef(RexPatternFieldRef fieldRef) {
    throw unbound(fieldRef);
  }

  public Comparable visitCall(RexCall call) {
    final List<Comparable> values = new ArrayList<>(call.operands.size());
    for (RexNode operand : call.operands) {
      values.add(operand.accept(this));
    }
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
      return cast(call, values);
    case COALESCE:
      return coalesce(call, values);
    case CEIL:
    case FLOOR:
      return ceil(call, values);
    case EXTRACT:
      return extract(call, values);
    default:
      throw unbound(call);
    }
  }

  private Comparable extract(RexCall call, List<Comparable> values) {
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

  private Comparable coalesce(RexCall call, List<Comparable> values) {
    for (Comparable value : values) {
      if (value != N) {
        return value;
      }
    }
    return N;
  }

  private Comparable ceil(RexCall call, List<Comparable> values) {
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

  private TimeUnitRange subUnit(TimeUnitRange unit) {
    switch (unit) {
    case QUARTER:
      return TimeUnitRange.MONTH;
    default:
      return TimeUnitRange.DAY;
    }
  }

  private Comparable cast(RexCall call, List<Comparable> values) {
    if (values.get(0) == N) {
      return N;
    }
    return values.get(0);
  }

  private Comparable not(Comparable value) {
    if (value.equals(true)) {
      return false;
    } else if (value.equals(false)) {
      return true;
    } else {
      return N;
    }
  }

  private Comparable case_(List<Comparable> values) {
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

  private BigDecimal number(Comparable comparable) {
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

  private Comparable compare(List<Comparable> values, IntPredicate p) {
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

  private boolean containsNull(List<Comparable> values) {
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

// End RexInterpreter.java
