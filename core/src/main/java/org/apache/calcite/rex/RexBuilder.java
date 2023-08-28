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

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.Spaces;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.runtime.Geometries;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.sql.type.MultisetSqlType;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.IntPredicate;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

/**
 * Factory for row expressions.
 *
 * <p>Some common literal values (NULL, TRUE, FALSE, 0, 1, '') are cached.</p>
 */
public class RexBuilder {
  /**
   * Special operator that accesses an unadvertised field of an input record.
   * This operator cannot be used in SQL queries; it is introduced temporarily
   * during sql-to-rel translation, then replaced during the process that
   * trims unwanted fields.
   */
  public static final SqlSpecialOperator GET_OPERATOR =
      new SqlSpecialOperator("_get", SqlKind.OTHER_FUNCTION);

  /** The smallest valid {@code int} value, as a {@link BigDecimal}. */
  private static final BigDecimal INT_MIN =
      BigDecimal.valueOf(Integer.MIN_VALUE);

  /** The largest valid {@code int} value, as a {@link BigDecimal}. */
  private static final BigDecimal INT_MAX =
      BigDecimal.valueOf(Integer.MAX_VALUE);

  //~ Instance fields --------------------------------------------------------

  protected final RelDataTypeFactory typeFactory;
  private final RexLiteral booleanTrue;
  private final RexLiteral booleanFalse;
  private final RexLiteral charEmpty;
  private final RexLiteral constantNull;
  private final SqlStdOperatorTable opTab = SqlStdOperatorTable.instance();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a RexBuilder.
   *
   * @param typeFactory Type factory
   */
  @SuppressWarnings("method.invocation.invalid")
  public RexBuilder(RelDataTypeFactory typeFactory) {
    this.typeFactory = typeFactory;
    this.booleanTrue =
        makeLiteral(
            Boolean.TRUE,
            typeFactory.createSqlType(SqlTypeName.BOOLEAN),
            SqlTypeName.BOOLEAN);
    this.booleanFalse =
        makeLiteral(
            Boolean.FALSE,
            typeFactory.createSqlType(SqlTypeName.BOOLEAN),
            SqlTypeName.BOOLEAN);
    this.charEmpty =
        makeLiteral(
            new NlsString("", null, null),
            typeFactory.createSqlType(SqlTypeName.CHAR, 0),
            SqlTypeName.CHAR);
    this.constantNull =
        makeLiteral(
            null,
            typeFactory.createSqlType(SqlTypeName.NULL),
            SqlTypeName.NULL);
  }

  /** Creates a list of {@link org.apache.calcite.rex.RexInputRef} expressions,
   * projecting the fields of a given record type. */
  public List<RexNode> identityProjects(final RelDataType rowType) {
    return Util.transform(rowType.getFieldList(),
        input -> new RexInputRef(input.getIndex(), input.getType()));
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns this RexBuilder's type factory.
   *
   * @return type factory
   */
  public RelDataTypeFactory getTypeFactory() {
    return typeFactory;
  }

  /**
   * Returns this RexBuilder's operator table.
   *
   * @return operator table
   */
  public SqlStdOperatorTable getOpTab() {
    return opTab;
  }

  /**
   * Creates an expression accessing a given named field from a record.
   *
   * <p>NOTE: Be careful choosing the value of {@code caseSensitive}.
   * If the field name was supplied by an end-user (e.g. as a column alias in
   * SQL), use your session's case-sensitivity setting.
   * Only hard-code {@code true} if you are sure that the field name is
   * internally generated.
   * Hard-coding {@code false} is almost certainly wrong.</p>
   *
   * @param expr      Expression yielding a record
   * @param fieldName Name of field in record
   * @param caseSensitive Whether match is case-sensitive
   * @return Expression accessing a given named field
   */
  public RexNode makeFieldAccess(RexNode expr, String fieldName,
      boolean caseSensitive) {
    final RelDataType type = expr.getType();
    final RelDataTypeField field =
        type.getField(fieldName, caseSensitive, false);
    if (field == null) {
      throw new AssertionError("Type '" + type + "' has no field '"
          + fieldName + "'");
    }
    return makeFieldAccessInternal(expr, field);
  }

  /**
   * Creates an expression accessing a field with a given ordinal from a
   * record.
   *
   * @param expr Expression yielding a record
   * @param i    Ordinal of field
   * @return Expression accessing given field
   */
  public RexNode makeFieldAccess(
      RexNode expr,
      int i) {
    final RelDataType type = expr.getType();
    final List<RelDataTypeField> fields = type.getFieldList();
    if ((i < 0) || (i >= fields.size())) {
      throw new AssertionError("Field ordinal " + i + " is invalid for "
          + " type '" + type + "'");
    }
    return makeFieldAccessInternal(expr, fields.get(i));
  }

  /**
   * Creates an expression accessing a given field from a record.
   *
   * @param expr  Expression yielding a record
   * @param field Field
   * @return Expression accessing given field
   */
  private RexNode makeFieldAccessInternal(
      RexNode expr,
      final RelDataTypeField field) {
    if (expr instanceof RexRangeRef) {
      RexRangeRef range = (RexRangeRef) expr;
      if (field.getIndex() < 0) {
        return makeCall(
            field.getType(),
            GET_OPERATOR,
            ImmutableList.of(
                expr,
                makeLiteral(field.getName())));
      }
      return new RexInputRef(
          range.getOffset() + field.getIndex(),
          field.getType());
    }
    return new RexFieldAccess(expr, field);
  }

  /**
   * Creates a call with a list of arguments and a predetermined type.
   */
  public RexNode makeCall(
      RelDataType returnType,
      SqlOperator op,
      List<RexNode> exprs) {
    return new RexCall(returnType, op, exprs);
  }

  /**
   * Creates a call with an array of arguments.
   *
   * <p>If you already know the return type of the call, then
   * {@link #makeCall(org.apache.calcite.rel.type.RelDataType, org.apache.calcite.sql.SqlOperator, java.util.List)}
   * is preferred.</p>
   */
  public RexNode makeCall(
      SqlOperator op,
      List<? extends RexNode> exprs) {
    final RelDataType type = deriveReturnType(op, exprs);
    return new RexCall(type, op, exprs);
  }

  /**
   * Creates a call with a list of arguments.
   *
   * <p>Equivalent to
   * <code>makeCall(op, exprList.toArray(new RexNode[exprList.size()]))</code>.
   */
  public final RexNode makeCall(
      SqlOperator op,
      RexNode... exprs) {
    return makeCall(op, ImmutableList.copyOf(exprs));
  }

  /**
   * Derives the return type of a call to an operator.
   *
   * @param op          the operator being called
   * @param exprs       actual operands
   * @return derived type
   */
  public RelDataType deriveReturnType(
      SqlOperator op,
      List<? extends RexNode> exprs) {
    return op.inferReturnType(
        new RexCallBinding(typeFactory, op, exprs,
            ImmutableList.of()));
  }

  /**
   * Creates a reference to an aggregate call, checking for repeated calls.
   *
   * <p>Argument types help to optimize for repeated aggregates.
   * For instance count(42) is equivalent to count(*).</p>
   *
   * @param aggCall aggregate call to be added
   * @param groupCount number of groups in the aggregate relation
   * @param aggCalls destination list of aggregate calls
   * @param aggCallMapping the dictionary of already added calls
   * @param isNullable Whether input field i is nullable
   *
   * @return Rex expression for the given aggregate call
   */
  public RexNode addAggCall(AggregateCall aggCall, int groupCount,
      List<AggregateCall> aggCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      IntPredicate isNullable) {
    if (aggCall.getAggregation() instanceof SqlCountAggFunction
        && !aggCall.isDistinct()) {
      final List<Integer> args = aggCall.getArgList();
      final List<Integer> nullableArgs = nullableArgs(args, isNullable);
      aggCall = aggCall.withArgList(nullableArgs);
    }
    RexNode rex = aggCallMapping.get(aggCall);
    if (rex == null) {
      int index = aggCalls.size() + groupCount;
      aggCalls.add(aggCall);
      rex = makeInputRef(aggCall.getType(), index);
      aggCallMapping.put(aggCall, rex);
    }
    return rex;
  }

  @Deprecated // to be removed before 2.0
  public RexNode addAggCall(final AggregateCall aggCall, int groupCount,
      List<AggregateCall> aggCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      final @Nullable List<RelDataType> aggArgTypes) {
    return addAggCall(aggCall, groupCount, aggCalls, aggCallMapping, i ->
        Objects.requireNonNull(aggArgTypes, "aggArgTypes")
            .get(aggCall.getArgList().indexOf(i)).isNullable());
  }

  /**
   * Creates a reference to an aggregate call, checking for repeated calls.
   */
  @Deprecated // to be removed before 2.0
  public RexNode addAggCall(AggregateCall aggCall, int groupCount,
      boolean indicator, List<AggregateCall> aggCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      final @Nullable List<RelDataType> aggArgTypes) {
    Preconditions.checkArgument(!indicator,
        "indicator is deprecated, use GROUPING function instead");
    return addAggCall(aggCall, groupCount, aggCalls,
        aggCallMapping, aggArgTypes);

  }

  private static List<Integer> nullableArgs(List<Integer> list0,
      IntPredicate isNullable) {
    return list0.stream()
        .filter(isNullable::test)
        .collect(Util.toImmutableList());
  }

  @Deprecated // to be removed before 2.0
  public RexNode makeOver(RelDataType type, SqlAggFunction operator,
      List<RexNode> exprs, List<RexNode> partitionKeys,
      ImmutableList<RexFieldCollation> orderKeys,
      RexWindowBound lowerBound, RexWindowBound upperBound,
      boolean rows, boolean allowPartial, boolean nullWhenCountZero,
      boolean distinct) {
    return makeOver(type, operator, exprs, partitionKeys, orderKeys, lowerBound,
        upperBound, rows, allowPartial, nullWhenCountZero, distinct, false);
  }

  /**
   * Creates a call to a windowed agg.
   */
  public RexNode makeOver(
      RelDataType type,
      SqlAggFunction operator,
      List<RexNode> exprs,
      List<RexNode> partitionKeys,
      ImmutableList<RexFieldCollation> orderKeys,
      RexWindowBound lowerBound,
      RexWindowBound upperBound,
      boolean rows,
      boolean allowPartial,
      boolean nullWhenCountZero,
      boolean distinct,
      boolean ignoreNulls) {
    final RexWindow window =
        makeWindow(
            partitionKeys,
            orderKeys,
            lowerBound,
            upperBound,
            rows);
    final RexOver over = new RexOver(type, operator, exprs, window,
        distinct, ignoreNulls);
    RexNode result = over;

    // This should be correct but need time to go over test results.
    // Also want to look at combing with section below.
    if (nullWhenCountZero) {
      final RelDataType bigintType =
          typeFactory.createSqlType(SqlTypeName.BIGINT);
      result = makeCall(
          SqlStdOperatorTable.CASE,
          makeCall(
              SqlStdOperatorTable.GREATER_THAN,
              new RexOver(
                  bigintType,
                  SqlStdOperatorTable.COUNT,
                  exprs,
                  window,
                  distinct,
                  ignoreNulls),
              makeLiteral(
                  BigDecimal.ZERO,
                  bigintType,
                  SqlTypeName.DECIMAL)),
          ensureType(type, // SUM0 is non-nullable, thus need a cast
              new RexOver(typeFactory.createTypeWithNullability(type, false),
                  operator, exprs, window, distinct, ignoreNulls),
              false),
          makeNullLiteral(type));
    }
    if (!allowPartial) {
      Preconditions.checkArgument(rows, "DISALLOW PARTIAL over RANGE");
      final RelDataType bigintType =
          typeFactory.createSqlType(SqlTypeName.BIGINT);
      // todo: read bound
      result =
          makeCall(
              SqlStdOperatorTable.CASE,
              makeCall(
                  SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                  new RexOver(
                      bigintType,
                      SqlStdOperatorTable.COUNT,
                      ImmutableList.of(),
                      window,
                      distinct,
                      ignoreNulls),
                  makeLiteral(
                      BigDecimal.valueOf(2),
                      bigintType,
                      SqlTypeName.DECIMAL)),
              result,
              constantNull);
    }
    return result;
  }

  /**
   * Creates a window specification.
   *
   * @param partitionKeys Partition keys
   * @param orderKeys     Order keys
   * @param lowerBound    Lower bound
   * @param upperBound    Upper bound
   * @param rows          Whether physical. True if row-based, false if
   *                      range-based
   * @return window specification
   */
  public RexWindow makeWindow(
      List<RexNode> partitionKeys,
      ImmutableList<RexFieldCollation> orderKeys,
      RexWindowBound lowerBound,
      RexWindowBound upperBound,
      boolean rows) {
    if (lowerBound.isUnbounded() && lowerBound.isPreceding()
        && upperBound.isUnbounded() && upperBound.isFollowing()) {
      // RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      //   is equivalent to
      // ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      //   but we prefer "RANGE"
      rows = false;
    }
    return new RexWindow(
        partitionKeys,
        orderKeys,
        lowerBound,
        upperBound,
        rows);
  }

  /**
   * Creates a constant for the SQL <code>NULL</code> value.
   *
   * @deprecated Use {@link #makeNullLiteral(RelDataType)}, which produces a
   * NULL of the correct type
   */
  @Deprecated // to be removed before 2.0
  public RexLiteral constantNull() {
    return constantNull;
  }

  /**
   * Creates an expression referencing a correlation variable.
   *
   * @param id Name of variable
   * @param type Type of variable
   * @return Correlation variable
   */
  public RexNode makeCorrel(
      RelDataType type,
      CorrelationId id) {
    return new RexCorrelVariable(id, type);
  }

  /**
   * Creates an invocation of the NEW operator.
   *
   * @param type  Type to be instantiated
   * @param exprs Arguments to NEW operator
   * @return Expression invoking NEW operator
   */
  public RexNode makeNewInvocation(
      RelDataType type,
      List<RexNode> exprs) {
    return new RexCall(
        type,
        SqlStdOperatorTable.NEW,
        exprs);
  }

  /**
   * Creates a call to the CAST operator.
   *
   * @param type Type to cast to
   * @param exp  Expression being cast
   * @return Call to CAST operator
   */
  public RexNode makeCast(
      RelDataType type,
      RexNode exp) {
    return makeCast(type, exp, false);
  }

  /**
   * Creates a call to the CAST operator, expanding if possible, and optionally
   * also preserving nullability.
   *
   * <p>Tries to expand the cast, and therefore the result may be something
   * other than a {@link RexCall} to the CAST operator, such as a
   * {@link RexLiteral}.
   *
   * @param type Type to cast to
   * @param exp  Expression being cast
   * @param matchNullability Whether to ensure the result has the same
   * nullability as {@code type}
   * @return Call to CAST operator
   */
  public RexNode makeCast(
      RelDataType type,
      RexNode exp,
      boolean matchNullability) {
    final SqlTypeName sqlType = type.getSqlTypeName();
    if (exp instanceof RexLiteral) {
      RexLiteral literal = (RexLiteral) exp;
      Comparable value = literal.getValueAs(Comparable.class);
      SqlTypeName typeName = literal.getTypeName();
      if (canRemoveCastFromLiteral(type, value, typeName)) {
        switch (typeName) {
        case INTERVAL_YEAR:
        case INTERVAL_YEAR_MONTH:
        case INTERVAL_MONTH:
        case INTERVAL_DAY:
        case INTERVAL_DAY_HOUR:
        case INTERVAL_DAY_MINUTE:
        case INTERVAL_DAY_SECOND:
        case INTERVAL_HOUR:
        case INTERVAL_HOUR_MINUTE:
        case INTERVAL_HOUR_SECOND:
        case INTERVAL_MINUTE:
        case INTERVAL_MINUTE_SECOND:
        case INTERVAL_SECOND:
          assert value instanceof BigDecimal;
          typeName = type.getSqlTypeName();
          switch (typeName) {
          case BIGINT:
          case INTEGER:
          case SMALLINT:
          case TINYINT:
          case FLOAT:
          case REAL:
          case DECIMAL:
            BigDecimal value2 = (BigDecimal) value;
            final BigDecimal multiplier =
                baseUnit(literal.getTypeName()).multiplier;
            final BigDecimal divider =
                literal.getTypeName().getEndUnit().multiplier;
            value = value2.multiply(multiplier)
                .divide(divider, 0, RoundingMode.HALF_DOWN);
            break;
          default:
            break;
          }

          // Not all types are allowed for literals
          switch (typeName) {
          case INTEGER:
            typeName = SqlTypeName.BIGINT;
            break;
          default:
            break;
          }
          break;
        default:
          break;
        }
        final RexLiteral literal2 =
            makeLiteral(value, type, typeName);
        if (type.isNullable()
            && !literal2.getType().isNullable()
            && matchNullability) {
          return makeAbstractCast(type, literal2);
        }
        return literal2;
      }
    } else if (SqlTypeUtil.isExactNumeric(type)
        && SqlTypeUtil.isInterval(exp.getType())) {
      return makeCastIntervalToExact(type, exp);
    } else if (sqlType == SqlTypeName.BOOLEAN
        && SqlTypeUtil.isExactNumeric(exp.getType())) {
      return makeCastExactToBoolean(type, exp);
    } else if (exp.getType().getSqlTypeName() == SqlTypeName.BOOLEAN
        && SqlTypeUtil.isExactNumeric(type)) {
      return makeCastBooleanToExact(type, exp);
    }
    return makeAbstractCast(type, exp);
  }

  /** Returns the lowest granularity unit for the given unit.
   * YEAR and MONTH intervals are stored as months;
   * HOUR, MINUTE, SECOND intervals are stored as milliseconds. */
  protected static TimeUnit baseUnit(SqlTypeName unit) {
    if (unit.isYearMonth()) {
      return TimeUnit.MONTH;
    } else {
      return TimeUnit.MILLISECOND;
    }
  }

  boolean canRemoveCastFromLiteral(RelDataType toType, @Nullable Comparable value,
      SqlTypeName fromTypeName) {
    final SqlTypeName sqlType = toType.getSqlTypeName();
    if (!RexLiteral.valueMatchesType(value, sqlType, false)) {
      return false;
    }
    if (toType.getSqlTypeName() != fromTypeName
        && SqlTypeFamily.DATETIME.getTypeNames().contains(fromTypeName)) {
      return false;
    }
    if (value instanceof NlsString) {
      final int length = ((NlsString) value).getValue().length();
      switch (toType.getSqlTypeName()) {
      case CHAR:
        return SqlTypeUtil.comparePrecision(toType.getPrecision(), length) == 0;
      case VARCHAR:
        return SqlTypeUtil.comparePrecision(toType.getPrecision(), length) >= 0;
      default:
        throw new AssertionError(toType);
      }
    }
    if (value instanceof ByteString) {
      final int length = ((ByteString) value).length();
      switch (toType.getSqlTypeName()) {
      case BINARY:
        return SqlTypeUtil.comparePrecision(toType.getPrecision(), length) == 0;
      case VARBINARY:
        return SqlTypeUtil.comparePrecision(toType.getPrecision(), length) >= 0;
      default:
        throw new AssertionError(toType);
      }
    }

    if (toType.getSqlTypeName() == SqlTypeName.DECIMAL) {
      final BigDecimal decimalValue = (BigDecimal) value;
      return SqlTypeUtil.isValidDecimalValue(decimalValue, toType);
    }

    return true;
  }

  private RexNode makeCastExactToBoolean(RelDataType toType, RexNode exp) {
    return makeCall(toType,
        SqlStdOperatorTable.NOT_EQUALS,
        ImmutableList.of(exp, makeZeroLiteral(exp.getType())));
  }

  private RexNode makeCastBooleanToExact(RelDataType toType, RexNode exp) {
    final RexNode casted = makeCall(SqlStdOperatorTable.CASE,
        exp,
        makeExactLiteral(BigDecimal.ONE, toType),
        makeZeroLiteral(toType));
    if (!exp.getType().isNullable()) {
      return casted;
    }
    return makeCall(toType,
        SqlStdOperatorTable.CASE,
        ImmutableList.of(makeCall(SqlStdOperatorTable.IS_NOT_NULL, exp),
            casted, makeNullLiteral(toType)));
  }

  private RexNode makeCastIntervalToExact(RelDataType toType, RexNode exp) {
    final TimeUnit endUnit = exp.getType().getSqlTypeName().getEndUnit();
    final TimeUnit baseUnit = baseUnit(exp.getType().getSqlTypeName());
    final BigDecimal multiplier = baseUnit.multiplier;
    final BigDecimal divider = endUnit.multiplier;
    RexNode value = multiplyDivide(decodeIntervalOrDecimal(exp),
        multiplier, divider);
    return ensureType(toType, value, false);
  }

  public RexNode multiplyDivide(RexNode e, BigDecimal multiplier,
      BigDecimal divider) {
    assert multiplier.signum() > 0;
    assert divider.signum() > 0;
    switch (multiplier.compareTo(divider)) {
    case 0:
      return e;
    case 1:
      // E.g. multiplyDivide(e, 1000, 10) ==> e * 100
      return makeCall(SqlStdOperatorTable.MULTIPLY, e,
          makeExactLiteral(
              multiplier.divide(divider, RoundingMode.UNNECESSARY)));
    case -1:
      // E.g. multiplyDivide(e, 10, 1000) ==> e / 100
      return makeCall(SqlStdOperatorTable.DIVIDE_INTEGER, e,
          makeExactLiteral(
              divider.divide(multiplier, RoundingMode.UNNECESSARY)));
    default:
      throw new AssertionError(multiplier + "/" + divider);
    }
  }

  /**
   * Casts a decimal's integer representation to a decimal node. If the
   * expression is not the expected integer type, then it is casted first.
   *
   * <p>An overflow check may be requested to ensure the internal value
   * does not exceed the maximum value of the decimal type.
   *
   * @param value         integer representation of decimal
   * @param type          type integer will be reinterpreted as
   * @param checkOverflow indicates whether an overflow check is required
   *                      when reinterpreting this particular value as the
   *                      decimal type. A check usually not required for
   *                      arithmetic, but is often required for rounding and
   *                      explicit casts.
   * @return the integer reinterpreted as an opaque decimal type
   */
  public RexNode encodeIntervalOrDecimal(
      RexNode value,
      RelDataType type,
      boolean checkOverflow) {
    RelDataType bigintType =
        typeFactory.createSqlType(SqlTypeName.BIGINT);
    RexNode cast = ensureType(bigintType, value, true);
    return makeReinterpretCast(type, cast, makeLiteral(checkOverflow));
  }

  /**
   * Retrieves an INTERVAL or DECIMAL node's integer representation.
   *
   * @param node the interval or decimal value as an opaque type
   * @return an integer representation of the decimal value
   */
  public RexNode decodeIntervalOrDecimal(RexNode node) {
    assert SqlTypeUtil.isDecimal(node.getType())
        || SqlTypeUtil.isInterval(node.getType());
    RelDataType bigintType = typeFactory.createSqlType(SqlTypeName.BIGINT);
    return makeReinterpretCast(
        matchNullability(bigintType, node), node, makeLiteral(false));
  }

  /**
   * Creates a call to the CAST operator.
   *
   * @param type Type to cast to
   * @param exp  Expression being cast
   * @return Call to CAST operator
   */
  public RexNode makeAbstractCast(
      RelDataType type,
      RexNode exp) {
    return new RexCall(
        type,
        SqlStdOperatorTable.CAST,
        ImmutableList.of(exp));
  }

  /**
   * Makes a reinterpret cast.
   *
   * @param type          type returned by the cast
   * @param exp           expression to be casted
   * @param checkOverflow whether an overflow check is required
   * @return a RexCall with two operands and a special return type
   */
  public RexNode makeReinterpretCast(
      RelDataType type,
      RexNode exp,
      RexNode checkOverflow) {
    List<RexNode> args;
    if ((checkOverflow != null) && checkOverflow.isAlwaysTrue()) {
      args = ImmutableList.of(exp, checkOverflow);
    } else {
      args = ImmutableList.of(exp);
    }
    return new RexCall(
        type,
        SqlStdOperatorTable.REINTERPRET,
        args);
  }

  /**
   * Makes a cast of a value to NOT NULL;
   * no-op if the type already has NOT NULL.
   */
  public RexNode makeNotNull(RexNode exp) {
    final RelDataType type = exp.getType();
    if (!type.isNullable()) {
      return exp;
    }
    final RelDataType notNullType =
        typeFactory.createTypeWithNullability(type, false);
    return makeAbstractCast(notNullType, exp);
  }

  /**
   * Creates a reference to all the fields in the row. That is, the whole row
   * as a single record object.
   *
   * @param input Input relational expression
   */
  public RexNode makeRangeReference(RelNode input) {
    return new RexRangeRef(input.getRowType(), 0);
  }

  /**
   * Creates a reference to all the fields in the row.
   *
   * <p>For example, if the input row has type <code>T{f0,f1,f2,f3,f4}</code>
   * then <code>makeRangeReference(T{f0,f1,f2,f3,f4}, S{f3,f4}, 3)</code> is
   * an expression which yields the last 2 fields.
   *
   * @param type     Type of the resulting range record.
   * @param offset   Index of first field.
   * @param nullable Whether the record is nullable.
   */
  public RexRangeRef makeRangeReference(
      RelDataType type,
      int offset,
      boolean nullable) {
    if (nullable && !type.isNullable()) {
      type =
          typeFactory.createTypeWithNullability(
              type,
              nullable);
    }
    return new RexRangeRef(type, offset);
  }

  /**
   * Creates a reference to a given field of the input record.
   *
   * @param type Type of field
   * @param i    Ordinal of field
   * @return Reference to field
   */
  public RexInputRef makeInputRef(
      RelDataType type,
      int i) {
    type = SqlTypeUtil.addCharsetAndCollation(type, typeFactory);
    return new RexInputRef(i, type);
  }

  /**
   * Creates a reference to a given field of the input relational expression.
   *
   * @param input Input relational expression
   * @param i    Ordinal of field
   * @return Reference to field
   *
   * @see #identityProjects(RelDataType)
   */
  public RexInputRef makeInputRef(RelNode input, int i) {
    return makeInputRef(input.getRowType().getFieldList().get(i).getType(), i);
  }

  /**
   * Creates a reference to a given field of the pattern.
   *
   * @param alpha the pattern name
   * @param type Type of field
   * @param i    Ordinal of field
   * @return Reference to field of pattern
   */
  public RexPatternFieldRef makePatternFieldRef(String alpha, RelDataType type, int i) {
    type = SqlTypeUtil.addCharsetAndCollation(type, typeFactory);
    return new RexPatternFieldRef(alpha, i, type);
  }

  /**
   * Create a reference to local variable.
   *
   * @param type Type of variable
   * @param i    Ordinal of variable
   * @return  Reference to local variable
   */
  public RexLocalRef makeLocalRef(RelDataType type, int i) {
    type = SqlTypeUtil.addCharsetAndCollation(type, typeFactory);
    return new RexLocalRef(i, type);
  }

  /**
   * Creates a literal representing a flag.
   *
   * @param flag Flag value
   */
  public RexLiteral makeFlag(Enum flag) {
    assert flag != null;
    return makeLiteral(flag,
        typeFactory.createSqlType(SqlTypeName.SYMBOL),
        SqlTypeName.SYMBOL);
  }

  /**
   * Internal method to create a call to a literal. Code outside this package
   * should call one of the type-specific methods such as
   * {@link #makeDateLiteral(DateString)}, {@link #makeLiteral(boolean)},
   * {@link #makeLiteral(String)}.
   *
   * @param o        Value of literal, must be appropriate for the type
   * @param type     Type of literal
   * @param typeName SQL type of literal
   * @return Literal
   */
  protected RexLiteral makeLiteral(
      @Nullable Comparable o,
      RelDataType type,
      SqlTypeName typeName) {
    // All literals except NULL have NOT NULL types.
    type = typeFactory.createTypeWithNullability(type, o == null);
    int p;
    switch (typeName) {
    case CHAR:
      // Character literals must have a charset and collation. Populate
      // from the type if necessary.
      assert o instanceof NlsString;
      NlsString nlsString = (NlsString) o;
      if (nlsString.getCollation() == null
          || nlsString.getCharset() == null
          || !Objects.equals(nlsString.getCharset(), type.getCharset())
          || !Objects.equals(nlsString.getCollation(), type.getCollation())) {
        assert type.getSqlTypeName() == SqlTypeName.CHAR
            || type.getSqlTypeName() == SqlTypeName.VARCHAR;
        Charset charset = type.getCharset();
        assert charset != null : "type.getCharset() must not be null";
        assert type.getCollation() != null : "type.getCollation() must not be null";
        o = new NlsString(
            nlsString.getValue(),
            charset.name(),
            type.getCollation());
      }
      break;
    case TIME:
    case TIME_WITH_LOCAL_TIME_ZONE:
      assert o instanceof TimeString;
      p = type.getPrecision();
      if (p == RelDataType.PRECISION_NOT_SPECIFIED) {
        p = 0;
      }
      o = ((TimeString) o).round(p);
      break;
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      assert o instanceof TimestampString;
      p = type.getPrecision();
      if (p == RelDataType.PRECISION_NOT_SPECIFIED) {
        p = 0;
      }
      o = ((TimestampString) o).round(p);
      break;
    default:
      break;
    }
    if (typeName == SqlTypeName.DECIMAL
        && !SqlTypeUtil.isValidDecimalValue((BigDecimal) o, type)) {
      throw new IllegalArgumentException(
          "Cannot convert " + o + " to " + type  + " due to overflow");
    }
    return new RexLiteral(o, type, typeName);
  }

  /**
   * Creates a boolean literal.
   */
  public RexLiteral makeLiteral(boolean b) {
    return b ? booleanTrue : booleanFalse;
  }

  /**
   * Creates a numeric literal.
   */
  public RexLiteral makeExactLiteral(BigDecimal bd) {
    RelDataType relType;
    int scale = bd.scale();
    assert scale >= 0;
    assert scale <= typeFactory.getTypeSystem().getMaxNumericScale() : scale;
    if (scale == 0) {
      if (bd.compareTo(INT_MIN) >= 0 && bd.compareTo(INT_MAX) <= 0) {
        relType = typeFactory.createSqlType(SqlTypeName.INTEGER);
      } else {
        relType = typeFactory.createSqlType(SqlTypeName.BIGINT);
      }
    } else {
      int precision = bd.unscaledValue().abs().toString().length();
      if (precision > scale) {
        // bd is greater than or equal to 1
        relType =
            typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
      } else {
        // bd is less than 1
        relType =
            typeFactory.createSqlType(SqlTypeName.DECIMAL, scale + 1, scale);
      }
    }
    return makeExactLiteral(bd, relType);
  }

  /**
   * Creates a BIGINT literal.
   */
  public RexLiteral makeBigintLiteral(@Nullable BigDecimal bd) {
    RelDataType bigintType =
        typeFactory.createSqlType(
            SqlTypeName.BIGINT);
    return makeLiteral(bd, bigintType, SqlTypeName.DECIMAL);
  }

  /**
   * Creates a numeric literal.
   */
  public RexLiteral makeExactLiteral(@Nullable BigDecimal bd, RelDataType type) {
    return makeLiteral(bd, type, SqlTypeName.DECIMAL);
  }

  /**
   * Creates a byte array literal.
   */
  public RexLiteral makeBinaryLiteral(ByteString byteString) {
    return makeLiteral(
        byteString,
        typeFactory.createSqlType(SqlTypeName.BINARY, byteString.length()),
        SqlTypeName.BINARY);
  }

  /**
   * Creates a double-precision literal.
   */
  public RexLiteral makeApproxLiteral(BigDecimal bd) {
    // Validator should catch if underflow is allowed
    // If underflow is allowed, let underflow become zero
    if (bd.doubleValue() == 0) {
      bd = BigDecimal.ZERO;
    }
    return makeApproxLiteral(bd, typeFactory.createSqlType(SqlTypeName.DOUBLE));
  }

  /**
   * Creates an approximate numeric literal (double or float).
   *
   * @param bd   literal value
   * @param type approximate numeric type
   * @return new literal
   */
  public RexLiteral makeApproxLiteral(@Nullable BigDecimal bd, RelDataType type) {
    assert SqlTypeFamily.APPROXIMATE_NUMERIC.getTypeNames().contains(
        type.getSqlTypeName());
    return makeLiteral(bd, type, SqlTypeName.DOUBLE);
  }

  /**
   * Creates a search argument literal.
   */
  public RexLiteral makeSearchArgumentLiteral(Sarg s, RelDataType type) {
    return makeLiteral(Objects.requireNonNull(s, "s"), type, SqlTypeName.SARG);
  }

  /**
   * Creates a character string literal.
   */
  public RexLiteral makeLiteral(String s) {
    assert s != null;
    return makePreciseStringLiteral(s);
  }

  /**
   * Creates a character string literal with type CHAR and default charset and
   * collation.
   *
   * @param s String value
   * @return Character string literal
   */
  protected RexLiteral makePreciseStringLiteral(String s) {
    assert s != null;
    if (s.equals("")) {
      return charEmpty;
    }
    return makeCharLiteral(new NlsString(s, null, null));
  }

  /**
   * Creates a character string literal with type CHAR.
   *
   * @param value       String value in bytes
   * @param charsetName SQL-level charset name
   * @param collation   Sql collation
   * @return String     literal
   */
  protected RexLiteral makePreciseStringLiteral(ByteString value,
      String charsetName, SqlCollation collation) {
    return makeCharLiteral(new NlsString(value, charsetName, collation));
  }

  /**
   * Ensures expression is interpreted as a specified type. The returned
   * expression may be wrapped with a cast.
   *
   * @param type             desired type
   * @param node             expression
   * @param matchNullability whether to correct nullability of specified
   *                         type to match the expression; this usually should
   *                         be true, except for explicit casts which can
   *                         override default nullability
   * @return a casted expression or the original expression
   */
  public RexNode ensureType(
      RelDataType type,
      RexNode node,
      boolean matchNullability) {
    RelDataType targetType = type;
    if (matchNullability) {
      targetType = matchNullability(type, node);
    }

    if (targetType.getSqlTypeName() == SqlTypeName.ANY
        && (!matchNullability
            || targetType.isNullable() == node.getType().isNullable())) {
      return node;
    }

    if (!node.getType().equals(targetType)) {
      return makeCast(targetType, node);
    }
    return node;
  }

  /**
   * Ensures that a type's nullability matches a value's nullability.
   */
  public RelDataType matchNullability(
      RelDataType type,
      RexNode value) {
    boolean typeNullability = type.isNullable();
    boolean valueNullability = value.getType().isNullable();
    if (typeNullability != valueNullability) {
      return typeFactory.createTypeWithNullability(type, valueNullability);
    }
    return type;
  }

  /**
   * Creates a character string literal from an {@link NlsString}.
   *
   * <p>If the string's charset and collation are not set, uses the system
   * defaults.
   */
  public RexLiteral makeCharLiteral(NlsString str) {
    assert str != null;
    RelDataType type = SqlUtil.createNlsStringType(typeFactory, str);
    return makeLiteral(str, type, SqlTypeName.CHAR);
  }

  // CHECKSTYLE: IGNORE 1
  /** @deprecated Use {@link #makeDateLiteral(DateString)}. */
  @Deprecated // to be removed before 2.0
  public RexLiteral makeDateLiteral(Calendar calendar) {
    return makeDateLiteral(DateString.fromCalendarFields(calendar));
  }

  /**
   * Creates a Date literal.
   */
  public RexLiteral makeDateLiteral(DateString date) {
    return makeLiteral(Objects.requireNonNull(date, "date"),
        typeFactory.createSqlType(SqlTypeName.DATE), SqlTypeName.DATE);
  }

  // CHECKSTYLE: IGNORE 1
  /** @deprecated Use {@link #makeTimeLiteral(TimeString, int)}. */
  @Deprecated // to be removed before 2.0
  public RexLiteral makeTimeLiteral(Calendar calendar, int precision) {
    return makeTimeLiteral(TimeString.fromCalendarFields(calendar), precision);
  }

  /**
   * Creates a Time literal.
   */
  public RexLiteral makeTimeLiteral(TimeString time, int precision) {
    return makeLiteral(Objects.requireNonNull(time, "time"),
        typeFactory.createSqlType(SqlTypeName.TIME, precision),
        SqlTypeName.TIME);
  }

  /**
   * Creates a Time with local time-zone literal.
   */
  public RexLiteral makeTimeWithLocalTimeZoneLiteral(
      TimeString time,
      int precision) {
    return makeLiteral(Objects.requireNonNull(time, "time"),
        typeFactory.createSqlType(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE, precision),
        SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE);
  }

  // CHECKSTYLE: IGNORE 1
  /** @deprecated Use {@link #makeTimestampLiteral(TimestampString, int)}. */
  @Deprecated // to be removed before 2.0
  public RexLiteral makeTimestampLiteral(Calendar calendar, int precision) {
    return makeTimestampLiteral(TimestampString.fromCalendarFields(calendar),
        precision);
  }

  /**
   * Creates a Timestamp literal.
   */
  public RexLiteral makeTimestampLiteral(TimestampString timestamp,
      int precision) {
    return makeLiteral(Objects.requireNonNull(timestamp, "timestamp"),
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP, precision),
        SqlTypeName.TIMESTAMP);
  }

  /**
   * Creates a Timestamp with local time-zone literal.
   */
  public RexLiteral makeTimestampWithLocalTimeZoneLiteral(
      TimestampString timestamp,
      int precision) {
    return makeLiteral(Objects.requireNonNull(timestamp, "timestamp"),
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, precision),
        SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
  }

  /**
   * Creates a literal representing an interval type, for example
   * {@code YEAR TO MONTH} or {@code DOW}.
   */
  public RexLiteral makeIntervalLiteral(
      SqlIntervalQualifier intervalQualifier) {
    assert intervalQualifier != null;
    return makeFlag(intervalQualifier.timeUnitRange);
  }

  /**
   * Creates a literal representing an interval value, for example
   * {@code INTERVAL '3-7' YEAR TO MONTH}.
   */
  public RexLiteral makeIntervalLiteral(
      @Nullable BigDecimal v,
      SqlIntervalQualifier intervalQualifier) {
    return makeLiteral(
        v,
        typeFactory.createSqlIntervalType(intervalQualifier),
        intervalQualifier.typeName());
  }

  /**
   * Creates a reference to a dynamic parameter.
   *
   * @param type  Type of dynamic parameter
   * @param index Index of dynamic parameter
   * @return Expression referencing dynamic parameter
   */
  public RexDynamicParam makeDynamicParam(
      RelDataType type,
      int index) {
    return new RexDynamicParam(type, index);
  }

  /**
   * Creates a literal whose value is NULL, with a particular type.
   *
   * <p>The typing is necessary because RexNodes are strictly typed. For
   * example, in the Rex world the <code>NULL</code> parameter to <code>
   * SUBSTRING(NULL FROM 2 FOR 4)</code> must have a valid VARCHAR type so
   * that the result type can be determined.
   *
   * @param type Type to cast NULL to
   * @return NULL literal of given type
   */
  public RexLiteral makeNullLiteral(RelDataType type) {
    if (!type.isNullable()) {
      type = typeFactory.createTypeWithNullability(type, true);
    }
    return (RexLiteral) makeCast(type, constantNull);
  }

  // CHECKSTYLE: IGNORE 1
  /** @deprecated Use {@link #makeNullLiteral(RelDataType)} */
  @Deprecated // to be removed before 2.0
  public RexNode makeNullLiteral(SqlTypeName typeName, int precision) {
    return makeNullLiteral(typeFactory.createSqlType(typeName, precision));
  }

  // CHECKSTYLE: IGNORE 1
  /** @deprecated Use {@link #makeNullLiteral(RelDataType)} */
  @Deprecated // to be removed before 2.0
  public RexNode makeNullLiteral(SqlTypeName typeName) {
    return makeNullLiteral(typeFactory.createSqlType(typeName));
  }

  /** Creates a {@link RexNode} representation a SQL "arg IN (point, ...)"
   * expression.
   *
   * <p>If all of the expressions are literals, creates a call {@link Sarg}
   * literal, "SEARCH(arg, SARG([point0..point0], [point1..point1], ...)";
   * otherwise creates a disjunction, "arg = point0 OR arg = point1 OR ...". */
  public RexNode makeIn(RexNode arg, List<? extends RexNode> ranges) {
    if (areAssignable(arg, ranges)) {
      final Sarg sarg = toSarg(Comparable.class, ranges, RexUnknownAs.UNKNOWN);
      if (sarg != null) {
        final RexNode range0 = ranges.get(0);
        return makeCall(SqlStdOperatorTable.SEARCH,
            arg,
            makeSearchArgumentLiteral(sarg, range0.getType()));
      }
    }
    return RexUtil.composeDisjunction(this, ranges.stream()
        .map(r -> makeCall(SqlStdOperatorTable.EQUALS, arg, r))
        .collect(Util.toImmutableList()));
  }

  /** Returns whether and argument and bounds are have types that are
   * sufficiently compatible to be converted to a {@link Sarg}. */
  private static boolean areAssignable(RexNode arg, List<? extends RexNode> bounds) {
    for (RexNode bound : bounds) {
      if (!SqlTypeUtil.inSameFamily(arg.getType(), bound.getType())
          && !(arg.getType().isStruct() && bound.getType().isStruct())) {
        return false;
      }
    }
    return true;
  }

  /** Creates a {@link RexNode} representation a SQL
   * "arg BETWEEN lower AND upper" expression.
   *
   * <p>If the expressions are all literals of compatible type, creates a call
   * to {@link Sarg} literal, {@code SEARCH(arg, SARG([lower..upper])};
   * otherwise creates a disjunction, {@code arg >= lower AND arg <= upper}. */
  @SuppressWarnings("BetaApi")
  public RexNode makeBetween(RexNode arg, RexNode lower, RexNode upper) {
    final Comparable lowerValue = toComparable(Comparable.class, lower);
    final Comparable upperValue = toComparable(Comparable.class, upper);
    if (lowerValue != null
        && upperValue != null
        && areAssignable(arg, Arrays.asList(lower, upper))) {
      final Sarg sarg =
          Sarg.of(RexUnknownAs.UNKNOWN,
              ImmutableRangeSet.<Comparable>of(
                  Range.closed(lowerValue, upperValue)));
      return makeCall(SqlStdOperatorTable.SEARCH, arg,
          makeSearchArgumentLiteral(sarg, lower.getType()));
    }
    return makeCall(SqlStdOperatorTable.AND,
        makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, arg, lower),
        makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, arg, upper));
  }

  /** Converts a list of expressions to a search argument, or returns null if
   * not possible. */
  @SuppressWarnings({"BetaApi", "UnstableApiUsage"})
  private static <C extends Comparable<C>> @Nullable Sarg<C> toSarg(Class<C> clazz,
      List<? extends RexNode> ranges, RexUnknownAs unknownAs) {
    if (ranges.isEmpty()) {
      // Cannot convert an empty list to a Sarg (by this interface, at least)
      // because we use the type of the first element.
      return null;
    }
    final RangeSet<C> rangeSet = TreeRangeSet.create();
    for (RexNode range : ranges) {
      final C value = toComparable(clazz, range);
      if (value == null) {
        return null;
      }
      rangeSet.add(Range.singleton(value));
    }
    return Sarg.of(unknownAs, rangeSet);
  }

  private static <C extends Comparable<C>> @Nullable C toComparable(Class<C> clazz,
      RexNode point) {
    switch (point.getKind()) {
    case LITERAL:
      final RexLiteral literal = (RexLiteral) point;
      return literal.getValueAs(clazz);

    case ROW:
      final RexCall call = (RexCall) point;
      final ImmutableList.Builder<Comparable> b = ImmutableList.builder();
      for (RexNode operand : call.operands) {
        //noinspection unchecked
        final Comparable value = toComparable(Comparable.class, operand);
        if (value == null) {
          return null; // not a constant value
        }
        b.add(value);
      }
      return clazz.cast(FlatLists.ofComparable(b.build()));

    default:
      return null; // not a constant value
    }
  }

  /**
   * Creates a copy of an expression, which may have been created using a
   * different RexBuilder and/or {@link RelDataTypeFactory}, using this
   * RexBuilder.
   *
   * @param expr Expression
   * @return Copy of expression
   * @see RelDataTypeFactory#copyType(RelDataType)
   */
  public RexNode copy(RexNode expr) {
    return expr.accept(new RexCopier(this));
  }

  /**
   * Creates a literal of the default value for the given type.
   *
   * <p>This value is:</p>
   *
   * <ul>
   * <li>0 for numeric types;
   * <li>FALSE for BOOLEAN;
   * <li>The epoch for TIMESTAMP and DATE;
   * <li>Midnight for TIME;
   * <li>The empty string for string types (CHAR, BINARY, VARCHAR, VARBINARY).
   * </ul>
   *
   * @param type      Type
   * @return Simple literal
   */
  public RexLiteral makeZeroLiteral(RelDataType type) {
    return makeLiteral(zeroValue(type), type);
  }

  private static Comparable zeroValue(RelDataType type) {
    switch (type.getSqlTypeName()) {
    case CHAR:
      return new NlsString(Spaces.of(type.getPrecision()), null, null);
    case VARCHAR:
      return new NlsString("", null, null);
    case BINARY:
      return new ByteString(new byte[type.getPrecision()]);
    case VARBINARY:
      return ByteString.EMPTY;
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
    case DECIMAL:
    case FLOAT:
    case REAL:
    case DOUBLE:
      return BigDecimal.ZERO;
    case BOOLEAN:
      return false;
    case TIME:
    case DATE:
    case TIMESTAMP:
      return DateTimeUtils.ZERO_CALENDAR;
    case TIME_WITH_LOCAL_TIME_ZONE:
      return new TimeString(0, 0, 0);
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      return new TimestampString(0, 0, 0, 0, 0, 0);
    default:
      throw Util.unexpected(type.getSqlTypeName());
    }
  }

  /**
   * Creates a literal of a given type, padding values of constant-width
   * types to match their type, not allowing casts.
   *
   * @param value     Value
   * @param type      Type
   * @return Simple literal
   */
  public RexLiteral makeLiteral(@Nullable Object value, RelDataType type) {
    return (RexLiteral) makeLiteral(value, type, false, false);
  }

  /**
   * Creates a literal of a given type, padding values of constant-width
   * types to match their type.
   *
   * @param value     Value
   * @param type      Type
   * @param allowCast Whether to allow a cast. If false, value is always a
   *                  {@link RexLiteral} but may not be the exact type
   * @return Simple literal, or cast simple literal
   */
  public RexNode makeLiteral(@Nullable Object value, RelDataType type,
      boolean allowCast) {
    return makeLiteral(value, type, allowCast, false);
  }

  /**
   * Creates a literal of a given type. The value is assumed to be
   * compatible with the type.
   *
   * <p>The {@code trim} parameter controls whether to trim values of
   * constant-width types such as {@code CHAR}. Consider a call to
   * {@code makeLiteral("foo ", CHAR(5)}, and note that the value is too short
   * for its type. If {@code trim} is true, the value is converted to "foo"
   * and the type to {@code CHAR(3)}; if {@code trim} is false, the value is
   * right-padded with spaces to {@code "foo  "}, to match the type
   * {@code CHAR(5)}.
   *
   * @param value     Value
   * @param type      Type
   * @param allowCast Whether to allow a cast. If false, value is always a
   *                  {@link RexLiteral} but may not be the exact type
   * @param trim      Whether to trim values and type to the shortest equivalent
   *                  value; for example whether to convert CHAR(4) 'foo '
   *                  to CHAR(3) 'foo'
   * @return Simple literal, or cast simple literal
   */
  public RexNode makeLiteral(@Nullable Object value, RelDataType type,
      boolean allowCast, boolean trim) {
    if (value == null) {
      return makeCast(type, constantNull);
    }
    if (type.isNullable()) {
      final RelDataType typeNotNull =
          typeFactory.createTypeWithNullability(type, false);
      if (allowCast) {
        RexNode literalNotNull = makeLiteral(value, typeNotNull, allowCast);
        return makeAbstractCast(type, literalNotNull);
      }
      type = typeNotNull;
    }
    value = clean(value, type);
    RexLiteral literal;
    final List<RexNode> operands;
    final SqlTypeName sqlTypeName = type.getSqlTypeName();
    switch (sqlTypeName) {
    case CHAR:
      final NlsString nlsString = (NlsString) value;
      if (trim) {
        return makeCharLiteral(nlsString.rtrim());
      } else {
        return makeCharLiteral(padRight(nlsString, type.getPrecision()));
      }
    case VARCHAR:
      literal = makeCharLiteral((NlsString) value);
      if (allowCast) {
        return makeCast(type, literal);
      } else {
        return literal;
      }
    case BINARY:
      return makeBinaryLiteral(
          padRight((ByteString) value, type.getPrecision()));
    case VARBINARY:
      literal = makeBinaryLiteral((ByteString) value);
      if (allowCast) {
        return makeCast(type, literal);
      } else {
        return literal;
      }
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
    case DECIMAL:
      return makeExactLiteral((BigDecimal) value, type);
    case FLOAT:
    case REAL:
    case DOUBLE:
      return makeApproxLiteral((BigDecimal) value, type);
    case BOOLEAN:
      return (Boolean) value ? booleanTrue : booleanFalse;
    case TIME:
      return makeTimeLiteral((TimeString) value, type.getPrecision());
    case TIME_WITH_LOCAL_TIME_ZONE:
      return makeTimeWithLocalTimeZoneLiteral((TimeString) value, type.getPrecision());
    case DATE:
      return makeDateLiteral((DateString) value);
    case TIMESTAMP:
      return makeTimestampLiteral((TimestampString) value, type.getPrecision());
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      return makeTimestampWithLocalTimeZoneLiteral((TimestampString) value, type.getPrecision());
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      return makeIntervalLiteral((BigDecimal) value,
          castNonNull(type.getIntervalQualifier()));
    case SYMBOL:
      return makeFlag((Enum) value);
    case MAP:
      final MapSqlType mapType = (MapSqlType) type;
      @SuppressWarnings("unchecked")
      final Map<Object, Object> map = (Map) value;
      operands = new ArrayList<>();
      for (Map.Entry<Object, Object> entry : map.entrySet()) {
        operands.add(
            makeLiteral(entry.getKey(), mapType.getKeyType(), allowCast));
        operands.add(
            makeLiteral(entry.getValue(), mapType.getValueType(), allowCast));
      }
      return makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, operands);
    case ARRAY:
      final ArraySqlType arrayType = (ArraySqlType) type;
      @SuppressWarnings("unchecked")
      final List<Object> listValue = (List) value;
      operands = new ArrayList<>();
      for (Object entry : listValue) {
        operands.add(
            makeLiteral(entry, arrayType.getComponentType(), allowCast));
      }
      return makeCall(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, operands);
    case MULTISET:
      final MultisetSqlType multisetType = (MultisetSqlType) type;
      operands = new ArrayList<>();
      for (Object entry : (List) value) {
        final RexNode e = entry instanceof RexLiteral
            ? (RexNode) entry
            : makeLiteral(entry, multisetType.getComponentType(), allowCast);
        operands.add(e);
      }
      if (allowCast) {
        return makeCall(SqlStdOperatorTable.MULTISET_VALUE, operands);
      } else {
        return new RexLiteral((Comparable) FlatLists.of(operands), type,
            sqlTypeName);
      }
    case ROW:
      operands = new ArrayList<>();
      //noinspection unchecked
      for (Pair<RelDataTypeField, Object> pair
          : Pair.zip(type.getFieldList(), (List<Object>) value)) {
        final RexNode e = pair.right instanceof RexLiteral
            ? (RexNode) pair.right
            : makeLiteral(pair.right, pair.left.getType(), allowCast);
        operands.add(e);
      }
      return new RexLiteral((Comparable) FlatLists.of(operands), type,
          sqlTypeName);
    case GEOMETRY:
      return new RexLiteral((Comparable) value, guessType(value),
          SqlTypeName.GEOMETRY);
    case ANY:
      return makeLiteral(value, guessType(value), allowCast);
    default:
      throw new IllegalArgumentException(
          "Cannot create literal for type '" + sqlTypeName + "'");
    }
  }

  /** Converts the type of a value to comply with
   * {@link org.apache.calcite.rex.RexLiteral#valueMatchesType}.
   *
   * <p>Returns null if and only if {@code o} is null. */
  private static @PolyNull Object clean(@PolyNull Object o, RelDataType type) {
    if (o == null) {
      return o;
    }
    switch (type.getSqlTypeName()) {
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
    case DECIMAL:
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      if (o instanceof BigDecimal) {
        return o;
      }
      assert !(o instanceof Float || o instanceof Double)
          : String.format(Locale.ROOT,
              "%s is not compatible with %s, try to use makeExactLiteral",
              o.getClass().getCanonicalName(),
              type.getSqlTypeName());
      return new BigDecimal(((Number) o).longValue());
    case FLOAT:
      if (o instanceof BigDecimal) {
        return o;
      }
      return new BigDecimal(((Number) o).doubleValue(), MathContext.DECIMAL32)
          .stripTrailingZeros();
    case REAL:
    case DOUBLE:
      if (o instanceof BigDecimal) {
        return o;
      }
      return new BigDecimal(((Number) o).doubleValue(), MathContext.DECIMAL64)
          .stripTrailingZeros();
    case CHAR:
    case VARCHAR:
      if (o instanceof NlsString) {
        return o;
      }
      assert type.getCharset() != null : type + ".getCharset() must not be null";
      return new NlsString((String) o, type.getCharset().name(),
          type.getCollation());
    case TIME:
      if (o instanceof TimeString) {
        return o;
      } else if (o instanceof Calendar) {
        if (!((Calendar) o).getTimeZone().equals(DateTimeUtils.UTC_ZONE)) {
          throw new AssertionError();
        }
        return TimeString.fromCalendarFields((Calendar) o);
      } else {
        return TimeString.fromMillisOfDay((Integer) o);
      }
    case TIME_WITH_LOCAL_TIME_ZONE:
      if (o instanceof TimeString) {
        return o;
      } else {
        return TimeString.fromMillisOfDay((Integer) o);
      }
    case DATE:
      if (o instanceof DateString) {
        return o;
      } else if (o instanceof Calendar) {
        if (!((Calendar) o).getTimeZone().equals(DateTimeUtils.UTC_ZONE)) {
          throw new AssertionError();
        }
        return DateString.fromCalendarFields((Calendar) o);
      } else {
        return DateString.fromDaysSinceEpoch((Integer) o);
      }
    case TIMESTAMP:
      if (o instanceof TimestampString) {
        return o;
      } else if (o instanceof Calendar) {
        if (!((Calendar) o).getTimeZone().equals(DateTimeUtils.UTC_ZONE)) {
          throw new AssertionError();
        }
        return TimestampString.fromCalendarFields((Calendar) o);
      } else {
        return TimestampString.fromMillisSinceEpoch((Long) o);
      }
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      if (o instanceof TimestampString) {
        return o;
      } else {
        return TimestampString.fromMillisSinceEpoch((Long) o);
      }
    default:
      return o;
    }
  }

  private RelDataType guessType(@Nullable Object value) {
    if (value == null) {
      return typeFactory.createSqlType(SqlTypeName.NULL);
    }
    if (value instanceof Float || value instanceof Double) {
      return typeFactory.createSqlType(SqlTypeName.DOUBLE);
    }
    if (value instanceof Number) {
      return typeFactory.createSqlType(SqlTypeName.BIGINT);
    }
    if (value instanceof Boolean) {
      return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    }
    if (value instanceof String) {
      return typeFactory.createSqlType(SqlTypeName.CHAR,
          ((String) value).length());
    }
    if (value instanceof ByteString) {
      return typeFactory.createSqlType(SqlTypeName.BINARY,
          ((ByteString) value).length());
    }
    if (value instanceof Geometries.Geom) {
      return typeFactory.createSqlType(SqlTypeName.GEOMETRY);
    }
    throw new AssertionError("unknown type " + value.getClass());
  }

  /** Returns an {@link NlsString} with spaces to make it at least a given
   * length. */
  private static NlsString padRight(NlsString s, int length) {
    if (s.getValue().length() >= length) {
      return s;
    }
    return s.copy(padRight(s.getValue(), length));
  }

  /** Returns a string padded with spaces to make it at least a given length. */
  private static String padRight(String s, int length) {
    if (s.length() >= length) {
      return s;
    }
    return new StringBuilder()
        .append(s)
        .append(Spaces.MAX, s.length(), length)
        .toString();
  }

  /** Returns a byte-string padded with zero bytes to make it at least a given
   * length. */
  private static ByteString padRight(ByteString s, int length) {
    if (s.length() >= length) {
      return s;
    }
    return new ByteString(Arrays.copyOf(s.getBytes(), length));
  }
}
