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
package org.eigenbase.rex;

import java.math.*;
import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.SqlIntervalQualifier.TimeUnit;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;
import org.eigenbase.util14.DateTimeUtil;

import net.hydromatic.avatica.ByteString;

import net.hydromatic.optiq.runtime.Spaces;
import net.hydromatic.optiq.runtime.SqlFunctions;

import com.google.common.collect.ImmutableList;

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

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns this RexBuilder's type factory
   *
   * @return type factory
   */
  public RelDataTypeFactory getTypeFactory() {
    return typeFactory;
  }

  /**
   * Returns this RexBuilder's operator table
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
    final RelDataTypeField field = type.getField(fieldName, caseSensitive);
    if (field == null) {
      throw Util.newInternal(
          "Type '" + type + "' has no field '"
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
      throw Util.newInternal(
          "Field ordinal " + i + " is invalid for "
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
   * {@link #makeCall(org.eigenbase.reltype.RelDataType, org.eigenbase.sql.SqlOperator, java.util.List)}
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
   * <p>Equivalent to <code>makeCall(op, exprList.toArray(new
   * RexNode[exprList.size()]))</code>.
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
    return op.inferReturnType(new RexCallBinding(typeFactory, op, exprs));
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
   * @param aggArgTypes Argument types, not null
   * @return Rex expression for the given aggregate call
   */
  public RexNode addAggCall(
      AggregateCall aggCall,
      int groupCount,
      List<AggregateCall> aggCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      final List<RelDataType> aggArgTypes) {
    if (aggCall.getAggregation() instanceof SqlCountAggFunction
        && !aggCall.isDistinct()) {
      final List<Integer> args = aggCall.getArgList();
      final List<Integer> nullableArgs = nullableArgs(args, aggArgTypes);
      if (!nullableArgs.equals(args)) {
        aggCall = aggCall.copy(nullableArgs);
      }
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

  private static List<Integer> nullableArgs(List<Integer> list0,
      List<RelDataType> types) {
    final List<Integer> list = new ArrayList<Integer>();
    for (Pair<Integer, RelDataType> pair : Pair.zip(list0, types)) {
      if (pair.right.isNullable()) {
        list.add(pair.left);
      }
    }
    return list;
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
      boolean physical,
      boolean allowPartial,
      boolean nullWhenCountZero) {
    assert operator != null;
    assert exprs != null;
    assert partitionKeys != null;
    assert orderKeys != null;
    final RexWindow window =
        makeWindow(
            partitionKeys,
            orderKeys,
            lowerBound,
            upperBound,
            physical);
    final RexOver over = new RexOver(type, operator, exprs, window);
    RexNode result = over;

    // This should be correct but need time to go over test results.
    // Also want to look at combing with section below.
    if (nullWhenCountZero) {
      final RelDataType bigintType = getTypeFactory().createSqlType(
          SqlTypeName.BIGINT);
      result = makeCall(
          SqlStdOperatorTable.CASE,
          makeCall(
              SqlStdOperatorTable.GREATER_THAN,
              new RexOver(
                  bigintType,
                  SqlStdOperatorTable.COUNT,
                  exprs,
                  window),
              makeLiteral(
                  BigDecimal.ZERO,
                  bigintType,
                  SqlTypeName.DECIMAL)),
          ensureType(type, // SUM0 is non-nullable, thus need a cast
              new RexOver(typeFactory.createTypeWithNullability(type, false),
              operator, exprs, window),
              false),
          makeCast(type, constantNull()));
    }
    if (!allowPartial) {
      Util.permAssert(physical, "DISALLOW PARTIAL over RANGE");
      final RelDataType bigintType = getTypeFactory().createSqlType(
          SqlTypeName.BIGINT);
      // todo: read bound
      result =
          makeCall(
              SqlStdOperatorTable.CASE,
              makeCall(
                  SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                  new RexOver(
                      bigintType,
                      SqlStdOperatorTable.COUNT,
                      ImmutableList.<RexNode>of(),
                      window),
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
   * @param isRows        Whether physical. True if row-based, false if
   *                      range-based
   * @return window specification
   */
  public RexWindow makeWindow(
      List<RexNode> partitionKeys,
      ImmutableList<RexFieldCollation> orderKeys,
      RexWindowBound lowerBound,
      RexWindowBound upperBound,
      boolean isRows) {
    return new RexWindow(
        partitionKeys,
        orderKeys,
        lowerBound,
        upperBound,
        isRows);
  }

  /**
   * Creates a constant for the SQL <code>NULL</code> value.
   */
  public RexLiteral constantNull() {
    return constantNull;
  }

  /**
   * Creates an expression referencing a correlation variable.
   *
   * @param type Type of variable
   * @param name Name of variable
   * @return Correlation variable
   */
  public RexNode makeCorrel(
      RelDataType type,
      String name) {
    return new RexCorrelVariable(name, type);
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
   * Creates a call to the CAST operator, expanding if possible.
   *
   * @param type Type to cast to
   * @param exp  Expression being cast
   * @return Call to CAST operator
   */
  public RexNode makeCast(
      RelDataType type,
      RexNode exp) {
    final SqlTypeName sqlType = type.getSqlTypeName();
    if (exp instanceof RexLiteral) {
      RexLiteral literal = (RexLiteral) exp;
      Comparable value = literal.getValue();
      if (RexLiteral.valueMatchesType(value, sqlType, false)
          && (!(value instanceof NlsString)
          || (type.getPrecision()
          >= ((NlsString) value).getValue().length()))
          && (!(value instanceof ByteString)
          || (type.getPrecision()
          >= ((ByteString) value).length()))) {
        switch (literal.getTypeName()) {
        case CHAR:
          if (value instanceof NlsString) {
            value = ((NlsString) value).rtrim();
          }
          break;
        case TIMESTAMP:
        case TIME:
          final Calendar calendar = (Calendar) value;
          int scale = type.getScale();
          if (scale == RelDataType.SCALE_NOT_SPECIFIED) {
            scale = 0;
          }
          calendar.setTimeInMillis(
              SqlFunctions.round(
                  calendar.getTimeInMillis(),
                  SqlFunctions.powerX(10, 3 - scale)));
          break;
        case INTERVAL_DAY_TIME:
          BigDecimal value2 = (BigDecimal) value;
          final long multiplier =
              literal.getType().getIntervalQualifier().getStartUnit()
                  .multiplier;
          SqlTypeName typeName = type.getSqlTypeName();
          // Not all types are allowed for literals
          switch (typeName) {
          case INTEGER:
            typeName = SqlTypeName.BIGINT;
          }
          return makeLiteral(
              value2.divide(
                  BigDecimal.valueOf(multiplier), 0,
                  BigDecimal.ROUND_HALF_DOWN),
              type, typeName);
        }
        return makeLiteral(value, type, literal.getTypeName());
      }
    } else if (SqlTypeUtil.isInterval(type)
        && SqlTypeUtil.isExactNumeric(exp.getType())) {
      return makeCastExactToInterval(type, exp);
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

  private RexNode makeCastExactToBoolean(RelDataType toType, RexNode exp) {
    return makeCall(
        toType,
        SqlStdOperatorTable.NOT_EQUALS,
        ImmutableList.of(exp, makeZeroLiteral(exp.getType())));
  }

  private RexNode makeCastBooleanToExact(RelDataType toType, RexNode exp) {
    final RexNode casted = makeCall(
        SqlStdOperatorTable.CASE,
        exp,
        makeExactLiteral(BigDecimal.ONE, toType),
        makeZeroLiteral(toType));
    if (!exp.getType().isNullable()) {
      return casted;
    }
    return makeCall(
        toType,
        SqlStdOperatorTable.CASE,
        ImmutableList.<RexNode>of(
            makeCall(SqlStdOperatorTable.IS_NOT_NULL, exp),
            casted,
            makeNullLiteral(toType.getSqlTypeName())));
  }

  private RexNode makeCastIntervalToExact(RelDataType toType, RexNode exp) {
    IntervalSqlType intervalType = (IntervalSqlType) exp.getType();
    TimeUnit endUnit = intervalType.getIntervalQualifier().getEndUnit();
    if (endUnit == null) {
      endUnit = intervalType.getIntervalQualifier().getStartUnit();
    }
    int scale = 0;
    if (endUnit == TimeUnit.SECOND) {
      scale = Math.min(
          intervalType.getIntervalQualifier()
              .getFractionalSecondPrecision(typeFactory.getTypeSystem()),
          3);
    }
    BigDecimal multiplier = BigDecimal.valueOf(endUnit.multiplier)
        .divide(BigDecimal.TEN.pow(scale));
    RexNode value = decodeIntervalOrDecimal(exp);
    if (multiplier.longValue() != 1) {
      value = makeCall(
          SqlStdOperatorTable.DIVIDE_INTEGER,
          value, makeBigintLiteral(multiplier));
    }
    if (scale > 0) {
      RelDataType decimalType =
          getTypeFactory().createSqlType(
              SqlTypeName.DECIMAL,
              scale + intervalType.getPrecision(),
              scale);
      value = encodeIntervalOrDecimal(value, decimalType, false);
    }
    return ensureType(toType, value, false);
  }

  private RexNode makeCastExactToInterval(RelDataType toType, RexNode exp) {
    IntervalSqlType intervalType = (IntervalSqlType) toType;
    TimeUnit endUnit = intervalType.getIntervalQualifier().getEndUnit();
    if (endUnit == null) {
      endUnit = intervalType.getIntervalQualifier().getStartUnit();
    }
    int scale = 0;
    if (endUnit == TimeUnit.SECOND) {
      scale = Math.min(
          intervalType.getIntervalQualifier()
              .getFractionalSecondPrecision(typeFactory.getTypeSystem()),
          3);
    }
    BigDecimal multiplier = BigDecimal.valueOf(endUnit.multiplier)
        .divide(BigDecimal.TEN.pow(scale));
    RelDataType decimalType =
        getTypeFactory().createSqlType(
            SqlTypeName.DECIMAL,
            scale + intervalType.getPrecision(),
            scale);
    RexNode value = decodeIntervalOrDecimal(ensureType(decimalType, exp, true));
    if (multiplier.longValue() != 1) {
      value = makeCall(
          SqlStdOperatorTable.MULTIPLY,
          value, makeExactLiteral(multiplier));
    }
    return encodeIntervalOrDecimal(value, toType, false);
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
        typeFactory.createSqlType(
            SqlTypeName.BIGINT);
    RexNode cast = ensureType(bigintType, value, true);
    return makeReinterpretCast(type, cast, makeLiteral(checkOverflow));
  }

  /**
   * Retrieves an interval or decimal node's integer representation
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
   * Makes an expression which converts a value of type T to a value of type T
   * NOT NULL, or throws if the value is NULL. If the expression is already
   * NOT NULL, does nothing.
   */
  public RexNode makeNotNullCast(RexNode expr) {
    RelDataType type = expr.getType();
    if (!type.isNullable()) {
      return expr;
    }
    RelDataType typeNotNull =
        getTypeFactory().createTypeWithNullability(type, false);
    return new RexCall(
        typeNotNull,
        SqlStdOperatorTable.CAST,
        ImmutableList.of(expr));
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
   */
  public RexInputRef makeInputRef(RelNode input, int i) {
    return makeInputRef(input.getRowType().getFieldList().get(i).getType(), i);
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
   * should call one of the type-specific methods such as {@link
   * #makeDateLiteral(Calendar)}, {@link #makeLiteral(boolean)}, {@link
   * #makeLiteral(String)}.
   *
   * @param o        Value of literal, must be appropriate for the type
   * @param type     Type of literal
   * @param typeName SQL type of literal
   * @return Literal
   */
  protected RexLiteral makeLiteral(
      Comparable o,
      RelDataType type,
      SqlTypeName typeName) {
    // All literals except NULL have NOT NULL types.
    type = typeFactory.createTypeWithNullability(type, o == null);
    if (typeName == SqlTypeName.CHAR) {
      // Character literals must have a charset and collation. Populate
      // from the type if necessary.
      assert o instanceof NlsString;
      NlsString nlsString = (NlsString) o;
      if ((nlsString.getCollation() == null)
          || (nlsString.getCharset() == null)) {
        assert type.getSqlTypeName() == SqlTypeName.CHAR;
        assert type.getCharset().name() != null;
        assert type.getCollation() != null;
        o = new NlsString(
            nlsString.getValue(),
            type.getCharset().name(),
            type.getCollation());
      }
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
    long l = bd.unscaledValue().longValue();
    assert scale >= 0;
    assert scale <= typeFactory.getTypeSystem().getMaxNumericScale() : scale;
    assert BigDecimal.valueOf(l, scale).equals(bd);
    if (scale == 0) {
      if ((l >= Integer.MIN_VALUE) && (l <= Integer.MAX_VALUE)) {
        relType = typeFactory.createSqlType(SqlTypeName.INTEGER);
      } else {
        relType = typeFactory.createSqlType(SqlTypeName.BIGINT);
      }
    } else {
      int precision = bd.unscaledValue().toString().length();
      relType =
          typeFactory.createSqlType(
              SqlTypeName.DECIMAL, scale, precision);
    }
    return makeExactLiteral(bd, relType);
  }

  /**
   * Creates a BIGINT literal.
   */
  public RexLiteral makeBigintLiteral(BigDecimal bd) {
    RelDataType bigintType =
        typeFactory.createSqlType(
            SqlTypeName.BIGINT);
    return makeLiteral(bd, bigintType, SqlTypeName.DECIMAL);
  }

  /**
   * Creates a numeric literal.
   */
  public RexLiteral makeExactLiteral(BigDecimal bd, RelDataType type) {
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
  public RexLiteral makeApproxLiteral(BigDecimal bd, RelDataType type) {
    assert SqlTypeFamily.APPROXIMATE_NUMERIC.getTypeNames().contains(
        type.getSqlTypeName());
    return makeLiteral(bd, type, SqlTypeName.DOUBLE);
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
    } else {
      return makeLiteral(
          new NlsString(s, null, null),
          typeFactory.createSqlType(
              SqlTypeName.CHAR,
              s.length()),
          SqlTypeName.CHAR);
    }
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
      return getTypeFactory().createTypeWithNullability(
          type,
          valueNullability);
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

  /**
   * Creates a Date literal.
   */
  public RexLiteral makeDateLiteral(Calendar date) {
    assert date != null;
    return makeLiteral(
        date, typeFactory.createSqlType(SqlTypeName.DATE), SqlTypeName.DATE);
  }

  /**
   * Creates a Time literal.
   */
  public RexLiteral makeTimeLiteral(
      Calendar time,
      int precision) {
    assert time != null;
    return makeLiteral(
        time,
        typeFactory.createSqlType(SqlTypeName.TIME, precision),
        SqlTypeName.TIME);
  }

  /**
   * Creates a Timestamp literal.
   */
  public RexLiteral makeTimestampLiteral(
      Calendar timestamp,
      int precision) {
    assert timestamp != null;
    return makeLiteral(
        timestamp,
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP, precision),
        SqlTypeName.TIMESTAMP);
  }

  /**
   * Creates an interval literal.
   */
  public RexLiteral makeIntervalLiteral(
      SqlIntervalQualifier intervalQualifier) {
    assert intervalQualifier != null;
    return makeFlag(intervalQualifier.foo());
  }

  /**
   * Creates an interval literal.
   */
  public RexLiteral makeIntervalLiteral(
      BigDecimal v,
      SqlIntervalQualifier intervalQualifier) {
    return makeLiteral(
        v,
        typeFactory.createSqlIntervalType(intervalQualifier),
        intervalQualifier.isYearMonth() ? SqlTypeName.INTERVAL_YEAR_MONTH
            : SqlTypeName.INTERVAL_DAY_TIME);
  }

  /**
   * Creates a reference to a dynamic parameter
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
   * Creates an expression corresponding to a null literal, cast to a specific
   * type and precision
   *
   * @param typeName  name of the type that the null will be cast to
   * @param precision precision of the type
   * @return created expression
   */
  public RexNode makeNullLiteral(SqlTypeName typeName, int precision) {
    RelDataType type =
        typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(typeName, precision),
            true);
    return makeCast(type, constantNull());
  }

  /**
   * Creates a literal whose value is NULL, with a particular type.
   *
   * <p>The typing is necessary because RexNodes are strictly typed. For
   * example, in the Rex world the <code>NULL</code> parameter to <code>
   * SUBSTRING(NULL FROM 2 FOR 4)</code> must have a valid VARCHAR type so
   * that the result type can be determined.
   *
   * @param typeName Type to cast NULL to
   * @return NULL literal of given type
   */
  public RexNode makeNullLiteral(SqlTypeName typeName) {
    RelDataType type =
        typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(typeName),
            true);
    return makeCast(type, constantNull());
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
   * @return Simple literal, or cast simple literal
   */
  public RexNode makeZeroLiteral(RelDataType type) {
    return makeLiteral(zeroValue(type), type, false);
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
      return DateTimeUtil.ZERO_CALENDAR;
    default:
      throw Util.unexpected(type.getSqlTypeName());
    }
  }

  /**
   * Creates a literal of a given type. The value is assumed to be
   * compatible with the type.
   *
   * @param value     Value
   * @param type      Type
   * @param allowCast Whether to allow a cast. If false, value is always a
   *                  {@link RexLiteral} but may not be the exact type
   * @return Simple literal, or cast simple literal
   */
  public RexNode makeLiteral(Object value, RelDataType type,
      boolean allowCast) {
    if (value == null) {
      return makeCast(type, constantNull);
    }
    if (type.isNullable()) {
      final RelDataType typeNotNull =
          typeFactory.createTypeWithNullability(type, false);
      RexNode literalNotNull = makeLiteral(value, typeNotNull, allowCast);
      return makeAbstractCast(type, literalNotNull);
    }
    value = clean(value, type);
    RexLiteral literal;
    final List<RexNode> operands;
    switch (type.getSqlTypeName()) {
    case CHAR:
      return makeCharLiteral(padRight((NlsString) value, type.getPrecision()));
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
      return makeTimeLiteral((Calendar) value, type.getPrecision());
    case DATE:
      return makeDateLiteral((Calendar) value);
    case TIMESTAMP:
      return makeTimestampLiteral((Calendar) value, type.getPrecision());
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_DAY_TIME:
      return makeIntervalLiteral((BigDecimal) value,
          type.getIntervalQualifier());
    case MAP:
      final MapSqlType mapType = (MapSqlType) type;
      @SuppressWarnings("unchecked")
      final Map<Object, Object> map = (Map) value;
      operands = new ArrayList<RexNode>();
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
      operands = new ArrayList<RexNode>();
      for (Object entry : listValue) {
        operands.add(
            makeLiteral(entry, arrayType.getComponentType(), allowCast));
      }
      return makeCall(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, operands);
    case ANY:
      return makeLiteral(value, guessType(value), allowCast);
    default:
      throw Util.unexpected(type.getSqlTypeName());
    }
  }

  /** Converts the type of a value to comply with
   * {@link org.eigenbase.rex.RexLiteral#valueMatchesType}. */
  private static Object clean(Object o, RelDataType type) {
    if (o == null) {
      return null;
    }
    final Calendar calendar;
    switch (type.getSqlTypeName()) {
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
    case DECIMAL:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_DAY_TIME:
      if (o instanceof BigDecimal) {
        return o;
      }
      return new BigDecimal(((Number) o).longValue());
    case FLOAT:
    case REAL:
    case DOUBLE:
      if (o instanceof BigDecimal) {
        return o;
      }
      return new BigDecimal(((Number) o).doubleValue());
    case CHAR:
    case VARCHAR:
      if (o instanceof NlsString) {
        return o;
      }
      return new NlsString((String) o, type.getCharset().name(),
          type.getCollation());
    case TIME:
      if (o instanceof Calendar) {
        return o;
      }
      calendar = Calendar.getInstance(DateTimeUtil.GMT_ZONE);
      calendar.setTimeInMillis((Integer) o);
      return calendar;
    case DATE:
      if (o instanceof Calendar) {
        return o;
      }
      calendar = Calendar.getInstance(DateTimeUtil.GMT_ZONE);
      calendar.setTimeInMillis(0);
      calendar.add(Calendar.DAY_OF_YEAR, (Integer) o);
      return calendar;
    case TIMESTAMP:
      if (o instanceof Calendar) {
        return o;
      }
      calendar = Calendar.getInstance(DateTimeUtil.GMT_ZONE);
      calendar.setTimeInMillis((Long) o);
      return calendar;
    default:
      return o;
    }
  }

  private RelDataType guessType(Object value) {
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
   * length, */
  private static ByteString padRight(ByteString s, int length) {
    if (s.length() >= length) {
      return s;
    }
    return new ByteString(Arrays.copyOf(s.getBytes(), length));
  }
}

// End RexBuilder.java
