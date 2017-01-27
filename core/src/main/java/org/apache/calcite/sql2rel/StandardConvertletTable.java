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
package org.apache.calcite.sql2rel;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJdbcFunctionCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.OracleSqlOperatorTable;
import org.apache.calcite.sql.fun.SqlArrayValueConstructor;
import org.apache.calcite.sql.fun.SqlBetweenOperator;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlDatetimeSubtractionOperator;
import org.apache.calcite.sql.fun.SqlExtractFunction;
import org.apache.calcite.sql.fun.SqlLiteralChainOperator;
import org.apache.calcite.sql.fun.SqlMapValueConstructor;
import org.apache.calcite.sql.fun.SqlMultisetQueryConstructor;
import org.apache.calcite.sql.fun.SqlMultisetValueConstructor;
import org.apache.calcite.sql.fun.SqlOverlapsOperator;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.fun.SqlSequenceValueOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Standard implementation of {@link SqlRexConvertletTable}.
 */
public class StandardConvertletTable extends ReflectiveConvertletTable {

  /** Singleton instance. */
  public static final StandardConvertletTable INSTANCE =
      new StandardConvertletTable();

  //~ Constructors -----------------------------------------------------------

  private StandardConvertletTable() {
    super();

    // Register aliases (operators which have a different name but
    // identical behavior to other operators).
    addAlias(
        SqlStdOperatorTable.CHARACTER_LENGTH,
        SqlStdOperatorTable.CHAR_LENGTH);
    addAlias(
        SqlStdOperatorTable.IS_UNKNOWN,
        SqlStdOperatorTable.IS_NULL);
    addAlias(
        SqlStdOperatorTable.IS_NOT_UNKNOWN,
        SqlStdOperatorTable.IS_NOT_NULL);

    // Register convertlets for specific objects.
    registerOp(
        SqlStdOperatorTable.CAST,
        new SqlRexConvertlet() {
          public RexNode convertCall(SqlRexContext cx, SqlCall call) {
            return convertCast(cx, call);
          }
        });
    registerOp(
        SqlStdOperatorTable.IS_DISTINCT_FROM,
        new SqlRexConvertlet() {
          public RexNode convertCall(SqlRexContext cx, SqlCall call) {
            return convertIsDistinctFrom(cx, call, false);
          }
        });
    registerOp(
        SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
        new SqlRexConvertlet() {
          public RexNode convertCall(SqlRexContext cx, SqlCall call) {
            return convertIsDistinctFrom(cx, call, true);
          }
        });

    registerOp(
        SqlStdOperatorTable.PLUS,
        new SqlRexConvertlet() {
          public RexNode convertCall(SqlRexContext cx, SqlCall call) {
            return convertPlus(cx, call);
          }
        });

    registerOp(SqlStdOperatorTable.MINUS,
        new SqlRexConvertlet() {
          public RexNode convertCall(SqlRexContext cx, SqlCall call) {
            final RexCall e =
                (RexCall) StandardConvertletTable.this.convertCall(cx, call,
                    call.getOperator());
            switch (e.getOperands().get(0).getType().getSqlTypeName()) {
            case DATE:
            case TIME:
            case TIMESTAMP:
              return convertDatetimeMinus(cx, SqlStdOperatorTable.MINUS_DATE,
                  call);
            default:
              return e;
            }
          }
        });

    registerOp(OracleSqlOperatorTable.LTRIM,
        new TrimConvertlet(SqlTrimFunction.Flag.LEADING));
    registerOp(OracleSqlOperatorTable.RTRIM,
        new TrimConvertlet(SqlTrimFunction.Flag.TRAILING));

    registerOp(OracleSqlOperatorTable.GREATEST, new GreatestConvertlet());
    registerOp(OracleSqlOperatorTable.LEAST, new GreatestConvertlet());

    registerOp(OracleSqlOperatorTable.NVL,
        new SqlRexConvertlet() {
          public RexNode convertCall(SqlRexContext cx, SqlCall call) {
            final RexBuilder rexBuilder = cx.getRexBuilder();
            final RexNode operand0 =
                cx.convertExpression(call.getOperandList().get(0));
            final RexNode operand1 =
                cx.convertExpression(call.getOperandList().get(1));
            final RelDataType type =
                cx.getValidator().getValidatedNodeType(call);
            return rexBuilder.makeCall(type, SqlStdOperatorTable.CASE,
                ImmutableList.of(
                    rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL,
                        operand0),
                    rexBuilder.makeCast(type, operand0),
                    rexBuilder.makeCast(type, operand1)));
          }
        });

    registerOp(OracleSqlOperatorTable.DECODE,
        new SqlRexConvertlet() {
          public RexNode convertCall(SqlRexContext cx, SqlCall call) {
            final RexBuilder rexBuilder = cx.getRexBuilder();
            final List<RexNode> operands = convertExpressionList(cx,
                call.getOperandList(), SqlOperandTypeChecker.Consistency.NONE);
            final RelDataType type =
                cx.getValidator().getValidatedNodeType(call);
            final List<RexNode> exprs = new ArrayList<>();
            for (int i = 1; i < operands.size() - 1; i += 2) {
              exprs.add(
                  RelOptUtil.isDistinctFrom(rexBuilder, operands.get(0),
                      operands.get(i), true));
              exprs.add(operands.get(i + 1));
            }
            if (operands.size() % 2 == 0) {
              exprs.add(Util.last(operands));
            } else {
              exprs.add(rexBuilder.makeNullLiteral(type.getSqlTypeName()));
            }
            return rexBuilder.makeCall(type, SqlStdOperatorTable.CASE, exprs);
          }
        });

    // Expand "x NOT LIKE y" into "NOT (x LIKE y)"
    registerOp(
        SqlStdOperatorTable.NOT_LIKE,
        new SqlRexConvertlet() {
          public RexNode convertCall(SqlRexContext cx, SqlCall call) {
            final SqlCall expanded =
                SqlStdOperatorTable.NOT.createCall(
                    SqlParserPos.ZERO,
                    SqlStdOperatorTable.LIKE.createCall(
                        SqlParserPos.ZERO,
                        call.getOperandList()));
            return cx.convertExpression(expanded);
          }
        });

    // Expand "x NOT SIMILAR y" into "NOT (x SIMILAR y)"
    registerOp(
        SqlStdOperatorTable.NOT_SIMILAR_TO,
        new SqlRexConvertlet() {
          public RexNode convertCall(SqlRexContext cx, SqlCall call) {
            final SqlCall expanded =
                SqlStdOperatorTable.NOT.createCall(
                    SqlParserPos.ZERO,
                    SqlStdOperatorTable.SIMILAR_TO.createCall(
                        SqlParserPos.ZERO,
                        call.getOperandList()));
            return cx.convertExpression(expanded);
          }
        });

    // Unary "+" has no effect, so expand "+ x" into "x".
    registerOp(
        SqlStdOperatorTable.UNARY_PLUS,
        new SqlRexConvertlet() {
          public RexNode convertCall(SqlRexContext cx, SqlCall call) {
            SqlNode expanded = call.operand(0);
            return cx.convertExpression(expanded);
          }
        });

    // "AS" has no effect, so expand "x AS id" into "x".
    registerOp(
        SqlStdOperatorTable.AS,
        new SqlRexConvertlet() {
          public RexNode convertCall(SqlRexContext cx, SqlCall call) {
            SqlNode expanded = call.operand(0);
            return cx.convertExpression(expanded);
          }
        });

    // "SQRT(x)" is equivalent to "POWER(x, .5)"
    registerOp(
        SqlStdOperatorTable.SQRT,
        new SqlRexConvertlet() {
          public RexNode convertCall(SqlRexContext cx, SqlCall call) {
            SqlNode expanded =
                SqlStdOperatorTable.POWER.createCall(
                    SqlParserPos.ZERO,
                    call.operand(0),
                    SqlLiteral.createExactNumeric(
                        "0.5", SqlParserPos.ZERO));
            return cx.convertExpression(expanded);
          }
        });

    // REVIEW jvs 24-Apr-2006: This only seems to be working from within a
    // windowed agg.  I have added an optimizer rule
    // org.apache.calcite.rel.rules.AggregateReduceFunctionsRule which handles
    // other cases post-translation.  The reason I did that was to defer the
    // implementation decision; e.g. we may want to push it down to a foreign
    // server directly rather than decomposed; decomposition is easier than
    // recognition.

    // Convert "avg(<expr>)" to "cast(sum(<expr>) / count(<expr>) as
    // <type>)". We don't need to handle the empty set specially, because
    // the SUM is already supposed to come out as NULL in cases where the
    // COUNT is zero, so the null check should take place first and prevent
    // division by zero. We need the cast because SUM and COUNT may use
    // different types, say BIGINT.
    //
    // Similarly STDDEV_POP and STDDEV_SAMP, VAR_POP and VAR_SAMP.
    registerOp(
        SqlStdOperatorTable.AVG,
        new AvgVarianceConvertlet(SqlKind.AVG));
    registerOp(
        SqlStdOperatorTable.STDDEV_POP,
        new AvgVarianceConvertlet(SqlKind.STDDEV_POP));
    registerOp(
        SqlStdOperatorTable.STDDEV_SAMP,
        new AvgVarianceConvertlet(SqlKind.STDDEV_SAMP));
    registerOp(
        SqlStdOperatorTable.VAR_POP,
        new AvgVarianceConvertlet(SqlKind.VAR_POP));
    registerOp(
        SqlStdOperatorTable.VAR_SAMP,
        new AvgVarianceConvertlet(SqlKind.VAR_SAMP));

    final SqlRexConvertlet floorCeilConvertlet = new FloorCeilConvertlet();
    registerOp(SqlStdOperatorTable.FLOOR, floorCeilConvertlet);
    registerOp(SqlStdOperatorTable.CEIL, floorCeilConvertlet);

    registerOp(SqlStdOperatorTable.TIMESTAMP_ADD, new TimestampAddConvertlet());
    registerOp(SqlStdOperatorTable.TIMESTAMP_DIFF,
        new TimestampDiffConvertlet());

    // Convert "element(<expr>)" to "$element_slice(<expr>)", if the
    // expression is a multiset of scalars.
    if (false) {
      registerOp(
          SqlStdOperatorTable.ELEMENT,
          new SqlRexConvertlet() {
            public RexNode convertCall(SqlRexContext cx, SqlCall call) {
              assert call.operandCount() == 1;
              final SqlNode operand = call.operand(0);
              final RelDataType type =
                  cx.getValidator().getValidatedNodeType(operand);
              if (!type.getComponentType().isStruct()) {
                return cx.convertExpression(
                    SqlStdOperatorTable.ELEMENT_SLICE.createCall(
                        SqlParserPos.ZERO,
                        operand));
              }

              // fallback on default behavior
              return StandardConvertletTable.this.convertCall(
                  cx,
                  call);
            }
          });
    }

    // Convert "$element_slice(<expr>)" to "element(<expr>).field#0"
    if (false) {
      registerOp(
          SqlStdOperatorTable.ELEMENT_SLICE,
          new SqlRexConvertlet() {
            public RexNode convertCall(SqlRexContext cx, SqlCall call) {
              assert call.operandCount() == 1;
              final SqlNode operand = call.operand(0);
              final RexNode expr =
                  cx.convertExpression(
                      SqlStdOperatorTable.ELEMENT.createCall(
                          SqlParserPos.ZERO,
                          operand));
              return cx.getRexBuilder().makeFieldAccess(
                  expr,
                  0);
            }
          });
    }
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Converts a CASE expression.
   */
  public RexNode convertCase(
      SqlRexContext cx,
      SqlCase call) {
    SqlNodeList whenList = call.getWhenOperands();
    SqlNodeList thenList = call.getThenOperands();
    assert whenList.size() == thenList.size();

    final List<RexNode> exprList = new ArrayList<>();
    for (int i = 0; i < whenList.size(); i++) {
      exprList.add(cx.convertExpression(whenList.get(i)));
      exprList.add(cx.convertExpression(thenList.get(i)));
    }
    exprList.add(cx.convertExpression(call.getElseOperand()));

    RexBuilder rexBuilder = cx.getRexBuilder();
    RelDataType type =
        rexBuilder.deriveReturnType(call.getOperator(), exprList);
    for (int i : elseArgs(exprList.size())) {
      exprList.set(i,
          rexBuilder.ensureType(type, exprList.get(i), false));
    }
    return rexBuilder.makeCall(type, SqlStdOperatorTable.CASE, exprList);
  }

  public RexNode convertMultiset(
      SqlRexContext cx,
      SqlMultisetValueConstructor op,
      SqlCall call) {
    final RelDataType originalType =
        cx.getValidator().getValidatedNodeType(call);
    RexRangeRef rr = cx.getSubQueryExpr(call);
    assert rr != null;
    RelDataType msType = rr.getType().getFieldList().get(0).getType();
    RexNode expr =
        cx.getRexBuilder().makeInputRef(
            msType,
            rr.getOffset());
    assert msType.getComponentType().isStruct();
    if (!originalType.getComponentType().isStruct()) {
      // If the type is not a struct, the multiset operator will have
      // wrapped the type as a record. Add a call to the $SLICE operator
      // to compensate. For example,
      // if '<ms>' has type 'RECORD (INTEGER x) MULTISET',
      // then '$SLICE(<ms>) has type 'INTEGER MULTISET'.
      // This will be removed as the expression is translated.
      expr =
          cx.getRexBuilder().makeCall(originalType, SqlStdOperatorTable.SLICE,
              ImmutableList.of(expr));
    }
    return expr;
  }

  public RexNode convertArray(
      SqlRexContext cx,
      SqlArrayValueConstructor op,
      SqlCall call) {
    return convertCall(cx, call);
  }

  public RexNode convertMap(
      SqlRexContext cx,
      SqlMapValueConstructor op,
      SqlCall call) {
    return convertCall(cx, call);
  }

  public RexNode convertMultisetQuery(
      SqlRexContext cx,
      SqlMultisetQueryConstructor op,
      SqlCall call) {
    final RelDataType originalType =
        cx.getValidator().getValidatedNodeType(call);
    RexRangeRef rr = cx.getSubQueryExpr(call);
    assert rr != null;
    RelDataType msType = rr.getType().getFieldList().get(0).getType();
    RexNode expr =
        cx.getRexBuilder().makeInputRef(
            msType,
            rr.getOffset());
    assert msType.getComponentType().isStruct();
    if (!originalType.getComponentType().isStruct()) {
      // If the type is not a struct, the multiset operator will have
      // wrapped the type as a record. Add a call to the $SLICE operator
      // to compensate. For example,
      // if '<ms>' has type 'RECORD (INTEGER x) MULTISET',
      // then '$SLICE(<ms>) has type 'INTEGER MULTISET'.
      // This will be removed as the expression is translated.
      expr =
          cx.getRexBuilder().makeCall(SqlStdOperatorTable.SLICE, expr);
    }
    return expr;
  }

  public RexNode convertJdbc(
      SqlRexContext cx,
      SqlJdbcFunctionCall op,
      SqlCall call) {
    // Yuck!! The function definition contains arguments!
    // TODO: adopt a more conventional definition/instance structure
    final SqlCall convertedCall = op.getLookupCall();
    return cx.convertExpression(convertedCall);
  }

  protected RexNode convertCast(
      SqlRexContext cx,
      final SqlCall call) {
    RelDataTypeFactory typeFactory = cx.getTypeFactory();
    assert call.getKind() == SqlKind.CAST;
    final SqlNode left = call.operand(0);
    final SqlNode right = call.operand(1);
    if (right instanceof SqlIntervalQualifier) {
      final SqlIntervalQualifier intervalQualifier =
          (SqlIntervalQualifier) right;
      if (left instanceof SqlIntervalLiteral) {
        RexLiteral sourceInterval =
            (RexLiteral) cx.convertExpression(left);
        BigDecimal sourceValue =
            (BigDecimal) sourceInterval.getValue();
        RexLiteral castedInterval =
            cx.getRexBuilder().makeIntervalLiteral(sourceValue,
                intervalQualifier);
        return castToValidatedType(cx, call, castedInterval);
      } else if (left instanceof SqlNumericLiteral) {
        RexLiteral sourceInterval =
            (RexLiteral) cx.convertExpression(left);
        BigDecimal sourceValue =
            (BigDecimal) sourceInterval.getValue();
        final BigDecimal multiplier = intervalQualifier.getUnit().multiplier;
        sourceValue = sourceValue.multiply(multiplier);
        RexLiteral castedInterval =
            cx.getRexBuilder().makeIntervalLiteral(
                sourceValue,
                intervalQualifier);
        return castToValidatedType(cx, call, castedInterval);
      }
      return castToValidatedType(cx, call, cx.convertExpression(left));
    }
    SqlDataTypeSpec dataType = (SqlDataTypeSpec) right;
    if (SqlUtil.isNullLiteral(left, false)) {
      return cx.convertExpression(left);
    }
    RexNode arg = cx.convertExpression(left);
    RelDataType type = dataType.deriveType(typeFactory);
    if (arg.getType().isNullable()) {
      type = typeFactory.createTypeWithNullability(type, true);
    }
    if (null != dataType.getCollectionsTypeName()) {
      final RelDataType argComponentType =
          arg.getType().getComponentType();
      final RelDataType componentType = type.getComponentType();
      if (argComponentType.isStruct()
          && !componentType.isStruct()) {
        RelDataType tt =
            typeFactory.builder()
                .add(
                    argComponentType.getFieldList().get(0).getName(),
                    componentType)
                .build();
        tt = typeFactory.createTypeWithNullability(
            tt,
            componentType.isNullable());
        boolean isn = type.isNullable();
        type = typeFactory.createMultisetType(tt, -1);
        type = typeFactory.createTypeWithNullability(type, isn);
      }
    }
    return cx.getRexBuilder().makeCast(type, arg);
  }

  protected RexNode convertFloorCeil(SqlRexContext cx, SqlCall call) {
    final boolean floor = call.getKind() == SqlKind.FLOOR;
    // Rewrite floor, ceil of interval
    if (call.operandCount() == 1
        && call.operand(0) instanceof SqlIntervalLiteral) {
      final SqlIntervalLiteral literal = call.operand(0);
      SqlIntervalLiteral.IntervalValue interval =
          (SqlIntervalLiteral.IntervalValue) literal.getValue();
      BigDecimal val =
          interval.getIntervalQualifier().getStartUnit().multiplier;
      RexNode rexInterval = cx.convertExpression(literal);

      RexNode res;

      final RexBuilder rexBuilder = cx.getRexBuilder();
      RexNode zero = rexBuilder.makeExactLiteral(BigDecimal.valueOf(0));
      RexNode cond =
          rexBuilder.makeCall(
              SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
              rexInterval,
              zero);

      RexNode pad =
          rexBuilder.makeExactLiteral(val.subtract(BigDecimal.ONE));
      RexNode cast = rexBuilder.makeReinterpretCast(
          rexInterval.getType(), pad, rexBuilder.makeLiteral(false));
      SqlOperator op =
          floor ? SqlStdOperatorTable.MINUS
              : SqlStdOperatorTable.PLUS;
      RexNode sum = rexBuilder.makeCall(op, rexInterval, cast);

      RexNode kase =
          floor
          ? rexBuilder.makeCall(SqlStdOperatorTable.CASE,
              cond, rexInterval, sum)
          : rexBuilder.makeCall(SqlStdOperatorTable.CASE,
              cond, sum, rexInterval);

      RexNode factor = rexBuilder.makeExactLiteral(val);
      RexNode div =
          rexBuilder.makeCall(
              SqlStdOperatorTable.DIVIDE_INTEGER,
              kase,
              factor);
      RexNode mult =
          rexBuilder.makeCall(
              SqlStdOperatorTable.MULTIPLY,
              div,
              factor);
      res = mult;
      return res;
    }

    // normal floor, ceil function
    return convertFunction(cx, (SqlFunction) call.getOperator(), call);
  }

  /**
   * Converts a call to the {@code EXTRACT} function.
   *
   * <p>Called automatically via reflection.
   */
  public RexNode convertExtract(
      SqlRexContext cx,
      SqlExtractFunction op,
      SqlCall call) {
    final RexBuilder rexBuilder = cx.getRexBuilder();
    final List<SqlNode> operands = call.getOperandList();
    final List<RexNode> exprs = convertExpressionList(cx, operands,
        SqlOperandTypeChecker.Consistency.NONE);

    // TODO: Will need to use decimal type for seconds with precision
    RelDataType resType =
        cx.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
    resType =
        cx.getTypeFactory().createTypeWithNullability(
            resType,
            exprs.get(1).getType().isNullable());
    RexNode res = rexBuilder.makeReinterpretCast(
        resType, exprs.get(1), rexBuilder.makeLiteral(false));

    final TimeUnit unit =
        ((SqlIntervalQualifier) operands.get(0)).getStartUnit();
    final SqlTypeName sqlTypeName = exprs.get(1).getType().getSqlTypeName();
    switch (unit) {
    case YEAR:
    case MONTH:
    case DAY:
      switch (sqlTypeName) {
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
        break;
      case TIMESTAMP:
        res = divide(rexBuilder, res, TimeUnit.DAY.multiplier);
        // fall through
      case DATE:
        return rexBuilder.makeCall(resType, SqlStdOperatorTable.EXTRACT_DATE,
            ImmutableList.of(exprs.get(0), res));
      default:
        throw new AssertionError("unexpected " + sqlTypeName);
      }
      break;
    case MILLENNIUM:
    case CENTURY:
    case DECADE:
      switch (sqlTypeName) {
      case TIMESTAMP:
        res = divide(rexBuilder, res, TimeUnit.DAY.multiplier);
        // fall through
      case DATE:
        res = rexBuilder.makeCall(resType, SqlStdOperatorTable.EXTRACT_DATE,
            ImmutableList.of(rexBuilder.makeFlag(TimeUnitRange.YEAR), res));
        return divide(rexBuilder, res, unit.multiplier.divide(TimeUnit.YEAR.multiplier));
      }
      break;
    case QUARTER:
      switch (sqlTypeName) {
      case TIMESTAMP:
        res = divide(rexBuilder, res, TimeUnit.DAY.multiplier);
        // fall through
      case DATE:
        res = rexBuilder.makeCall(resType, SqlStdOperatorTable.EXTRACT_DATE,
            ImmutableList.of(rexBuilder.makeFlag(TimeUnitRange.MONTH), res));
        res = rexBuilder.makeCall(SqlStdOperatorTable.MINUS, res,
            rexBuilder.makeExactLiteral(BigDecimal.ONE));
        res = divide(rexBuilder, res, unit.multiplier);
        return rexBuilder.makeCall(SqlStdOperatorTable.PLUS, res,
            rexBuilder.makeExactLiteral(BigDecimal.ONE));
      }
      break;
    case EPOCH:
      switch (sqlTypeName) {
      case DATE:
        // convert to milliseconds
        res = rexBuilder.makeCall(resType, SqlStdOperatorTable.MULTIPLY,
            ImmutableList.of(res, rexBuilder.makeExactLiteral(TimeUnit.DAY.multiplier)));
        // fall through
      case TIMESTAMP:
        // convert to seconds
        return divide(rexBuilder, res, TimeUnit.SECOND.multiplier);
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
        // no convertlet conversion, pass it as extract
        return convertFunction(cx, (SqlFunction) call.getOperator(), call);
      }
      break;
    case DOW:
    case DOY:
    case WEEK:
      switch (sqlTypeName) {
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
        // TODO: is this check better to do in validation phase?
        // Currently there is parameter on TimeUnit to identify these type of units.
        throw new IllegalArgumentException("Extract " + unit + " from "
            + sqlTypeName + " type data is not supported");
      case TIMESTAMP: // fall through
      case DATE:
        // no convertlet conversion, pass it as extract
        return convertFunction(cx, (SqlFunction) call.getOperator(), call);
      }
    }

    res = mod(rexBuilder, resType, res, getFactor(unit));
    if (unit == TimeUnit.QUARTER) {
      res = rexBuilder.makeCall(SqlStdOperatorTable.MINUS, res,
          rexBuilder.makeExactLiteral(BigDecimal.ONE));
    }
    res = divide(rexBuilder, res, unit.multiplier);
    if (unit == TimeUnit.QUARTER) {
      res = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, res,
          rexBuilder.makeExactLiteral(BigDecimal.ONE));
    }
    return res;
  }

  private static BigDecimal getFactor(TimeUnit unit) {
    switch (unit) {
    case DAY:
      return BigDecimal.ONE;
    case HOUR:
      return TimeUnit.DAY.multiplier;
    case MINUTE:
      return TimeUnit.HOUR.multiplier;
    case SECOND:
      return TimeUnit.MINUTE.multiplier;
    case MONTH:
      return TimeUnit.YEAR.multiplier;
    case QUARTER:
      return TimeUnit.YEAR.multiplier;
    case YEAR:
    case DECADE:
    case CENTURY:
    case MILLENNIUM:
      return BigDecimal.ONE;
    default:
      throw Util.unexpected(unit);
    }
  }

  private RexNode mod(RexBuilder rexBuilder, RelDataType resType, RexNode res,
      BigDecimal val) {
    if (val.equals(BigDecimal.ONE)) {
      return res;
    }
    return rexBuilder.makeCall(SqlStdOperatorTable.MOD, res,
        rexBuilder.makeExactLiteral(val, resType));
  }

  private static RexNode divide(RexBuilder rexBuilder, RexNode res,
      BigDecimal val) {
    if (val.equals(BigDecimal.ONE)) {
      return res;
    }
    // If val is between 0 and 1, rather than divide by val, multiply by its
    // reciprocal. For example, rather than divide by 0.001 multiply by 1000.
    if (val.compareTo(BigDecimal.ONE) < 0
        && val.signum() == 1) {
      try {
        final BigDecimal reciprocal =
            BigDecimal.ONE.divide(val, RoundingMode.UNNECESSARY);
        return rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, res,
            rexBuilder.makeExactLiteral(reciprocal));
      } catch (ArithmeticException e) {
        // ignore - reciprocal is not an integer
      }
    }
    return rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE_INTEGER, res,
        rexBuilder.makeExactLiteral(val));
  }

  public RexNode convertDatetimeMinus(
      SqlRexContext cx,
      SqlDatetimeSubtractionOperator op,
      SqlCall call) {
    // Rewrite datetime minus
    final RexBuilder rexBuilder = cx.getRexBuilder();
    final List<SqlNode> operands = call.getOperandList();
    final List<RexNode> exprs = convertExpressionList(cx, operands,
        SqlOperandTypeChecker.Consistency.NONE);

    RelDataType int8Type =
        cx.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
    final RexNode[] casts = new RexNode[2];
    casts[0] =
        rexBuilder.makeCast(
            cx.getTypeFactory().createTypeWithNullability(
                int8Type,
                exprs.get(0).getType().isNullable()),
            exprs.get(0));
    casts[1] =
        rexBuilder.makeCast(
            cx.getTypeFactory().createTypeWithNullability(
                int8Type,
                exprs.get(1).getType().isNullable()),
            exprs.get(1));
    final RelDataType resType =
        cx.getValidator().getValidatedNodeType(call);
    return rexBuilder.makeCall(resType, op, exprs.subList(0, 2));
  }

  public RexNode convertFunction(
      SqlRexContext cx,
      SqlFunction fun,
      SqlCall call) {
    final List<SqlNode> operands = call.getOperandList();
    final List<RexNode> exprs = convertExpressionList(cx, operands,
        SqlOperandTypeChecker.Consistency.NONE);
    if (fun.getFunctionType() == SqlFunctionCategory.USER_DEFINED_CONSTRUCTOR) {
      return makeConstructorCall(cx, fun, exprs);
    }
    RelDataType returnType =
        cx.getValidator().getValidatedNodeTypeIfKnown(call);
    if (returnType == null) {
      returnType = cx.getRexBuilder().deriveReturnType(fun, exprs);
    }
    return cx.getRexBuilder().makeCall(returnType, fun, exprs);
  }

  public RexNode convertSequenceValue(
      SqlRexContext cx,
      SqlSequenceValueOperator fun,
      SqlCall call) {
    final List<SqlNode> operands = call.getOperandList();
    assert operands.size() == 1;
    assert operands.get(0) instanceof SqlIdentifier;
    final SqlIdentifier id = (SqlIdentifier) operands.get(0);
    final String key = Util.listToString(id.names);
    RelDataType returnType =
        cx.getValidator().getValidatedNodeType(call);
    return cx.getRexBuilder().makeCall(returnType, fun,
        ImmutableList.<RexNode>of(cx.getRexBuilder().makeLiteral(key)));
  }

  public RexNode convertAggregateFunction(
      SqlRexContext cx,
      SqlAggFunction fun,
      SqlCall call) {
    final List<SqlNode> operands = call.getOperandList();
    final List<RexNode> exprs;
    if (call.isCountStar()) {
      exprs = ImmutableList.of();
    } else {
      exprs = convertExpressionList(cx, operands,
          SqlOperandTypeChecker.Consistency.NONE);
    }
    RelDataType returnType =
        cx.getValidator().getValidatedNodeTypeIfKnown(call);
    final int groupCount = cx.getGroupCount();
    if (returnType == null) {
      RexCallBinding binding =
          new RexCallBinding(cx.getTypeFactory(), fun, exprs, ImmutableList.<RelCollation>of()) {
            @Override public int getGroupCount() {
              return groupCount;
            }
          };
      returnType = fun.inferReturnType(binding);
    }
    return cx.getRexBuilder().makeCall(returnType, fun, exprs);
  }

  private static RexNode makeConstructorCall(
      SqlRexContext cx,
      SqlFunction constructor,
      List<RexNode> exprs) {
    final RexBuilder rexBuilder = cx.getRexBuilder();
    RelDataType type = rexBuilder.deriveReturnType(constructor, exprs);

    int n = type.getFieldCount();
    ImmutableList.Builder<RexNode> initializationExprs =
        ImmutableList.builder();
    for (int i = 0; i < n; ++i) {
      initializationExprs.add(
          cx.getDefaultValueFactory().newAttributeInitializer(
              type, constructor, i, exprs));
    }

    List<RexNode> defaultCasts =
        RexUtil.generateCastExpressions(
            rexBuilder,
            type,
            initializationExprs.build());

    return rexBuilder.makeNewInvocation(type, defaultCasts);
  }

  /**
   * Converts a call to an operator into a {@link RexCall} to the same
   * operator.
   *
   * <p>Called automatically via reflection.
   *
   * @param cx   Context
   * @param call Call
   * @return Rex call
   */
  public RexNode convertCall(
      SqlRexContext cx,
      SqlCall call) {
    return convertCall(cx, call, call.getOperator());
  }

  /** Converts a {@link SqlCall} to a {@link RexCall} with a perhaps different
   * operator. */
  private RexNode convertCall(
      SqlRexContext cx,
      SqlCall call,
      SqlOperator op) {
    final List<SqlNode> operands = call.getOperandList();
    final RexBuilder rexBuilder = cx.getRexBuilder();
    final SqlOperandTypeChecker.Consistency consistency =
        op.getOperandTypeChecker() == null
            ? SqlOperandTypeChecker.Consistency.NONE
            : op.getOperandTypeChecker().getConsistency();
    final List<RexNode> exprs =
        convertExpressionList(cx, operands, consistency);
    RelDataType type = rexBuilder.deriveReturnType(op, exprs);
    return rexBuilder.makeCall(type, op, RexUtil.flatten(exprs, op));
  }

  private List<Integer> elseArgs(int count) {
    // If list is odd, e.g. [0, 1, 2, 3, 4] we get [1, 3, 4]
    // If list is even, e.g. [0, 1, 2, 3, 4, 5] we get [2, 4, 5]
    final List<Integer> list = new ArrayList<>();
    for (int i = count % 2;;) {
      list.add(i);
      i += 2;
      if (i >= count) {
        list.add(i - 1);
        break;
      }
    }
    return list;
  }

  private static List<RexNode> convertExpressionList(SqlRexContext cx,
      List<SqlNode> nodes, SqlOperandTypeChecker.Consistency consistency) {
    final List<RexNode> exprs = Lists.newArrayList();
    for (SqlNode node : nodes) {
      exprs.add(cx.convertExpression(node));
    }
    if (exprs.size() > 1) {
      final RelDataType type =
          consistentType(cx, consistency, RexUtil.types(exprs));
      if (type != null) {
        final List<RexNode> oldExprs = Lists.newArrayList(exprs);
        exprs.clear();
        for (RexNode expr : oldExprs) {
          exprs.add(cx.getRexBuilder().ensureType(type, expr, true));
        }
      }
    }
    return exprs;
  }

  private static RelDataType consistentType(SqlRexContext cx,
      SqlOperandTypeChecker.Consistency consistency, List<RelDataType> types) {
    switch (consistency) {
    case COMPARE:
      final Set<RelDataTypeFamily> families =
          Sets.newHashSet(RexUtil.families(types));
      if (families.size() < 2) {
        // All arguments are of same family. No need for explicit casts.
        return null;
      }
      final List<RelDataType> nonCharacterTypes = Lists.newArrayList();
      for (RelDataType type : types) {
        if (type.getFamily() != SqlTypeFamily.CHARACTER) {
          nonCharacterTypes.add(type);
        }
      }
      if (!nonCharacterTypes.isEmpty()) {
        final int typeCount = types.size();
        types = nonCharacterTypes;
        if (nonCharacterTypes.size() < typeCount) {
          final RelDataTypeFamily family =
              nonCharacterTypes.get(0).getFamily();
          if (family instanceof SqlTypeFamily) {
            // The character arguments might be larger than the numeric
            // argument. Give ourselves some headroom.
            switch ((SqlTypeFamily) family) {
            case INTEGER:
            case NUMERIC:
              nonCharacterTypes.add(
                  cx.getTypeFactory().createSqlType(SqlTypeName.BIGINT));
            }
          }
        }
      }
      // fall through
    case LEAST_RESTRICTIVE:
      return cx.getTypeFactory().leastRestrictive(types);
    default:
      return null;
    }
  }

  private RexNode convertPlus(SqlRexContext cx, SqlCall call) {
    final RexNode rex = convertCall(cx, call);
    switch (rex.getType().getSqlTypeName()) {
    case DATE:
    case TIME:
    case TIMESTAMP:
      // Use special "+" operator for datetime + interval.
      // Re-order operands, if necessary, so that interval is second.
      final RexBuilder rexBuilder = cx.getRexBuilder();
      List<RexNode> operands = ((RexCall) rex).getOperands();
      if (operands.size() == 2) {
        final SqlTypeName sqlTypeName = operands.get(0).getType().getSqlTypeName();
        switch (sqlTypeName) {
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
          operands = ImmutableList.of(operands.get(1), operands.get(0));
        }
      }
      return rexBuilder.makeCall(rex.getType(),
          SqlStdOperatorTable.DATETIME_PLUS, operands);
    default:
      return rex;
    }
  }

  private RexNode convertIsDistinctFrom(
      SqlRexContext cx,
      SqlCall call,
      boolean neg) {
    RexNode op0 = cx.convertExpression(call.operand(0));
    RexNode op1 = cx.convertExpression(call.operand(1));
    return RelOptUtil.isDistinctFrom(
        cx.getRexBuilder(), op0, op1, neg);
  }

  /**
   * Converts a BETWEEN expression.
   *
   * <p>Called automatically via reflection.
   */
  public RexNode convertBetween(
      SqlRexContext cx,
      SqlBetweenOperator op,
      SqlCall call) {
    final List<RexNode> list =
        convertExpressionList(cx, call.getOperandList(),
            op.getOperandTypeChecker().getConsistency());
    final RexNode x = list.get(SqlBetweenOperator.VALUE_OPERAND);
    final RexNode y = list.get(SqlBetweenOperator.LOWER_OPERAND);
    final RexNode z = list.get(SqlBetweenOperator.UPPER_OPERAND);

    final RexBuilder rexBuilder = cx.getRexBuilder();
    RexNode ge1 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, x, y);
    RexNode le1 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            x,
            z);
    RexNode and1 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            ge1,
            le1);

    RexNode res;
    final SqlBetweenOperator.Flag symmetric = op.flag;
    switch (symmetric) {
    case ASYMMETRIC:
      res = and1;
      break;
    case SYMMETRIC:
      RexNode ge2 =
          rexBuilder.makeCall(
              SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
              x,
              z);
      RexNode le2 =
          rexBuilder.makeCall(
              SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
              x,
              y);
      RexNode and2 =
          rexBuilder.makeCall(
              SqlStdOperatorTable.AND,
              ge2,
              le2);
      res =
          rexBuilder.makeCall(
              SqlStdOperatorTable.OR,
              and1,
              and2);
      break;
    default:
      throw Util.unexpected(symmetric);
    }
    final SqlBetweenOperator betweenOp =
        (SqlBetweenOperator) call.getOperator();
    if (betweenOp.isNegated()) {
      res = rexBuilder.makeCall(SqlStdOperatorTable.NOT, res);
    }
    return res;
  }

  /**
   * Converts a LiteralChain expression: that is, concatenates the operands
   * immediately, to produce a single literal string.
   *
   * <p>Called automatically via reflection.
   */
  public RexNode convertLiteralChain(
      SqlRexContext cx,
      SqlLiteralChainOperator op,
      SqlCall call) {
    Util.discard(cx);

    SqlLiteral sum = SqlLiteralChainOperator.concatenateOperands(call);
    return cx.convertLiteral(sum);
  }

  /**
   * Converts a ROW.
   *
   * <p>Called automatically via reflection.
   */
  public RexNode convertRow(
      SqlRexContext cx,
      SqlRowOperator op,
      SqlCall call) {
    if (cx.getValidator().getValidatedNodeType(call).getSqlTypeName()
        != SqlTypeName.COLUMN_LIST) {
      return convertCall(cx, call);
    }
    final RexBuilder rexBuilder = cx.getRexBuilder();
    final List<RexNode> columns = new ArrayList<>();
    for (SqlNode operand : call.getOperandList()) {
      columns.add(
          rexBuilder.makeLiteral(
              ((SqlIdentifier) operand).getSimple()));
    }
    final RelDataType type =
        rexBuilder.deriveReturnType(SqlStdOperatorTable.COLUMN_LIST, columns);
    return rexBuilder.makeCall(type, SqlStdOperatorTable.COLUMN_LIST, columns);
  }

  /**
   * Converts a call to OVERLAPS.
   *
   * <p>Called automatically via reflection.
   */
  public RexNode convertOverlaps(
      SqlRexContext cx,
      SqlOverlapsOperator op,
      SqlCall call) {
    // for intervals [t0, t1] overlaps [t2, t3], we can find if the
    // intervals overlaps by: ~(t1 < t2 or t3 < t0)
    final SqlNode[] operands = ((SqlBasicCall) call).getOperands();
    assert operands.length == 4;
    if (operands[1] instanceof SqlIntervalLiteral) {
      // make t1 = t0 + t1 when t1 is an interval.
      SqlOperator op1 = SqlStdOperatorTable.PLUS;
      SqlNode[] second = new SqlNode[2];
      second[0] = operands[0];
      second[1] = operands[1];
      operands[1] =
          op1.createCall(
              call.getParserPosition(),
              second);
    }
    if (operands[3] instanceof SqlIntervalLiteral) {
      // make t3 = t2 + t3 when t3 is an interval.
      SqlOperator op1 = SqlStdOperatorTable.PLUS;
      SqlNode[] four = new SqlNode[2];
      four[0] = operands[2];
      four[1] = operands[3];
      operands[3] =
          op1.createCall(
              call.getParserPosition(),
              four);
    }

    // This captures t1 >= t2
    SqlOperator op1 = SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
    SqlNode[] left = new SqlNode[2];
    left[0] = operands[1];
    left[1] = operands[2];
    SqlCall call1 =
        op1.createCall(
            call.getParserPosition(),
            left);

    // This captures t3 >= t0
    SqlOperator op2 = SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
    SqlNode[] right = new SqlNode[2];
    right[0] = operands[3];
    right[1] = operands[0];
    SqlCall call2 =
        op2.createCall(
            call.getParserPosition(),
            right);

    // This captures t1 >= t2 and t3 >= t0
    SqlOperator and = SqlStdOperatorTable.AND;
    SqlNode[] overlaps = new SqlNode[2];
    overlaps[0] = call1;
    overlaps[1] = call2;
    SqlCall call3 =
        and.createCall(
            call.getParserPosition(),
            overlaps);

    return cx.convertExpression(call3);
  }

  /**
   * Casts a RexNode value to the validated type of a SqlCall. If the value
   * was already of the validated type, then the value is returned without an
   * additional cast.
   */
  public RexNode castToValidatedType(
      SqlRexContext cx,
      SqlCall call,
      RexNode value) {
    return castToValidatedType(call, value, cx.getValidator(),
        cx.getRexBuilder());
  }

  /**
   * Casts a RexNode value to the validated type of a SqlCall. If the value
   * was already of the validated type, then the value is returned without an
   * additional cast.
   */
  public static RexNode castToValidatedType(SqlNode node, RexNode e,
      SqlValidator validator, RexBuilder rexBuilder) {
    final RelDataType type = validator.getValidatedNodeType(node);
    if (e.getType() == type) {
      return e;
    }
    return rexBuilder.makeCast(type, e);
  }

  /** Convertlet that handles {@code AVG} and {@code VARIANCE}
   * windowed aggregate functions. */
  private static class AvgVarianceConvertlet implements SqlRexConvertlet {
    private final SqlKind kind;

    public AvgVarianceConvertlet(SqlKind kind) {
      this.kind = kind;
    }

    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      assert call.operandCount() == 1;
      final SqlNode arg = call.operand(0);
      final SqlNode expr;
      switch (kind) {
      case AVG:
        expr = expandAvg(arg);
        break;
      case STDDEV_POP:
        expr = expandVariance(arg, true, true);
        break;
      case STDDEV_SAMP:
        expr = expandVariance(arg, false, true);
        break;
      case VAR_POP:
        expr = expandVariance(arg, true, false);
        break;
      case VAR_SAMP:
        expr = expandVariance(arg, false, false);
        break;
      default:
        throw Util.unexpected(kind);
      }
      RelDataType type =
          cx.getValidator().getValidatedNodeType(call);
      RexNode rex = cx.convertExpression(expr);
      return cx.getRexBuilder().ensureType(type, rex, true);
    }

    private SqlNode expandAvg(
        final SqlNode arg) {
      final SqlParserPos pos = SqlParserPos.ZERO;
      final SqlNode sum =
          SqlStdOperatorTable.SUM.createCall(pos, arg);
      final SqlNode count =
          SqlStdOperatorTable.COUNT.createCall(pos, arg);
      return SqlStdOperatorTable.DIVIDE.createCall(
          pos, sum, count);
    }

    private SqlNode expandVariance(
        final SqlNode arg,
        boolean biased,
        boolean sqrt) {
      // stddev_pop(x) ==>
      //   power(
      //     (sum(x * x) - sum(x) * sum(x) / count(x))
      //     / count(x),
      //     .5)
      //
      // stddev_samp(x) ==>
      //   power(
      //     (sum(x * x) - sum(x) * sum(x) / count(x))
      //     / (count(x) - 1),
      //     .5)
      //
      // var_pop(x) ==>
      //     (sum(x * x) - sum(x) * sum(x) / count(x))
      //     / count(x)
      //
      // var_samp(x) ==>
      //     (sum(x * x) - sum(x) * sum(x) / count(x))
      //     / (count(x) - 1)
      final SqlParserPos pos = SqlParserPos.ZERO;
      final SqlNode argSquared =
          SqlStdOperatorTable.MULTIPLY.createCall(pos, arg, arg);
      final SqlNode sumArgSquared =
          SqlStdOperatorTable.SUM.createCall(pos, argSquared);
      final SqlNode sum =
          SqlStdOperatorTable.SUM.createCall(pos, arg);
      final SqlNode sumSquared =
          SqlStdOperatorTable.MULTIPLY.createCall(pos, sum, sum);
      final SqlNode count =
          SqlStdOperatorTable.COUNT.createCall(pos, arg);
      final SqlNode avgSumSquared =
          SqlStdOperatorTable.DIVIDE.createCall(
              pos, sumSquared, count);
      final SqlNode diff =
          SqlStdOperatorTable.MINUS.createCall(
              pos, sumArgSquared, avgSumSquared);
      final SqlNode denominator;
      if (biased) {
        denominator = count;
      } else {
        final SqlNumericLiteral one =
            SqlLiteral.createExactNumeric("1", pos);
        denominator =
            SqlStdOperatorTable.MINUS.createCall(
                pos, count, one);
      }
      final SqlNode div =
          SqlStdOperatorTable.DIVIDE.createCall(
              pos, diff, denominator);
      SqlNode result = div;
      if (sqrt) {
        final SqlNumericLiteral half =
            SqlLiteral.createExactNumeric("0.5", pos);
        result =
            SqlStdOperatorTable.POWER.createCall(pos, div, half);
      }
      return result;
    }
  }

  /** Convertlet that converts {@code LTRIM} and {@code RTRIM} to
   * {@code TRIM}. */
  private static class TrimConvertlet implements SqlRexConvertlet {
    private final SqlTrimFunction.Flag flag;

    public TrimConvertlet(SqlTrimFunction.Flag flag) {
      this.flag = flag;
    }

    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      final RexBuilder rexBuilder = cx.getRexBuilder();
      final RexNode operand =
          cx.convertExpression(call.getOperandList().get(0));
      return rexBuilder.makeCall(SqlStdOperatorTable.TRIM,
          rexBuilder.makeFlag(flag), rexBuilder.makeLiteral(" "), operand);
    }
  }

  /** Convertlet that converts {@code GREATEST} and {@code LEAST}. */
  private static class GreatestConvertlet implements SqlRexConvertlet {
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      // Translate
      //   GREATEST(a, b, c, d)
      // to
      //   CASE
      //   WHEN a IS NULL OR b IS NULL OR c IS NULL OR d IS NULL
      //   THEN NULL
      //   WHEN a > b AND a > c AND a > d
      //   THEN a
      //   WHEN b > c AND b > d
      //   THEN b
      //   WHEN c > d
      //   THEN c
      //   ELSE d
      //   END
      final RexBuilder rexBuilder = cx.getRexBuilder();
      final RelDataType type =
          cx.getValidator().getValidatedNodeType(call);
      final SqlBinaryOperator op;
      switch (call.getKind()) {
      case GREATEST:
        op = SqlStdOperatorTable.GREATER_THAN;
        break;
      case LEAST:
        op = SqlStdOperatorTable.LESS_THAN;
        break;
      default:
        throw new AssertionError();
      }
      final List<RexNode> exprs = convertExpressionList(cx,
          call.getOperandList(), SqlOperandTypeChecker.Consistency.NONE);
      final List<RexNode> list = new ArrayList<>();
      final List<RexNode> orList = new ArrayList<>();
      for (RexNode expr : exprs) {
        orList.add(rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, expr));
      }
      list.add(RexUtil.composeDisjunction(rexBuilder, orList, false));
      list.add(rexBuilder.makeNullLiteral(type.getSqlTypeName()));
      for (int i = 0; i < exprs.size() - 1; i++) {
        RexNode expr = exprs.get(i);
        final List<RexNode> andList = new ArrayList<>();
        for (int j = i + 1; j < exprs.size(); j++) {
          final RexNode expr2 = exprs.get(j);
          andList.add(rexBuilder.makeCall(op, expr, expr2));
        }
        list.add(RexUtil.composeConjunction(rexBuilder, andList, false));
        list.add(expr);
      }
      list.add(exprs.get(exprs.size() - 1));
      return rexBuilder.makeCall(type, SqlStdOperatorTable.CASE, list);
    }
  }

  /** Convertlet that handles {@code FLOOR} and {@code CEIL} functions. */
  private class FloorCeilConvertlet implements SqlRexConvertlet {
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      return convertFloorCeil(cx, call);
    }
  }

  /** Convertlet that handles the {@code TIMESTAMPADD} function. */
  private static class TimestampAddConvertlet implements SqlRexConvertlet {
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      // TIMESTAMPADD(unit, count, timestamp)
      //  => timestamp + count * INTERVAL '1' UNIT
      final RexBuilder rexBuilder = cx.getRexBuilder();
      final SqlLiteral unitLiteral = call.operand(0);
      final TimeUnit unit = unitLiteral.symbolValue(TimeUnit.class);
      return rexBuilder.makeCall(SqlStdOperatorTable.DATETIME_PLUS,
          cx.convertExpression(call.operand(2)),
          rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY,
              rexBuilder.makeIntervalLiteral(unit.multiplier,
                  new SqlIntervalQualifier(unit, null,
                      unitLiteral.getParserPosition())),
              cx.convertExpression(call.operand(1))));
    }
  }

  /** Convertlet that handles the {@code TIMESTAMPDIFF} function. */
  private static class TimestampDiffConvertlet implements SqlRexConvertlet {
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      // TIMESTAMPDIFF(unit, t1, t2)
      //    => (t2 - t1) UNIT
      final RexBuilder rexBuilder = cx.getRexBuilder();
      final SqlLiteral unitLiteral = call.operand(0);
      TimeUnit unit = unitLiteral.symbolValue(TimeUnit.class);
      BigDecimal multiplier = BigDecimal.ONE;
      BigDecimal divider = BigDecimal.ONE;
      switch (unit) {
      case MICROSECOND:
      case MILLISECOND:
      case WEEK:
        multiplier = BigDecimal.valueOf(DateTimeUtils.MILLIS_PER_SECOND);
        divider = unit.multiplier;
        unit = TimeUnit.SECOND;
        break;
      case QUARTER:
        divider = unit.multiplier;
        unit = TimeUnit.MONTH;
        break;
      }
      final SqlIntervalQualifier qualifier =
          new SqlIntervalQualifier(unit, null, SqlParserPos.ZERO);
      final RexNode op2 = cx.convertExpression(call.operand(2));
      final RexNode op1 = cx.convertExpression(call.operand(1));
      final RelDataType intervalType =
          cx.getTypeFactory().createTypeWithNullability(
              cx.getTypeFactory().createSqlIntervalType(qualifier),
              op1.getType().isNullable() || op2.getType().isNullable());
      final RexCall rexCall = (RexCall) rexBuilder.makeCall(
          intervalType, SqlStdOperatorTable.MINUS_DATE,
          ImmutableList.of(op2, op1));
      final RelDataType intType =
          cx.getTypeFactory().createTypeWithNullability(
              cx.getTypeFactory().createSqlType(SqlTypeName.INTEGER),
              SqlTypeUtil.containsNullable(rexCall.getType()));
      RexNode e = rexBuilder.makeCast(intType, rexCall);
      return rexBuilder.multiplyDivide(e, multiplier, divider);
    }
  }
}

// End StandardConvertletTable.java
