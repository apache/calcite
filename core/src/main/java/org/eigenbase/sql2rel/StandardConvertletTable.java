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
package org.eigenbase.sql2rel;

import java.math.*;
import java.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;
import org.eigenbase.util14.DateTimeUtil;

import com.google.common.collect.ImmutableList;

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
    // org.eigenbase.rel.rules.ReduceAggregatesRule which handles other
    // cases post-translation.  The reason I did that was to defer the
    // implementation decision; e.g. we may want to push it down to a
    // foreign server directly rather than decomposed; decomposition is
    // easier than recognition.

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
        new AvgVarianceConvertlet(SqlAvgAggFunction.Subtype.AVG));
    registerOp(
        SqlStdOperatorTable.STDDEV_POP,
        new AvgVarianceConvertlet(SqlAvgAggFunction.Subtype.STDDEV_POP));
    registerOp(
        SqlStdOperatorTable.STDDEV_SAMP,
        new AvgVarianceConvertlet(SqlAvgAggFunction.Subtype.STDDEV_SAMP));
    registerOp(
        SqlStdOperatorTable.VAR_POP,
        new AvgVarianceConvertlet(SqlAvgAggFunction.Subtype.VAR_POP));
    registerOp(
        SqlStdOperatorTable.VAR_SAMP,
        new AvgVarianceConvertlet(SqlAvgAggFunction.Subtype.VAR_SAMP));

    registerOp(
        SqlStdOperatorTable.FLOOR, new FloorCeilConvertlet(true));
    registerOp(
        SqlStdOperatorTable.CEIL, new FloorCeilConvertlet(false));

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

    final List<RexNode> exprList = new ArrayList<RexNode>();
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
    RexRangeRef rr = cx.getSubqueryExpr(call);
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
    RexRangeRef rr = cx.getSubqueryExpr(call);
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
      if (left instanceof SqlIntervalLiteral
          || left instanceof SqlNumericLiteral) {
        RexLiteral sourceInterval =
            (RexLiteral) cx.convertExpression(left);
        BigDecimal sourceValue =
            (BigDecimal) sourceInterval.getValue();
        RexLiteral castedInterval =
            cx.getRexBuilder().makeIntervalLiteral(
                sourceValue.multiply(
                    BigDecimal.valueOf(
                        intervalQualifier.getStartUnit().multiplier),
                    MathContext.UNLIMITED),
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

  protected RexNode convertFloorCeil(
      SqlRexContext cx,
      SqlCall call,
      boolean floor) {
    // Rewrite floor, ceil of interval
    if (call.operandCount() == 1
        && call.operand(0) instanceof SqlIntervalLiteral) {
      final SqlIntervalLiteral literal = call.operand(0);
      SqlIntervalLiteral.IntervalValue interval =
          (SqlIntervalLiteral.IntervalValue) literal.getValue();
      long val =
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
          rexBuilder.makeExactLiteral(BigDecimal.valueOf(val - 1));
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

      RexNode factor =
          rexBuilder.makeExactLiteral(BigDecimal.valueOf(val));
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

  public RexNode convertExtract(
      SqlRexContext cx,
      SqlExtractFunction op,
      SqlCall call) {
    final RexBuilder rexBuilder = cx.getRexBuilder();
    final List<SqlNode> operands = call.getOperandList();
    final List<RexNode> exprs = convertExpressionList(cx, operands);

    // TODO: Will need to use decimal type for seconds with precision
    RelDataType resType =
        cx.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
    resType =
        cx.getTypeFactory().createTypeWithNullability(
            resType,
            exprs.get(1).getType().isNullable());
    RexNode res = rexBuilder.makeReinterpretCast(
        resType, exprs.get(1), rexBuilder.makeLiteral(false));

    final SqlIntervalQualifier.TimeUnit unit =
        ((SqlIntervalQualifier) operands.get(0)).getStartUnit();
    final SqlTypeName sqlTypeName = exprs.get(1).getType().getSqlTypeName();
    switch (unit) {
    case YEAR:
    case MONTH:
    case DAY:
      switch (sqlTypeName) {
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_DAY_TIME:
        break;
      case TIMESTAMP:
        res = divide(rexBuilder, res, DateTimeUtil.MILLIS_PER_DAY);
        // fall through
      case DATE:
        return rexBuilder.makeCall(resType, SqlStdOperatorTable.EXTRACT_DATE,
            ImmutableList.of(exprs.get(0), res));
      default:
        throw new AssertionError("unexpected " + sqlTypeName);
      }
    }

    res = mod(rexBuilder, resType, res, getFactor(unit));
    res = divide(rexBuilder, res, unit.multiplier);
    return res;
  }

  private static long getFactor(SqlIntervalQualifier.TimeUnit unit) {
    switch (unit) {
    case DAY:
      return 1;
    case HOUR:
      return SqlIntervalQualifier.TimeUnit.DAY.multiplier;
    case MINUTE:
      return SqlIntervalQualifier.TimeUnit.HOUR.multiplier;
    case SECOND:
      return SqlIntervalQualifier.TimeUnit.MINUTE.multiplier;
    case YEAR:
      return 1;
    case MONTH:
      return SqlIntervalQualifier.TimeUnit.YEAR.multiplier;
    default:
      throw Util.unexpected(unit);
    }
  }

  private RexNode mod(RexBuilder rexBuilder, RelDataType resType, RexNode res,
      long val) {
    if (val == 1L) {
      return res;
    }
    return rexBuilder.makeCall(SqlStdOperatorTable.MOD, res,
        rexBuilder.makeExactLiteral(BigDecimal.valueOf(val), resType));
  }

  private RexNode divide(RexBuilder rexBuilder, RexNode res, long val) {
    if (val == 1L) {
      return res;
    }
    return rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE_INTEGER, res,
        rexBuilder.makeExactLiteral(BigDecimal.valueOf(val)));
  }

  public RexNode convertDatetimeMinus(
      SqlRexContext cx,
      SqlDatetimeSubtractionOperator op,
      SqlCall call) {
    // Rewrite datetime minus
    final RexBuilder rexBuilder = cx.getRexBuilder();
    final List<SqlNode> operands = call.getOperandList();
    final List<RexNode> exprs = convertExpressionList(cx, operands);

    // TODO: Handle year month interval (represented in months)
    for (RexNode expr : exprs) {
      if (SqlTypeName.INTERVAL_YEAR_MONTH
          == expr.getType().getSqlTypeName()) {
        Util.needToImplement(
            "Datetime subtraction of year month interval");
      }
    }
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
    final RexNode minus =
        rexBuilder.makeCall(
            SqlStdOperatorTable.MINUS,
            casts);
    final RelDataType resType =
        cx.getValidator().getValidatedNodeType(call);
    return rexBuilder.makeReinterpretCast(
        resType,
        minus,
        rexBuilder.makeLiteral(false));
  }

  public RexNode convertFunction(
      SqlRexContext cx,
      SqlFunction fun,
      SqlCall call) {
    final List<SqlNode> operands = call.getOperandList();
    final List<RexNode> exprs = convertExpressionList(cx, operands);
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

  public RexNode convertAggregateFunction(
      SqlRexContext cx,
      SqlAggFunction fun,
      SqlCall call) {
    final List<SqlNode> operands = call.getOperandList();
    final List<RexNode> exprs;
    if (call.isCountStar()) {
      exprs = ImmutableList.of();
    } else {
      exprs = convertExpressionList(cx, operands);
    }
    RelDataType returnType =
        cx.getValidator().getValidatedNodeTypeIfKnown(call);
    final int groupCount = cx.getGroupCount();
    if (returnType == null) {
      RexCallBinding binding =
          new RexCallBinding(cx.getTypeFactory(), fun, exprs) {
            @Override
            public int getGroupCount() {
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
    final List<RexNode> exprs = convertExpressionList(cx, operands);
    if (op.getOperandTypeChecker()
        == OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED) {
      ensureSameType(cx, exprs);
    }
    RelDataType type = rexBuilder.deriveReturnType(op, exprs);
    return rexBuilder.makeCall(type, op, RexUtil.flatten(exprs, op));
  }

  private List<Integer> elseArgs(int count) {
    // If list is odd, e.g. [0, 1, 2, 3, 4] we get [1, 3, 4]
    // If list is even, e.g. [0, 1, 2, 3, 4, 5] we get [2, 4, 5]
    List<Integer> list = new ArrayList<Integer>();
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

  private void ensureSameType(SqlRexContext cx, final List<RexNode> exprs) {
    RelDataType type =
        cx.getTypeFactory().leastRestrictive(
            new AbstractList<RelDataType>() {
              public RelDataType get(int index) {
                return exprs.get(index).getType();
              }

              public int size() {
                return exprs.size();
              }
            });
    for (int i = 0; i < exprs.size(); i++) {
      // REVIEW: assigning to a list that may be immutable?
      exprs.set(
          i, cx.getRexBuilder().ensureType(type, exprs.get(i), true));
    }
  }

  private static List<RexNode> convertExpressionList(
      SqlRexContext cx,
      List<SqlNode> nodes) {
    final ArrayList<RexNode> exprs = new ArrayList<RexNode>();
    for (SqlNode node : nodes) {
      exprs.add(cx.convertExpression(node));
    }
    return exprs;
  }

  private RexNode convertPlus(SqlRexContext cx, SqlCall call) {
    final RexNode rex = convertCall(cx, call);
    switch (rex.getType().getSqlTypeName()) {
    case DATE:
    case TIME:
    case TIMESTAMP:
      return convertCall(cx, call, SqlStdOperatorTable.DATETIME_PLUS);
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
    final SqlNode value = call.operand(SqlBetweenOperator.VALUE_OPERAND);
    RexNode x = cx.convertExpression(value);
    final SqlBetweenOperator.Flag symmetric = op.flag;
    final SqlNode lower = call.operand(SqlBetweenOperator.LOWER_OPERAND);
    RexNode y = cx.convertExpression(lower);
    final SqlNode upper = call.operand(SqlBetweenOperator.UPPER_OPERAND);
    RexNode z = cx.convertExpression(upper);

    final RexBuilder rexBuilder = cx.getRexBuilder();
    RexNode ge1 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            x,
            y);
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
    final List<RexNode> columns = new ArrayList<RexNode>();
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
    final RelDataType resType =
        cx.getValidator().getValidatedNodeType(call);
    if (value.getType() == resType) {
      return value;
    }
    return cx.getRexBuilder().makeCast(resType, value);
  }

  private static class AvgVarianceConvertlet implements SqlRexConvertlet {
    private final SqlAvgAggFunction.Subtype subtype;

    public AvgVarianceConvertlet(SqlAvgAggFunction.Subtype subtype) {
      this.subtype = subtype;
    }

    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      assert call.operandCount() == 1;
      final SqlNode arg = call.operand(0);
      final SqlNode expr;
      switch (subtype) {
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
        throw Util.unexpected(subtype);
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

  private class FloorCeilConvertlet implements SqlRexConvertlet {
    private final boolean floor;

    public FloorCeilConvertlet(boolean floor) {
      this.floor = floor;
    }

    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      return convertFloorCeil(cx, call, floor);
    }
  }
}

// End StandardConvertletTable.java
