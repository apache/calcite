/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.sql2rel;

import java.math.*;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * Standard implementation of {@link SqlRexConvertletTable}.
 */
public class StandardConvertletTable
    extends ReflectiveConvertletTable
{
    //~ Constructors -----------------------------------------------------------

    public StandardConvertletTable()
    {
        super();

        // Register aliases (operators which have a different name but
        // identical behavior to other operators).
        addAlias(
            SqlStdOperatorTable.characterLengthFunc,
            SqlStdOperatorTable.charLengthFunc);
        addAlias(
            SqlStdOperatorTable.isUnknownOperator,
            SqlStdOperatorTable.isNullOperator);
        addAlias(
            SqlStdOperatorTable.isNotUnknownOperator,
            SqlStdOperatorTable.isNotNullOperator);

        // Register convertlets for specific objects.
        registerOp(
            SqlStdOperatorTable.castFunc,
            new SqlRexConvertlet() {
                public RexNode convertCall(SqlRexContext cx, SqlCall call)
                {
                    return convertCast(cx, call);
                }
            });
        registerOp(
            SqlStdOperatorTable.isDistinctFromOperator,
            new SqlRexConvertlet() {
                public RexNode convertCall(SqlRexContext cx, SqlCall call)
                {
                    return convertIsDistinctFrom(cx, call, false);
                }
            });
        registerOp(
            SqlStdOperatorTable.isNotDistinctFromOperator,
            new SqlRexConvertlet() {
                public RexNode convertCall(SqlRexContext cx, SqlCall call)
                {
                    return convertIsDistinctFrom(cx, call, true);
                }
            });

        // Expand "x NOT LIKE y" into "NOT (x LIKE y)"
        registerOp(
            SqlStdOperatorTable.notLikeOperator,
            new SqlRexConvertlet() {
                public RexNode convertCall(SqlRexContext cx, SqlCall call)
                {
                    final SqlCall expanded =
                        SqlStdOperatorTable.notOperator.createCall(
                            SqlParserPos.ZERO,
                            SqlStdOperatorTable.likeOperator.createCall(
                                SqlParserPos.ZERO,
                                call.getOperands()));
                    return cx.convertExpression(expanded);
                }
            });

        // Expand "x NOT SIMILAR y" into "NOT (x SIMILAR y)"
        registerOp(
            SqlStdOperatorTable.notSimilarOperator,
            new SqlRexConvertlet() {
                public RexNode convertCall(SqlRexContext cx, SqlCall call)
                {
                    final SqlCall expanded =
                        SqlStdOperatorTable.notOperator.createCall(
                            SqlParserPos.ZERO,
                            SqlStdOperatorTable.similarOperator.createCall(
                                SqlParserPos.ZERO,
                                call.getOperands()));
                    return cx.convertExpression(expanded);
                }
            });

        // Unary "+" has no effect, so expand "+ x" into "x".
        registerOp(
            SqlStdOperatorTable.prefixPlusOperator,
            new SqlRexConvertlet() {
                public RexNode convertCall(SqlRexContext cx, SqlCall call)
                {
                    SqlNode expanded = call.getOperands()[0];
                    return cx.convertExpression(expanded);
                }
            });

        // "AS" has no effect, so expand "x AS id" into "x".
        registerOp(
            SqlStdOperatorTable.asOperator,
            new SqlRexConvertlet() {
                public RexNode convertCall(SqlRexContext cx, SqlCall call)
                {
                    SqlNode expanded = call.getOperands()[0];
                    return cx.convertExpression(expanded);
                }
            });

        // "SQRT(x)" is equivalent to "POWER(x, .5)"
        registerOp(
            SqlStdOperatorTable.sqrtFunc,
            new SqlRexConvertlet() {
                public RexNode convertCall(SqlRexContext cx, SqlCall call)
                {
                    SqlNode expanded =
                        SqlStdOperatorTable.powerFunc.createCall(
                            SqlParserPos.ZERO,
                            call.getOperands()[0],
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
            SqlStdOperatorTable.avgOperator,
            new AvgVarianceConvertlet(SqlAvgAggFunction.Subtype.AVG));
        registerOp(
            SqlStdOperatorTable.stddevPopOperator,
            new AvgVarianceConvertlet(SqlAvgAggFunction.Subtype.STDDEV_POP));
        registerOp(
            SqlStdOperatorTable.stddevSampOperator,
            new AvgVarianceConvertlet(SqlAvgAggFunction.Subtype.STDDEV_SAMP));
        registerOp(
            SqlStdOperatorTable.varPopOperator,
            new AvgVarianceConvertlet(SqlAvgAggFunction.Subtype.VAR_POP));
        registerOp(
            SqlStdOperatorTable.varSampOperator,
            new AvgVarianceConvertlet(SqlAvgAggFunction.Subtype.VAR_SAMP));

        registerOp(
            SqlStdOperatorTable.floorFunc, new FloorCeilConvertlet(true));
        registerOp(
            SqlStdOperatorTable.ceilFunc, new FloorCeilConvertlet(false));

        // Convert "element(<expr>)" to "$element_slice(<expr>)", if the
        // expression is a multiset of scalars.
        if (false) {
            registerOp(
                SqlStdOperatorTable.elementFunc,
                new SqlRexConvertlet() {
                    public RexNode convertCall(SqlRexContext cx, SqlCall call)
                    {
                        final SqlNode [] operands = call.getOperands();
                        Util.permAssert(
                            operands.length == 1,
                            "operands.length == 1");
                        final SqlNode operand = operands[0];
                        final RelDataType type =
                            cx.getValidator().getValidatedNodeType(operand);
                        if (!type.getComponentType().isStruct()) {
                            return cx.convertExpression(
                                SqlStdOperatorTable.elementSlicefunc.createCall(
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
                SqlStdOperatorTable.elementSlicefunc,
                new SqlRexConvertlet() {
                    public RexNode convertCall(SqlRexContext cx, SqlCall call)
                    {
                        final SqlNode [] operands = call.getOperands();
                        Util.permAssert(
                            operands.length == 1,
                            "operands.length == 1");
                        final SqlNode operand = operands[0];
                        final RexNode expr =
                            cx.convertExpression(
                                SqlStdOperatorTable.elementFunc.createCall(
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
        SqlCase call)
    {
        SqlNodeList whenList = call.getWhenOperands();
        SqlNodeList thenList = call.getThenOperands();
        assert (whenList.size() == thenList.size());

        final List<RexNode> exprList = new ArrayList<RexNode>();
        for (int i = 0; i < whenList.size(); i++) {
            exprList.add(cx.convertExpression(whenList.get(i)));
            exprList.add(cx.convertExpression(thenList.get(i)));
        }
        exprList.add(cx.convertExpression(call.getElseOperand()));

        RexNode[] exprs = exprList.toArray(new RexNode[exprList.size()]);
        RexBuilder rexBuilder = cx.getRexBuilder();
        RelDataType type =
            rexBuilder.deriveReturnType(
                call.getOperator(), cx.getTypeFactory(), exprs);
        for (int i : elseArgs(exprs.length)) {
            exprList.set(
                i,
                rexBuilder.ensureType(type, exprList.get(i), false));
        }
        return rexBuilder.makeCall(
            SqlStdOperatorTable.caseOperator, exprList);
    }

    public RexNode convertMultiset(
        SqlRexContext cx,
        SqlMultisetValueConstructor op,
        SqlCall call)
    {
        final RelDataType originalType =
            cx.getValidator().getValidatedNodeType(call);
        RexRangeRef rr = cx.getSubqueryExpr(call);
        assert rr != null;
        RelDataType msType = rr.getType().getFields()[0].getType();
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
                cx.getRexBuilder().makeCall(SqlStdOperatorTable.sliceOp, expr);
        }
        return expr;
    }

    public RexNode convertArray(
        SqlRexContext cx,
        SqlArrayValueConstructor op,
        SqlCall call)
    {
        return convertCall(cx, call);
    }

    public RexNode convertMap(
        SqlRexContext cx,
        SqlMapValueConstructor op,
        SqlCall call)
    {
        return convertCall(cx, call);
    }

    public RexNode convertMultisetQuery(
        SqlRexContext cx,
        SqlMultisetQueryConstructor op,
        SqlCall call)
    {
        final RelDataType originalType =
            cx.getValidator().getValidatedNodeType(call);
        RexRangeRef rr = cx.getSubqueryExpr(call);
        assert rr != null;
        RelDataType msType = rr.getType().getFields()[0].getType();
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
                cx.getRexBuilder().makeCall(SqlStdOperatorTable.sliceOp, expr);
        }
        return expr;
    }

    public RexNode convertJdbc(
        SqlRexContext cx,
        SqlJdbcFunctionCall op,
        SqlCall call)
    {
        // Yuck!! The function definition contains arguments!
        // TODO: adopt a more conventional definition/instance structure
        final SqlCall convertedCall = op.getLookupCall();
        return cx.convertExpression(convertedCall);
    }

    protected RexNode convertCast(
        SqlRexContext cx,
        SqlCall call)
    {
        RelDataTypeFactory typeFactory = cx.getTypeFactory();
        assert call.getKind() == SqlKind.CAST;
        if (call.operands[1] instanceof SqlIntervalQualifier) {
            SqlNode node = call.operands[0];
            if (node instanceof SqlIntervalLiteral) {
                RexLiteral sourceInterval =
                    (RexLiteral) cx.convertExpression(node);
                long sourceValue =
                    ((BigDecimal) sourceInterval.getValue()).longValue();
                SqlIntervalQualifier intervalQualifier =
                    (SqlIntervalQualifier) call.operands[1];
                RexLiteral castedInterval =
                    cx.getRexBuilder().makeIntervalLiteral(
                        sourceValue, intervalQualifier);
                return castToValidatedType(cx, call, castedInterval);
            }
            return castToValidatedType(cx, call, cx.convertExpression(node));
        }
        SqlDataTypeSpec dataType = (SqlDataTypeSpec) call.operands[1];
        if (SqlUtil.isNullLiteral(call.operands[0], false)) {
            return cx.convertExpression(call.operands[0]);
        }
        RexNode arg = cx.convertExpression(call.operands[0]);
        RelDataType type = dataType.deriveType(typeFactory);
        if (arg.getType().isNullable()) {
            type = typeFactory.createTypeWithNullability(type, true);
        }
        if (null != dataType.getCollectionsTypeName()) {
            final RelDataType argComponentType =
                arg.getType().getComponentType();
            final RelDataType componentType = type.getComponentType();
            if (argComponentType.isStruct()
                && !componentType.isStruct())
            {
                RelDataType tt =
                    typeFactory.createStructType(
                        new RelDataType[] { componentType },
                        new String[] {
                            argComponentType.getFields()[0].getName()
                        });
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
        boolean floor)
    {
        final SqlNode [] operands = call.getOperands();

        // Rewrite floor, ceil of interval
        if ((operands.length == 1)
            && (operands[0] instanceof SqlIntervalLiteral))
        {
            SqlIntervalLiteral.IntervalValue interval =
                (SqlIntervalLiteral.IntervalValue)
                ((SqlIntervalLiteral) operands[0]).getValue();
            long val =
                interval.getIntervalQualifier().getStartUnit().multiplier;
            RexNode rexInterval = cx.convertExpression(operands[0]);

            RexNode res;

            final RexBuilder rexBuilder = cx.getRexBuilder();
            RexNode zero = rexBuilder.makeExactLiteral(BigDecimal.valueOf(0));
            RexNode cond =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.greaterThanOrEqualOperator,
                    rexInterval,
                    zero);

            RexNode pad =
                rexBuilder.makeExactLiteral(BigDecimal.valueOf(val - 1));
            RexNode cast = rexBuilder.makeReinterpretCast(
                rexInterval.getType(), pad, rexBuilder.makeLiteral(false));
            SqlOperator op =
                floor ? SqlStdOperatorTable.minusOperator
                : SqlStdOperatorTable.plusOperator;
            RexNode sum = rexBuilder.makeCall(op, rexInterval, cast);

            RexNode kase =
                floor
                ? rexBuilder.makeCall(
                    SqlStdOperatorTable.caseOperator,
                    cond,
                    rexInterval,
                    sum)
                : rexBuilder.makeCall(
                    SqlStdOperatorTable.caseOperator,
                    cond,
                    sum,
                    rexInterval);

            RexNode factor =
                rexBuilder.makeExactLiteral(BigDecimal.valueOf(val));
            RexNode div =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.divideIntegerOperator,
                    kase,
                    factor);
            RexNode mult =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.multiplyOperator,
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
        SqlCall call)
    {
        final RexBuilder rexBuilder = cx.getRexBuilder();
        final SqlNode [] operands = call.getOperands();
        final RexNode [] exprs = convertExpressionList(cx, operands);

        // TODO: Will need to use decimal type for seconds with precision
        RelDataType resType =
            cx.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        resType =
            cx.getTypeFactory().createTypeWithNullability(
                resType,
                exprs[1].getType().isNullable());
        RexNode cast = rexBuilder.makeReinterpretCast(
            resType, exprs[1], rexBuilder.makeLiteral(false));

        SqlIntervalQualifier.TimeUnit unit =
            ((SqlIntervalQualifier) operands[0]).getStartUnit();
        long val = unit.multiplier;
        RexNode factor = rexBuilder.makeExactLiteral(BigDecimal.valueOf(val));
        switch (unit) {
        case DAY:
            val = 0;
            break;
        case HOUR:
            val = SqlIntervalQualifier.TimeUnit.DAY.multiplier;
            break;
        case MINUTE:
            val = SqlIntervalQualifier.TimeUnit.HOUR.multiplier;
            break;
        case SECOND:
            val = SqlIntervalQualifier.TimeUnit.MINUTE.multiplier;
            break;
        case YEAR:
            val = 0;
            break;
        case MONTH:
            val = SqlIntervalQualifier.TimeUnit.YEAR.multiplier;
            break;
        default:
            throw Util.unexpected(unit);
        }

        RexNode res = cast;
        if (val != 0) {
            RexNode modVal =
                rexBuilder.makeExactLiteral(BigDecimal.valueOf(val), resType);
            res =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.modFunc,
                    res,
                    modVal);
        }

        res =
            rexBuilder.makeCall(
                SqlStdOperatorTable.divideIntegerOperator,
                res,
                factor);
        return res;
    }

    public RexNode convertDatetimeMinus(
        SqlRexContext cx,
        SqlDatetimeSubtractionOperator op,
        SqlCall call)
    {
        // Rewrite datetime minus
        final RexBuilder rexBuilder = cx.getRexBuilder();
        final SqlNode [] operands = call.getOperands();
        final RexNode [] exprs = convertExpressionList(cx, operands);

        // TODO: Handle year month interval (represented in months)
        for (RexNode expr : exprs) {
            if (SqlTypeName.INTERVAL_YEAR_MONTH
                == expr.getType().getSqlTypeName())
            {
                Util.needToImplement(
                    "Datetime subtraction of year month interval");
            }
        }
        RelDataType int8Type =
            cx.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        final RexNode [] casts = new RexNode[2];
        casts[0] =
            rexBuilder.makeCast(
                cx.getTypeFactory().createTypeWithNullability(
                    int8Type,
                    exprs[0].getType().isNullable()),
                exprs[0]);
        casts[1] =
            rexBuilder.makeCast(
                cx.getTypeFactory().createTypeWithNullability(
                    int8Type,
                    exprs[1].getType().isNullable()),
                exprs[1]);
        final RexNode minus =
            rexBuilder.makeCall(
                SqlStdOperatorTable.minusOperator,
                casts);
        final RelDataType resType =
            cx.getValidator().getValidatedNodeType(call);
        final RexNode res = rexBuilder.makeReinterpretCast(
            resType,
            minus,
            rexBuilder.makeLiteral(false));
        return res;
    }

    public RexNode convertFunction(
        SqlRexContext cx,
        SqlFunction fun,
        SqlCall call)
    {
        final SqlNode [] operands = call.getOperands();
        final RexNode [] exprs = convertExpressionList(cx, operands);
        if (fun.getFunctionType()
            == SqlFunctionCategory.UserDefinedConstructor)
        {
            return makeConstructorCall(cx, fun, exprs);
        }
        return cx.getRexBuilder().makeCall(fun, exprs);
    }

    public RexNode convertAggregateFunction(
        SqlRexContext cx,
        SqlAggFunction fun,
        SqlCall call)
    {
        final SqlNode [] operands = call.getOperands();
        final RexNode [] exprs;
        if (call.isCountStar()) {
            exprs = RexNode.EMPTY_ARRAY;
        } else {
            exprs = convertExpressionList(cx, operands);
        }
        return cx.getRexBuilder().makeCall(fun, exprs);
    }

    private static RexNode makeConstructorCall(
        SqlRexContext cx,
        SqlFunction constructor,
        RexNode [] exprs)
    {
        final RexBuilder rexBuilder = cx.getRexBuilder();
        final RelDataTypeFactory typeFactory = cx.getTypeFactory();
        RelDataType type =
            rexBuilder.deriveReturnType(
                constructor, typeFactory, exprs);

        int n = type.getFieldCount();
        RexNode [] initializationExprs = new RexNode[n];
        for (int i = 0; i < n; ++i) {
            initializationExprs[i] =
                cx.getDefaultValueFactory().newAttributeInitializer(
                    type,
                    constructor,
                    i,
                    exprs);
        }

        RexNode [] defaultCasts =
            RexUtil.generateCastExpressions(
                rexBuilder,
                type,
                initializationExprs);

        return rexBuilder.makeNewInvocation(type, defaultCasts);
    }

    /**
     * Converts a call to an operator into a {@link RexCall} to the same
     * operator.
     *
     * <p>Called automatically via reflection.
     *
     * @param cx Context
     * @param call Call
     *
     * @return Rex call
     */
    public RexNode convertCall(
        SqlRexContext cx,
        SqlCall call)
    {
        final SqlOperator op = call.getOperator();
        final SqlNode [] operands = call.getOperands();
        final RexBuilder rexBuilder = cx.getRexBuilder();
        final RexNode [] exprs = convertExpressionList(cx, operands);
        if (op.getOperandTypeChecker()
            == SqlTypeStrategies.otcComparableUnorderedX2)
        {
            ensureSameType(cx, exprs);
        }
        return rexBuilder.makeCall(op, exprs);
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

    private void ensureSameType(SqlRexContext cx, final RexNode[] exprs) {
        RelDataType type =
            cx.getTypeFactory().leastRestrictive(
                new AbstractList<RelDataType>() {
                    public RelDataType get(int index) {
                        return exprs[index].getType();
                    }
                    public int size() {
                        return exprs.length;
                    }
                });
        for (int i = 0; i < exprs.length; i++) {
            exprs[i] = cx.getRexBuilder().ensureType(type, exprs[i], true);
        }
    }

    private static RexNode [] convertExpressionList(
        SqlRexContext cx,
        SqlNode [] nodes)
    {
        final RexNode [] exprs = new RexNode[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            SqlNode node = nodes[i];
            exprs[i] = cx.convertExpression(node);
        }
        return exprs;
    }

    private RexNode convertIsDistinctFrom(
        SqlRexContext cx,
        SqlCall call,
        boolean neg)
    {
        RexNode op0 = cx.convertExpression(call.operands[0]);
        RexNode op1 = cx.convertExpression(call.operands[1]);
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
        SqlCall call)
    {
        final SqlNode value = call.operands[SqlBetweenOperator.VALUE_OPERAND];
        RexNode x = cx.convertExpression(value);
        final SqlBetweenOperator.Flag symmetric = op.flag;
        final SqlNode lower = call.operands[SqlBetweenOperator.LOWER_OPERAND];
        RexNode y = cx.convertExpression(lower);
        final SqlNode upper = call.operands[SqlBetweenOperator.UPPER_OPERAND];
        RexNode z = cx.convertExpression(upper);

        RexNode res;

        final RexBuilder rexBuilder = cx.getRexBuilder();
        RexNode ge1 =
            rexBuilder.makeCall(
                SqlStdOperatorTable.greaterThanOrEqualOperator,
                x,
                y);
        RexNode le1 =
            rexBuilder.makeCall(
                SqlStdOperatorTable.lessThanOrEqualOperator,
                x,
                z);
        RexNode and1 =
            rexBuilder.makeCall(
                SqlStdOperatorTable.andOperator,
                ge1,
                le1);

        switch (symmetric) {
        case ASYMMETRIC:
            res = and1;
            break;
        case SYMMETRIC:
            RexNode ge2 =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.greaterThanOrEqualOperator,
                    x,
                    z);
            RexNode le2 =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.lessThanOrEqualOperator,
                    x,
                    y);
            RexNode and2 =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.andOperator,
                    ge2,
                    le2);
            res =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.orOperator,
                    and1,
                    and2);
            break;
        default:
            throw Util.unexpected(symmetric);
        }
        final SqlBetweenOperator betweenOp =
            (SqlBetweenOperator) call.getOperator();
        if (betweenOp.isNegated()) {
            res = rexBuilder.makeCall(SqlStdOperatorTable.notOperator, res);
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
        SqlCall call)
    {
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
        SqlCall call)
    {
        if (cx.getValidator().getValidatedNodeType(call).getSqlTypeName()
            != SqlTypeName.COLUMN_LIST)
        {
            return convertCall(cx, call);
        }
        final RexBuilder rexBuilder = cx.getRexBuilder();
        final List<RexNode> columns = new ArrayList<RexNode>();
        for (SqlNode operand : call.getOperands()) {
            columns.add(
                rexBuilder.makeLiteral(
                    ((SqlIdentifier) operand).getSimple()));
        }
        return rexBuilder.makeCall(
            SqlStdOperatorTable.columnListConstructor,
            columns);
    }

    /**
     * Converts a call to OVERLAPS.
     *
     * <p>Called automatically via reflection.
     */
    public RexNode convertOverlaps(
        SqlRexContext cx,
        SqlOverlapsOperator op,
        SqlCall call)
    {
        // for intervals [t0, t1] overlaps [t2, t3], we can find if the
        // intervals overlaps by: ~(t1 < t2 or t3 < t0)
        final SqlNode [] operands = call.getOperands();
        assert operands.length == 4;
        if (operands[1] instanceof SqlIntervalLiteral) {
            // make t1 = t0 + t1 when t1 is an interval.
            SqlOperator op1 = SqlStdOperatorTable.plusOperator;
            SqlNode [] second = new SqlNode[2];
            second[0] = operands[0];
            second[1] = operands[1];
            operands[1] =
                op1.createCall(
                    call.getParserPosition(),
                    second);
        }
        if (operands[3] instanceof SqlIntervalLiteral) {
            // make t3 = t2 + t3 when t3 is an interval.
            SqlOperator op1 = SqlStdOperatorTable.plusOperator;
            SqlNode [] four = new SqlNode[2];
            four[0] = operands[2];
            four[1] = operands[3];
            operands[3] =
                op1.createCall(
                    call.getParserPosition(),
                    four);
        }

        // This captures t1 >= t2
        SqlOperator op1 = SqlStdOperatorTable.greaterThanOrEqualOperator;
        SqlNode [] left = new SqlNode[2];
        left[0] = operands[1];
        left[1] = operands[2];
        SqlCall call1 =
            op1.createCall(
                call.getParserPosition(),
                left);

        // This captures t3 >= t0
        SqlOperator op2 = SqlStdOperatorTable.greaterThanOrEqualOperator;
        SqlNode [] right = new SqlNode[2];
        right[0] = operands[3];
        right[1] = operands[0];
        SqlCall call2 =
            op2.createCall(
                call.getParserPosition(),
                right);

        // This captures t1 >= t2 and t3 >= t0
        SqlOperator and = SqlStdOperatorTable.andOperator;
        SqlNode [] overlaps = new SqlNode[2];
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
        RexNode value)
    {
        final RelDataType resType =
            cx.getValidator().getValidatedNodeType(call);
        if (value.getType() == resType) {
            return value;
        }
        return cx.getRexBuilder().makeCast(resType, value);
    }

    private static class AvgVarianceConvertlet implements SqlRexConvertlet
    {
        private final SqlAvgAggFunction.Subtype subtype;

        public AvgVarianceConvertlet(SqlAvgAggFunction.Subtype subtype)
        {
            this.subtype = subtype;
        }

        public RexNode convertCall(SqlRexContext cx, SqlCall call)
        {
            final SqlNode[] operands = call.getOperands();
            Util.permAssert(
                operands.length == 1,
                "operands.length == 1");
            final SqlNode arg = operands[0];
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
            final SqlNode arg)
        {
            final SqlParserPos pos = SqlParserPos.ZERO;
            final SqlNode sum =
                SqlStdOperatorTable.sumOperator.createCall(pos, arg);
            final SqlNode count =
                SqlStdOperatorTable.countOperator.createCall(pos, arg);
            return SqlStdOperatorTable.divideOperator.createCall(
                pos, sum, count);
        }

        private SqlNode expandVariance(
            final SqlNode arg,
            boolean biased,
            boolean sqrt)
        {
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
                SqlStdOperatorTable.multiplyOperator.createCall(pos, arg, arg);
            final SqlNode sumArgSquared =
                SqlStdOperatorTable.sumOperator.createCall(pos, argSquared);
            final SqlNode sum =
                SqlStdOperatorTable.sumOperator.createCall(pos, arg);
            final SqlNode sumSquared =
                SqlStdOperatorTable.multiplyOperator.createCall(pos, sum, sum);
            final SqlNode count =
                SqlStdOperatorTable.countOperator.createCall(pos, arg);
            final SqlNode avgSumSquared =
                SqlStdOperatorTable.divideOperator.createCall(
                    pos, sumSquared, count);
            final SqlNode diff =
                SqlStdOperatorTable.minusOperator.createCall(
                    pos, sumArgSquared, avgSumSquared);
            final SqlNode denominator;
            if (biased) {
                denominator = count;
            } else {
                final SqlNumericLiteral one =
                    SqlLiteral.createExactNumeric("1", pos);
                denominator =
                    SqlStdOperatorTable.minusOperator.createCall(
                        pos, count, one);
            }
            final SqlNode div =
                SqlStdOperatorTable.divideOperator.createCall(
                    pos, diff, denominator);
            SqlNode result = div;
            if (sqrt) {
                final SqlNumericLiteral half =
                    SqlLiteral.createExactNumeric("0.5", pos);
                result =
                    SqlStdOperatorTable.powerFunc.createCall(pos, div, half);
            }
            return result;
        }
    }

    private class FloorCeilConvertlet implements SqlRexConvertlet
    {
        private final boolean floor;

        public FloorCeilConvertlet(boolean floor)
        {
            this.floor = floor;
        }

        public RexNode convertCall(SqlRexContext cx, SqlCall call)
        {
            return convertFloorCeil(cx, call, floor);
        }
    }
}

// End StandardConvertletTable.java
