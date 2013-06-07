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
package org.eigenbase.rel.rules;

import java.math.*;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * ReduceDecimalsRule is a rule which reduces decimal operations (such as casts
 * or arithmetic) into operations involving more primitive types (such as longs
 * and doubles). The rule allows eigenbase implementations to deal with decimals
 * in a consistent manner, while saving the effort of implementing them.
 *
 * <p>The rule can be applied to a {@link CalcRel} with a program for which
 * {@link RexUtil#requiresDecimalExpansion} returns true. The rule relies on a
 * {@link RexShuttle} to walk over relational expressions and replace them.
 *
 * <p>While decimals are generally not implemented by the eigenbase runtime, the
 * rule is optionally applied, in order to support the situation in which we
 * would like to push down decimal operations to an external database.
 */
public class ReduceDecimalsRule
    extends RelOptRule
{
    public static final ReduceDecimalsRule instance = new ReduceDecimalsRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a ReduceDecimalsRule.
     */
    private ReduceDecimalsRule()
    {
        super(any(CalcRel.class));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public Convention getOutConvention()
    {
        return Convention.NONE;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        CalcRel calcRel = call.rel(0);

        // Expand decimals in every expression in this program. If no
        // expression changes, don't apply the rule.
        final RexProgram program = calcRel.getProgram();
        if (!RexUtil.requiresDecimalExpansion(program, true)) {
            return;
        }

        final RexBuilder rexBuilder = calcRel.getCluster().getRexBuilder();
        final RexShuttle shuttle = new DecimalShuttle(rexBuilder);
        RexProgramBuilder programBuilder =
            RexProgramBuilder.create(
                rexBuilder,
                calcRel.getChild().getRowType(),
                program.getExprList(),
                program.getProjectList(),
                program.getCondition(),
                program.getOutputRowType(),
                shuttle,
                true);

        final RexProgram newProgram = programBuilder.getProgram();
        CalcRel newCalcRel =
            new CalcRel(
                calcRel.getCluster(),
                calcRel.getTraitSet(),
                calcRel.getChild(),
                newProgram.getOutputRowType(),
                newProgram,
                Collections.<RelCollation>emptyList());
        call.transformTo(newCalcRel);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * A shuttle which converts decimal expressions to expressions based on
     * longs.
     */
    public class DecimalShuttle
        extends RexShuttle
    {
        private final Map<String, RexNode> irreducible;
        private final Map<String, RexNode> results;
        private final ExpanderMap expanderMap;

        public DecimalShuttle(RexBuilder rexBuilder)
        {
            irreducible = new HashMap<String, RexNode>();
            results = new HashMap<String, RexNode>();
            expanderMap = new ExpanderMap(rexBuilder);
        }

        /**
         * Rewrites a call in place, from bottom up, as follows:
         *
         * <ol>
         * <li>visit operands
         * <li>visit call node
         *
         * <ol>
         * <li>rewrite call
         * <li>visit the rewritten call
         * </ol>
         * </ol>
         */
        public RexNode visitCall(RexCall call)
        {
            RexNode savedResult = lookup(call);
            if (savedResult != null) {
                return savedResult;
            }

            // permanently updates a call in place
            apply(Arrays.asList(call.operands));

            RexNode newCall = call;
            RexNode rewrite = rewriteCall(call);
            if (rewrite != call) {
                newCall = rewrite.accept(this);
            }

            register(call, newCall);
            return newCall;
        }

        /**
         * Registers node so it will not be computed again
         */
        private void register(RexNode node, RexNode reducedNode)
        {
            String key = RexUtil.makeKey(node);
            if (node == reducedNode) {
                irreducible.put(key, reducedNode);
            } else {
                results.put(key, reducedNode);
            }
        }

        /**
         * Lookup registered node
         */
        private RexNode lookup(RexNode node)
        {
            String key = RexUtil.makeKey(node);
            if (irreducible.get(key) != null) {
                return node;
            }
            return results.get(key);
        }

        /**
         * Rewrites a call, if required, or returns the original call
         */
        private RexNode rewriteCall(RexCall call)
        {
            SqlOperator operator = call.getOperator();
            if (!operator.requiresDecimalExpansion()) {
                return call;
            }

            RexExpander expander = getExpander(call);
            if (expander.canExpand(call)) {
                return expander.expand(call);
            }
            return call;
        }

        /**
         * Returns a {@link RexExpander} for a call
         */
        private RexExpander getExpander(RexCall call)
        {
            return expanderMap.getExpander(call);
        }
    }

    /**
     * Maps a RexCall to a RexExpander
     */
    private class ExpanderMap
    {
        private final Map<SqlOperator, RexExpander> map;
        private RexExpander defaultExpander;

        private ExpanderMap(RexBuilder rexBuilder)
        {
            map = new HashMap<SqlOperator, RexExpander>();
            registerExpanders(rexBuilder);
        }

        private void registerExpanders(RexBuilder rexBuilder)
        {
            RexExpander cast = new CastExpander(rexBuilder);
            map.put(SqlStdOperatorTable.castFunc, cast);

            RexExpander passThrough = new PassThroughExpander(rexBuilder);
            map.put(SqlStdOperatorTable.prefixMinusOperator, passThrough);
            map.put(SqlStdOperatorTable.absFunc, passThrough);

            map.put(SqlStdOperatorTable.isNullOperator, passThrough);
            map.put(SqlStdOperatorTable.isNotNullOperator, passThrough);

            RexExpander arithmetic = new BinaryArithmeticExpander(rexBuilder);
            map.put(SqlStdOperatorTable.divideOperator, arithmetic);
            map.put(SqlStdOperatorTable.multiplyOperator, arithmetic);
            map.put(SqlStdOperatorTable.plusOperator, arithmetic);
            map.put(SqlStdOperatorTable.minusOperator, arithmetic);
            map.put(SqlStdOperatorTable.modFunc, arithmetic);

            map.put(SqlStdOperatorTable.equalsOperator, arithmetic);
            map.put(SqlStdOperatorTable.greaterThanOperator, arithmetic);
            map.put(
                SqlStdOperatorTable.greaterThanOrEqualOperator,
                arithmetic);
            map.put(SqlStdOperatorTable.lessThanOperator, arithmetic);
            map.put(SqlStdOperatorTable.lessThanOrEqualOperator, arithmetic);

            RexExpander floor = new FloorExpander(rexBuilder);
            map.put(SqlStdOperatorTable.floorFunc, floor);
            RexExpander ceil = new CeilExpander(rexBuilder);
            map.put(SqlStdOperatorTable.ceilFunc, ceil);

            RexExpander reinterpret = new ReinterpretExpander(rexBuilder);
            map.put(SqlStdOperatorTable.reinterpretOperator, reinterpret);

            RexExpander caseExpander = new CaseExpander(rexBuilder);
            map.put(SqlStdOperatorTable.caseOperator, caseExpander);

            defaultExpander = new CastArgAsDoubleExpander(rexBuilder);
        }

        public RexExpander getExpander(RexCall call)
        {
            RexExpander expander = map.get(call.getOperator());
            return (expander != null) ? expander : defaultExpander;
        }
    }

    /**
     * Rewrites a decimal expression for a specific set of SqlOperator's. In
     * general, most expressions are rewritten in such a way that SqlOperator's
     * do not have to deal with decimals. Decimals are represented by their
     * unscaled integer representations, similar to {@link
     * BigDecimal#unscaledValue()} (i.e. 10^scale). Once decimals are decoded,
     * SqlOperators can then operate on the integer representations. The value
     * can later be recoded as a decimal.
     *
     * <p>For example, suppose one casts 2.0 as a decima(10,4). The value is
     * decoded (20), multiplied by a scale factor (1000), for a result of
     * (20000) which is encoded as a decimal(10,4), in this case 2.0000
     *
     * <p>To avoid the lengthy coding of RexNode expressions, this base class
     * provides succinct methods for building expressions used in rewrites.
     */
    public abstract class RexExpander
    {
        /**
         * Factory for constructing new relational expressions
         */
        RexBuilder builder;

        /**
         * Type for the internal representation of decimals. This type is a
         * non-nullable type and requires extra work to make it nullable.
         */
        RelDataType int8;

        /**
         * Type for doubles. This type is a non-nullable type and requires extra
         * work to make it nullable.
         */
        RelDataType real8;

        /**
         * Constructs a RexExpander
         */
        public RexExpander(RexBuilder builder)
        {
            this.builder = builder;
            int8 = builder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
            real8 = builder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE);
        }

        /**
         * This defaults to the utility method, {@link
         * RexUtil#requiresDecimalExpansion(RexNode, boolean)} which checks
         * general guidelines on whether a rewrite should be considered at all.
         * In general, it is helpful to update the utility method since that
         * method is often used to filter the somewhat expensive rewrite
         * process.
         *
         * <p>However, this method provides another place for implementations of
         * RexExpander to make a more detailed analysis before deciding on
         * whether to perform a rewrite.
         */
        public boolean canExpand(RexCall call)
        {
            return RexUtil.requiresDecimalExpansion(call, false);
        }

        /**
         * Rewrites an expression containing decimals. Normally, this method
         * always performs a rewrite, but implementations may choose to return
         * the original expression if no change was required.
         */
        public abstract RexNode expand(RexCall call);

        /**
         * Makes an exact numeric literal to be used for scaling
         *
         * @param scale a scale from one to max precision - 1
         *
         * @return 10^scale as an exact numeric value
         */
        protected RexNode makeScaleFactor(int scale)
        {
            assert (scale > 0) && (scale < SqlTypeName.MAX_NUMERIC_PRECISION);
            return makeExactLiteral(powerOfTen(scale));
        }

        /**
         * Makes an approximate literal to be used for scaling
         *
         * @param scale a scale from -99 to 99
         *
         * @return 10^scale as an approximate value
         */
        protected RexNode makeApproxScaleFactor(int scale)
        {
            assert ((-100 < scale) && (scale < 100))
                : "could not make approximate scale factor";
            if (scale >= 0) {
                return makeApproxLiteral(BigDecimal.TEN.pow(scale));
            } else {
                BigDecimal tenth = BigDecimal.valueOf(1, 1);
                return makeApproxLiteral(tenth.pow(-scale));
            }
        }

        /**
         * Makes an exact numeric value to be used for rounding.
         *
         * @param scale a scale from 1 to max precision - 1
         *
         * @return 10^scale / 2 as an exact numeric value
         */
        protected RexNode makeRoundFactor(int scale)
        {
            assert (scale > 0) && (scale < SqlTypeName.MAX_NUMERIC_PRECISION);
            return makeExactLiteral(powerOfTen(scale) / 2);
        }

        /**
         * Calculates a power of ten, as a long value
         */
        protected long powerOfTen(int scale)
        {
            assert (scale >= 0) && (scale < SqlTypeName.MAX_NUMERIC_PRECISION);
            return BigInteger.TEN.pow(scale).longValue();
        }

        /**
         * Makes an exact, non-nullable literal of Bigint type
         */
        protected RexNode makeExactLiteral(long l)
        {
            BigDecimal bd = BigDecimal.valueOf(l);
            return builder.makeExactLiteral(bd, int8);
        }

        /**
         * Makes an approximate literal of double precision
         */
        protected RexNode makeApproxLiteral(BigDecimal bd)
        {
            return builder.makeApproxLiteral(bd);
        }

        /**
         * Scales up a decimal value and returns the scaled value as an exact
         * number.
         *
         * @param value the integer representation of a decimal
         * @param scale a value from zero to max precision - 1
         *
         * @return value * 10^scale as an exact numeric value
         */
        protected RexNode scaleUp(RexNode value, int scale)
        {
            assert (scale >= 0) && (scale < SqlTypeName.MAX_NUMERIC_PRECISION);
            if (scale == 0) {
                return value;
            }
            return builder.makeCall(
                SqlStdOperatorTable.multiplyOperator,
                value,
                makeScaleFactor(scale));
        }

        /**
         * Scales down a decimal value, and returns the scaled value as an exact
         * numeric. with the rounding convention {@link BigDecimal#ROUND_HALF_UP
         * BigDecimal.ROUND_HALF_UP}. (Values midway between two points are
         * rounded away from zero.)
         *
         * @param value the integer representation of a decimal
         * @param scale a value from zero to max precision
         *
         * @return value/10^scale, rounded away from zero and returned as an
         * exact numeric value
         */
        protected RexNode scaleDown(RexNode value, int scale)
        {
            int maxPrecision = SqlTypeName.MAX_NUMERIC_PRECISION;
            assert (scale >= 0) && (scale <= maxPrecision);
            if (scale == 0) {
                return value;
            }
            if (scale == maxPrecision) {
                long half = BigInteger.TEN.pow(scale - 1).longValue() * 5;
                return makeCase(
                    builder.makeCall(
                        SqlStdOperatorTable.greaterThanOrEqualOperator,
                        value,
                        makeExactLiteral(half)),
                    makeExactLiteral(1),
                    builder.makeCall(
                        SqlStdOperatorTable.lessThanOrEqualOperator,
                        value,
                        makeExactLiteral(-half)),
                    makeExactLiteral(-1),
                    makeExactLiteral(0));
            }
            RexNode roundFactor = makeRoundFactor(scale);
            RexNode roundValue =
                makeCase(
                    builder.makeCall(
                        SqlStdOperatorTable.greaterThanOperator,
                        value,
                        makeExactLiteral(0)),
                    makePlus(value, roundFactor),
                    makeMinus(value, roundFactor));
            return makeDivide(
                roundValue,
                makeScaleFactor(scale));
        }

        /**
         * Scales down a decimal value and returns the scaled value as a an
         * double precision approximate value. Scaling is implemented with
         * double precision arithmetic.
         *
         * @param value the integer representation of a decimal
         * @param scale a value from zero to {@link
         * SqlTypeName#MAX_NUMERIC_PRECISION MAX_NUMERIC_PRECISION}
         *
         * @return value/10^scale as a double precision value
         */
        protected RexNode scaleDownDouble(RexNode value, int scale)
        {
            assert (scale >= 0) && (scale <= SqlTypeName.MAX_NUMERIC_PRECISION);
            RexNode cast = ensureType(real8, value);
            if (scale == 0) {
                return cast;
            }
            return makeDivide(
                cast,
                makeApproxScaleFactor(scale));
        }

        /**
         * Ensures a value is of a required scale. If it is not, then the value
         * is multiplied by a scale factor. Scaling up an exact value is limited
         * to max precision - 1, because we cannot represent the result of
         * larger scales internally. Scaling up a floating point value is more
         * flexible since the value may be very small despite having a scale of
         * zero and the scaling may still produce a reasonable result
         *
         * @param value integer representation of decimal, or a floating point
         * number
         * @param scale current scale, 0 for floating point numbers
         * @param required required scale, must be at least the current scale;
         * the scale difference may not be greater than max precision - 1 for
         * exact numerics
         *
         * @return value * 10^scale, returned as an exact or approximate value
         * corresponding to the input value
         */
        protected RexNode ensureScale(RexNode value, int scale, int required)
        {
            int maxPrecision = SqlTypeName.MAX_NUMERIC_PRECISION;
            assert (scale <= maxPrecision) && (required <= maxPrecision);
            assert required >= scale;
            if (scale == required) {
                return value;
            }
            int scaleDiff = required - scale;
            if (SqlTypeUtil.isApproximateNumeric(value.getType())) {
                return makeMultiply(
                    value,
                    makeApproxScaleFactor(scaleDiff));
            }

            // TODO: make a validator exception for this
            if (scaleDiff >= SqlTypeName.MAX_NUMERIC_PRECISION) {
                throw Util.needToImplement(
                    "Source type with scale " + scale
                    + " cannot be converted to target type with scale "
                    + required + " because the smallest value of the "
                    + "source type is too large to be encoded by the "
                    + "target type");
            }
            return scaleUp(value, scaleDiff);
        }

        /**
         * Retrieves a decimal node's integer representation
         *
         * @param decimalNode the decimal value as an opaque type
         *
         * @return an integer representation of the decimal value
         */
        protected RexNode decodeValue(RexNode decimalNode)
        {
            assert (SqlTypeUtil.isDecimal(decimalNode.getType()));
            return builder.decodeIntervalOrDecimal(decimalNode);
        }

        /**
         * Retrieves the primitive value of a numeric node. If the node is a
         * decimal, then it must first be decoded. Otherwise the original node
         * may be returned.
         *
         * @param node a numeric node, possibly a decimal
         *
         * @return the primitive value of the numeric node
         */
        protected RexNode accessValue(RexNode node)
        {
            assert SqlTypeUtil.isNumeric(node.getType());
            if (SqlTypeUtil.isDecimal(node.getType())) {
                return decodeValue(node);
            }
            return node;
        }

        /**
         * Casts a decimal's integer representation to a decimal node. If the
         * expression is not the expected integer type, then it is casted first.
         *
         * <p>This method does not request an overflow check.
         *
         * @param value integer representation of decimal
         * @param decimalType type integer will be reinterpreted as
         *
         * @return the integer representation reinterpreted as a decimal type
         */
        protected RexNode encodeValue(RexNode value, RelDataType decimalType)
        {
            return encodeValue(value, decimalType, false);
        }

        /**
         * Casts a decimal's integer representation to a decimal node. If the
         * expression is not the expected integer type, then it is casted first.
         *
         * <p>An overflow check may be requested to ensure the internal value
         * does not exceed the maximum value of the decimal type.
         *
         * @param value integer representation of decimal
         * @param decimalType type integer will be reinterpreted as
         * @param checkOverflow indicates whether an overflow check is required
         * when reinterpreting this particular value as the decimal type. A
         * check usually not required for arithmetic, but is often required for
         * rounding and explicit casts.
         *
         * @return the integer reinterpreted as an opaque decimal type
         */
        protected RexNode encodeValue(
            RexNode value,
            RelDataType decimalType,
            boolean checkOverflow)
        {
            return builder.encodeIntervalOrDecimal(
                value, decimalType, checkOverflow);
        }

        /**
         * Ensures expression is interpreted as a specified type. The returned
         * expression may be wrapped with a cast.
         *
         * <p>This method corrects the nullability of the specified type to
         * match the nullability of the expression.
         *
         * @param type desired type
         * @param node expression
         *
         * @return a casted expression or the original expression
         */
        protected RexNode ensureType(RelDataType type, RexNode node)
        {
            return ensureType(type, node, true);
        }

        /**
         * Ensures expression is interpreted as a specified type. The returned
         * expression may be wrapped with a cast.
         *
         * @param type desired type
         * @param node expression
         * @param matchNullability whether to correct nullability of specified
         * type to match the expression; this usually should be true, except for
         * explicit casts which can override default nullability
         *
         * @return a casted expression or the original expression
         */
        protected RexNode ensureType(
            RelDataType type,
            RexNode node,
            boolean matchNullability)
        {
            return builder.ensureType(type, node, matchNullability);
        }

        protected RexNode makeCase(
            RexNode condition,
            RexNode thenClause,
            RexNode elseClause)
        {
            return builder.makeCall(
                SqlStdOperatorTable.caseOperator,
                condition,
                thenClause,
                elseClause);
        }

        protected RexNode makeCase(
            RexNode whenA,
            RexNode thenA,
            RexNode whenB,
            RexNode thenB,
            RexNode elseClause)
        {
            return builder.makeCall(
                SqlStdOperatorTable.caseOperator,
                whenA,
                thenA,
                whenB,
                thenB,
                elseClause);
        }

        protected RexNode makePlus(
            RexNode a,
            RexNode b)
        {
            return builder.makeCall(
                SqlStdOperatorTable.plusOperator,
                a,
                b);
        }

        protected RexNode makeMinus(
            RexNode a,
            RexNode b)
        {
            return builder.makeCall(
                SqlStdOperatorTable.minusOperator,
                a,
                b);
        }

        protected RexNode makeDivide(
            RexNode a,
            RexNode b)
        {
            return builder.makeCall(
                SqlStdOperatorTable.divideIntegerOperator,
                a,
                b);
        }

        protected RexNode makeMultiply(
            RexNode a,
            RexNode b)
        {
            return builder.makeCall(
                SqlStdOperatorTable.multiplyOperator,
                a,
                b);
        }

        protected RexNode makeIsPositive(
            RexNode a)
        {
            return builder.makeCall(
                SqlStdOperatorTable.greaterThanOperator,
                a,
                makeExactLiteral(0));
        }

        protected RexNode makeIsNegative(
            RexNode a)
        {
            return builder.makeCall(
                SqlStdOperatorTable.lessThanOperator,
                a,
                makeExactLiteral(0));
        }
    }

    /**
     * Expands a decimal cast expression
     */
    private class CastExpander
        extends RexExpander
    {
        private CastExpander(RexBuilder builder)
        {
            super(builder);
        }

        // implement RexExpander
        public RexNode expand(RexCall call)
        {
            RexNode [] operands = call.operands;
            Util.pre(
                call.isA(RexKind.Cast),
                "call.isA(RexKind.Cast)");
            Util.pre(operands.length == 1, "operands.length == 1");
            assert (!RexLiteral.isNullLiteral(operands[0]));

            RexNode operand = operands[0].clone();
            RelDataType fromType = operand.getType();
            RelDataType toType = call.getType();
            assert (SqlTypeUtil.isDecimal(fromType)
                || SqlTypeUtil.isDecimal(toType));

            if (SqlTypeUtil.isIntType(toType)) {
                // decimal to int
                return ensureType(
                    toType,
                    scaleDown(
                        decodeValue(operand),
                        fromType.getScale()),
                    false);
            } else if (SqlTypeUtil.isApproximateNumeric(toType)) {
                // decimal to floating point
                return ensureType(
                    toType,
                    scaleDownDouble(
                        decodeValue(operand),
                        fromType.getScale()),
                    false);
            } else if (SqlTypeUtil.isApproximateNumeric(fromType)) {
                // real to decimal
                return encodeValue(
                    ensureScale(
                        operand,
                        0,
                        toType.getScale()),
                    toType,
                    true);
            }

            if (!SqlTypeUtil.isExactNumeric(fromType)
                || !SqlTypeUtil.isExactNumeric(toType))
            {
                throw Util.needToImplement(
                    "Cast from '" + fromType.toString()
                    + "' to '" + toType.toString() + "'");
            }
            int fromScale = fromType.getScale();
            int toScale = toType.getScale();
            int fromDigits = fromType.getPrecision() - fromScale;
            int toDigits = toType.getPrecision() - toScale;

            // NOTE: precision 19 overflows when its underlying
            // bigint representation overflows
            boolean checkOverflow =
                (toType.getPrecision() < 19) && (toDigits < fromDigits);

            if (SqlTypeUtil.isIntType(fromType)) {
                // int to decimal
                return encodeValue(
                    ensureScale(
                        operand,
                        0,
                        toType.getScale()),
                    toType,
                    checkOverflow);
            } else if (
                SqlTypeUtil.isDecimal(fromType)
                && SqlTypeUtil.isDecimal(toType))
            {
                // decimal to decimal
                RexNode value = decodeValue(operand);
                RexNode scaled;
                if (fromScale <= toScale) {
                    scaled = ensureScale(value, fromScale, toScale);
                } else {
                    if ((toDigits == fromDigits)
                        && (toScale < fromScale))
                    {
                        // rounding away from zero may cause an overflow
                        // for example: cast(9.99 as decimal(2,1))
                        checkOverflow = true;
                    }
                    scaled = scaleDown(value, fromScale - toScale);
                }

                return encodeValue(scaled, toType, checkOverflow);
            } else {
                throw Util.needToImplement(
                    "Reduce decimal cast from " + fromType + " to " + toType);
            }
        }
    }

    /**
     * Expands a decimal arithmetic expression
     */
    private class BinaryArithmeticExpander
        extends RexExpander
    {
        RelDataType typeA, typeB;
        int scaleA, scaleB;

        private BinaryArithmeticExpander(RexBuilder builder)
        {
            super(builder);
        }

        // implement RexExpander
        public RexNode expand(RexCall call)
        {
            RexNode [] operands = call.operands;
            Util.pre(operands.length == 2, "operands.length == 2");
            RelDataType typeA = operands[0].getType();
            RelDataType typeB = operands[1].getType();
            assert (SqlTypeUtil.isNumeric(typeA)
                && SqlTypeUtil.isNumeric(typeB));

            if (SqlTypeUtil.isApproximateNumeric(typeA)
                || SqlTypeUtil.isApproximateNumeric(typeB))
            {
                int castIndex = SqlTypeUtil.isApproximateNumeric(typeA) ? 1 : 0;
                int otherIndex = (castIndex == 0) ? 1 : 0;
                RexNode [] newOperands = new RexNode[2];
                newOperands[castIndex] = ensureType(real8, operands[castIndex]);
                newOperands[otherIndex] = operands[otherIndex];
                return builder.makeCall(
                    call.getOperator(),
                    newOperands);
            }

            analyzeOperands(operands);
            if (call.isA(RexKind.Plus)) {
                return expandPlusMinus(call, operands);
            } else if (call.isA(RexKind.Minus)) {
                return expandPlusMinus(call, operands);
            } else if (call.isA(RexKind.Divide)) {
                return expandDivide(call, operands);
            } else if (call.isA(RexKind.Times)) {
                return expandTimes(call, operands);
            } else if (call.isA(RexKind.Comparison)) {
                return expandComparison(call, operands);
            } else if (call.getOperator() == SqlStdOperatorTable.modFunc) {
                return expandMod(call, operands);
            } else {
                throw Util.newInternal(
                    "ReduceDecimalsRule could not expand "
                    + call.getOperator());
            }
        }

        /**
         * Convenience method for reading characteristics of operands (such as
         * scale, precision, whole digits) into an ArithmeticExpander. The
         * operands are restricted by the following contraints:
         *
         * <ul>
         * <li>there are exactly two operands
         * <li>both are exact numeric types
         * </ul>
         */
        private void analyzeOperands(RexNode [] operands)
        {
            assert (operands.length == 2);
            typeA = operands[0].getType();
            typeB = operands[1].getType();
            assert (SqlTypeUtil.isExactNumeric(typeA)
                && SqlTypeUtil.isExactNumeric(typeB));

            scaleA = typeA.getScale();
            scaleB = typeB.getScale();
        }

        private RexNode expandPlusMinus(RexCall call, RexNode [] operands)
        {
            RelDataType outType = call.getType();
            int outScale = outType.getScale();
            return encodeValue(
                builder.makeCall(
                    call.getOperator(),
                    ensureScale(
                        accessValue(operands[0]),
                        scaleA,
                        outScale),
                    ensureScale(
                        accessValue(operands[1]),
                        scaleB,
                        outScale)),
                outType);
        }

        private RexNode expandDivide(RexCall call, RexNode [] operands)
        {
            RelDataType outType = call.getType();
            RexNode dividend =
                builder.makeCall(
                    call.getOperator(),
                    ensureType(
                        real8,
                        accessValue(operands[0])),
                    ensureType(
                        real8,
                        accessValue(operands[1])));
            int scaleDifference = outType.getScale() - scaleA + scaleB;
            RexNode rescale =
                builder.makeCall(
                    SqlStdOperatorTable.multiplyOperator,
                    dividend,
                    makeApproxScaleFactor(scaleDifference));
            return encodeValue(rescale, outType);
        }

        private RexNode expandTimes(RexCall call, RexNode [] operands)
        {
            // Multiplying the internal values of the two arguments leads to
            // a number with scale = scaleA + scaleB. If the result type has
            // a lower scale, then the number should be scaled down.
            int divisor = scaleA + scaleB - call.getType().getScale();

            if (builder.getTypeFactory().useDoubleMultiplication(
                    typeA,
                    typeB))
            {
                // Approximate implementation:
                // cast (a as double) * cast (b as double)
                //     / 10^divisor
                RexNode division =
                    makeDivide(
                        makeMultiply(
                            ensureType(real8, accessValue(operands[0])),
                            ensureType(real8, accessValue(operands[1]))),
                        makeApproxLiteral(BigDecimal.TEN.pow(divisor)));
                return encodeValue(division, call.getType(), true);
            } else {
                // Exact implementation: scaleDown(a * b)
                return encodeValue(
                    scaleDown(
                        builder.makeCall(
                            call.getOperator(),
                            accessValue(operands[0]),
                            accessValue(operands[1])),
                        divisor),
                    call.getType());
            }
        }

        private RexNode expandComparison(RexCall call, RexNode [] operands)
        {
            int commonScale = Math.max(scaleA, scaleB);
            return builder.makeCall(
                call.getOperator(),
                ensureScale(
                    accessValue(operands[0]),
                    scaleA,
                    commonScale),
                ensureScale(
                    accessValue(operands[1]),
                    scaleB,
                    commonScale));
        }

        private RexNode expandMod(RexCall call, RexNode [] operands)
        {
            assert SqlTypeUtil.isExactNumeric(typeA);
            assert SqlTypeUtil.isExactNumeric(typeB);
            if ((scaleA != 0) || (scaleB != 0)) {
                throw EigenbaseResource.instance().ArgumentMustHaveScaleZero.ex(
                    call.getOperator().getName());
            }
            RexNode result =
                builder.makeCall(
                    call.getOperator(),
                    accessValue(operands[0]),
                    accessValue(operands[1]));
            RelDataType retType = call.getType();
            if (SqlTypeUtil.isDecimal(retType)) {
                return encodeValue(result, retType);
            }
            return ensureType(
                call.getType(),
                result);
        }
    }

    /**
     * Expander that rewrites floor(decimal) expressions:
     *
     * <pre>
     * if (value < 0)
     *     (value-0.99...)/(10^scale)
     * else
     *     value/(10^scale)
     * </pre>
     */
    private class FloorExpander
        extends RexExpander
    {
        private FloorExpander(RexBuilder rexBuilder)
        {
            super(rexBuilder);
        }

        public RexNode expand(RexCall call)
        {
            assert call.getOperator() == SqlStdOperatorTable.floorFunc;
            RexNode decValue = call.operands[0];
            int scale = decValue.getType().getScale();
            RexNode value = decodeValue(decValue);

            RexNode rewrite;
            if (scale == 0) {
                rewrite = decValue;
            } else if (scale == SqlTypeName.MAX_NUMERIC_PRECISION) {
                rewrite =
                    makeCase(
                        makeIsNegative(value),
                        makeExactLiteral(-1),
                        makeExactLiteral(0));
            } else {
                RexNode round = makeExactLiteral(1 - powerOfTen(scale));
                RexNode scaleFactor = makeScaleFactor(scale);
                rewrite =
                    makeCase(
                        makeIsNegative(value),
                        makeDivide(
                            makePlus(value, round),
                            scaleFactor),
                        makeDivide(value, scaleFactor));
            }
            return encodeValue(
                rewrite,
                call.getType());
        }
    }

    /**
     * Expander that rewrites ceiling(decimal) expressions:
     *
     * <pre>
     * if (value > 0)
     *     (value+0.99...)/(10^scale)
     * else
     *     value/(10^scale)
     * </pre>
     */
    private class CeilExpander
        extends RexExpander
    {
        private CeilExpander(RexBuilder rexBuilder)
        {
            super(rexBuilder);
        }

        public RexNode expand(RexCall call)
        {
            assert call.getOperator() == SqlStdOperatorTable.ceilFunc;
            RexNode decValue = call.operands[0];
            int scale = decValue.getType().getScale();
            RexNode value = decodeValue(decValue);

            RexNode rewrite;
            if (scale == 0) {
                rewrite = decValue;
            } else if (scale == SqlTypeName.MAX_NUMERIC_PRECISION) {
                rewrite =
                    makeCase(
                        makeIsPositive(value),
                        makeExactLiteral(1),
                        makeExactLiteral(0));
            } else {
                RexNode round = makeExactLiteral(powerOfTen(scale) - 1);
                RexNode scaleFactor = makeScaleFactor(scale);
                rewrite =
                    makeCase(
                        makeIsPositive(value),
                        makeDivide(
                            makePlus(value, round),
                            scaleFactor),
                        makeDivide(value, scaleFactor));
            }
            return encodeValue(
                rewrite,
                call.getType());
        }
    }

    /**
     * Expander that rewrites case expressions, in place. Starting from:
     *
     * <pre>(when $cond then $val)+ else $default</pre>
     *
     * this expander casts all values to the return type. If the target type is
     * a decimal, then the values are then decoded. The result of expansion is
     * that the case operator no longer deals with decimals args. (The return
     * value is encoded if necessary.)
     *
     * <p>Note: a decimal type is returned iff arguments have decimals
     */
    private class CaseExpander
        extends RexExpander
    {
        private CaseExpander(RexBuilder rexBuilder)
        {
            super(rexBuilder);
        }

        public RexNode expand(RexCall call)
        {
            RelDataType retType = call.getType();
            int argCount = call.operands.length;
            RexNode [] newOperands = new RexNode[argCount];

            for (int i = 0; i < argCount; i++) {
                // skip case conditions
                if (((i % 2) == 0) && (i != (argCount - 1))) {
                    newOperands[i] = call.operands[i];
                    continue;
                }
                RexNode expr = ensureType(retType, call.operands[i], false);
                if (SqlTypeUtil.isDecimal(retType)) {
                    expr = decodeValue(expr);
                }
                newOperands[i] = expr;
            }

            RexNode newCall =
                builder.makeCall(
                    call.getOperator(),
                    newOperands);
            if (SqlTypeUtil.isDecimal(retType)) {
                newCall = encodeValue(newCall, retType);
            }
            return newCall;
        }
    }

    /**
     * An expander that substitutes decimals with their integer representations.
     * If the output is decimal, the output is reinterpreted from the integer
     * representation into a decimal.
     */
    private class PassThroughExpander
        extends RexExpander
    {
        private PassThroughExpander(RexBuilder builder)
        {
            super(builder);
        }

        public boolean canExpand(RexCall call)
        {
            return RexUtil.requiresDecimalExpansion(call, false);
        }

        public RexNode expand(RexCall call)
        {
            RexNode [] operands = call.operands;
            RexNode [] newOperands = new RexNode[operands.length];
            for (int i = 0; i < operands.length; i++) {
                if (SqlTypeUtil.isNumeric(operands[i].getType())) {
                    newOperands[i] = accessValue(operands[i]);
                } else {
                    newOperands[i] = operands[i];
                }
            }

            RexNode newCall =
                builder.makeCall(
                    call.getOperator(),
                    newOperands);
            if (SqlTypeUtil.isDecimal(call.getType())) {
                return encodeValue(
                    newCall,
                    call.getType());
            } else {
                return newCall;
            }
        }
    }

    /**
     * An expander which casts decimal arguments as doubles
     */
    private class CastArgAsDoubleExpander
        extends CastArgAsTypeExpander
    {
        private CastArgAsDoubleExpander(RexBuilder builder)
        {
            super(builder);
        }

        public RelDataType getArgType(RexCall call, int ordinal)
        {
            RelDataType type = real8;
            if (call.operands[ordinal].getType().isNullable()) {
                type =
                    builder.getTypeFactory().createTypeWithNullability(
                        type,
                        true);
            }
            return type;
        }
    }

    /**
     * An expander which casts decimal arguments as another type
     */
    private abstract class CastArgAsTypeExpander
        extends RexExpander
    {
        private CastArgAsTypeExpander(RexBuilder builder)
        {
            super(builder);
        }

        public abstract RelDataType getArgType(RexCall call, int ordinal);

        public RexNode expand(RexCall call)
        {
            RexNode [] operands = call.operands;
            RexNode [] newOperands = new RexNode[operands.length];

            for (int i = 0; i < operands.length; i++) {
                RelDataType targetType = getArgType(call, i);
                if (SqlTypeUtil.isDecimal(operands[i].getType())) {
                    newOperands[i] = ensureType(targetType, operands[i], true);
                } else {
                    newOperands[i] = operands[i];
                }
            }

            RexNode ret =
                builder.makeCall(
                    call.getOperator(),
                    newOperands);
            ret =
                ensureType(
                    call.getType(),
                    ret,
                    true);
            return ret;
        }
    }

    /**
     * This expander simplifies reinterpret calls. Consider (1.0+1)*1. The inner
     * operation encodes a decimal (Reinterpret(...)) which the outer operation
     * immediately decodes: (Reinterpret(Reinterpret(...))). Arithmetic overflow
     * is handled by underlying integer operations, so we don't have to consider
     * it. Simply remove the nested Reinterpret.
     */
    private class ReinterpretExpander
        extends RexExpander
    {
        private ReinterpretExpander(RexBuilder builder)
        {
            super(builder);
        }

        public boolean canExpand(RexCall call)
        {
            return call.isA(RexKind.Reinterpret)
                && call.operands[0].isA(RexKind.Reinterpret);
        }

        public RexNode expand(RexCall call)
        {
            RexNode [] operands = call.operands;
            RexCall subCall = (RexCall) operands[0];
            RexNode innerValue = subCall.operands[0];
            if (canSimplify(call, subCall, innerValue)) {
                return innerValue.clone();
            }
            return call;
        }

        /**
         * Detect, in a generic, but strict way, whether it is possible to
         * simplify a reinterpret cast. The rules are as follows:
         *
         * <ol>
         * <li>If value is not the same basic type as outer, then we cannot
         * simplify
         * <li>If the value is nullable but the inner or outer are not, then we
         * cannot simplify.
         * <li>If inner is nullable but outer is not, we cannot simplify.
         * <li>If an overflow check is required from either inner or outer, we
         * cannot simplify.
         * <li>Otherwise, given the same type, and sufficient nullability
         * constraints, we can simplify.
         * </ol>
         *
         * @param outer outer call to reinterpret
         * @param inner inner call to reinterpret
         * @param value inner value
         *
         * @return whether the two reinterpret casts can be removed
         */
        private boolean canSimplify(
            RexCall outer,
            RexCall inner,
            RexNode value)
        {
            RelDataType outerType = outer.getType();
            RelDataType innerType = inner.getType();
            RelDataType valueType = value.getType();
            boolean outerCheck = RexUtil.canReinterpretOverflow(outer);
            boolean innerCheck = RexUtil.canReinterpretOverflow(inner);

            if ((outerType.getSqlTypeName() != valueType.getSqlTypeName())
                || (outerType.getPrecision() != valueType.getPrecision())
                || (outerType.getScale() != valueType.getScale()))
            {
                return false;
            }
            if (valueType.isNullable()
                && (!innerType.isNullable() || !outerType.isNullable()))
            {
                return false;
            }
            if (innerType.isNullable() && !outerType.isNullable()) {
                return false;
            }

            // One would think that we could go from Nullable -> Not Nullable
            // since we are substituting a general type with a more specific
            // type. However the optimizer doesn't like it.
            if (valueType.isNullable() != outerType.isNullable()) {
                return false;
            }
            if (innerCheck || outerCheck) {
                return false;
            }
            return true;
        }
    }
}

// End ReduceDecimalsRule.java
