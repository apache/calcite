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
package org.eigenbase.sql;

import java.math.*;

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * An operator describing a window specification.
 *
 * <p>Operands are as follows:
 *
 * <ul>
 * <li>0: name of referenced window ({@link SqlIdentifier})</li>
 * <li>1: partition clause ({@link SqlNodeList})</li>
 * <li>2: order clause ({@link SqlNodeList})</li>
 * <li>3: isRows ({@link SqlLiteral})</li>
 * <li>4: lowerBound ({@link SqlNode})</li>
 * <li>5: upperBound ({@link SqlNode})</li>
 * </ul>
 *
 * All operands are optional.</p>
 *
 * @author jhyde
 * @version $Id$
 * @since Oct 19, 2004
 */
public class SqlWindowOperator
    extends SqlOperator
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * The FOLLOWING operator used exclusively in a window specification.
     */
    static final SqlPostfixOperator followingOperator =
        new SqlPostfixOperator(
            "FOLLOWING",
            SqlKind.FOLLOWING,
            20,
            null,
            null,
            null);

    /**
     * The PRECEDING operator used exclusively in a window specification.
     */
    static final SqlPostfixOperator precedingOperator =
        new SqlPostfixOperator(
            "PRECEDING",
            SqlKind.PRECEDING,
            20,
            null,
            null,
            null);

    //~ Enums ------------------------------------------------------------------

    /**
     * An enumeration of types of bounds in a window: <code>CURRENT ROW</code>,
     * <code>UNBOUNDED PRECEDING</code>, and <code>UNBOUNDED FOLLOWING</code>.
     */
    enum Bound
        implements SqlLiteral.SqlSymbol
    {
        CURRENT_ROW("CURRENT ROW"), UNBOUNDED_PRECEDING("UNBOUNDED PRECEDING"),
        UNBOUNDED_FOLLOWING("UNBOUNDED FOLLOWING");
        private final String sql;

        Bound(String sql)
        {
            this.sql = sql;
        }

        public String toString()
        {
            return sql;
        }

        /** Creates a parse-tree node representing an occurrence of this bound
         * type at a particular position in the parsed text. */
        public SqlNode symbol(SqlParserPos pos) {
            return SqlLiteral.createSymbol(this, pos);
        }
    }

    //~ Constructors -----------------------------------------------------------

    public SqlWindowOperator()
    {
        super("WINDOW", SqlKind.WINDOW, 2, true, null, null, null);
    }

    //~ Methods ----------------------------------------------------------------

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Special;
    }

    public SqlCall createCall(
        SqlLiteral functionQualifier,
        SqlParserPos pos,
        SqlNode ... operands)
    {
        assert functionQualifier == null;
        return new SqlWindow(this, operands, pos);
    }

    public SqlWindow createCall(
        SqlIdentifier declName,
        SqlIdentifier refName,
        SqlNodeList partitionList,
        SqlNodeList orderList,
        SqlLiteral isRows,
        SqlNode lowerBound,
        SqlNode upperBound,
        SqlLiteral allowPartial,
        SqlParserPos pos)
    {
        // If there's only one bound and it's 'FOLLOWING', make it the upper
        // bound.
        if ((upperBound == null)
            && (lowerBound != null)
            && lowerBound.getKind() == SqlKind.FOLLOWING)
        {
            upperBound = lowerBound;
            lowerBound = null;
        }
        return (SqlWindow) createCall(
            pos,
            declName,
            refName,
            partitionList,
            orderList,
            isRows,
            lowerBound,
            upperBound,
            allowPartial);
    }

    public <R> void acceptCall(
        SqlVisitor<R> visitor,
        SqlCall call,
        boolean onlyExpressions,
        SqlBasicVisitor.ArgHandler<R> argHandler)
    {
        if (onlyExpressions) {
            for (int i = 0; i < call.operands.length; i++) {
                SqlNode operand = call.operands[i];

                // if the second parm is an Identifier then it's supposed to
                // be a name from a window clause and isn't part of the
                // group by check
                if (operand == null) {
                    continue;
                }
                if ((i == SqlWindow.RefName_OPERAND)
                    && (operand instanceof SqlIdentifier))
                {
                    continue;
                }
                argHandler.visitChild(visitor, call, i, operand);
            }
        } else {
            super.acceptCall(visitor, call, onlyExpressions, argHandler);
        }
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        final SqlWriter.Frame frame =
            writer.startList(SqlWriter.FrameTypeEnum.Window, "(", ")");
        SqlIdentifier refName =
            (SqlIdentifier) operands[SqlWindow.RefName_OPERAND];
        if (refName != null) {
            refName.unparse(writer, 0, 0);
        }
        SqlNodeList partitionList =
            (SqlNodeList) operands[SqlWindow.PartitionList_OPERAND];
        if (partitionList.size() > 0) {
            writer.sep("PARTITION BY");
            final SqlWriter.Frame partitionFrame = writer.startList("", "");
            partitionList.unparse(writer, 0, 0);
            writer.endList(partitionFrame);
        }
        SqlNodeList orderList =
            (SqlNodeList) operands[SqlWindow.OrderList_OPERAND];
        if (orderList.size() > 0) {
            writer.sep("ORDER BY");
            final SqlWriter.Frame orderFrame = writer.startList("", "");
            orderList.unparse(writer, 0, 0);
            writer.endList(orderFrame);
        }
        boolean isRows =
            SqlLiteral.booleanValue(operands[SqlWindow.IsRows_OPERAND]);
        SqlNode lowerBound = operands[SqlWindow.LowerBound_OPERAND],
            upperBound = operands[SqlWindow.UpperBound_OPERAND];
        if (lowerBound == null) {
            // No ROWS or RANGE clause
        } else if (upperBound == null) {
            if (isRows) {
                writer.sep("ROWS");
            } else {
                writer.sep("RANGE");
            }
            lowerBound.unparse(writer, 0, 0);
        } else {
            if (isRows) {
                writer.sep("ROWS BETWEEN");
            } else {
                writer.sep("RANGE BETWEEN");
            }
            lowerBound.unparse(writer, 0, 0);
            writer.keyword("AND");
            upperBound.unparse(writer, 0, 0);
        }

        // ALLOW PARTIAL/DISALLOW PARTIAL
        SqlNode allowPartial = operands[SqlWindow.AllowPartial_OPERAND];
        if (allowPartial == null) {
            ;
        } else if (SqlLiteral.booleanValue(allowPartial)) {
            // We could output "ALLOW PARTIAL", but this syntax is
            // non-standard. Omitting the clause has the same effect.
            ;
        } else {
            writer.keyword("DISALLOW PARTIAL");
        }

        writer.endList(frame);
    }

    public void validateCall(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlValidatorScope operandScope)
    {
        assert call.getOperator() == this;
        final SqlWindow window = (SqlWindow) call;
        final SqlCall windowCall = window.getWindowCall();
        SqlNode [] operands = call.operands;

        //        operandScope = validator.getScope(operands[0]);
        SqlIdentifier refName =
            (SqlIdentifier) operands[SqlWindow.RefName_OPERAND];
        if (refName != null) {
            SqlWindow win = validator.resolveWindow(call, operandScope, false);
            operands = win.operands;
        }

        SqlNodeList partitionList =
            (SqlNodeList) operands[SqlWindow.PartitionList_OPERAND];
        if (null != partitionList) {
            if (0 != partitionList.size()) {
                for (int i = 0; i < partitionList.size(); i++) {
                    SqlNode partitionItem = partitionList.get(i);
                    partitionItem.validateExpr(validator, operandScope);
                }
            } else {
                partitionList = null;
            }
        }

        SqlNodeList orderList =
            (SqlNodeList) operands[SqlWindow.OrderList_OPERAND];
        if (orderList != null) {
            if (0 != orderList.size()) {
                for (int i = 0; i < orderList.size(); i++) {
                    SqlNode orderItem = orderList.get(i);
                    boolean savedColumnReferenceExpansion =
                        validator.getColumnReferenceExpansion();
                    validator.setColumnReferenceExpansion(false);
                    try {
                        orderItem.validateExpr(validator, scope);
                    } finally {
                        validator.setColumnReferenceExpansion(
                            savedColumnReferenceExpansion);
                    }
                }
            } else {
                // list is empty so reset the base reference to null so
                // we don't need to keep checking two conditions
                orderList = null;
            }
        }

        boolean isRows =
            SqlLiteral.booleanValue(operands[SqlWindow.IsRows_OPERAND]);
        SqlNode lowerBound = operands[SqlWindow.LowerBound_OPERAND],
            upperBound = operands[SqlWindow.UpperBound_OPERAND];

        boolean triggerFunction = false;
        if (null != windowCall) {
            if (windowCall.isName("RANK") || windowCall.isName("DENSE_RANK")) {
                triggerFunction = true;
            }
        }

        // 6.10 rule 6a Function RANk & DENSE_RANK require OBC
        if ((null == orderList) && triggerFunction && !isTableSorted(scope)) {
            throw validator.newValidationError(
                call,
                EigenbaseResource.instance().FuncNeedsOrderBy.ex());
        }

        // Run framing checks if there are any
        if ((null != upperBound) || (null != lowerBound)) {
            // 6.10 Rule 6a
            if (triggerFunction) {
                throw validator.newValidationError(
                    operands[SqlWindow.IsRows_OPERAND],
                    EigenbaseResource.instance().RankWithFrame.ex());
            }
            SqlTypeFamily orderTypeFam = null;

            // SQL03 7.10 Rule 11a
            if (null != orderList) {
                // if order by is a conpound list then range not allowed
                if ((orderList.size() > 1) && !isRows) {
                    throw validator.newValidationError(
                        operands[SqlWindow.IsRows_OPERAND],
                        EigenbaseResource.instance()
                        .CompoundOrderByProhibitsRange.ex());
                }

                // get the type family for the sort key for Frame Boundary Val.
                RelDataType orderType =
                    validator.deriveType(
                        operandScope,
                        orderList.get(0));
                orderTypeFam =
                    SqlTypeFamily.getFamilyForSqlType(
                        orderType.getSqlTypeName());
            } else {
                // requires an ORDER BY clause if frame is logical(RANGE)
                // We relax this requirment if the table appears to be
                // sorted already
                if (!isRows && !isTableSorted(scope)) {
                    throw validator.newValidationError(
                        call,
                        EigenbaseResource.instance().OverMissingOrderBy.ex());
                }
            }

            // Let the bounds validate themselves
            validateFrameBoundary(
                lowerBound,
                isRows,
                orderTypeFam,
                validator,
                operandScope);
            validateFrameBoundary(
                upperBound,
                isRows,
                orderTypeFam,
                validator,
                operandScope);

            // Validate across boundries. 7.10 Rule 8 a-d
            checkSpecialLiterals(window, validator);
        } else if ((null == orderList) && !isTableSorted(scope)) {
            throw validator.newValidationError(
                call,
                EigenbaseResource.instance().OverMissingOrderBy.ex());
        }

        SqlNode allowPartialOperand = operands[SqlWindow.AllowPartial_OPERAND];
        boolean allowPartial =
            (allowPartialOperand == null)
            || SqlLiteral.booleanValue(allowPartialOperand);

        if (!isRows && !allowPartial) {
            throw validator.newValidationError(
                allowPartialOperand,
                EigenbaseResource.instance().CannotUseDisallowPartialWithRange
                .ex());
        }
    }

    private void validateFrameBoundary(
        SqlNode bound,
        boolean isRows,
        SqlTypeFamily orderTypeFam,
        SqlValidator validator,
        SqlValidatorScope scope)
    {
        if (null == bound) {
            return;
        }
        bound.validate(validator, scope);
        switch (bound.getKind()) {
        case LITERAL:

            // is there really anything to validate here? this covers
            // "CURRENT_ROW","unbounded preceding" & "unbounded following"
            break;

        case OTHER:
        case FOLLOWING:
        case PRECEDING:
            assert (bound instanceof SqlCall);
            final SqlNode boundVal = ((SqlCall) bound).getOperands()[0];

            // Boundries must be a constant
            if (!(boundVal instanceof SqlLiteral)) {
                throw validator.newValidationError(
                    boundVal,
                    EigenbaseResource.instance().RangeOrRowMustBeConstant.ex());
            }

            // SQL03 7.10 rule 11b Physical ROWS must be a numeric constant. JR:
            // actually it's SQL03 7.11 rule 11b "exact numeric with scale 0"
            // means not only numeric constant but exact numeric integral
            // constant. We also interpret the spec. to not allow negative
            // values, but allow zero.
            if (isRows) {
                if (boundVal instanceof SqlNumericLiteral) {
                    final SqlNumericLiteral boundLiteral =
                        (SqlNumericLiteral) boundVal;
                    if ((!boundLiteral.isExact())
                        || (boundLiteral.getScale() != 0)
                        || (0 > boundLiteral.longValue(true)))
                    { // true==throw if not exact (we just tested that - right?)
                        throw validator.newValidationError(
                            boundVal,
                            EigenbaseResource.instance()
                            .RowMustBeNonNegativeIntegral.ex());
                    }
                } else {
                    throw validator.newValidationError(
                        boundVal,
                        EigenbaseResource.instance()
                        .RowMustBeNonNegativeIntegral.ex());
                }
            }

            // if this is a range spec check and make sure the boundery type
            // and order by type are compatible
            if ((null != orderTypeFam) && !isRows) {
                RelDataType bndType = validator.deriveType(scope, boundVal);
                SqlTypeFamily bndTypeFam =
                    SqlTypeFamily.getFamilyForSqlType(
                        bndType.getSqlTypeName());
                switch (orderTypeFam) {
                case NUMERIC:
                    if (SqlTypeFamily.NUMERIC != bndTypeFam) {
                        throw validator.newValidationError(
                            boundVal,
                            EigenbaseResource.instance().OrderByRangeMismatch
                            .ex());
                    }
                    break;
                case DATE:
                case TIME:
                case TIMESTAMP:
                    if ((SqlTypeFamily.INTERVAL_DAY_TIME != bndTypeFam)
                        && (SqlTypeFamily.INTERVAL_YEAR_MONTH != bndTypeFam))
                    {
                        throw validator.newValidationError(
                            boundVal,
                            EigenbaseResource.instance().OrderByRangeMismatch
                            .ex());
                    }
                    break;
                default:
                    throw validator.newValidationError(
                        boundVal,
                        EigenbaseResource.instance()
                        .OrderByDataTypeProhibitsRange.ex());
                }
            }
            break;
        default:
            throw Util.newInternal("Unexpected node type");
        }
    }

    private static void checkSpecialLiterals(
        SqlWindow window,
        SqlValidator validator)
    {
        final SqlNode lowerBound = window.getLowerBound();
        final SqlNode upperBound = window.getUpperBound();
        Object lowerLitType = null;
        Object upperLitType = null;
        SqlOperator lowerOp = null;
        SqlOperator upperOp = null;
        if (null != lowerBound) {
            if (lowerBound.getKind() == SqlKind.LITERAL) {
                lowerLitType = ((SqlLiteral) lowerBound).getValue();
                if (Bound.UNBOUNDED_FOLLOWING == lowerLitType) {
                    throw validator.newValidationError(
                        lowerBound,
                        EigenbaseResource.instance().BadLowerBoundary.ex());
                }
            } else if (lowerBound instanceof SqlCall) {
                lowerOp = ((SqlCall) lowerBound).getOperator();
            }
        }
        if (null != upperBound) {
            if (upperBound.getKind() == SqlKind.LITERAL) {
                upperLitType = ((SqlLiteral) upperBound).getValue();
                if (Bound.UNBOUNDED_PRECEDING == upperLitType) {
                    throw validator.newValidationError(
                        upperBound,
                        EigenbaseResource.instance().BadUpperBoundary.ex());
                }
            } else if (upperBound instanceof SqlCall) {
                upperOp = ((SqlCall) upperBound).getOperator();
            }
        }

        if (Bound.CURRENT_ROW == lowerLitType) {
            if (null != upperOp) {
                if (upperOp == precedingOperator) {
                    throw validator.newValidationError(
                        upperBound,
                        EigenbaseResource.instance().CurrentRowPrecedingError
                        .ex());
                }
            }
        } else if (null != lowerOp) {
            if (lowerOp == followingOperator) {
                if (null != upperOp) {
                    if (upperOp == precedingOperator) {
                        throw validator.newValidationError(
                            upperBound,
                            EigenbaseResource.instance()
                            .FollowingBeforePrecedingError.ex());
                    }
                } else if (null != upperLitType) {
                    if (Bound.CURRENT_ROW == upperLitType) {
                        throw validator.newValidationError(
                            upperBound,
                            EigenbaseResource.instance()
                            .CurrentRowFollowingError.ex());
                    }
                }
            }
        }

        // Check that window size is non-negative. I would prefer to allow
        // negative windows and return NULL (as Oracle does) but this is
        // expedient.
        final OffsetRange offsetAndRange =
            getOffsetAndRange(lowerBound, upperBound, false);
        if (offsetAndRange == null) {
            throw validator.newValidationError(
                window,
                EigenbaseResource.instance()
                .UnboundedFollowingWindowNotSupported.ex());
        }
        if (offsetAndRange.range < 0) {
            throw validator.newValidationError(
                window,
                EigenbaseResource.instance().WindowHasNegativeSize.ex());
        }
    }

    /**
     * This method retrieves the list of columns for the current table then
     * walks through the list looking for a column that is monotonic (sorted)
     */
    private static boolean isTableSorted(SqlValidatorScope scope)
    {
        List<SqlMoniker> columnNames = new ArrayList<SqlMoniker>();

        // REVIEW: jhyde, 2007/11/7: This is the only use of
        // findAllColumnNames. Find a better way to detect monotonicity, then
        // remove that method.
        scope.findAllColumnNames(columnNames);
        for (SqlMoniker columnName : columnNames) {
            SqlIdentifier columnId = columnName.toIdentifier();
            final SqlMonotonicity monotonicity =
                scope.getMonotonicity(columnId);
            if (monotonicity != SqlMonotonicity.NotMonotonic) {
                return true;
            }
        }
        return false;
    }

    /**
     * Creates a window <code>(RANGE <i>columnName</i> CURRENT ROW)</code>.
     *
     * @param columnName Order column
     */
    public SqlWindow createCurrentRowWindow(final String columnName)
    {
        return createCall(
            null,
            null,
            new SqlNodeList(SqlParserPos.ZERO),
            new SqlNodeList(
                Collections.singletonList(
                    new SqlIdentifier(
                        new String[] { columnName },
                        SqlParserPos.ZERO)),
                SqlParserPos.ZERO),
            SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
            createCurrentRow(SqlParserPos.ZERO),
            createCurrentRow(SqlParserPos.ZERO),
            SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
            SqlParserPos.ZERO);
    }

    /**
     * Creates a window <code>(RANGE <i>columnName</i> UNBOUNDED
     * PRECEDING)</code>.
     *
     * @param columnName Order column
     */
    public SqlWindow createUnboundedPrecedingWindow(final String columnName)
    {
        return createCall(
            null,
            null,
            new SqlNodeList(SqlParserPos.ZERO),
            new SqlNodeList(
                Collections.singletonList(
                    new SqlIdentifier(
                        new String[] { columnName },
                        SqlParserPos.ZERO)),
                SqlParserPos.ZERO),
            SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
            createUnboundedPreceding(SqlParserPos.ZERO),
            createCurrentRow(SqlParserPos.ZERO),
            SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
            SqlParserPos.ZERO);
    }

    public static SqlNode createCurrentRow(SqlParserPos pos)
    {
        return Bound.CURRENT_ROW.symbol(pos);
    }

    public static SqlNode createUnboundedFollowing(SqlParserPos pos)
    {
        return Bound.UNBOUNDED_FOLLOWING.symbol(pos);
    }

    public static SqlNode createUnboundedPreceding(SqlParserPos pos)
    {
        return Bound.UNBOUNDED_PRECEDING.symbol(pos);
    }

    public static SqlNode createFollowing(SqlLiteral literal, SqlParserPos pos)
    {
        return followingOperator.createCall(pos, literal);
    }

    public static SqlNode createPreceding(SqlLiteral literal, SqlParserPos pos)
    {
        return precedingOperator.createCall(pos, literal);
    }

    public static SqlNode createBound(SqlLiteral range)
    {
        return range;
    }

    /**
     * Returns whether an expression represents the "CURRENT ROW" bound.
     */
    public static boolean isCurrentRow(SqlNode node)
    {
        return (node instanceof SqlLiteral)
            && (SqlLiteral.symbolValue(node) == Bound.CURRENT_ROW);
    }

    /**
     * Returns whether an expression represents the "UNBOUNDED PRECEDING" bound.
     */
    public static boolean isUnboundedPreceding(SqlNode node)
    {
        return (node instanceof SqlLiteral)
            && (SqlLiteral.symbolValue(node) == Bound.UNBOUNDED_PRECEDING);
    }

    /**
     * Returns whether an expression represents the "UNBOUNDED FOLLOWING" bound.
     */
    public static boolean isUnboundedFollowing(SqlNode node)
    {
        return (node instanceof SqlLiteral)
            && (SqlLiteral.symbolValue(node) == Bound.UNBOUNDED_FOLLOWING);
    }

    /**
     * Converts a pair of bounds into a (range, offset) pair.
     *
     * <p>If the upper bound is unbounded, returns null, since that cannot be
     * represented as a (range, offset) pair. (The offset would be +infinity,
     * but what would the range be?)
     *
     * @param lowerBound Lower bound
     * @param upperBound Upper bound
     * @param physical Whether interval is physical (rows), as opposed to
     * logical (values)
     *
     * @return range-offset pair, or null
     */
    public static OffsetRange getOffsetAndRange(
        final SqlNode lowerBound,
        final SqlNode upperBound,
        boolean physical)
    {
        ValSign upper = getRangeOffset(upperBound, precedingOperator);
        ValSign lower = getRangeOffset(lowerBound, followingOperator);
        long offset;
        long range;
        if (upper == null) {
            // cannot represent this as a range-offset pair
            return null;
        } else if (lower == null) {
            offset = upper.signedVal();
            range = Long.MAX_VALUE;
        } else {
            offset = upper.signedVal();
            range = lower.signedVal() + upper.signedVal();
        }

        // if range is physical and crosses or touches zero (current row),
        // increase the size by one
        if (physical
            && (lower != null)
            && ((lower.sign != upper.sign)
                || (lower.val == 0)
                || (upper.val == 0)))
        {
            ++range;
        }
        return new OffsetRange(offset, range);
    }

    /**
     * Decodes a node, representing an upper or lower bound to a window, into a
     * range offset. For example, '3 FOLLOWING' is 3, '3 PRECEDING' is -3, and
     * 'UNBOUNDED PRECEDING' or 'UNBOUNDED FOLLOWING' is null.
     *
     * @param node Node representing window bound
     * @param op Either {@link #precedingOperator} or {@link #followingOperator}
     *
     * @return range
     */
    private static ValSign getRangeOffset(SqlNode node, SqlPostfixOperator op)
    {
        assert (op == precedingOperator)
            || (op == followingOperator);
        if (node == null) {
            return new ValSign(0, 1);
        } else if (node instanceof SqlLiteral) {
            SqlLiteral literal = (SqlLiteral) node;
            if (literal.getValue() == Bound.CURRENT_ROW) {
                return new ValSign(0, 1);
            } else if (
                (literal.getValue() == Bound.UNBOUNDED_FOLLOWING)
                && (op == precedingOperator))
            {
                return null;
            } else if (
                (literal.getValue() == Bound.UNBOUNDED_PRECEDING)
                && (op == followingOperator))
            {
                return null;
            } else {
                throw Util.newInternal("unexpected literal " + literal);
            }
        } else if (node instanceof SqlCall) {
            final SqlCall call = (SqlCall) node;
            long sign = (call.getOperator() == op) ? -1 : 1;
            SqlNode [] operands = call.getOperands();
            assert (operands.length == 1) && (operands[0] != null);
            SqlLiteral operand = (SqlLiteral) operands[0];
            Object obj = operand.getValue();
            long val;
            if (obj instanceof BigDecimal) {
                val = ((BigDecimal) obj).intValue();
            } else if (obj instanceof SqlIntervalLiteral.IntervalValue) {
                val =
                    SqlParserUtil.intervalToMillis(
                        (SqlIntervalLiteral.IntervalValue) obj);
            } else {
                val = 0;
            }
            return new ValSign(val, sign);
        } else {
            return new ValSign(0, 1);
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    public static class OffsetRange
    {
        public final long offset;
        public final long range;

        OffsetRange(long offset, long range)
        {
            this.offset = offset;
            this.range = range;
        }
    }

    private static class ValSign
    {
        long val;
        long sign;

        ValSign(long val, long sign)
        {
            this.val = val;
            this.sign = sign;
            assert (sign == 1) || (sign == -1);
        }

        long signedVal()
        {
            return val * sign;
        }
    }
}

// End SqlWindowOperator.java
