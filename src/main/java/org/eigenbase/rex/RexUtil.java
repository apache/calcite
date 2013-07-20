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
package org.eigenbase.rex;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;
import org.eigenbase.util.mapping.*;

/**
 * Utility methods concerning row-expressions.
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 23, 2003
 */
public class RexUtil
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Returns a guess for the selectivity of an expression.
     *
     * @param exp expression of interest, or null for none (implying a
     * selectivity of 1.0)
     *
     * @return guessed selectivity
     */
    public static double getSelectivity(RexNode exp)
    {
        if ((exp == null) || exp.isAlwaysTrue()) {
            return 1;
        }
        return 0.1;
    }

    /**
     * Returns a copy of an array of row-expressions.
     */
    public static RexNode [] clone(RexNode [] exps)
    {
        if (null == exps) {
            return null;
        }
        RexNode [] exps2 = new RexNode[exps.length];
        for (int i = 0; i < exps.length; i++) {
            exps2[i] = exps[i].clone();
        }
        return exps2;
    }

    /**
     * Generates a cast from one row type to another
     *
     * @param rexBuilder RexBuilder to use for constructing casts
     * @param lhsRowType target row type
     * @param rhsRowType source row type; fields must be 1-to-1 with lhsRowType,
     * in same order
     *
     * @return cast expressions
     */
    public static List<RexNode> generateCastExpressions(
        RexBuilder rexBuilder,
        RelDataType lhsRowType,
        RelDataType rhsRowType)
    {
        final List<RelDataTypeField> fieldList = rhsRowType.getFieldList();
        int n = fieldList.size();
        assert n == lhsRowType.getFieldCount()
            : "field count: lhs [" + lhsRowType + "] rhs [" + rhsRowType + "]";
        List<RexNode> rhsExps = new ArrayList<RexNode>();
        for (RelDataTypeField field : fieldList) {
            rhsExps.add(
                rexBuilder.makeInputRef(field.getType(), field.getIndex()));
        }
        return generateCastExpressions(rexBuilder, lhsRowType, rhsExps);
    }

    /**
     * Generates a cast for a row type.
     *
     * @param rexBuilder RexBuilder to use for constructing casts
     * @param lhsRowType target row type
     * @param rhsExps expressions to be cast
     *
     * @return cast expressions
     */
    public static List<RexNode> generateCastExpressions(
        RexBuilder rexBuilder,
        RelDataType lhsRowType,
        List<RexNode> rhsExps)
    {
        List<RelDataTypeField> lhsFields = lhsRowType.getFieldList();
        List<RexNode> castExps = new ArrayList<RexNode>();
        for (Pair<RelDataTypeField, RexNode> pair
            : Pair.zip(lhsFields, rhsExps, true))
        {
            RelDataTypeField lhsField = pair.left;
            RelDataType lhsType = lhsField.getType();
            final RexNode rhsExp = pair.right;
            RelDataType rhsType = rhsExp.getType();
            if (lhsType.equals(rhsType)) {
                castExps.add(rhsExp);
            } else {
                castExps.add(rexBuilder.makeCast(lhsType, rhsExp));
            }
        }
        return castExps;
    }

    /**
     * Returns whether a node represents the NULL value.
     *
     * <p>Examples:
     *
     * <ul>
     * <li>For {@link org.eigenbase.rex.RexLiteral} Unknown, returns false.
     * <li>For <code>CAST(NULL AS <i>type</i>)</code>, returns true if <code>
     * allowCast</code> is true, false otherwise.
     * <li>For <code>CAST(CAST(NULL AS <i>type</i>) AS <i>type</i>))</code>,
     * returns false.
     * </ul>
     */
    public static boolean isNullLiteral(
        RexNode node,
        boolean allowCast)
    {
        if (node instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) node;
            if (literal.getTypeName() == SqlTypeName.NULL) {
                assert (null == literal.getValue());
                return true;
            } else {
                // We don't regard UNKNOWN -- SqlLiteral(null,Boolean) -- as
                // NULL.
                return false;
            }
        }
        if (allowCast) {
            if (node.isA(RexKind.Cast)) {
                RexCall call = (RexCall) node;
                if (isNullLiteral(call.operands.get(0), false)) {
                    // node is "CAST(NULL as type)"
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns whether a node represents the NULL value or a series of nested
     * CAST(NULL as <TYPE>) calls<br>
     * For Example:<br>
     * isNull(CAST(CAST(NULL as INTEGER) AS VARCHAR(1))) returns true
     */
    public static boolean isNull(RexNode node)
    {
        /* Checks to see if the RexNode is null */
        return RexLiteral.isNullLiteral(node)
            || ((node.getKind() == RexKind.Cast)
                && isNull(((RexCall) node).operands.get(0)));
    }

    /**
     * Returns whether a given node contains a RexCall with a specified operator
     *
     * @param operator to look for
     * @param node a RexNode tree
     */
    public static RexCall findOperatorCall(
        final SqlOperator operator,
        RexNode node)
    {
        try {
            RexVisitor<Void> visitor =
                new RexVisitorImpl<Void>(true) {
                    public Void visitCall(RexCall call)
                    {
                        if (call.getOperator().equals(operator)) {
                            throw new Util.FoundOne(call);
                        }
                        return super.visitCall(call);
                    }
                };
            node.accept(visitor);
            return null;
        } catch (Util.FoundOne e) {
            Util.swallow(e, null);
            return (RexCall) e.getNode();
        }
    }

    /**
     * Returns whether a given tree contains any {link RexInputRef} nodes.
     *
     * @param node a RexNode tree
     */
    public static boolean containsInputRef(
        RexNode node)
    {
        try {
            RexVisitor<Void> visitor =
                new RexVisitorImpl<Void>(true) {
                    public Void visitInputRef(RexInputRef inputRef)
                    {
                        throw new Util.FoundOne(inputRef);
                    }
                };
            node.accept(visitor);
            return false;
        } catch (Util.FoundOne e) {
            Util.swallow(e, null);
            return true;
        }
    }

    /**
     * Returns whether a given tree contains any {@link
     * org.eigenbase.rex.RexFieldAccess} nodes.
     *
     * @param node a RexNode tree
     */
    public static boolean containsFieldAccess(RexNode node) {
        try {
            RexVisitor<Void> visitor =
                new RexVisitorImpl<Void>(true) {
                    public Void visitFieldAccess(RexFieldAccess fieldAccess)
                    {
                        throw new Util.FoundOne(fieldAccess);
                    }
                };
            node.accept(visitor);
            return false;
        } catch (Util.FoundOne e) {
            Util.swallow(e, null);
            return true;
        }
    }

    /**
     * Determines whether a {@link RexCall} requires decimal expansion. It
     * usually requires expansion if it has decimal operands.
     *
     * <p>Exceptions to this rule are:
     *
     * <ul>
     * <li>isNull doesn't require expansion
     * <li>It's okay to cast decimals to and from char types
     * <li>It's okay to cast nulls as decimals
     * <li>Casts require expansion if their return type is decimal
     * <li>Reinterpret casts can handle a decimal operand
     * </ul>
     *
     * @param expr expression possibly in need of expansion
     * @param recurse whether to check nested calls
     *
     * @return whether the expression requires expansion
     */
    public static boolean requiresDecimalExpansion(
        RexNode expr,
        boolean recurse)
    {
        if (!(expr instanceof RexCall)) {
            return false;
        }
        RexCall call = (RexCall) expr;

        boolean localCheck = true;
        switch (call.getKind()) {
        case Reinterpret:
        case IsNull:
            localCheck = false;
            break;
        case Cast:
            RelDataType lhsType = call.getType();
            RelDataType rhsType = call.operands.get(0).getType();
            if (rhsType.getSqlTypeName() == SqlTypeName.NULL) {
                return false;
            }
            if (SqlTypeUtil.inCharFamily(lhsType)
                || SqlTypeUtil.inCharFamily(rhsType))
            {
                localCheck = false;
            } else if (SqlTypeUtil.isDecimal(lhsType)
                && (lhsType != rhsType))
            {
                return true;
            }
            break;
        default:
            localCheck = call.getOperator().requiresDecimalExpansion();
        }

        if (localCheck) {
            if (SqlTypeUtil.isDecimal(call.getType())) {
                // NOTE jvs 27-Mar-2007: Depending on the type factory, the
                // result of a division may be decimal, even though both inputs
                // are integer.
                return true;
            }
            for (int i = 0; i < call.operands.size(); i++) {
                if (SqlTypeUtil.isDecimal(call.operands.get(i).getType())) {
                    return true;
                }
            }
        }
        return recurse && requiresDecimalExpansion(call.operands, recurse);
    }

    /**
     * Determines whether any operand of a set requires decimal expansion
     */
    public static boolean requiresDecimalExpansion(
        List<RexNode> operands,
        boolean recurse)
    {
        for (RexNode operand : operands) {
            if (operand instanceof RexCall) {
                RexCall call = (RexCall) operand;
                if (requiresDecimalExpansion(call, recurse)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns whether a {@link RexProgram} contains expressions which require
     * decimal expansion.
     */
    public static boolean requiresDecimalExpansion(
        RexProgram program,
        boolean recurse)
    {
        final List<RexNode> exprList = program.getExprList();
        for (RexNode expr : exprList) {
            if (requiresDecimalExpansion(expr, recurse)) {
                return true;
            }
        }
        return false;
    }

    public static boolean canReinterpretOverflow(RexCall call)
    {
        assert (call.isA(RexKind.Reinterpret)) : "call is not a reinterpret";
        return call.operands.size() > 1;
    }

    /**
     * Returns whether an array of expressions has any common sub-expressions.
     */
    public static boolean containCommonExprs(List<RexNode> exprs, boolean fail)
    {
        final ExpressionNormalizer visitor = new ExpressionNormalizer(false);
        for (RexNode expr : exprs) {
            try {
                expr.accept(visitor);
            } catch (ExpressionNormalizer.SubExprExistsException e) {
                Util.swallow(e, null);
                assert !fail;
            }
        }
        return false;
    }

    /**
     * Returns whether an array of expressions contains a forward reference.
     * That is, if expression #i contains a {@link RexInputRef} referencing
     * field i or greater.
     *
     *
     * @param exprs Array of expressions
     * @param inputRowType Input row type
     * @param fail Whether to assert if there is a forward reference
     *
     * @return Whether there is a forward reference
     */
    public static boolean containForwardRefs(
        List<RexNode> exprs,
        RelDataType inputRowType,
        boolean fail)
    {
        final ForwardRefFinder visitor = new ForwardRefFinder(inputRowType);
        for (int i = 0; i < exprs.size(); i++) {
            RexNode expr = exprs.get(i);
            visitor.setLimit(i); // field cannot refer to self or later field
            try {
                expr.accept(visitor);
            } catch (ForwardRefFinder.IllegalForwardRefException e) {
                Util.swallow(e, null);
                assert !fail : "illegal forward reference in " + expr;
                return true;
            }
        }
        return false;
    }

    /**
     * Returns whether an array of exp contains aggregate function calls whose
     * arguments are not {@link RexInputRef}.s
     *
     * @param exprs Expressions
     * @param fail Whether to assert if there is such a function call
     */
    static boolean containNonTrivialAggs(List<RexNode> exprs, boolean fail)
    {
        for (RexNode expr : exprs) {
            if (expr instanceof RexCall) {
                RexCall rexCall = (RexCall) expr;
                if (rexCall.getOperator() instanceof SqlAggFunction) {
                    for (RexNode operand : rexCall.operands) {
                        if (!(operand instanceof RexLocalRef)) {
                            assert !fail : "contains non trivial agg";
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    /**
     * Returns whether a list of expressions contains complex expressions, that
     * is, a call whose arguments are not {@link RexVariable} (or a subtype such
     * as {@link RexInputRef}) or {@link RexLiteral}.
     */
    public static boolean containComplexExprs(List<RexNode> exprs)
    {
        for (RexNode expr : exprs) {
            if (expr instanceof RexCall) {
                for (RexNode operand : ((RexCall) expr).operands) {
                    if (!isAtomic(operand)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public static boolean isAtomic(RexNode expr)
    {
        return (expr instanceof RexLiteral) || (expr instanceof RexVariable);
    }

    /**
     * Returns whether a {@link RexNode node} is a {@link RexCall call} to a
     * given {@link SqlOperator operator}.
     */
    public static boolean isCallTo(RexNode expr, SqlOperator op)
    {
        return (expr instanceof RexCall)
            && (((RexCall) expr).getOperator() == op);
    }

    /**
     * Creates a record type with anonymous field names.
     *
     * @param typeFactory Type factory
     * @param exprs Expressions
     * @return Record type
     */
    public static RelDataType createStructType(
        RelDataTypeFactory typeFactory,
        final List<RexNode> exprs)
    {
        return createStructType(typeFactory, exprs, null);
    }

    /**
     * Creates a record type with specified field names.
     *
     * <p>The array of field names may be null, or any of the names within it
     * can be null. We recommend using explicit names where possible, because it
     * makes it much easier to figure out the intent of fields when looking at
     * planner output.
     *
     * @param typeFactory Type factory
     * @param exprs Expressions
     * @param names Field names, may be null, or elements may be null
     * @return Record type
     */
    public static RelDataType createStructType(
        RelDataTypeFactory typeFactory,
        final List<RexNode> exprs,
        final List<String> names)
    {
        return typeFactory.createStructType(
            new RelDataTypeFactory.FieldInfo() {
                public int getFieldCount()
                {
                    return exprs.size();
                }

                public String getFieldName(int index)
                {
                    if (names == null) {
                        return "$f" + index;
                    }
                    final String name = names.get(index);
                    if (name == null) {
                        return "$f" + index;
                    }
                    return name;
                }

                public RelDataType getFieldType(int index)
                {
                    return exprs.get(index).getType();
                }
            });
    }

    /**
     * Returns whether the type of an array of expressions is compatible with a
     * struct type.
     *
     *
     * @param exprs Array of expressions
     * @param type Type
     * @param fail Whether to fail if there is a mismatch
     *
     * @return Whether every expression has the same type as the corresponding
     * member of the struct type
     *
     * @see RelOptUtil#eq(String, RelDataType, String, RelDataType, boolean)
     */
    public static boolean compatibleTypes(
        List<RexNode> exprs,
        RelDataType type,
        boolean fail)
    {
        final List<RelDataTypeField> fields = type.getFieldList();
        if (exprs.size() != fields.size()) {
            assert !fail : "rowtype mismatches expressions";
            return false;
        }
        for (int i = 0; i < fields.size(); i++) {
            final RelDataType exprType = exprs.get(i).getType();
            final RelDataType fieldType = fields.get(i).getType();
            if (!RelOptUtil.eq("type1", exprType, "type2", fieldType, fail)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Creates a key for {@link RexNode} which is the same as another key of
     * another RexNode only if the two have both the same type and textual
     * representation. For example, "10" integer and "10" bigint result in
     * different keys.
     */
    public static String makeKey(RexNode expr)
    {
        String type = expr.getType().getFullTypeString();
        String separator = ";";
        String node = expr.toString();
        StringBuilder keyBuilder =
            new StringBuilder(
                type.length() + separator.length() + node.length());
        keyBuilder.append(type).append(separator).append(node);
        return keyBuilder.toString();
    }

    /**
     * Returns whether the leading edge of a given array of expressions is
     * wholly {@link RexInputRef} objects with types corresponding to the
     * underlying datatype.
     */
    public static boolean containIdentity(
        List<RexNode> exprs,
        RelDataType rowType,
        boolean fail)
    {
        final List<RelDataTypeField> fields = rowType.getFieldList();
        if (exprs.size() < fields.size()) {
            assert !fail : "exprs/rowType length mismatch";
            return false;
        }
        for (int i = 0; i < fields.size(); i++) {
            if (!(exprs.get(i) instanceof RexInputRef)) {
                assert !fail : "expr[" + i + "] is not a RexInputRef";
                return false;
            }
            RexInputRef inputRef = (RexInputRef) exprs.get(i);
            if (inputRef.getIndex() != i) {
                assert !fail : "expr[" + i + "] has ordinal "
                    + inputRef.getIndex();
                return false;
            }
            if (!RelOptUtil.eq("type1",
                exprs.get(i).getType(),
                "type2",
                fields.get(i).getType(),
                fail))
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Creates an AND expression from a list of RexNodes
     *
     * @param rexList list of RexNodes
     *
     * @return AND'd expression
     */
    public static RexNode andRexNodeList(
        RexBuilder rexBuilder,
        List<RexNode> rexList)
    {
        if (rexList.isEmpty()) {
            return null;
        }

        // create a right-deep tree to allow short-circuiting during
        // expression evaluation
        RexNode andExpr = rexList.get(rexList.size() - 1);
        for (int i = rexList.size() - 2; i >= 0; i--) {
            andExpr =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.andOperator,
                    rexList.get(i),
                    andExpr);
        }
        return andExpr;
    }

    /**
     * Creates an OR expression from a list of RexNodes
     *
     * @param rexList list of RexNodes
     *
     * @return OR'd expression
     */
    public static RexNode orRexNodeList(
        RexBuilder rexBuilder,
        List<RexNode> rexList)
    {
        if (rexList.isEmpty()) {
            return null;
        }

        RexNode orExpr = rexList.get(rexList.size() - 1);
        for (int i = rexList.size() - 2; i >= 0; i--) {
            orExpr =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.orOperator,
                    rexList.get(i),
                    orExpr);
        }
        return orExpr;
    }

    /**
     * Applies a mapping to a collation list.
     *
     * @param mapping Mapping
     * @param collationList Collation list
     * @return collation list with mapping applied to each field
     */
    public static List<RelCollation> apply(
        Mappings.TargetMapping mapping,
        List<RelCollation> collationList)
    {
        final List<RelCollation> newCollationList =
            new ArrayList<RelCollation>();
        for (RelCollation collation : collationList) {
            final List<RelFieldCollation> newFieldCollationList =
                new ArrayList<RelFieldCollation>();
            for (RelFieldCollation fieldCollation
                : collation.getFieldCollations())
            {
                final RelFieldCollation newFieldCollation =
                    apply(mapping, fieldCollation);
                if (newFieldCollation == null) {
                    // This field is not mapped. Stop here. The leading edge
                    // of the collation is still valid (although it's useless
                    // if it's empty).
                    break;
                }
                newFieldCollationList.add(newFieldCollation);
            }
            // Truncation to collations to their leading edge creates empty
            // and duplicate collations. Ignore these.
            if (!newFieldCollationList.isEmpty()) {
                final RelCollation newCollation =
                    RelCollationImpl.of(newFieldCollationList);
                if (!newCollationList.contains(newCollation)) {
                    newCollationList.add(newCollation);
                }
            }
        }

        // REVIEW: There might be redundant collations in the list. For example,
        // in {(x), (x, y)}, (x) is redundant because it is a leading edge of
        // another collation in the list. Could remove redundant collations.

        return newCollationList;
    }

    /**
     * Applies a mapping to a collation.
     *
     * @param mapping Mapping
     * @param collation Collation
     * @return collation with mapping applied
     */
    public static RelCollation apply(
        Mappings.TargetMapping mapping,
        RelCollation collation)
    {
        List<RelFieldCollation> fieldCollations =
            applyFields(mapping, collation.getFieldCollations());
        return fieldCollations.equals(collation.getFieldCollations())
            ? collation
            : RelCollationImpl.of(fieldCollations);
    }

    /**
     * Applies a mapping to a field collation.
     *
     * <p>If the field is not mapped, returns null.
     *
     * @param mapping Mapping
     * @param fieldCollation Field collation
     * @return collation with mapping applied
     */
    public static RelFieldCollation apply(
        Mappings.TargetMapping mapping,
        RelFieldCollation fieldCollation)
    {
        final int target =
            mapping.getTargetOpt(fieldCollation.getFieldIndex());
        if (target < 0) {
            return null;
        }
        return fieldCollation.copy(target);
    }

    /**
     * Applies a mapping to a list of field collations.
     *
     * @param mapping Mapping
     * @param fieldCollations Field collations
     * @return collations with mapping applied
     */
    public static List<RelFieldCollation> applyFields(
        Mappings.TargetMapping mapping,
        List<RelFieldCollation> fieldCollations)
    {
        final List<RelFieldCollation> newFieldCollations =
            new ArrayList<RelFieldCollation>(fieldCollations.size());
        for (RelFieldCollation fieldCollation : fieldCollations) {
            newFieldCollations.add(apply(mapping, fieldCollation));
        }
        return newFieldCollations;
    }

    /**
     * Applies a shuttle to an array of expressions. Creates a copy first.
     *
     * @param shuttle Shuttle
     * @param exprs Array of expressions
     */
    public static <T extends RexNode> T[] apply(
        RexVisitor<T> shuttle,
        T[] exprs)
    {
        T[] newExprs = exprs.clone();
        for (int i = 0; i < newExprs.length; i++) {
            final RexNode expr = newExprs[i];
            if (expr != null) {
                newExprs[i] = expr.accept(shuttle);
            }
        }
        return newExprs;
    }

    /**
     * Applies a visitor to an array of expressions and, if specified, a single
     * expression.
     *
     * @param visitor Visitor
     * @param exprs Array of expressions
     * @param expr Single expression, may be null
     */
    public static void apply(
        RexVisitor<Void> visitor,
        RexNode [] exprs,
        RexNode expr)
    {
        for (RexNode e : exprs) {
            e.accept(visitor);
        }
        if (expr != null) {
            expr.accept(visitor);
        }
    }

    /**
     * Applies a visitor to a list of expressions and, if specified, a single
     * expression.
     *
     * @param visitor Visitor
     * @param exprs List of expressions
     * @param expr Single expression, may be null
     */
    public static void apply(
        RexVisitor<Void> visitor,
        List<? extends RexNode> exprs,
        RexNode expr)
    {
        for (RexNode e : exprs) {
            e.accept(visitor);
        }
        if (expr != null) {
            expr.accept(visitor);
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Walks over expressions and builds a bank of common sub-expressions.
     */
    private static class ExpressionNormalizer
        extends RexVisitorImpl<RexNode>
    {
        final Map<String, RexNode> mapDigestToExpr =
            new HashMap<String, RexNode>();
        final boolean allowDups;

        protected ExpressionNormalizer(boolean allowDups)
        {
            super(true);
            this.allowDups = allowDups;
        }

        protected RexNode register(RexNode expr)
        {
            final String key = expr.toString();
            final RexNode previous = mapDigestToExpr.put(key, expr);
            if (!allowDups && (previous != null)) {
                throw new SubExprExistsException(expr);
            }
            return expr;
        }

        protected RexNode lookup(RexNode expr)
        {
            return mapDigestToExpr.get(expr.toString());
        }

        public RexNode visitInputRef(RexInputRef inputRef)
        {
            return register(inputRef);
        }

        public RexNode visitLiteral(RexLiteral literal)
        {
            return register(literal);
        }

        public RexNode visitCorrelVariable(RexCorrelVariable correlVariable)
        {
            return register(correlVariable);
        }

        public RexNode visitCall(RexCall call)
        {
            List<RexNode> normalizedOperands = new ArrayList<RexNode>();
            int diffCount = 0;
            for (RexNode operand : call.getOperands()) {
                operand.accept(this);
                final RexNode normalizedOperand = lookup(operand);
                normalizedOperands.add(normalizedOperand);
                if (normalizedOperand != operand) {
                    ++diffCount;
                }
            }
            if (diffCount > 0) {
                call =
                    call.clone(
                        call.getType(),
                        normalizedOperands);
            }
            return register(call);
        }

        public RexNode visitDynamicParam(RexDynamicParam dynamicParam)
        {
            return register(dynamicParam);
        }

        public RexNode visitRangeRef(RexRangeRef rangeRef)
        {
            return register(rangeRef);
        }

        public RexNode visitFieldAccess(RexFieldAccess fieldAccess)
        {
            final RexNode expr = fieldAccess.getReferenceExpr();
            expr.accept(this);
            final RexNode normalizedExpr = lookup(expr);
            if (normalizedExpr != expr) {
                fieldAccess =
                    new RexFieldAccess(
                        normalizedExpr,
                        fieldAccess.getField());
            }
            return register(fieldAccess);
        }

        /**
         * Thrown if there is a sub-expression.
         */
        private static class SubExprExistsException
            extends RuntimeException
        {
            SubExprExistsException(RexNode expr)
            {
                Util.discard(expr);
            }
        }
    }

    /**
     * Walks over an expression and throws an exception if it finds an {@link
     * RexInputRef} with an ordinal beyond the number of fields in the input row
     * type, or a {@link RexLocalRef} with ordinal greater than that set using
     * {@link #setLimit(int)}.
     */
    private static class ForwardRefFinder
        extends RexVisitorImpl<Void>
    {
        private int limit = -1;
        private final RelDataType inputRowType;

        public ForwardRefFinder(RelDataType inputRowType)
        {
            super(true);
            this.inputRowType = inputRowType;
        }

        public Void visitInputRef(RexInputRef inputRef)
        {
            super.visitInputRef(inputRef);
            if (inputRef.getIndex() >= inputRowType.getFieldCount()) {
                throw new IllegalForwardRefException();
            }
            return null;
        }

        public Void visitLocalRef(RexLocalRef inputRef)
        {
            super.visitLocalRef(inputRef);
            if (inputRef.getIndex() >= limit) {
                throw new IllegalForwardRefException();
            }
            return null;
        }

        public void setLimit(int limit)
        {
            this.limit = limit;
        }

        static class IllegalForwardRefException
            extends RuntimeException
        {
        }
    }

    /**
     * Visitor which builds a bitmap of the inputs used by an expression.
     */
    public static class FieldAccessFinder
        extends RexVisitorImpl<Void>
    {
        private final List<RexFieldAccess> fieldAccessList;

        public FieldAccessFinder()
        {
            super(true);
            fieldAccessList = new ArrayList<RexFieldAccess>();
        }

        public Void visitFieldAccess(RexFieldAccess fieldAccess)
        {
            fieldAccessList.add(fieldAccess);
            return null;
        }

        public Void visitCall(RexCall call)
        {
            for (RexNode operand : call.operands) {
                operand.accept(this);
            }
            return null;
        }

        public List<RexFieldAccess> getFieldAccessList()
        {
            return fieldAccessList;
        }
    }
}

// End RexUtil.java
