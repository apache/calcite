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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;

import com.google.common.collect.ImmutableList;

/**
 * A collection of expressions which read inputs, compute output expressions,
 * and optionally use a condition to filter rows.
 *
 * <p>Programs are immutable. It may help to use a {@link RexProgramBuilder},
 * which has the same relationship to {@link RexProgram} as {@link StringBuffer}
 * does has to {@link String}.
 *
 * <p>A program can contain aggregate functions. If it does, the arguments to
 * each aggregate function must be an {@link RexInputRef}.
 *
 * @author jhyde
 * @version $Id$
 * @see RexProgramBuilder
 * @since Aug 18, 2005
 */
public class RexProgram
{
    //~ Instance fields --------------------------------------------------------

    /**
     * First stage of expression evaluation. The expressions in this array can
     * refer to inputs (using input ordinal #0) or previous expressions in the
     * array (using input ordinal #1).
     */
    private final List<RexNode> exprs;

    /**
     * With {@link #condition}, the second stage of expression evaluation.
     */
    private final List<RexLocalRef> projects;

    /**
     * The optional condition. If null, the calculator does not filter rows.
     */
    private final RexLocalRef condition;

    private final RelDataType inputRowType;

    /**
     * Whether this program contains aggregates. TODO: obsolete this
     */
    private boolean aggs;
    private final RelDataType outputRowType;

    /**
     * Reference counts for each expression, computed on demand.
     */
    private int [] refCounts;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a program.
     *
     * @param inputRowType Input row type
     * @param exprs Common expressions
     * @param projects Projection expressions
     * @param condition Condition expression. If null, calculator does not
     * filter rows
     * @param outputRowType Description of the row produced by the program
     *
     * @pre !containCommonExprs(exprs)
     * @pre !containForwardRefs(exprs)
     * @pre !containNonTrivialAggs(exprs)
     */
    public RexProgram(
        RelDataType inputRowType,
        List<? extends RexNode> exprs,
        List<RexLocalRef> projects,
        RexLocalRef condition,
        RelDataType outputRowType)
    {
        this.inputRowType = inputRowType;
        this.exprs = ImmutableList.copyOf(exprs);
        this.projects = ImmutableList.copyOf(projects);
        this.condition = condition;
        this.outputRowType = outputRowType;
        assert isValid(true);
    }

    //~ Methods ----------------------------------------------------------------

    // REVIEW jvs 16-Oct-2006:  The description below is confusing.  I
    // think it means "none of the entries are null, there may be none,
    // and there is no further reduction into smaller common sub-expressions
    // possible"?
    /**
     * Returns the common sub-expressions of this program.
     *
     * <p>The list is never null but may be empty; each the expression in the
     * list is not null; and no further reduction into smaller common
     * sub-expressions is possible.
     *
     * @post return != null
     * @post !containCommonExprs(exprs)
     */
    public List<RexNode> getExprList()
    {
        return exprs;
    }

    /**
     * Returns an array of references to the expressions which this program is
     * to project. Never null, may be empty.
     */
    public List<RexLocalRef> getProjectList()
    {
        return projects;
    }

    /**
     * Returns the field reference of this program's filter condition, or null
     * if there is no condition.
     */
    public RexLocalRef getCondition()
    {
        return condition;
    }

    /**
     * Creates a program which calculates projections and filters rows based
     * upon a condition. Does not attempt to eliminate common sub-expressions.
     *
     *
     * @param projectExprs Project expressions
     * @param conditionExpr Condition on which to filter rows, or null if rows
     * are not to be filtered
     * @param outputRowType Output row type
     * @param rexBuilder Builder of rex expressions
     *
     * @return A program
     */
    public static RexProgram create(
        RelDataType inputRowType,
        List<RexNode> projectExprs,
        RexNode conditionExpr,
        RelDataType outputRowType,
        RexBuilder rexBuilder)
    {
        final RexProgramBuilder programBuilder =
            new RexProgramBuilder(inputRowType, rexBuilder);
        final List<RelDataTypeField> fields = outputRowType.getFieldList();
        for (int i = 0; i < projectExprs.size(); i++) {
            programBuilder.addProject(
                projectExprs.get(i),
                fields.get(i).getName());
        }
        if (conditionExpr != null) {
            programBuilder.addCondition(conditionExpr);
        }
        return programBuilder.getProgram();
    }

    // description of this calc, chiefly intended for debugging
    public String toString()
    {
        // Intended to produce similar output to explainCalc,
        // but without requiring a RelNode or RelOptPlanWriter.
        final RelOptPlanWriter pw =
            new RelOptPlanWriter(new PrintWriter(new StringWriter()));
        collectExplainTerms("", pw);
        return pw.simple();
    }

    /**
     * Writes an explanation of the expressions in this program to a plan
     * writer.
     *
     * @param pw Plan writer
     */
    public RelOptPlanWriter explainCalc(RelOptPlanWriter pw) {
        return collectExplainTerms("", pw, pw.getDetailLevel());
    }

    public RelOptPlanWriter collectExplainTerms(
        String prefix,
        RelOptPlanWriter pw)
    {
        return collectExplainTerms(
            prefix,
            pw,
            SqlExplainLevel.EXPPLAN_ATTRIBUTES);
    }

    /**
     * Collects the expressions in this program into a list of terms and values.
     *
     * @param prefix Prefix for term names, usually the empty string, but useful
     * if a relational expression contains more than one program
     * @param pw Plan writer
     */
    public RelOptPlanWriter collectExplainTerms(
        String prefix,
        RelOptPlanWriter pw,
        SqlExplainLevel level)
    {
        final List<RelDataTypeField> inFields = inputRowType.getFieldList();
        final List<RelDataTypeField> outFields = outputRowType.getFieldList();
        assert outFields.size() == projects.size() : "outFields.length="
            + outFields.size()
            + ", projects.length=" + projects.size();
        pw.item(
            prefix + "expr#0"
            + ((inFields.size() > 1) ? (".." + (inFields.size() - 1)) : ""),
            "{inputs}");
        for (int i = inFields.size(); i < exprs.size(); i++) {
            pw.item(prefix + "expr#" + i, exprs.get(i));
        }

        // If a lot of the fields are simply projections of the underlying
        // expression, try to be a bit less verbose.
        int trivialCount = 0;

        // Do not use the trivialCount optimization if computing digest for the
        // optimizer (as opposed to doing an explain plan).
        if (level != SqlExplainLevel.DIGEST_ATTRIBUTES) {
            trivialCount = countTrivial(projects);
        }

        switch (trivialCount) {
        case 0:
            break;
        case 1:
            trivialCount = 0;
            break;
        default:
            pw.item(prefix + "proj#0.." + (trivialCount - 1), "{exprs}");
            break;
        }

        // Print the non-trivial fields with their names as they appear in the
        // output row type.
        for (int i = trivialCount; i < projects.size(); i++) {
            pw.item(prefix + outFields.get(i).getName(), projects.get(i));
        }
        if (condition != null) {
            pw.item(prefix + "$condition", condition);
        }
        return pw;
    }

    /**
     * Returns the number of expressions at the front of an array which are
     * simply projections of the same field.
     *
     * @param refs References
     */
    private static int countTrivial(List<RexLocalRef> refs)
    {
        for (int i = 0; i < refs.size(); i++) {
            RexLocalRef ref = refs.get(i);
            if (ref.getIndex() != i) {
                return i;
            }
        }
        return refs.size();
    }

    /**
     * Returns the number of expressions in this program.
     */
    public int getExprCount()
    {
        return exprs.size()
            + projects.size()
            + ((condition == null) ? 0 : 1);
    }

    /**
     * Creates a copy of this program.
     */
    public RexProgram copy()
    {
        return new RexProgram(
            inputRowType,
            exprs,
            projects,
            (condition == null) ? null : condition.clone(),
            outputRowType);
    }

    /**
     * Creates the identity program.
     */
    public static RexProgram createIdentity(RelDataType rowType)
    {
        final List<RelDataTypeField> fields = rowType.getFieldList();
        final List<RexLocalRef> projectRefs = new ArrayList<RexLocalRef>();
        final List<RexInputRef> refs = new ArrayList<RexInputRef>();
        for (int i = 0; i < fields.size(); i++) {
            final RexInputRef ref = RexInputRef.of(i, fields);
            refs.add(ref);
            projectRefs.add(new RexLocalRef(i, ref.getType()));
        }
        return new RexProgram(rowType, refs, projectRefs, null, rowType);
    }

    /**
     * Returns the type of the input row to the program.
     *
     * @return input row type
     */
    public RelDataType getInputRowType()
    {
        return inputRowType;
    }

    /**
     * Returns whether this program contains windowed aggregate functions
     *
     * @return whether this program contains windowed aggregate functions
     */
    public boolean containsAggs()
    {
        return aggs || RexOver.containsOver(this);
    }

    public void setAggs(boolean aggs)
    {
        this.aggs = aggs;
    }

    /**
     * Returns the type of the output row from this program.
     *
     * @return output row type
     */
    public RelDataType getOutputRowType()
    {
        return outputRowType;
    }

    /**
     * Checks that this program is valid.
     *
     * <p>If <code>fail</code> is true, executes <code>assert false</code>, so
     * will throw an {@link AssertionError} if assertions are enabled. If <code>
     * fail</code> is false, merely returns whether the program is valid.
     *
     * @param fail Whether to fail
     *
     * @return Whether the program is valid
     *
     * @throws AssertionError if program is invalid and <code>fail</code> is
     * true and assertions are enabled
     */
    public boolean isValid(boolean fail)
    {
        if (inputRowType == null) {
            assert !fail;
            return false;
        }
        if (exprs == null) {
            assert !fail;
            return false;
        }
        if (projects == null) {
            assert !fail;
            return false;
        }
        if (outputRowType == null) {
            assert !fail;
            return false;
        }

        // If the input row type is a struct (contains fields) then the leading
        // expressions must be references to those fields. But we don't require
        // this if the input row type is, say, a java class.
        if (inputRowType.isStruct()) {
            if (!RexUtil.containIdentity(exprs, inputRowType, fail)) {
                assert !fail;
                return false;
            }

            // None of the other fields should be inputRefs.
            for (int i = inputRowType.getFieldCount(); i < exprs.size(); i++) {
                RexNode expr = exprs.get(i);
                if (expr instanceof RexInputRef) {
                    assert !fail;
                    return false;
                }
            }
        }
        if (false && RexUtil.containCommonExprs(exprs, fail)) { // todo: enable
            assert !fail;
            return false;
        }
        if (RexUtil.containForwardRefs(exprs, inputRowType, fail)) {
            assert !fail;
            return false;
        }
        if (RexUtil.containNonTrivialAggs(exprs, fail)) {
            assert !fail;
            return false;
        }
        final Checker checker =
            new Checker(
                fail,
                inputRowType,
                new AbstractList<RelDataType>()
                {
                    public RelDataType get(int index)
                    {
                        return exprs.get(index).getType();
                    }

                    @Override
                    public int size()
                    {
                        return exprs.size();
                    }
                });
        if (condition != null) {
            if (!SqlTypeUtil.inBooleanFamily(condition.getType())) {
                assert !fail : "condition must be boolean";
                return false;
            }
            condition.accept(checker);
            if (checker.failCount > 0) {
                assert !fail;
                return false;
            }
        }
        for (int i = 0; i < projects.size(); i++) {
            projects.get(i).accept(checker);
            if (checker.failCount > 0) {
                assert !fail;
                return false;
            }
        }
        for (int i = 0; i < exprs.size(); i++) {
            exprs.get(i).accept(checker);
            if (checker.failCount > 0) {
                assert !fail;
                return false;
            }
        }
        return true;
    }

    /**
     * Returns whether an expression always evaluates to null.
     *
     * <p/>Like {@link RexUtil#isNull(RexNode)}, null literals are null, and
     * casts of null literals are null. But this method also regards references
     * to null expressions as null.
     *
     * @param expr Expression
     *
     * @return Whether expression always evaluates to null
     */
    public boolean isNull(RexNode expr)
    {
        if (RexLiteral.isNullLiteral(expr)) {
            return true;
        }
        if (expr instanceof RexLocalRef) {
            RexLocalRef inputRef = (RexLocalRef) expr;
            return isNull(exprs.get(inputRef.index));
        }
        if (expr.getKind() == RexKind.Cast) {
            return isNull(((RexCall) expr).operands.get(0));
        }
        return false;
    }

    /**
     * Fully expands a RexLocalRef back into a pure RexNode tree containing no
     * RexLocalRefs (reversing the effect of common subexpression elimination).
     * For example, <code>program.expandLocalRef(program.getCondition())</code>
     * will return the expansion of a program's condition.
     *
     * @param ref a RexLocalRef from this program
     *
     * @return expanded form
     */
    public RexNode expandLocalRef(RexLocalRef ref)
    {
        // TODO jvs 19-Apr-2006:  assert that ref is part of
        // this program
        ExpansionShuttle shuttle = new ExpansionShuttle();
        return ref.accept(shuttle);
    }

    /**
     * Given a list of collations which hold for the input to this program,
     * returns a list of collations which hold for its output. The result is
     * mutable.
     */
    public List<RelCollation> getCollations(List<RelCollation> inputCollations)
    {
        List<RelCollation> outputCollations = new ArrayList<RelCollation>(1);
        deduceCollations(
            outputCollations,
            inputRowType.getFieldCount(), projects,
            inputCollations);
        return outputCollations;
    }

    /**
     * Given a list of expressions and a description of which are ordered,
     * computes a list of collations. The result is mutable.
     */
    public static void deduceCollations(
        List<RelCollation> outputCollations,
        final int sourceCount,
        List<RexLocalRef> refs,
        List<RelCollation> inputCollations)
    {
        int [] targets = new int[sourceCount];
        Arrays.fill(targets, -1);
        for (int i = 0; i < refs.size(); i++) {
            final RexLocalRef ref = refs.get(i);
            final int source = ref.getIndex();
            if ((source < sourceCount) && (targets[source] == -1)) {
                targets[source] = i;
            }
        }
loop:
        for (RelCollation collation : inputCollations) {
            final ArrayList<RelFieldCollation> fieldCollations =
                new ArrayList<RelFieldCollation>(0);
            for (
                RelFieldCollation fieldCollation
                : collation.getFieldCollations())
            {
                final int source = fieldCollation.getFieldIndex();
                final int target = targets[source];
                if (target < 0) {
                    continue loop;
                }
                fieldCollations.add(
                    fieldCollation.copy(target));
            }

            // Success -- all of the source fields of this key are mapped
            // to the output.
            outputCollations.add(RelCollationImpl.of(fieldCollations));
        }
    }

    /**
     * Returns whether the fields on the leading edge of the project list are
     * the input fields.
     *
     * @param fail Whether to throw an assert failure if does not project
     * identity
     */
    public boolean projectsIdentity(final boolean fail)
    {
        final int fieldCount = inputRowType.getFieldCount();
        if (projects.size() < fieldCount) {
            assert !fail : "program '" + toString()
                + "' does not project identity for input row type '"
                + inputRowType + "'";
            return false;
        }
        for (int i = 0; i < fieldCount; i++) {
            RexLocalRef project = projects.get(i);
            if (project.index != i) {
                assert !fail : "program " + toString()
                    + "' does not project identity for input row type '"
                    + inputRowType + "', field #" + i;
                return false;
            }
        }
        return true;
    }

    /**
     * Returns whether this program returns its input exactly.
     *
     * <p>This is a stronger condition than {@link #projectsIdentity(boolean)}.
     */
    public boolean isTrivial()
    {
        if (getCondition() != null) {
            return false;
        }
        if (projects.size() != inputRowType.getFieldCount()) {
            return false;
        }
        for (int i = 0; i < projects.size(); i++) {
            RexLocalRef project = projects.get(i);
            if (project.index != i) {
                return false;
            }
        }
        return true;
    }

    /**
     * Gets reference counts for each expression in the program, where the
     * references are detected from later expressions in the same program, as
     * well as the project list and condition. Expressions with references
     * counts greater than 1 are true common sub-expressions.
     *
     * @return array of reference counts; the ith element in the returned array
     * is the number of references to getExprList()[i]
     */
    public int [] getReferenceCounts()
    {
        if (refCounts != null) {
            return refCounts;
        }
        refCounts = new int[exprs.size()];
        ReferenceCounter refCounter = new ReferenceCounter();
        apply(refCounter, exprs, null);
        if (condition != null) {
            refCounter.visitLocalRef(condition);
        }
        for (RexLocalRef project : projects) {
            refCounter.visitLocalRef(project);
        }
        return refCounts;
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
        List<RexNode> exprs,
        RexNode expr)
    {
        for (RexNode expr0 : exprs) {
            expr0.accept(visitor);
        }
        if (expr != null) {
            expr.accept(visitor);
        }
    }

    /**
     * Returns whether an expression is constant.
     */
    public boolean isConstant(RexNode ref)
    {
        return ref.accept(new ConstantFinder());
    }

    public RexNode gatherExpr(RexNode expr)
    {
        return expr.accept(new Marshaller());
    }

    /**
     * Returns the input field that an output field is populated from, or -1 if
     * it is populated from an expression.
     */
    public int getSourceField(int outputOrdinal)
    {
        assert (outputOrdinal >= 0) && (outputOrdinal < this.projects.size());
        RexLocalRef project = projects.get(outputOrdinal);
        int index = project.index;
        while (true) {
            RexNode expr = exprs.get(index);
            if (expr instanceof RexCall
                && ((RexCall) expr).getOperator()
                == SqlStdOperatorTable.inFennelFunc)
            {
                // drill through identity function
                expr = ((RexCall) expr).getOperands().get(0);
            }
            if (expr instanceof RexLocalRef) {
                index = ((RexLocalRef) expr).index;
            } else if (expr instanceof RexInputRef) {
                return ((RexInputRef) expr).index;
            } else {
                return -1;
            }
        }
    }

    /**
     * Returns whether this program is a permutation of its inputs.
     */
    public boolean isPermutation()
    {
        if (projects.size() != inputRowType.getFieldList().size()) {
            return false;
        }
        for (int i = 0; i < projects.size(); ++i) {
            if (getSourceField(i) < 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns a permutation, if this program is a permutation, otherwise null.
     */
    public Permutation getPermutation()
    {
        Permutation permutation = new Permutation(projects.size());
        if (projects.size() != inputRowType.getFieldList().size()) {
            return null;
        }
        for (int i = 0; i < projects.size(); ++i) {
            int sourceField = getSourceField(i);
            if (sourceField < 0) {
                return null;
            }
            permutation.set(i, sourceField);
        }
        return permutation;
    }

    /**
     * Returns the set of correlation variables used (read) by this program.
     *
     * @return set of correlation variable names
     */
    public HashSet<String> getCorrelVariableNames()
    {
        final HashSet<String> paramIdSet = new HashSet<String>();
        apply(
            new RexVisitorImpl<Void>(true) {
                public Void visitCorrelVariable(
                    RexCorrelVariable correlVariable)
                {
                    paramIdSet.add(correlVariable.getName());
                    return null;
                }
            },
            exprs,
            null);
        return paramIdSet;
    }

    /**
     * Returns whether this program is in canonical form.
     *
     * @param fail Whether to throw an assertion error if not in canonical form
     * @param rexBuilder Rex builder
     * @return whether in canonical form
     */
    public boolean isNormalized(boolean fail, RexBuilder rexBuilder)
    {
        final RexProgram normalizedProgram =
            RexProgramBuilder.normalize(rexBuilder, this);
        String normalized = normalizedProgram.toString();
        String string = toString();
        if (!normalized.equals(string)) {
            assert !fail
                : "Program is not normalized:\n"
                + "program:    " + string + "\n"
                + "normalized: " + normalized + "\n";
            return false;
        }
        return true;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Visitor which walks over a program and checks validity.
     */
    static class Checker extends RexChecker
    {
        private final List<RelDataType> internalExprTypeList;

        /**
         * Creates a Checker.
         *
         * @param fail Whether to fail
         * @param inputRowType Types of the input fields
         * @param internalExprTypeList Types of the internal expressions
         */
        public Checker(
            boolean fail,
            RelDataType inputRowType,
            List<RelDataType> internalExprTypeList)
        {
            super(inputRowType, fail);
            this.internalExprTypeList = internalExprTypeList;
        }

        // override RexChecker; RexLocalRef is illegal in most rex expressions,
        // but legal in a program
        public Boolean visitLocalRef(RexLocalRef localRef)
        {
            final int index = localRef.getIndex();
            if ((index < 0) || (index >= internalExprTypeList.size())) {
                assert !fail;
                ++failCount;
                return false;
            }
            if (!RelOptUtil.eq(
                    "type1",
                    localRef.getType(),
                    "type2",
                    internalExprTypeList.get(index),
                    fail))
            {
                assert !fail;
                ++failCount;
                return false;
            }
            return true;
        }
    }

    /**
     * A RexShuttle used in the implementation of {@link
     * RexProgram#expandLocalRef}.
     */
    private class ExpansionShuttle
        extends RexShuttle
    {
        public RexNode visitLocalRef(RexLocalRef localRef)
        {
            RexNode tree = getExprList().get(localRef.getIndex());
            return tree.accept(this);
        }
    }

    /**
     * Walks over an expression and determines whether it is constant.
     */
    private class ConstantFinder
        implements RexVisitor<Boolean>
    {
        private ConstantFinder()
        {
        }

        public Boolean visitLiteral(RexLiteral literal)
        {
            return true;
        }

        public Boolean visitInputRef(RexInputRef inputRef)
        {
            return false;
        }

        public Boolean visitLocalRef(RexLocalRef localRef)
        {
            final RexNode expr = exprs.get(localRef.index);
            return expr.accept(this);
        }

        public Boolean visitOver(RexOver over)
        {
            return false;
        }

        public Boolean visitCorrelVariable(RexCorrelVariable correlVariable)
        {
            // Correlating variables are constant WITHIN A RESTART, so that's
            // good enough.
            return true;
        }

        public Boolean visitDynamicParam(RexDynamicParam dynamicParam)
        {
            // Dynamic parameters are constant WITHIN A RESTART, so that's
            // good enough.
            return true;
        }

        public Boolean visitCall(RexCall call)
        {
            // Constant if operator is deterministic and all operands are
            // constant.
            return call.getOperator().isDeterministic()
                && RexVisitorImpl.visitArrayAnd(
                    this,
                    call.getOperands());
        }

        public Boolean visitRangeRef(RexRangeRef rangeRef)
        {
            return false;
        }

        public Boolean visitFieldAccess(RexFieldAccess fieldAccess)
        {
            // "<expr>.FIELD" is constant iff "<expr>" is constant.
            return fieldAccess.getReferenceExpr().accept(this);
        }
    }

    /**
     * Given an expression in a program, creates a clone of the expression with
     * sub-expressions (represented by {@link RexLocalRef}s) fully expanded.
     */
    private class Marshaller
        extends RexVisitorImpl<RexNode>
    {
        Marshaller()
        {
            super(false);
        }

        public RexNode visitInputRef(RexInputRef inputRef)
        {
            return inputRef;
        }

        public RexNode visitLocalRef(RexLocalRef localRef)
        {
            final RexNode expr = exprs.get(localRef.index);
            return expr.accept(this);
        }

        public RexNode visitLiteral(RexLiteral literal)
        {
            return literal;
        }

        public RexNode visitCall(RexCall call)
        {
            final List<RexNode> newOperands = new ArrayList<RexNode>();
            for (RexNode operand : call.getOperands()) {
                newOperands.add(operand.accept(this));
            }
            return call.clone(call.getType(), newOperands);
        }

        public RexNode visitOver(RexOver over)
        {
            return visitCall(over);
        }

        public RexNode visitCorrelVariable(RexCorrelVariable correlVariable)
        {
            return correlVariable;
        }

        public RexNode visitDynamicParam(RexDynamicParam dynamicParam)
        {
            return dynamicParam;
        }

        public RexNode visitRangeRef(RexRangeRef rangeRef)
        {
            return rangeRef;
        }

        public RexNode visitFieldAccess(RexFieldAccess fieldAccess)
        {
            final RexNode referenceExpr =
                fieldAccess.getReferenceExpr().accept(this);
            return new RexFieldAccess(
                referenceExpr,
                fieldAccess.getField());
        }
    }

    /**
     * Visitor which marks which expressions are used.
     */
    private class ReferenceCounter
        extends RexVisitorImpl<Void>
    {
        ReferenceCounter()
        {
            super(true);
        }

        public Void visitLocalRef(RexLocalRef localRef)
        {
            final int index = localRef.getIndex();
            refCounts[index]++;
            return null;
        }
    }
}

// End RexProgram.java
