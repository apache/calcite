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

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;


/**
 * PushProjector is a utility class used to perform operations used in push
 * projection rules.
 *
 * <p>Pushing is particularly interesting in the case of join, because there
 * are multiple inputs. Generally an expression can be pushed down to a
 * particular input if it depends upon no other inputs. If it can be pushed
 * down to both sides, it is pushed down to the left.
 *
 * <p>Sometimes an expression needs to be split before it can be pushed down.
 * To flag that an expression cannot be split, specify a rule that it must be
 * <dfn>preserved</dfn>. Such an expression will be pushed down intact to one
 * of the inputs, or not pushed down at all.</p>
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class PushProjector
{
    //~ Instance fields --------------------------------------------------------

    private final ProjectRel origProj;
    private final RexNode origFilter;
    private final RelNode childRel;
    private final ExprCondition preserveExprCondition;

    /**
     * Original projection expressions
     */
    final RexNode [] origProjExprs;

    /**
     * Fields from the RelNode that the projection is being pushed past
     */
    final RelDataTypeField [] childFields;

    /**
     * Number of fields in the RelNode that the projection is being pushed past
     */
    final int nChildFields;

    /**
     * Bitmap containing the references in the original projection
     */
    final BitSet projRefs;

    /**
     * Bitmap containing the fields in the RelNode that the projection is being
     * pushed past, if the RelNode is not a join. If the RelNode is a join, then
     * the fields correspond to the left hand side of the join.
     */
    final BitSet childBitmap;

    /**
     * Bitmap containing the fields in the right hand side of a join, in the
     * case where the projection is being pushed past a join. Not used
     * otherwise.
     */
    BitSet rightBitmap;

    /**
     * Number of fields in the RelNode that the projection is being pushed past,
     * if the RelNode is not a join. If the RelNode is a join, then this is the
     * number of fields in the left hand side of the join.
     *
     * <p>The identity
     * {@code nChildFields == nSysFields + nFields + nFieldsRight}
     * holds. {@code nFields} does not include {@code nSysFields}.
     * The output of a join looks like this:
     *
     * <blockquote><pre>
     * | nSysFields | nFields | nFieldsRight |
     * </pre></blockquote>
     *
     * The output of a single-input rel looks like this:
     *
     * <blockquote><pre>
     * | nSysFields | nFields |
     * </pre></blockquote>
     */
    final int nFields;

    /**
     * Number of fields in the right hand side of a join, in the case where the
     * projection is being pushed past a join. Always 0 otherwise.
     */
    final int nFieldsRight;

    /**
     * Number of system fields. System fields appear at the start of a join,
     * before the first field from the left input.
     */
    private final int nSysFields;

    /**
     * Expressions referenced in the projection/filter that should be preserved.
     * In the case where the projection is being pushed past a join, then the
     * list only contains the expressions corresponding to the left hand side of
     * the join.
     */
    final List<RexNode> childPreserveExprs;

    /**
     * Expressions referenced in the projection/filter that should be preserved,
     * corresponding to expressions on the right hand side of the join, if the
     * projection is being pushed past a join. Empty list otherwise.
     */
    final List<RexNode> rightPreserveExprs;

    /**
     * Number of system fields being projected.
     */
    int nSystemProject;

    /**
     * Number of fields being projected. In the case where the projection is
     * being pushed past a join, the number of fields being projected from the
     * left hand side of the join.
     */
    int nProject;

    /**
     * Number of fields being projected from the right hand side of a join, in
     * the case where the projection is being pushed past a join. 0 otherwise.
     */
    int nRightProject;

    /**
     * Rex builder used to create new expressions.
     */
    final RexBuilder rexBuilder;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a PushProjector object for pushing projects past a RelNode.
     *
     * @param origProj the original projection that is being pushed; may be null
     * if the projection is implied as a result of a projection having been
     * trivially removed
     * @param origFilter the filter that the projection must also be pushed
     * past, if applicable
     * @param childRel the RelNode that the projection is being pushed past
     * @param preserveExprCondition condition for whether an expression should
     * be preserved in the projection
     */
    public PushProjector(
        ProjectRel origProj,
        RexNode origFilter,
        RelNode childRel,
        ExprCondition preserveExprCondition)
    {
        this.origProj = origProj;
        this.origFilter = origFilter;
        this.childRel = childRel;
        this.preserveExprCondition = preserveExprCondition;

        if (origProj == null) {
            origProjExprs = new RexNode[] {};
        } else {
            origProjExprs = origProj.getChildExps();
        }

        childFields = childRel.getRowType().getFields();
        nChildFields = childFields.length;

        projRefs = new BitSet(nChildFields);
        if (childRel instanceof JoinRelBase) {
            JoinRelBase joinRel = (JoinRelBase) childRel;
            RelDataTypeField [] leftFields =
                joinRel.getLeft().getRowType().getFields();
            RelDataTypeField [] rightFields =
                joinRel.getRight().getRowType().getFields();
            nFields = leftFields.length;
            nFieldsRight = rightFields.length;
            childBitmap = new BitSet(nFields);
            rightBitmap = new BitSet(nFieldsRight);
            nSysFields = joinRel.getSystemFieldList().size();
            RelOptUtil.setRexInputBitmap(
                childBitmap,
                nSysFields,
                nFields + nSysFields);
            RelOptUtil.setRexInputBitmap(
                rightBitmap,
                nFields + nSysFields,
                nChildFields);
        } else {
            nFields = nChildFields;
            nFieldsRight = 0;
            childBitmap = new BitSet(nChildFields);
            nSysFields = 0;
            RelOptUtil.setRexInputBitmap(childBitmap, 0, nChildFields);
        }
        assert nChildFields == nSysFields + nFields + nFieldsRight;

        childPreserveExprs = new ArrayList<RexNode>();
        rightPreserveExprs = new ArrayList<RexNode>();

        rexBuilder = childRel.getCluster().getRexBuilder();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Decomposes a projection to the input references referenced by a
     * projection and a filter, either of which is optional. If both are
     * provided, the filter is underneath the project.
     *
     * <p>Creates a projection containing all input references as well as
     * preserving any special expressions. Converts the original projection
     * and/or filter to reference the new projection. Then, finally puts on top,
     * a final projection corresponding to the original projection.
     *
     * @param defaultExpr expression to be used in the projection if no fields
     * or special columns are selected
     *
     * @return the converted projection if it makes sense to push elements of
     * the projection; otherwise returns null
     */
    public ProjectRel convertProject(RexNode defaultExpr)
    {
        // locate all fields referenced in the projection and filter
        locateAllRefs();

        // if all columns are being selected (either explicitly in the
        // projection) or via a "select *", then there needs to be some
        // special expressions to preserve in the projection; otherwise,
        // there's no point in proceeding any further
        if (origProj == null) {
            if (childPreserveExprs.size() == 0) {
                return null;
            }

            // even though there is no projection, this is the same as
            // selecting all fields
            RelOptUtil.setRexInputBitmap(projRefs, 0, nChildFields);
            nProject = nChildFields;
        } else if (
            (projRefs.cardinality() == nChildFields)
            && (childPreserveExprs.size() == 0))
        {
            return null;
        }

        // if nothing is being selected from the underlying rel, just
        // project the default expression passed in as a parameter or the
        // first column if there is no default expression
        if ((projRefs.cardinality() == 0) && (childPreserveExprs.size() == 0)) {
            if (defaultExpr != null) {
                childPreserveExprs.add(defaultExpr);
            } else if (nChildFields == 1) {
                return null;
            } else {
                projRefs.set(0);
                nProject = 1;
            }
        }

        // create a new projection referencing all fields referenced in
        // either the project or the filter
        RelNode newProject = createProjectRefsAndExprs(childRel, false, false);

        int [] adjustments = getAdjustments();

        // if a filter was passed in, convert it to reference the projected
        // columns, placing it on top of the project just created
        RelNode projChild;
        if (origFilter != null) {
            RexNode newFilter =
                convertRefsAndExprs(
                    origFilter,
                    newProject.getRowType().getFields(),
                    adjustments);
            projChild = CalcRel.createFilter(newProject, newFilter);
        } else {
            projChild = newProject;
        }

        // put the original project on top of the filter/project, converting
        // it to reference the modified projection list; otherwise, create
        // a projection that essentially selects all fields
        ProjectRel topProject = createNewProject(projChild, adjustments);

        return topProject;
    }

    /**
     * Locates all references found in either the projection expressions a
     * filter, as well as references to expressions that should be preserved.
     * Based on that, determines whether pushing the projection makes sense.
     *
     * @return true if all inputs from the child that the projection is being
     * pushed past are referenced in the projection/filter and no special
     * preserve expressions are referenced; in that case, it does not make sense
     * to push the projection
     */
    public boolean locateAllRefs()
    {
        new InputSpecialOpFinder(
            projRefs,
            childBitmap,
            rightBitmap,
            preserveExprCondition,
            childPreserveExprs,
            rightPreserveExprs).apply(origProjExprs, origFilter);
        nSystemProject = 0;
        nProject = 0;
        nRightProject = 0;
        for (
            int bit = projRefs.nextSetBit(0);
            bit >= 0;
            bit = projRefs.nextSetBit(bit + 1))
        {
            if (bit < nSysFields) {
                nSystemProject++;
            } else if (bit < nSysFields + nFields) {
                nProject++;
            } else {
                nRightProject++;
            }
        }

        assert nSystemProject + nProject + nRightProject
            == projRefs.cardinality();

        if ((childRel instanceof JoinRelBase)
            || (childRel instanceof SetOpRel))
        {
            // if nothing is projected from the children, arbitrarily project
            // the first columns; this is necessary since Fennel doesn't
            // handle 0-column projections
            if ((nProject == 0) && (childPreserveExprs.size() == 0)) {
                projRefs.set(0);
                nProject = 1;
            }
            if (childRel instanceof JoinRelBase) {
                if ((nRightProject == 0) && (rightPreserveExprs.size() == 0)) {
                    projRefs.set(nFields);
                    nRightProject = 1;
                }
            }
        }

        // no need to push projections if all children fields are being
        // referenced and there are no special preserve expressions; note
        // that we need to do this check after we've handled the 0-column
        // project cases
        if (((projRefs.cardinality() == nChildFields)
                && (childPreserveExprs.size() == 0)
                && (rightPreserveExprs.size() == 0)))
        {
            return true;
        }

        return false;
    }

    /**
     * Creates a projection based on the inputs specified in a bitmap and the
     * expressions that need to be preserved. The expressions are appended after
     * the input references.
     *
     * @param projChild child that the projection will be created on top of
     * @param adjust if true, need to create new projection expressions;
     * otherwise, the existing ones are reused
     * @param rightSide if true, creating a projection for the right hand side
     * of a join
     *
     * @return created projection
     */
    public ProjectRel createProjectRefsAndExprs(
        RelNode projChild,
        boolean adjust,
        boolean rightSide)
    {
        List<RexNode> preserveExprs;
        int nInputRefs;
        int offset;

        if (rightSide) {
            preserveExprs = rightPreserveExprs;
            nInputRefs = nRightProject;
            offset = nSysFields + nFields;
        } else {
            preserveExprs = childPreserveExprs;
            nInputRefs = nProject;
            offset = nSysFields;
        }
        int refIdx = offset - 1;
        int projLength = nInputRefs + preserveExprs.size();
        RexNode [] newProjExprs = new RexNode[projLength];
        String [] fieldNames = new String[projLength];
        RelDataTypeField [] destFields = projChild.getRowType().getFields();
        int i;

        // add on the input references
        for (i = 0; i < nInputRefs; i++) {
            refIdx = projRefs.nextSetBit(refIdx + 1);
            assert (refIdx >= 0);
            newProjExprs[i] =
                rexBuilder.makeInputRef(
                    destFields[refIdx - offset].getType(),
                    refIdx - offset);
            fieldNames[i] = destFields[refIdx - offset].getName();
        }

        // add on the expressions that need to be preserved, converting the
        // arguments to reference the projected columns (if necessary)
        int [] adjustments = {};
        if ((preserveExprs.size() > 0) && adjust) {
            adjustments = new int[childFields.length];
            for (int idx = offset; idx < childFields.length; idx++) {
                adjustments[idx] = -offset;
            }
        }
        for (RexNode projExpr : preserveExprs) {
            RexNode newExpr;
            if (adjust) {
                newExpr =
                    projExpr.accept(
                        new RelOptUtil.RexInputConverter(
                            rexBuilder,
                            childFields,
                            destFields,
                            adjustments));
            } else {
                newExpr = projExpr;
            }
            newProjExprs[i] = newExpr;
            RexCall call = (RexCall) projExpr;
            fieldNames[i] = call.getOperator().getName();
            i++;
        }

        return (ProjectRel) CalcRel.createProject(
            projChild,
            newProjExprs,
            fieldNames);
    }

    /**
     * Determines how much each input reference needs to be adjusted as a result
     * of projection
     *
     * @return array indicating how much each input needs to be adjusted by
     */
    public int [] getAdjustments()
    {
        int [] adjustments = new int[nChildFields];
        int newIdx = 0;
        int rightOffset = childPreserveExprs.size();
        for (
            int pos = projRefs.nextSetBit(0);
            pos >= 0;
            pos = projRefs.nextSetBit(pos + 1))
        {
            adjustments[pos] = -(pos - newIdx);
            if (pos >= nSysFields + nFields) {
                adjustments[pos] += rightOffset;
            }
            newIdx++;
        }
        return adjustments;
    }

    /**
     * Clones an expression tree and walks through it, adjusting each
     * RexInputRef index by some amount, and converting expressions that need to
     * be preserved to field references.
     *
     * @param rex the expression
     * @param destFields fields that the new expressions will be referencing
     * @param adjustments the amount each input reference index needs to be
     * adjusted by
     *
     * @return modified expression tree
     */
    public RexNode convertRefsAndExprs(
        RexNode rex,
        RelDataTypeField [] destFields,
        int [] adjustments)
    {
        return rex.accept(
            new RefAndExprConverter(
                rexBuilder,
                childFields,
                destFields,
                adjustments,
                childPreserveExprs,
                nProject,
                rightPreserveExprs,
                nProject + childPreserveExprs.size() + nRightProject));
    }

    /**
     * Creates a new projection based on the original projection, adjusting all
     * input refs using an adjustment array passed in. If there was no original
     * projection, create a new one that selects every field from the underlying
     * rel
     *
     * @param projChild child of the new project
     * @param adjustments array indicating how much each input reference should
     * be adjusted by
     *
     * @return the created projection
     */
    public ProjectRel createNewProject(RelNode projChild, int [] adjustments)
    {
        RexNode [] projExprs;
        String [] fieldNames;
        RexNode [] origProjExprs = null;
        int origProjLength;
        if (origProj == null) {
            origProjLength = childFields.length;
        } else {
            origProjExprs = origProj.getChildExps();
            origProjLength = origProjExprs.length;
        }
        projExprs = new RexNode[origProjLength];
        fieldNames = new String[origProjLength];

        if (origProj != null) {
            for (int i = 0; i < origProjLength; i++) {
                projExprs[i] =
                    convertRefsAndExprs(
                        origProjExprs[i],
                        projChild.getRowType().getFields(),
                        adjustments);
                fieldNames[i] = origProj.getRowType().getFields()[i].getName();
            }
        } else {
            for (int i = 0; i < origProjLength; i++) {
                projExprs[i] =
                    rexBuilder.makeInputRef(
                        childFields[i].getType(),
                        i);
                fieldNames[i] = childFields[i].getName();
            }
        }

        ProjectRel projRel =
            (ProjectRel) CalcRel.createProject(
                projChild,
                projExprs,
                fieldNames);

        return projRel;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Visitor which builds a bitmap of the inputs used by an expressions, as
     * well as locating expressions corresponding to special operators.
     */
    private class InputSpecialOpFinder
        extends RexVisitorImpl<Void>
    {
        private final BitSet rexRefs;
        private final BitSet leftFields;
        private final BitSet rightFields;
        private final ExprCondition preserveExprCondition;
        private final List<RexNode> preserveLeft;
        private final List<RexNode> preserveRight;

        public InputSpecialOpFinder(
            BitSet rexRefs,
            BitSet leftFields,
            BitSet rightFields,
            ExprCondition preserveExprCondition,
            List<RexNode> preserveLeft,
            List<RexNode> preserveRight)
        {
            super(true);
            this.rexRefs = rexRefs;
            this.leftFields = leftFields;
            this.rightFields = rightFields;
            this.preserveExprCondition = preserveExprCondition;
            this.preserveLeft = preserveLeft;
            this.preserveRight = preserveRight;
        }

        public Void visitCall(RexCall call)
        {
            if (preserve(call)) {
                return null;
            }
            super.visitCall(call);
            return null;
        }

        private boolean preserve(RexNode call)
        {
            if (preserveExprCondition.test(call)) {
                // if the arguments of the expression only reference the
                // left hand side, preserve it on the left; similarly, if
                // it only references expressions on the right
                int totalFields = leftFields.size();
                if (rightFields != null) {
                    totalFields += rightFields.size();
                }
                final BitSet exprArgs = RelOptUtil.InputFinder.bits(call);
                if (exprArgs.cardinality() > 0) {
                    if (RelOptUtil.contains(leftFields, exprArgs)) {
                        addExpr(preserveLeft, call);
                        return true;
                    } else if (RelOptUtil.contains(rightFields, exprArgs)) {
                        assert (preserveRight != null);
                        addExpr(preserveRight, call);
                        return true;
                    }
                }
                // if the expression arguments reference both the left and
                // right, fall through and don't attempt to preserve the
                // expression, but instead locate references and special
                // ops in the call operands
            }
            return false;
        }

        public Void visitInputRef(RexInputRef inputRef)
        {
            rexRefs.set(inputRef.getIndex());
            return null;
        }

        /**
         * Applies this visitor to an array of expressions and an optional
         * single expression.
         */
        public void apply(RexNode [] exprs, RexNode expr)
        {
            RexProgram.apply(this, exprs, expr);
        }

        /**
         * Adds an expression to a list if the same expression isn't already in
         * the list. Expressions are identical if their digests are the same.
         *
         * @param exprList current list of expressions
         * @param newExpr new expression to be added
         */
        private void addExpr(List<RexNode> exprList, RexNode newExpr)
        {
            String newExprString = newExpr.toString();
            for (RexNode expr : exprList) {
                if (newExprString.compareTo(expr.toString()) == 0) {
                    return;
                }
            }
            exprList.add(newExpr);
        }
    }

    /**
     * Walks an expression tree, replacing input refs with new values to reflect
     * projection and converting special expressions to field references.
     */
    private class RefAndExprConverter
        extends RelOptUtil.RexInputConverter
    {
        private final List<RexNode> preserveLeft;
        private final int firstLeftRef;
        private final List<RexNode> preserveRight;
        private final int firstRightRef;

        public RefAndExprConverter(
            RexBuilder rexBuilder,
            RelDataTypeField [] srcFields,
            RelDataTypeField [] destFields,
            int [] adjustments,
            List<RexNode> preserveLeft,
            int firstLeftRef,
            List<RexNode> preserveRight,
            int firstRightRef)
        {
            super(rexBuilder, srcFields, destFields, adjustments);
            this.preserveLeft = preserveLeft;
            this.firstLeftRef = firstLeftRef;
            this.preserveRight = preserveRight;
            this.firstRightRef = firstRightRef;
        }

        public RexNode visitCall(RexCall call)
        {
            // if the expression corresponds to one that needs to be preserved,
            // convert it to a field reference; otherwise, convert the entire
            // expression
            int match =
                findExprInLists(
                    call,
                    preserveLeft,
                    firstLeftRef,
                    preserveRight,
                    firstRightRef);
            if (match >= 0) {
                return rexBuilder.makeInputRef(
                    destFields[match].getType(),
                    match);
            }
            return super.visitCall(call);
        }

        /**
         * Looks for a matching RexNode from among two lists of RexNodes and
         * returns the offset into the list corresponding to the match, adjusted
         * by an amount, depending on whether the match was from the first or
         * second list.
         *
         * @param rex RexNode that is being matched against
         * @param rexList1 first list of RexNodes
         * @param adjust1 adjustment if match occurred in first list
         * @param rexList2 second list of RexNodes
         * @param adjust2 adjustment if match occurred in the second list
         *
         * @return index in the list corresponding to the matching RexNode; -1
         * if no match
         */
        private int findExprInLists(
            RexNode rex,
            List<RexNode> rexList1,
            int adjust1,
            List<RexNode> rexList2,
            int adjust2)
        {
            int match = findExprInList(rex, rexList1);
            if (match >= 0) {
                return match + adjust1;
            }

            if (rexList2 != null) {
                match = findExprInList(rex, rexList2);
                if (match >= 0) {
                    return match + adjust2;
                }
            }

            return -1;
        }

        private int findExprInList(RexNode rex, List<RexNode> rexList)
        {
            int match = 0;
            for (RexNode rexElement : rexList) {
                if (rexElement.toString().compareTo(rex.toString()) == 0) {
                    return match;
                }
                match++;
            }
            return -1;
        }
    }

    /**
     * A functor that replies true or false for a given expression.
     *
     * @see org.eigenbase.rel.rules.PushProjector.OperatorExprCondition
     */
    public interface ExprCondition
    {
        /**
         * Evaluates a condition for a given expression.
         *
         * @param expr Expression
         * @return result of evaluating the condition
         */
        boolean test(RexNode expr);

        /**
         * Constant condition that replies {@code false} for all expressions.
         */
        public static final ExprCondition FALSE =
            new ExprCondition()
            {
                public boolean test(RexNode expr)
                {
                    return false;
                }
            };
    }

    /**
     * An expression condition that evaluates to true if the expression is
     * a call to one of a set of operators.
     */
    public static class OperatorExprCondition implements ExprCondition
    {
        private final Set<SqlOperator> operatorSet;

        /**
         * Creates an OperatorExprCondition.
         *
         * @param operatorSet Set of operators
         */
        public OperatorExprCondition(Set<SqlOperator> operatorSet)
        {
            this.operatorSet = operatorSet;
        }

        public boolean test(RexNode expr)
        {
            return expr instanceof RexCall
                && operatorSet.contains(
                    ((RexCall) expr).getOperator());
        }
    }
}

// End PushProjector.java
