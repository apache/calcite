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
package org.eigenbase.oj.rel;

import java.util.List;
import java.util.logging.*;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.oj.rex.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.runtime.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.trace.*;
import org.eigenbase.util.*;

/**
 * <code>IterCalcRel</code> is an iterator implementation of a combination of
 * {@link ProjectRel} above an optional {@link FilterRel}. It takes a {@link
 * TupleIter iterator} as input, and for each row applies the filter condition
 * if defined. Rows passing the filter expression are transformed via projection
 * and returned. Note that the same object is always returned (with different
 * values), so parents must not buffer the result.
 *
 * <p>Rules:
 *
 * <ul>
 * <li>{@link org.eigenbase.oj.rel.IterRules.IterCalcRule} creates an
 * IterCalcRel from a {@link org.eigenbase.rel.CalcRel}</li>
 * </ul>
 */
public class IterCalcRel
    extends SingleRel
    implements JavaRel
{
    //~ Static fields/initializers ---------------------------------------------

    private static boolean abortOnError = true;
    private static boolean errorBuffering = false;

    //~ Instance fields --------------------------------------------------------

    private final RexProgram program;

    /**
     * Values defined in {@link org.eigenbase.rel.ProjectRelBase.Flags}.
     */
    protected int flags;

    private String tag;

    //~ Constructors -----------------------------------------------------------

    public IterCalcRel(
        RelOptCluster cluster,
        RelNode child,
        RexProgram program,
        int flags)
    {
        this(cluster, cluster.getEmptyTraitSet(), child, program, flags, null);
    }

    public IterCalcRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        RexProgram program,
        int flags,
        String tag)
    {
        super(
            cluster,
            traitSet.plus(CallingConvention.ITERATOR),
            child);
        this.flags = flags;
        this.program = program;
        this.rowType = program.getOutputRowType();
        this.tag = tag;
    }

    //~ Methods ----------------------------------------------------------------

    // TODO jvs 10-May-2004: need a computeSelfCost which takes condition into
    // account; maybe inherit from CalcRelBase?

    public RelOptPlanWriter explainTerms(RelOptPlanWriter pw) {
        return program.explainCalc(super.explainTerms(pw));
    }

    protected String computeDigest()
    {
        String tempDigest = super.computeDigest();
        if (tag != null) {
            // append logger type to digest
            int lastParen = tempDigest.lastIndexOf(')');
            tempDigest =
                tempDigest.substring(0, lastParen)
                + ",type=" + tag
                + tempDigest.substring(lastParen);
        }
        return tempDigest;
    }

    public double getRows()
    {
        return FilterRel.estimateFilteredRows(
            getChild(), program);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double dRows = RelMetadataQuery.getRowCount(this);
        double dCpu =
            RelMetadataQuery.getRowCount(getChild())
            * program.getExprCount();
        double dIo = 0;
        return planner.makeCost(dRows, dCpu, dIo);
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs)
    {
        return new IterCalcRel(
            getCluster(),
            traitSet,
            sole(inputs),
            program.copy(),
            getFlags(),
            tag);
    }

    public int getFlags()
    {
        return flags;
    }

    public boolean isBoxed()
    {
        return (flags & ProjectRelBase.Flags.Boxed)
            == ProjectRelBase.Flags.Boxed;
    }

    /**
     * Burrows into a synthetic record and returns the underlying relation which
     * provides the field called <code>fieldName</code>.
     */
    public JavaRel implementFieldAccess(
        JavaRelImplementor implementor,
        String fieldName)
    {
        if (!isBoxed()) {
            return implementor.implementFieldAccess(
                (JavaRel) getChild(),
                fieldName);
        }
        RelDataType type = getRowType();
        int field = type.getFieldOrdinal(fieldName);
        RexLocalRef ref = program.getProjectList().get(field);
        final int index = ref.getIndex();
        return implementor.findRel(
            (JavaRel) this,
            program.getExprList().get(index));
    }

    /**
     * Disables throwing of exceptions on error. Do not set this false without a
     * very good reason! Doing so will prevent type cast, overflow/underflow,
     * etc. errors in Farrago.
     */
    public static void setAbortOnError(boolean abortOnError)
    {
        IterCalcRel.abortOnError = abortOnError;
    }

    /**
     * Allows errors to be buffered, in the event that they overflow the error
     * handler.
     *
     * @param errorBuffering whether to buffer errors
     */
    public static void setErrorBuffering(boolean errorBuffering)
    {
        IterCalcRel.errorBuffering = errorBuffering;
    }

    public static Expression implementAbstract(
        JavaRelImplementor implementor,
        JavaRel rel,
        Expression childExp,
        Variable varInputRow,
        final RelDataType inputRowType,
        final RelDataType outputRowType,
        RexProgram program,
        String tag)
    {
        return implementAbstractTupleIter(
            implementor,
            rel,
            childExp,
            varInputRow,
            inputRowType,
            outputRowType,
            program,
            tag);
    }

    /**
     * Generates code for a Java expression satisfying the {@link
     * org.eigenbase.runtime.TupleIter} interface. The generated code allocates
     * a {@code org.eigenbase.runtime.CalcTupleIter} with a dynamic {@link
     * org.eigenbase.runtime.TupleIter#fetchNext()} method. If the "abort on
     * error" flag is false, or an error handling tag is specified, then
     * fetchNext is written to handle row errors.
     *
     * <p>Row errors are handled by wrapping expressions that can fail with a
     * try/catch block. A caught RuntimeException is then published to an
     * "connection variable." In the event that errors can overflow, an "error
     * buffering" flag allows them to be posted again on the next iteration of
     * fetchNext.
     *
     * @param implementor an object that implements relations as Java code
     * @param rel the relation to be implemented
     * @param childExp the implemented child of the relation
     * @param varInputRow the Java variable to use for the input row
     * @param inputRowType the rel data type of the input row
     * @param outputRowType the rel data type of the output row
     * @param program the rex program to implemented by the relation
     * @param tag an error handling tag
     *
     * @return a Java expression satisfying the TupleIter interface
     */
    public static Expression implementAbstractTupleIter(
        JavaRelImplementor implementor,
        JavaRel rel,
        Expression childExp,
        Variable varInputRow,
        final RelDataType inputRowType,
        final RelDataType outputRowType,
        RexProgram program,
        String tag)
    {
        MemberDeclarationList memberList = new MemberDeclarationList();

        // Perform error recovery if continuing on errors or if
        // an error handling tag has been specified
        boolean errorRecovery = !abortOnError || (tag != null);

        // Error buffering should not be enabled unless error recovery is
        assert !errorBuffering || errorRecovery;

        // Allow backwards compatibility until all Farrago extensions are
        // satisfied with the new error handling semantics. The new semantics
        // include:
        //   (1) cast input object to input row object outside of try block,
        //         should be fine, at least for base Farrago
        //   (2) maintain a columnIndex counter to better locate of error,
        //         at the cost of a few cycles
        //   (3) publish errors to the runtime context. FarragoRuntimeContext
        //         now supports this API
        boolean backwardsCompatible = true;
        if (tag != null) {
            backwardsCompatible = false;
        }

        RelDataTypeFactory typeFactory = implementor.getTypeFactory();
        OJClass outputRowClass =
            OJUtil.typeToOJClass(
                outputRowType,
                typeFactory);
        OJClass inputRowClass =
            OJUtil.typeToOJClass(
                inputRowType,
                typeFactory);

        Variable varOutputRow = implementor.newVariable();

        FieldDeclaration inputRowVarDecl =
            new FieldDeclaration(
                new ModifierList(ModifierList.PRIVATE),
                TypeName.forOJClass(inputRowClass),
                varInputRow.toString(),
                null);

        FieldDeclaration outputRowVarDecl =
            new FieldDeclaration(
                new ModifierList(ModifierList.PRIVATE),
                TypeName.forOJClass(outputRowClass),
                varOutputRow.toString(),
                new AllocationExpression(
                    outputRowClass,
                    new ExpressionList()));

        // The method body for fetchNext, a main target of code generation
        StatementList nextMethodBody = new StatementList();

        // First, post an error if it overflowed the previous time
        //     if (pendingError) {
        //         rc = handleRowError(...);
        //         if (rc instanceof NoDataReason) {
        //             return rc;
        //         }
        //         pendingError = false;
        //     }
        if (errorBuffering) {
            // add to next method body...
        }

        // Most of fetchNext falls within a while() block. The while block
        // allows us to try multiple input rows against a filter condition
        // before returning a single row.
        //     while (true) {
        //         Object varInputObj = inputIterator.fetchNext();
        //         if (varInputObj instanceof TupleIter.NoDataReason) {
        //             return varInputObj;
        //         }
        //         varInputRow = (InputRowClass) varInputObj;
        //         int columnIndex = 0;
        //         [calculation statements]
        //     }
        StatementList whileBody = new StatementList();

        Variable varInputObj = implementor.newVariable();

        whileBody.add(
            new VariableDeclaration(
                OJUtil.typeNameForClass(Object.class),
                varInputObj.toString(),
                new MethodCall(
                    new FieldAccess("inputIterator"),
                    "fetchNext",
                    new ExpressionList())));

        StatementList ifNoDataReasonBody = new StatementList();

        whileBody.add(
            new IfStatement(
                new InstanceofExpression(
                    varInputObj,
                    OJUtil.typeNameForClass(TupleIter.NoDataReason.class)),
                ifNoDataReasonBody));

        ifNoDataReasonBody.add(new ReturnStatement(varInputObj));

        // Push up the row declaration for new error handling so that the
        // input row is available to the error handler
        if (!backwardsCompatible) {
            whileBody.add(
                assignInputRow(inputRowClass, varInputRow, varInputObj));
        }

        Variable varColumnIndex = null;
        if (errorRecovery && !backwardsCompatible) {
            // NOTE jvs 7-Oct-2006:  Declare varColumnIndex as a member
            // (rather than a local) in case in the future we want
            // to decompose complex expressions into helper methods.
            varColumnIndex = implementor.newVariable();
            FieldDeclaration varColumnIndexDecl =
                new FieldDeclaration(
                    new ModifierList(ModifierList.PRIVATE),
                    OJUtil.typeNameForClass(int.class),
                    varColumnIndex.toString(),
                    null);
            memberList.add(varColumnIndexDecl);
            whileBody.add(
                new ExpressionStatement(
                    new AssignmentExpression(
                        varColumnIndex,
                        AssignmentExpression.EQUALS,
                        Literal.makeLiteral(0))));
        }

        // Calculator (projection, filtering) statements are later appended
        // to calcStmts. Typically, this target will be the while list itself.
        StatementList calcStmts;
        if (!errorRecovery) {
            calcStmts = whileBody;
        } else {
            // For error recovery, we wrap the calc statements
            // (e.g., everything but the code that reads rows from the
            // inputIterator) in a try/catch that publishes exceptions.

            calcStmts = new StatementList();

            // try { /* calcStmts */ }
            // catch(RuntimeException ex) {
            //     Object rc = connection.handleRowError(...);
            //     [buffer error if necessary]
            // }
            StatementList catchStmts = new StatementList();

            if (backwardsCompatible) {
                catchStmts.add(
                    new ExpressionStatement(
                        new MethodCall(
                            new MethodCall(
                                OJUtil.typeNameForClass(EigenbaseTrace.class),
                                "getStatementTracer",
                                null),
                            "log",
                            new ExpressionList(
                                new FieldAccess(
                                    OJUtil.typeNameForClass(Level.class),
                                    "WARNING"),
                                Literal.makeLiteral("java calc exception"),
                                new FieldAccess("ex")))));
            } else {
                Variable varRc = implementor.newVariable();
                ExpressionList handleRowErrorArgs =
                    new ExpressionList(
                        varInputRow,
                        new FieldAccess("ex"),
                        varColumnIndex);
                handleRowErrorArgs.add(Literal.makeLiteral(tag));
                catchStmts.add(
                    new VariableDeclaration(
                        OJUtil.typeNameForClass(Object.class),
                        varRc.toString(),
                        new MethodCall(
                            implementor.getConnectionVariable(),
                            "handleRowError",
                            handleRowErrorArgs)));

                // Buffer an error if it overflowed
                //     if (rc instanceof NoDataReason) {
                //         pendingError = true;
                //         [save error state]
                //         return rc;
                //     }
                if (errorBuffering) {
                    // add to catch statements...
                }
            }

            CatchList catchList =
                new CatchList(
                    new CatchBlock(
                        new Parameter(
                            OJUtil.typeNameForClass(RuntimeException.class),
                            "ex"),
                        catchStmts));

            TryStatement tryStmt = new TryStatement(calcStmts, catchList);

            whileBody.add(tryStmt);
        }

        if (backwardsCompatible) {
            calcStmts.add(
                assignInputRow(inputRowClass, varInputRow, varInputObj));
        }

        StatementList condBody;
        RexToOJTranslator translator =
            implementor.newStmtTranslator(rel, calcStmts, memberList);
        try {
            translator.pushProgram(program);
            if (program.getCondition() != null) {
                // TODO jvs 8-Oct-2006:  move condition to its own
                // method if big, as below for project exprs.
                condBody = new StatementList();
                RexNode rexIsTrue =
                    rel.getCluster().getRexBuilder().makeCall(
                        SqlStdOperatorTable.isTrueOperator,
                        program.getCondition());
                Expression conditionExp =
                    translator.translateRexNode(rexIsTrue);
                calcStmts.add(new IfStatement(conditionExp, condBody));
            } else {
                condBody = calcStmts;
            }

            RelDataTypeField [] fields = outputRowType.getFields();
            final List<RexLocalRef> projectRefList = program.getProjectList();
            int i = -1;
            for (RexLocalRef rhs : projectRefList) {
                // NOTE jvs 14-Sept-2006:  Put complicated project expressions
                // into their own method, otherwise a big select list can easily
                // blow the 64K Java limit on method bytecode size.  Make
                // methods private final in the hopes that they will get inlined
                // JIT.  For now we decide "complicated" based on the size of
                // the generated Java parse tree. A big enough select list of
                // simple expressions could still blow the limit, so we may need
                // to group them together, sub-divide, etc.

                StatementList projMethodBody = new StatementList();

                if (errorRecovery && !backwardsCompatible) {
                    projMethodBody.add(
                        new ExpressionStatement(
                            new UnaryExpression(
                                varColumnIndex,
                                UnaryExpression.POST_INCREMENT)));
                }
                ++i;

                RexToOJTranslator projTranslator =
                    translator.push(projMethodBody);
                String javaFieldName =
                    Util.toJavaId(
                        fields[i].getName(),
                        i);
                Expression lhs = new FieldAccess(varOutputRow, javaFieldName);
                projTranslator.translateAssignment(fields[i], lhs, rhs);

                int complexity = OJUtil.countParseTreeNodes(projMethodBody);

                // REVIEW: HCP 5/18/2011
                // The projMethod should be checked
                // for causing possible compiler errors caused by the use of
                // variables declared in other projMethods.  Also the
                // local declaration of variabled used by other proj methods
                // should also be checked.

                // Fixing for backswing integration 14270
                // TODO: check if abstracting this method body will cause
                // a compiler error
                if (true) {
                    // No method needed; just append.
                    condBody.addAll(projMethodBody);
                    continue;
                }

                // Need a separate method.

                String projMethodName =
                    "calc_" + varOutputRow.toString() + "_f_" + i;
                MemberDeclaration projMethodDecl =
                    new MethodDeclaration(
                        new ModifierList(
                            ModifierList.PRIVATE | ModifierList.FINAL),
                        TypeName.forOJClass(OJSystem.VOID),
                        projMethodName,
                        new ParameterList(),
                        null,
                        projMethodBody);
                memberList.add(projMethodDecl);
                condBody.add(
                    new ExpressionStatement(
                        new MethodCall(
                            projMethodName,
                            new ExpressionList())));
            }
        } finally {
            translator.popProgram(program);
        }

        condBody.add(new ReturnStatement(varOutputRow));

        WhileStatement whileStmt =
            new WhileStatement(
                Literal.makeLiteral(true),
                whileBody);

        nextMethodBody.add(whileStmt);

        MemberDeclaration fetchNextMethodDecl =
            new MethodDeclaration(
                new ModifierList(ModifierList.PUBLIC),
                OJUtil.typeNameForClass(Object.class),
                "fetchNext",
                new ParameterList(),
                null,
                nextMethodBody);

        // The restart() method should reset variables used to buffer errors
        //     pendingError = false
        if (errorBuffering) {
            // declare refinement of restart() and add to member list...
        }

        memberList.add(inputRowVarDecl);
        memberList.add(outputRowVarDecl);
        memberList.add(fetchNextMethodDecl);
        Expression newTupleIterExp =
            new AllocationExpression(
                OJUtil.typeNameForClass(Object.class/*CalcTupleIter.class*/),
                new ExpressionList(childExp),
                memberList);

        return newTupleIterExp;
    }

    public ParseTree implement(JavaRelImplementor implementor)
    {
        Expression childExp =
            implementor.visitJavaChild(this, 0, (JavaRel) getChild());
        RelDataType outputRowType = getRowType();
        RelDataType inputRowType = getChild().getRowType();

        Variable varInputRow = implementor.newVariable();
        implementor.bind(
            getChild(),
            varInputRow);

        return implementAbstract(
            implementor,
            this,
            childExp,
            varInputRow,
            inputRowType,
            outputRowType,
            program,
            tag);
    }

    public RexProgram getProgram()
    {
        return program;
    }

    public String getTag()
    {
        return tag;
    }

    private static Statement assignInputRow(
        OJClass inputRowClass,
        Variable varInputRow,
        Variable varInputObj)
    {
        return new ExpressionStatement(
            new AssignmentExpression(
                varInputRow,
                AssignmentExpression.EQUALS,
                new CastExpression(
                    TypeName.forOJClass(inputRowClass),
                    varInputObj)));
    }
}

// End IterCalcRel.java
