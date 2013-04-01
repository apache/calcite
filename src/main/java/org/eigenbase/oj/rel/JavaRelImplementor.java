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

import java.util.*;
import java.util.List;
import java.util.logging.*;

import openjava.ptree.*;

import org.eigenbase.oj.rex.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.*;


/**
 * <code>JavaRelImplementor</code> deals with the nastiness of converting a tree
 * of relational expressions into an implementation, generally an {@link
 * ParseTree openjava parse tree}.
 *
 * <p>The {@link #bind} method allows relational expressions to register which
 * Java variable holds their row. They can bind 'lazily', so that the variable
 * is only declared and initialized if it is actually used in another
 * expression.</p>
 */
public class JavaRelImplementor extends RelImplementorImpl {

    final HashMap<String, Variable> mapCorrelNameToVariable =
        new HashMap<String, Variable>();

    /**
     * Stack of {@link StatementList} objects.
     */
    final Stack<StatementList> stmtListStack = new Stack<StatementList>();
    Statement exitStatement;
    private int nextVariableId;
    protected final OJRexImplementorTable implementorTable;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a JavaRelImplementor
     *
     * @param rexBuilder Builder for {@link RexNode}s
     * @param implementorTable Table of implementations of operators. Must not
     * be null
     */
    public JavaRelImplementor(
        RexBuilder rexBuilder,
        OJRexImplementorTable implementorTable)
    {
        super(rexBuilder);
        Util.pre(rexBuilder != null, "rexBuilder != null");
        Util.pre(implementorTable != null, "implementorTable != null");
        this.implementorTable = implementorTable;
        nextVariableId = 0;
    }

    //~ Methods ----------------------------------------------------------------

    public void setExitStatement(openjava.ptree.Statement stmt)
    {
        this.exitStatement = stmt;
    }

    public Statement getExitStatement()
    {
        return exitStatement;
    }

    public StatementList getStatementList()
    {
        return stmtListStack.peek();
    }

    /**
     * Records the fact that instances of <code>rel</code> are available in
     * <code>variable</code>.
     */
    public void bind(
        RelNode rel,
        Variable variable)
    {
        bind(
            rel, new EagerBind(variable));
    }

    /**
     * Declares a variable, and binds it lazily, so it only gets initialized if
     * it is actually used.
     *
     * @return the Variable so declared
     */
    public Variable bind(
        RelNode rel,
        StatementList statementList,
        final VariableInitializer initializer)
    {
        VariableInitializerThunk thunk =
            new VariableInitializerThunk() {
                public VariableInitializer getInitializer()
                {
                    return initializer;
                }
            };

        Variable variable = newVariable();
        LazyBind bind =
            new LazyBind(
                variable,
                statementList,
                getTypeFactory(),
                rel.getRowType(),
                thunk);
        bind(rel, bind);
        return bind.getVariable();
    }

    /**
     * Shares a variable between relations. <code>previous</code> already has a
     * variable, and calling this method indicates that <code>rel</code>'s
     * output will appear in this variable too.
     */
    public void bind(
        RelNode rel,
        RelNode previous)
    {
        bind(
            rel,
            new RelBind(previous));
    }

    /**
     * Binds a correlating variable. References to correlating variables such as
     * <code>$cor2</code> will be replaced with java variables such as <code>
     * $Oj14</code>.
     */
    public void bindCorrel(
        String correlName,
        Variable variable)
    {
        mapCorrelNameToVariable.put(correlName, variable);
    }

    public JavaRel findRel(
        JavaRel rel,
        RexNode expression)
    {
        if (expression instanceof RexInputRef) {
            RexInputRef variable = (RexInputRef) expression;

            // REVIEW jvs 30-May-2005:  What's up with this?  The "&& false"
            // should have at least a comment!
            if ((rel instanceof JoinRelBase) && false) {
                return (JavaRel) findInputRel(
                    rel,
                    variable.getIndex());
            } else {
                return (JavaRel) rel.getInput(variable.getIndex());
            }
        } else if (expression instanceof RexFieldAccess) {
            RexFieldAccess fieldAccess = (RexFieldAccess) expression;
            String fieldName = fieldAccess.getName();
            RexNode refExp = fieldAccess.getReferenceExpr();
            JavaRel rel2 = findRel(rel, refExp); // recursive
            if (rel2 == null) {
                return null;
            }
            return implementFieldAccess(rel2, fieldName);
        } else {
            return null;
        }
    }

    /**
     * Burrows into a synthetic record and returns the underlying relation which
     * provides the field called <code>fieldName</code>.
     */
    public JavaRel implementFieldAccess(
        JavaRel rel,
        String fieldName)
    {
        if (rel instanceof IterCalcRel) {
            return ((IterCalcRel) rel).implementFieldAccess(this, fieldName);
        } else {
            return null;
        }
    }

    /**
     * Implements the body of the current expression's parent. If <code>
     * variable</code> is not null, bind the current expression to <code>
     * variable</code>. For example, a nested loops join would generate
     *
     * <blockquote>
     * <pre>
     * for (int i = 0; i < emps.length; i++) {
     *   Emp emp = emps[i];
     *   for (int j = 0; j < depts.length; j++) {
     *     Dept dept = depts[j];
     *     if (emp.deptno == dept.deptno) {
     *       <<parent body>>
     *     }
     *   }
     * }
     * </pre>
     * </blockquote>
     *
     * which corresponds to
     *
     * <blockquote>
     * <pre>
     * [emp:iter
     *   [dept:iter
     *     [join:body(emp,dept)
     *       [parent:body]
     *     ]
     *   ]
     * ]
     * </pre>
     * </blockquote>
     *
     * @param rel child relation
     * @param stmtList block that child was generating its code into
     */
    public void generateParentBody(
        RelNode rel,
        StatementList stmtList)
    {
        if (stmtList != null) {
            pushStatementList(stmtList);
        }
        JavaFrame frame = (JavaFrame) mapRel2Frame.get(rel);
        bindDeferred(frame, rel);
        ((JavaLoopRel) frame.parent).implementJavaParent(this, frame.ordinal);
        if (stmtList != null) {
            popStatementList(stmtList);
        }
    }

    private void bindDeferred(
        JavaFrame frame,
        final RelNode rel)
    {
        final StatementList statementList = getStatementList();
        if (frame.bind == null) {
            // this relational expression has not bound itself, so we presume
            // that we can call its implementSelf() method
            if (!(rel instanceof JavaSelfRel)) {
                throw Util.newInternal(
                    "In order to bind-deferred, a "
                    + "relational expression must implement JavaSelfRel: "
                    + rel);
            }
            final JavaSelfRel selfRel = (JavaSelfRel) rel;
            LazyBind lazyBind =
                new LazyBind(
                    newVariable(),
                    statementList,
                    getTypeFactory(),
                    rel.getRowType(),
                    new VariableInitializerThunk() {
                        public VariableInitializer getInitializer()
                        {
                            return selfRel.implementSelf(
                                JavaRelImplementor.this);
                        }
                    });
            bind(rel, lazyBind);
        } else if (
            (frame.bind instanceof LazyBind)
            && (((LazyBind) frame.bind).statementList != statementList))
        {
            // Frame is already bound, but to a variable declared in a different
            // scope. Re-bind it.
            final LazyBind lazyBind = (LazyBind) frame.bind;
            lazyBind.statementList = statementList;
            lazyBind.bound = false;
        }
    }

    /**
     * Convenience wrapper around {@link RelImplementor#visitChild} for the
     * common case where {@link JavaRel} has a child which is a {@link JavaRel}.
     */
    public final Expression visitJavaChild(
        RelNode parent,
        int ordinal,
        JavaRel child)
    {
        return (Expression) visitChild(parent, ordinal, child);
    }

    @Override
    public ParseTree visitChildInternal(RelNode child, int ordinal)
    {
        final Convention convention = child.getConvention();
        if (!(child instanceof JavaRel)) {
            throw Util.newInternal(
                "Relational expression '" + child
                + "' has '" + convention
                + "' calling convention, so must implement interface "
                + JavaRel.class);
        }
        JavaRel javaRel = (JavaRel) child;
        final ParseTree p = javaRel.implement(this);
        if ((convention == CallingConvention.JAVA) && (p != null)) {
            throw Util.newInternal(
                "Relational expression '" + child
                + "' returned '" + p + " on implement, but should have "
                + "returned null, because it has JAVA calling-convention. "
                + "(Note that similar calling-conventions, such as "
                + "Iterator, must return a value.)");
        }
        return p;
    }

    /**
     * Starts an iteration, by calling {@link
     * org.eigenbase.oj.rel.JavaRel#implement} on the root element.
     */
    public Expression implementRoot(JavaRel rel)
    {
        return visitJavaChild(null, -1, rel);
    }

    /**
     * Creates an expression which references correlating variable <code>
     * correlName</code> from the context of <code>rel</code>. For example, if
     * <code>correlName</code> is set by the 1st child of <code>rel</code>'s 2nd
     * child, then this method returns <code>$input2.$input1</code>.
     */
    public Expression makeReference(
        String correlName,
        RelNode rel)
    {
        JavaFrame frame = (JavaFrame) mapCorrel2Frame.get(correlName);
        assert (frame != null);
        assert Util.equal(frame.rel.getCorrelVariable(), correlName);
        assert (frame.hasVariable());
        return frame.getVariable();
    }

    /**
     * Generates a variable with a unique name.
     */
    public Variable newVariable()
    {
        return newVariable("oj_var");
    }

    /**
     * Generates a variable with a unique name and a stem which indicates its
     * purpose. For example, <code>newVariable("binding")</code> might generate
     * a variable called <code>"binding_12"</code>.
     */
    public Variable newVariable(String base)
    {
        assert base != null;
        return new Variable(base + generateVariableId());
    }

    /**
     * @return unique generated variable ID
     */
    public int generateVariableId()
    {
        return nextVariableId++;
    }

    public Variable getConnectionVariable()
    {
        throw Util.needToImplement("getConnectionVariable");
    }

    public void popStatementList(StatementList stmtList)
    {
        assert (stmtList == getStatementList());
        stmtListStack.pop();
    }

    public void pushStatementList(StatementList stmtList)
    {
        stmtListStack.push(stmtList);
    }

    /**
     * Converts an expression in internal form (the input relation is referenced
     * using the variable <code>$input0</code>) to generated form (the input
     * relation is referenced using the bindings in this <code>
     * JavaRelImplementor</code>). Compare this method with
     * net.sf.saffron.oj.xlat.QueryInfo.convertExpToInternal(), which converts
     * from source form to internal form.
     *
     * @param exp the expression to translate (it is cloned, not modified)
     * @param rel the relational expression which is the context for <code>
     * exp</code>
     */
    public Expression translate(
        JavaRel rel,
        RexNode exp)
    {
        RexToOJTranslator translator = newTranslator(rel);
        return translator.translateRexNode(exp);
    }

    /**
     * Determines whether it is possible to implement a set of expressions in
     * Java.
     *
     * @param program Program to translate
     *
     * @return whether all expressions in the program can be implemented
     */
    public boolean canTranslate(
        RelNode rel,
        RexProgram program)
    {
        RexToOJTranslator translator = newTranslator(rel);
        TranslationTester tester = new TranslationTester(translator, true);
        final List<RexNode> exprList = program.getExprList();
        for (RexNode expr : exprList) {
            if (!tester.canTranslate(expr)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Determines whether it is possible to implement an expression in Java.
     *
     * @param rel Relational expression
     * @param expression Expression
     * @param deep if true, operands of the given expression are tested for
     * translatability as well; if false only the top level expression is tested
     *
     * @return whether the expression can be implemented
     */
    public boolean canTranslate(
        RelNode rel,
        RexNode expression,
        boolean deep)
    {
        RexToOJTranslator translator = newTranslator(rel);
        TranslationTester tester = new TranslationTester(translator, deep);
        return tester.canTranslate(expression);
    }

    /**
     * Generates code for an expression, possibly using multiple statements,
     * scratch variables, and helper functions.
     *
     * <p>If you want to avoid generating common expressions, it is better to
     * create a translator using {@link #newStmtTranslator(JavaRel,
     * StatementList, MemberDeclarationList)} and use it to translate multiple
     * expressions.
     *
     * @param rel the relational expression which is the context for exp
     * @param exp the row expression to be translated
     * @param stmtList optional code can be appended here
     * @param memberList optional member declarations can be appended here (if
     * needed for reusable scratch space or helper functions; local variables
     * can also be allocated in stmtList)
     */
    public Expression translateViaStatements(
        JavaRel rel,
        RexNode exp,
        StatementList stmtList,
        MemberDeclarationList memberList)
    {
        RexToOJTranslator translator =
            newStmtTranslator(rel, stmtList, memberList);
        return translator.translateRexNode(exp);
    }

    /**
     * Converts an array of expressions in internal into a list of expressions
     * in generated form.
     *
     * @see #translate(JavaRel,RexNode)
     */
    public ExpressionList translateList(
        JavaRel rel,
        RexNode [] exps)
    {
        final ExpressionList list = new ExpressionList();
        RexToOJTranslator translator = newTranslator(rel);
        for (int i = 0; i < exps.length; i++) {
            list.add(translator.translateRexNode(exps[i]));
        }
        return list;
    }

    /**
     * Creates a {@link RexToOJTranslator} with which to translate the {@link
     * RexNode row-expressions} within a relational expression into OpenJava
     * expressions.
     */
    protected RexToOJTranslator newTranslator(RelNode rel)
    {
        return new RexToOJTranslator(this, rel, implementorTable);
    }

    /**
     * Creates a translator which can translate a succession of expressions,
     * possibly using multiple statements, scratch variables, and helper
     * functions.
     *
     * <p>Typical usage:
     *
     * <blockquote>
     * <pre>
     * Translator translator = newStmtTranslator(rel, stmtList, memberList);
     * translator.translateRexNode(exp1);
     * translator.translateRexNode(exp2);
     * </pre>
     * </blockquote>
     *
     * @param rel the relational expression which is the context for exp
     * @param stmtList optional code can be appended here
     * @param memberList optional member declarations can be appended here (if
     * needed for reusable scratch space or helper functions; local variables
     */
    public RexToOJTranslator newStmtTranslator(
        final JavaRel rel,
        StatementList stmtList,
        MemberDeclarationList memberList)
    {
        return newTranslator(rel);
    }

    /**
     * Creates an expression which references the <i>ordinal</i><sup>th</sup>
     * input.
     */
    public Expression translateInput(
        JavaRel rel,
        int ordinal)
    {
        final RelDataType rowType = rel.getRowType();
        int fieldOffset = computeFieldOffset(rel, ordinal);
        return translate(
            rel,
            rexBuilder.makeRangeReference(rowType, fieldOffset, false));
    }

    /**
     * Returns the index of the first field in <code>rel</code> which comes from
     * its <code>ordinal</code>th input.
     *
     * <p>For example, if rel joins T0(A,B,C) to T1(D,E), then
     * countFields(0,rel) yields 0, and countFields(1,rel) yields 3.</p>
     */
    private int computeFieldOffset(
        RelNode rel,
        int ordinal)
    {
        if (ordinal == 0) {
            // short-circuit for the common case
            return 0;
        }
        int fieldOffset = 0;
        final List<RelNode> inputs = rel.getInputs();
        for (int i = 0; i < ordinal; i++) {
            RelNode input = inputs.get(i);
            fieldOffset += input.getRowType().getFieldList().size();
        }
        return fieldOffset;
    }

    /**
     * Creates an expression which references the <i>
     * fieldOrdinal</i><sup>th</sup> field of the <i>ordinal</i><sup>th</sup>
     * input.
     *
     * <p>(We can potentially optimize the generation process, so we can access
     * field values without actually instantiating the row.)</p>
     */
    public Expression translateInputField(
        JavaRel rel,
        int ordinal,
        int fieldOrdinal)
    {
        assert ordinal >= 0;
        assert ordinal < rel.getInputs().size();
        assert fieldOrdinal >= 0;
        assert fieldOrdinal
            < rel.getInput(ordinal).getRowType().getFieldList().size();
        RelDataType rowType = rel.getRowType();
        final RelDataTypeField [] fields = rowType.getFields();
        final int fieldIndex = computeFieldOffset(rel, ordinal) + fieldOrdinal;
        assert fieldIndex >= 0;
        assert fieldIndex < fields.length;
        final RexNode expr =
            rexBuilder.makeInputRef(
                fields[fieldIndex].getType(),
                fieldIndex);
        return translate(rel, expr);
    }

    /**
     * Records the fact that instances of <code>rel</code> are available via
     * <code>bind</code> (which may be eager or lazy).
     */
    private void bind(
        RelNode rel,
        Bind bind)
    {
        tracer.log(Level.FINE, "Bind " + rel.toString() + " to " + bind);
        JavaFrame frame = (JavaFrame) mapRel2Frame.get(rel);
        frame.bind = bind;
        boolean stupid = SaffronProperties.instance().stupid.get();
        if (stupid) {
            // trigger the declaration of the variable, even though it
            // may not be used
            Util.discard(bind.getVariable());
        }
    }

    /**
     * Returns the variable which, in the generated program, will hold the
     * current row of a given relational expression. This method is only
     * applicable if the relational expression is the current one or an input;
     * if it is an ancestor, there is no current value, and this method returns
     * null.
     */
    public Variable findInputVariable(RelNode rel)
    {
        while (true) {
            JavaFrame frame = (JavaFrame) mapRel2Frame.get(rel);
            if ((frame != null) && frame.hasVariable()) {
                return frame.getVariable();
            }
            List<RelNode> inputs = rel.getInputs();
            if (inputs.size() == 1) {
                rel = inputs.get(0);
            } else {
                return null;
            }
        }
    }

    public Expression implementStart(
        AggregateCall call,
        JavaRel rel)
    {
        OJAggImplementor aggImplementor =
            implementorTable.get(call.getAggregation());
        return aggImplementor.implementStart(this, rel, call);
    }

    public Expression implementStartAndNext(
        AggregateCall call,
        JavaRel rel)
    {
        OJAggImplementor aggImplementor =
            implementorTable.get(call.getAggregation());
        return aggImplementor.implementStartAndNext(this, rel, call);
    }

    public void implementNext(
        AggregateCall call,
        JavaRel rel,
        Expression accumulator)
    {
        OJAggImplementor aggImplementor =
            implementorTable.get(call.getAggregation());
        aggImplementor.implementNext(this, rel, accumulator, call);
    }

    /**
     * Generates the expression to retrieve the result of this aggregation.
     */
    public Expression implementResult(
        AggregateCall call,
        Expression accumulator)
    {
        OJAggImplementor aggImplementor =
            implementorTable.get(call.getAggregation());
        return aggImplementor.implementResult(this, accumulator, call);
    }

    //~ Inner Interfaces -------------------------------------------------------

    /**
     * A <code>VariableInitializerThunk</code> yields a {@link
     * VariableInitializer}.
     */
    public interface VariableInitializerThunk
    {
        VariableInitializer getInitializer();
    }

    private interface Bind
    {
        Variable getVariable();
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class EagerBind
        implements Bind
    {
        Variable variable;

        EagerBind(Variable variable)
        {
            this.variable = variable;
        }

        public Variable getVariable()
        {
            return variable;
        }

        public String toString()
        {
            return super.toString() + "(variable=" + variable.toString() + ")";
        }
    }

    /**
     * Binds a relational expression to whatever another relational expression
     * is currently bound to.
     *
     * <p>Note that if relational expressions are shared, then this binding can
     * change over time. Consider, for instance "select * from emps where deptno
     * = 10 union select * from emps where deptno = 20". There will be a reader
     * of "emps" which is used by the two filter expressions "deptno = 10" and
     * "deptno = 20". The "emp" reader will assign its current row to "var4" in
     * the first binding, and to "var8" in the second.</p>
     */
    private class RelBind
        implements Bind
    {
        private final RelNode rel;

        RelBind(RelNode rel)
        {
            this.rel = rel;
        }

        public Variable getVariable()
        {
            final JavaFrame frame = findFrame();
            return frame.getVariable();
        }

        private JavaFrame findFrame()
        {
            RelNode previous = rel;
            while (true) {
                JavaFrame frame = (JavaFrame) mapRel2Frame.get(previous);
                if (frame.bind != null) {
                    tracer.log(
                        Level.FINE,
                        "Bind " + rel.toString() + " to "
                        + previous.toString() + "(" + frame.bind + ")");
                    return frame;
                }

                // go deeper
                List<RelNode> inputs = previous.getInputs();
                assert (inputs.size() == 1) : "input is not bound";
                previous = inputs.get(0);
            }
        }
    }

    private static class LazyBind
        implements Bind
    {
        final RelDataTypeFactory typeFactory;
        final RelDataType type;
        final Statement after;
        StatementList statementList;
        final Variable variable;
        final VariableInitializerThunk thunk;
        boolean bound;

        LazyBind(
            Variable variable,
            StatementList statementList,
            RelDataTypeFactory typeFactory,
            RelDataType type,
            VariableInitializerThunk thunk)
        {
            this.variable = variable;
            this.statementList = statementList;
            this.after =
                (statementList.size() == 0) ? null
                : statementList.get(statementList.size() - 1);
            this.type = type;
            this.typeFactory = typeFactory;
            this.thunk = thunk;
        }

        public Variable getVariable()
        {
            if (!bound) {
                bound = true;
                int position = find(statementList, after);
                VariableDeclaration varDecl =
                    new VariableDeclaration(
                        OJUtil.toTypeName(type, typeFactory),
                        variable.toString(),
                        null);
                statementList.insertElementAt(varDecl, position);
                varDecl.setInitializer(thunk.getInitializer());
            }
            return variable;
        }

        public String toString()
        {
            return super.toString() + "(variable=" + variable.toString()
                + ", thunk=" + thunk.toString() + ")";
        }

        private static int find(
            StatementList list,
            Statement statement)
        {
            if (statement == null) {
                return 0;
            } else {
                for (int i = 0, n = list.size(); i < n; i++) {
                    if (list.get(i) == statement) {
                        return i + 1;
                    }
                }
                throw Util.newInternal(
                    "could not find statement " + statement
                    + " in list " + list);
            }
        }
    }

    /**
     * Similar to {@link RexToOJTranslator}, but instead of translating, merely
     * tests whether an expression can be translated.
     */
    public static class TranslationTester
    {
        private final RexToOJTranslator translator;
        private final boolean deep;

        public TranslationTester(
            RexToOJTranslator translator,
            boolean deep)
        {
            this.translator = translator;
            this.deep = deep;
        }

        /**
         * Returns whether an expression can be translated.
         */
        public boolean canTranslate(RexNode rex)
        {
            try {
                go(rex);
                return true;
            } catch (CannotTranslate cannotTranslate) {
                return false;
            }
        }

        /**
         * Walks over an expression, and throws <code>CannotTranslate</code>
         * if expression cannot be translated.
         *
         * @param rex Expression
         *
         * @throws org.eigenbase.oj.rel.JavaRelImplementor.TranslationTester.CannotTranslate
         * if expression or a sub-expression cannot be translated
         */
        protected void go(RexNode rex)
            throws CannotTranslate
        {
            if (rex instanceof RexCall) {
                final RexCall call = (RexCall) rex;
                if (!translator.canConvertCall(call)) {
                    throw new CannotTranslate();
                }
                if (!deep) {
                    return;
                }
                RexNode [] operands = call.operands;
                for (int i = 0; i < operands.length; i++) {
                    go(operands[i]);
                }
            } else if (rex instanceof RexFieldAccess) {
                if (!deep) {
                    return;
                }

                go(((RexFieldAccess) rex).getReferenceExpr());
            }
        }

        /**
         * Thrown when we encounter an expression which cannot be translated. It
         * is always handled by {@link TranslationTester#canTranslate(RexNode)},
         * and is not really an error.
         */
        private static class CannotTranslate
            extends Exception
        {
            public CannotTranslate()
            {
            }
        }
    }

    private static class JavaFrame extends Frame {
        /**
         * Holds variable which hasn't been declared yet.
         */
        protected JavaRelImplementor.Bind bind;

        /**
         * Retrieves the variable, executing the lazy bind if necessary.
         */
        Variable getVariable()
        {
            assert (hasVariable());
            return bind.getVariable();
        }

        /**
         * Returns whether the frame has, or potentially has, a variable.
         */
        boolean hasVariable()
        {
            return bind != null;
        }
    }
}

// End JavaRelImplementor.java
