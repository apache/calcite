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
package org.eigenbase.oj.rex;

import java.util.*;
import java.util.List;

import openjava.ptree.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.util.*;


/**
 * OJRexImplementorTableImpl is a default implementation of {@link
 * OJRexImplementorTable}, containing implementors for standard operators,
 * functions, and aggregates. Say that three times fast.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class OJRexImplementorTableImpl
    implements OJRexImplementorTable
{
    //~ Static fields/initializers ---------------------------------------------

    private static OJRexImplementorTableImpl instance;
    private static final String holderClassName = "saffron.runtime.Holder";

    //~ Instance fields --------------------------------------------------------

    private final Map<SqlOperator, OJRexImplementor> implementorMap =
        new HashMap<SqlOperator, OJRexImplementor>();

    private final Map<SqlAggFunction, OJSumAggImplementor> aggImplementorMap =
        new HashMap<SqlAggFunction, OJSumAggImplementor>();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an empty table.
     *
     * <p>You probably want to call the public method {@link #instance} instead.
     */
    protected OJRexImplementorTableImpl()
    {
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Creates a table and initializes it with implementations of all of the
     * standard SQL functions and operators.
     */
    public synchronized static OJRexImplementorTable instance()
    {
        if (instance == null) {
            instance = new OJRexImplementorTableImpl();
            instance.initStandard(SqlStdOperatorTable.instance());
        }
        return instance;
    }

    // implement OJRexImplementorTable
    public OJRexImplementor get(SqlOperator op)
    {
        return implementorMap.get(op);
    }

    // implement OJRexImplementorTable
    public OJAggImplementor get(Aggregation agg)
    {
        return aggImplementorMap.get(agg);
    }

    /**
     * Registers implementations for the standard set of functions and
     * operators.
     */
    protected void initStandard(final SqlStdOperatorTable opTab)
    {
        registerBinaryOperator(
            SqlStdOperatorTable.equalsOperator,
            BinaryExpression.EQUAL);

        registerBinaryOperator(
            SqlStdOperatorTable.notEqualsOperator,
            BinaryExpression.NOTEQUAL);

        registerBinaryOperator(
            SqlStdOperatorTable.lessThanOperator,
            BinaryExpression.LESS);

        registerBinaryOperator(
            SqlStdOperatorTable.lessThanOrEqualOperator,
            BinaryExpression.LESSEQUAL);

        registerBinaryOperator(
            SqlStdOperatorTable.greaterThanOperator,
            BinaryExpression.GREATER);

        registerBinaryOperator(
            SqlStdOperatorTable.greaterThanOrEqualOperator,
            BinaryExpression.GREATEREQUAL);

        registerBinaryOperator(
            SqlStdOperatorTable.plusOperator,
            BinaryExpression.PLUS);

        registerBinaryOperator(
            SqlStdOperatorTable.minusOperator,
            BinaryExpression.MINUS);

        registerOperator(
            SqlStdOperatorTable.minusDateOperator,
            new OJRexBinaryExpressionImplementor(BinaryExpression.MINUS));

        registerBinaryOperator(
            SqlStdOperatorTable.multiplyOperator,
            BinaryExpression.TIMES);

        registerBinaryOperator(
            SqlStdOperatorTable.divideOperator,
            BinaryExpression.DIVIDE);

        registerBinaryOperator(
            SqlStdOperatorTable.divideIntegerOperator,
            BinaryExpression.DIVIDE);

        registerBinaryOperator(
            SqlStdOperatorTable.andOperator,
            BinaryExpression.LOGICAL_AND);

        registerBinaryOperator(
            SqlStdOperatorTable.orOperator,
            BinaryExpression.LOGICAL_OR);

        registerUnaryOperator(
            SqlStdOperatorTable.prefixMinusOperator,
            UnaryExpression.MINUS);

        registerUnaryOperator(
            SqlStdOperatorTable.notOperator,
            UnaryExpression.NOT);

        registerOperator(
            SqlStdOperatorTable.isTrueOperator,
            new OJRexIgnoredCallImplementor());

        registerOperator(
            SqlStdOperatorTable.castFunc,
            new OJRexCastImplementor());

        // We hope that the internal 'slice' operator will be expanded away
        // before we need to generate code for it.
        registerOperator(
            SqlStdOperatorTable.sliceOp,
            new OJRexIgnoredCallImplementor());

        // Register the standard aggregations.
        aggImplementorMap.put(
            SqlStdOperatorTable.sumOperator,
            new OJSumAggImplementor());
    }

    public void registerOperator(
        SqlOperator op,
        OJRexImplementor implementor)
    {
        implementorMap.put(op, implementor);
    }

    protected void registerBinaryOperator(
        SqlBinaryOperator op,
        int ojBinaryExpressionOrdinal)
    {
        registerOperator(
            op,
            new OJRexBinaryExpressionImplementor(ojBinaryExpressionOrdinal));
    }

    protected void registerUnaryOperator(
        SqlPrefixOperator op,
        int ojUnaryExpressionOrdinal)
    {
        registerOperator(
            op,
            new OJRexUnaryExpressionImplementor(ojUnaryExpressionOrdinal));
    }

    //~ Inner Classes ----------------------------------------------------------

    public abstract static class OJBasicAggImplementor
        implements OJAggImplementor
    {
        // implement Aggregation
        public boolean canMerge()
        {
            return false;
        }

        // implement Aggregation
        public void implementMerge(
            JavaRelImplementor implementor,
            RelNode rel,
            Expression accumulator,
            Expression otherAccumulator)
        {
            throw Util.newInternal(
                "This method shouldn't have been called, because canMerge "
                + "returned " + canMerge());
        }

        /**
         * This is a default implementation of {@link
         * org.eigenbase.oj.rex.OJAggImplementor#implementStartAndNext};
         * particular derived classes may do better.
         */
        public Expression implementStartAndNext(
            JavaRelImplementor implementor,
            JavaRel rel,
            AggregateCall call)
        {
            StatementList stmtList = implementor.getStatementList();
            Variable var = implementor.newVariable();
            stmtList.add(
                new VariableDeclaration(
                    TypeName.forOJClass(OJUtil.clazzObject),
                    var.toString(),
                    implementStart(implementor, rel, call)));
            implementNext(implementor, rel, var, call);
            return var;
        }
    }

    /**
     * <code>Sum</code> is an aggregator which returns the sum of the values
     * which go into it. It has precisely one argument of numeric type
     * (<code>int</code>, <code>long</code>, <code>float</code>, <code>
     * double</code>), and the result is the same type.
     */
    public static class OJSumAggImplementor
        extends OJBasicAggImplementor
    {
        public OJSumAggImplementor()
        {
        }

        public boolean canMerge()
        {
            return true;
        }

        public void implementNext(
            JavaRelImplementor implementor,
            JavaRel rel,
            Expression accumulator,
            AggregateCall call)
        {
            final List<Integer> args = call.getArgList();
            final SqlSumAggFunction agg =
                (SqlSumAggFunction) call.getAggregation();
            assert (args.size() == 1);
            StatementList stmtList = implementor.getStatementList();
            Expression arg =
                implementor.translateInputField(rel, 0, args.get(0));

            // e.g. "((Holder.int_Holder) acc).value += arg"
            stmtList.add(
                new ExpressionStatement(
                    new AssignmentExpression(
                        new FieldAccess(
                            new CastExpression(
                                new TypeName(
                                    holderClassName + "."
                                    + agg.getType()
                                    + "_Holder"),
                                accumulator),
                            "value"),
                        AssignmentExpression.ADD,
                        arg)));
        }

        public Expression implementResult(
            JavaRelImplementor implementor,
            Expression accumulator,
            AggregateCall call)
        {
            // e.g. "o" becomes "((Holder.int_Holder) o).value"
            final SqlSumAggFunction agg =
                (SqlSumAggFunction) call.getAggregation();
            return new FieldAccess(
                new CastExpression(
                    new TypeName(
                        holderClassName + "."
                        + agg.getType() + "_Holder"),
                    accumulator),
                "value");
        }

        public Expression implementStart(
            JavaRelImplementor implementor,
            JavaRel rel,
            AggregateCall call)
        {
            // e.g. "new Holder.int_Holder(0)"
            final SqlSumAggFunction agg =
                (SqlSumAggFunction) call.getAggregation();
            return new AllocationExpression(
                new TypeName(
                    holderClassName + "." + agg.getType() + "_Holder"),
                new ExpressionList(Literal.constantZero()));
        }

        String getName()
        {
            return "sum";
        }
    }

    public static class OJCountAggImplementor
        extends OJBasicAggImplementor
    {
        public OJCountAggImplementor()
        {
        }

        public boolean canMerge()
        {
            return true;
        }

        public void implementNext(
            JavaRelImplementor implementor,
            JavaRel rel,
            Expression accumulator,
            AggregateCall call)
        {
            SqlCountAggFunction agg =
                (SqlCountAggFunction) call.getAggregation();
            StatementList stmtList = implementor.getStatementList();
            ExpressionStatement stmt =
                new ExpressionStatement(
                    new UnaryExpression(
                        UnaryExpression.POST_INCREMENT,
                        new FieldAccess(
                            new CastExpression(
                                new TypeName(
                                    holderClassName + "."
                                    + SqlCountAggFunction.type
                                    + "_Holder"),
                                accumulator),
                            "value")));
            final List<Integer> argList = call.getArgList();
            if (argList.size() == 0) {
                // e.g. "((Holder.int_Holder) acc).value++;"
                stmtList.add(stmt);
            } else {
                // if (arg1 != null && arg2 != null) {
                //  ((Holder.int_Holder) acc).value++;
                // }
                Expression condition = null;
                for (Integer arg : argList) {
                    Expression term =
                        new BinaryExpression(
                            implementor.translateInputField(rel, 0, arg),
                            BinaryExpression.NOTEQUAL,
                            Literal.constantNull());
                    if (condition == null) {
                        condition = term;
                    } else {
                        condition =
                            new BinaryExpression(
                                condition,
                                BinaryExpression.LOGICAL_AND,
                                term);
                    }
                }
                stmtList.add(
                    new IfStatement(
                        condition,
                        new StatementList(stmt)));
            }
        }

        public Expression implementResult(
            JavaRelImplementor implementor,
            Expression accumulator,
            AggregateCall call)
        {
            // e.g. "o" becomes "((Holder.int_Holder) o).value"
            SqlCountAggFunction agg =
                (SqlCountAggFunction) call.getAggregation();
            return new FieldAccess(
                new CastExpression(
                    new TypeName(
                        holderClassName
                        + "." + SqlCountAggFunction.type + "_Holder"),
                    accumulator),
                "value");
        }

        public Expression implementStart(
            JavaRelImplementor implementor,
            JavaRel rel,
            AggregateCall call)
        {
            // e.g. "new Holder.int_Holder(0)"
            SqlCountAggFunction agg =
                (SqlCountAggFunction) call.getAggregation();
            return new AllocationExpression(
                new TypeName(
                    holderClassName
                    + "." + SqlCountAggFunction.type + "_Holder"),
                new ExpressionList(Literal.constantZero()));
        }
    }

    public static class OJMinMaxAggImplementor
        extends OJBasicAggImplementor
    {
        public OJMinMaxAggImplementor()
        {
        }

        public boolean canMerge()
        {
            return true;
        }

        public void implementNext(
            JavaRelImplementor implementor,
            JavaRel rel,
            Expression accumulator,
            AggregateCall call)
        {
            SqlMinMaxAggFunction agg =
                (SqlMinMaxAggFunction) call.getAggregation();
            StatementList stmtList = implementor.getStatementList();
            final List<Integer> argList = call.getArgList();
            switch (agg.getMinMaxKind()) {
            case SqlMinMaxAggFunction.MINMAX_PRIMITIVE:

                // "((Holder.int_Holder) acc).setLesser(arg)"
                Expression arg =
                    implementor.translateInputField(rel, 0, argList.get(0));
                stmtList.add(
                    new ExpressionStatement(
                        new MethodCall(
                            new CastExpression(
                                new TypeName(
                                    holderClassName + "."
                                    + agg.argTypes[0] + "_Holder"),
                                accumulator),
                            agg.isMin() ? "setLesser" : "setGreater",
                            new ExpressionList(arg))));
                return;
            case SqlMinMaxAggFunction.MINMAX_COMPARABLE:

                // T t = arg;
                // if (acc == null || (t != null && t.compareTo(acc) < 0)) {
                //   acc = t;
                // }
                arg = implementor.translateInputField(rel, 0, argList.get(0));
                Variable var_t = implementor.newVariable();
                stmtList.add(
                    new VariableDeclaration(
                        TypeName.forOJClass(
                            OJUtil.typeToOJClass(
                                agg.argTypes[0],
                                implementor.getTypeFactory())),
                        var_t.toString(),
                        arg));
                stmtList.add(
                    new IfStatement(
                        new BinaryExpression(
                            new BinaryExpression(
                                accumulator,
                                BinaryExpression.EQUAL,
                                Literal.constantNull()),
                            BinaryExpression.LOGICAL_OR,
                            new BinaryExpression(
                                new BinaryExpression(
                                    var_t,
                                    BinaryExpression.NOTEQUAL,
                                    Literal.constantNull()),
                                BinaryExpression.LOGICAL_AND,
                                new BinaryExpression(
                                    new MethodCall(
                                        var_t,
                                        "compareTo",
                                        new ExpressionList(accumulator)),
                                    BinaryExpression.LESS,
                                    Literal.constantZero()))),
                        new StatementList(
                            new ExpressionStatement(
                                new AssignmentExpression(
                                    accumulator,
                                    AssignmentExpression.EQUALS,
                                    var_t)))));
                return;
            case SqlMinMaxAggFunction.MINMAX_COMPARATOR:

                // "((Holder.ComparatorHolder)
                // acc).setLesser(arg)"
                arg = implementor.translateInputField(rel, 0, argList.get(1));
                stmtList.add(
                    new ExpressionStatement(
                        new MethodCall(
                            new CastExpression(
                                new TypeName(
                                    holderClassName + "."
                                    + agg.argTypes[1] + "_Holder"),
                                accumulator),
                            agg.isMin() ? "setLesser" : "setGreater",
                            new ExpressionList(arg))));
                return;
            default:
                throw Util.newInternal("bad kind: " + agg.getKind());
            }
        }

        public Expression implementResult(
            JavaRelImplementor implementor,
            Expression accumulator,
            AggregateCall call)
        {
            SqlMinMaxAggFunction agg =
                (SqlMinMaxAggFunction) call.getAggregation();
            switch (agg.getMinMaxKind()) {
            case SqlMinMaxAggFunction.MINMAX_PRIMITIVE:

                // ((Holder.int_Holder) acc).value
                return new FieldAccess(
                    new CastExpression(
                        new TypeName(
                            holderClassName + "." + agg.argTypes[1]
                            + "_Holder"),
                        accumulator),
                    "value");
            case SqlMinMaxAggFunction.MINMAX_COMPARABLE:

                // (T) acc
                return new CastExpression(
                    TypeName.forOJClass(
                        OJUtil.typeToOJClass(
                            agg.argTypes[0],
                            implementor.getTypeFactory())),
                    accumulator);
            case SqlMinMaxAggFunction.MINMAX_COMPARATOR:

                // (T) ((Holder.int_Holder) acc).value
                return new CastExpression(
                    TypeName.forOJClass(
                        OJUtil.typeToOJClass(
                            agg.argTypes[1],
                            implementor.getTypeFactory())),
                    new FieldAccess(
                        new CastExpression(
                            new TypeName(
                                holderClassName + ".ComparatorHolder"),
                            accumulator),
                        "value"));
            default:
                throw Util.newInternal("bad kind: " + agg.getKind());
            }
        }

        public Expression implementStart(
            JavaRelImplementor implementor,
            JavaRel rel,
            AggregateCall call)
        {
            SqlMinMaxAggFunction agg =
                (SqlMinMaxAggFunction) call.getAggregation();
            switch (agg.getMinMaxKind()) {
            case SqlMinMaxAggFunction.MINMAX_PRIMITIVE:

                // "new Holder.int_Holder(Integer.MAX_VALUE)" if
                // the type is "int" and the function is "min"
                return new AllocationExpression(
                    new TypeName(
                        holderClassName + "." + agg.argTypes[0]
                        + "_Holder"),
                    new ExpressionList(
                        new FieldAccess(
                            TypeName.forOJClass(
                                OJUtil.typeToOJClass(
                                    agg.argTypes[0],
                                    implementor.getTypeFactory())
                                .primitiveWrapper()),
                            agg.isMin() ? "MAX_VALUE" : "MIN_VALUE")));
            case SqlMinMaxAggFunction.MINMAX_COMPARABLE:

                // "null"
                return Literal.constantNull();
            case SqlMinMaxAggFunction.MINMAX_COMPARATOR:

                // "new saffron.runtime.ComparatorAndObject(comparator, null)"
                Expression arg =
                    implementor.translateInputField(
                        rel,
                        0,
                        call.getArgList().get(0));
                return new AllocationExpression(
                    new TypeName("saffron.runtime.ComparatorAndObject"),
                    new ExpressionList(
                        arg,
                        Literal.constantNull()));
            default:
                throw Util.newInternal("bad kind: " + agg.getKind());
            }
        }
    }
}

// End OJRexImplementorTableImpl.java
