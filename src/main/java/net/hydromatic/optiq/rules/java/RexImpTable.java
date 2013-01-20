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
package net.hydromatic.optiq.rules.java;

import net.hydromatic.linq4j.expressions.*;
import net.hydromatic.optiq.runtime.SqlFunctions;

import org.eigenbase.rel.Aggregation;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.fun.SqlStdOperatorTable;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.hydromatic.linq4j.expressions.ExpressionType.*;
import static org.eigenbase.sql.fun.SqlStdOperatorTable.*;

/**
 * Contains implementations of Rex operators as Java code.
 */
public class RexImpTable {
    private final Map<SqlOperator, CallImplementor> map =
        new HashMap<SqlOperator, CallImplementor>();
    final Map<Aggregation, AggregateImplementor> aggMap =
        new HashMap<Aggregation, AggregateImplementor>();
    private final Map<Aggregation, AggImplementor2> agg2Map =
        new HashMap<Aggregation, AggImplementor2>();

    RexImpTable() {
        defineMethod(upperFunc, "upper");
        defineMethod(lowerFunc, "lower");
        defineMethod(substringFunc, "substring");
        if (false) {
            defineBinary(andOperator, AndAlso);
            defineBinary(orOperator, OrElse);
            defineBinary(lessThanOperator, LessThan);
            defineBinary(lessThanOrEqualOperator, LessThanOrEqual);
            defineBinary(greaterThanOperator, GreaterThan);
            defineBinary(greaterThanOrEqualOperator, GreaterThanOrEqual);
            defineBinary(equalsOperator, Equal);
            defineBinary(notEqualsOperator, NotEqual);
            defineUnary(notOperator, Not);
        } else {
            // logical
            defineMethod(andOperator, "and");
            defineMethod(orOperator, "or");
            defineMethod(notOperator, "not");

            // comparisons
            defineMethod(lessThanOperator, "lt");
            defineMethod(lessThanOrEqualOperator, "le");
            defineMethod(greaterThanOperator, "gt");
            defineMethod(greaterThanOrEqualOperator, "ge");
            defineMethod(equalsOperator, "eq");
            defineMethod(notEqualsOperator, "ne");

            // arithmetic
            defineMethod(plusOperator, "plus");
            defineMethod(minusOperator, "minus");
            defineMethod(multiplyOperator, "multiply");
            defineMethod(divideOperator, "divide");
            defineMethod(modFunc, "mod");
            defineMethod(expFunc, "exp");
        }
        map.put(
            isNullOperator,
            new CallImplementor() {
                public Expression implement(
                    RexToLixTranslator translator, RexCall call)
                {
                    RexNode[] operands = call.getOperands();
                    assert operands.length == 1;
                    final Expression translate =
                        translator.translate(operands[0]);
                    if (!isNullable(translate.getType())) {
                        return Expressions.constant(false);
                    }
                    return Expressions.equal(
                        translate, Expressions.constant(null));
                }
        });
        map.put(
            isNotNullOperator,
            new CallImplementor() {
                public Expression implement(
                    RexToLixTranslator translator, RexCall call)
                {
                    RexNode[] operands = call.getOperands();
                    assert operands.length == 1;
                    final Expression translate =
                        translator.translate(operands[0]);
                    if (!isNullable(translate.getType())) {
                        return Expressions.constant(true);
                    }
                    return Expressions.notEqual(
                        translate, Expressions.constant(null));
                }
            });
        map.put(
            isTrueOperator,
            new CallImplementor() {
                public Expression implement(
                    RexToLixTranslator translator, RexCall call)
                {
                    RexNode[] operands = call.getOperands();
                    assert operands.length == 1;
                    final Expression translate =
                        translator.translate(operands[0]);
                    if (!isNullable(translate.getType())) {
                        return translate;
                    }
                    return Expressions.andAlso(
                        Expressions.notEqual(
                            translate, Expressions.constant(null)),
                        translate);
                }
        });
        map.put(
            isNotTrueOperator,
            new CallImplementor() {
                public Expression implement(
                    RexToLixTranslator translator, RexCall call)
                {
                    RexNode[] operands = call.getOperands();
                    assert operands.length == 1;
                    final Expression translate =
                        translator.translate(operands[0]);
                    if (!isNullable(translate.getType())) {
                        return Expressions.not(translate);
                    }
                    return Expressions.orElse(
                        Expressions.equal(
                            translate, Expressions.constant(null)),
                        Expressions.not(translate));
                }
        });
        map.put(
            isFalseOperator,
            new CallImplementor() {
                public Expression implement(
                    RexToLixTranslator translator, RexCall call)
                {
                    RexNode[] operands = call.getOperands();
                    assert operands.length == 1;
                    final Expression translate =
                        translator.translate(operands[0]);
                    if (!isNullable(translate.getType())) {
                        return Expressions.not(translate);
                    }
                    return Expressions.andAlso(
                        Expressions.notEqual(
                            translate, Expressions.constant(null)),
                        Expressions.not(translate));
                }
            });
        map.put(
            isNotFalseOperator,
            new CallImplementor() {
                public Expression implement(
                    RexToLixTranslator translator, RexCall call)
                {
                    RexNode[] operands = call.getOperands();
                    assert operands.length == 1;
                    final Expression translate =
                        translator.translate(operands[0]);
                    if (!isNullable(translate.getType())) {
                        return translate;
                    }
                    return Expressions.orElse(
                        Expressions.equal(
                            translate, Expressions.constant(null)),
                        translate);
                }
            });
        map.put(
            caseOperator,
            new CallImplementor() {
                public Expression implement(
                    RexToLixTranslator translator,
                    RexCall call)
                {
                    return implementRecurse(translator, call, 0);
                }

                private Expression implementRecurse(
                    RexToLixTranslator translator,
                    RexCall call,
                    int i)
                {
                    RexNode[] operands = call.getOperands();
                    if (i == operands.length - 1) {
                        // the "else" clause
                        return translator.translate(operands[i]);
                    } else {
                        return Expressions.condition(
                            translator.translate(operands[i]),
                            translator.translate(operands[i + 1]),
                            implementRecurse(translator, call, i + 2));
                    }
                }
            });
        map.put(
            SqlStdOperatorTable.castFunc,
            new CallImplementor() {
                public Expression implement(
                    RexToLixTranslator translator,
                    RexCall call)
                {
                    assert call.getOperands().length == 1;
                    RexNode expr = call.getOperands()[0];
                    Expression operand = translator.translate(expr);
                    return RexToLixTranslator.convert(
                        operand,
                        translator.typeFactory.getJavaClass(call.getType()));
                }
            });
        aggMap.put(
            countOperator,
            new BuiltinAggregateImplementor("longCount"));
        aggMap.put(
            sumOperator,
            new BuiltinAggregateImplementor("sum"));
        aggMap.put(
            minOperator,
            new BuiltinAggregateImplementor("min"));
        aggMap.put(
            maxOperator,
            new BuiltinAggregateImplementor("max"));

        agg2Map.put(countOperator, new CountImplementor2());
        agg2Map.put(sumOperator, new SumImplementor2());
        final MinMaxImplementor2 minMax =
            new MinMaxImplementor2();
        agg2Map.put(minOperator, minMax);
        agg2Map.put(maxOperator, minMax);
    }

    private boolean isNullable(Type type) {
        return Primitive.of(type) == null;
    }

    private void defineMethod(SqlOperator operator, String functionName) {
        map.put(operator, new MethodImplementor(functionName));
    }

    private void defineUnary(
        SqlOperator operator, ExpressionType expressionType)
    {
        map.put(operator, new UnaryImplementor(expressionType));
    }

    private void defineBinary(
        SqlOperator operator, ExpressionType expressionType)
    {
        map.put(operator, new BinaryImplementor(expressionType));
    }

    public static final RexImpTable INSTANCE = new RexImpTable();

    public CallImplementor get(final SqlOperator operator) {
        return map.get(operator);
    }

    public AggregateImplementor get(final Aggregation aggregation) {
        return aggMap.get(aggregation);
    }

    public AggImplementor2 get2(final Aggregation aggregation) {
        return agg2Map.get(aggregation);
    }

    /** Implements an aggregate function by generating a call to a method that
     * takes an enumeration and an accessor function. */
    interface AggregateImplementor {
        Expression implementAggregate(
            Expression grouping, Expression accessor);
    }

    /** Implements an aggregate function by generating expressions to
     * initialize, add to, and get a result from, an accumulator. */
    interface AggImplementor2 {
        /** Whether "add" code is called if any of the arguments are null. If
         * false, the container will ensure that the "add" arguments are always
         * not-null. If true, the container code must handle null values
         * appropriately. */
        boolean callOnNull();
        Expression implementInit(
            Aggregation aggregation,
            Type returnType,
            List<Type> parameterTypes);
        Expression implementAdd(
            Aggregation aggregation,
            Expression accumulator,
            List<Expression> arguments);
        Expression implementResult(
            Aggregation aggregation, Expression accumulator);
    }

    interface CallImplementor {
        Expression implement(
            RexToLixTranslator translator, RexCall call);
    }

    static class BuiltinAggregateImplementor
        implements AggregateImplementor
    {
        private final String methodName;

        public BuiltinAggregateImplementor(String methodName) {
            this.methodName = methodName;
        }

        public Expression implementAggregate(
            Expression grouping, Expression accessor)
        {
            return accessor == null
                ?  Expressions.call(grouping, methodName)
                :  Expressions.call(grouping, methodName, accessor);
        }
    }

    static class CountImplementor2 implements AggImplementor2 {
        public boolean callOnNull() {
            return false;
        }

        public Expression implementInit(
            Aggregation aggregation,
            Type returnType,
            List<Type> parameterTypes)
        {
            return Expressions.constant(0, returnType);
        }

        public Expression implementAdd(
            Aggregation aggregation,
            Expression accumulator,
            List<Expression> arguments)
        {
            // We don't need to check whether the argument is NULL. callOnNull()
            // returned false, so that container has checked for us.
            return Expressions.add(
                accumulator, Expressions.constant(1, accumulator.type));
        }

        public Expression implementResult(
            Aggregation aggregation, Expression accumulator)
        {
            return accumulator;
        }
    }

    static class SumImplementor2 implements AggImplementor2 {
        public boolean callOnNull() {
            return false;
        }

        public Expression implementInit(
            Aggregation aggregation,
            Type returnType,
            List<Type> parameterTypes)
        {
            return Expressions.constant(0, returnType);
        }

        public Expression implementAdd(
            Aggregation aggregation,
            Expression accumulator,
            List<Expression> arguments)
        {
            assert arguments.size() == 1;
            if (accumulator.type == BigDecimal.class
                || accumulator.type == BigInteger.class)
            {
                return Expressions.call(
                    accumulator,
                    "add",
                    arguments.get(0));
            }
            return Expressions.add(
                accumulator,
                Types.castIfNecessary(accumulator.type, arguments.get(0)));
        }

        public Expression implementResult(
            Aggregation aggregation, Expression accumulator)
        {
            return accumulator;
        }
    }

    static class MinMaxImplementor2 implements AggImplementor2 {
        public boolean callOnNull() {
            return false;
        }

        public Expression implementInit(
            Aggregation aggregation,
            Type returnType,
            List<Type> parameterTypes)
        {
            final Primitive primitive = Primitive.of(returnType);
            if (primitive != null) {
                // allow nulls even if input does not
                returnType = primitive.boxClass;
            }
            return Types.castIfNecessary(
                returnType,
                Expressions.constant(null));
        }

        public Expression implementAdd(
            Aggregation aggregation,
            Expression accumulator,
            List<Expression> arguments)
        {
            // Need to check for null accumulator (e.g. first call to "add"
            // after "init") but because callWithNull() returned false, the
            // container has ensured that argument is not null.
            //
            // acc = acc == null
            //   ? arg
            //   : lesser(acc, arg)
            assert arguments.size() == 1;
            final Expression arg = arguments.get(0);
            final ConstantExpression constantNull = Expressions.constant(null);
            return Expressions.condition(
                Expressions.equal(accumulator, constantNull),
                arg,
                Expressions.convert_(
                    Expressions.call(
                        SqlFunctions.class,
                        aggregation == minOperator ? "lesser" : "greater",
                        unbox(accumulator),
                        arg), arg.getType()));
        }

        /** Converts e.g. "anInteger" to "anInteger.intValue()". */
        private static Expression unbox(Expression expression) {
            Primitive primitive = Primitive.ofBox(expression.getType());
            if (primitive != null) {
                return RexToLixTranslator.convert(
                    expression,
                    primitive.primitiveClass);
            }
            return expression;
        }

        private static Expression optimizedCondition(
            Expression condition,
            Expression ifTrue,
            Expression ifFalse)
        {
            if (alwaysTrue(condition)) {
                return ifTrue;
            } else if (alwaysFalse(condition)) {
                return ifFalse;
            } else {
                return Expressions.condition(condition, ifTrue, ifFalse);
            }
        }

        private static boolean alwaysFalse(Expression x) {
            return x.equals(Expressions.constant(false));
        }

        private static boolean alwaysTrue(Expression x) {
            return x.equals(Expressions.constant(true));
        }

        public Expression implementResult(
            Aggregation aggregation, Expression accumulator)
        {
            return accumulator;
        }
    }

    private static class MethodImplementor implements CallImplementor {
        private final String methodName;

        MethodImplementor(String methodName) {
            this.methodName = methodName;
        }

        public Expression implement(
            RexToLixTranslator translator, RexCall call)
        {
            return Expressions.call(
                SqlFunctions.class,
                methodName,
                translator.translateList(Arrays.asList(call.getOperands())));
        }
    }

    private static class BinaryImplementor implements CallImplementor {
        private final ExpressionType expressionType;

        BinaryImplementor(ExpressionType expressionType) {
            this.expressionType = expressionType;
        }

        public Expression implement(
            RexToLixTranslator translator, RexCall call)
        {
            return Expressions.makeBinary(
                expressionType,
                translator.translate(call.getOperands()[0]),
                translator.translate(call.getOperands()[1]));
        }
    }

    private static class UnaryImplementor implements CallImplementor {
        private final ExpressionType expressionType;

        UnaryImplementor(ExpressionType expressionType) {
            this.expressionType = expressionType;
        }

        public Expression implement(
            RexToLixTranslator translator, RexCall call)
        {
            return Expressions.makeUnary(
                expressionType,
                translator.translate(call.getOperands()[0]));
        }
    }
}

// End RexImpTable.java

