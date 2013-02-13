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

import net.hydromatic.linq4j.Ord;
import net.hydromatic.linq4j.expressions.*;
import net.hydromatic.optiq.runtime.SqlFunctions;

import org.eigenbase.rel.Aggregation;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.*;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.fun.SqlStdOperatorTable;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

import static net.hydromatic.linq4j.expressions.ExpressionType.*;
import static org.eigenbase.sql.fun.SqlStdOperatorTable.*;

/**
 * Contains implementations of Rex operators as Java code.
 */
public class RexImpTable {
    public static final ConstantExpression NULL_EXPR =
        Expressions.constant(null);
    public static final ConstantExpression FALSE_EXPR =
        Expressions.constant(false);
    public static final ConstantExpression TRUE_EXPR =
        Expressions.constant(true);
    public static final MemberExpression BOXED_FALSE_EXPR =
        Expressions.field(null, Boolean.class, "FALSE");
    public static final MemberExpression BOXED_TRUE_EXPR =
        Expressions.field(null, Boolean.class, "TRUE");

    private final Map<SqlOperator, CallImplementor> map =
        new HashMap<SqlOperator, CallImplementor>();
    final Map<Aggregation, AggregateImplementor> aggMap =
        new HashMap<Aggregation, AggregateImplementor>();
    private final Map<Aggregation, AggImplementor2> agg2Map =
        new HashMap<Aggregation, AggImplementor2>();

    RexImpTable() {
        defineMethod(upperFunc, "upper", NullPolicy.ANY);
        defineMethod(lowerFunc, "lower", NullPolicy.ANY);
        defineMethod(substringFunc, "substring", NullPolicy.ANY);
        defineMethod(characterLengthFunc, "charLength", NullPolicy.ANY);
        defineMethod(charLengthFunc, "charLength", NullPolicy.ANY);
        defineMethod(concatOperator, "concat", NullPolicy.ANY);
        defineMethod(overlayFunc, "overlay", NullPolicy.ANY);

        // logical
        defineBinary(andOperator, AndAlso, NullPolicy.AND, null);
        defineBinary(orOperator, OrElse, NullPolicy.OR, null);
        defineUnary(notOperator, Not);

        // comparisons
        defineBinary(lessThanOperator, LessThan, NullPolicy.ANY, "lt");
        defineBinary(
            lessThanOrEqualOperator, LessThanOrEqual, NullPolicy.ANY, "le");
        defineBinary(
            greaterThanOperator, GreaterThan, NullPolicy.ANY, "gt");
        defineBinary(
            greaterThanOrEqualOperator, GreaterThanOrEqual, NullPolicy.ANY,
            "ge");
        defineBinary(equalsOperator, Equal, NullPolicy.ANY, "eq");
        defineBinary(notEqualsOperator, NotEqual, NullPolicy.ANY, "ne");

        // arithmetic
        defineMethod(plusOperator, "plus", NullPolicy.ANY);
        defineMethod(minusOperator, "minus", NullPolicy.ANY);
        defineMethod(multiplyOperator, "multiply", NullPolicy.ANY);
        defineMethod(divideOperator, "divide", NullPolicy.ANY);
        defineMethod(modFunc, "mod", NullPolicy.ANY);
        defineMethod(expFunc, "exp", NullPolicy.ANY);
        defineMethod(powerFunc, "power", NullPolicy.ANY);
        defineMethod(lnFunc, "ln", NullPolicy.ANY);

        map.put(isNullOperator, new IsNullImplementor());
        map.put(isNotNullOperator, new IsNotNullImplementor());
        map.put(isTrueOperator, new IsTrueImplementor());
        map.put(isNotTrueOperator, new IsNotTrueImplementor());
        map.put(isFalseOperator, new IsFalseImplementor());
        map.put(isNotFalseOperator, new IsNotFalseImplementor());
        map.put(caseOperator, new CaseImplementor());
        map.put(SqlStdOperatorTable.castFunc, new CastImplementor());

        aggMap.put(countOperator, new BuiltinAggregateImplementor("longCount"));
        aggMap.put(sumOperator, new BuiltinAggregateImplementor("sum"));
        aggMap.put(minOperator, new BuiltinAggregateImplementor("min"));
        aggMap.put(maxOperator, new BuiltinAggregateImplementor("max"));

        agg2Map.put(countOperator, new CountImplementor2());
        agg2Map.put(sumOperator, new SumImplementor2());
        final MinMaxImplementor2 minMax =
            new MinMaxImplementor2();
        agg2Map.put(minOperator, minMax);
        agg2Map.put(maxOperator, minMax);
    }

    private static boolean isNullable(Type type) {
        return Primitive.of(type) == null;
    }

    private void defineMethod(
        SqlOperator operator, String functionName, NullPolicy policy)
    {
        map.put(operator, new MethodImplementor(functionName, policy));
    }

    private void defineUnary(
        SqlOperator operator, ExpressionType expressionType)
    {
        map.put(operator, new UnaryImplementor(expressionType));
    }

    private void defineBinary(
        SqlOperator operator,
        ExpressionType expressionType,
        NullPolicy policy,
        String backupMethodName)
    {
        map.put(
            operator,
            new BinaryImplementor(expressionType, policy, backupMethodName));
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

    static Expression optimize(Expression expression) {
        return expression.accept(new OptimizeVisitor());
    }

    static Expression optimize2(Expression operand, Expression expression) {
        return optimize(
            Expressions.condition(
                Expressions.equal(
                    operand,
                    NULL_EXPR),
                NULL_EXPR,
                expression));
    }

    private static boolean nullable(RexCall call, int i) {
        return call.getOperands()[i].getType().isNullable();
    }

    /** Ensures that operands have identical type. */
    private static List<RexNode> harmonize(
        final RexBuilder builder, final List<RexNode> operands)
    {
        int nullCount = 0;
        final List<RelDataType> types = new ArrayList<RelDataType>();
        for (RexNode operand : operands) {
            if (operand.getType().isNullable()) {
                ++nullCount;
            }
            types.add(operand.getType());
        }
        if (allSame(types)) {
            // Operands have the same nullability and type. Return them
            // unchanged.
            return operands;
        }
        final RelDataType type =
            builder.getTypeFactory().leastRestrictive(types);
        assert (nullCount > 0) == type.isNullable();
        final List<RexNode> list = new ArrayList<RexNode>();
        for (RexNode operand : operands) {
            list.add(
                builder.ensureType(
                    type,
                    operand,
                    true));
        }
        return list;
    }

    private static <E> boolean allSame(List<E> list) {
        E prev = null;
        for (E e : list) {
            if (prev != null && !prev.equals(e)) {
                return false;
            }
            prev = e;
        }
        return true;
    }

    private static Expression implementNullSemantics(
        RexToLixTranslator translator,
        RexCall call,
        List<RexNode> operands,
        NullableCallImplementor implementor)
    {
        final List<Expression> list = new ArrayList<Expression>();
        final List<RexNode> operands2 =
            new ArrayList<RexNode>(operands);
        for (Ord<RexNode> operand : Ord.zip(operands)) {
            if (operand.e.getType().isNullable()) {
                list.add(
                    Expressions.equal(
                        translator.translate(operand.e), NULL_EXPR));
                operands2.set(
                    operand.i,
                    translator.builder.makeCast(
                        translator.typeFactory.createTypeWithNullability(
                            operand.e.getType(), false),
                        operand.e));
            }
        }
        return optimize(
            Expressions.condition(
                JavaRules.EnumUtil.foldOr(list),
                NULL_EXPR,
                box(
                    implementor.implement(
                        translator, call, operands2, NullPolicy.NONE))));
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

    /** Converts e.g. "anInteger" to "Integer.valueOf(anInteger)". */
    private static Expression box(Expression expression) {
        Primitive primitive = Primitive.of(expression.getType());
        if (primitive != null) {
            return RexToLixTranslator.convert(
                expression,
                primitive.boxClass);
        }
        return expression;
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
        /** Implements a call. */
        Expression implement(
            RexToLixTranslator translator,
            RexCall call,
            boolean mayBeNull);
    }

    interface NullableCallImplementor extends CallImplementor {
        /** Implements a call using a particular policy for handling null
         * values. Intended to be called back internally. */
        Expression implement(
            RexToLixTranslator translator,
            RexCall call,
            List<RexNode> operands,
            NullPolicy nullPolicy);
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
            Primitive primitive = Primitive.of(returnType);
            if (primitive == null) {
                primitive = Primitive.ofBox(returnType);
            }
            if (primitive == null) {
                assert returnType == BigDecimal.class
                    : "expected primitive or boxed primitive, got "
                    + returnType;
                primitive = Primitive.INT;
            }
            return Expressions.constant(primitive.number(0), returnType);
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
            return Types.castIfNecessary(
                accumulator.type,
                Expressions.add(
                    accumulator,
                    Types.castIfNecessary(accumulator.type, arguments.get(0))));
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
                returnType, Expressions.constant(null));
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
            return Expressions.condition(
                Expressions.equal(accumulator, NULL_EXPR),
                arg,
                Expressions.convert_(
                    Expressions.call(
                        SqlFunctions.class,
                        aggregation == minOperator ? "lesser" : "greater",
                        unbox(accumulator),
                        arg),
                    arg.getType()));
        }

        public Expression implementResult(
            Aggregation aggregation, Expression accumulator)
        {
            return accumulator;
        }
    }

    private static class MethodImplementor implements NullableCallImplementor {
        private final String methodName;
        private final NullPolicy nullPolicy;

        MethodImplementor(String methodName, NullPolicy nullPolicy) {
            this.methodName = methodName;
            this.nullPolicy = nullPolicy;
        }

        public Expression implement(
            RexToLixTranslator translator,
            RexCall call,
            boolean mayBeNull)
        {
            return implement(
                translator, call, call.getOperandList(), nullPolicy);
        }

        public Expression implement(
            RexToLixTranslator translator,
            RexCall call,
            List<RexNode> operands,
            NullPolicy nullPolicy)
        {
            switch (nullPolicy) {
            case ANY:
                return implementNullSemantics(translator, call, operands, this);
            default:
                return Expressions.call(
                    SqlFunctions.class,
                    methodName,
                    translator.translateList(operands));
            }
        }
    }

    private static class BinaryImplementor implements NullableCallImplementor {
        /** Types that can be arguments to comparison operators such as
         * {@code <}. */
        private static final List<Primitive> COMP_OP_TYPES =
            Arrays.asList(
                Primitive.BYTE,
                Primitive.CHAR,
                Primitive.SHORT,
                Primitive.INT,
                Primitive.LONG,
                Primitive.FLOAT,
                Primitive.DOUBLE);

        private final ExpressionType expressionType;
        private final NullPolicy nullPolicy;
        private final String backupMethodName;

        BinaryImplementor(
            ExpressionType expressionType,
            NullPolicy nullPolicy,
            String backupMethodName)
        {
            this.expressionType = expressionType;
            this.nullPolicy = nullPolicy;
            this.backupMethodName = backupMethodName;
        }

        public Expression implement(
            RexToLixTranslator translator,
            RexCall call,
            boolean mayBeNull)
        {
            return implement(
                translator, call, call.getOperandList(), nullPolicy);
        }

        public Expression implement(
            RexToLixTranslator translator,
            RexCall call,
            List<RexNode> operands,
            NullPolicy nullPolicy)
        {
            // neither nullable:
            //   return x OP y
            // x nullable
            //   null_returns_null
            //     return x == null ? null : x OP y
            //   ignore_null
            //     return x == null ? null : y
            // x, y both nullable
            //   null_returns_null
            //     return x == null || y == null ? null : x OP y
            //   ignore_null
            //     return x == null ? y : y == null ? x : x OP y
            final List<RexNode> operands2 =
                harmonize(translator.builder, operands);
            final Expression t0 = translator.translate(operands2.get(0));
            final Expression t1 = translator.translate(operands2.get(1));
            switch (nullPolicy) {
            case ANY:
                return implementNullSemantics(
                    translator, call, operands2, this);

            case AND:
                // If any of the arguments are false, result is false;
                // else if any arguments are null, result is null;
                // else true.
                //
                // b0 == null ? (b1 == null || b1 ? null : Boolean.FALSE)
                //   : b0 ? b1
                //   : Boolean.FALSE;
                if (!nullable(call, 0) && !nullable(call, 1)) {
                    return Expressions.andAlso(t0, t1);
                }
                return optimize(
                    Expressions.condition(
                        Expressions.equal(t0, NULL_EXPR),
                        Expressions.condition(
                            Expressions.orElse(
                                Expressions.equal(t1, NULL_EXPR),
                                t1),
                            NULL_EXPR,
                            BOXED_FALSE_EXPR),
                        Expressions.condition(
                            t0,
                            t1,
                            BOXED_FALSE_EXPR)));

            case OR:
                // If any of the arguments are true, result is true;
                // else if any arguments are null, result is null;
                // else false.
                //
                // b0 == null ? (b1 == null || !b1 ? null : Boolean.TRUE)
                //   : !b0 ? b1
                //   : Boolean.TRUE;
                if (!nullable(call, 0) && !nullable(call, 1)) {
                    return Expressions.orElse(t0, t1);
                }
                return optimize(
                    Expressions.condition(
                        Expressions.equal(t0, NULL_EXPR),
                        Expressions.condition(
                            Expressions.orElse(
                                Expressions.equal(t1, NULL_EXPR),
                                Expressions.not(t1)),
                            NULL_EXPR,
                            BOXED_TRUE_EXPR),
                        Expressions.condition(
                            Expressions.not(t0),
                            t1,
                            BOXED_TRUE_EXPR)));

            case NONE:
                if (backupMethodName != null) {
                    final Primitive primitive = Primitive.of(t0.getType());
                    if (primitive == null
                        || !COMP_OP_TYPES.contains(primitive))
                    {
                        return Expressions.call(
                            SqlFunctions.class,
                            backupMethodName,
                            translator.translateList(operands));
                    }
                }
                return Expressions.makeBinary(expressionType, t0, t1);

            default:
                throw new AssertionError(nullPolicy);
            }
        }
    }

    private static class UnaryImplementor implements NullableCallImplementor {
        private final ExpressionType expressionType;

        UnaryImplementor(ExpressionType expressionType) {
            this.expressionType = expressionType;
        }

        public Expression implement(
            RexToLixTranslator translator,
            RexCall call,
            boolean mayBeNull)
        {
            return implement(
                translator, call, call.getOperandList(), NullPolicy.ANY);
        }

        public Expression implement(
            RexToLixTranslator translator,
            RexCall call,
            List<RexNode> operands,
            NullPolicy nullPolicy)
        {
            switch (nullPolicy) {
            case ANY:
                final List<RexNode> operands2 =
                    harmonize(translator.builder, operands);
                return implementNullSemantics(
                    translator, call, operands2, this);
            case NONE:
                return Expressions.makeUnary(
                    expressionType,
                    translator.translate(operands.get(0)));
            default:
                throw new AssertionError(nullPolicy);
            }
        }
    }

    enum NullPolicy {
        /** If any of the arguments are null, return null. */
        ANY,
        /** If any of the arguments are false, result is false; else if any
         * arguments are null, result is null; else true. */
        AND,
        /** If any of the arguments are true, result is true; else if any
         * arguments are null, result is null; else false. */
        OR,
        NONE
    }

    /** Visitor that optimizes expressions.
     *
     * <p>The optimizations are essential, not mere tweaks. Without
     * optimization, expressions such as {@code false == null} will be left in,
     * which are invalid to Janino (because it does not automatically box
     * primitives).</p>
     */
    static class OptimizeVisitor extends Visitor {
        @Override
        public Expression visit(
            TernaryExpression ternaryExpression,
            Expression expression0,
            Expression expression1,
            Expression expression2)
        {
            final TernaryExpression ternary = (TernaryExpression) super.visit(
                ternaryExpression, expression0, expression1, expression2);
            switch (ternary.getNodeType()) {
            case Conditional:
                Boolean always = always(ternary.expression0);
                if (always != null) {
                    // true ? y : z  ===  y
                    // false ? y : z  === z
                    return always
                        ? ternary.expression1
                        : ternary.expression2;
                }
                if (ternary.expression1.equals(ternary.expression2)) {
                    // a ? b : b   ===   b
                    return ternary.expression1;
                }
            }
            return ternary;
        }

        @Override
        public Expression visit(
            BinaryExpression binaryExpression,
            Expression expression0,
            Expression expression1)
        {
            final BinaryExpression binary = (BinaryExpression) super.visit(
                binaryExpression, expression0, expression1);
            Boolean always;
            switch (binary.getNodeType()) {
            case AndAlso:
                always = always(binary.expression0);
                if (always != null) {
                    return always
                        ? binary.expression1
                        : FALSE_EXPR;
                }
                always = always(binary.expression1);
                if (always != null) {
                    return always
                        ? binary.expression0
                        : FALSE_EXPR;
                }
                break;
            case OrElse:
                always = always(binary.expression0);
                if (always != null) {
                    // true or x  --> true
                    // null or x  --> x
                    // false or x --> x
                    return !always
                        ? binary.expression1
                        : TRUE_EXPR;
                }
                always = always(binary.expression1);
                if (always != null) {
                    return !always
                        ? binary.expression0
                        : TRUE_EXPR;
                }
                break;
            case Equal:
                if (binary.expression0 instanceof ConstantExpression
                    && binary.expression1 instanceof ConstantExpression)
                {
                    return binary.expression0.equals(binary.expression1)
                        ? TRUE_EXPR : FALSE_EXPR;
                }
                break;
            case NotEqual:
                if (binary.expression0 instanceof ConstantExpression
                    && binary.expression1 instanceof ConstantExpression)
                {
                    return !binary.expression0.equals(binary.expression1)
                        ? TRUE_EXPR : FALSE_EXPR;
                }
                break;
            }
            return binary;
        }

        /** Returns whether an expression always evaluates to true or false.
         * Assumes that expression has already been optimized. */
        private static Boolean always(Expression x) {
            if (x.equals(FALSE_EXPR) || x.equals(BOXED_FALSE_EXPR)) {
                return Boolean.FALSE;
            }
            if (x.equals(TRUE_EXPR) || x.equals(BOXED_TRUE_EXPR)) {
                return Boolean.TRUE;
            }
            return null;
        }
    }

    private static class CaseImplementor implements CallImplementor {
        public Expression implement(
            RexToLixTranslator translator,
            RexCall call,
            boolean mayBeNull)
        {
            return implementRecurse(translator, call, 0);
        }

        private Expression implementRecurse(
            RexToLixTranslator translator, RexCall call, int i)
        {
            RexNode[] operands = call.getOperands();
            if (i == operands.length - 1) {
                // the "else" clause
                return translator.translate(
                    translator.builder.ensureType(
                        call.getType(), operands[i], false));
            } else {
                return Expressions.condition(
                    translator.translateCondition(operands[i]),
                    translator.translate(
                        translator.builder.ensureType(
                            call.getType(), operands[i + 1], false)),
                    implementRecurse(translator, call, i + 2));
            }
        }
    }

    private static class CastImplementor implements CallImplementor {
        public Expression implement(
            RexToLixTranslator translator,
            RexCall call,
            boolean mayBeNull)
        {
            assert call.getOperands().length == 1;
            RexNode expr = call.getOperands()[0];
            RelDataType type = call.getType();
            if (!mayBeNull) {
                type = translator.typeFactory
                    .createTypeWithNullability(type, false);
            }
            return translator.translateCast(expr, type);
        }
    }

    private static class IsNullImplementor implements CallImplementor {
        public Expression implement(
            RexToLixTranslator translator,
            RexCall call,
            boolean mayBeNull)
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
    }

    private static class IsNotNullImplementor implements CallImplementor {
        public Expression implement(
            RexToLixTranslator translator,
            RexCall call,
            boolean mayBeNull)
        {
            RexNode[] operands = call.getOperands();
            assert operands.length == 1;
            final Expression translate =
                translator.translate(operands[0]);
            if (!isNullable(translate.getType())) {
                return TRUE_EXPR;
            }
            return Expressions.notEqual(translate, NULL_EXPR);
        }
    }

    private static class IsTrueImplementor implements CallImplementor {
        public Expression implement(
            RexToLixTranslator translator,
            RexCall call,
            boolean mayBeNull)
        {
            RexNode[] operands = call.getOperands();
            assert operands.length == 1;
            final Expression translate =
                translator.translate(operands[0]);
            if (!isNullable(translate.getType())) {
                return translate;
            }
            return Expressions.andAlso(
                Expressions.notEqual(translate, NULL_EXPR), translate);
        }
    }

    private static class IsNotTrueImplementor implements CallImplementor {
        public Expression implement(
            RexToLixTranslator translator,
            RexCall call,
            boolean mayBeNull)
        {
            RexNode[] operands = call.getOperands();
            assert operands.length == 1;
            final Expression translate =
                translator.translate(operands[0]);
            if (!isNullable(translate.getType())) {
                return Expressions.not(translate);
            }
            return Expressions.orElse(
                Expressions.equal(translate, NULL_EXPR),
                Expressions.not(translate));
        }
    }

    private static class IsFalseImplementor implements CallImplementor {
        public Expression implement(
            RexToLixTranslator translator,
            RexCall call,
            boolean mayBeNull)
        {
            RexNode[] operands = call.getOperands();
            assert operands.length == 1;
            final Expression translate =
                translator.translate(operands[0]);
            if (!isNullable(translate.getType())) {
                return Expressions.not(translate);
            }
            return Expressions.andAlso(
                Expressions.notEqual(translate, NULL_EXPR),
                Expressions.not(translate));
        }
    }

    private static class IsNotFalseImplementor implements CallImplementor {
        public Expression implement(
            RexToLixTranslator translator,
            RexCall call,
            boolean mayBeNull)
        {
            RexNode[] operands = call.getOperands();
            assert operands.length == 1;
            final Expression translate =
                translator.translate(operands[0]);
            if (!isNullable(translate.getType())) {
                return translate;
            }
            return Expressions.orElse(
                Expressions.equal(translate, NULL_EXPR), translate);
        }
    }
}

// End RexImpTable.java
