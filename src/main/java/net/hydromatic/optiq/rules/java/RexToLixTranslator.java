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

import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.runtime.SqlFunctions;

import org.eigenbase.rel.Aggregation;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.*;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.util.Util;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.*;

import static net.hydromatic.linq4j.expressions.ExpressionType.*;
import static org.eigenbase.sql.fun.SqlStdOperatorTable.*;

/**
 * Translates {@link org.eigenbase.rex.RexNode REX expressions} to
 * {@link net.hydromatic.linq4j.expressions.Expression linq4j expressions}.
 *
 * @author jhyde
 */
public class RexToLixTranslator {
    public static final Map<Method, SqlOperator> JAVA_TO_SQL_METHOD_MAP =
        Util.<Method, SqlOperator>mapOf(
            findMethod(String.class, "toUpperCase"), upperFunc,
            findMethod(
                SqlFunctions.class, "substring", String.class, Integer.TYPE,
                Integer.TYPE),
            substringFunc);

    private final Map<RexNode, Slot> map = new HashMap<RexNode, Slot>();
    private final JavaTypeFactory typeFactory;
    private final RexProgram program;
    private final List<Slot> inputSlots = new ArrayList<Slot>();

    /** Set of expressions which are to be translated inline. That is, they
     * should not be assigned to variables on first use. At present, the
     * algorithm is to use a first pass to determine how many times each
     * expression is used, and expressions are marked for inlining if they are
     * used at most once. */
    private final Set<RexNode> inlineRexSet = new HashSet<RexNode>();

    private List<Statement> list;

    private static Method findMethod(
        Class<?> clazz, String name, Class... parameterTypes)
    {
        try {
            return clazz.getMethod(name, parameterTypes);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private RexToLixTranslator(
        RexProgram program,
        JavaTypeFactory typeFactory,
        List<Expression> inputs)
    {
        this.program = program;
        this.typeFactory = typeFactory;
        for (Expression input : inputs) {
            inputSlots.add(new Slot(null, input));
        }
    }

    /**
     * Translates a {@link RexProgram} to a sequence of expressions and
     * declarations.
     *
     * @param inputs Variables holding the current record of each input
     * relational expression
     * @param program Program to be translated
     * @return Sequence of expressions, optional condition
     */
    public static List<Expression> translateProjects(
        List<Expression> inputs,
        RexProgram program,
        JavaTypeFactory typeFactory,
        List<Statement> list)
    {
        return new RexToLixTranslator(program, typeFactory, inputs)
            .translate(list, program.getProjectList());
    }

    private Expression translate(RexNode expr) {
        Slot slot = map.get(expr);
        if (slot == null) {
            Expression expression = translate0(expr);
            assert expression != null;
            final ParameterExpression parameter;
            if (!inlineRexSet.contains(expr)
                && !(expr instanceof RexLocalRef))
            {
                parameter =
                    Expressions.parameter(
                        expression.getType(),
                        "v" + map.size());
            } else {
                parameter = null;
            }
            slot = new Slot(
                parameter,
                expression);
            if (parameter != null && list != null) {
                list.add(
                    Expressions.declare(
                        Modifier.FINAL,
                        slot.parameterExpression,
                        slot.expression));
            }
            map.put(expr, slot);
        }
        slot.count++;
        return slot.parameterExpression != null
            ? slot.parameterExpression
            : slot.expression;
    }

    private Expression translate0(RexNode expr) {
        if (expr instanceof RexInputRef) {
            // TODO: multiple inputs, e.g. joins
            final Expression input = getInput(0);
            final int index = ((RexInputRef) expr).getIndex();
            final List<RelDataTypeField> fields =
                program.getInputRowType().getFieldList();
            final RelDataTypeField field = fields.get(index);
            if (fields.size() == 1) {
                return input;
            } else if (input.getType() == Object[].class) {
                return Expressions.convert_(
                    Expressions.arrayIndex(
                        input, Expressions.constant(field.getIndex())),
                    Types.box(
                        JavaRules.EnumUtil.javaClass(
                            typeFactory, field.getType())));
            } else if (Types.isPrimitive(Types.unbox(input.getType()))
                || input.getType() == String.class)
            {
                return input;
            } else {
                return Expressions.field(input, field.getName());
            }
        }
        if (expr instanceof RexLocalRef) {
            return translate(
                program.getExprList().get(((RexLocalRef) expr).getIndex()));
        }
        if (expr instanceof RexLiteral) {
            Type javaClass = typeFactory.getJavaClass(expr.getType());
            if (javaClass == BigDecimal.class) {
                return Expressions.new_(
                    BigDecimal.class,
                    Arrays.<Expression>asList(
                        Expressions.constant(
                            ((RexLiteral) expr).getValue3().toString())));
            }
            return Expressions.constant(
                ((RexLiteral) expr).getValue3(), javaClass);
        }
        if (expr instanceof RexCall) {
            final RexCall call = (RexCall) expr;
            final SqlOperator operator = call.getOperator();
            CallImplementor implementor = ImpTable.INSTANCE.get(operator);
            if (implementor != null) {
                return implementor.implement(this, call);
            }
        }
        switch (expr.getKind()) {
        default:
            throw new RuntimeException(
                "cannot translate expression " + expr);
        }
    }

    /**
     * Gets the expression for an input and counts it.
     *
     * @param index Input ordinal
     * @return Expression to which an input should be translated
     */
    private Expression getInput(int index) {
        Slot slot = inputSlots.get(index);
        if (list == null) {
            slot.count++;
        } else {
            if (slot.count > 1 && slot.parameterExpression == null) {
                slot.parameterExpression =
                    Expressions.parameter(
                        slot.expression.type, "current" + index);
                list.add(
                    Expressions.declare(
                        Modifier.FINAL,
                        slot.parameterExpression,
                        slot.expression));
            }
        }
        return slot.parameterExpression != null
            ? slot.parameterExpression
            : slot.expression;
    }

    private List<Expression> translateList(List<RexNode> operandList) {
        final List<Expression> list = new ArrayList<Expression>();
        for (RexNode rex : operandList) {
            list.add(translate(rex));
        }
        return list;
    }

    private List<Expression> translate(
        List<Statement> list,
        List<RexLocalRef> rexList)
    {
        // First pass. Count how many times each sub-expression is used.
        this.list = null;
        for (RexNode rexExpr : rexList) {
            translate(rexExpr);
        }

        // Mark expressions as inline if they are not used more than once.
        for (Map.Entry<RexNode, Slot> entry : map.entrySet()) {
            if (entry.getValue().count < 2
                || entry.getKey() instanceof RexLiteral)
            {
                inlineRexSet.add(entry.getKey());
            }
        }

        // Second pass. When translating each expression, if it is used more
        // than once, the first time it is encountered, add a declaration to the
        // list and set its usage count to 0.
        this.list = list;
        this.map.clear();
        List<Expression> translateds = new ArrayList<Expression>();
        for (RexNode rexExpr : rexList) {
            translateds.add(translate(rexExpr));
        }
        return translateds;
    }

    public static Expression translateCondition(
        List<Expression> inputs,
        RexProgram program,
        JavaTypeFactory typeFactory,
        List<Statement> list)
    {
        List<Expression> x =
            new RexToLixTranslator(program, typeFactory, inputs)
                .translate(
                    list, Collections.singletonList(program.getCondition()));
        assert x.size() == 1;
        return x.get(0);
    }

    public static Expression translateAggregate(
        Expression grouping,
        Aggregation aggregation,
        Expression accessor)
    {
        final AggregateImplementor implementor =
            ImpTable.INSTANCE.aggMap.get(aggregation);
        if (aggregation == countOperator) {
            // FIXME: count(x) and count(distinct x) don't work currently
            accessor = null;
        }
        if (implementor != null) {
            return implementor.implementAggregate(grouping, accessor);
        }
        throw new AssertionError("unknown agg " + aggregation);
    }

    public static Expression convert(
        Expression operand, Type javaType)
    {
        if (operand.getType().equals(javaType)) {
            return operand;
        }
        // E.g. from "Short" to "int".
        // Generate "x.intValue()".
        final Primitive primitive = Primitive.of(javaType);
        final Primitive fromPrimitive =
            Primitive.ofBox(operand.getType());
        if (primitive != null) {
            if (fromPrimitive == null) {
                // E.g. from "Object" to "short".
                // Generate "(Short) x".
                return Expressions.convert_(operand, primitive.boxClass);
            }
            if (fromPrimitive != primitive) {
                return Expressions.call(
                    operand, primitive.primitiveName + "Value");
            }
        }
        return Expressions.convert_(operand, javaType);
    }

    private static class Slot {
        ParameterExpression parameterExpression;
        Expression expression;
        int count;

        public Slot(
            ParameterExpression parameterExpression,
            Expression expression)
        {
            this.parameterExpression = parameterExpression;
            this.expression = expression;
        }
    }

    private interface CallImplementor {
        Expression implement(
            RexToLixTranslator translator,
            RexCall call);
    }

    public static class ImpTable {
        private final Map<SqlOperator, CallImplementor> map =
            new HashMap<SqlOperator, CallImplementor>();
        private final Map<Aggregation, AggregateImplementor> aggMap =
            new HashMap<Aggregation, AggregateImplementor>();
        private final Map<Aggregation, AggregateImplementor2> agg2Map =
            new HashMap<Aggregation, AggregateImplementor2>();

        private ImpTable() {
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
                defineMethod(andOperator, "and");
                defineMethod(orOperator, "or");
                defineMethod(lessThanOperator, "lt");
                defineMethod(lessThanOrEqualOperator, "le");
                defineMethod(greaterThanOperator, "gt");
                defineMethod(greaterThanOrEqualOperator, "ge");
                defineMethod(equalsOperator, "eq");
                defineMethod(notEqualsOperator, "ne");
                defineMethod(notOperator, "not");
            }
            map.put(
                isNullOperator,
                new CallImplementor() {
                    public Expression implement(
                        RexToLixTranslator translator,
                        RexCall call)
                    {
                        RexNode[] operands = call.getOperands();
                        assert operands.length == 1;
                        final Expression translate =
                            translator.translate(operands[0]);
                        return Expressions.notEqual(
                            translate,
                            Expressions.constant(null));
                    }
                }
            );
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
                        return convert(
                            operand,
                            translator.typeFactory.getJavaClass(
                                call.getType()));
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
            final MinMaxImplementor2 minMax = new MinMaxImplementor2();
            agg2Map.put(minOperator, minMax);
            agg2Map.put(maxOperator, minMax);
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

        public static final ImpTable INSTANCE = new ImpTable();

        public CallImplementor get(final SqlOperator operator) {
            return map.get(operator);
        }

        public AggregateImplementor get(final Aggregation aggregation) {
            return aggMap.get(aggregation);
        }

        public AggregateImplementor2 get2(final Aggregation aggregation) {
            return agg2Map.get(aggregation);
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

    /** Implements an aggregate function by generating a call to a method that
     * takes an enumeration and an accessor function. */
    interface AggregateImplementor {
        Expression implementAggregate(
            Expression grouping, Expression accessor);
    }

    /** Implements an aggregate function by generating expressions to
     * initialize, add to, and get a result from, an accumulator. */
    interface AggregateImplementor2 {
        Expression implementInit(
            Aggregation aggregation,
            Type returnType,
            List<Type> parameterTypes);
        Expression implementAdd(
            Aggregation aggregation,
            Expression accumulator,
            List<Expression> arguments);
        Expression implementResult(
            Aggregation aggregation,
            Expression accumulator);
    }

    private static class BuiltinAggregateImplementor
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

    private static class CountImplementor2 implements AggregateImplementor2 {
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
            // REVIEW: Should we check whether value is NOT NULL?
            //  Or should the container do that, and only call us if
            //  the value is NOT NULL?
            return Expressions.add(
                accumulator, Expressions.constant(1, accumulator.type));
        }

        public Expression implementResult(
            Aggregation aggregation, Expression accumulator)
        {
            return accumulator;
        }
    }

    private static class SumImplementor2 implements AggregateImplementor2 {
        public Expression implementInit(
            Aggregation aggregation,
            Type returnType,
            List<Type> parameterTypes)
        {
            return Expressions.constant(0);
        }

        public Expression implementAdd(
            Aggregation aggregation,
            Expression accumulator,
            List<Expression> arguments)
        {
            assert arguments.size() == 1;
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

    private static class MinMaxImplementor2 implements AggregateImplementor2 {
        public Expression implementInit(
            Aggregation aggregation,
            Type returnType,
            List<Type> parameterTypes)
        {
            return Types.castIfNecessary(
                returnType,
                Expressions.constant(null));
        }

        public Expression implementAdd(
            Aggregation aggregation,
            Expression accumulator,
            List<Expression> arguments)
        {
            assert arguments.size() == 1;
            return Expressions.call(
                SqlFunctions.class,
                aggregation == minOperator ? "lesser" : "greater",
                accumulator,
                arguments.get(0));
        }

        public Expression implementResult(
            Aggregation aggregation, Expression accumulator)
        {
            return accumulator;
        }
    }
}

// End RexToLixTranslator.java
