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

import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.*;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.util.Util;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
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

    private static final Map<SqlOperator, ExpressionType>
        SQL_TO_LINQ_OPERATOR_MAP = Util.<SqlOperator, ExpressionType>mapOf(
            andOperator, AndAlso,
            orOperator, OrElse,
            lessThanOperator, LessThan,
            lessThanOrEqualOperator, LessThanOrEqual,
            greaterThanOperator, GreaterThan,
            greaterThanOrEqualOperator, GreaterThanOrEqual,
            equalsOperator, Equal,
            notEqualsOperator, NotEqual,
            notOperator, Not);

    public static final Map<SqlOperator, Method> SQL_OP_TO_JAVA_METHOD_MAP =
        new HashMap<SqlOperator, Method>();

    static {
        for (Map.Entry<Method, SqlOperator> entry
            : JAVA_TO_SQL_METHOD_MAP.entrySet())
        {
            SQL_OP_TO_JAVA_METHOD_MAP.put(entry.getValue(), entry.getKey());
        }
    }

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
            } else {
                return Expressions.field(input, field.getName());
            }
        }
        if (expr instanceof RexLocalRef) {
            return translate(
                program.getExprList().get(((RexLocalRef) expr).getIndex()));
        }
        if (expr instanceof RexLiteral) {
            return Expressions.constant(
                ((RexLiteral) expr).getValue(),
                typeFactory.getJavaClass(expr.getType()));
        }
        if (expr instanceof RexCall) {
            final RexCall call = (RexCall) expr;
            final SqlOperator operator = call.getOperator();
            final ExpressionType expressionType =
                SQL_TO_LINQ_OPERATOR_MAP.get(operator);
            if (expressionType != null) {
                switch (operator.getSyntax()) {
                case Binary:
                    return Expressions.makeBinary(
                        expressionType,
                        translate(call.getOperands()[0]),
                        translate(call.getOperands()[1]));
                case Postfix:
                case Prefix:
                    return Expressions.makeUnary(
                        expressionType, translate(call.getOperands()[0]));
                default:
                    throw new RuntimeException(
                        "unknown syntax " + operator.getSyntax());
                }
            }

            Method method = SQL_OP_TO_JAVA_METHOD_MAP.get(operator);
            if (method != null) {
                List<Expression> exprs =
                    translateList(Arrays.asList(call.operands));
                return !Modifier.isStatic(method.getModifiers())
                    ? Expressions.call(
                        exprs.get(0), method, exprs.subList(1, exprs.size()))
                    : Expressions.call(method, exprs);
            }

            switch (expr.getKind()) {
            default:
                throw new RuntimeException(
                    "cannot translate expression " + expr);
            }
        }
        throw new RuntimeException("cannot translate expression " + expr);
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
}

// End RexToLixTranslator.java
