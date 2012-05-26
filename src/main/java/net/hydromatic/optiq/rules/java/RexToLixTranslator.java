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
import org.eigenbase.rex.*;

import java.lang.reflect.Modifier;
import java.util.*;

/**
 * Translates {@link org.eigenbase.rex.RexNode REX expressions} to
 * {@link net.hydromatic.linq4j.expressions.Expression linq4j expressions}.
 *
 * @author jhyde
 */
public class RexToLixTranslator {
    private final Map<RexNode, Slot> map = new HashMap<RexNode, Slot>();
    private final RexProgram program;
    private final List<ParameterExpression> inputs;
    private List<Expression> list;

    private RexToLixTranslator(
        RexProgram program, List<ParameterExpression> inputs)
    {
        this.program = program;
        this.inputs = inputs;
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
        List<ParameterExpression> inputs,
        RexProgram program,
        List<Expression> list)
    {
        return new RexToLixTranslator(program, inputs)
            .translate(list, program.getProjectList());
    }

    private ParameterExpression define(RexNode rexExpr) {
        Slot pair = map.get(rexExpr);
        if (pair != null) {
            pair.count++;
            return pair.parameterExpression;
        }
        final Expression expression = translate(rexExpr);
        String variableName = "v" + map.size();
        final ParameterExpression parameter =
            Expressions.parameter(
                expression.getType(),
                variableName);
        pair = new Slot(parameter, expression);
        pair.count = 1;
        map.put(rexExpr, pair);
        return parameter;
    }

    private Expression translate0(RexNode expr) {
        if (expr instanceof RexInputRef) {
            return inputs.get(((RexInputRef) expr).getIndex());
        }
        if (expr instanceof RexLocalRef) {
            return translate(
                program.getExprList().get(((RexLocalRef) expr).getIndex()));
        }
        if (expr instanceof RexLiteral) {
            return Expressions.constant(((RexLiteral) expr).getValue());
        }
        if (expr instanceof RexCall) {
            RexCall call = (RexCall) expr;
            switch (expr.getKind()) {
            case And:
                return Expressions.andAlso(
                    translate(call.getOperands()[0]),
                    translate(call.getOperands()[1]));
            case LessThan:
                return Expressions.lessThan(
                    translate(call.getOperands()[0]),
                    translate(call.getOperands()[1]));
            case GreaterThan:
                return Expressions.greaterThan(
                    translate(call.getOperands()[0]),
                    translate(call.getOperands()[1]));
            default:
                throw new RuntimeException("cannot translate expression " + expr);
            }
        }
        throw new RuntimeException("cannot translate expression " + expr);
    }

    private Expression translate(RexNode expr) {
        Slot slot = map.get(expr);
        if (slot != null) {
            if (list == null) {
                ++slot.count;
                return slot.parameterExpression;
            } else {
                if (slot.count > 1) {
                    list.add(
                        Expressions.declare(
                            Modifier.FINAL,
                            slot.parameterExpression,
                            slot.expression));
                    slot.count = 0; // prevent further declaration
                    return slot.parameterExpression;
                }
                if (slot.count == 1) {
                    return slot.expression;
                } else {
                    return slot.parameterExpression;
                }
            }
        } else {
            Expression expression = translate0(expr);
            slot = new Slot(
                Expressions.parameter(
                    expression.getType(),
                    "v" + map.size()),
                expression);
            slot.count++;
            map.put(expr, slot);
            return slot.parameterExpression;
        }
    }

    private List<Expression> translate(
        List<Expression> list,
        List<RexLocalRef> rexList)
    {
        // First pass. Count how many times each sub-expression is used.
        this.list = null;
        for (RexNode rexExpr : rexList) {
            define(rexExpr);
        }
        // Second pass. When translating each expression, if it is used more
        // than once, the first time it is encountered, add a declaration to the
        // list and set its usage count to 0.
        this.list = list;
        List<Expression> translateds = new ArrayList<Expression>();
        for (RexNode rexExpr : rexList) {
            translateds.add(define(rexExpr));
        }
        return translateds;
    }

    public static Expression translateCondition(
        List<ParameterExpression> inputs,
        RexProgram program,
        List<Expression> list)
    {
        List<Expression> x = new RexToLixTranslator(program, inputs)
            .translate(list, Collections.singletonList(program.getCondition()));
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
