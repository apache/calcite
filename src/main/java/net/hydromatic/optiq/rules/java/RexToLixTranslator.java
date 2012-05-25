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
import org.eigenbase.util.Pair;

import java.util.*;

/**
 * Translates {@link org.eigenbase.rex.RexNode REX expressions} to
 * {@link net.hydromatic.linq4j.expressions.Expression linq4j expressions}.
 *
 * @author jhyde
 */
public class RexToLixTranslator {
    private final Map<RexNode, Pair<ParameterExpression, Expression>> map =
        new HashMap<RexNode, Pair<ParameterExpression, Expression>>();
    private final Map<Integer, Pair<ParameterExpression, Expression>> localMap =
        new HashMap<Integer, Pair<ParameterExpression, Expression>>();
    private final List<ParameterExpression> inputs;

    private RexToLixTranslator(List<ParameterExpression> inputs) {
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
    public static Pair<Expression, List<Expression>> translateProgram(
        List<ParameterExpression> inputs,
        RexProgram program)
    {
        return new RexToLixTranslator(inputs).translateProgram(program);
    }

    private ParameterExpression define(RexNode rexExpr, int ordinal) {
        Pair<ParameterExpression, Expression> pair = map.get(rexExpr);
        if (pair != null) {
            return pair.left;
        }
        final Expression expression = translate(rexExpr);
        String variableName = "v" + map.size();
        final ParameterExpression parameter =
            Expressions.parameter(
                expression.getType(),
                variableName);
        pair = Pair.of(parameter, expression);
        map.put(rexExpr, pair);
        if (ordinal >= 0) {
            localMap.put(ordinal, pair);
        }
        return parameter;
    }

    private Expression translate(RexNode expr) {
        if (expr instanceof RexInputRef) {
            return inputs.get(((RexInputRef) expr).getIndex());
        }
        if (expr instanceof RexLocalRef) {
            // we require that expression has previously been registered
            return localMap.get(((RexLocalRef) expr).getIndex()).left;
        }
        if (expr instanceof RexLiteral) {
            return Expressions.constant(((RexLiteral) expr).getValue());
        }
        throw new RuntimeException("cannot translate expression " + expr);
    }

    private Pair<Expression, List<Expression>> translateProgram(
        RexProgram program)
    {
        int i = 0;
        for (RexNode rexExpr : program.getExprList()) {
            define(rexExpr, i++);
        }
        final List<RexLocalRef> projectList = program.getProjectList();
        return Pair.<Expression, List<Expression>>of(
            program.getCondition() == null
                ? null
                : translate(program.getCondition()),
            new AbstractList<Expression>() {
                public Expression get(int index) {
                    return translate(projectList.get(index));
                }

                public int size() {
                    return projectList.size();
                }
            });
    }
}

// End RexToLixTranslator.java
