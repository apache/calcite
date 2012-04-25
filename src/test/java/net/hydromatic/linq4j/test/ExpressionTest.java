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
package net.hydromatic.linq4j.test;

import junit.framework.TestCase;

import net.hydromatic.linq4j.expressions.*;

import java.util.*;

/**
 * Unit test for {@link net.hydromatic.linq4j.expressions.Expression}
 * and subclasses.
 */
public class ExpressionTest extends TestCase {
    public void testLambdaCallsBinaryOp() {
        // A parameter for the lambda expression.
        ParameterExpression paramExpr =
            Expressions.parameter(Integer.TYPE, "arg");

        // This expression represents a lambda expression
        // that adds 1 to the parameter value.
        FunctionExpression lambdaExpr = Expressions.lambda(
            Expressions.add(
                paramExpr,
                Expressions.constant(2)),
            Arrays.asList(paramExpr));

        // Print out the expression.
        String s = lambdaExpr.toString();

        // Compile and run the lambda expression.
        // The value of the parameter is 1.
        int n = (Integer) lambdaExpr.compile().dynamicInvoke(1);

        // This code example produces the following output:
        //
        // arg => (arg +2)
        // 3
        assertEquals(3, n);
    }

    public void testLambdaCallsTwoArgMethod() throws NoSuchMethodException {
        // A parameter for the lambda expression.
        ParameterExpression paramS =
            Expressions.parameter(String.class, "s");
        ParameterExpression paramBegin =
            Expressions.parameter(Integer.TYPE, "begin");
        ParameterExpression paramEnd =
            Expressions.parameter(Integer.TYPE, "end");

        // This expression represents a lambda expression
        // that adds 1 to the parameter value.
        FunctionExpression lambdaExpr =
            Expressions.lambda(
                Expressions.call(
                    paramS,
                    String.class.getMethod(
                        "substring", Integer.TYPE, Integer.TYPE),
                    paramBegin,
                    paramEnd),
                paramS, paramBegin, paramEnd);

        // Compile and run the lambda expression.
        String s =
            (String) lambdaExpr.compile().dynamicInvoke("hello world", 3, 7);

        assertEquals("lo w", s);
    }
}

// End ExpressionTest.java
