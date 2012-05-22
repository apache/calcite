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

import net.hydromatic.linq4j.expressions.*;
import net.hydromatic.linq4j.function.Function1;

import junit.framework.TestCase;

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
        String s = Expressions.toString(lambdaExpr);
        assertEquals(
            "new net.hydromatic.linq4j.function.Function1() {\n"
            + "  public Integer apply(Integer arg) {\n"
            + "    return arg + 2;\n"
            + "  }\n"
            + "  public Object apply(Object arg) {\n"
            + "    return apply(\n"
            + "      (Integer) arg);\n"
            + "  }\n"
            + "}\n",
            s);

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

    public void testWrite() {
        assertEquals(
            "1 + 2 + 3",
            Expressions.toString(
                Expressions.add(
                    Expressions.add(
                        Expressions.constant(1),
                        Expressions.constant(2)),
                    Expressions.constant(3))));

        // Parentheses needed, to override the left-associativity of +.
        assertEquals(
            "1 + (2 + 3)",
            Expressions.toString(
                Expressions.add(
                    Expressions.constant(1),
                    Expressions.add(
                        Expressions.constant(2),
                        Expressions.constant(3)))));

        // No parentheses needed; higher precedence of * achieves the desired
        // effect.
        assertEquals(
            "1 + 2 * 3",
            Expressions.toString(
                Expressions.add(
                    Expressions.constant(1),
                    Expressions.multiply(
                        Expressions.constant(2),
                        Expressions.constant(3)))));

        assertEquals(
            "1 * (2 + 3)",
            Expressions.toString(
                Expressions.multiply(
                    Expressions.constant(1),
                    Expressions.add(
                        Expressions.constant(2),
                        Expressions.constant(3)))));

        // Parentheses needed, to overcome right-associativity of =.
        assertEquals(
            "(1 = 2) = 3",
            Expressions.toString(
                Expressions.assign(
                    Expressions.assign(
                        Expressions.constant(1), Expressions.constant(2)),
                    Expressions.constant(3))));

        assertEquals(
            "0 + (2 + 3).compareTo(1)",
            Expressions.toString(
                Expressions.add(
                    Expressions.constant(0),
                    Expressions.call(
                        Expressions.add(
                            Expressions.constant(2),
                            Expressions.constant(3)),
                        "compareTo",
                        Arrays.<Expression>asList(
                            Expressions.constant(1))))));

        assertEquals(
            "a.empno",
            Expressions.toString(
                Expressions.field(
                    Expressions.parameter(Linq4jTest.Employee.class, "a"),
                    "empno")));

        final ParameterExpression paramX =
            Expressions.parameter(String.class, "x");
        assertEquals(
            "new net.hydromatic.linq4j.function.Function1() {\n"
            + "  public Integer apply(String x) {\n"
            + "    return x.length();\n"
            + "  }\n"
            + "  public Object apply(Object x) {\n"
            + "    return apply(\n"
            + "      (String) x);\n"
            + "  }\n"
            + "}\n",
            Expressions.toString(
                Expressions.lambda(
                    Function1.class,
                    Expressions.return_(
                        null,
                        Expressions.call(
                            paramX,
                            "length",
                            Collections.<Expression>emptyList())),
                    Arrays.asList(paramX))));

        assertEquals(
            "new String[] {\n"
            + "  \"foo\",\n"
            + "  null,\n"
            + "  \"bar\\\"baz\"}",
            Expressions.toString(
                Expressions.newArrayInit(
                    String.class,
                    Arrays.<Expression>asList(
                        Expressions.constant("foo"),
                        Expressions.constant(null),
                        Expressions.constant("bar\"baz")))));

        assertEquals(
            "(int) ((String) (Object) \"foo\").length()",
            Expressions.toString(
                Expressions.convert_(
                    Expressions.call(
                        Expressions.convert_(
                            Expressions.convert_(
                                Expressions.constant("foo"),
                                Object.class),
                            String.class),
                        "length",
                        Collections.<Expression>emptyList()),
                    Integer.TYPE)));

        // resolving a static method
        assertEquals(
            "Integer.valueOf(\"0123\")",
            Expressions.toString(
                Expressions.call(
                    Integer.class,
                    "valueOf",
                    Collections.<Expression>singletonList(
                        Expressions.constant("0123")))));
    }

    public void testWriteConstant() {
        // primitives
        assertEquals(
            "new int[] {\n"
            + "  1,\n"
            + "  2,\n"
            + "  -1}",
            Expressions.toString(
                Expressions.constant(new int[]{1, 2, -1})));

        // objects and nulls
        assertEquals(
            "new String[] {\n"
            + "  \"foo\",\n"
            + "  null}",
            Expressions.toString(
                Expressions.constant(new String[] {"foo", null})));

        // automatically call constructor if it matches fields
        assertEquals(
            "new net.hydromatic.linq4j.test.Linq4jTest$Employee[] {\n"
            + "  new net.hydromatic.linq4j.test.Linq4jTest$Employee(\n"
            + "    100,\n"
            + "    \"Fred\",\n"
            + "    10),\n"
            + "  new net.hydromatic.linq4j.test.Linq4jTest$Employee(\n"
            + "    110,\n"
            + "    \"Bill\",\n"
            + "    30),\n"
            + "  new net.hydromatic.linq4j.test.Linq4jTest$Employee(\n"
            + "    120,\n"
            + "    \"Eric\",\n"
            + "    10),\n"
            + "  new net.hydromatic.linq4j.test.Linq4jTest$Employee(\n"
            + "    130,\n"
            + "    \"Jane\",\n"
            + "    10)}",
            Expressions.toString(
                Expressions.constant(Linq4jTest.emps)));
    }

    public void testCompile() throws NoSuchMethodException {
        // Creating a parameter for the expression tree.
        ParameterExpression param = Expressions.parameter(String.class);

        // Creating an expression for the method call and specifying its
        // parameter.
        MethodCallExpression methodCall =
            Expressions.call(
                Integer.class,
                "valueOf",
                Collections.<Expression>singletonList(param));

        // The following statement first creates an expression tree,
        // then compiles it, and then runs it.
        int x =
            Expressions.<Function1<String, Integer>>lambda(
                methodCall,
                new ParameterExpression[] { param })
                .getFunction()
                .apply("1234");
        assertEquals(1234, x);
    }
}

// End ExpressionTest.java
