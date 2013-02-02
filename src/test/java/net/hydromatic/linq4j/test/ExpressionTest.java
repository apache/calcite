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

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
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
        + "  public int apply(Integer arg) {\n"
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
                paramEnd), paramS, paramBegin, paramEnd);

    // Compile and run the lambda expression.
    String s =
        (String) lambdaExpr.compile().dynamicInvoke("hello world", 3, 7);

    assertEquals("lo w", s);
  }

  public void testWrite() {
    assertEquals(
        "1 + 2.0F + 3L + Long.valueOf(4L)",
        Expressions.toString(
            Expressions.add(
                Expressions.add(
                    Expressions.add(
                        Expressions.constant(1),
                        Expressions.constant(2F, Float.TYPE)),
                    Expressions.constant(3L, Long.TYPE)),
                Expressions.constant(4L, Long.class))));

    assertEquals(
        "new java.math.BigDecimal(31415926L, 7)",
        Expressions.toString(
            Expressions.constant(
                BigDecimal.valueOf(314159260, 8))));

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

    // Ternary operator.
    assertEquals(
        "1 < 2 ? (3 < 4 ? 5 : 6) : 7 < 8 ? 9 : 10",
        Expressions.toString(
            Expressions.condition(
                Expressions.lessThan(
                    Expressions.constant(1),
                    Expressions.constant(2)),
                Expressions.condition(
                    Expressions.lessThan(
                        Expressions.constant(3),
                        Expressions.constant(4)),
                    Expressions.constant(5),
                    Expressions.constant(6)),
                Expressions.condition(
                    Expressions.lessThan(
                        Expressions.constant(7),
                        Expressions.constant(8)),
                    Expressions.constant(9),
                    Expressions.constant(10)))));

    assertEquals(
        "0 + (double) (2 + 3)",
        Expressions.toString(
            Expressions.add(
                Expressions.constant(0),
                Expressions.convert_(
                    Expressions.add(
                        Expressions.constant(2), Expressions.constant(3)),
                    Double.TYPE))));

    assertEquals(
        "a.empno",
        Expressions.toString(
            Expressions.field(
                Expressions.parameter(Linq4jTest.Employee.class, "a"),
                "empno")));

    assertEquals(
        "java.util.Collections.EMPTY_LIST",
        Expressions.toString(
            Expressions.field(
                null, Collections.class, "EMPTY_LIST")));

    final ParameterExpression paramX =
        Expressions.parameter(String.class, "x");
    assertEquals(
        "new net.hydromatic.linq4j.function.Function1() {\n"
        + "  public int apply(String x) {\n"
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
                Expressions.call(
                    paramX, "length", Collections.<Expression>emptyList()),
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

    // precedence of not and instanceof
    assertEquals(
        "!(o instanceof String)",
        Expressions.toString(
            Expressions.not(
                Expressions.typeIs(
                    Expressions.parameter(Object.class, "o"),
                    String.class))));

    // not not
    assertEquals(
        "!!(o instanceof String)",
        Expressions.toString(
            Expressions.not(
                Expressions.not(
                    Expressions.typeIs(
                        Expressions.parameter(Object.class, "o"),
                        String.class)))));
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
        "new net.hydromatic.linq4j.test.Linq4jTest.Employee[] {\n"
        + "  new net.hydromatic.linq4j.test.Linq4jTest.Employee(\n"
        + "    100,\n"
        + "    \"Fred\",\n"
        + "    10),\n"
        + "  new net.hydromatic.linq4j.test.Linq4jTest.Employee(\n"
        + "    110,\n"
        + "    \"Bill\",\n"
        + "    30),\n"
        + "  new net.hydromatic.linq4j.test.Linq4jTest.Employee(\n"
        + "    120,\n"
        + "    \"Eric\",\n"
        + "    10),\n"
        + "  new net.hydromatic.linq4j.test.Linq4jTest.Employee(\n"
        + "    130,\n"
        + "    \"Janet\",\n"
        + "    10)}",
        Expressions.toString(
            Expressions.constant(Linq4jTest.emps)));
  }

  public void testWriteArray() {
    assertEquals(
        "1 + integers[2 + index]",
        Expressions.toString(
            Expressions.add(
                Expressions.constant(1),
                Expressions.arrayIndex(
                    Expressions.variable(int[].class, "integers"),
                    Expressions.add(
                        Expressions.constant(2),
                        Expressions.variable(int.class, "index"))))));
  }

  public void testWriteAnonymousClass() {
    // final List<String> baz = Arrays.asList("foo", "bar");
    // new AbstractList<String>() {
    //     public int size() {
    //         return baz.size();
    //     }
    //     public String get(int index) {
    //         return ((String) baz.get(index)).toUpperCase();
    //     }
    // }
    final ParameterExpression bazParameter =
        Expressions.parameter(
            Types.of(List.class, String.class),
            "baz");
    final ParameterExpression indexParameter =
        Expressions.parameter(
            Integer.TYPE,
            "index");
    BlockExpression e =
        Expressions.block(
            Expressions.declare(
                Modifier.FINAL,
                bazParameter,
                Expressions.call(
                    Arrays.class,
                    "asList",
                    Arrays.<Expression>asList(
                        Expressions.constant("foo"),
                        Expressions.constant("bar")))),
            Expressions.statement(
                Expressions.new_(
                    Types.of(AbstractList.class, String.class),
                    Collections.<Expression>emptyList(),
                    Arrays.<MemberDeclaration>asList(
                        Expressions.fieldDecl(
                            Modifier.PUBLIC | Modifier.FINAL,
                            Expressions.parameter(
                                String.class,
                                "qux"),
                            Expressions.constant("xyzzy")),
                        Expressions.methodDecl(
                            Modifier.PUBLIC,
                            Integer.TYPE,
                            "size",
                            Collections.<ParameterExpression>emptyList(),
                            Blocks.toFunctionBlock(
                                Expressions.call(
                                    bazParameter,
                                    "size",
                                    Collections.<Expression>emptyList()))),
                        Expressions.methodDecl(
                            Modifier.PUBLIC,
                            String.class,
                            "get",
                            Arrays.asList(indexParameter),
                            Blocks.toFunctionBlock(
                                Expressions.call(
                                    Expressions.convert_(
                                        Expressions.call(
                                            bazParameter,
                                            "get",
                                            Arrays.<Expression>asList(
                                                indexParameter)),
                                        String.class),
                                    "toUpperCase",
                                    Collections
                                        .<Expression>emptyList())))))));
    assertEquals(
        "{\n"
        + "  final java.util.List<String> baz = java.util.Arrays.asList(\"foo\", \"bar\");\n"
        + "  new java.util.AbstractList<String>(){\n"
        + "    public final String qux = \"xyzzy\";\n"
        + "    public int size() {\n"
        + "      return baz.size();\n"
        + "    }\n"
        + "\n"
        + "    public String get(int index) {\n"
        + "      return ((String) baz.get(index)).toUpperCase();\n"
        + "    }\n"
        + "\n"
        + "  };\n"
        + "}\n",
        Expressions.toString(e));
  }

  public void testWriteWhile() {
    DeclarationExpression xDecl, yDecl;
    Node node =
        Expressions.block(
            xDecl = Expressions.declare(
                0,
                "x",
                Expressions.constant(10)),
            yDecl = Expressions.declare(
                0,
                "y",
                Expressions.constant(0)),
            Expressions.while_(
                Expressions.lessThan(
                    xDecl.parameter,
                    Expressions.constant(5)),
                Expressions.statement(
                    Expressions.preIncrementAssign(yDecl.parameter))));
    assertEquals(
        "{\n"
        + "  int x = 10;\n"
        + "  int y = 0;\n"
        + "  while (x < 5) {\n"
        + "    ++y;\n"
        + "  }\n"
        + "}\n",
        Expressions.toString(node));
  }

  public void testType() {
    // Type of ternary operator is the gcd of its arguments.
    assertEquals(
        long.class,
        Expressions.condition(
            Expressions.constant(true),
            Expressions.constant(5),
            Expressions.constant(6L)).getType());
    assertEquals(
        long.class,
        Expressions.condition(
            Expressions.constant(true),
            Expressions.constant(5L),
            Expressions.constant(6)).getType());

    // If one of the arguments is null constant, it is implicitly coerced.
    assertEquals(
        String.class,
        Expressions.condition(
            Expressions.constant(true),
            Expressions.constant("xxx"),
            Expressions.constant(null)).getType());
    assertEquals(
        Integer.class,
        Expressions.condition(
            Expressions.constant(true),
            Expressions.constant(Integer.valueOf(0)),
            Expressions.constant(null)).getType());
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

  public void testBlockBuilder() {
    checkBlockBuilder(
        false,
        "{\n"
        + "  final int three = 1 + 2;\n"
        + "  final int six = three * 2;\n"
        + "  final int nine = three * three;\n"
        + "  final int eighteen = three + six + nine;\n"
        + "  return eighteen;\n"
        + "}\n");
    checkBlockBuilder(
        true,
        "{\n"
        + "  final int three = 1 + 2;\n"
        + "  return three + three * 2 + three * three;\n"
        + "}\n");
  }

  public void checkBlockBuilder(boolean optimizing, String expected) {
    BlockBuilder statements = new BlockBuilder(optimizing);
    Expression one =
        statements.append(
            "one", Expressions.constant(1));
    Expression two =
        statements.append(
            "two", Expressions.constant(2));
    Expression three =
        statements.append(
            "three", Expressions.add(one, two));
    Expression six =
        statements.append(
            "six",
            Expressions.multiply(three, two));
    Expression nine =
        statements.append(
            "nine",
            Expressions.multiply(three, three));
    Expression eighteen =
        statements.append(
            "eighteen",
            Expressions.add(
                Expressions.add(three, six),
                nine));
    statements.add(Expressions.return_(null, eighteen));
    assertEquals(expected, Expressions.toString(statements.toBlock()));
  }

  public void testBlockBuilder2() {
    BlockBuilder statements = new BlockBuilder();
    Expression element =
        statements.append(
            "element", Expressions.constant(null));
    Expression comparator =
        statements.append(
            "comparator", Expressions.constant(null, Comparator.class));
    Expression treeSet =
        statements.append(
            "treeSet",
            Expressions.new_(
                TreeSet.class,
                Arrays.asList(comparator)));
    statements.add(
        Expressions.return_(
            null,
            Expressions.call(
                treeSet,
                "add",
                element)));
    assertEquals(
        "{\n"
        + "  final java.util.Comparator comparator = null;\n"
        + "  return new java.util.TreeSet(\n"
        + "      comparator).add(null);\n"
        + "}\n",
        Expressions.toString(statements.toBlock()));
  }

  public void testClassDecl() {
    final NewExpression newExpression =
        Expressions.new_(
            Object.class,
            Collections.<Expression>emptyList(),
            Arrays.<MemberDeclaration>asList(
                new FieldDeclaration(
                    Modifier.PUBLIC | Modifier.FINAL,
                    Expressions.parameter(String.class, "foo"),
                    Expressions.constant("bar")),
                new ClassDeclaration(
                    Modifier.PUBLIC | Modifier.STATIC,
                    "MyClass",
                    null,
                    Collections.<Type>emptyList(),
                    Arrays.<MemberDeclaration>asList(
                        new FieldDeclaration(
                            0,
                            Expressions.parameter(int.class, "x"),
                            Expressions.constant(0)))),
                new FieldDeclaration(
                    0,
                    Expressions.parameter(int.class, "i"),
                    null)));
    assertEquals(
        "new Object(){\n"
        + "  public final String foo = \"bar\";\n"
        + "  public static class MyClass {\n"
        + "    int x = 0;\n"
        + "  }\n"
        + "  int i;\n"
        + "}",
        Expressions.toString(newExpression));
  }

  public void testReturn() {
    assertEquals(
        "if (true) {\n"
        + "  return;\n"
        + "}\n",
        Expressions.toString(
            Expressions.ifThen(
                Expressions.constant(true),
                Expressions.return_(null))));
  }
}

// End ExpressionTest.java
