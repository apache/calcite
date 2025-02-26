/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.linq4j.test;

import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.linq4j.tree.DeclarationStatement;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.FieldDeclaration;
import org.apache.calcite.linq4j.tree.FunctionExpression;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.NewExpression;
import org.apache.calcite.linq4j.tree.Node;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Shuttle;
import org.apache.calcite.linq4j.tree.Types;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.calcite.linq4j.test.BlockBuilderBase.ONE;
import static org.apache.calcite.linq4j.test.BlockBuilderBase.TWO;
import static org.apache.calcite.linq4j.test.util.RecordHelper.createInstance;
import static org.apache.calcite.linq4j.test.util.RecordHelper.createRecordClass;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;

/**
 * Unit test for {@link org.apache.calcite.linq4j.tree.Expression}
 * and subclasses.
 */
public class ExpressionTest {

  @Test void testLambdaCallsBinaryOpInt() {
    // A parameter for the lambda expression.
    ParameterExpression paramExpr =
        Expressions.parameter(Integer.TYPE, "arg");

    // This expression represents a lambda expression
    // that adds 1 to the parameter value.
    FunctionExpression lambdaExpr =
        Expressions.lambda(
            Expressions.add(paramExpr, Expressions.constant(2)),
            Arrays.asList(paramExpr));

    // Print out the expression.
    String s = Expressions.toString(lambdaExpr);
    assertThat(s,
        is("new org.apache.calcite.linq4j.function.Function1() {\n"
            + "  public int apply(int arg) {\n"
            + "    return arg + 2;\n"
            + "  }\n"
            + "  public Object apply(Integer arg) {\n"
            + "    return apply(\n"
            + "      arg.intValue());\n"
            + "  }\n"
            + "  public Object apply(Object arg) {\n"
            + "    return apply(\n"
            + "      (Integer) arg);\n"
            + "  }\n"
            + "}\n"));

    // Compile and run the lambda expression.
    // The value of the parameter is 1
    Integer n = (Integer) lambdaExpr.compile().dynamicInvoke(1);

    // This code example produces the following output:
    //
    // arg => (arg +2)
    // 3
    assertThat(n, notNullValue());
    assertThat(n, is(3));
  }

  @Test void testLambdaCallsBinaryOpShort() {
    // A parameter for the lambda expression.
    ParameterExpression paramExpr =
        Expressions.parameter(Short.TYPE, "arg");

    // This expression represents a lambda expression
    // that adds 1 to the parameter value.
    Short a = 2;
    FunctionExpression lambdaExpr =
        Expressions.lambda(
            Expressions.add(paramExpr, Expressions.constant(a)),
            Arrays.asList(paramExpr));

    // Print out the expression.
    String s = Expressions.toString(lambdaExpr);
    assertThat(s,
        is("new org.apache.calcite.linq4j.function.Function1() {\n"
            + "  public int apply(short arg) {\n"
            + "    return arg + (short)2;\n"
            + "  }\n"
            + "  public Object apply(Short arg) {\n"
            + "    return apply(\n"
            + "      arg.shortValue());\n"
            + "  }\n"
            + "  public Object apply(Object arg) {\n"
            + "    return apply(\n"
            + "      (Short) arg);\n"
            + "  }\n"
            + "}\n"));

    // Compile and run the lambda expression.
    // The value of the parameter is 1.
    Short b = 1;
    Integer n = (Integer) lambdaExpr.compile().dynamicInvoke(b);

    // This code example produces the following output:
    //
    // arg => (arg +2)
    // 3
    assertThat(n, notNullValue());
    assertThat(n, is(3));
  }

  @Test void testLambdaCallsBinaryOpByte() {
    // A parameter for the lambda expression.
    ParameterExpression paramExpr =
        Expressions.parameter(Byte.TYPE, "arg");

    // This expression represents a lambda expression
    // that adds 1 to the parameter value.
    FunctionExpression lambdaExpr =
        Expressions.lambda(
            Expressions.add(paramExpr, Expressions.constant(Byte.valueOf("2"))),
            Arrays.asList(paramExpr));

    // Print out the expression.
    String s = Expressions.toString(lambdaExpr);
    assertThat(s,
        is("new org.apache.calcite.linq4j.function.Function1() {\n"
            + "  public int apply(byte arg) {\n"
            + "    return arg + (byte)2;\n"
            + "  }\n"
            + "  public Object apply(Byte arg) {\n"
            + "    return apply(\n"
            + "      arg.byteValue());\n"
            + "  }\n"
            + "  public Object apply(Object arg) {\n"
            + "    return apply(\n"
            + "      (Byte) arg);\n"
            + "  }\n"
            + "}\n"));

    // Compile and run the lambda expression.
    // The value of the parameter is 1.
    Integer n = (Integer) lambdaExpr.compile().dynamicInvoke(Byte.valueOf("1"));

    // This code example produces the following output:
    //
    // arg => (arg +2)
    // 3
    assertThat(n, notNullValue());
    assertThat(n, is(3));
  }

  @Test void testLambdaCallsBinaryOpDouble() {
    // A parameter for the lambda expression.
    ParameterExpression paramExpr =
        Expressions.parameter(Double.TYPE, "arg");

    // This expression represents a lambda expression
    // that adds 1 to the parameter value.
    FunctionExpression lambdaExpr =
        Expressions.lambda(
            Expressions.add(paramExpr, Expressions.constant(2d)),
            Arrays.asList(paramExpr));

    // Print out the expression.
    String s = Expressions.toString(lambdaExpr);
    assertThat(s,
        is("new org.apache.calcite.linq4j.function.Function1() {\n"
            + "  public double apply(double arg) {\n"
            + "    return arg + 2.0D;\n"
            + "  }\n"
            + "  public Object apply(Double arg) {\n"
            + "    return apply(\n"
            + "      arg.doubleValue());\n"
            + "  }\n"
            + "  public Object apply(Object arg) {\n"
            + "    return apply(\n"
            + "      (Double) arg);\n"
            + "  }\n"
            + "}\n"));

    // Compile and run the lambda expression.
    // The value of the parameter is 1.5.
    Double n = (Double) lambdaExpr.compile().dynamicInvoke(1.5d);

    // This code example produces the following output:
    //
    // arg => (arg +2)
    // 3.5
    assertThat(n, notNullValue());
    assertThat(n, is(3.5D));
  }

  @Test void testLambdaCallsBinaryOpLong() {
    // A parameter for the lambda expression.
    ParameterExpression paramExpr =
        Expressions.parameter(Long.TYPE, "arg");

    // This expression represents a lambda expression
    // that adds 1L to the parameter value.
    FunctionExpression lambdaExpr =
        Expressions.lambda(
            Expressions.add(paramExpr, Expressions.constant(2L)),
            Arrays.asList(paramExpr));
    // Print out the expression.
    String s = Expressions.toString(lambdaExpr);
    assertThat(s,
        is("new org.apache.calcite.linq4j.function.Function1() {\n"
            + "  public long apply(long arg) {\n"
            + "    return arg + 2L;\n"
            + "  }\n"
            + "  public Object apply(Long arg) {\n"
            + "    return apply(\n"
            + "      arg.longValue());\n"
            + "  }\n"
            + "  public Object apply(Object arg) {\n"
            + "    return apply(\n"
            + "      (Long) arg);\n"
            + "  }\n"
            + "}\n"));

    // Compile and run the lambda expression.
    // The value of the parameter is 1L.
    Long n = (Long) lambdaExpr.compile().dynamicInvoke(1L);

    // This code example produces the following output:
    //
    // arg => (arg +2)
    // 3
    assertThat(n, notNullValue());
    assertThat(n, is(3L));
  }

  @Test void testLambdaCallsBinaryOpFloat() {
    // A parameter for the lambda expression.
    ParameterExpression paramExpr =
        Expressions.parameter(Float.TYPE, "arg");

    // This expression represents a lambda expression
    // that adds 1f to the parameter value.
    FunctionExpression lambdaExpr =
        Expressions.lambda(
            Expressions.add(paramExpr, Expressions.constant(2.0f)),
            Arrays.asList(paramExpr));
    // Print out the expression.
    String s = Expressions.toString(lambdaExpr);
    assertThat(s,
        is("new org.apache.calcite.linq4j.function.Function1() {\n"
            + "  public float apply(float arg) {\n"
            + "    return arg + 2.0F;\n"
            + "  }\n"
            + "  public Object apply(Float arg) {\n"
            + "    return apply(\n"
            + "      arg.floatValue());\n"
            + "  }\n"
            + "  public Object apply(Object arg) {\n"
            + "    return apply(\n"
            + "      (Float) arg);\n"
            + "  }\n"
            + "}\n"));

    // Compile and run the lambda expression.
    // The value of the parameter is 1f
    Float n = (Float) lambdaExpr.compile().dynamicInvoke(1f);

    // This code example produces the following output:
    //
    // arg => (arg +2)
    // 3.0
    assertThat(n, notNullValue());
    assertThat(n, is(3f));
  }

  @Test void testLambdaCallsBinaryOpMixType() {
    // A parameter for the lambda expression.
    ParameterExpression paramExpr =
        Expressions.parameter(Long.TYPE, "arg");

    // This expression represents a lambda expression
    // that adds (int)10 to the parameter value.
    FunctionExpression lambdaExpr =
        Expressions.lambda(
            Expressions.add(paramExpr, Expressions.constant(10)),
            Arrays.asList(paramExpr));
    // Print out the expression.
    String s = Expressions.toString(lambdaExpr);
    assertThat(s,
        is("new org.apache.calcite.linq4j.function.Function1() {\n"
            + "  public long apply(long arg) {\n"
            + "    return arg + 10;\n"
            + "  }\n"
            + "  public Object apply(Long arg) {\n"
            + "    return apply(\n"
            + "      arg.longValue());\n"
            + "  }\n"
            + "  public Object apply(Object arg) {\n"
            + "    return apply(\n"
            + "      (Long) arg);\n"
            + "  }\n"
            + "}\n"));

    // Compile and run the lambda expression.
    // The value of the parameter is 5L.
    Long n = (Long) lambdaExpr.compile().dynamicInvoke(5L);

    // This code example produces the following output:
    //
    // arg => (arg +10)
    // 15
    assertThat(n, notNullValue());
    assertThat(n, is(15L));
  }

  @Test void testLambdaCallsBinaryOpMixDoubleType() {
    // A parameter for the lambda expression.
    ParameterExpression paramExpr =
        Expressions.parameter(Double.TYPE, "arg");

    // This expression represents a lambda expression
    // that adds 10.1d to the parameter value.
    FunctionExpression lambdaExpr =
        Expressions.lambda(
            Expressions.add(paramExpr, Expressions.constant(10.1d)),
            Arrays.asList(paramExpr));
    // Print out the expression.
    String s = Expressions.toString(lambdaExpr);
    assertThat(s,
        is("new org.apache.calcite.linq4j.function.Function1() {\n"
            + "  public double apply(double arg) {\n"
            + "    return arg + 10.1D;\n"
            + "  }\n"
            + "  public Object apply(Double arg) {\n"
            + "    return apply(\n"
            + "      arg.doubleValue());\n"
            + "  }\n"
            + "  public Object apply(Object arg) {\n"
            + "    return apply(\n"
            + "      (Double) arg);\n"
            + "  }\n"
            + "}\n"));

    // Compile and run the lambda expression.
    // The value of the parameter is 5.0f.
    Double n = (Double) lambdaExpr.compile().dynamicInvoke(5.0f);

    // This code example produces the following output:
    //
    // arg => (arg +10.1d)
    // 15.1d
    assertThat(n, notNullValue());
    assertThat(n, is(15.1d));
  }

  @Test void testLambdaPrimitiveTwoArgs() {
    // Parameters for the lambda expression.
    ParameterExpression paramExpr =
        Expressions.parameter(int.class, "key");
    ParameterExpression param2Expr =
        Expressions.parameter(int.class, "key2");

    FunctionExpression lambdaExpr =
        Expressions.lambda(
            Expressions.block((Type) null,
                Expressions.return_(null, paramExpr)),
            Arrays.asList(paramExpr, param2Expr));

    // Print out the expression.
    String s = Expressions.toString(lambdaExpr);
    assertThat(s,
        is("new org.apache.calcite.linq4j.function.Function2() {\n"
            + "  public int apply(int key, int key2) {\n"
            + "    return key;\n"
            + "  }\n"
            + "  public Integer apply(Integer key, Integer key2) {\n"
            + "    return apply(\n"
            + "      key.intValue(),\n"
            + "      key2.intValue());\n"
            + "  }\n"
            + "  public Integer apply(Object key, Object key2) {\n"
            + "    return apply(\n"
            + "      (Integer) key,\n"
            + "      (Integer) key2);\n"
            + "  }\n"
            + "}\n"));
  }

  @Test void testLambdaCallsTwoArgMethod() throws NoSuchMethodException {
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

    assertThat(s, is("lo w"));
  }

  @Test void testFoldAnd() {
    // empty list yields true
    final List<Expression> list0 = Collections.emptyList();
    assertThat(
        Expressions.toString(
            Expressions.foldAnd(list0)),
        is("true"));
    assertThat(
        Expressions.toString(
            Expressions.foldOr(list0)),
        is("false"));

    final List<Expression> list1 =
        Arrays.asList(
            Expressions.equal(Expressions.constant(1), Expressions.constant(2)),
            Expressions.equal(Expressions.constant(3), Expressions.constant(4)),
            Expressions.constant(true),
            Expressions.equal(Expressions.constant(5),
                Expressions.constant(6)));
    // true is eliminated from AND
    assertThat(
        Expressions.toString(
            Expressions.foldAnd(list1)),
        is("1 == 2 && 3 == 4 && 5 == 6"));
    // a single true makes OR true
    assertThat(
        Expressions.toString(
            Expressions.foldOr(list1)),
        is("true"));

    final List<Expression> list2 =
        Collections.singletonList(
            Expressions.constant(true));
    assertThat(
        Expressions.toString(
            Expressions.foldAnd(list2)),
        is("true"));
    assertThat(
        Expressions.toString(
            Expressions.foldOr(list2)),
        is("true"));

    final List<Expression> list3 =
        Arrays.asList(
            Expressions.equal(Expressions.constant(1), Expressions.constant(2)),
            Expressions.constant(false),
            Expressions.equal(Expressions.constant(5),
                Expressions.constant(6)));
    // false causes whole list to be false
    assertThat(
        Expressions.toString(
            Expressions.foldAnd(list3)),
        is("false"));
    assertThat(
        Expressions.toString(
            Expressions.foldOr(list3)),
        is("1 == 2 || 5 == 6"));
  }

  @Test void testWrite() {
    assertThat(
        Expressions.toString(
            Expressions.add(
                Expressions.add(
                    Expressions.add(
                        Expressions.constant(1),
                        Expressions.constant(2F, Float.TYPE)),
                    Expressions.constant(3L, Long.TYPE)),
                Expressions.constant(4L, Long.class))),
        is("1 + 2.0F + 3L + Long.valueOf(4L)"));

    assertThat(
        Expressions.toString(
            Expressions.constant(
                BigDecimal.valueOf(314159260, 8))),
        is("java.math.BigDecimal.valueOf(31415926L, 7)"));

    // Parentheses needed, to override the left-associativity of +.
    assertThat(
        Expressions.toString(
            Expressions.add(
                Expressions.constant(1),
                Expressions.add(
                    Expressions.constant(2),
                    Expressions.constant(3)))),
        is("1 + (2 + 3)"));

    // No parentheses needed; higher precedence of * achieves the desired
    // effect.
    assertThat(
        Expressions.toString(
            Expressions.add(
                Expressions.constant(1),
                Expressions.multiply(
                    Expressions.constant(2),
                    Expressions.constant(3)))),
        is("1 + 2 * 3"));

    assertThat(
        Expressions.toString(
            Expressions.multiply(
                Expressions.constant(1),
                Expressions.add(
                    Expressions.constant(2),
                    Expressions.constant(3)))),
        is("1 * (2 + 3)"));

    // Parentheses needed, to overcome right-associativity of =.
    assertThat(
        Expressions.toString(
            Expressions.assign(
                Expressions.assign(
                    Expressions.constant(1), Expressions.constant(2)),
                Expressions.constant(3))),
        is("(1 = 2) = 3"));

    // Ternary operator.
    assertThat(
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
                    Expressions.constant(10)))),
        is("1 < 2 ? (3 < 4 ? 5 : 6) : 7 < 8 ? 9 : 10"));

    assertThat(
        Expressions.toString(
            Expressions.add(
                Expressions.constant(0),
                Expressions.convert_(
                    Expressions.add(
                        Expressions.constant(2), Expressions.constant(3)),
                    Double.TYPE))),
        is("0 + (double) (2 + 3)"));

    // "--5" would be a syntax error
    assertThat(
        Expressions.toString(
            Expressions.negate(
                Expressions.negate(
                    Expressions.constant(5)))),
        is("(- (- 5))"));

    assertThat(
        Expressions.toString(
            Expressions.field(
                Expressions.parameter(Linq4jTest.Employee.class, "a"),
                "empno")),
        is("a.empno"));

    assertThat(
        Expressions.toString(
            Expressions.field(
                Expressions.parameter(Object[].class, "a"),
                "length")),
        is("a.length"));

    assertThat(
        Expressions.toString(
            Expressions.field(
                null, Collections.class, "EMPTY_LIST")),
        is("java.util.Collections.EMPTY_LIST"));

    final ParameterExpression paramX =
        Expressions.parameter(String.class, "x");
    assertThat(
        Expressions.toString(
            Expressions.lambda(
                Function1.class,
                Expressions.call(
                    paramX, "length", Collections.emptyList()),
                Arrays.asList(paramX))),
        is("new org.apache.calcite.linq4j.function.Function1() {\n"
            + "  public int apply(String x) {\n"
            + "    return x.length();\n"
            + "  }\n"
            + "  public Object apply(Object x) {\n"
            + "    return apply(\n"
            + "      (String) x);\n"
            + "  }\n"
            + "}\n"));

    // 1-dimensional array with initializer
    assertThat(
        Expressions.toString(
            Expressions.newArrayInit(
                String.class,
                Expressions.constant("foo"),
                Expressions.constant(null),
                Expressions.constant("bar\"baz"))),
        is("new String[] {\n"
            + "  \"foo\",\n"
            + "  null,\n"
            + "  \"bar\\\"baz\"}"));

    // 2-dimensional array with initializer
    assertThat(
        Expressions.toString(
            Expressions.newArrayInit(
                String.class,
                2,
                Expressions.constant(new String[] {"foo", "bar"}),
                Expressions.constant(null),
                Expressions.constant(new String[] {null}))),
        is("new String[][] {\n"
            + "  new String[] {\n"
            + "    \"foo\",\n"
            + "    \"bar\"},\n"
            + "  null,\n"
            + "  new String[] {\n"
            + "    null}}"));

    // 1-dimensional array
    assertThat(
        Expressions.toString(
            Expressions.newArrayBounds(
                String.class,
                1,
                Expressions.add(
                    Expressions.parameter(0, int.class, "x"),
                    Expressions.constant(1)))),
        is("new String[x + 1]"));

    // 3-dimensional array
    assertThat(
        Expressions.toString(
            Expressions.newArrayBounds(
                String.class,
                3,
                Expressions.add(
                    Expressions.parameter(0, int.class, "x"),
                    Expressions.constant(1)))),
        is("new String[x + 1][][]"));

    assertThat(
        Expressions.toString(
            Expressions.convert_(
                Expressions.call(
                    Expressions.convert_(
                        Expressions.convert_(
                            Expressions.constant("foo"),
                            Object.class),
                        String.class),
                    "length",
                    Collections.emptyList()),
                Integer.TYPE)),
        is("(int) ((String) (Object) \"foo\").length()"));

    // resolving a static method
    assertThat(
        Expressions.toString(
            Expressions.call(
                Integer.class,
                "valueOf",
                Collections.<Expression>singletonList(
                    Expressions.constant("0123")))),
        is("Integer.valueOf(\"0123\")"));

    // precedence of not and instanceof
    assertThat(
        Expressions.toString(
            Expressions.not(
                Expressions.typeIs(
                    Expressions.parameter(Object.class, "o"),
                    String.class))),
        is("(!(o instanceof String))"));

    // not not
    assertThat(
        Expressions.toString(
            Expressions.not(
                Expressions.not(
                    Expressions.typeIs(
                        Expressions.parameter(Object.class, "o"),
                        String.class)))),
        is("(!(!(o instanceof String)))"));
  }

  @Test void testWriteConstant() {
    // array of primitives
    assertThat(
        Expressions.toString(
            Expressions.constant(new int[]{1, 2, -1})),
        is("new int[] {\n"
            + "  1,\n"
            + "  2,\n"
            + "  -1}"));

    // primitive
    assertThat(
        Expressions.toString(
            Expressions.constant(-12)),
        is("-12"));

    assertThat(
        Expressions.toString(
            Expressions.constant((short) -12)),
        is("(short)-12"));

    assertThat(
        Expressions.toString(
            Expressions.constant((byte) -12)),
        is("(byte)-12"));

    // boxed primitives
    assertThat(
        Expressions.toString(
            Expressions.constant(1, Integer.class)),
        is("Integer.valueOf(1)"));

    assertThat(
        Expressions.toString(
            Expressions.constant(-3.14, Double.class)),
        is("Double.valueOf(-3.14D)"));

    assertThat(
        Expressions.toString(
            Expressions.constant(true, Boolean.class)),
        is("Boolean.valueOf(true)"));

    // primitive with explicit class
    assertThat(
        Expressions.toString(
            Expressions.constant(1, int.class)),
        is("1"));

    assertThat(
        Expressions.toString(
            Expressions.constant(1, short.class)),
        is("(short)1"));

    assertThat(
        Expressions.toString(
            Expressions.constant(1, byte.class)),
        is("(byte)1"));

    assertThat(
        Expressions.toString(
            Expressions.constant(-3.14, double.class)),
        is("-3.14D"));

    assertThat(
        Expressions.toString(
            Expressions.constant(true, boolean.class)),
        is("true"));

    // objects and nulls
    assertThat(
        Expressions.toString(
            Expressions.constant(new String[] {"foo", null})),
        is("new String[] {\n"
            + "  \"foo\",\n"
            + "  null}"));

    // string
    assertThat(
        Expressions.toString(
            Expressions.constant("hello, \"world\"!")),
        is("\"hello, \\\"world\\\"!\""));

    // enum
    assertThat(
        Expressions.toString(
            Expressions.constant(MyEnum.X)),
        is("org.apache.calcite.linq4j.test.ExpressionTest.MyEnum.X"));

    // array of enum
    assertThat(
        Expressions.toString(
            Expressions.constant(new MyEnum[]{MyEnum.X, MyEnum.Y})),
        is("new org.apache.calcite.linq4j.test.ExpressionTest.MyEnum[] {\n"
            + "  org.apache.calcite.linq4j.test.ExpressionTest.MyEnum.X,\n"
            + "  org.apache.calcite.linq4j.test.ExpressionTest.MyEnum.Y}"));

    // class
    assertThat(
        Expressions.toString(
            Expressions.constant(String.class)),
        is("java.lang.String.class"));

    // array class
    assertThat(
        Expressions.toString(
            Expressions.constant(int[].class)),
        is("int[].class"));

    assertThat(
        Expressions.toString(
            Expressions.constant(List[][].class)),
        is("java.util.List[][].class"));

    // automatically call constructor if it matches fields
    assertThat(
        Expressions.toString(
            Expressions.constant(Linq4jTest.emps)),
        is("new org.apache.calcite.linq4j.test.Linq4jTest.Employee[] {\n"
            + "  new org.apache.calcite.linq4j.test.Linq4jTest.Employee(\n"
            + "    100,\n"
            + "    \"Fred\",\n"
            + "    10),\n"
            + "  new org.apache.calcite.linq4j.test.Linq4jTest.Employee(\n"
            + "    110,\n"
            + "    \"Bill\",\n"
            + "    30),\n"
            + "  new org.apache.calcite.linq4j.test.Linq4jTest.Employee(\n"
            + "    120,\n"
            + "    \"Eric\",\n"
            + "    10),\n"
            + "  new org.apache.calcite.linq4j.test.Linq4jTest.Employee(\n"
            + "    130,\n"
            + "    \"Janet\",\n"
            + "    10)}"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6244">[CALCITE-6244]
   * Allow passing record as constant expression</a>. */
  @Test void testWriteRecordConstant(@TempDir Path tempDir) {
    Class<?> recordClass = createRecordClass(tempDir, "RecordModel");

    // Call constructor for record
    assertThat(
        Expressions.toString(
            Expressions.constant(
                ImmutableSet.of(createInstance(recordClass, "test1", 1),
                    createInstance(recordClass, "test2", 2),
                    createInstance(recordClass, "test3", 3),
                    createInstance(recordClass, "test4", 4)))),
        is("com.google.common.collect.ImmutableSet.of(new RecordModel(\n"
            +  "  \"test1\",\n"
            +  "  1),new RecordModel(\n"
            +  "  \"test2\",\n"
            +  "  2),new RecordModel(\n"
            +  "  \"test3\",\n"
            +  "  3),new RecordModel(\n"
            +  "  \"test4\",\n"
            +  "  4))"));
  }

  @Test void testWriteArray() {
    assertThat(
        Expressions.toString(
            Expressions.add(
                Expressions.constant(1),
                Expressions.arrayIndex(
                    Expressions.variable(int[].class, "integers"),
                    Expressions.add(
                        Expressions.constant(2),
                        Expressions.variable(int.class, "index"))))),
        is("1 + integers[2 + index]"));
  }

  @Test void testWriteAnonymousClass() {
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
    BlockStatement e =
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
                    Collections.emptyList(),
                    Arrays.asList(
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
                            Collections.emptyList(),
                            Blocks.toFunctionBlock(
                                Expressions.call(
                                    bazParameter,
                                    "size",
                                    Collections.emptyList()))),
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
                                    ImmutableList.of())))))));
    assertThat(Expressions.toString(e),
        is("{\n"
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
            + "}\n"));
  }

  @Test void testWriteWhile() {
    DeclarationStatement xDecl =
        Expressions.declare(0, "x", Expressions.constant(10));
    DeclarationStatement yDecl =
        Expressions.declare(0, "y", Expressions.constant(0));
    Node node =
        Expressions.block(xDecl, yDecl,
            Expressions.while_(
                Expressions.lessThan(xDecl.parameter, Expressions.constant(5)),
                Expressions.statement(
                    Expressions.preIncrementAssign(yDecl.parameter))));
    assertThat(node,
        hasToString("{\n"
                + "  int x = 10;\n"
                + "  int y = 0;\n"
                + "  while (x < 5) {\n"
                + "    (++y);\n"
                + "  }\n"
                + "}\n"));
  }

  @Test void testWriteTryCatchFinally() {
    final ParameterExpression cce_ =
        Expressions.parameter(Modifier.FINAL, ClassCastException.class, "cce");
    final ParameterExpression re_ =
        Expressions.parameter(0, RuntimeException.class, "re");
    Node node =
        Expressions.tryCatchFinally(
            Expressions.block(
                Expressions.return_(null,
                    Expressions.call(
                        Expressions.constant("foo"),
                        "length"))),
            Expressions.statement(
                Expressions.call(
                    Expressions.constant("foo"),
                    "toUpperCase")),
            Expressions.catch_(cce_,
                Expressions.return_(null, Expressions.constant(null))),
            Expressions.catch_(re_,
                Expressions.throw_(
                    Expressions.new_(IndexOutOfBoundsException.class))));
    assertThat(Expressions.toString(node),
        is("try {\n"
            + "  return \"foo\".length();\n"
            + "} catch (final ClassCastException cce) {\n"
            + "  return null;\n"
            + "} catch (RuntimeException re) {\n"
            + "  throw new IndexOutOfBoundsException();\n"
            + "} finally {\n"
            + "  \"foo\".toUpperCase();\n"
            + "}\n"));
  }

  @Test void testWriteTryFinally() {
    Node node =
        Expressions.ifThen(
            Expressions.constant(true),
            Expressions.tryFinally(
                Expressions.block(
                    Expressions.return_(null,
                        Expressions.call(
                            Expressions.constant("foo"),
                            "length"))),
                Expressions.statement(
                    Expressions.call(
                        Expressions.constant("foo"),
                        "toUpperCase"))));
    assertThat(Expressions.toString(node),
        is("if (true) {\n"
            + "  try {\n"
            + "    return \"foo\".length();\n"
            + "  } finally {\n"
            + "    \"foo\".toUpperCase();\n"
            + "  }\n"
            + "}\n"));
  }

  @Test void testWriteTryCatch() {
    final ParameterExpression cce_ =
        Expressions.parameter(Modifier.FINAL, ClassCastException.class, "cce");
    final ParameterExpression re_ =
        Expressions.parameter(0, RuntimeException.class, "re");
    Node node =
        Expressions.tryCatch(
            Expressions.block(
                Expressions.return_(null,
                    Expressions.call(Expressions.constant("foo"), "length"))),
            Expressions.catch_(cce_,
                Expressions.return_(null, Expressions.constant(null))),
            Expressions.catch_(re_,
                Expressions.return_(null,
                    Expressions.call(re_, "toString"))));
    assertThat(Expressions.toString(node),
        is("try {\n"
            + "  return \"foo\".length();\n"
            + "} catch (final ClassCastException cce) {\n"
            + "  return null;\n"
            + "} catch (RuntimeException re) {\n"
            + "  return re.toString();\n"
            + "}\n"));
  }

  @Test void testType() {
    // Type of ternary operator is the gcd of its arguments.
    assertThat(
        Expressions.condition(
            Expressions.constant(true),
            Expressions.constant(5),
            Expressions.constant(6L)).getType(),
        is(long.class));
    assertThat(
        Expressions.condition(
            Expressions.constant(true),
            Expressions.constant(5L),
            Expressions.constant(6)).getType(),
        is(long.class));

    // If one of the arguments is null constant, it is implicitly coerced.
    assertThat(
        Expressions.condition(
            Expressions.constant(true),
            Expressions.constant("xxx"),
            Expressions.constant(null)).getType(),
        is(String.class));
    assertThat(
        Expressions.condition(
            Expressions.constant(true),
            Expressions.constant(0),
            Expressions.constant(null)).getType(),
        is(Integer.class));

    // In Java, "-" applied to short and byte yield int.
    assertThat(Expressions.negate(Expressions.constant((double) 1)).getType(),
        is(double.class));
    assertThat(Expressions.negate(Expressions.constant((float) 1)).getType(),
        is(float.class));
    assertThat(Expressions.negate(Expressions.constant((long) 1)).getType(),
        is(long.class));
    assertThat(Expressions.negate(Expressions.constant(1)).getType(),
        is(int.class));
    assertThat(Expressions.negate(Expressions.constant((short) 1)).getType(),
        is(int.class));
    assertThat(Expressions.negate(Expressions.constant((byte) 1)).getType(),
        is(int.class));
  }

  @Test void testCompile() {
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
    assertThat(x, is(1234));
  }

  @Test void testBlockBuilder() {
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
    BlockStatement expression = statements.toBlock();
    assertThat(Expressions.toString(expression), is(expected));
    expression.accept(new Shuttle());
  }

  @Test void testBlockBuilder2() {
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
    BlockStatement expression = statements.toBlock();
    final String expected = "{\n"
        + "  final java.util.TreeSet treeSet = new java.util.TreeSet(\n"
        + "    (java.util.Comparator) null);\n"
        + "  return treeSet.add(null);\n"
        + "}\n";
    assertThat(Expressions.toString(expression), is(expected));
    expression.accept(new Shuttle());
  }

  @Test void testBlockBuilder3() {
/*
    int a = 1;
    int b = a + 2;
    int c = a + 3;
    int d = a + 4;
    int e = {
      int b = a + 3;
      foo(b);
    }
    bar(a, b, c, d, e);
*/
    BlockBuilder builder0 = new BlockBuilder();
    final Expression a = builder0.append("_a", Expressions.constant(1));
    final Expression b =
        builder0.append("_b", Expressions.add(a, Expressions.constant(2)));
    final Expression c =
        builder0.append("_c", Expressions.add(a, Expressions.constant(3)));
    final Expression d =
        builder0.append("_d", Expressions.add(a, Expressions.constant(4)));

    BlockBuilder builder1 = new BlockBuilder();
    final Expression b1 =
        builder1.append("_b", Expressions.add(a, Expressions.constant(3)));
    builder1.add(
        Expressions.statement(
            Expressions.call(ExpressionTest.class, "foo", b1)));
    final Expression e = builder0.append("e", builder1.toBlock());
    builder0.add(
        Expressions.statement(
            Expressions.call(ExpressionTest.class, "bar", a, b, c, d, e)));
    // With the bug in BlockBuilder.append(String, BlockExpression),
    //    bar(1, _b, _c, _d, foo(_d));
    // Correct result is
    //    bar(1, _b, _c, _d, foo(_c));
    // because _c has the same expression (a + 3) as inner b.
    BlockStatement expression = builder0.toBlock();
    assertThat(Expressions.toString(expression),
        is("{\n"
            + "  final int _b = 1 + 2;\n"
            + "  final int _c = 1 + 3;\n"
            + "  final int _d = 1 + 4;\n"
            + "  final int _b0 = 1 + 3;\n"
            + "  org.apache.calcite.linq4j.test.ExpressionTest.bar(1, _b, _c, _d, org.apache.calcite.linq4j.test.ExpressionTest.foo(_b0));\n"
            + "}\n"));
    expression.accept(new Shuttle());
  }

  @Test void testConstantExpression() {
    final Expression constant =
        Expressions.constant(new Object[] {
            1,
            new Object[] {
                (byte) 1, (short) 2, (int) 3, (long) 4,
                (float) 5, (double) 6, (char) 7, true, "string", null
            },
            new AllType(true, (byte) 100, (char) 101, (short) 102, 103,
                104L, (float) 105, 106D, new BigDecimal(107),
                new BigInteger("108"), "109", null)
        });
    assertThat(constant,
        hasToString("new Object[] {\n"
            + "  1,\n"
            + "  new Object[] {\n"
            + "    (byte)1,\n"
            + "    (short)2,\n"
            + "    3,\n"
            + "    4L,\n"
            + "    5.0F,\n"
            + "    6.0D,\n"
            + "    (char)7,\n"
            + "    true,\n"
            + "    \"string\",\n"
            + "    null},\n"
            + "  new org.apache.calcite.linq4j.test.ExpressionTest.AllType(\n"
            + "    true,\n"
            + "    (byte)100,\n"
            + "    (char)101,\n"
            + "    (short)102,\n"
            + "    103,\n"
            + "    104L,\n"
            + "    105.0F,\n"
            + "    106.0D,\n"
            + "    java.math.BigDecimal.valueOf(107L),\n"
            + "    new java.math.BigInteger(\"108\"),\n"
            + "    \"109\",\n"
            + "    null)}"));
    constant.accept(new Shuttle());
  }

  @Test void testBigDecimalConstantExpression() {
    assertThat(
        Expressions.toString(Expressions.constant("104", BigDecimal.class)),
        is("java.math.BigDecimal.valueOf(104L)"));
    assertThat(
        Expressions.toString(Expressions.constant("1000", BigDecimal.class)),
        is("java.math.BigDecimal.valueOf(1L, -3)"));
    assertThat(
        Expressions.toString(Expressions.constant(1000, BigDecimal.class)),
        is("java.math.BigDecimal.valueOf(1L, -3)"));
    assertThat(
        Expressions.toString(Expressions.constant(107, BigDecimal.class)),
        is("java.math.BigDecimal.valueOf(107L)"));
    assertThat(
        Expressions.toString(
            Expressions.constant(199999999999999L, BigDecimal.class)),
        is("java.math.BigDecimal.valueOf(199999999999999L)"));
    assertThat(
        Expressions.toString(Expressions.constant(12.34, BigDecimal.class)),
        is("java.math.BigDecimal.valueOf(1234L, 2)"));
  }

  @Test void testObjectConstantExpression() {
    assertThat(
        Expressions.toString(Expressions.constant((byte) 100, Object.class)),
        is("(byte)100"));
    assertThat(
        Expressions.toString(Expressions.constant((char) 100, Object.class)),
        is("(char)100"));
    assertThat(
        Expressions.toString(Expressions.constant((short) 100, Object.class)),
        is("(short)100"));
    assertThat(Expressions.toString(Expressions.constant(100L, Object.class)),
        is("100L"));
    assertThat(Expressions.toString(Expressions.constant(100F, Object.class)),
        is("100.0F"));
    assertThat(Expressions.toString(Expressions.constant(100D, Object.class)),
        is("100.0D"));
  }

  @Test void testClassDecl() {
    final NewExpression newExpression =
        Expressions.new_(
            Object.class,
            ImmutableList.of(),
            Arrays.asList(
                Expressions.fieldDecl(
                    Modifier.PUBLIC | Modifier.FINAL,
                    Expressions.parameter(String.class, "foo"),
                    Expressions.constant("bar")),
                new ClassDeclaration(
                    Modifier.PUBLIC | Modifier.STATIC,
                    "MyClass",
                    null,
                    ImmutableList.of(),
                    Arrays.asList(
                        new FieldDeclaration(
                            0,
                            Expressions.parameter(int.class, "x"),
                            Expressions.constant(0)))),
                Expressions.fieldDecl(
                    0,
                    Expressions.parameter(int.class, "i"))));
    assertThat(Expressions.toString(newExpression),
        is("new Object(){\n"
            + "  public final String foo = \"bar\";\n"
            + "  public static class MyClass {\n"
            + "    int x = 0;\n"
            + "  }\n"
            + "  int i;\n"
            + "}"));
    newExpression.accept(new Shuttle());
  }

  @Test void testReturn() {
    assertThat(
        Expressions.toString(
            Expressions.ifThenElse(
                Expressions.constant(true),
                Expressions.return_(null),
                Expressions.return_(null, Expressions.constant(1)))),
        is("if (true) {\n"
            + "  return;\n"
            + "} else {\n"
            + "  return 1;\n"
            + "}\n"));
  }

  @Test void testIfElseIfElse() {
    assertThat(
        Expressions.toString(
            Expressions.ifThenElse(
                Expressions.constant(true),
                Expressions.return_(null),
                Expressions.constant(false),
                Expressions.return_(null),
                Expressions.return_(null, Expressions.constant(1)))),
        is("if (true) {\n"
            + "  return;\n"
            + "} else if (false) {\n"
            + "  return;\n"
            + "} else {\n"
            + "  return 1;\n"
            + "}\n"));
  }

  /** Test for common sub-expression elimination. */
  @Test void testSubExpressionElimination() {
    final BlockBuilder builder = new BlockBuilder(true);
    ParameterExpression x = Expressions.parameter(Object.class, "p");
    Expression current4 =
        builder.append("current4",
            Expressions.convert_(x, Object[].class));
    Expression v =
        builder.append("v",
            Expressions.convert_(
                Expressions.arrayIndex(current4, Expressions.constant(4)),
                Short.class));
    Expression v0 =
        builder.append("v0",
            Expressions.convert_(v, Number.class));
    Expression v1 =
        builder.append("v1",
            Expressions.convert_(
                Expressions.arrayIndex(current4, Expressions.constant(4)),
                Short.class));
    Expression v2 =
        builder.append("v2", Expressions.convert_(v, Number.class));
    Expression v3 =
        builder.append("v3",
        Expressions.convert_(
            Expressions.arrayIndex(current4, Expressions.constant(4)),
            Short.class));
    Expression v4 =
        builder.append("v4",
            Expressions.convert_(v3, Number.class));
    Expression v5 = builder.append("v5", Expressions.call(v4, "intValue"));
    Expression v6 =
        builder.append("v6",
            Expressions.condition(
                Expressions.equal(v2, Expressions.constant(null)),
                Expressions.constant(null),
                Expressions.equal(v5, Expressions.constant(1997))));
    builder.add(Expressions.return_(null, v6));
    assertThat(Expressions.toString(builder.toBlock()),
        is("{\n"
            + "  final Short v = (Short) ((Object[]) p)[4];\n"
            + "  return (Number) v == null ? null : ("
            + "(Number) v).intValue() == 1997;\n"
            + "}\n"));
  }

  @Test void testFor() throws NoSuchFieldException {
    final BlockBuilder builder = new BlockBuilder();
    final ParameterExpression i_ = Expressions.parameter(int.class, "i");
    builder.add(
        Expressions.for_(
            Expressions.declare(
                0, i_, Expressions.constant(0)),
            Expressions.lessThan(i_, Expressions.constant(10)),
            Expressions.postIncrementAssign(i_),
            Expressions.block(
                Expressions.statement(
                    Expressions.call(
                        Expressions.field(
                            null, System.class.getField("out")),
                        "println",
                        i_)))));
    assertThat(Expressions.toString(builder.toBlock()),
        is("{\n"
            + "  for (int i = 0; i < 10; i++) {\n"
            + "    System.out.println(i);\n"
            + "  }\n"
            + "}\n"));
  }

  @Test void testFor2() {
    final BlockBuilder builder = new BlockBuilder();
    final ParameterExpression i_ = Expressions.parameter(int.class, "i");
    final ParameterExpression j_ = Expressions.parameter(int.class, "j");
    builder.add(
        Expressions.for_(
            Arrays.asList(
                Expressions.declare(
                    0, i_, Expressions.constant(0)),
                Expressions.declare(
                    0, j_, Expressions.constant(10))),
            null,
            null,
            Expressions.block(
                Expressions.ifThen(
                    Expressions.lessThan(
                        Expressions.preIncrementAssign(i_),
                        Expressions.preDecrementAssign(j_)),
                    Expressions.break_(null)))));
    assertThat(Expressions.toString(builder.toBlock()),
        is("{\n"
            + "  for (int i = 0, j = 10; ; ) {\n"
            + "    if ((++i) < (--j)) {\n"
            + "      break;\n"
            + "    }\n"
            + "  }\n"
            + "}\n"));
  }

  @Test void testForEach() {
    final BlockBuilder builder = new BlockBuilder();
    final ParameterExpression i_ = Expressions.parameter(int.class, "i");
    final ParameterExpression list_ = Expressions.parameter(List.class, "list");
    builder.add(
        Expressions.forEach(i_, list_,
            Expressions.ifThen(
                Expressions.lessThan(
                    Expressions.constant(1),
                    Expressions.constant(2)),
                Expressions.break_(null))));
    assertThat(Expressions.toString(builder.toBlock()),
        is("{\n"
            + "  for (int i : list) {\n"
            + "    if (1 < 2) {\n"
            + "      break;\n"
            + "    }\n"
            + "  }\n"
            + "}\n"));
  }

  @Test void testEmptyListLiteral() {
    assertThat(Expressions.toString(Expressions.constant(Arrays.asList())),
        is("java.util.Collections.EMPTY_LIST"));
  }

  @Test void testOneElementListLiteral() {
    assertThat(Expressions.toString(Expressions.constant(Arrays.asList(1))),
        is("java.util.Arrays.asList(1)"));
  }

  @Test void testTwoElementsListLiteral() {
    assertThat(Expressions.toString(Expressions.constant(Arrays.asList(1, 2))),
        is("java.util.Arrays.asList(1,\n"
            + "  2)"));
  }

  @Test void testNestedListsLiteral() {
    assertThat(
        Expressions.toString(
            Expressions.constant(
                Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4)))),
        is("java.util.Arrays.asList(java.util.Arrays.asList(1,\n"
            + "    2),\n"
            + "  java.util.Arrays.asList(3,\n"
            + "    4))"));
  }

  @Test void testEmptyMapLiteral() {
    assertThat(Expressions.toString(Expressions.constant(new HashMap<>())),
        is("com.google.common.collect.ImmutableMap.of()"));
  }

  @Test void testOneElementMapLiteral() {
    assertThat(
        Expressions.toString(
            Expressions.constant(Collections.singletonMap("abc", 42))),
        is("com.google.common.collect.ImmutableMap.of(\"abc\", 42)"));
  }

  @Test void testTwoElementsMapLiteral() {
    assertThat(
        Expressions.toString(
            Expressions.constant(ImmutableMap.of("abc", 42, "def", 43))),
        is("com.google.common.collect.ImmutableMap.of(\"abc\", 42,\n"
            + "\"def\", 43)"));
  }

  @Test void testTenElementsMapLiteral() {
    Map<String, String> map = new LinkedHashMap<>(); // for consistent output
    for (int i = 0; i < 10; i++) {
      map.put("key_" + i, "value_" + i);
    }
    assertThat(Expressions.toString(Expressions.constant(map)),
        is("com.google.common.collect.ImmutableMap.builder()"
            + ".put(\"key_0\", \"value_0\")\n"
            + ".put(\"key_1\", \"value_1\")\n"
            + ".put(\"key_2\", \"value_2\")\n"
            + ".put(\"key_3\", \"value_3\")\n"
            + ".put(\"key_4\", \"value_4\")\n"
            + ".put(\"key_5\", \"value_5\")\n"
            + ".put(\"key_6\", \"value_6\")\n"
            + ".put(\"key_7\", \"value_7\")\n"
            + ".put(\"key_8\", \"value_8\")\n"
            + ".put(\"key_9\", \"value_9\").build()"));
  }

  @Test void testEvaluate() {
    Expression x = Expressions.add(ONE, TWO);
    Object value = Expressions.evaluate(x);
    assertThat(value, is(3));
  }

  @Test void testEmptySetLiteral() {
    assertThat(Expressions.toString(Expressions.constant(new HashSet<>())),
        is("com.google.common.collect.ImmutableSet.of()"));
  }

  @Test void testOneElementSetLiteral() {
    assertThat(Expressions.toString(Expressions.constant(Sets.newHashSet(1))),
        is("com.google.common.collect.ImmutableSet.of(1)"));
  }

  @Test void testTwoElementsSetLiteral() {
    assertThat(
        Expressions.toString(Expressions.constant(ImmutableSet.of(1, 2))),
        is("com.google.common.collect.ImmutableSet.of(1,2)"));
  }

  @Test void testTenElementsSetLiteral() {
    Set<Integer> set = new LinkedHashSet<>(); // for consistent output
    for (int i = 0; i < 10; i++) {
      set.add(i);
    }
    assertThat(Expressions.toString(Expressions.constant(set)),
        is("com.google.common.collect.ImmutableSet.builder().add(0)\n"
            + ".add(1)\n"
            + ".add(2)\n"
            + ".add(3)\n"
            + ".add(4)\n"
            + ".add(5)\n"
            + ".add(6)\n"
            + ".add(7)\n"
            + ".add(8)\n"
            + ".add(9).build()"));
  }

  @Test void testTenElementsLinkedHashSetLiteral() {
    Set<Integer> set = new LinkedHashSet<>(); // for consistent output
    for (int i = 0; i < 10; i++) {
      set.add(i);
    }
    assertThat(Expressions.toString(Expressions.constant(set)),
        is("com.google.common.collect.ImmutableSet.builder().add(0)\n"
            + ".add(1)\n"
            + ".add(2)\n"
            + ".add(3)\n"
            + ".add(4)\n"
            + ".add(5)\n"
            + ".add(6)\n"
            + ".add(7)\n"
            + ".add(8)\n"
            + ".add(9).build()"));
  }

  @Test void testTenElementsSetStringLiteral() {
    Set<String> set = new LinkedHashSet<>(); // for consistent output
    for (int i = 10; i > 0; i--) {
      set.add(String.valueOf(i));
    }
    assertThat(Expressions.toString(Expressions.constant(set)),
        is("com.google.common.collect.ImmutableSet.builder().add(\"10\")\n"
            + ".add(\"9\")\n"
            + ".add(\"8\")\n"
            + ".add(\"7\")\n"
            + ".add(\"6\")\n"
            + ".add(\"5\")\n"
            + ".add(\"4\")\n"
            + ".add(\"3\")\n"
            + ".add(\"2\")\n"
            + ".add(\"1\").build()"));
  }

  /** An enum. */
  enum MyEnum {
    X,
    Y {
      public String toString() {
        return "YYY";
      }
    }
  }

  public static int foo(int x) {
    return 0;
  }

  public static int bar(int v, int w, int x, int y, int z) {
    return 0;
  }

  /** A class with a field for each type of interest. */
  public static class AllType {
    public final boolean b;
    public final byte y;
    public final char c;
    public final short s;
    public final int i;
    public final long l;
    public final float f;
    public final double d;
    public final BigDecimal bd;
    public final BigInteger bi;
    public final String str;
    public final @Nullable Object o;

    public AllType(boolean b, byte y, char c, short s, int i, long l, float f,
        double d, BigDecimal bd, BigInteger bi, String str, @Nullable Object o) {
      this.b = b;
      this.y = y;
      this.c = c;
      this.s = s;
      this.i = i;
      this.l = l;
      this.f = f;
      this.d = d;
      this.bd = bd;
      this.bi = bi;
      this.str = str;
      this.o = o;
    }
  }
}
