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

import org.junit.Test;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Unit test for {@link net.hydromatic.linq4j.expressions.Expression}
 * and subclasses.
 */
public class ExpressionTest {
  @Test public void testLambdaCallsBinaryOp() {
    // A parameter for the lambda expression.
    ParameterExpression paramExpr =
        Expressions.parameter(Double.TYPE, "arg");

    // This expression represents a lambda expression
    // that adds 1 to the parameter value.
    FunctionExpression lambdaExpr = Expressions.lambda(
        Expressions.add(
            paramExpr,
            Expressions.constant(2d)),
        Arrays.asList(paramExpr));

    // Print out the expression.
    String s = Expressions.toString(lambdaExpr);
    assertEquals(
        "new net.hydromatic.linq4j.function.Function1() {\n"
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
        + "}\n",
        s);

    // Compile and run the lambda expression.
    // The value of the parameter is 1.5.
    double n = (Double) lambdaExpr.compile().dynamicInvoke(1.5d);

    // This code example produces the following output:
    //
    // arg => (arg +2)
    // 3
    assertEquals(3.5D, n, 0d);
  }

  @Test public void testLambdaPrimitiveTwoArgs() {
    // Parameters for the lambda expression.
    ParameterExpression paramExpr =
        Expressions.parameter(int.class, "key");
    ParameterExpression param2Expr =
        Expressions.parameter(int.class, "key2");

    FunctionExpression lambdaExpr = Expressions.lambda(
        Expressions.block(
            (Type) null,
            Expressions.return_(
                null, paramExpr)),
        Arrays.asList(paramExpr, param2Expr));

    // Print out the expression.
    String s = Expressions.toString(lambdaExpr);
    assertEquals("new net.hydromatic.linq4j.function.Function2() {\n"
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
        + "}\n",
        s);
  }

  @Test public void testLambdaCallsTwoArgMethod() throws NoSuchMethodException {
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

  @Test public void testFoldAnd() {
    // empty list yields true
    final List<Expression> list0 = Collections.emptyList();
    assertEquals(
        "true",
        Expressions.toString(
            Expressions.foldAnd(list0)));
    assertEquals(
        "false",
        Expressions.toString(
            Expressions.foldOr(list0)));

    final List<Expression> list1 =
        Arrays.asList(
            Expressions.equal(Expressions.constant(1), Expressions.constant(2)),
            Expressions.equal(Expressions.constant(3), Expressions.constant(4)),
            Expressions.constant(true),
            Expressions.equal(Expressions.constant(5),
                Expressions.constant(6)));
    // true is eliminated from AND
    assertEquals(
        "1 == 2 && 3 == 4 && 5 == 6",
        Expressions.toString(
            Expressions.foldAnd(list1)));
    // a single true makes OR true
    assertEquals(
        "true",
        Expressions.toString(
            Expressions.foldOr(list1)));

    final List<Expression> list2 =
        Collections.<Expression>singletonList(
            Expressions.constant(true));
    assertEquals(
        "true",
        Expressions.toString(
            Expressions.foldAnd(list2)));
    assertEquals(
        "true",
        Expressions.toString(
            Expressions.foldOr(list2)));

    final List<Expression> list3 =
        Arrays.asList(
            Expressions.equal(Expressions.constant(1), Expressions.constant(2)),
            Expressions.constant(false),
            Expressions.equal(Expressions.constant(5),
                Expressions.constant(6)));
    // false causes whole list to be false
    assertEquals(
        "false",
        Expressions.toString(
            Expressions.foldAnd(list3)));
    assertEquals(
        "1 == 2 || 5 == 6",
        Expressions.toString(
            Expressions.foldOr(list3)));
  }

  @Test public void testWrite() {
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

    // "--5" would be a syntax error
    assertEquals(
        "- - 5",
        Expressions.toString(
            Expressions.negate(
                Expressions.negate(
                    Expressions.constant(5)))));

    assertEquals(
        "a.empno",
        Expressions.toString(
            Expressions.field(
                Expressions.parameter(Linq4jTest.Employee.class, "a"),
                "empno")));

    assertEquals(
        "a.length",
        Expressions.toString(
            Expressions.field(
                Expressions.parameter(Object[].class, "a"),
                "length")));

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

    // 1-dimensional array with initializer
    assertEquals(
        "new String[] {\n"
        + "  \"foo\",\n"
        + "  null,\n"
        + "  \"bar\\\"baz\"}",
        Expressions.toString(
            Expressions.newArrayInit(
                String.class,
                Expressions.constant("foo"),
                Expressions.constant(null),
                Expressions.constant("bar\"baz"))));

    // 2-dimensional array with initializer
    assertEquals(
        "new String[][] {\n"
        + "  new String[] {\n"
        + "    \"foo\",\n"
        + "    \"bar\"},\n"
        + "  null,\n"
        + "  new String[] {\n"
        + "    null}}",
        Expressions.toString(
            Expressions.newArrayInit(
                String.class,
                2,
                Expressions.constant(new String[] {"foo", "bar"}),
                Expressions.constant(null),
                Expressions.constant(new String[] {null}))));

    // 1-dimensional array
    assertEquals(
        "new String[x + 1]",
        Expressions.toString(
            Expressions.newArrayBounds(
                String.class,
                1,
                Expressions.add(
                    Expressions.parameter(0, int.class, "x"),
                    Expressions.constant(1)))));

    // 3-dimensional array
    assertEquals(
        "new String[x + 1][][]",
        Expressions.toString(
            Expressions.newArrayBounds(
                String.class,
                3,
                Expressions.add(
                    Expressions.parameter(0, int.class, "x"),
                    Expressions.constant(1)))));

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

  @Test public void testWriteConstant() {
    // array of primitives
    assertEquals(
        "new int[] {\n"
        + "  1,\n"
        + "  2,\n"
        + "  -1}",
        Expressions.toString(
            Expressions.constant(new int[]{1, 2, -1})));

    // primitive
    assertEquals(
        "-12",
        Expressions.toString(
            Expressions.constant(-12)));

    assertEquals(
        "(short)-12",
        Expressions.toString(
            Expressions.constant((short) -12)));

    assertEquals(
        "(byte)-12",
        Expressions.toString(
            Expressions.constant((byte) -12)));

    // boxed primitives
    assertEquals(
        "Integer.valueOf(1)",
        Expressions.toString(
            Expressions.constant(1, Integer.class)));

    assertEquals(
        "Double.valueOf(-3.14D)",
        Expressions.toString(
            Expressions.constant(-3.14, Double.class)));

    assertEquals(
        "Boolean.valueOf(true)",
        Expressions.toString(
            Expressions.constant(true, Boolean.class)));

    // primitive with explicit class
    assertEquals(
        "1",
        Expressions.toString(
            Expressions.constant(1, int.class)));

    assertEquals(
        "(short)1",
        Expressions.toString(
            Expressions.constant(1, short.class)));

    assertEquals(
        "(byte)1",
        Expressions.toString(
            Expressions.constant(1, byte.class)));

    assertEquals(
        "-3.14D",
        Expressions.toString(
            Expressions.constant(-3.14, double.class)));

    assertEquals(
        "true",
        Expressions.toString(
            Expressions.constant(true, boolean.class)));

    // objects and nulls
    assertEquals(
        "new String[] {\n"
        + "  \"foo\",\n"
        + "  null}",
        Expressions.toString(
            Expressions.constant(new String[] {"foo", null})));

    // string
    assertEquals(
        "\"hello, \\\"world\\\"!\"",
        Expressions.toString(
            Expressions.constant("hello, \"world\"!")));

    // enum
    assertEquals(
        "net.hydromatic.linq4j.test.ExpressionTest.MyEnum.X",
        Expressions.toString(
            Expressions.constant(MyEnum.X)));

    // array of enum
    assertEquals(
        "new net.hydromatic.linq4j.test.ExpressionTest.MyEnum[] {\n"
        + "  net.hydromatic.linq4j.test.ExpressionTest.MyEnum.X,\n"
        + "  net.hydromatic.linq4j.test.ExpressionTest.MyEnum.Y}",
        Expressions.toString(
            Expressions.constant(new MyEnum[]{MyEnum.X, MyEnum.Y})));

    // class
    assertEquals(
        "java.lang.String.class",
        Expressions.toString(
            Expressions.constant(String.class)));

    // array class
    assertEquals(
        "int[].class",
        Expressions.toString(
            Expressions.constant(int[].class)));

    assertEquals(
        "java.util.List[][].class",
        Expressions.toString(
            Expressions.constant(List[][].class)));

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

  @Test public void testWriteArray() {
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

  @Test public void testWriteAnonymousClass() {
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

  @Test public void testWriteWhile() {
    DeclarationStatement xDecl;
    DeclarationStatement yDecl;
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

  @Test public void testWriteTryCatchFinally() {
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
    assertEquals(
        "try {\n"
        + "  return \"foo\".length();\n"
        + "} catch (final ClassCastException cce) {\n"
        + "  return null;\n"
        + "} catch (RuntimeException re) {\n"
        + "  throw new IndexOutOfBoundsException();\n"
        + "} finally {\n"
        + "  \"foo\".toUpperCase();\n"
        + "}\n",
        Expressions.toString(node));
  }

  @Test public void testWriteTryFinally() {
    final ParameterExpression cce_ =
        Expressions.parameter(Modifier.FINAL, ClassCastException.class, "cce");
    final ParameterExpression re_ =
        Expressions.parameter(0, RuntimeException.class, "re");
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
    assertEquals(
        "if (true) {\n"
        + "  try {\n"
        + "    return \"foo\".length();\n"
        + "  } finally {\n"
        + "    \"foo\".toUpperCase();\n"
        + "  }\n"
        + "}\n",
        Expressions.toString(node));
  }

  @Test public void testWriteTryCatch() {
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
    assertEquals(
        "try {\n"
        + "  return \"foo\".length();\n"
        + "} catch (final ClassCastException cce) {\n"
        + "  return null;\n"
        + "} catch (RuntimeException re) {\n"
        + "  return re.toString();\n"
        + "}\n",
        Expressions.toString(node));
  }

  @Test public void testType() {
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
            Expressions.constant(0),
            Expressions.constant(null)).getType());
  }

  @Test public void testCompile() throws NoSuchMethodException {
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

  @Test public void testBlockBuilder() {
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
    assertEquals(expected, Expressions.toString(expression));
    expression.accept(new Visitor());
  }

  @Test public void testBlockBuilder2() {
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
    assertEquals(
        "{\n"
        + "  return new java.util.TreeSet(\n"
        + "      (java.util.Comparator) null).add(null);\n"
        + "}\n",
        Expressions.toString(expression));
    expression.accept(new Visitor());
  }

  @Test public void testBlockBuilder3() {
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
    assertEquals(
        "{\n"
        + "  final int _b = 1 + 2;\n"
        + "  final int _c = 1 + 3;\n"
        + "  final int _d = 1 + 4;\n"
        + "  net.hydromatic.linq4j.test.ExpressionTest.bar(1, _b, _c, _d, net.hydromatic.linq4j.test.ExpressionTest.foo(_c));\n"
        + "}\n",
        Expressions.toString(expression));
    expression.accept(new Visitor());
  }

  @Test public void testConstantExpression() {
    final Expression constant = Expressions.constant(
        new Object[] {
          1,
          new Object[] {
            (byte) 1, (short) 2, (int) 3, (long) 4,
            (float) 5, (double) 6, (char) 7, true, "string", null
          },
          new AllType(true, (byte) 100, (char) 101, (short) 102, 103,
            (long) 104, (float) 105, (double) 106, new BigDecimal(107),
            new BigInteger("108"), "109", null)
        });
    assertEquals(
        "new Object[] {\n"
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
        + "  new net.hydromatic.linq4j.test.ExpressionTest.AllType(\n"
        + "    true,\n"
        + "    (byte)100,\n"
        + "    (char)101,\n"
        + "    (short)102,\n"
        + "    103,\n"
        + "    104L,\n"
        + "    105.0F,\n"
        + "    106.0D,\n"
        + "    new java.math.BigDecimal(107L),\n"
        + "    new java.math.BigInteger(\"108\"),\n"
        + "    \"109\",\n"
        + "    null)}",
        constant.toString());
    constant.accept(new Visitor());
  }

  @Test public void testClassDecl() {
    final NewExpression newExpression =
        Expressions.new_(
            Object.class,
            Collections.<Expression>emptyList(),
            Arrays.<MemberDeclaration>asList(
                Expressions.fieldDecl(
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
                Expressions.fieldDecl(
                    0,
                    Expressions.parameter(int.class, "i"))));
    assertEquals(
        "new Object(){\n"
        + "  public final String foo = \"bar\";\n"
        + "  public static class MyClass {\n"
        + "    int x = 0;\n"
        + "  }\n"
        + "  int i;\n"
        + "}",
        Expressions.toString(newExpression));
    newExpression.accept(new Visitor());
  }

  @Test public void testReturn() {
    assertEquals(
        "if (true) {\n"
        + "  return;\n"
        + "} else {\n"
        + "  return 1;\n"
        + "}\n",
        Expressions.toString(
            Expressions.ifThenElse(
                Expressions.constant(true),
                Expressions.return_(null),
                Expressions.return_(null, Expressions.constant(1)))));
  }

  @Test public void testIfElseIfElse() {
    assertEquals(
        "if (true) {\n"
        + "  return;\n"
        + "} else if (false) {\n"
        + "  return;\n"
        + "} else {\n"
        + "  return 1;\n"
        + "}\n",
        Expressions.toString(
            Expressions.ifThenElse(
                Expressions.constant(true),
                Expressions.return_(null),
                Expressions.constant(false),
                Expressions.return_(null),
                Expressions.return_(null, Expressions.constant(1)))));
  }

  /** Test for common sub-expression elimination. */
  @Test public void testSubExpressionElimination() {
    final BlockBuilder builder = new BlockBuilder(true);
    ParameterExpression x = Expressions.parameter(Object.class, "p");
    Expression current4 = builder.append(
        "current4",
        Expressions.convert_(x, Object[].class));
    Expression v = builder.append(
        "v",
        Expressions.convert_(
            Expressions.arrayIndex(
                current4,
                Expressions.constant(4)), Short.class));
    Expression v0 = builder.append(
        "v0",
        Expressions.convert_(v, Number.class));
    Expression v1 = builder.append(
        "v1",
        Expressions.convert_(
            Expressions.arrayIndex(
                current4,
                Expressions.constant(4)), Short.class));
    Expression v2 = builder.append(
        "v2",
        Expressions.convert_(v, Number.class));
    Expression v3 = builder.append(
        "v3",
        Expressions.convert_(
            Expressions.arrayIndex(
                current4,
                Expressions.constant(4)), Short.class));
    Expression v4 = builder.append(
        "v4",
        Expressions.convert_(v3, Number.class));
    Expression v5 = builder.append("v5", Expressions.call(v4, "intValue"));
    Expression v6 = builder.append(
        "v6",
        Expressions.condition(
            Expressions.equal(v2, Expressions.constant(null)),
            Expressions.constant(null),
            Expressions.equal(v5, Expressions.constant(1997))));
    builder.add(Expressions.return_(null, v6));
    assertEquals(
        "{\n"
        + "  final Short v = (Short) ((Object[]) p)[4];\n"
        + "  return (Number) v == null ? (Boolean) null : ("
        + "(Number) v).intValue() == 1997;\n"
        + "}\n",
        Expressions.toString(builder.toBlock()));
  }

  @Test public void testFor() throws NoSuchFieldException {
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
    assertEquals(
        "{\n"
        + "  for (int i = 0; i < 10; i++) {\n"
        + "    System.out.println(i);\n"
        + "  }\n"
        + "}\n",
        Expressions.toString(builder.toBlock()));
  }

  @Test public void testFor2() throws NoSuchFieldException {
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
    assertEquals(
        "{\n"
        + "  for (int i = 0, j = 10; ; ) {\n"
        + "    if (++i < --j) {\n"
        + "      break;\n"
        + "    }\n"
        + "  }\n"
        + "}\n",
        Expressions.toString(builder.toBlock()));
  }

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
    public final Object o;

    public AllType(boolean b, byte y, char c, short s, int i, long l, float f,
        double d, BigDecimal bd, BigInteger bi, String str, Object o) {
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

// End ExpressionTest.java
