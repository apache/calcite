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

import net.hydromatic.linq4j.function.Deterministic;
import net.hydromatic.linq4j.function.NonDeterministic;

import org.junit.Test;

import java.lang.reflect.Method;
import java.math.BigInteger;
import java.util.Collections;
import java.util.concurrent.Callable;

import static net.hydromatic.linq4j.test.BlockBuilderBase.*;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

/**
 * Tests factoring out deterministic expressions.
 */
public class DeterministicTest {
  /**
   * Class to test @Deterministic annotation
   */
  public static class TestClass {
    @Deterministic
    public static int deterministic(int a) {
      return a + 1;
    }

    public static int nonDeterministic(int a) {
      return a + 2;
    }
  }

  /**
   * Class to test @NonDeterministic annotation
   */
  @Deterministic
  public static class TestDeterministicClass {
    public static int deterministic(int a) {
      return a + 1;
    }

    @NonDeterministic
    public static int nonDeterministic(int a) {
      return a + 2;
    }
  }

  private boolean isAtomic(Expression e) {
    /** Subclass to make a protected method public. */
    class MyDeterministicCodeOptimizer extends DeterministicCodeOptimizer {
      public MyDeterministicCodeOptimizer() {
        super(ClassDeclarationFinder.create());
      }

      @Override public boolean isConstant(Expression expression) {
        return super.isConstant(expression);
      }
    }
    return new MyDeterministicCodeOptimizer().isConstant(e);
  }

  private static Method getMethod(Class<?> thisClass, String methodName,
      Class<?>... paramClasses) {
    try {
      return thisClass.getMethod(methodName, paramClasses);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean isConstant(Expression e) {
    Expression e2 =
        e.accept(
            new DeterministicCodeOptimizer(ClassDeclarationFinder.create()));
    return !e.equals(e2);
  }

  @Test public void testConstantIsConstant() {
    // Small expressions are atomic.
    assertThat(isAtomic(Expressions.constant(0)), is(true));
    assertThat(isAtomic(Expressions.constant("xxx")), is(true));
    assertThat(isAtomic(Expressions.constant(null)), is(true));

    Expression e =
        Expressions.call(getMethod(Integer.class, "valueOf", int.class),
            Expressions.constant(-100));
    assertThat(isAtomic(e), is(false));
    assertThat(isConstant(e), is(true));

    e = Expressions.call(
        Integer.class, "valueOf", Expressions.constant(0));
    assertThat(isAtomic(e), is(false));
    assertThat(isConstant(e), is(true));

    e = Expressions.call(Expressions.constant("xxx"), "length");
    assertThat(isAtomic(e), is(false));
    assertThat(isConstant(e), is(true));
  }

  @Test public void testFactorOutBinaryAdd() {
    assertThat(
        optimize(
            Expressions.new_(
                Runnable.class,
                Collections.<Expression>emptyList(),
                Expressions.methodDecl(
                    0,
                    int.class,
                    "test",
                    Collections.<ParameterExpression>emptyList(),
                    Blocks.toFunctionBlock(Expressions.add(ONE, TWO))))),
        equalTo(
            "{\n"
            + "  return new Runnable(){\n"
            + "      int test() {\n"
            + "        return $L4J$C$1_2;\n"
            + "      }\n"
            + "\n"
            + "      static final int $L4J$C$1_2 = 1 + 2;\n"
            + "    };\n"
            + "}\n"));
  }

  @Test public void testFactorOutBinaryAddSurvivesMultipleOptimizations() {
    assertThat(
        optimize(optimizeExpression(
            Expressions.new_(
                Runnable.class,
                Collections.<Expression>emptyList(),
                Expressions.methodDecl(
                    0,
                    int.class,
                    "test",
                    Collections.<ParameterExpression>emptyList(),
                    Blocks.toFunctionBlock(Expressions.add(ONE, TWO)))))),
        equalTo(
            "{\n"
            + "  return new Runnable(){\n"
            + "      int test() {\n"
            + "        return $L4J$C$1_2;\n"
            + "      }\n"
            + "\n"
            + "      static final int $L4J$C$1_2 = 1 + 2;\n"
            + "    };\n"
            + "}\n"));
  }

  @Test public void testFactorOutBinaryAddNameCollision() {
    assertThat(
        optimize(
            Expressions.new_(
                Runnable.class,
                Collections.<Expression>emptyList(),
                Expressions.methodDecl(
                    0,
                    int.class,
                    "test",
                    Collections.<ParameterExpression>emptyList(),
                    Blocks.toFunctionBlock(Expressions.multiply(
                        Expressions.add(ONE, TWO),
                        Expressions.subtract(ONE, TWO)))))),
        equalTo(
            "{\n"
            + "  return new Runnable(){\n"
            + "      int test() {\n"
            + "        return $L4J$C$1_2_1_20;\n"
            + "      }\n"
            + "\n"
            + "      static final int $L4J$C$1_2 = 1 + 2;\n"
            + "      static final int $L4J$C$1_20 = 1 - 2;\n"
            + "      static final int $L4J$C$1_2_1_20 = $L4J$C$1_2 * $L4J$C$1_20;\n"
            + "    };\n"
            + "}\n"));
  }

  @Test public void testFactorOutBinaryAddMul() {
    assertThat(
        optimize(
            Expressions.new_(
                Runnable.class,
                Collections.<Expression>emptyList(),
                Expressions.methodDecl(
                    0,
                    int.class,
                    "test",
                    Collections.<ParameterExpression>emptyList(),
                    Blocks.toFunctionBlock(
                        Expressions.multiply(Expressions.add(ONE, TWO),
                            THREE))))),
      equalTo("{\n"
          + "  return new Runnable(){\n"
          + "      int test() {\n"
          + "        return $L4J$C$1_2_3;\n"
          + "      }\n"
          + "\n"
          + "      static final int $L4J$C$1_2 = 1 + 2;\n"
          + "      static final int $L4J$C$1_2_3 = $L4J$C$1_2 * 3;\n"
          + "    };\n"
          + "}\n"));
  }

  @Test public void testFactorOutNestedClasses() {
    assertThat(
        optimize(
            Expressions.new_(
                Runnable.class,
                Collections.<Expression>emptyList(),
                Expressions.methodDecl(
                    0,
                    int.class,
                    "test",
                    Collections.<ParameterExpression>emptyList(),
                    Blocks.toFunctionBlock(
                        Expressions.add(
                            Expressions.add(ONE, FOUR),
                            Expressions.call(
                                Expressions.new_(
                                    Callable.class,
                                    Collections.<Expression>emptyList(),
                                    Expressions.methodDecl(
                                        0,
                                        Object.class,
                                        "call",
                                        Collections
                                            .<ParameterExpression>emptyList(),
                                        Blocks.toFunctionBlock(
                                            Expressions.multiply(
                                                Expressions.add(ONE, TWO),
                                                THREE)))),
                                "call",
                                Collections.<Expression>emptyList())))))),
        equalTo(
            "{\n"
            + "  return new Runnable(){\n"
            + "      int test() {\n"
            + "        return $L4J$C$1_4 + new java.util.concurrent.Callable(){\n"
            + "            Object call() {\n"
            + "              return $L4J$C$1_2_3;\n"
            + "            }\n"
            + "\n"
            + "            static final int $L4J$C$1_2 = 1 + 2;\n"
            + "            static final int $L4J$C$1_2_3 = $L4J$C$1_2 * 3;\n"
            + "          }.call();\n"
            + "      }\n"
            + "\n"
            + "      static final int $L4J$C$1_4 = 1 + 4;\n"
            + "    };\n"
            + "}\n"));
  }

  @Test public void testNewBigInteger() {
    assertThat(
        optimize(
            Expressions.new_(
                Runnable.class,
                Collections.<Expression>emptyList(),
                Expressions.methodDecl(
                    0, int.class, "test",
                    Collections.<ParameterExpression>emptyList(),
                    Blocks.toFunctionBlock(
                        Expressions.new_(BigInteger.class,
                            Expressions.constant("42")))))),
        equalTo("{\n"
            + "  return new Runnable(){\n"
            + "      int test() {\n"
            + "        return $L4J$C$new_java_math_BigInteger_42_;\n"
            + "      }\n"
            + "\n"
            + "      static final java.math.BigInteger "
            + "$L4J$C$new_java_math_BigInteger_42_ = new java.math.BigInteger(\n"
            + "        \"42\");\n"
            + "    };\n"
            + "}\n"));
  }

  @Test public void testInstanceofTest() {
    // Single instanceof is not optimized
    assertThat(
        optimize(
            Expressions.new_(
                Runnable.class,
                Collections.<Expression>emptyList(),
                Expressions.methodDecl(
                    0, int.class, "test",
                    Collections.<ParameterExpression>emptyList(),
                    Blocks.toFunctionBlock(
                        Expressions.typeIs(ONE, Boolean.class))))),
        equalTo(
            "{\n"
            + "  return new Runnable(){\n"
            + "      int test() {\n"
            + "        return 1 instanceof Boolean;\n"
            + "      }\n"
            + "\n"
            + "    };\n"
            + "}\n"));
  }

  @Test public void testInstanceofComplexTest() {
    // instanceof is optimized in complex expressions
    assertThat(
        optimize(
            Expressions.new_(Runnable.class,
                Collections.<Expression>emptyList(),
                Expressions.methodDecl(0, int.class, "test",
                    Collections.<ParameterExpression>emptyList(),
                    Blocks.toFunctionBlock(
                        Expressions.orElse(
                            Expressions.typeIs(ONE, Boolean.class),
                            Expressions.typeIs(TWO, Integer.class)))))),
        equalTo("{\n"
            + "  return new Runnable(){\n"
            + "      int test() {\n"
            + "        return $L4J$C$1_instanceof_Boolean_2_instanceof_Integer;\n"
            + "      }\n"
            + "\n"
            + "      static final boolean "
            + "$L4J$C$1_instanceof_Boolean_2_instanceof_Integer = 1 instanceof "
            + "Boolean || 2 instanceof Integer;\n"
            + "    };\n"
            + "}\n"));
  }

  @Test public void testIntegerValueOfZeroComplexTest() {
    // Integer.valueOf(0) is optimized in complex expressions
    assertThat(
        optimize(
            Expressions.new_(Runnable.class,
                Collections.<Expression>emptyList(),
                Expressions.methodDecl(0, int.class, "test",
                    Collections.<ParameterExpression>emptyList(),
                    Blocks.toFunctionBlock(
                        Expressions.call(
                            getMethod(Integer.class, "valueOf", int.class),
                            Expressions.constant(0)))))),
        equalTo("{\n"
            + "  return new Runnable(){\n"
            + "      int test() {\n"
            + "        return $L4J$C$Integer_valueOf_0_;\n"
            + "      }\n"
            + "\n"
            + "      static final Integer $L4J$C$Integer_valueOf_0_ = Integer.valueOf(0);\n"
            + "    };\n"
            + "}\n"));
  }

  @Test public void testStaticField() {
    // instanceof is optimized in complex expressions
    assertThat(
        optimize(
            Expressions.new_(Runnable.class,
                Collections.<Expression>emptyList(),
                Expressions.methodDecl(0, int.class, "test",
                    Collections.<ParameterExpression>emptyList(),
                    Blocks.toFunctionBlock(
                        Expressions.call(
                            Expressions.field(null, BigInteger.class, "ONE"),
                            "add",
                            Expressions.call(null,
                                Types.lookupMethod(BigInteger.class, "valueOf",
                                    long.class),
                                Expressions.constant(42L))))))),
        equalTo(
            "{\n"
            + "  return new Runnable(){\n"
            + "      int test() {\n"
            + "        return "
            + "$L4J$C$java_math_BigInteger_ONE_add_java_math_BigInteger_valueOf_42L_;\n"
            + "      }\n"
            + "\n"
            + "      static final java.math.BigInteger "
            + "$L4J$C$java_math_BigInteger_valueOf_42L_ = java.math.BigInteger"
            + ".valueOf(42L);\n"
            + "      static final java.math.BigInteger "
            + "$L4J$C$java_math_BigInteger_ONE_add_java_math_BigInteger_valueOf_42L_ = java.math.BigInteger.ONE.add($L4J$C$java_math_BigInteger_valueOf_42L_);\n"
            + "    };\n"
            + "}\n"));
  }

  @Test public void testBigIntegerValueOf() {
    // instanceof is optimized in complex expressions
    assertThat(
        optimize(
            Expressions.new_(Runnable.class,
                Collections.<Expression>emptyList(),
                Expressions.methodDecl(0, int.class, "test",
                    Collections.<ParameterExpression>emptyList(),
                    Blocks.toFunctionBlock(
                        Expressions.call(
                            Expressions.call(null,
                                Types.lookupMethod(BigInteger.class, "valueOf",
                                    long.class),
                                Expressions.constant(42L)),
                            "add",
                            Expressions.call(null,
                                Types.lookupMethod(BigInteger.class, "valueOf",
                                    long.class),
                                Expressions.constant(42L))))))),
        equalTo("{\n"
            + "  return new Runnable(){\n"
            + "      int test() {\n"
            + "        return "
            + "$L4J$C$java_math_BigInteger_valueOf_42L_add_java_math_BigInteger_valued8d57d69;\n"
            + "      }\n"
            + "\n"
            + "      static final java.math.BigInteger "
            + "$L4J$C$java_math_BigInteger_valueOf_42L_ = java.math.BigInteger"
            + ".valueOf(42L);\n"
            + "      static final java.math.BigInteger "
            + "$L4J$C$java_math_BigInteger_valueOf_42L_add_java_math_BigInteger_valued8d57d69 = $L4J$C$java_math_BigInteger_valueOf_42L_.add($L4J$C$java_math_BigInteger_valueOf_42L_);\n"
            + "    };\n"
            + "}\n"));
  }

  @Test public void testDeterministicMethodCall() {
    assertThat(
        optimize(
            Expressions.new_(
                Runnable.class,
                Collections.<Expression>emptyList(),
                Expressions.methodDecl(
                    0,
                    int.class,
                    "test",
                    Collections.<ParameterExpression>emptyList(),
                    Blocks.toFunctionBlock(Expressions.call(null,
                        Types.lookupMethod(TestClass.class,
                            "deterministic", int.class), ONE))))),
        equalTo(
            "{\n"
            + "  return new Runnable(){\n"
            + "      int test() {\n"
            + "        return $L4J$C$net_hydromatic_linq4j_test_DeterministicTest_TestClass_determin1da033bf;\n"
            + "      }\n"
            + "\n"
            + "      static final int $L4J$C$net_hydromatic_linq4j_test_DeterministicTest_TestClass_determin1da033bf = net.hydromatic.linq4j.test.DeterministicTest.TestClass.deterministic(1);\n"
            + "    };\n"
            + "}\n"));
  }

  @Test public void testNonDeterministicMethodCall() {
    assertThat(
        optimize(
            Expressions.new_(
                Runnable.class,
                Collections.<Expression>emptyList(),
                Expressions.methodDecl(
                    0,
                    int.class,
                    "test",
                    Collections.<ParameterExpression>emptyList(),
                    Blocks.toFunctionBlock(Expressions.call(null,
                        Types.lookupMethod(TestClass.class,
                            "nonDeterministic", int.class), ONE))))),
        equalTo(
            "{\n"
            + "  return new Runnable(){\n"
            + "      int test() {\n"
            + "        return net.hydromatic.linq4j.test.DeterministicTest.TestClass.nonDeterministic(1);\n"
            + "      }\n"
            + "\n"
            + "    };\n"
            + "}\n"));
  }

  @Test public void testDeterministicClassDefaultMethod() {
    assertThat(
        optimize(
            Expressions.new_(
                Runnable.class,
                Collections.<Expression>emptyList(),
                Expressions.methodDecl(
                    0,
                    int.class,
                    "test",
                    Collections.<ParameterExpression>emptyList(),
                    Blocks.toFunctionBlock(Expressions.call(null,
                        Types.lookupMethod(TestDeterministicClass.class,
                            "deterministic", int.class), ONE))))),
        equalTo(
            "{\n"
            + "  return new Runnable(){\n"
            + "      int test() {\n"
            + "        return $L4J$C$net_hydromatic_linq4j_test_DeterministicTest_TestDeterministicCa1bc6d17;\n"
            + "      }\n"
            + "\n"
            + "      static final int $L4J$C$net_hydromatic_linq4j_test_DeterministicTest_TestDeterministicCa1bc6d17 = net.hydromatic.linq4j.test.DeterministicTest.TestDeterministicClass.deterministic(1);\n"
            + "    };\n"
            + "}\n"));
  }

  @Test public void testDeterministicClassNonDeterministicMethod() {
    assertThat(
        optimize(
            Expressions.new_(
                Runnable.class,
                Collections.<Expression>emptyList(),
                Expressions.methodDecl(
                    0,
                    int.class,
                    "test",
                    Collections.<ParameterExpression>emptyList(),
                    Blocks.toFunctionBlock(Expressions.call(null,
                        Types.lookupMethod(TestDeterministicClass.class,
                            "nonDeterministic", int.class), ONE))))),
        equalTo(
            "{\n"
            + "  return new Runnable(){\n"
            + "      int test() {\n"
            + "        return net.hydromatic.linq4j.test.DeterministicTest.TestDeterministicClass.nonDeterministic(1);\n"
            + "      }\n"
            + "\n"
            + "    };\n"
            + "}\n"));
  }
}

// End DeterministicTest.java
