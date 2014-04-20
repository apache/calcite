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

import org.junit.Test;

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
            + "        return 1_2_$L4J$C$;\n"
            + "      }\n"
            + "\n"
            + "      static final int 1_2_$L4J$C$ = 1 + 2;\n"
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
          + "        return 1_2_3_$L4J$C$;\n"
          + "      }\n"
          + "\n"
          + "      static final int 1_2_$L4J$C$ = 1 + 2;\n"
          + "      static final int 1_2_3_$L4J$C$ = 1_2_$L4J$C$ * 3;\n"
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
            + "        return 1_4_$L4J$C$ + new java.util.concurrent.Callable(){\n"
            + "            Object call() {\n"
            + "              return 1_2_3_$L4J$C$;\n"
            + "            }\n"
            + "\n"
            + "            static final int 1_2_$L4J$C$ = 1 + 2;\n"
            + "            static final int 1_2_3_$L4J$C$ = 1_2_$L4J$C$ * 3;\n"
            + "          }.call();\n"
            + "      }\n"
            + "\n"
            + "      static final int 1_4_$L4J$C$ = 1 + 4;\n"
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
            + "        return new_java_math_BigInteger_42__$L4J$C$;\n"
            + "      }\n"
            + "\n"
            + "      static final java.math.BigInteger "
            + "new_java_math_BigInteger_42__$L4J$C$ = new java.math.BigInteger(\n"
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
            + "        return 1_instanceof_Boolean_2_instanceof_Integer_$L4J$C$;\n"
            + "      }\n"
            + "\n"
            + "      static final boolean "
            + "1_instanceof_Boolean_2_instanceof_Integer_$L4J$C$ = 1 instanceof "
            + "Boolean || 2 instanceof Integer;\n"
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
            + "java_math_BigInteger_ONE_add_java_math_BigInteger_valueOf_42L__$L4J$C$;\n"
            + "      }\n"
            + "\n"
            + "      static final java.math.BigInteger "
            + "java_math_BigInteger_valueOf_42L__$L4J$C$ = java.math.BigInteger"
            + ".valueOf(42L);\n"
            + "      static final java.math.BigInteger "
            + "java_math_BigInteger_ONE_add_java_math_BigInteger_valueOf_42L__$L4J$C$ = java.math.BigInteger.ONE.add(java_math_BigInteger_valueOf_42L__$L4J$C$);\n"
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
            + "java_math_BigInteger_valueOf_42L_add_java_math_BigInteger_valueOf_42L__$L4J$C$;\n"
            + "      }\n"
            + "\n"
            + "      static final java.math.BigInteger "
            + "java_math_BigInteger_valueOf_42L__$L4J$C$ = java.math.BigInteger"
            + ".valueOf(42L);\n"
            + "      static final java.math.BigInteger "
            + "java_math_BigInteger_valueOf_42L_add_java_math_BigInteger_valueOf_42L__$L4J$C$ = java_math_BigInteger_valueOf_42L__$L4J$C$.add(java_math_BigInteger_valueOf_42L__$L4J$C$);\n"
            + "    };\n"
            + "}\n"));
  }
}

// End DeterministicTest.java
