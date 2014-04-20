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

import static org.junit.Assert.assertEquals;

/**
 * Tests factoring out deterministic expressions.
 */
public class DeterministicTest extends BlockBuilderBase {
  @Test
  public void factorOutBinaryAdd() {
    assertEquals("{\n"
        + "  return new Runnable(){\n"
        + "      int test() {\n"
        + "        return 1_2_$L4J$C$;\n"
        + "      }\n"
        + "\n"
        + "      static final int 1_2_$L4J$C$ = 1 + 2;\n"
        + "    };\n"
        + "}\n", optimize(Expressions.new_(Runnable.class,
        Collections.<Expression>emptyList(),
        Expressions.methodDecl(0, int.class, "test",
            Collections.<ParameterExpression>emptyList(),
            Blocks.toFunctionBlock(Expressions.add(ONE, TWO))))));
  }

  @Test
  public void factorOutBinaryAddMul() {
    assertEquals("{\n"
        + "  return new Runnable(){\n"
        + "      int test() {\n"
        + "        return 1_2_3_$L4J$C$;\n"
        + "      }\n"
        + "\n"
        + "      static final int 1_2_$L4J$C$ = 1 + 2;\n"
        + "      static final int 1_2_3_$L4J$C$ = 1_2_$L4J$C$ * 3;\n"
        + "    };\n"
        + "}\n", optimize(Expressions.new_(Runnable.class,
        Collections.<Expression>emptyList(),
        Expressions.methodDecl(0, int.class, "test",
            Collections.<ParameterExpression>emptyList(),
            Blocks.toFunctionBlock(
                Expressions.multiply(Expressions.add(ONE, TWO), THREE))))));
  }

  @Test
  public void factorOutNestedClasses() {
    assertEquals("{\n"
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
        + "}\n", optimize(Expressions.new_(Runnable.class,
        Collections.<Expression>emptyList(),
        Expressions.methodDecl(0, int.class, "test",
            Collections.<ParameterExpression>emptyList(),
            Blocks.toFunctionBlock(
                Expressions.add(Expressions.add(ONE, FOUR),
                    Expressions.call(Expressions
                        .new_(Callable.class,
                                Collections.<Expression>emptyList(),
                                Expressions.methodDecl(0, Object.class, "call",
                                    Collections.EMPTY_LIST,
                                    Blocks.toFunctionBlock(
                                        Expressions.multiply(Expressions.add(
                                          ONE, TWO),
                                                THREE))))
                        , "call", Collections.<Expression>emptyList())))))));
  }

  @Test
  public void newBigInteger() {
    assertEquals("{\n"
        + "  return new Runnable(){\n"
        + "      int test() {\n"
        + "        return new_java_math_BigInteger_42__$L4J$C$;\n"
        + "      }\n"
        + "\n"
        + "      static final java.math.BigInteger "
        + "new_java_math_BigInteger_42__$L4J$C$ = new java.math.BigInteger(\n"
        + "        \"42\");\n"
        + "    };\n"
        + "}\n", optimize(Expressions.new_(Runnable.class,
        Collections.<Expression>emptyList(),
        Expressions.methodDecl(0, int.class, "test",
            Collections.<ParameterExpression>emptyList(),
            Blocks.toFunctionBlock(Expressions.new_(BigInteger.class,
                Expressions.constant("42")))))));
  }

  @Test
  public void instanceofTest() {
    // Single instanceof is not optimized
    assertEquals("{\n"
        + "  return new Runnable(){\n"
        + "      int test() {\n"
        + "        return 1 instanceof Boolean;\n"
        + "      }\n"
        + "\n"
        + "    };\n"
        + "}\n", optimize(Expressions.new_(Runnable.class,
        Collections.<Expression>emptyList(),
        Expressions.methodDecl(0, int.class, "test",
            Collections.<ParameterExpression>emptyList(),
            Blocks.toFunctionBlock(Expressions.typeIs(ONE, Boolean.class))))));
  }

  @Test
  public void instanceofComplexTest() {
    // instanceof is optimized in complex expressinos
    assertEquals("{\n"
        + "  return new Runnable(){\n"
        + "      int test() {\n"
        + "        return 1_instanceof_Boolean_2_instanceof_Integer_$L4J$C$;\n"
        + "      }\n"
        + "\n"
        + "      static final boolean "
        + "1_instanceof_Boolean_2_instanceof_Integer_$L4J$C$ = 1 instanceof "
        + "Boolean || 2 instanceof Integer;\n"
        + "    };\n"
        + "}\n", optimize(Expressions.new_(Runnable.class,
        Collections.<Expression>emptyList(),
        Expressions.methodDecl(0, int.class, "test",
            Collections.<ParameterExpression>emptyList(),
            Blocks.toFunctionBlock(Expressions.orElse(Expressions.typeIs(ONE,
              Boolean.class),
                    Expressions.typeIs(TWO,
                        Integer.class)))))));
  }

  @Test
  public void staticField() {
    // instanceof is optimized in complex expressinos
    assertEquals("{\n"
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
        + "}\n", optimize(Expressions.new_(Runnable.class,
        Collections.<Expression>emptyList(),
        Expressions.methodDecl(0, int.class, "test",
            Collections.<ParameterExpression>emptyList(),
            Blocks.toFunctionBlock(
                Expressions.call(Expressions.field(null, BigInteger.class,
                    "ONE"), "add", Expressions.call(null,
                    Types.lookupMethod(BigInteger.class, "valueOf",
                        long.class), Expressions.constant(42L))))))));
  }

  @Test
  public void bigIntegerValueOf() {
    // instanceof is optimized in complex expressinos
    assertEquals("{\n"
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
        + "}\n", optimize(Expressions.new_(Runnable.class,
        Collections.<Expression>emptyList(),
        Expressions.methodDecl(0, int.class, "test",
            Collections.<ParameterExpression>emptyList(),
            Blocks.toFunctionBlock(
                Expressions.call(Expressions.call(null,
                    Types.lookupMethod(BigInteger.class, "valueOf",
                        long.class), Expressions.constant(42L)), "add",
                    Expressions.call(null,
                        Types.lookupMethod(BigInteger.class, "valueOf",
                            long.class), Expressions.constant(42L))))))));
  }

}
