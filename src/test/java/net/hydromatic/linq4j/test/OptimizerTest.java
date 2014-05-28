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

import net.hydromatic.linq4j.Linq4j;
import net.hydromatic.linq4j.expressions.*;

import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import static net.hydromatic.linq4j.test.BlockBuilderBase.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

/**
 * Unit test for {@link net.hydromatic.linq4j.expressions.BlockBuilder}
 * optimization capabilities.
 */
public class OptimizerTest {
  @Test public void testOptimizeComparison() {
    assertEquals("{\n  return true;\n}\n",
        optimize(Expressions.equal(ONE, ONE)));
  }

  @Test public void testOptimizeTernaryAlwaysTrue() {
    // true ? 1 : 2
    assertEquals("{\n  return 1;\n}\n",
        optimize(Expressions.condition(TRUE, ONE, TWO)));
  }

  @Test public void testOptimizeTernaryAlwaysFalse() {
    // false ? 1 : 2
    assertEquals("{\n  return 2;\n}\n",
        optimize(Expressions.condition(FALSE, ONE, TWO)));
  }

  @Test public void testOptimizeTernaryAlwaysSame() {
    // bool ? 1 : 1
    assertEquals("{\n  return 1;\n}\n",
        optimize(
            Expressions.condition(
                Expressions.parameter(boolean.class, "bool"), ONE, ONE)));
  }

  @Test public void testNonOptimizableTernary() {
    // bool ? 1 : 2
    assertEquals("{\n  return bool ? 1 : 2;\n}\n",
        optimize(
            Expressions.condition(
                Expressions.parameter(boolean.class, "bool"), ONE, TWO)));
  }

  @Test public void testOptimizeTernaryRotateNot() {
    // !bool ? 1 : 2
    assertEquals("{\n  return bool ? 2 : 1;\n}\n",
        optimize(
            Expressions.condition(
                Expressions.not(Expressions.parameter(boolean.class, "bool")),
                ONE, TWO)));
  }

  @Test public void testOptimizeTernaryRotateEqualFalse() {
    // bool == false ? 1 : 2
    assertEquals("{\n  return bool ? 2 : 1;\n}\n",
        optimize(
            Expressions.condition(
                Expressions.equal(Expressions.parameter(boolean.class, "bool"),
                    FALSE),
                ONE, TWO)));
  }

  @Test public void testOptimizeTernaryAtrueB() {
    // a ? true : b  === a || b
    assertEquals("{\n  return a || b;\n}\n",
        optimize(
            Expressions.condition(
                Expressions.parameter(boolean.class, "a"),
                TRUE, Expressions.parameter(boolean.class, "b"))));
  }

  @Test public void testOptimizeTernaryAtrueNull() {
    // a ? Boolean.TRUE : null  === a ? Boolean.TRUE : (Boolean) null
    assertEquals("{\n  return a ? Boolean.TRUE : (Boolean) null;\n}\n",
        optimize(
            Expressions.condition(
                Expressions.parameter(boolean.class, "a"),
                TRUE_B, Expressions.constant(null, Boolean.class))));
  }

  @Test public void testOptimizeTernaryAtrueBoxed() {
    // a ? Boolean.TRUE : Boolean.valueOf(b)  === a || b
    assertEquals("{\n  return a || Boolean.valueOf(b);\n}\n",
        optimize(Expressions.condition(
            Expressions.parameter(boolean.class, "a"),
            TRUE_B, Expressions.call(Boolean.class, "valueOf",
            Expressions.parameter(boolean.class, "b")))));
  }

  @Test public void testOptimizeTernaryABtrue() {
    // a ? b : true  === !a || b
    assertEquals("{\n  return !a || b;\n}\n",
        optimize(Expressions.condition(
            Expressions.parameter(boolean.class, "a"),
            Expressions.parameter(boolean.class, "b"), TRUE)));
  }

  @Test public void testOptimizeTernaryAfalseB() {
    // a ? false : b === !a && b
    assertEquals("{\n  return !a && b;\n}\n",
        optimize(Expressions.condition(
            Expressions.parameter(boolean.class, "a"),
            FALSE, Expressions.parameter(boolean.class, "b"))));
  }

  @Test public void testOptimizeTernaryABfalse() {
    // a ? b : false === a && b
    assertEquals("{\n  return a && b;\n}\n",
        optimize(Expressions.condition(
            Expressions.parameter(boolean.class, "a"),
            Expressions.parameter(boolean.class, "b"), FALSE)));
  }

  @Test public void testOptimizeTernaryInEqualABCeqB() {
    // (v ? (Integer) null : inp0_) == null
    assertEquals("{\n  return v || inp0_ == null;\n}\n",
        optimize(Expressions.equal(Expressions.condition(
            Expressions.parameter(boolean.class, "v"),
            NULL_INTEGER, Expressions.parameter(Integer.class, "inp0_")),
            NULL)));
  }

  @Test public void testOptimizeTernaryInEqualABCeqC() {
    // (v ? inp0_ : (Integer) null) == null
    assertEquals("{\n  return !v || inp0_ == null;\n}\n",
        optimize(Expressions.equal(Expressions.condition(
            Expressions.parameter(boolean.class, "v"),
            Expressions.parameter(Integer.class, "inp0_"), NULL_INTEGER),
            NULL)));
  }

  @Test public void testOptimizeTernaryAeqBBA() {
    // a == b ? b : a
    ParameterExpression a = Expressions.parameter(boolean.class, "a");
    ParameterExpression b = Expressions.parameter(boolean.class, "b");
    assertEquals("{\n  return a;\n}\n",
        optimize(Expressions.condition(Expressions.equal(a, b), b, a)));
  }

  @Test public void testOptimizeTernaryAeqBAB() {
    // a == b ? a : b
    ParameterExpression a = Expressions.parameter(boolean.class, "a");
    ParameterExpression b = Expressions.parameter(boolean.class, "b");
    assertEquals("{\n  return b;\n}\n",
        optimize(Expressions.condition(Expressions.equal(a, b), a, b)));
  }

  @Test public void testOptimizeTernaryInEqualABCneqB() {
    // (v ? (Integer) null : inp0_) != null
    assertEquals("{\n  return !(v || inp0_ == null);\n}\n",
        optimize(Expressions.notEqual(Expressions.condition(
            Expressions.parameter(boolean.class, "v"),
            NULL_INTEGER, Expressions.parameter(Integer.class, "inp0_")),
            NULL)));
  }

  @Test public void testOptimizeTernaryInEqualABCneqC() {
    // (v ? inp0_ : (Integer) null) != null
    assertEquals("{\n  return !(!v || inp0_ == null);\n}\n",
        optimize(Expressions.notEqual(Expressions.condition(
            Expressions.parameter(boolean.class, "v"),
            Expressions.parameter(Integer.class, "inp0_"), NULL_INTEGER),
            NULL)));
  }

  @Test public void testOptimizeTernaryAneqBBA() {
    // a != b ? b : a
    ParameterExpression a = Expressions.parameter(boolean.class, "a");
    ParameterExpression b = Expressions.parameter(boolean.class, "b");
    assertEquals("{\n  return b;\n}\n",
        optimize(Expressions.condition(Expressions.notEqual(a, b), b, a)));
  }

  @Test public void testOptimizeTernaryAneqBAB() {
    // a != b ? a : b
    ParameterExpression a = Expressions.parameter(boolean.class, "a");
    ParameterExpression b = Expressions.parameter(boolean.class, "b");
    assertEquals("{\n  return a;\n}\n",
        optimize(Expressions.condition(Expressions.notEqual(a, b), a, b)));
  }

  @Test public void testAndAlsoTrueBool() {
    // true && bool
    assertEquals("{\n  return bool;\n}\n",
        optimize(
            Expressions.andAlso(TRUE,
                Expressions.parameter(boolean.class, "bool"))));
  }

  @Test public void testAndAlsoBoolTrue() {
    // bool && true
    assertEquals("{\n  return bool;\n}\n",
        optimize(
            Expressions.andAlso(
                Expressions.parameter(boolean.class, "bool"), TRUE)));
  }

  @Test public void testAndAlsoFalseBool() {
    // false && bool
    assertEquals("{\n  return false;\n}\n",
        optimize(
            Expressions.andAlso(FALSE,
                Expressions.parameter(boolean.class, "bool"))));
  }

  @Test public void testAndAlsoNullBool() {
    // null && bool
    assertEquals("{\n  return null && bool;\n}\n",
        optimize(Expressions.andAlso(NULL,
            Expressions.parameter(boolean.class,
                "bool"))));
  }

  @Test public void testAndAlsoXY() {
    // x && y
    assertEquals("{\n  return x && y;\n}\n",
        optimize(
            Expressions.andAlso(
                Expressions.parameter(boolean.class, "x"),
                Expressions.parameter(boolean.class, "y"))));
  }

  @Test public void testAndAlsoXX() {
    // x && x
    ParameterExpression x = Expressions.parameter(boolean.class, "x");
    assertEquals("{\n  return x;\n}\n",
        optimize(Expressions.andAlso(x, x)));
  }

  @Test public void testOrElseTrueBool() {
    // true || bool
    assertEquals("{\n  return true;\n}\n",
        optimize(
            Expressions.orElse(TRUE,
                Expressions.parameter(boolean.class, "bool"))));
  }

  @Test public void testOrElseFalseBool() {
    // false || bool
    assertEquals("{\n  return bool;\n}\n",
        optimize(
            Expressions.orElse(FALSE,
                Expressions.parameter(boolean.class, "bool"))));
  }

  @Test public void testOrElseNullBool() {
    // null || bool
    assertEquals("{\n  return null || bool;\n}\n",
        optimize(Expressions.orElse(NULL,
            Expressions.parameter(boolean.class,
                "bool"))));
  }

  @Test public void testOrElseXY() {
    // x || y
    assertEquals("{\n  return x || y;\n}\n",
        optimize(
            Expressions.orElse(
                Expressions.parameter(boolean.class, "x"),
                Expressions.parameter(boolean.class, "y"))));
  }

  @Test public void testOrElseXX() {
    // x || x
    ParameterExpression x = Expressions.parameter(boolean.class, "x");
    assertEquals("{\n  return x;\n}\n", optimize(Expressions.orElse(x, x)));
  }

  @Test public void testEqualSameConst() {
    // 1 == 1
    assertEquals("{\n  return true;\n}\n",
        optimize(Expressions.equal(ONE, Expressions.constant(1))));
  }

  @Test public void testEqualDifferentConst() {
    // 1 == 2
    assertEquals("{\n  return false;\n}\n",
        optimize(Expressions.equal(ONE, TWO)));
  }

  @Test public void testEqualSameExpr() {
    // x == x
    ParameterExpression x = Expressions.parameter(int.class, "x");
    assertEquals("{\n  return true;\n}\n", optimize(Expressions.equal(x, x)));
  }

  @Test public void testEqualDifferentExpr() {
    // x == y
    ParameterExpression x = Expressions.parameter(int.class, "x");
    ParameterExpression y = Expressions.parameter(int.class, "y");
    assertEquals("{\n  return x == y;\n}\n", optimize(Expressions.equal(x, y)));
  }

  @Test public void testEqualPrimitiveNull() {
    // (int) x == null
    ParameterExpression x = Expressions.parameter(int.class, "x");
    assertEquals("{\n  return false;\n}\n",
        optimize(Expressions.equal(x, NULL)));
  }

  @Test public void testEqualObjectNull() {
    // (Integer) x == null
    ParameterExpression x = Expressions.parameter(Integer.class, "x");
    assertEquals("{\n  return x == null;\n}\n",
        optimize(Expressions.equal(x,
            NULL)));
  }

  @Test public void testEqualStringNull() {
    // "Y" == null
    assertEquals("{\n  return false;\n}\n",
        optimize(Expressions.equal(Expressions.constant("Y"), NULL)));
  }

  @Test public void testEqualTypedNullUntypedNull() {
    // (Integer) null == null
    assertEquals("{\n  return true;\n}\n",
        optimize(Expressions.equal(NULL_INTEGER, NULL)));
  }

  @Test public void testEqualUnypedNullTypedNull() {
    // null == (Integer) null
    assertEquals("{\n  return true;\n}\n",
        optimize(Expressions.equal(NULL, NULL_INTEGER)));
  }

  @Test public void testEqualBoolTrue() {
    // x == true
    ParameterExpression x = Expressions.parameter(boolean.class, "x");
    assertEquals("{\n  return x;\n}\n", optimize(Expressions.equal(x, TRUE)));
  }

  @Test public void testEqualBoolFalse() {
    // x == false
    ParameterExpression x = Expressions.parameter(boolean.class, "x");
    assertEquals("{\n  return !x;\n}\n", optimize(Expressions.equal(x, FALSE)));
  }

  @Test public void testNotEqualSameConst() {
    // 1 != 1
    assertEquals("{\n  return false;\n}\n",
        optimize(Expressions.notEqual(ONE, Expressions.constant(1))));
  }

  @Test public void testNotEqualDifferentConst() {
    // 1 != 2
    assertEquals("{\n  return true;\n}\n",
        optimize(Expressions.notEqual(ONE, TWO)));
  }

  @Test public void testNotEqualSameExpr() {
    // x != x
    ParameterExpression x = Expressions.parameter(int.class, "x");
    assertEquals("{\n  return false;\n}\n",
        optimize(Expressions.notEqual(x, x)));
  }

  @Test public void testNotEqualDifferentExpr() {
    // x != y
    ParameterExpression x = Expressions.parameter(int.class, "x");
    ParameterExpression y = Expressions.parameter(int.class, "y");
    assertEquals("{\n  return x != y;\n}\n",
        optimize(Expressions.notEqual(x, y)));
  }

  @Test public void testNotEqualPrimitiveNull() {
    // (int) x == null
    ParameterExpression x = Expressions.parameter(int.class, "x");
    assertEquals("{\n  return true;\n}\n",
        optimize(Expressions.notEqual(x, NULL)));
  }

  @Test public void testNotEqualObjectNull() {
    // (Integer) x == null
    ParameterExpression x = Expressions.parameter(Integer.class, "x");
    assertEquals("{\n  return x != null;\n}\n",
        optimize(Expressions.notEqual(
            x, NULL)));
  }

  @Test public void testNotEqualStringNull() {
    // "Y" != null
    assertEquals("{\n  return true;\n}\n",
        optimize(Expressions.notEqual(Expressions.constant("Y"), NULL)));
  }

  @Test public void testNotEqualTypedNullUntypedNull() {
    // (Integer) null != null
    assertEquals("{\n  return false;\n}\n",
        optimize(Expressions.notEqual(NULL_INTEGER, NULL)));
  }

  @Test public void testNotEqualUnypedNullTypedNull() {
    // null != (Integer) null
    assertEquals("{\n  return false;\n}\n",
        optimize(Expressions.notEqual(NULL, NULL_INTEGER)));
  }

  @Test public void testNotEqualBoolTrue() {
    // x != true
    ParameterExpression x = Expressions.parameter(boolean.class, "x");
    assertEquals("{\n  return !x;\n}\n",
        optimize(Expressions.notEqual(x, TRUE)));
  }

  @Test public void testNotEqualBoolFalse() {
    // x != false
    ParameterExpression x = Expressions.parameter(boolean.class, "x");
    assertEquals("{\n  return x;\n}\n",
        optimize(Expressions.notEqual(x, FALSE)));
  }

  @Test public void testMultipleFolding() {
    // (1 == 2 ? 3 : 4) != (5 != 6 ? 4 : 8) ? 9 : 10
    assertEquals("{\n  return 10;\n}\n",
        optimize(
            Expressions.condition(
                Expressions.notEqual(
                    Expressions.condition(Expressions.equal(ONE, TWO),
                        Expressions.constant(3), Expressions.constant(4)),
                    Expressions.condition(
                        Expressions.notEqual(
                            Expressions.constant(5), Expressions.constant(6)),
                        Expressions.constant(4), Expressions.constant(8))),
                Expressions.constant(9),
                Expressions.constant(10))));
  }

  @Test public void testConditionalIfTrue() {
    // if (true) {return 1}
    assertEquals("{\n  return 1;\n}\n",
        optimize(Expressions.ifThen(TRUE, Expressions.return_(null, ONE))));
  }

  @Test public void testConditionalIfTrueElse() {
    // if (true) {return 1} else {return 2}
    assertEquals("{\n  return 1;\n}\n",
        optimize(
            Expressions.ifThenElse(TRUE,
                Expressions.return_(null, ONE),
                Expressions.return_(null, TWO))));
  }

  @Test public void testConditionalIfFalse() {
    // if (false) {return 1}
    assertEquals("{}",
        optimize(Expressions.ifThen(FALSE, Expressions.return_(null, ONE))));
  }

  @Test public void testConditionalIfFalseElse() {
    // if (false) {return 1} else {return 2}
    assertEquals("{\n  return 2;\n}\n",
        optimize(
            Expressions.ifThenElse(FALSE,
                Expressions.return_(null, ONE),
                Expressions.return_(null, TWO))));
  }

  @Test public void testConditionalIfBoolTrue() {
    // if (bool) {return 1} else if (true) {return 2}
    Expression bool = Expressions.parameter(boolean.class, "bool");
    assertEquals(
        "{\n"
        + "  if (bool) {\n"
        + "    return 1;\n"
        + "  } else {\n"
        + "    return 2;\n"
        + "  }\n"
        + "}\n",
        optimize(
            Expressions.ifThenElse(bool,
                Expressions.return_(null, ONE),
                TRUE,
                Expressions.return_(null, TWO))));
  }

  @Test public void testConditionalIfBoolTrueElse() {
    // if (bool) {return 1} else if (true) {return 2} else {return 3}
    Expression bool = Expressions.parameter(boolean.class, "bool");
    assertEquals(
        "{\n"
        + "  if (bool) {\n"
        + "    return 1;\n"
        + "  } else {\n"
        + "    return 2;\n"
        + "  }\n"
        + "}\n",
        optimize(
            Expressions.ifThenElse(bool,
                Expressions.return_(null, ONE),
                TRUE,
                Expressions.return_(null, TWO),
                Expressions.return_(null, THREE))));
  }

  @Test public void testConditionalIfBoolFalse() {
    // if (bool) {return 1} else if (false) {return 2}
    Expression bool = Expressions.parameter(boolean.class, "bool");
    assertEquals(
        "{\n"
        + "  if (bool) {\n"
        + "    return 1;\n"
        + "  }\n"
        + "}\n",
        optimize(
            Expressions.ifThenElse(bool,
                Expressions.return_(null, ONE),
                FALSE,
                Expressions.return_(null, TWO))));
  }

  @Test public void testConditionalIfBoolFalseElse() {
    // if (bool) {return 1} else if (false) {return 2} else {return 3}
    Expression bool = Expressions.parameter(boolean.class, "bool");
    assertEquals(
        "{\n"
        + "  if (bool) {\n"
        + "    return 1;\n"
        + "  } else {\n"
        + "    return 3;\n"
        + "  }\n"
        + "}\n",
        optimize(
            Expressions.ifThenElse(bool,
                Expressions.return_(null, ONE),
                FALSE,
                Expressions.return_(null, TWO),
                Expressions.return_(null, THREE))));
  }

  @Test public void testConditionalIfBoolFalseTrue() {
    // if (bool) {1} else if (false) {2} if (true) {4} else {5}
    Expression bool = Expressions.parameter(boolean.class, "bool");
    assertEquals(
        "{\n"
        + "  if (bool) {\n"
        + "    return 1;\n"
        + "  } else {\n"
        + "    return 4;\n"
        + "  }\n"
        + "}\n",
        optimize(
            Expressions.ifThenElse(bool,
                Expressions.return_(null, ONE),
                FALSE,
                Expressions.return_(null, TWO),
                TRUE,
                Expressions.return_(null, FOUR),
                Expressions.return_(null, Expressions.constant(5)))));
  }

  @Test public void testCastIntToShort() {
    // return (short) 1 --> return (short) 1
    assertEquals("{\n  return (short)1;\n}\n",
        optimize(Expressions.convert_(ONE, short.class)));
  }

  @Test public void testCastIntToInt() {
    // return (int) 1 --> return 1L
    assertEquals("{\n  return 1;\n}\n",
        optimize(Expressions.convert_(ONE, int.class)));
  }

  @Test public void testCastIntToLong() {
    // return (long) 1 --> return 1L
    assertEquals("{\n  return 1L;\n}\n",
        optimize(Expressions.convert_(ONE, long.class)));
  }

  @Test public void testNotTrue() {
    // !true -> false
    assertEquals("{\n  return false;\n}\n", optimize(Expressions.not(TRUE)));
  }

  @Test public void testNotFalse() {
    // !false -> true
    assertEquals("{\n  return true;\n}\n", optimize(Expressions.not(FALSE)));
  }

  @Test public void testNotNotA() {
    // !!a -> a
    assertEquals("{\n  return a;\n}\n",
        optimize(Expressions.not(Expressions.not(bool("a")))));
  }

  @Test public void testNotEq() {
    // !(a == b) -> a != b
    assertEquals("{\n  return a != b;\n}\n",
        optimize(Expressions.not(Expressions.equal(bool("a"), bool("b")))));
  }

  @Test public void testNotNeq() {
    // !(a != b) -> a == b
    assertEquals("{\n  return a == b;\n}\n",
        optimize(
            Expressions.not(Expressions.notEqual(bool("a"), bool("b")))));
  }

  @Test public void testNotGt() {
    // !(a > b) -> a <= b
    assertEquals("{\n  return a <= b;\n}\n",
        optimize(
            Expressions.not(Expressions.greaterThan(bool("a"), bool("b")))));
  }

  @Test public void testNotGte() {
    // !(a >= b) -> a < b
    assertEquals("{\n  return a < b;\n}\n",
        optimize(
            Expressions.not(
                Expressions.greaterThanOrEqual(bool("a"), bool("b")))));
  }

  @Test public void testNotLt() {
    // !(a < b) -> a >= b
    assertEquals("{\n  return a >= b;\n}\n",
        optimize(
            Expressions.not(Expressions.lessThan(bool("a"), bool("b")))));
  }

  @Test public void testNotLte() {
    // !(a <= b) -> a > b
    assertEquals("{\n  return a > b;\n}\n",
        optimize(
            Expressions.not(
                Expressions.lessThanOrEqual(bool("a"), bool("b")))));
  }

  @Test public void booleanValueOfTrue() {
    // Boolean.valueOf(true) -> true
    assertEquals("{\n  return true;\n}\n",
        optimize(Expressions.call(Boolean.class, "valueOf", TRUE)));
  }

  @Test public void testBooleanValueOfFalse() {
    // Boolean.valueOf(false) -> false
    assertEquals("{\n  return false;\n}\n",
        optimize(Expressions.call(Boolean.class, "valueOf", FALSE)));
  }

  @Test public void testAssign() {
    // long x = 0;
    // final long y = System.currentTimeMillis();
    // if (System.nanoTime() > 0) {
    //   x = y;
    // }
    // System.out.println(x);
    //
    // In bug https://github.com/julianhyde/linq4j/issues/27, this was
    // incorrectly optimized to
    //
    // if (System.nanoTime() > 0L) {
    //    System.currentTimeMillis();
    // }
    // System.out.println(0L);
    final ParameterExpression x_ = Expressions.parameter(long.class, "x");
    final ParameterExpression y_ = Expressions.parameter(long.class, "y");
    final Method mT = Linq4j.getMethod("java.lang.System", "currentTimeMillis");
    final Method mNano = Linq4j.getMethod("java.lang.System", "nanoTime");
    final ConstantExpression zero = Expressions.constant(0L);
    assertThat(
        optimize(
            Expressions.block(
                Expressions.declare(0, x_, zero),
                Expressions.declare(Modifier.FINAL, y_, Expressions.call(mT)),
                Expressions.ifThen(
                    Expressions.greaterThan(Expressions.call(mNano), zero),
                    Expressions.statement(Expressions.assign(x_, y_))),
                Expressions.statement(
                    Expressions.call(
                        Expressions.field(null, System.class, "out"),
                        "println",
                        x_)))),
        equalTo(
            "{\n"
            + "  long x = 0L;\n"
            + "  if (System.nanoTime() > 0L) {\n"
            + "    x = System.currentTimeMillis();\n"
            + "  }\n"
            + "  System.out.println(x);\n"
            + "}\n"));
  }

  @Test public void testAssign2() {
    // long x = 0;
    // final long y = System.currentTimeMillis();
    // if (System.currentTimeMillis() > 0) {
    //   x = y;
    // }
    //
    // Make sure we don't fold two calls to System.currentTimeMillis into one.
    final ParameterExpression x_ = Expressions.parameter(long.class, "x");
    final ParameterExpression y_ = Expressions.parameter(long.class, "y");
    final Method mT = Linq4j.getMethod("java.lang.System", "currentTimeMillis");
    final ConstantExpression zero = Expressions.constant(0L);
    assertThat(
        optimize(
            Expressions.block(
                Expressions.declare(0, x_, zero),
                Expressions.declare(Modifier.FINAL, y_, Expressions.call(mT)),
                Expressions.ifThen(
                    Expressions.greaterThan(Expressions.call(mT), zero),
                    Expressions.statement(Expressions.assign(x_, y_))))),
        equalTo(
            "{\n"
            + "  long x = 0L;\n"
            + "  if (System.currentTimeMillis() > 0L) {\n"
            + "    x = System.currentTimeMillis();\n"
            + "  }\n"
            + "}\n"));
  }
}

// End OptimizerTest.java
