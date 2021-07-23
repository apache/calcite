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

import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import static org.apache.calcite.linq4j.test.BlockBuilderBase.FALSE;
import static org.apache.calcite.linq4j.test.BlockBuilderBase.FOUR;
import static org.apache.calcite.linq4j.test.BlockBuilderBase.NULL;
import static org.apache.calcite.linq4j.test.BlockBuilderBase.NULL_INTEGER;
import static org.apache.calcite.linq4j.test.BlockBuilderBase.ONE;
import static org.apache.calcite.linq4j.test.BlockBuilderBase.THREE;
import static org.apache.calcite.linq4j.test.BlockBuilderBase.TRUE;
import static org.apache.calcite.linq4j.test.BlockBuilderBase.TRUE_B;
import static org.apache.calcite.linq4j.test.BlockBuilderBase.TWO;
import static org.apache.calcite.linq4j.test.BlockBuilderBase.bool;
import static org.apache.calcite.linq4j.test.BlockBuilderBase.optimize;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit test for {@link org.apache.calcite.linq4j.tree.BlockBuilder}
 * optimization capabilities.
 */
class OptimizerTest {
  @Test void testOptimizeComparison() {
    assertEquals("{\n  return true;\n}\n",
        optimize(Expressions.equal(ONE, ONE)));
  }

  @Test void testOptimizeTernaryAlwaysTrue() {
    // true ? 1 : 2
    assertEquals("{\n  return 1;\n}\n",
        optimize(Expressions.condition(TRUE, ONE, TWO)));
  }

  @Test void testOptimizeTernaryAlwaysFalse() {
    // false ? 1 : 2
    assertEquals("{\n  return 2;\n}\n",
        optimize(Expressions.condition(FALSE, ONE, TWO)));
  }

  @Test void testOptimizeTernaryAlwaysSame() {
    // bool ? 1 : 1
    assertEquals("{\n  return 1;\n}\n",
        optimize(
            Expressions.condition(
                Expressions.parameter(boolean.class, "bool"), ONE, ONE)));
  }

  @Test void testNonOptimizableTernary() {
    // bool ? 1 : 2
    assertEquals("{\n  return bool ? 1 : 2;\n}\n",
        optimize(
            Expressions.condition(
                Expressions.parameter(boolean.class, "bool"), ONE, TWO)));
  }

  @Test void testOptimizeTernaryRotateNot() {
    // !bool ? 1 : 2
    assertEquals("{\n  return bool ? 2 : 1;\n}\n",
        optimize(
            Expressions.condition(
                Expressions.not(Expressions.parameter(boolean.class, "bool")),
                ONE, TWO)));
  }

  @Test void testOptimizeTernaryRotateEqualFalse() {
    // bool == false ? 1 : 2
    assertEquals("{\n  return bool ? 2 : 1;\n}\n",
        optimize(
            Expressions.condition(
                Expressions.equal(Expressions.parameter(boolean.class, "bool"),
                    FALSE),
                ONE, TWO)));
  }

  @Test void testOptimizeTernaryAtrueB() {
    // a ? true : b  === a || b
    assertEquals("{\n  return a || b;\n}\n",
        optimize(
            Expressions.condition(
                Expressions.parameter(boolean.class, "a"),
                TRUE, Expressions.parameter(boolean.class, "b"))));
  }

  @Test void testOptimizeTernaryAtrueNull() {
    // a ? Boolean.TRUE : null  === a ? Boolean.TRUE : (Boolean) null
    assertEquals("{\n  return a ? Boolean.TRUE : (Boolean) null;\n}\n",
        optimize(
            Expressions.condition(
                Expressions.parameter(boolean.class, "a"),
                TRUE_B, Expressions.constant(null, Boolean.class))));
  }

  @Test void testOptimizeTernaryAtrueBoxed() {
    // a ? Boolean.TRUE : Boolean.valueOf(b)  === a || b
    assertEquals("{\n  return a || Boolean.valueOf(b);\n}\n",
        optimize(
            Expressions.condition(Expressions.parameter(boolean.class, "a"),
                TRUE_B,
                Expressions.call(Boolean.class, "valueOf",
                    Expressions.parameter(boolean.class, "b")))));
  }

  @Test void testOptimizeTernaryABtrue() {
    // a ? b : true  === !a || b
    assertEquals("{\n  return !a || b;\n}\n",
        optimize(
            Expressions.condition(
                Expressions.parameter(boolean.class, "a"),
                Expressions.parameter(boolean.class, "b"), TRUE)));
  }

  @Test void testOptimizeTernaryAfalseB() {
    // a ? false : b === !a && b
    assertEquals("{\n  return !a && b;\n}\n",
        optimize(
            Expressions.condition(
                Expressions.parameter(boolean.class, "a"),
                FALSE, Expressions.parameter(boolean.class, "b"))));
  }

  @Test void testOptimizeTernaryABfalse() {
    // a ? b : false === a && b
    assertEquals("{\n  return a && b;\n}\n",
        optimize(
            Expressions.condition(Expressions.parameter(boolean.class, "a"),
                Expressions.parameter(boolean.class, "b"), FALSE)));
  }

  @Test void testOptimizeTernaryInEqualABCeqB() {
    // (v ? (Integer) null : inp0_) == null
    assertEquals("{\n  return v || inp0_ == null;\n}\n",
        optimize(
            Expressions.equal(
                Expressions.condition(Expressions.parameter(boolean.class, "v"),
                    NULL_INTEGER,
                    Expressions.parameter(Integer.class, "inp0_")),
            NULL)));
  }

  @Test void testOptimizeTernaryInEqualABCeqC() {
    // (v ? inp0_ : (Integer) null) == null
    assertEquals("{\n  return !v || inp0_ == null;\n}\n",
        optimize(
            Expressions.equal(
                Expressions.condition(Expressions.parameter(boolean.class, "v"),
                    Expressions.parameter(Integer.class, "inp0_"),
                    NULL_INTEGER),
            NULL)));
  }

  @Test void testOptimizeTernaryAeqBBA() {
    // a == b ? b : a
    ParameterExpression a = Expressions.parameter(boolean.class, "a");
    ParameterExpression b = Expressions.parameter(boolean.class, "b");
    assertEquals("{\n  return a;\n}\n",
        optimize(Expressions.condition(Expressions.equal(a, b), b, a)));
  }

  @Test void testOptimizeTernaryAeqBAB() {
    // a == b ? a : b
    ParameterExpression a = Expressions.parameter(boolean.class, "a");
    ParameterExpression b = Expressions.parameter(boolean.class, "b");
    assertEquals("{\n  return b;\n}\n",
        optimize(Expressions.condition(Expressions.equal(a, b), a, b)));
  }

  @Test void testOptimizeTernaryInEqualABCneqB() {
    // (v ? (Integer) null : inp0_) != null
    assertEquals("{\n  return !(v || inp0_ == null);\n}\n",
        optimize(
            Expressions.notEqual(
                Expressions.condition(Expressions.parameter(boolean.class, "v"),
                    NULL_INTEGER,
                    Expressions.parameter(Integer.class, "inp0_")),
            NULL)));
  }

  @Test void testOptimizeTernaryInEqualABCneqC() {
    // (v ? inp0_ : (Integer) null) != null
    assertEquals("{\n  return !(!v || inp0_ == null);\n}\n",
        optimize(
            Expressions.notEqual(
                Expressions.condition(Expressions.parameter(boolean.class, "v"),
                    Expressions.parameter(Integer.class, "inp0_"),
                    NULL_INTEGER),
            NULL)));
  }

  @Test void testOptimizeTernaryAneqBBA() {
    // a != b ? b : a
    ParameterExpression a = Expressions.parameter(boolean.class, "a");
    ParameterExpression b = Expressions.parameter(boolean.class, "b");
    assertEquals("{\n  return b;\n}\n",
        optimize(Expressions.condition(Expressions.notEqual(a, b), b, a)));
  }

  @Test void testOptimizeTernaryAneqBAB() {
    // a != b ? a : b
    ParameterExpression a = Expressions.parameter(boolean.class, "a");
    ParameterExpression b = Expressions.parameter(boolean.class, "b");
    assertEquals("{\n  return a;\n}\n",
        optimize(Expressions.condition(Expressions.notEqual(a, b), a, b)));
  }

  @Test void testAndAlsoTrueBool() {
    // true && bool
    assertEquals("{\n  return bool;\n}\n",
        optimize(
            Expressions.andAlso(TRUE,
                Expressions.parameter(boolean.class, "bool"))));
  }

  @Test void testAndAlsoBoolTrue() {
    // bool && true
    assertEquals("{\n  return bool;\n}\n",
        optimize(
            Expressions.andAlso(
                Expressions.parameter(boolean.class, "bool"), TRUE)));
  }

  @Test void testAndAlsoFalseBool() {
    // false && bool
    assertEquals("{\n  return false;\n}\n",
        optimize(
            Expressions.andAlso(FALSE,
                Expressions.parameter(boolean.class, "bool"))));
  }

  @Test void testAndAlsoNullBool() {
    // null && bool
    assertEquals("{\n  return null && bool;\n}\n",
        optimize(
            Expressions.andAlso(NULL,
                Expressions.parameter(boolean.class, "bool"))));
  }

  @Test void testAndAlsoXY() {
    // x && y
    assertEquals("{\n  return x && y;\n}\n",
        optimize(
            Expressions.andAlso(
                Expressions.parameter(boolean.class, "x"),
                Expressions.parameter(boolean.class, "y"))));
  }

  @Test void testAndAlsoXX() {
    // x && x
    ParameterExpression x = Expressions.parameter(boolean.class, "x");
    assertEquals("{\n  return x;\n}\n",
        optimize(Expressions.andAlso(x, x)));
  }

  @Test void testOrElseTrueBool() {
    // true || bool
    assertEquals("{\n  return true;\n}\n",
        optimize(
            Expressions.orElse(TRUE,
                Expressions.parameter(boolean.class, "bool"))));
  }

  @Test void testOrElseFalseBool() {
    // false || bool
    assertEquals("{\n  return bool;\n}\n",
        optimize(
            Expressions.orElse(FALSE,
                Expressions.parameter(boolean.class, "bool"))));
  }

  @Test void testOrElseNullBool() {
    // null || bool
    assertEquals("{\n  return null || bool;\n}\n",
        optimize(
            Expressions.orElse(NULL,
                Expressions.parameter(boolean.class, "bool"))));
  }

  @Test void testOrElseXY() {
    // x || y
    assertEquals("{\n  return x || y;\n}\n",
        optimize(
            Expressions.orElse(
                Expressions.parameter(boolean.class, "x"),
                Expressions.parameter(boolean.class, "y"))));
  }

  @Test void testOrElseXX() {
    // x || x
    ParameterExpression x = Expressions.parameter(boolean.class, "x");
    assertEquals("{\n  return x;\n}\n", optimize(Expressions.orElse(x, x)));
  }

  @Test void testEqualSameConst() {
    // 1 == 1
    assertEquals("{\n  return true;\n}\n",
        optimize(Expressions.equal(ONE, Expressions.constant(1))));
  }

  @Test void testEqualDifferentConst() {
    // 1 == 2
    assertEquals("{\n  return false;\n}\n",
        optimize(Expressions.equal(ONE, TWO)));
  }

  @Test void testEqualSameExpr() {
    // x == x
    ParameterExpression x = Expressions.parameter(int.class, "x");
    assertEquals("{\n  return true;\n}\n", optimize(Expressions.equal(x, x)));
  }

  @Test void testEqualDifferentExpr() {
    // x == y
    ParameterExpression x = Expressions.parameter(int.class, "x");
    ParameterExpression y = Expressions.parameter(int.class, "y");
    assertEquals("{\n  return x == y;\n}\n", optimize(Expressions.equal(x, y)));
  }

  @Test void testEqualPrimitiveNull() {
    // (int) x == null
    ParameterExpression x = Expressions.parameter(int.class, "x");
    assertEquals("{\n  return false;\n}\n",
        optimize(Expressions.equal(x, NULL)));
  }

  @Test void testEqualObjectNull() {
    // (Integer) x == null
    ParameterExpression x = Expressions.parameter(Integer.class, "x");
    assertEquals("{\n  return x == null;\n}\n",
        optimize(Expressions.equal(x, NULL)));
  }

  @Test void testEqualStringNull() {
    // "Y" == null
    assertEquals("{\n  return false;\n}\n",
        optimize(Expressions.equal(Expressions.constant("Y"), NULL)));
  }

  @Test void testEqualTypedNullUntypedNull() {
    // (Integer) null == null
    assertEquals("{\n  return true;\n}\n",
        optimize(Expressions.equal(NULL_INTEGER, NULL)));
  }

  @Test void testEqualUnypedNullTypedNull() {
    // null == (Integer) null
    assertEquals("{\n  return true;\n}\n",
        optimize(Expressions.equal(NULL, NULL_INTEGER)));
  }

  @Test void testEqualBoolTrue() {
    // x == true
    ParameterExpression x = Expressions.parameter(boolean.class, "x");
    assertEquals("{\n  return x;\n}\n", optimize(Expressions.equal(x, TRUE)));
  }

  @Test void testEqualBoolFalse() {
    // x == false
    ParameterExpression x = Expressions.parameter(boolean.class, "x");
    assertEquals("{\n  return !x;\n}\n", optimize(Expressions.equal(x, FALSE)));
  }

  @Test void testNotEqualSameConst() {
    // 1 != 1
    assertEquals("{\n  return false;\n}\n",
        optimize(Expressions.notEqual(ONE, Expressions.constant(1))));
  }

  @Test void testNotEqualDifferentConst() {
    // 1 != 2
    assertEquals("{\n  return true;\n}\n",
        optimize(Expressions.notEqual(ONE, TWO)));
  }

  @Test void testNotEqualSameExpr() {
    // x != x
    ParameterExpression x = Expressions.parameter(int.class, "x");
    assertEquals("{\n  return false;\n}\n",
        optimize(Expressions.notEqual(x, x)));
  }

  @Test void testNotEqualDifferentExpr() {
    // x != y
    ParameterExpression x = Expressions.parameter(int.class, "x");
    ParameterExpression y = Expressions.parameter(int.class, "y");
    assertEquals("{\n  return x != y;\n}\n",
        optimize(Expressions.notEqual(x, y)));
  }

  @Test void testNotEqualPrimitiveNull() {
    // (int) x == null
    ParameterExpression x = Expressions.parameter(int.class, "x");
    assertEquals("{\n  return true;\n}\n",
        optimize(Expressions.notEqual(x, NULL)));
  }

  @Test void testNotEqualObjectNull() {
    // (Integer) x == null
    ParameterExpression x = Expressions.parameter(Integer.class, "x");
    assertEquals("{\n  return x != null;\n}\n",
        optimize(Expressions.notEqual(x, NULL)));
  }

  @Test void testNotEqualStringNull() {
    // "Y" != null
    assertEquals("{\n  return true;\n}\n",
        optimize(Expressions.notEqual(Expressions.constant("Y"), NULL)));
  }

  @Test void testNotEqualTypedNullUntypedNull() {
    // (Integer) null != null
    assertEquals("{\n  return false;\n}\n",
        optimize(Expressions.notEqual(NULL_INTEGER, NULL)));
  }

  @Test void testNotEqualUnypedNullTypedNull() {
    // null != (Integer) null
    assertEquals("{\n  return false;\n}\n",
        optimize(Expressions.notEqual(NULL, NULL_INTEGER)));
  }

  @Test void testNotEqualBoolTrue() {
    // x != true
    ParameterExpression x = Expressions.parameter(boolean.class, "x");
    assertEquals("{\n  return !x;\n}\n",
        optimize(Expressions.notEqual(x, TRUE)));
  }

  @Test void testNotEqualBoolFalse() {
    // x != false
    ParameterExpression x = Expressions.parameter(boolean.class, "x");
    assertEquals("{\n  return x;\n}\n",
        optimize(Expressions.notEqual(x, FALSE)));
  }

  @Test void testMultipleFolding() {
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

  @Test void testConditionalIfTrue() {
    // if (true) {return 1}
    assertEquals("{\n  return 1;\n}\n",
        optimize(Expressions.ifThen(TRUE, Expressions.return_(null, ONE))));
  }

  @Test void testConditionalIfTrueElse() {
    // if (true) {return 1} else {return 2}
    assertEquals("{\n  return 1;\n}\n",
        optimize(
            Expressions.ifThenElse(TRUE,
                Expressions.return_(null, ONE),
                Expressions.return_(null, TWO))));
  }

  @Test void testConditionalIfFalse() {
    // if (false) {return 1}
    assertEquals("{}",
        optimize(Expressions.ifThen(FALSE, Expressions.return_(null, ONE))));
  }

  @Test void testConditionalIfFalseElse() {
    // if (false) {return 1} else {return 2}
    assertEquals("{\n  return 2;\n}\n",
        optimize(
            Expressions.ifThenElse(FALSE,
                Expressions.return_(null, ONE),
                Expressions.return_(null, TWO))));
  }

  @Test void testConditionalIfBoolTrue() {
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

  @Test void testConditionalIfBoolTrueElse() {
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

  @Test void testConditionalIfBoolFalse() {
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

  @Test void testConditionalIfBoolFalseElse() {
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

  @Test void testConditionalIfBoolFalseTrue() {
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

  @Test void testCastIntToShort() {
    // return (short) 1 --> return (short) 1
    assertEquals("{\n  return (short)1;\n}\n",
        optimize(Expressions.convert_(ONE, short.class)));
  }

  @Test void testCastIntToInt() {
    // return (int) 1 --> return 1L
    assertEquals("{\n  return 1;\n}\n",
        optimize(Expressions.convert_(ONE, int.class)));
  }

  @Test void testCastIntToLong() {
    // return (long) 1 --> return 1L
    assertEquals("{\n  return 1L;\n}\n",
        optimize(Expressions.convert_(ONE, long.class)));
  }

  @Test void testNotTrue() {
    // !true -> false
    assertEquals("{\n  return false;\n}\n", optimize(Expressions.not(TRUE)));
  }

  @Test void testNotFalse() {
    // !false -> true
    assertEquals("{\n  return true;\n}\n", optimize(Expressions.not(FALSE)));
  }

  @Test void testNotNotA() {
    // !!a -> a
    assertEquals("{\n  return a;\n}\n",
        optimize(Expressions.not(Expressions.not(bool("a")))));
  }

  @Test void testNotEq() {
    // !(a == b) -> a != b
    assertEquals("{\n  return a != b;\n}\n",
        optimize(Expressions.not(Expressions.equal(bool("a"), bool("b")))));
  }

  @Test void testNotNeq() {
    // !(a != b) -> a == b
    assertEquals("{\n  return a == b;\n}\n",
        optimize(
            Expressions.not(Expressions.notEqual(bool("a"), bool("b")))));
  }

  @Test void testNotGt() {
    // !(a > b) -> a <= b
    assertEquals("{\n  return a <= b;\n}\n",
        optimize(
            Expressions.not(Expressions.greaterThan(bool("a"), bool("b")))));
  }

  @Test void testNotGte() {
    // !(a >= b) -> a < b
    assertEquals("{\n  return a < b;\n}\n",
        optimize(
            Expressions.not(
                Expressions.greaterThanOrEqual(bool("a"), bool("b")))));
  }

  @Test void testNotLt() {
    // !(a < b) -> a >= b
    assertEquals("{\n  return a >= b;\n}\n",
        optimize(
            Expressions.not(Expressions.lessThan(bool("a"), bool("b")))));
  }

  @Test void testNotLte() {
    // !(a <= b) -> a > b
    assertEquals("{\n  return a > b;\n}\n",
        optimize(
            Expressions.not(
                Expressions.lessThanOrEqual(bool("a"), bool("b")))));
  }

  @Test void booleanValueOfTrue() {
    // Boolean.valueOf(true) -> true
    assertEquals("{\n  return true;\n}\n",
        optimize(Expressions.call(Boolean.class, "valueOf", TRUE)));
  }

  @Test void testBooleanValueOfFalse() {
    // Boolean.valueOf(false) -> false
    assertEquals("{\n  return false;\n}\n",
        optimize(Expressions.call(Boolean.class, "valueOf", FALSE)));
  }

  @Test void testAssign() {
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
        equalTo("{\n"
            + "  long x = 0L;\n"
            + "  if (System.nanoTime() > 0L) {\n"
            + "    x = System.currentTimeMillis();\n"
            + "  }\n"
            + "  System.out.println(x);\n"
            + "}\n"));
  }

  @Test void testAssign2() {
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
        equalTo("{\n"
            + "  long x = 0L;\n"
            + "  if (System.currentTimeMillis() > 0L) {\n"
            + "    x = System.currentTimeMillis();\n"
            + "  }\n"
            + "}\n"));
  }
}
