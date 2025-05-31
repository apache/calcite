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
import org.apache.calcite.linq4j.tree.BinaryExpression;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.ConditionalStatement;
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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit test for {@link org.apache.calcite.linq4j.tree.BlockBuilder}
 * optimization capabilities.
 */
class OptimizerTest {
  @Test void testOptimizeComparison() {
    assertThat(optimize(Expressions.equal(ONE, ONE)),
        is("{\n  return true;\n}\n"));
  }

  @Test void testOptimizeTernaryAlwaysTrue() {
    // true ? 1 : 2
    assertThat(optimize(Expressions.condition(TRUE, ONE, TWO)),
        is("{\n  return 1;\n}\n"));
  }

  @Test void testOptimizeTernaryAlwaysFalse() {
    // false ? 1 : 2
    assertThat(optimize(Expressions.condition(FALSE, ONE, TWO)),
        is("{\n  return 2;\n}\n"));
  }

  @Test void testOptimizeTernaryAlwaysSame() {
    // bool ? 1 : 1
    assertThat(
        optimize(
            Expressions.condition(
                Expressions.parameter(boolean.class, "bool"), ONE, ONE)),
        is("{\n  return 1;\n}\n"));
  }

  @Test void testNonOptimizableTernary() {
    // bool ? 1 : 2
    assertThat(
        optimize(
            Expressions.condition(
                Expressions.parameter(boolean.class, "bool"), ONE, TWO)),
        is("{\n  return bool ? 1 : 2;\n}\n"));
  }

  @Test void testOptimizeTernaryRotateNot() {
    // !bool ? 1 : 2
    assertThat(
        optimize(
            Expressions.condition(
                Expressions.not(Expressions.parameter(boolean.class, "bool")),
                ONE, TWO)),
        is("{\n  return bool ? 2 : 1;\n}\n"));
  }

  @Test void testOptimizeTernaryRotateEqualFalse() {
    // bool == false ? 1 : 2
    assertThat(
        optimize(
            Expressions.condition(
                Expressions.equal(Expressions.parameter(boolean.class, "bool"),
                    FALSE),
                ONE, TWO)),
        is("{\n  return bool ? 2 : 1;\n}\n"));
  }

  @Test void testOptimizeTernaryAtrueB() {
    // a ? true : b  === a || b
    assertThat(
        optimize(
            Expressions.condition(
                Expressions.parameter(boolean.class, "a"),
                TRUE, Expressions.parameter(boolean.class, "b"))),
        is("{\n  return a || b;\n}\n"));
  }

  @Test void testOptimizeTernaryAtrueNull() {
    // a ? Boolean.TRUE : null  === a ? Boolean.TRUE : (Boolean) null
    assertThat(
        optimize(
            Expressions.condition(
                Expressions.parameter(boolean.class, "a"),
                TRUE_B, Expressions.constant(null, Boolean.class))),
        is("{\n  return a ? Boolean.TRUE : null;\n}\n"));
  }

  @Test void testOptimizeTernaryAtrueBoxed() {
    // a ? Boolean.TRUE : Boolean.valueOf(b)  === a || b
    assertThat(
        optimize(
            Expressions.condition(Expressions.parameter(boolean.class, "a"),
                TRUE_B,
                Expressions.call(Boolean.class, "valueOf",
                    Expressions.parameter(boolean.class, "b")))),
        is("{\n  return a || Boolean.valueOf(b);\n}\n"));
  }

  @Test void testOptimizeTernaryABtrue() {
    // a ? b : true  === !a || b
    assertThat(
        optimize(
            Expressions.condition(
                Expressions.parameter(boolean.class, "a"),
                Expressions.parameter(boolean.class, "b"), TRUE)),
        is("{\n  return (!a) || b;\n}\n"));
  }

  @Test void testOptimizeTernaryAfalseB() {
    // a ? false : b === !a && b
    assertThat(
        optimize(
            Expressions.condition(
                Expressions.parameter(boolean.class, "a"),
                FALSE, Expressions.parameter(boolean.class, "b"))),
        is("{\n  return (!a) && b;\n}\n"));
  }

  @Test void testOptimizeTernaryABfalse() {
    // a ? b : false === a && b
    assertThat(
        optimize(
            Expressions.condition(Expressions.parameter(boolean.class, "a"),
                Expressions.parameter(boolean.class, "b"), FALSE)),
        is("{\n  return a && b;\n}\n"));
  }

  @Test void testOptimizeTernaryInEqualABCeqB() {
    // (v ? (Integer) null : inp0_) == null
    assertThat(
        optimize(
            Expressions.equal(
                Expressions.condition(Expressions.parameter(boolean.class, "v"),
                    NULL_INTEGER,
                    Expressions.parameter(Integer.class, "inp0_")),
            NULL)),
        is("{\n  return v || inp0_ == null;\n}\n"));
  }

  @Test void testOptimizeTernaryNullCasting1() {
    assertThat(
        optimize(
            Expressions.equal(
                Expressions.condition(Expressions.parameter(boolean.class, "v"),
                    new ConstantExpression(Long.class, 1L),
                    new ConstantExpression(Long.class, null)),
                new ConstantExpression(Long.class, 2L))),
        is("{\n  return (v ? Long.valueOf(1L) : null) == Long.valueOf(2L);\n}\n"));

    assertThat(
        optimize(
            Expressions.equal(
                Expressions.condition(Expressions.parameter(boolean.class, "v"),
                    new ConstantExpression(Long.class, null),
                    new ConstantExpression(Long.class, 1L)),
                new ConstantExpression(Long.class, 2L))),
        is("{\n  return (v ? null : Long.valueOf(1L)) == Long.valueOf(2L);\n}\n"));

    assertThat(
        optimize(
            Expressions.equal(
                Expressions.condition(Expressions.parameter(boolean.class, "v"),
                    new ConstantExpression(Object.class, null),
                    new ConstantExpression(Long.class, 1L)),
                new ConstantExpression(Long.class, 2L))),
        is("{\n  return (v ? null : Long.valueOf(1L)) == Long.valueOf(2L);\n}\n"));
  }

  @Test void testOptimizeTernaryNullCasting2() {
    ParameterExpression o = Expressions.parameter(Boolean.class, "o");
    ParameterExpression v = Expressions.parameter(Boolean.class, "v");

    BlockStatement bl =
        Expressions.block(
            Expressions.declare(0, v,
                new ConstantExpression(Boolean.class, false)),
        Expressions.declare(0, o,
            Expressions.condition(v,
                new ConstantExpression(Object.class, null),
                new ConstantExpression(Boolean.class, true))));

    assertThat(optimize(bl),
        is("{\n  Boolean v = Boolean.valueOf(false);\n"
            + "  Boolean o = v ? null : Boolean.valueOf(true);\n}\n"));

    bl =
        Expressions.block(
            Expressions.declare(0, o,
            Expressions.orElse(
                new ConstantExpression(Boolean.class, true),
                new ConstantExpression(Boolean.class, null))));

    assertThat(optimize(bl),
        is("{\n  Boolean o = Boolean.valueOf(true) || (Boolean) null;\n}\n"));

    bl =
        Expressions.block(
            Expressions.declare(0, o,
            Expressions.orElse(
                new ConstantExpression(Boolean.class, null),
                new ConstantExpression(Boolean.class, true))));

    assertThat(optimize(bl),
        is("{\n  Boolean o = (Boolean) null || Boolean.valueOf(true);\n}\n"));
  }

  @Test void testOptimizeBinaryNullCasting1() {
    ParameterExpression x = Expressions.variable(String.class, "x");
    ConstantExpression one = new ConstantExpression(String.class, "one");
    ConstantExpression second = new ConstantExpression(String.class, null);

    ConstantExpression innerExp = new ConstantExpression(Long.class, 2L);
    ParameterExpression y = Expressions.parameter(Long.class, "y");
    BinaryExpression exp0 = Expressions.greaterThan(y, innerExp);
    ConditionalStatement finalExp =
        Expressions.ifThenElse(exp0, Expressions.assign(x, one),
            Expressions.assign(x, second));

    assertThat(optimize(finalExp),
        is("{\n  if (y > Long.valueOf(2L)) {\n"
            + "    return x = \"one\";\n"
            + "  } else {\n"
            + "    return x = null;\n"
            + "  }\n}\n"));
  }

  @Test void testOptimizeBinaryNullCasting2() {
    // Boolean x;
    ParameterExpression x = Expressions.variable(Boolean.class, "x");
    ParameterExpression y = Expressions.variable(Boolean.class, "y");
    // Boolean y = x || (Boolean) null;
    BinaryExpression yt =
        Expressions.assign(
            y, Expressions.orElse(x,
            new ConstantExpression(Boolean.class, null)));
    assertThat(optimize(yt),
        is("{\n  return y = x || (Boolean) null;\n}\n"));
  }

  @Test void testOptimizeTernaryInEqualABCeqC() {
    // (v ? inp0_ : (Integer) null) == null
    assertThat(
        optimize(
            Expressions.equal(
                Expressions.condition(Expressions.parameter(boolean.class, "v"),
                    Expressions.parameter(Integer.class, "inp0_"),
                    NULL_INTEGER),
            NULL)),
        is("{\n  return (!v) || inp0_ == null;\n}\n"));
  }

  @Test void testOptimizeTernaryAeqBBA() {
    // a == b ? b : a
    ParameterExpression a = Expressions.parameter(boolean.class, "a");
    ParameterExpression b = Expressions.parameter(boolean.class, "b");
    assertThat(optimize(Expressions.condition(Expressions.equal(a, b), b, a)),
        is("{\n  return a;\n}\n"));
  }

  @Test void testOptimizeTernaryAeqBAB() {
    // a == b ? a : b
    ParameterExpression a = Expressions.parameter(boolean.class, "a");
    ParameterExpression b = Expressions.parameter(boolean.class, "b");
    assertThat(optimize(Expressions.condition(Expressions.equal(a, b), a, b)),
        is("{\n  return b;\n}\n"));
  }

  @Test void testOptimizeTernaryInEqualABCneqB() {
    // (v ? (Integer) null : inp0_) != null
    assertThat(
        optimize(
            Expressions.notEqual(
                Expressions.condition(Expressions.parameter(boolean.class, "v"),
                    NULL_INTEGER,
                    Expressions.parameter(Integer.class, "inp0_")),
            NULL)),
        is("{\n  return (!(v || inp0_ == null));\n}\n"));
  }

  @Test void testOptimizeTernaryInEqualABCneqC() {
    // (v ? inp0_ : (Integer) null) != null
    assertThat(
        optimize(
            Expressions.notEqual(
                Expressions.condition(Expressions.parameter(boolean.class, "v"),
                    Expressions.parameter(Integer.class, "inp0_"),
                    NULL_INTEGER),
            NULL)),
        is("{\n  return (!((!v) || inp0_ == null));\n}\n"));
  }

  @Test void testOptimizeTernaryAneqBBA() {
    // a != b ? b : a
    ParameterExpression a = Expressions.parameter(boolean.class, "a");
    ParameterExpression b = Expressions.parameter(boolean.class, "b");
    assertThat(
        optimize(Expressions.condition(Expressions.notEqual(a, b), b, a)),
        is("{\n  return b;\n}\n"));
  }

  @Test void testOptimizeTernaryAneqBAB() {
    // a != b ? a : b
    ParameterExpression a = Expressions.parameter(boolean.class, "a");
    ParameterExpression b = Expressions.parameter(boolean.class, "b");
    assertThat(
        optimize(Expressions.condition(Expressions.notEqual(a, b), a, b)),
        is("{\n  return a;\n}\n"));
  }

  @Test void testAndAlsoTrueBool() {
    // true && bool
    assertThat(
        optimize(
            Expressions.andAlso(TRUE,
                Expressions.parameter(boolean.class, "bool"))),
        is("{\n  return bool;\n}\n"));
  }

  @Test void testAndAlsoBoolTrue() {
    // bool && true
    assertThat(
        optimize(
            Expressions.andAlso(
                Expressions.parameter(boolean.class, "bool"), TRUE)),
        is("{\n  return bool;\n}\n"));
  }

  @Test void testAndAlsoFalseBool() {
    // false && bool
    assertThat(
        optimize(
            Expressions.andAlso(FALSE,
                Expressions.parameter(boolean.class, "bool"))),
        is("{\n  return false;\n}\n"));
  }

  @Test void testAndAlsoNullBool() {
    // null && bool
    assertThat(
        optimize(
            Expressions.andAlso(NULL,
                Expressions.parameter(boolean.class, "bool"))),
        is("{\n  return null && bool;\n}\n"));
  }

  @Test void testAndAlsoXY() {
    // x && y
    assertThat(
        optimize(
            Expressions.andAlso(
                Expressions.parameter(boolean.class, "x"),
                Expressions.parameter(boolean.class, "y"))),
        is("{\n  return x && y;\n}\n"));
  }

  @Test void testAndAlsoXX() {
    // x && x
    ParameterExpression x = Expressions.parameter(boolean.class, "x");
    assertThat(optimize(Expressions.andAlso(x, x)),
        is("{\n  return x;\n}\n"));
  }

  @Test void testOrElseTrueBool() {
    // true || bool
    assertThat(
        optimize(
            Expressions.orElse(TRUE,
                Expressions.parameter(boolean.class, "bool"))),
        is("{\n  return true;\n}\n"));
  }

  @Test void testOrElseFalseBool() {
    // false || bool
    assertThat(
        optimize(
            Expressions.orElse(FALSE,
                Expressions.parameter(boolean.class, "bool"))),
        is("{\n  return bool;\n}\n"));
  }

  @Test void testOrElseNullBool() {
    // null || bool
    assertThat(
        optimize(
            Expressions.orElse(NULL,
                Expressions.parameter(boolean.class, "bool"))),
        is("{\n  return null || bool;\n}\n"));
  }

  @Test void testOrElseXY() {
    // x || y
    assertThat(
        optimize(
            Expressions.orElse(
                Expressions.parameter(boolean.class, "x"),
                Expressions.parameter(boolean.class, "y"))),
        is("{\n  return x || y;\n}\n"));
  }

  @Test void testOrElseXX() {
    // x || x
    ParameterExpression x = Expressions.parameter(boolean.class, "x");
    assertThat(optimize(Expressions.orElse(x, x)),
        is("{\n  return x;\n}\n"));
  }

  @Test void testEqualSameConst() {
    // 1 == 1
    assertThat(optimize(Expressions.equal(ONE, Expressions.constant(1))),
        is("{\n  return true;\n}\n"));
  }

  @Test void testEqualDifferentConst() {
    // 1 == 2
    assertThat(optimize(Expressions.equal(ONE, TWO)),
        is("{\n  return false;\n}\n"));
  }

  @Test void testEqualSameExpr() {
    // x == x
    ParameterExpression x = Expressions.parameter(int.class, "x");
    assertThat(optimize(Expressions.equal(x, x)),
        is("{\n  return true;\n}\n"));
  }

  @Test void testEqualDifferentExpr() {
    // x == y
    ParameterExpression x = Expressions.parameter(int.class, "x");
    ParameterExpression y = Expressions.parameter(int.class, "y");
    assertThat(optimize(Expressions.equal(x, y)),
        is("{\n  return x == y;\n}\n"));
  }

  @Test void testEqualPrimitiveNull() {
    // (int) x == null
    ParameterExpression x = Expressions.parameter(int.class, "x");
    assertThat(optimize(Expressions.equal(x, NULL)),
        is("{\n  return false;\n}\n"));
  }

  @Test void testEqualObjectNull() {
    // (Integer) x == null
    ParameterExpression x = Expressions.parameter(Integer.class, "x");
    assertThat(optimize(Expressions.equal(x, NULL)),
        is("{\n  return x == null;\n}\n"));
  }

  @Test void testEqualStringNull() {
    // "Y" == null
    assertThat(optimize(Expressions.equal(Expressions.constant("Y"), NULL)),
        is("{\n  return false;\n}\n"));
  }

  @Test void testEqualTypedNullUntypedNull() {
    // (Integer) null == null
    assertThat(optimize(Expressions.equal(NULL_INTEGER, NULL)),
        is("{\n  return true;\n}\n"));
  }

  @Test void testEqualUnypedNullTypedNull() {
    // null == (Integer) null
    assertThat(optimize(Expressions.equal(NULL, NULL_INTEGER)),
        is("{\n  return true;\n}\n"));
  }

  @Test void testEqualBoolTrue() {
    // x == true
    ParameterExpression x = Expressions.parameter(boolean.class, "x");
    assertThat(optimize(Expressions.equal(x, TRUE)),
        is("{\n  return x;\n}\n"));
  }

  @Test void testEqualBoolFalse() {
    // x == false
    ParameterExpression x = Expressions.parameter(boolean.class, "x");
    assertThat(optimize(Expressions.equal(x, FALSE)),
        is("{\n  return (!x);\n}\n"));
  }

  @Test void testNotEqualSameConst() {
    // 1 != 1
    assertThat(optimize(Expressions.notEqual(ONE, Expressions.constant(1))),
        is("{\n  return false;\n}\n"));
  }

  @Test void testNotEqualDifferentConst() {
    // 1 != 2
    assertThat(optimize(Expressions.notEqual(ONE, TWO)),
        is("{\n  return true;\n}\n"));
  }

  @Test void testNotEqualSameExpr() {
    // x != x
    ParameterExpression x = Expressions.parameter(int.class, "x");
    assertThat(optimize(Expressions.notEqual(x, x)),
        is("{\n  return false;\n}\n"));
  }

  @Test void testNotEqualDifferentExpr() {
    // x != y
    ParameterExpression x = Expressions.parameter(int.class, "x");
    ParameterExpression y = Expressions.parameter(int.class, "y");
    assertThat(optimize(Expressions.notEqual(x, y)),
        is("{\n  return x != y;\n}\n"));
  }

  @Test void testNotEqualPrimitiveNull() {
    // (int) x == null
    ParameterExpression x = Expressions.parameter(int.class, "x");
    assertThat(optimize(Expressions.notEqual(x, NULL)),
        is("{\n  return true;\n}\n"));
  }

  @Test void testNotEqualObjectNull() {
    // (Integer) x == null
    ParameterExpression x = Expressions.parameter(Integer.class, "x");
    assertThat(optimize(Expressions.notEqual(x, NULL)),
        is("{\n  return x != null;\n}\n"));
  }

  @Test void testNotEqualStringNull() {
    // "Y" != null
    assertThat(optimize(Expressions.notEqual(Expressions.constant("Y"), NULL)),
        is("{\n  return true;\n}\n"));
  }

  @Test void testNotEqualTypedNullUntypedNull() {
    // (Integer) null != null
    assertThat(optimize(Expressions.notEqual(NULL_INTEGER, NULL)),
        is("{\n  return false;\n}\n"));
  }

  @Test void testNotEqualUnypedNullTypedNull() {
    // null != (Integer) null
    assertThat(optimize(Expressions.notEqual(NULL, NULL_INTEGER)),
        is("{\n  return false;\n}\n"));
  }

  @Test void testNotEqualBoolTrue() {
    // x != true
    ParameterExpression x = Expressions.parameter(boolean.class, "x");
    assertThat(optimize(Expressions.notEqual(x, TRUE)),
        is("{\n  return (!x);\n}\n"));
  }

  @Test void testNotEqualBoolFalse() {
    // x != false
    ParameterExpression x = Expressions.parameter(boolean.class, "x");
    assertThat(optimize(Expressions.notEqual(x, FALSE)),
        is("{\n  return x;\n}\n"));
  }

  @Test void testMultipleFolding() {
    // (1 == 2 ? 3 : 4) != (5 != 6 ? 4 : 8) ? 9 : 10
    assertThat(
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
                Expressions.constant(10))),
        is("{\n  return 10;\n}\n"));
  }

  @Test void testConditionalIfTrue() {
    // if (true) {return 1}
    assertThat(
        optimize(Expressions.ifThen(TRUE, Expressions.return_(null, ONE))),
        is("{\n  return 1;\n}\n"));
  }

  @Test void testConditionalIfTrueElse() {
    // if (true) {return 1} else {return 2}
    assertThat(
        optimize(
            Expressions.ifThenElse(TRUE,
                Expressions.return_(null, ONE),
                Expressions.return_(null, TWO))),
        is("{\n  return 1;\n}\n"));
  }

  @Test void testConditionalIfFalse() {
    // if (false) {return 1}
    assertThat(
        optimize(Expressions.ifThen(FALSE, Expressions.return_(null, ONE))),
        is("{}"));
  }

  @Test void testConditionalIfFalseElse() {
    // if (false) {return 1} else {return 2}
    assertThat(
        optimize(
            Expressions.ifThenElse(FALSE,
                Expressions.return_(null, ONE),
                Expressions.return_(null, TWO))),
        is("{\n  return 2;\n}\n"));
  }

  @Test void testConditionalIfBoolTrue() {
    // if (bool) {return 1} else if (true) {return 2}
    Expression bool = Expressions.parameter(boolean.class, "bool");
    assertThat(
        optimize(
            Expressions.ifThenElse(bool,
                Expressions.return_(null, ONE),
                TRUE,
                Expressions.return_(null, TWO))),
        is("{\n"
            + "  if (bool) {\n"
            + "    return 1;\n"
            + "  } else {\n"
            + "    return 2;\n"
            + "  }\n"
            + "}\n"));
  }

  @Test void testConditionalIfBoolTrueElse() {
    // if (bool) {return 1} else if (true) {return 2} else {return 3}
    Expression bool = Expressions.parameter(boolean.class, "bool");
    assertThat(
        optimize(
            Expressions.ifThenElse(bool,
                Expressions.return_(null, ONE),
                TRUE,
                Expressions.return_(null, TWO),
                Expressions.return_(null, THREE))),
        is("{\n"
            + "  if (bool) {\n"
            + "    return 1;\n"
            + "  } else {\n"
            + "    return 2;\n"
            + "  }\n"
            + "}\n"));
  }

  @Test void testConditionalIfBoolFalse() {
    // if (bool) {return 1} else if (false) {return 2}
    Expression bool = Expressions.parameter(boolean.class, "bool");
    assertThat(
        optimize(
            Expressions.ifThenElse(bool,
                Expressions.return_(null, ONE),
                FALSE,
                Expressions.return_(null, TWO))),
        is("{\n"
            + "  if (bool) {\n"
            + "    return 1;\n"
            + "  }\n"
            + "}\n"));
  }

  @Test void testConditionalIfBoolFalseElse() {
    // if (bool) {return 1} else if (false) {return 2} else {return 3}
    Expression bool = Expressions.parameter(boolean.class, "bool");
    assertThat(
        optimize(
            Expressions.ifThenElse(bool,
                Expressions.return_(null, ONE),
                FALSE,
                Expressions.return_(null, TWO),
                Expressions.return_(null, THREE))),
        is("{\n"
            + "  if (bool) {\n"
            + "    return 1;\n"
            + "  } else {\n"
            + "    return 3;\n"
            + "  }\n"
            + "}\n"));
  }

  @Test void testConditionalIfBoolFalseTrue() {
    // if (bool) {1} else if (false) {2} if (true) {4} else {5}
    Expression bool = Expressions.parameter(boolean.class, "bool");
    assertThat(
        optimize(
            Expressions.ifThenElse(bool,
                Expressions.return_(null, ONE),
                FALSE,
                Expressions.return_(null, TWO),
                TRUE,
                Expressions.return_(null, FOUR),
                Expressions.return_(null, Expressions.constant(5)))),
        is("{\n"
            + "  if (bool) {\n"
            + "    return 1;\n"
            + "  } else {\n"
            + "    return 4;\n"
            + "  }\n"
            + "}\n"));
  }

  @Test void testCastIntToShort() {
    // return (short) 1 --> return (short) 1
    assertThat(optimize(Expressions.convert_(ONE, short.class)),
        is("{\n  return (short) 1;\n}\n"));
  }

  @Test void testCastIntToInt() {
    // return (int) 1 --> return 1L
    assertThat(optimize(Expressions.convert_(ONE, int.class)),
        is("{\n  return 1;\n}\n"));
  }

  @Test void testCastIntToLong() {
    // return (long) 1 --> return 1L
    assertThat(optimize(Expressions.convert_(ONE, long.class)),
        is("{\n  return (long) 1;\n}\n"));
  }

  @Test void testNotTrue() {
    // !true -> false
    assertThat(optimize(Expressions.not(TRUE)),
        is("{\n  return false;\n}\n"));
  }

  @Test void testNotFalse() {
    // !false -> true
    assertThat(optimize(Expressions.not(FALSE)),
        is("{\n  return true;\n}\n"));
  }

  @Test void testNotNotA() {
    // !!a -> a
    assertThat(optimize(Expressions.not(Expressions.not(bool("a")))),
        is("{\n  return a;\n}\n"));
  }

  @Test void testNotEq() {
    // !(a == b) -> a != b
    assertThat(
        optimize(Expressions.not(Expressions.equal(bool("a"), bool("b")))),
        is("{\n  return a != b;\n}\n"));
  }

  @Test void testNotNeq() {
    // !(a != b) -> a == b
    assertThat(
        optimize(
            Expressions.not(Expressions.notEqual(bool("a"), bool("b")))),
        is("{\n  return a == b;\n}\n"));
  }

  @Test void testNotGt() {
    // !(a > b) -> a <= b
    assertThat(
        optimize(
            Expressions.not(Expressions.greaterThan(bool("a"), bool("b")))),
        is("{\n  return a <= b;\n}\n"));
  }

  @Test void testNotGte() {
    // !(a >= b) -> a < b
    assertThat(
        optimize(
            Expressions.not(
                Expressions.greaterThanOrEqual(bool("a"), bool("b")))),
        is("{\n  return a < b;\n}\n"));
  }

  @Test void testNotLt() {
    // !(a < b) -> a >= b
    assertThat(
        optimize(
            Expressions.not(Expressions.lessThan(bool("a"), bool("b")))),
        is("{\n  return a >= b;\n}\n"));
  }

  @Test void testNotLte() {
    // !(a <= b) -> a > b
    assertThat(
        optimize(
            Expressions.not(
                Expressions.lessThanOrEqual(bool("a"), bool("b")))),
        is("{\n  return a > b;\n}\n"));
  }

  @Test void booleanValueOfTrue() {
    // Boolean.valueOf(true) -> true
    assertThat(optimize(Expressions.call(Boolean.class, "valueOf", TRUE)),
        is("{\n  return true;\n}\n"));
  }

  @Test void testBooleanValueOfFalse() {
    // Boolean.valueOf(false) -> false
    assertThat(optimize(Expressions.call(Boolean.class, "valueOf", FALSE)),
        is("{\n  return false;\n}\n"));
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
