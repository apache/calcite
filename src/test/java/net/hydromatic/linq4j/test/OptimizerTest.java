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

import static org.junit.Assert.*;

/**
 * Unit test for {@link net.hydromatic.linq4j.expressions.BlockBuilder}
 * optimization capabilities.
 */
public class OptimizerTest extends BlockBuilderBase {
  @Test
  public void optimizeComparison() {
    assertEquals("{\n  return true;\n}\n", optimize(
        Expressions.equal(ONE, ONE)));
  }

  @Test
  public void optimizeTernaryAlwaysTrue() {
    // true ? 1 : 2
    assertEquals("{\n  return 1;\n}\n", optimize(
        Expressions.condition(TRUE, ONE, TWO)));
  }

  @Test
  public void optimizeTernaryAlwaysFalse() {
    // false ? 1 : 2
    assertEquals("{\n  return 2;\n}\n", optimize(
        Expressions.condition(FALSE, ONE, TWO)));
  }

  @Test
  public void optimizeTernaryAlwaysSame() {
    // bool ? 1 : 1
    assertEquals("{\n  return 1;\n}\n", optimize(Expressions.condition(
        Expressions.parameter(boolean.class, "bool"), ONE, ONE)));
  }

  @Test
  public void nonOptimizableTernary() {
    // bool ? 1 : 2
    assertEquals("{\n  return bool ? 1 : 2;\n}\n",
        optimize(Expressions.condition(
            Expressions.parameter(boolean.class, "bool"), ONE, TWO)));
  }

  @Test
  public void optimizeTernaryRotateNot() {
    // !bool ? 1 : 2
    assertEquals("{\n  return bool ? 2 : 1;\n}\n",
        optimize(Expressions.condition(
            Expressions.not(Expressions.parameter(boolean.class, "bool")),
            ONE, TWO)));
  }

  @Test
  public void optimizeTernaryRotateEqualFalse() {
    // bool == false ? 1 : 2
    assertEquals("{\n  return bool ? 2 : 1;\n}\n",
        optimize(Expressions.condition(
            Expressions.equal(Expressions.parameter(boolean.class, "bool"),
                FALSE),
            ONE, TWO)));
  }

  @Test
  public void optimizeTernaryAtrueB() {
    // a ? true : b  === a || b
    assertEquals("{\n  return a || b;\n}\n",
        optimize(Expressions.condition(
            Expressions.parameter(boolean.class, "a"),
            TRUE, Expressions.parameter(boolean.class, "b"))));
  }

  @Test
  public void optimizeTernaryAtrueNull() {
    // a ? Boolean.TRUE : null  === a ? Boolean.TRUE : (Boolean) null
    assertEquals("{\n  return a ? Boolean.TRUE : (Boolean) null;\n}\n",
        optimize(Expressions.condition(
            Expressions.parameter(boolean.class, "a"),
            TRUE_B, Expressions.constant(null, Boolean.class))));
  }

  @Test
  public void optimizeTernaryAtrueBoxed() {
    // a ? Boolean.TRUE : Boolean.valueOf(b)  === a || b
    assertEquals("{\n  return a || Boolean.valueOf(b);\n}\n",
        optimize(Expressions.condition(
            Expressions.parameter(boolean.class, "a"),
            TRUE_B, Expressions.call(Boolean.class, "valueOf",
            Expressions.parameter(boolean.class, "b")))));
  }

  @Test
  public void optimizeTernaryABtrue() {
    // a ? b : true  === !a || b
    assertEquals("{\n  return !a || b;\n}\n",
        optimize(Expressions.condition(
            Expressions.parameter(boolean.class, "a"),
            Expressions.parameter(boolean.class, "b"), TRUE)));
  }

  @Test
  public void optimizeTernaryAfalseB() {
    // a ? false : b === !a && b
    assertEquals("{\n  return !a && b;\n}\n",
        optimize(Expressions.condition(
            Expressions.parameter(boolean.class, "a"),
            FALSE, Expressions.parameter(boolean.class, "b"))));
  }

  @Test
  public void optimizeTernaryABfalse() {
    // a ? b : false === a && b
    assertEquals("{\n  return a && b;\n}\n",
        optimize(Expressions.condition(
            Expressions.parameter(boolean.class, "a"),
            Expressions.parameter(boolean.class, "b"), FALSE)));
  }

  @Test
  public void optimizeTernaryInEqualABCeqB() {
    // (v ? (Integer) null : inp0_) == null
    assertEquals("{\n  return v || inp0_ == null;\n}\n",
        optimize(Expressions.equal(Expressions.condition(
            Expressions.parameter(boolean.class, "v"),
            NULL_INTEGER, Expressions.parameter(Integer.class, "inp0_")),
            NULL))
    );
  }

  @Test
  public void optimizeTernaryInEqualABCeqC() {
    // (v ? inp0_ : (Integer) null) == null
    assertEquals("{\n  return !v || inp0_ == null;\n}\n",
        optimize(Expressions.equal(Expressions.condition(
            Expressions.parameter(boolean.class, "v"),
            Expressions.parameter(Integer.class, "inp0_"), NULL_INTEGER),
            NULL))
    );
  }

  @Test
  public void optimizeTernaryAeqBBA() {
    // a == b ? b : a
    ParameterExpression a = Expressions.parameter(boolean.class, "a");
    ParameterExpression b = Expressions.parameter(boolean.class, "b");
    assertEquals("{\n  return a;\n}\n",
        optimize(Expressions.condition(Expressions.equal(a, b), b, a)));
  }

  @Test
  public void optimizeTernaryAeqBAB() {
    // a == b ? a : b
    ParameterExpression a = Expressions.parameter(boolean.class, "a");
    ParameterExpression b = Expressions.parameter(boolean.class, "b");
    assertEquals("{\n  return b;\n}\n",
        optimize(Expressions.condition(Expressions.equal(a, b), a, b)));
  }

  @Test
  public void optimizeTernaryInEqualABCneqB() {
    // (v ? (Integer) null : inp0_) != null
    assertEquals("{\n  return !(v || inp0_ == null);\n}\n",
        optimize(Expressions.notEqual(Expressions.condition(
            Expressions.parameter(boolean.class, "v"),
            NULL_INTEGER, Expressions.parameter(Integer.class, "inp0_")),
            NULL))
    );
  }

  @Test
  public void optimizeTernaryInEqualABCneqC() {
    // (v ? inp0_ : (Integer) null) != null
    assertEquals("{\n  return !(!v || inp0_ == null);\n}\n",
        optimize(Expressions.notEqual(Expressions.condition(
            Expressions.parameter(boolean.class, "v"),
            Expressions.parameter(Integer.class, "inp0_"), NULL_INTEGER),
            NULL))
    );
  }

  @Test
  public void optimizeTernaryAneqBBA() {
    // a != b ? b : a
    ParameterExpression a = Expressions.parameter(boolean.class, "a");
    ParameterExpression b = Expressions.parameter(boolean.class, "b");
    assertEquals("{\n  return b;\n}\n",
        optimize(Expressions.condition(Expressions.notEqual(a, b), b, a)));
  }

  @Test
  public void optimizeTernaryAneqBAB() {
    // a != b ? a : b
    ParameterExpression a = Expressions.parameter(boolean.class, "a");
    ParameterExpression b = Expressions.parameter(boolean.class, "b");
    assertEquals("{\n  return a;\n}\n",
        optimize(Expressions.condition(Expressions.notEqual(a, b), a, b)));
  }
  @Test
  public void andAlsoTrueBool() {
    // true && bool
    assertEquals("{\n  return bool;\n}\n", optimize(Expressions.andAlso(TRUE,
        Expressions.parameter(boolean.class,
            "bool"))));
  }

  @Test
  public void andAlsoBoolTrue() {
    // bool && true
    assertEquals("{\n  return bool;\n}\n", optimize(Expressions.andAlso(
        Expressions.parameter(boolean.class,
            "bool"), TRUE)));
  }

  @Test
  public void andAlsoFalseBool() {
    // false && bool
    assertEquals("{\n  return false;\n}\n", optimize(Expressions.andAlso(FALSE,
        Expressions.parameter(boolean.class,
            "bool"))));
  }

  @Test
  public void andAlsoNullBool() {
    // null && bool
    assertEquals("{\n  return null && bool;\n}\n",
        optimize(Expressions.andAlso(NULL,
            Expressions.parameter(boolean.class,
                "bool"))));
  }

  @Test
  public void andAlsoXY() {
    // x && y
    assertEquals("{\n  return x && y;\n}\n", optimize(Expressions.andAlso(
        Expressions.parameter(
            boolean.class, "x"),
        Expressions.parameter(boolean.class,
            "y"))));
  }

  @Test
  public void andAlsoXX() {
    // x && x
    ParameterExpression x = Expressions.parameter(boolean.class, "x");
    assertEquals("{\n  return x;\n}\n", optimize(Expressions.andAlso(x, x)));
  }

  @Test
  public void orElseTrueBool() {
    // true || bool
    assertEquals("{\n  return true;\n}\n", optimize(Expressions.orElse(TRUE,
        Expressions.parameter(boolean.class,
            "bool"))));
  }

  @Test
  public void orElseFalseBool() {
    // false || bool
    assertEquals("{\n  return bool;\n}\n", optimize(Expressions.orElse(FALSE,
        Expressions.parameter(boolean.class,
            "bool"))));
  }

  @Test
  public void orElseNullBool() {
    // null || bool
    assertEquals("{\n  return null || bool;\n}\n",
        optimize(Expressions.orElse(NULL,
            Expressions.parameter(boolean.class,
                "bool"))));
  }

  @Test
  public void orElseXY() {
    // x || y
    assertEquals("{\n  return x || y;\n}\n", optimize(Expressions.orElse(
        Expressions.parameter(
            boolean.class, "x"),
        Expressions.parameter(boolean.class,
            "y"))));
  }

  @Test
  public void orElseXX() {
    // x || x
    ParameterExpression x = Expressions.parameter(boolean.class, "x");
    assertEquals("{\n  return x;\n}\n", optimize(Expressions.orElse(x, x)));
  }

  @Test
  public void equalSameConst() {
    // 1 == 1
    assertEquals("{\n  return true;\n}\n", optimize(Expressions.equal(ONE,
        Expressions.constant(1))));
  }

  @Test
  public void equalDifferentConst() {
    // 1 == 2
    assertEquals("{\n  return false;\n}\n", optimize(Expressions.equal(ONE,
        TWO)));
  }

  @Test
  public void equalSameExpr() {
    // x == x
    ParameterExpression x = Expressions.parameter(int.class, "x");
    assertEquals("{\n  return true;\n}\n", optimize(Expressions.equal(x, x)));
  }

  @Test
  public void equalDifferentExpr() {
    // x == y
    ParameterExpression x = Expressions.parameter(int.class, "x");
    ParameterExpression y = Expressions.parameter(int.class, "y");
    assertEquals("{\n  return x == y;\n}\n", optimize(Expressions.equal(x, y)));
  }

  @Test
  public void equalPrimitiveNull() {
    // (int) x == null
    ParameterExpression x = Expressions.parameter(int.class, "x");
    assertEquals("{\n  return false;\n}\n", optimize(Expressions.equal(x,
        NULL)));
  }

  @Test
  public void equalObjectNull() {
    // (Integer) x == null
    ParameterExpression x = Expressions.parameter(Integer.class, "x");
    assertEquals("{\n  return x == null;\n}\n",
        optimize(Expressions.equal(x,
            NULL)));
  }

  @Test
  public void equalStringNull() {
    // "Y" == null
    assertEquals("{\n  return false;\n}\n",
        optimize(Expressions.equal(Expressions.constant("Y"), NULL)));
  }

  @Test
  public void equalTypedNullUntypedNull() {
    // (Integer) null == null
    assertEquals("{\n  return true;\n}\n",
        optimize(Expressions.equal(NULL_INTEGER, NULL)));
  }

  @Test
  public void equalUnypedNullTypedNull() {
    // null == (Integer) null
    assertEquals("{\n  return true;\n}\n",
        optimize(Expressions.equal(NULL, NULL_INTEGER)));
  }

  @Test
  public void equalBoolTrue() {
    // x == true
    ParameterExpression x = Expressions.parameter(boolean.class, "x");
    assertEquals("{\n  return x;\n}\n", optimize(Expressions.equal(x, TRUE)));
  }

  @Test
  public void equalBoolFalse() {
    // x == false
    ParameterExpression x = Expressions.parameter(boolean.class, "x");
    assertEquals("{\n  return !x;\n}\n", optimize(Expressions.equal(x, FALSE)));
  }

  @Test
  public void notEqualSameConst() {
    // 1 != 1
    assertEquals("{\n  return false;\n}\n", optimize(Expressions.notEqual(
        ONE, Expressions.constant(1))));
  }

  @Test
  public void notEqualDifferentConst() {
    // 1 != 2
    assertEquals("{\n  return true;\n}\n", optimize(Expressions.notEqual(ONE,
        TWO)));
  }

  @Test
  public void notEqualSameExpr() {
    // x != x
    ParameterExpression x = Expressions.parameter(int.class, "x");
    assertEquals("{\n  return false;\n}\n", optimize(Expressions.notEqual(x,
        x)));
  }

  @Test
  public void notEqualDifferentExpr() {
    // x != y
    ParameterExpression x = Expressions.parameter(int.class, "x");
    ParameterExpression y = Expressions.parameter(int.class, "y");
    assertEquals("{\n  return x != y;\n}\n", optimize(Expressions.notEqual(x,
        y)));
  }

  @Test
  public void notEqualPrimitiveNull() {
    // (int) x == null
    ParameterExpression x = Expressions.parameter(int.class, "x");
    assertEquals("{\n  return true;\n}\n", optimize(Expressions.notEqual(x,
        NULL)));
  }

  @Test
  public void notEqualObjectNull() {
    // (Integer) x == null
    ParameterExpression x = Expressions.parameter(Integer.class, "x");
    assertEquals("{\n  return x != null;\n}\n",
        optimize(Expressions.notEqual(
            x, NULL)));
  }

  @Test
  public void notEqualStringNull() {
    // "Y" != null
    assertEquals("{\n  return true;\n}\n",
        optimize(Expressions.notEqual(Expressions.constant("Y"), NULL)));
  }

  @Test
  public void notEqualTypedNullUntypedNull() {
    // (Integer) null != null
    assertEquals("{\n  return false;\n}\n",
        optimize(Expressions.notEqual(NULL_INTEGER, NULL)));
  }

  @Test
  public void notEqualUnypedNullTypedNull() {
    // null != (Integer) null
    assertEquals("{\n  return false;\n}\n",
        optimize(Expressions.notEqual(NULL, NULL_INTEGER)));
  }

  @Test
  public void notEqualBoolTrue() {
    // x != true
    ParameterExpression x = Expressions.parameter(boolean.class, "x");
    assertEquals("{\n  return !x;\n}\n", optimize(Expressions.notEqual(x,
        TRUE)));
  }

  @Test
  public void notEqualBoolFalse() {
    // x != false
    ParameterExpression x = Expressions.parameter(boolean.class, "x");
    assertEquals("{\n  return x;\n}\n", optimize(Expressions.notEqual(x,
        FALSE)));
  }

  @Test
  public void multipleFolding() {
    // ( 1 == 2 ? 3 : 4 ) != (5 != 6 ? 4 : 8) ? 9 : 10
    assertEquals("{\n  return 10;\n}\n", optimize(Expressions.condition(
        Expressions.notEqual(
            Expressions.condition(Expressions.equal(ONE, TWO),
                Expressions.constant(3),
                Expressions.constant(4)), Expressions.condition(
            Expressions.notEqual(
                Expressions.constant(5), Expressions.constant(6)),
            Expressions.constant(4), Expressions.constant(8))),
        Expressions.constant(9),
        Expressions.constant(10))));
  }

  @Test
  public void conditionalIfTrue() {
    // if (true) {return 1}
    assertEquals("{\n  return 1;\n}\n", optimize(Expressions.ifThen(TRUE,
        Expressions.return_(null, ONE))));
  }

  @Test
  public void conditionalIfTrueElse() {
    // if (true) {return 1} else {return 2}
    assertEquals("{\n  return 1;\n}\n", optimize(Expressions.ifThenElse(TRUE,
        Expressions.return_(null, ONE), Expressions.return_(null, TWO))));
  }

  @Test
  public void conditionalIfFalse() {
    // if (false) {return 1}
    assertEquals("{}", optimize(Expressions.ifThen(FALSE,
        Expressions.return_(null, ONE))));
  }

  @Test
  public void conditionalIfFalseElse() {
    // if (false) {return 1} else {return 2}
    assertEquals("{\n  return 2;\n}\n", optimize(Expressions.ifThenElse(FALSE,
        Expressions.return_(null, ONE), Expressions.return_(null, TWO))));
  }

  @Test
  public void conditionalIfBoolTrue() {
    // if (bool) {return 1} else if (true) {return 2}
    Expression bool = Expressions.parameter(boolean.class, "bool");
    assertEquals("{\n"
                 + "  if (bool) {\n"
                 + "    return 1;\n"
                 + "  } else {\n"
                 + "    return 2;\n"
                 + "  }\n"
                 + "}\n", optimize(Expressions.ifThenElse(bool,
        Expressions.return_(null, ONE), TRUE, Expressions.return_(null,
          TWO))));
  }

  @Test
  public void conditionalIfBoolTrueElse() {
    // if (bool) {return 1} else if (true) {return 2} else {return 3}
    Expression bool = Expressions.parameter(boolean.class, "bool");
    assertEquals("{\n"
                 + "  if (bool) {\n"
                 + "    return 1;\n"
                 + "  } else {\n"
                 + "    return 2;\n"
                 + "  }\n"
                 + "}\n", optimize(Expressions.ifThenElse(bool,
        Expressions.return_(null, ONE), TRUE, Expressions.return_(null,
          TWO), Expressions.return_(null, THREE))));

  }

  @Test
  public void conditionalIfBoolFalse() {
    // if (bool) {return 1} else if (false) {return 2}
    Expression bool = Expressions.parameter(boolean.class, "bool");
    assertEquals("{\n"
                 + "  if (bool) {\n"
                 + "    return 1;\n"
                 + "  }\n"
                 + "}\n", optimize(Expressions.ifThenElse(bool,
        Expressions.return_(null, ONE), FALSE, Expressions.return_(null,
          TWO))));
  }

  @Test
  public void conditionalIfBoolFalseElse() {
    // if (bool) {return 1} else if (false) {return 2} else {return 3}
    Expression bool = Expressions.parameter(boolean.class, "bool");
    assertEquals("{\n"
                 + "  if (bool) {\n"
                 + "    return 1;\n"
                 + "  } else {\n"
                 + "    return 3;\n"
                 + "  }\n"
                 + "}\n", optimize(Expressions.ifThenElse(bool,
        Expressions.return_(null, ONE), FALSE, Expressions.return_(null,
          TWO), Expressions.return_(null, THREE))));
  }

  @Test
  public void conditionalIfBoolFalseTrue() {
    // if (bool) {1} else if (false) {2} if (true) {4} else {5}
    Expression bool = Expressions.parameter(boolean.class, "bool");
    assertEquals("{\n"
                 + "  if (bool) {\n"
                 + "    return 1;\n"
                 + "  } else {\n"
                 + "    return 4;\n"
                 + "  }\n"
                 + "}\n", optimize(Expressions.ifThenElse(bool,
        Expressions.return_(null, ONE), FALSE, Expressions.return_(null,
          TWO), TRUE, Expressions.return_(null, FOUR),
          Expressions.return_(null, Expressions.constant(5)))));
  }

  @Test
  public void castIntToShort() {
    // return (short) 1 --> return (short) 1
    assertEquals("{\n  return (short)1;\n}\n", optimize(Expressions.convert_(
        ONE, short.class)));
  }

  @Test
  public void castIntToInt() {
    // return (int) 1 --> return 1L
    assertEquals("{\n  return 1;\n}\n", optimize(Expressions.convert_(ONE,
        int.class)));
  }

  @Test
  public void castIntToLong() {
    // return (long) 1 --> return 1L
    assertEquals("{\n  return 1L;\n}\n", optimize(Expressions.convert_(ONE,
        long.class)));
  }
}
