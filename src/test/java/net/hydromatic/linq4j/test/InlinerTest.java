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

import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Modifier;

import static net.hydromatic.linq4j.test.BlockBuilderBase.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Tests expression inlining in BlockBuilder.
 */
public class InlinerTest {
  BlockBuilder b;

  @Before
  public void prepareBuilder() {
    b = new BlockBuilder(true);
  }

  @Test public void testInlineSingleUsage() {
    DeclarationStatement decl = Expressions.declare(16, "x",
        Expressions.add(ONE, TWO));
    b.add(decl);
    b.add(Expressions.return_(null, decl.parameter));
    assertEquals("{\n  return 1 + 2;\n}\n", b.toBlock().toString());
  }

  @Test public void testInlineConstant() {
    DeclarationStatement decl = Expressions.declare(16, "x", ONE);
    b.add(decl);
    b.add(
        Expressions.return_(null,
            Expressions.add(decl.parameter, decl.parameter)));
    assertEquals("{\n  return 1 + 1;\n}\n", b.toBlock().toString());
  }

  @Test public void testInlineParameter() {
    ParameterExpression pe = Expressions.parameter(int.class, "p");
    DeclarationStatement decl = Expressions.declare(16, "x", pe);
    b.add(decl);
    b.add(
        Expressions.return_(null,
            Expressions.add(decl.parameter, decl.parameter)));
    assertEquals("{\n  return p + p;\n}\n", b.toBlock().toString());
  }

  @Test public void testNoInlineMultipleUsage() {
    ParameterExpression p1 = Expressions.parameter(int.class, "p1");
    ParameterExpression p2 = Expressions.parameter(int.class, "p2");
    DeclarationStatement decl = Expressions.declare(16, "x",
        Expressions.subtract(p1, p2));
    b.add(decl);
    b.add(
        Expressions.return_(null,
            Expressions.add(decl.parameter, decl.parameter)));
    assertEquals(
        "{\n"
        + "  final int x = p1 - p2;\n"
        + "  return x + x;\n"
        + "}\n",
        b.toBlock().toString());
  }

  @Test public void testAssignInConditionMultipleUsage() {
    // int t;
    // return (t = 1) != a ? t : c
    final BlockBuilder builder = new BlockBuilder(true);
    final ParameterExpression t = Expressions.parameter(int.class, "t");

    builder.add(Expressions.declare(0, t, null));

    Expression v = builder.append("v",
        Expressions.makeTernary(ExpressionType.Conditional,
            Expressions.makeBinary(ExpressionType.NotEqual,
                Expressions.assign(t, Expressions.constant(1)),
                Expressions.parameter(int.class, "a")),
            t,
            Expressions.parameter(int.class, "c")));
    builder.add(Expressions.return_(null, v));
    assertEquals(
        "{\n"
        + "  int t;\n"
        + "  return (t = 1) != a ? t : c;\n"
        + "}\n",
        Expressions.toString(builder.toBlock()));
  }

  @Test public void testAssignInConditionOptimizedOut() {
    checkAssignInConditionOptimizedOut(Modifier.FINAL,
        "{\n"
        + "  return 1 != a ? b : c;\n"
        + "}\n");
  }

  @Test public void testAssignInConditionNotOptimizedWithoutFinal() {
    checkAssignInConditionOptimizedOut(0,
        "{\n"
        + "  int t;\n"
        + "  return (t = 1) != a ? b : c;\n"
        + "}\n");
  }

  void checkAssignInConditionOptimizedOut(int modifiers, String s) {
    // int t;
    // return (t = 1) != a ? b : c
    final BlockBuilder builder = new BlockBuilder(true);
    final ParameterExpression t =
        Expressions.parameter(int.class, "t");

    builder.add(Expressions.declare(modifiers, t, null));

    Expression v = builder.append("v",
        Expressions.makeTernary(ExpressionType.Conditional,
            Expressions.makeBinary(ExpressionType.NotEqual,
                Expressions.assign(t, Expressions.constant(1)),
                Expressions.parameter(int.class, "a")),
            Expressions.parameter(int.class, "b"),
            Expressions.parameter(int.class, "c")));
    builder.add(Expressions.return_(null, v));
    assertThat(Expressions.toString(builder.toBlock()),
        CoreMatchers.equalTo(s));
  }

  @Test public void testAssignInConditionMultipleUsageNonOptimized() {
    // int t = 2;
    // return (t = 1) != a ? 1 : c
    final BlockBuilder builder = new BlockBuilder(true);
    final ParameterExpression t = Expressions.parameter(int.class, "t");

    builder.add(Expressions.declare(0, t, TWO));

    Expression v = builder.append("v",
        Expressions.makeTernary(ExpressionType.Conditional,
            Expressions.makeBinary(ExpressionType.NotEqual,
                Expressions.assign(t, Expressions.constant(1)),
                Expressions.parameter(int.class, "a")),
            t,
            Expressions.parameter(int.class, "c")));
    builder.add(Expressions.return_(null, v));
    assertEquals(
        "{\n"
        + "  int t = 2;\n"
        + "  return (t = 1) != a ? t : c;\n"
        + "}\n",
        Expressions.toString(builder.toBlock()));
  }

  @Test public void testMultiPassOptimization() {
    // int t = u + v;
    // boolean b = t > 1 ? true : true; -- optimized out, thus t can be inlined
    // return b ? t : 2
    final BlockBuilder builder = new BlockBuilder(true);
    final ParameterExpression u = Expressions.parameter(int.class, "u");
    final ParameterExpression v = Expressions.parameter(int.class, "v");

    Expression t = builder.append("t", Expressions.add(u, v));
    Expression b = builder.append("b",
        Expressions.condition(Expressions.greaterThan(t, ONE), TRUE, TRUE));

    builder.add(Expressions.return_(null, Expressions.condition(b, t, TWO)));
    assertEquals(
        "{\n"
        + "  return u + v;\n"
        + "}\n",
        Expressions.toString(builder.toBlock()));
  }
}

// End InlinerTest.java
