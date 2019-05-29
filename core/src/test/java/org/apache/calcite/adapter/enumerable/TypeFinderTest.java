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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.FunctionExpression;
import org.apache.calcite.linq4j.tree.Node;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.UnaryExpression;

import org.hamcrest.BaseMatcher;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Test for {@link org.apache.calcite.adapter.enumerable.EnumerableRelImplementor.TypeFinder}
 */
public class TypeFinderTest {

  @Test public void testConstantExpression() {
    ConstantExpression expr = Expressions.constant(null, Integer.class);
    assertJavaCodeContains("(Integer) null\n", expr);
    assertTypeContains(Integer.class, expr);
  }

  @Test public void testConvertExpression() {
    UnaryExpression expr = Expressions.convert_(Expressions.new_(String.class), Object.class);
    assertJavaCodeContains("(Object) new String()\n", expr);
    assertTypeContains(Arrays.asList(String.class, Object.class), expr);
  }

  @Test public void testFunctionExpression1() {
    ParameterExpression param = Expressions.parameter(String.class, "input");
    FunctionExpression expr = Expressions.lambda(Function1.class,
        Expressions.block(
            Expressions.return_(null, param)),
        param);
    assertJavaCodeContains("new org.apache.calcite.linq4j.function.Function1() {\n"
        + "  public String apply(String input) {\n"
        + "    return input;\n"
        + "  }\n"
        + "  public Object apply(Object input) {\n"
        + "    return apply(\n"
        + "      (String) input);\n"
        + "  }\n"
        + "}\n", expr);
    assertTypeContains(String.class, expr);
  }

  @Test public void testFunctionExpression2() {
    FunctionExpression expr = Expressions.lambda(Function1.class,
        Expressions.block(
            Expressions.return_(null, Expressions.constant(1L, Long.class))),
        Expressions.parameter(String.class, "input"));
    assertJavaCodeContains("new org.apache.calcite.linq4j.function.Function1() {\n"
        + "  public Long apply(String input) {\n"
        + "    return Long.valueOf(1L);\n"
        + "  }\n"
        + "  public Object apply(Object input) {\n"
        + "    return apply(\n"
        + "      (String) input);\n"
        + "  }\n"
        + "}\n", expr);
    assertTypeContains(Arrays.asList(String.class, Long.class), expr);
  }
  private void assertJavaCodeContains(String expected, Node node) {
    assertJavaCodeContains(expected, Collections.singletonList(node));
  }

  private void assertJavaCodeContains(String expected, List<Node> nodes) {
    final String javaCode = Expressions.toString(nodes, "\n", false);
    Assert.assertThat(javaCode, CoreMatchers.containsString(expected));
  }

  private void assertTypeContains(Type expectedType, Node node) {
    assertTypeContains(Collections.singletonList(expectedType),
        Collections.singletonList(node));
  }

  private void assertTypeContains(List<Type> expectedType, Node node) {
    assertTypeContains(expectedType,
        Collections.singletonList(node));
  }

  private void assertTypeContains(List<Type> expectedTypes, List<Node> nodes) {
    final HashSet<Type> types = new HashSet<>();
    final EnumerableRelImplementor.TypeFinder typeFinder =
        new EnumerableRelImplementor.TypeFinder(types);
    for (Node node : nodes) {
      node.accept(typeFinder);
    }
    Assert.assertThat(types, new BaseMatcher<HashSet<Type>>() {
      @Override public boolean matches(Object o) {
        final Set<Type> actual = (HashSet<Type>) o;
        return actual.containsAll(expectedTypes);
      }

      @Override public void describeTo(Description description) {
        description.appendText("Expected a set of types containing all of: ")
            .appendText(Objects.toString(expectedTypes));
      }
    });
  }
}

// End TypeFinderTest.java
