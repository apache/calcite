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
package org.apache.calcite.linq4j.tree;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link Types#isValidJavaIdentifier} and for the constructor
 * checks in the expression-tree nodes that emit an identifier into
 * generated source: {@link ParameterExpression} (also used by
 * {@link FieldDeclaration}) and {@link MemberExpression}.
 */
public class IdentifierValidationTest {
  /** A representative bad name: starts with a legal identifier character
   * but contains characters that {@code Character#isJavaIdentifierPart}
   * rejects. Historically only the first character was checked. */
  private static final String BAD_NAME = "a b.c";

  @Test void testIsValidJavaIdentifier() {
    assertThat(Types.isValidJavaIdentifier("a"), is(true));
    assertThat(Types.isValidJavaIdentifier("f0"), is(true));
    assertThat(Types.isValidJavaIdentifier("_a$b9"), is(true));
    assertThat(Types.isValidJavaIdentifier(""), is(false));
    assertThat(Types.isValidJavaIdentifier("9a"), is(false));
    assertThat(Types.isValidJavaIdentifier("a b"), is(false));
    assertThat(Types.isValidJavaIdentifier("a.b"), is(false));
    assertThat(Types.isValidJavaIdentifier("a\nb"), is(false));
    assertThat(Types.isValidJavaIdentifier(BAD_NAME), is(false));
    // First-character-only validation would accept this; the full check must reject it
    assertThat(Types.isValidJavaIdentifier("ok name"), is(false));
  }

  @Test void testParameterExpressionRejectsNonIdentifier() {
    // Valid names are accepted
    ParameterExpression p =
        new ParameterExpression(0, int.class, "p0");
    assertThat(p.name, is("p0"));
    assertThrows(IllegalArgumentException.class, () ->
        new ParameterExpression(0, int.class, BAD_NAME));
    assertThrows(IllegalArgumentException.class, () ->
        new ParameterExpression(0, int.class, "ok name"));
  }

  @Test void testFieldDeclarationCoveredViaParameterExpression() {
    // FieldDeclaration emits parameter.name as a field name; it takes a
    // ParameterExpression, so the constructor check above covers it
    assertThrows(IllegalArgumentException.class, () ->
        new FieldDeclaration(Modifier.PUBLIC,
            new ParameterExpression(0, int.class, BAD_NAME), null));
  }

  @Test void testMemberExpressionRejectsNonIdentifierFieldName() {
    // MemberExpression takes a PseudoField and emits field.getName()
    // directly; it does not go through ParameterExpression
    assertThrows(IllegalArgumentException.class, () ->
        new MemberExpression(null, new NamedStaticField(BAD_NAME)));
    // Sane names still work
    MemberExpression m =
        new MemberExpression(null, new NamedStaticField("f0"));
    assertThat(m.field.getName(), is("f0"));
  }

  /** A synthetic static field with an arbitrary name. */
  private static class NamedStaticField implements PseudoField {
    private final String name;

    NamedStaticField(String name) {
      this.name = name;
    }

    @Override public String getName() {
      return name;
    }

    @Override public Type getType() {
      return int.class;
    }

    @Override public int getModifiers() {
      return Modifier.PUBLIC | Modifier.STATIC;
    }

    @Override public @Nullable Object get(@Nullable Object o) {
      return 0;
    }

    @Override public Type getDeclaringClass() {
      return Object.class;
    }
  }
}
