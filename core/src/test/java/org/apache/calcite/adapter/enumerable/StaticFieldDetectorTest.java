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

import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.FieldDeclaration;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link EnumerableInterpretable.StaticFieldDetector}.
 */
public final class StaticFieldDetectorTest {

  @Test void testClassWithoutStaticFields() {
    ClassDeclaration classDeclaration =
        createClassDeclaration(
            new FieldDeclaration(
                Modifier.PUBLIC,
                Expressions.parameter(int.class, "x"),
                Expressions.constant(0)));

    EnumerableInterpretable.StaticFieldDetector detector =
        new EnumerableInterpretable.StaticFieldDetector();
    classDeclaration.accept(detector);
    assertThat(detector.containsStaticField, is(false));
  }

  @Test void testClassWithOnlyStaticFields() {
    ClassDeclaration classDeclaration =
        createClassDeclaration(
            new FieldDeclaration(
                Modifier.PUBLIC | Modifier.STATIC,
                Expressions.parameter(int.class, "x"),
                Expressions.constant(0)),
            new FieldDeclaration(
                Modifier.STATIC,
                Expressions.parameter(int.class, "y"),
                Expressions.constant(0)));

    EnumerableInterpretable.StaticFieldDetector detector =
        new EnumerableInterpretable.StaticFieldDetector();
    classDeclaration.accept(detector);
    assertThat(detector.containsStaticField, is(true));
  }

  @Test void testClassWithStaticAndNonStaticFields() {
    ClassDeclaration classDeclaration =
        createClassDeclaration(
            new FieldDeclaration(
                Modifier.PUBLIC | Modifier.STATIC,
                Expressions.parameter(int.class, "x"),
                Expressions.constant(0)),
            new FieldDeclaration(
                Modifier.PUBLIC,
                Expressions.parameter(int.class, "y"),
                Expressions.constant(0)));

    EnumerableInterpretable.StaticFieldDetector detector =
        new EnumerableInterpretable.StaticFieldDetector();
    classDeclaration.accept(detector);
    assertThat(detector.containsStaticField, is(true));
  }

  @Test void testClassWithNonStaticAndStaticFields() {
    ClassDeclaration classDeclaration =
        createClassDeclaration(
            new FieldDeclaration(
                Modifier.PUBLIC,
                Expressions.parameter(int.class, "x"),
                Expressions.constant(0)),
            new FieldDeclaration(
                Modifier.PUBLIC | Modifier.STATIC,
                Expressions.parameter(int.class, "y"),
                Expressions.constant(0)));

    EnumerableInterpretable.StaticFieldDetector detector =
        new EnumerableInterpretable.StaticFieldDetector();
    classDeclaration.accept(detector);
    assertThat(detector.containsStaticField, is(true));
  }

  private static ClassDeclaration createClassDeclaration(FieldDeclaration... fieldDeclarations) {
    return new ClassDeclaration(
        Modifier.PUBLIC,
        "MyClass",
        null,
        ImmutableList.of(),
        Arrays.asList(fieldDeclarations));
  }

}
