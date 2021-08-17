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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.extension.ConditionEvaluationResult.disabled;
import static org.junit.jupiter.api.extension.ConditionEvaluationResult.enabled;

/**
 * Test for {@link Types#gcd}.
 */
class TypeTest {

  @Test void testGcd() {
    int i = 0;
    char c = 0;
    byte b = 0;
    short s = 0;
    int l = 0;

    // int to long
    l = i;
    assertEquals(long.class, Types.gcd(int.class, long.class));

    // reverse args
    assertEquals(long.class, Types.gcd(long.class, int.class));

    // char to int
    i = c;
    assertEquals(int.class, Types.gcd(char.class, int.class));

    // can assign byte to short
    assertEquals(short.class, Types.gcd(byte.class, short.class));
    s = b;

    // cannot assign byte to char
    // cannot assign char to short
    // can assign byte and char to int
    // fails: c = b;
    // fails: s = c;
    i = b;
    i = c;
    assertEquals(int.class, Types.gcd(char.class, byte.class));

    assertEquals(int.class, Types.gcd(byte.class, char.class));

    // mix a primitive with an object
    // (correct answer is java.io.Serializable)
    assertEquals(Object.class, Types.gcd(String.class, int.class));
    java.io.Serializable o = true ? "x" : 1;
  }

  private static void assertFields(List<Field> expected, List<Field> computed, Class<?> clazz) {
    assertEquals(expected, computed, "The list of computed fields for class "
        + clazz.getName() + " does not match the list of its public and non-static fields");
  }

  @Test void testExcludedStaticFieldsAndViaAnnotation() throws NoSuchFieldException {
    List<Field> fieldNames = Collections.singletonList(A.class.getField("strField"));

    assertFields(fieldNames, Types.getClassFields(A.class), A.class);
  }

  @Test void testAlphabeticalAndHierarchicalOrder() throws NoSuchFieldException {
    List<Field> fieldNames = Arrays.asList(AB.class.getField("aField"),
        A.class.getField("strField"),
        AB.class.getField("zField"));

    assertFields(
        fieldNames, Types.getClassFields(AB.class, true,
        Types.FieldsOrdering.ALPHABETICAL, null), AB.class);
  }

  @Test void testOrderViaFieldNamesAndClassHierarchy() throws NoSuchFieldException {
    List<Field> fieldNames = Arrays.asList(A.class.getField("strField"),
        AB.class.getField("aField"),
        AB.class.getField("zField"));

    assertFields(
        fieldNames, Types.getClassFields(AB.class, true,
        Types.FieldsOrdering.ALPHABETICAL_AND_HIERARCHY, null), AB.class);
  }

  @Test void testNoHierarchyConstructorAlphabetical() throws NoSuchFieldException {
    List<Field> fieldNames = Arrays.asList(AB.class.getField("aField"),
        AB.class.getField("zField"));

    assertFields(
        fieldNames, Types.getClassFields(AB.class, false,
        Types.FieldsOrdering.CONSTRUCTOR, null), AB.class);
  }

  @TestIfParameterNames(clazz = AC.class)
  void testNoHierarchyConstructorInDeclarationOrder() throws NoSuchFieldException {
    List<Field> fieldNames = Arrays.asList(AC.class.getField("zField"),
        AC.class.getField("aField"));

    assertFields(
        fieldNames, Types.getClassFields(AC.class, false,
        Types.FieldsOrdering.CONSTRUCTOR, null), AC.class);
  }


  @TestIfParameterNames(clazz = AB.class)
  void testHierarchyConstructorAlphabetical() throws NoSuchFieldException {
    List<Field> fieldNames = Arrays.asList(AB.class.getField("aField"),
        AB.class.getField("zField"),
        A.class.getField("strField"));

    assertFields(
        fieldNames, Types.getClassFields(AB.class, true,
        Types.FieldsOrdering.CONSTRUCTOR, null), AB.class);
  }

  @TestIfParameterNames(clazz = AC.class)
  void testHierarchyConstructorInDeclarationOrder() throws NoSuchFieldException {
    List<Field> fieldNames = Arrays.asList(A.class.getField("strField"),
        AC.class.getField("zField"),
        AC.class.getField("aField"));

    assertFields(
        fieldNames, Types.getClassFields(AC.class, true,
            Types.FieldsOrdering.CONSTRUCTOR, null), AC.class);
  }

  @TestIfParameterNames(clazz = AD.class)
  void testIncompleteConstructors() throws NoSuchFieldException {
    List<Field> fieldNames = Arrays.asList(A.class.getField("strField"),
        AD.class.getField("zField"),
        AD.class.getField("aField"));

    assertFields(
        fieldNames, Types.getClassFields(AD.class, true,
            Types.FieldsOrdering.CONSTRUCTOR, null), AD.class);
  }

  @Test void testExplicitFieldNames() throws NoSuchFieldException {
    Map<Class, List<Field>> classFieldsMap = new HashMap<>();
    List<Field> fieldNames = Arrays.asList(AC.class.getField("zField"),
        A.class.getField("strField"),
        AC.class.getField("aField"));
    classFieldsMap.put(AC.class, fieldNames);

    assertFields(
        fieldNames, Types.getClassFields(AC.class, true,
            Types.FieldsOrdering.EXPLICIT, classFieldsMap), AC.class);
  }

  @Test void testExplicitFieldNamesIncompleteKO() throws NoSuchFieldException {
    Map<Class, List<Field>> classFieldsMap = new HashMap<>();
    List<Field> fieldNames = Arrays.asList(AC.class.getField("zField"),
        A.class.getField("strField"));
    classFieldsMap.put(AC.class, fieldNames);
    Field missingField = AC.class.getField("aField");

    Types.FieldsOrdering explicitFieldsOrdering = Types.FieldsOrdering.EXPLICIT;

    IllegalArgumentException thrown = assertThrows(
        IllegalArgumentException.class,
        () -> Types.getClassFields(AC.class, true, explicitFieldsOrdering, classFieldsMap),
        "Expected 'Types.getClassFields' to throw on incomplete list of fields ("
            + missingField + " is missing)"
    );

    String expectedErrorPrefix = "Incomplete list of fields is not compatible with \""
        + explicitFieldsOrdering + "\"";
    assertThat(thrown.getMessage(), containsString(expectedErrorPrefix));
  }

  @Test void testExplicitTolerantFieldNamesIncomplete() throws NoSuchFieldException {
    Map<Class, List<Field>> classFieldsMap = new HashMap<>();
    List<Field> fieldNames = Arrays.asList(AC.class.getField("zField"),
        A.class.getField("strField"));
    classFieldsMap.put(AC.class, fieldNames);

    assertFields(
        fieldNames, Types.getClassFields(AC.class, true,
            Types.FieldsOrdering.EXPLICIT_TOLERANT, classFieldsMap), AC.class);
  }

  /** Parent class with ignored (static and private) fields. */
  @SuppressWarnings({"unused", "NotNullFieldNotInitialized"})
  private static class A {
    public String strField;
    public static Integer staticField;
    private long privateField;

    A(String strField, long privateField){
    }

    A(String strField){
    }
  }

  /** Second-level derived class, constructor with parameters in different order than
   * alphabetical order and declaration order. */
  @SuppressWarnings({"unused", "NotNullFieldNotInitialized"})
  private static class AA extends A {
    public Integer aField;
    public String zField;

    AA(String strField, String zField, String aField) {
      super(strField);
    }
  }

  /** Second level derived class, one constructor in alphabetical order,
   * different than declaration order, the other one in alphabetical but
   * respecting the class hierarchy. */
  @SuppressWarnings({"unused", "NotNullFieldNotInitialized"})
  private static class AB extends A {
    public String zField;
    public Integer aField;

    AB(Integer aField, String zField, String strField) {
      super(strField);
    }

    AB(String strField, Integer aField, String zField) {
      super(strField);
    }
  }

  /** Second level derived class, multiple constructors. */
  @SuppressWarnings({"unused", "NotNullFieldNotInitialized"})
  private static class AC extends A {
    public String zField;
    public Integer aField;

    AC(String strField) {
      super(strField);
    }

    AC(String strField, String zField, Integer aField) {
      super(strField);
    }

    AC(String strField, Integer aField, String unrelatedField, String unrelatedField2) {
      super(strField);
    }
  }

  /** Second level derived class, constructors do not cover all fields. */
  @SuppressWarnings({"unused", "NotNullFieldNotInitialized"})
  private static class AD extends A {
    public String zField;
    public Integer aField;

    AD(String strField, String zField) {
      super(strField);
    }

    AD(String strField, Integer aField, String unrelatedField, String unrelatedField2) {
      super(strField);
    }
  }
}

/**
 * Enables the test conditionally if parameter names are available.
 */
class RequiresParameterNamesCondition implements ExecutionCondition {
  @Override public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    return context.getElement()
        .flatMap(element ->
            AnnotationSupport.findAnnotation(element, TestIfParameterNames.class))
        .map(pnc -> {
          if (pnc.clazz().getDeclaredConstructors()[0].getParameters()[0].isNamePresent()) {
            return enabled("Parameter names available for class " + pnc.clazz().getName());
          }
          return disabled("Parameter names are not available for class " + pnc.clazz().getName());
        })
        .orElseGet(() -> enabled("@RequiresParameterNames not found"));
  }
}

/**
 * Enables the test conditionally if parameter names are available for the given class.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
@Test
@ExtendWith(RequiresParameterNamesCondition.class)
@interface TestIfParameterNames {
  Class clazz();
}
