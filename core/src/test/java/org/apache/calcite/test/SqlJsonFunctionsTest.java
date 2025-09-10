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
package org.apache.calcite.test;

import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.JsonFunctions;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlJsonConstructorNullClause;
import org.apache.calcite.sql.SqlJsonExistsErrorBehavior;
import org.apache.calcite.sql.SqlJsonQueryEmptyOrErrorBehavior;
import org.apache.calcite.sql.SqlJsonQueryWrapperBehavior;
import org.apache.calcite.sql.SqlJsonValueEmptyOrErrorBehavior;
import org.apache.calcite.util.BuiltInMethod;

import com.google.common.primitives.Longs;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.PathNotFoundException;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.calcite.test.Matchers.isListOf;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit test for the methods in {@link SqlFunctions} that implement JSON processing functions.
 */
class SqlJsonFunctionsTest {

  @Test void testJsonValueExpression() {
    assertJsonValueExpression("{}",
        is(JsonFunctions.JsonValueContext.withJavaObj(Collections.emptyMap())));
  }

  @Test void testJsonNullExpression() {
    assertJsonValueExpression("null",
        is(JsonFunctions.JsonValueContext.withJavaObj(null)));
  }

  @Test void testJsonApiCommonSyntax() {
    assertJsonApiCommonSyntax("{\"foo\": \"bar\"}", "$.foo",
        contextMatches(
            JsonFunctions.JsonPathContext.withJavaObj(JsonFunctions.PathMode.STRICT, "bar")));
    assertJsonApiCommonSyntax("{\"foo\": \"bar\"}", "lax $.foo",
        contextMatches(
            JsonFunctions.JsonPathContext.withJavaObj(JsonFunctions.PathMode.LAX, "bar")));
    assertJsonApiCommonSyntax("{\"foo\": \"bar\"}", "strict $.foo",
        contextMatches(
            JsonFunctions.JsonPathContext.withJavaObj(JsonFunctions.PathMode.STRICT, "bar")));
    assertJsonApiCommonSyntax("{\"foo\": \"bar\"}", "lax $.foo1",
        contextMatches(
            JsonFunctions.JsonPathContext.withJavaObj(JsonFunctions.PathMode.LAX, null)));
    assertJsonApiCommonSyntax("{\"foo\": \"bar\"}", "strict $.foo1",
        contextMatches(
            JsonFunctions.JsonPathContext.withStrictException(
                new PathNotFoundException("No results for path: $['foo1']"))));
    assertJsonApiCommonSyntax("{\"foo\": 100}", "lax $.foo",
        contextMatches(
            JsonFunctions.JsonPathContext.withJavaObj(JsonFunctions.PathMode.LAX, 100)));
  }

  @Test void testJsonExists() {
    assertJsonExists(
        JsonFunctions.JsonPathContext.withJavaObj(JsonFunctions.PathMode.STRICT, "bar"),
        SqlJsonExistsErrorBehavior.FALSE,
        is(true));

    assertJsonExists(
        JsonFunctions.JsonPathContext.withJavaObj(JsonFunctions.PathMode.STRICT, "bar"),
        SqlJsonExistsErrorBehavior.TRUE,
        is(true));

    assertJsonExists(
        JsonFunctions.JsonPathContext.withJavaObj(JsonFunctions.PathMode.STRICT, "bar"),
        SqlJsonExistsErrorBehavior.UNKNOWN,
        is(true));

    assertJsonExists(
        JsonFunctions.JsonPathContext.withJavaObj(JsonFunctions.PathMode.STRICT, "bar"),
        SqlJsonExistsErrorBehavior.ERROR,
        is(true));

    assertJsonExists(
        JsonFunctions.JsonPathContext.withJavaObj(JsonFunctions.PathMode.LAX, null),
        SqlJsonExistsErrorBehavior.FALSE,
        is(false));

    assertJsonExists(
        JsonFunctions.JsonPathContext.withJavaObj(JsonFunctions.PathMode.LAX, null),
        SqlJsonExistsErrorBehavior.TRUE,
        is(false));

    assertJsonExists(
        JsonFunctions.JsonPathContext.withJavaObj(JsonFunctions.PathMode.LAX, null),
        SqlJsonExistsErrorBehavior.UNKNOWN,
        is(false));

    assertJsonExists(
        JsonFunctions.JsonPathContext.withJavaObj(JsonFunctions.PathMode.LAX, null),
        SqlJsonExistsErrorBehavior.ERROR,
        is(false));

    assertJsonExists(
        JsonFunctions.JsonPathContext.withStrictException(new Exception("test message")),
        SqlJsonExistsErrorBehavior.FALSE,
        is(false));

    assertJsonExists(
        JsonFunctions.JsonPathContext.withStrictException(new Exception("test message")),
        SqlJsonExistsErrorBehavior.TRUE,
        is(true));

    assertJsonExists(
        JsonFunctions.JsonPathContext.withStrictException(new Exception("test message")),
        SqlJsonExistsErrorBehavior.UNKNOWN,
        nullValue());

    assertJsonExistsFailed(
        JsonFunctions.JsonPathContext.withStrictException(new Exception("test message")),
        SqlJsonExistsErrorBehavior.ERROR,
        errorMatches(new RuntimeException("java.lang.Exception: test message")));
  }

  @Test void testJsonValueAny() {
    assertJsonValueAny(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.LAX, "bar"),
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        is("bar"));
    assertJsonValueAny(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.LAX, null),
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        nullValue());
    assertJsonValueAny(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.LAX, null),
        SqlJsonValueEmptyOrErrorBehavior.DEFAULT,
        "empty",
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        is("empty"));
    assertJsonValueAnyFailed(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.LAX, null),
        SqlJsonValueEmptyOrErrorBehavior.ERROR,
        null,
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        errorMatches(
            new CalciteException("Empty result of JSON_VALUE function is not "
                + "allowed", null)));
    assertJsonValueAny(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.LAX, Collections.emptyList()),
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        nullValue());
    assertJsonValueAny(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.LAX, Collections.emptyList()),
        SqlJsonValueEmptyOrErrorBehavior.DEFAULT,
        "empty",
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        is("empty"));
    assertJsonValueAnyFailed(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.LAX, Collections.emptyList()),
        SqlJsonValueEmptyOrErrorBehavior.ERROR,
        null,
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        errorMatches(
            new CalciteException("Empty result of JSON_VALUE function is not "
                + "allowed", null)));
    assertJsonValueAny(
        JsonFunctions.JsonPathContext
            .withStrictException(new Exception("test message")),
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        nullValue());
    assertJsonValueAny(
        JsonFunctions.JsonPathContext
            .withStrictException(new Exception("test message")),
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        SqlJsonValueEmptyOrErrorBehavior.DEFAULT,
        "empty",
        is("empty"));
    assertJsonValueAnyFailed(
        JsonFunctions.JsonPathContext
            .withStrictException(new Exception("test message")),
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        SqlJsonValueEmptyOrErrorBehavior.ERROR,
        null,
        errorMatches(
            new RuntimeException("java.lang.Exception: test message")));
    assertJsonValueAny(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.STRICT, Collections.emptyList()),
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        nullValue());
    assertJsonValueAny(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.STRICT, Collections.emptyList()),
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        SqlJsonValueEmptyOrErrorBehavior.DEFAULT,
        "empty",
        is("empty"));
    assertJsonValueAnyFailed(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.STRICT, Collections.emptyList()),
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        SqlJsonValueEmptyOrErrorBehavior.ERROR,
        null,
        errorMatches(
            new CalciteException("Strict jsonpath mode requires scalar value, "
                + "and the actual value is: '[]'", null)));
  }

  @Test void testJsonQuery() {
    assertJsonQuery(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.LAX, Collections.singletonList("bar")),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        is("[\"bar\"]"));
    assertJsonQuery(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.LAX, null),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        nullValue());
    assertJsonQuery(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.LAX, null),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.EMPTY_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        is("[]"));
    assertJsonQuery(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.LAX, null),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.EMPTY_OBJECT,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        is("{}"));
    assertJsonQueryFailed(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.LAX, null),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.ERROR,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        errorMatches(
            new CalciteException("Empty result of JSON_QUERY function is not "
                + "allowed", null)));

    assertJsonQuery(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.LAX, "bar"),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        nullValue());
    assertJsonQuery(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.LAX, "bar"),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.EMPTY_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        is("[]"));
    assertJsonQuery(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.LAX, "bar"),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.EMPTY_OBJECT,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        is("{}"));
    assertJsonQueryFailed(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.LAX, "bar"),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.ERROR,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        errorMatches(
            new CalciteException("Empty result of JSON_QUERY function is not "
                + "allowed", null)));
    assertJsonQuery(
        JsonFunctions.JsonPathContext
            .withStrictException(new Exception("test message")),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.EMPTY_ARRAY,
        is("[]"));
    assertJsonQuery(
        JsonFunctions.JsonPathContext
            .withStrictException(new Exception("test message")),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.EMPTY_OBJECT,
        is("{}"));
    assertJsonQueryFailed(
        JsonFunctions.JsonPathContext
            .withStrictException(new Exception("test message")),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.ERROR,
        errorMatches(
            new RuntimeException("java.lang.Exception: test message")));
    assertJsonQuery(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.STRICT, "bar"),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        nullValue());
    assertJsonQuery(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.STRICT, "bar"),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.EMPTY_ARRAY,
        is("[]"));
    assertJsonQueryFailed(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.STRICT, "bar"),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.ERROR,
        errorMatches(
            new CalciteException("Strict jsonpath mode requires array or "
                + "object value, and the actual value is: 'bar'", null)));

    // wrapper behavior test

    assertJsonQuery(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.STRICT, "bar"),
        SqlJsonQueryWrapperBehavior.WITH_UNCONDITIONAL_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        is("[\"bar\"]"));

    assertJsonQuery(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.STRICT, "bar"),
        SqlJsonQueryWrapperBehavior.WITH_CONDITIONAL_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        is("[\"bar\"]"));

    assertJsonQuery(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.STRICT,
                Collections.singletonList("bar")),
        SqlJsonQueryWrapperBehavior.WITH_UNCONDITIONAL_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        is("[[\"bar\"]]"));

    assertJsonQuery(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.STRICT,
                Collections.singletonList("bar")),
        SqlJsonQueryWrapperBehavior.WITH_CONDITIONAL_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        is("[\"bar\"]"));

    // jsonize test

    assertJsonQuery(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.STRICT,
                Collections.singletonList("bar")),
        SqlJsonQueryWrapperBehavior.WITH_CONDITIONAL_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        false,
        isListOf("bar"));
    assertJsonQuery(
        JsonFunctions.JsonPathContext
            .withUnknownException(new Exception("test message")),
        SqlJsonQueryWrapperBehavior.WITH_CONDITIONAL_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.EMPTY_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.EMPTY_ARRAY,
        false,
        is(Collections.emptyList()));
  }

  @Test void testJsonize() {
    assertJsonize(new HashMap<>(),
        is("{}"));
  }

  @Test void assertJsonPretty() {
    assertJsonPretty(
        JsonFunctions.JsonValueContext.withJavaObj(new HashMap<>()), is("{ }"));
    assertJsonPretty(
        JsonFunctions.JsonValueContext.withJavaObj(Longs.asList(1, 2)), is("[ 1, 2 ]"));
  }

  @Test void testDejsonize() {
    assertDejsonize("{}",
        is(Collections.emptyMap()));
    assertDejsonize("[]",
        is(Collections.emptyList()));

    // expect exception thrown
    final String message = "com.fasterxml.jackson.core.JsonParseException: "
        + "Unexpected close marker '}': expected ']' (for Array starting at "
        + "[Source: REDACTED (`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` "
        + "disabled); line: 1, column: 1])\n at [Source: REDACTED "
        + "(`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled); line: "
        + "1, column: 2]";
    assertDejsonizeFailed("[}",
        errorMatches(new InvalidJsonException(message)));
  }

  @Test void testJsonObject() {
    assertJsonObject(is("{}"), SqlJsonConstructorNullClause.NULL_ON_NULL);
    assertJsonObject(
        is("{\"foo\":\"bar\"}"), SqlJsonConstructorNullClause.NULL_ON_NULL,
        "foo",
        "bar");
    assertJsonObject(
        is("{\"foo\":null}"), SqlJsonConstructorNullClause.NULL_ON_NULL,
        "foo",
        null);
    assertJsonObject(
        is("{}"), SqlJsonConstructorNullClause.ABSENT_ON_NULL,
        "foo",
        null);
  }

  @Test void testJsonType() {
    assertJsonType(is("OBJECT"), "{}");
    assertJsonType(is("ARRAY"),
        "[\"foo\",null]");
    assertJsonType(is("NULL"), "null");
    assertJsonType(is("BOOLEAN"), "false");
    assertJsonType(is("INTEGER"), "12");
    assertJsonType(is("DOUBLE"), "11.22");
  }

  @Test void testJsonDepth() {
    assertJsonDepth(is(1), "{}");
    assertJsonDepth(is(1), "false");
    assertJsonDepth(is(1), "12");
    assertJsonDepth(is(1), "11.22");
    assertJsonDepth(is(2),
        "[\"foo\",null]");
    assertJsonDepth(is(3),
        "{\"a\": [10, true]}");
    assertJsonDepth(nullValue(), "null");
  }

  @Test void testJsonLength() {
    assertJsonLength(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.LAX, Collections.singletonList("bar")),
        is(1));
    assertJsonLength(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.LAX, null),
        nullValue());
    assertJsonLength(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.STRICT, Collections.singletonList("bar")),
        is(1));
    assertJsonLength(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.LAX, "bar"),
        is(1));
  }

  @Test void testJsonKeys() {
    assertJsonKeys(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.LAX, Collections.singletonList("bar")),
        is("null"));
    assertJsonKeys(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.LAX, null),
        is("null"));
    assertJsonKeys(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.STRICT, Collections.singletonList("bar")),
        is("null"));
    assertJsonKeys(
        JsonFunctions.JsonPathContext
            .withJavaObj(JsonFunctions.PathMode.LAX, "bar"),
        is("null"));
  }

  @Test void testJsonRemove() {
    assertJsonRemove(
        JsonFunctions.jsonValueExpression("{\"a\": 1, \"b\": [2]}"),
        new String[]{"$.a"},
        is("{\"b\":[2]}"));
    assertJsonRemove(
        JsonFunctions.jsonValueExpression("{\"a\": 1, \"b\": [2]}"),
        new String[]{"$.a", "$.b"},
        is("{}"));
  }

  @Test void testJsonStorageSize() {
    assertJsonStorageSize("[100, \"sakila\", [1, 3, 5], 425.05]", is(29));
    assertJsonStorageSize("null", is(4));
    assertJsonStorageSize(JsonFunctions.JsonValueContext.withJavaObj(null), is(4));
  }

  @Test void testJsonObjectAggAdd() {
    Map<String, Object> map = new HashMap<>();
    Map<String, Object> expected = new HashMap<>();
    expected.put("foo", "bar");
    assertJsonObjectAggAdd(map, "foo", "bar",
        SqlJsonConstructorNullClause.NULL_ON_NULL, is(expected));
    expected.put("foo1", null);
    assertJsonObjectAggAdd(map, "foo1", null,
        SqlJsonConstructorNullClause.NULL_ON_NULL, is(expected));
    assertJsonObjectAggAdd(map, "foo2", null,
        SqlJsonConstructorNullClause.ABSENT_ON_NULL, is(expected));
  }

  @Test void testJsonArray() {
    assertJsonArray(is("[]"), SqlJsonConstructorNullClause.NULL_ON_NULL);
    assertJsonArray(
        is("[\"foo\"]"), SqlJsonConstructorNullClause.NULL_ON_NULL, "foo");
    assertJsonArray(
        is("[\"foo\",null]"), SqlJsonConstructorNullClause.NULL_ON_NULL,
        "foo",
        null);
    assertJsonArray(
        is("[\"foo\"]"),
        SqlJsonConstructorNullClause.ABSENT_ON_NULL,
        "foo",
        null);
  }

  @Test void testJsonArrayAggAdd() {
    List<Object> list = new ArrayList<>();
    List<Object> expected = new ArrayList<>();
    expected.add("foo");
    assertJsonArrayAggAdd(list, "foo",
        SqlJsonConstructorNullClause.NULL_ON_NULL, is(expected));
    expected.add(null);
    assertJsonArrayAggAdd(list, null,
        SqlJsonConstructorNullClause.NULL_ON_NULL, is(expected));
    assertJsonArrayAggAdd(list, null,
        SqlJsonConstructorNullClause.ABSENT_ON_NULL, is(expected));
  }

  @Test void testJsonPredicate() {
    assertIsJsonValue("[]", is(true));
    assertIsJsonValue("{}", is(true));
    assertIsJsonValue("100", is(true));
    assertIsJsonValue("{]", is(false));
    assertIsJsonValue(null, nullValue());
    assertIsJsonObject("[]", is(false));
    assertIsJsonObject("{}", is(true));
    assertIsJsonObject("100", is(false));
    assertIsJsonObject("{]", is(false));
    assertIsJsonObject(null, nullValue());
    assertIsJsonArray("[]", is(true));
    assertIsJsonArray("{}", is(false));
    assertIsJsonArray("100", is(false));
    assertIsJsonArray("{]", is(false));
    assertIsJsonArray(null, nullValue());
    assertIsJsonScalar("[]", is(false));
    assertIsJsonScalar("{}", is(false));
    assertIsJsonScalar("100", is(true));
    assertIsJsonScalar("{]", is(false));
    assertIsJsonScalar(null, nullValue());
  }

  @Test public void testJsonInsert() {
    assertJsonInsert(
        JsonFunctions.jsonValueExpression("{\"a\": 1, \"b\": [2]}"),
        new Object[]{"$.a", 10, "$.c", "[true]"},
        is("{\"a\":1,\"b\":[2],\"c\":\"[true]\"}"));
    assertJsonInsert(
        JsonFunctions.jsonValueExpression("{\"a\": 1, \"b\": [2]}"),
        new Object[]{"$", 10, "$.c", "[true]"},
        is("{\"a\":1,\"b\":[2],\"c\":\"[true]\"}"));
  }

  @Test public void testJsonReplace() {
    assertJsonReplace(
        JsonFunctions.jsonValueExpression("{\"a\": 1, \"b\": [2]}"),
        new Object[]{"$.a", 10, "$.c", "[true]"},
        is("{\"a\":10,\"b\":[2]}"));
    assertJsonReplace(
        JsonFunctions.jsonValueExpression("{\"a\": 1, \"b\": [2]}"),
        new Object[]{"$", 10, "$.c", "[true]"},
        is("10"));
  }

  @Test public void testJsonSet() {
    assertJsonSet(
        JsonFunctions.jsonValueExpression("{\"a\": 1, \"b\": [2]}"),
        new Object[]{"$.a", 10, "$.c", "[true]"},
        is("{\"a\":10,\"b\":[2],\"c\":\"[true]\"}"));
    assertJsonSet(
        JsonFunctions.jsonValueExpression("{\"a\": 1, \"b\": [2]}"),
        new Object[]{"$", 10, "$.c", "[true]"},
        is("10"));
  }

  private void assertJsonValueExpression(String input,
      Matcher<? super JsonFunctions.JsonValueContext> matcher) {
    assertThat(
        invocationDesc(BuiltInMethod.JSON_VALUE_EXPRESSION, input),
        JsonFunctions.jsonValueExpression(input), matcher);
  }

  private void assertJsonApiCommonSyntax(String input, String pathSpec,
      Matcher<? super JsonFunctions.JsonPathContext> matcher) {
    assertThat(
        invocationDesc(BuiltInMethod.JSON_API_COMMON_SYNTAX, input, pathSpec),
        JsonFunctions.jsonApiCommonSyntax(input, pathSpec), matcher);
  }

  private void assertJsonApiCommonSyntax(JsonFunctions.JsonValueContext input,
      String pathSpec, Matcher<? super JsonFunctions.JsonPathContext> matcher) {
    assertThat(
        invocationDesc(BuiltInMethod.JSON_API_COMMON_SYNTAX, input, pathSpec),
        JsonFunctions.jsonApiCommonSyntax(input, pathSpec), matcher);
  }

  private void assertJsonExists(JsonFunctions.JsonPathContext context,
      SqlJsonExistsErrorBehavior errorBehavior,
      Matcher<? super Boolean> matcher) {
    final JsonFunctions.StatefulFunction f =
        new JsonFunctions.StatefulFunction();
    assertThat(
        invocationDesc(BuiltInMethod.JSON_EXISTS2, context, errorBehavior),
        f.jsonExists(context, errorBehavior), matcher);
  }

  private void assertJsonExistsFailed(JsonFunctions.JsonPathContext context,
      SqlJsonExistsErrorBehavior errorBehavior,
      Matcher<? super Throwable> matcher) {
    final JsonFunctions.StatefulFunction f =
        new JsonFunctions.StatefulFunction();
    assertFailed(
        invocationDesc(BuiltInMethod.JSON_EXISTS2, context, errorBehavior),
        () -> f.jsonExists(
            context, errorBehavior), matcher);
  }

  private void assertJsonValueAny(JsonFunctions.JsonPathContext context,
      SqlJsonValueEmptyOrErrorBehavior emptyBehavior,
      Object defaultValueOnEmpty,
      SqlJsonValueEmptyOrErrorBehavior errorBehavior,
      Object defaultValueOnError,
      Matcher<Object> matcher) {
    final JsonFunctions.StatefulFunction f =
        new JsonFunctions.StatefulFunction();
    assertThat(
        invocationDesc(BuiltInMethod.JSON_VALUE, context, emptyBehavior,
            defaultValueOnEmpty, errorBehavior, defaultValueOnError),
        f.jsonValue(context, emptyBehavior, defaultValueOnEmpty,
            errorBehavior, defaultValueOnError),
        matcher);
  }

  private void assertJsonValueAnyFailed(JsonFunctions.JsonPathContext input,
      SqlJsonValueEmptyOrErrorBehavior emptyBehavior,
      Object defaultValueOnEmpty,
      SqlJsonValueEmptyOrErrorBehavior errorBehavior,
      Object defaultValueOnError,
      Matcher<? super Throwable> matcher) {
    final JsonFunctions.StatefulFunction f =
        new JsonFunctions.StatefulFunction();
    assertFailed(
        invocationDesc(BuiltInMethod.JSON_VALUE, input, emptyBehavior,
            defaultValueOnEmpty, errorBehavior, defaultValueOnError),
        () -> f.jsonValue(input, emptyBehavior,
            defaultValueOnEmpty, errorBehavior, defaultValueOnError),
        matcher);
  }

  private void assertJsonQuery(JsonFunctions.JsonPathContext input,
      SqlJsonQueryWrapperBehavior wrapperBehavior,
      SqlJsonQueryEmptyOrErrorBehavior emptyBehavior,
      SqlJsonQueryEmptyOrErrorBehavior errorBehavior,
      Matcher<? super Object> matcher) {
    assertJsonQuery(input, wrapperBehavior, emptyBehavior, errorBehavior, true, matcher);
  }

  private void assertJsonQuery(JsonFunctions.JsonPathContext input,
      SqlJsonQueryWrapperBehavior wrapperBehavior,
      SqlJsonQueryEmptyOrErrorBehavior emptyBehavior,
      SqlJsonQueryEmptyOrErrorBehavior errorBehavior,
      boolean jsonize,
      Matcher<? super Object> matcher) {
    final JsonFunctions.StatefulFunction f =
        new JsonFunctions.StatefulFunction();
    assertThat(
        invocationDesc(BuiltInMethod.JSON_QUERY, input, wrapperBehavior,
            emptyBehavior, errorBehavior),
        f.jsonQuery(input, wrapperBehavior, emptyBehavior,
            errorBehavior, jsonize),
        matcher);
  }

  private void assertJsonQueryFailed(JsonFunctions.JsonPathContext input,
      SqlJsonQueryWrapperBehavior wrapperBehavior,
      SqlJsonQueryEmptyOrErrorBehavior emptyBehavior,
      SqlJsonQueryEmptyOrErrorBehavior errorBehavior,
      Matcher<? super Throwable> matcher) {
    final JsonFunctions.StatefulFunction f =
        new JsonFunctions.StatefulFunction();
    assertFailed(
        invocationDesc(BuiltInMethod.JSON_QUERY, input, wrapperBehavior,
            emptyBehavior, errorBehavior),
        () -> f.jsonQuery(input, wrapperBehavior, emptyBehavior,
            errorBehavior, true),
        matcher);
  }

  private void assertJsonize(Object input,
      Matcher<? super String> matcher) {
    assertThat(invocationDesc(BuiltInMethod.JSONIZE, input),
        JsonFunctions.jsonize(input),
        matcher);
  }

  private void assertJsonPretty(JsonFunctions.JsonValueContext input,
      Matcher<? super String> matcher) {
    assertThat(invocationDesc(BuiltInMethod.JSON_PRETTY, input),
        JsonFunctions.jsonPretty(input),
        matcher);
  }

  private void assertJsonPrettyFailed(JsonFunctions.JsonValueContext input,
      Matcher<? super Throwable> matcher) {
    assertFailed(invocationDesc(BuiltInMethod.JSON_PRETTY, input),
        () -> JsonFunctions.jsonPretty(input),
        matcher);
  }

  private void assertJsonLength(JsonFunctions.JsonPathContext input,
      Matcher<? super Integer> matcher) {
    assertThat(invocationDesc(BuiltInMethod.JSON_LENGTH, input),
        JsonFunctions.jsonLength(input),
        matcher);
  }

  private void assertJsonLengthFailed(JsonFunctions.JsonValueContext input,
      Matcher<? super Throwable> matcher) {
    assertFailed(invocationDesc(BuiltInMethod.JSON_LENGTH, input),
        () -> JsonFunctions.jsonLength(input),
        matcher);
  }

  private void assertJsonKeys(JsonFunctions.JsonPathContext input,
      Matcher<? super String> matcher) {
    assertThat(invocationDesc(BuiltInMethod.JSON_KEYS, input),
        JsonFunctions.jsonKeys(input),
        matcher);
  }

  private void assertJsonKeysFailed(JsonFunctions.JsonValueContext input,
      Matcher<? super Throwable> matcher) {
    assertFailed(invocationDesc(BuiltInMethod.JSON_KEYS, input),
        () -> JsonFunctions.jsonKeys(input),
        matcher);
  }

  private void assertJsonRemove(JsonFunctions.JsonValueContext input,
      String[] pathSpecs, Matcher<? super String> matcher) {
    assertThat(invocationDesc(BuiltInMethod.JSON_REMOVE, input, pathSpecs),
        JsonFunctions.jsonRemove(input, pathSpecs),
        matcher);
  }

  private void assertJsonStorageSize(String input,
      Matcher<? super Integer> matcher) {
    assertThat(invocationDesc(BuiltInMethod.JSON_STORAGE_SIZE, input),
        JsonFunctions.jsonStorageSize(input),
        matcher);
  }

  private void assertJsonStorageSize(JsonFunctions.JsonValueContext input,
      Matcher<? super Integer> matcher) {
    assertThat(invocationDesc(BuiltInMethod.JSON_STORAGE_SIZE, input),
        JsonFunctions.jsonStorageSize(input),
        matcher);
  }

  private void assertJsonStorageSizeFailed(String input,
      Matcher<? super Throwable> matcher) {
    assertFailed(invocationDesc(BuiltInMethod.JSON_STORAGE_SIZE, input),
        () -> JsonFunctions.jsonStorageSize(input),
        matcher);
  }

  private void assertJsonInsert(JsonFunctions.JsonValueContext jsonDoc,
      Object[] kvs,
      Matcher<? super String> matcher) {
    assertThat(invocationDesc(BuiltInMethod.JSON_INSERT, jsonDoc, kvs),
        JsonFunctions.jsonInsert(jsonDoc, kvs),
        matcher);
  }

  private void assertJsonReplace(JsonFunctions.JsonValueContext jsonDoc,
      Object[] kvs,
      Matcher<? super String> matcher) {
    assertThat(invocationDesc(BuiltInMethod.JSON_REPLACE, jsonDoc, kvs),
        JsonFunctions.jsonReplace(jsonDoc, kvs),
        matcher);
  }

  private void assertJsonSet(JsonFunctions.JsonValueContext jsonDoc,
      Object[] kvs,
      Matcher<? super String> matcher) {
    assertThat(invocationDesc(BuiltInMethod.JSON_SET, jsonDoc, kvs),
        JsonFunctions.jsonSet(jsonDoc, kvs),
        matcher);
  }

  private void assertDejsonize(String input,
      Matcher<Object> matcher) {
    assertThat(invocationDesc(BuiltInMethod.DEJSONIZE, input),
        JsonFunctions.dejsonize(input),
        matcher);
  }

  private void assertDejsonizeFailed(String input,
      Matcher<? super Throwable> matcher) {
    assertFailed(invocationDesc(BuiltInMethod.DEJSONIZE, input),
        () -> JsonFunctions.dejsonize(input),
        matcher);
  }

  private void assertJsonObject(Matcher<? super String> matcher,
      SqlJsonConstructorNullClause nullClause,
      Object... kvs) {
    assertThat(invocationDesc(BuiltInMethod.JSON_OBJECT, nullClause, kvs),
        JsonFunctions.jsonObject(nullClause, kvs),
        matcher);
  }

  private void assertJsonType(Matcher<? super String> matcher,
      String input) {
    assertThat(invocationDesc(BuiltInMethod.JSON_TYPE, input),
        JsonFunctions.jsonType(input),
        matcher);
  }

  private void assertJsonDepth(Matcher<? super Integer> matcher,
      String input) {
    assertThat(invocationDesc(BuiltInMethod.JSON_DEPTH, input),
        JsonFunctions.jsonDepth(input),
        matcher);
  }

  private void assertJsonObjectAggAdd(Map map, String k, Object v,
      SqlJsonConstructorNullClause nullClause,
      Matcher<? super Map> matcher) {
    JsonFunctions.jsonObjectAggAdd(map, k, v, nullClause);
    assertThat(
        invocationDesc(BuiltInMethod.JSON_ARRAYAGG_ADD, map, k, v, nullClause),
        map, matcher);
  }

  private void assertJsonArray(Matcher<? super String> matcher,
      SqlJsonConstructorNullClause nullClause, Object... elements) {
    assertThat(invocationDesc(BuiltInMethod.JSON_ARRAY, nullClause, elements),
        JsonFunctions.jsonArray(nullClause, elements),
        matcher);
  }

  private void assertJsonArrayAggAdd(List list, Object element,
      SqlJsonConstructorNullClause nullClause,
      Matcher<? super List> matcher) {
    JsonFunctions.jsonArrayAggAdd(list, element, nullClause);
    assertThat(
        invocationDesc(BuiltInMethod.JSON_ARRAYAGG_ADD, list, element,
            nullClause),
        list, matcher);
  }

  private void assertIsJsonValue(String input,
      Matcher<? super Boolean> matcher) {
    assertThat(invocationDesc(BuiltInMethod.IS_JSON_VALUE, input),
        JsonFunctions.isJsonValue(input),
        matcher);
  }

  private void assertIsJsonScalar(String input,
      Matcher<? super Boolean> matcher) {
    assertThat(invocationDesc(BuiltInMethod.IS_JSON_SCALAR, input),
        JsonFunctions.isJsonScalar(input),
        matcher);
  }

  private void assertIsJsonArray(String input,
      Matcher<? super Boolean> matcher) {
    assertThat(invocationDesc(BuiltInMethod.IS_JSON_ARRAY, input),
        JsonFunctions.isJsonArray(input),
        matcher);
  }

  private void assertIsJsonObject(String input,
      Matcher<? super Boolean> matcher) {
    assertThat(invocationDesc(BuiltInMethod.IS_JSON_OBJECT, input),
        JsonFunctions.isJsonObject(input),
        matcher);
  }

  private static String invocationDesc(BuiltInMethod method, Object... args) {
    return Arrays.stream(args)
        .map(Objects::toString)
        .collect(Collectors.joining(", ", method.getMethodName() + "(", ")"));
  }

  private void assertFailed(String invocationDesc, Supplier<?> supplier,
      Matcher<? super Throwable> matcher) {
    try {
      supplier.get();
      fail("expect exception, but not: " + invocationDesc);
    } catch (Throwable t) {
      assertThat(invocationDesc, t, matcher);
    }
  }

  private Matcher<? super Throwable> errorMatches(Throwable expected) {
    return new BaseMatcher<Throwable>() {
      @Override public boolean matches(Object item) {
        if (!(item instanceof Throwable)) {
          return false;
        }
        Throwable error = (Throwable) item;
        return expected != null
            && Objects.equals(error.getClass(), expected.getClass())
            && Objects.equals(error.getMessage(), expected.getMessage());
      }

      @Override public void describeTo(Description description) {
        description.appendText("is ").appendText(expected.toString());
      }
    };
  }

  private BaseMatcher<JsonFunctions.JsonPathContext> contextMatches(
      JsonFunctions.JsonPathContext expected) {
    return new BaseMatcher<JsonFunctions.JsonPathContext>() {
      @Override public boolean matches(Object item) {
        if (!(item instanceof JsonFunctions.JsonPathContext)) {
          return false;
        }
        JsonFunctions.JsonPathContext context = (JsonFunctions.JsonPathContext) item;
        if (Objects.equals(context.mode, expected.mode)
            && Objects.equals(context.obj, expected.obj)) {
          if (context.exc == null && expected.exc == null) {
            return true;
          }
          return context.exc != null && expected.exc != null
              && Objects.equals(context.exc.getClass(), expected.exc.getClass())
              && Objects.equals(context.exc.getMessage(), expected.exc.getMessage());
        }
        return false;
      }

      @Override public void describeTo(Description description) {
        description.appendText("is ").appendText(expected.toString());
      }
    };
  }
}
