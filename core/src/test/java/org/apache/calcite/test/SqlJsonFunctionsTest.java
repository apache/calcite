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
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlJsonConstructorNullClause;
import org.apache.calcite.sql.SqlJsonExistsErrorBehavior;
import org.apache.calcite.sql.SqlJsonQueryEmptyOrErrorBehavior;
import org.apache.calcite.sql.SqlJsonQueryWrapperBehavior;
import org.apache.calcite.sql.SqlJsonValueEmptyOrErrorBehavior;
import org.apache.calcite.util.BuiltInMethod;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.PathNotFoundException;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Unit test for the methods in {@link SqlFunctions} that implement JSON processing functions.
 */
public class SqlJsonFunctionsTest {

  @Test
  public void testJsonValueExpression() {
    assertJsonValueExpression("{}", is(Collections.emptyMap()));
  }

  @Test
  public void testJsonStructuredValueExpression() {
    assertJsonStructuredValueExpression("bar", is("bar"));
    assertJsonStructuredValueExpression(100, is(100));
  }

  @Test
  public void testJsonApiCommonSyntax() {
    assertJsonApiCommonSyntax(ImmutableMap.of("foo", "bar"), "lax $.foo",
        contextMatches(
            SqlFunctions.PathContext.withReturned(SqlFunctions.PathMode.LAX, "bar")));
    assertJsonApiCommonSyntax(ImmutableMap.of("foo", "bar"), "strict $.foo",
        contextMatches(
            SqlFunctions.PathContext.withReturned(SqlFunctions.PathMode.STRICT, "bar")));
    assertJsonApiCommonSyntax(ImmutableMap.of("foo", "bar"), "lax $.foo1",
        contextMatches(
            SqlFunctions.PathContext.withReturned(SqlFunctions.PathMode.LAX, null)));
    assertJsonApiCommonSyntax(ImmutableMap.of("foo", "bar"), "strict $.foo1",
        contextMatches(
            SqlFunctions.PathContext.withStrictException(
                new PathNotFoundException("No results for path: $['foo1']"))));
    assertJsonApiCommonSyntax(ImmutableMap.of("foo", 100), "lax $.foo",
        contextMatches(
            SqlFunctions.PathContext.withReturned(SqlFunctions.PathMode.LAX, 100)));
  }

  @Test
  public void testJsonExists() {
    assertJsonExists(
        SqlFunctions.PathContext.withReturned(SqlFunctions.PathMode.STRICT, "bar"),
        SqlJsonExistsErrorBehavior.FALSE,
        is(true));

    assertJsonExists(
        SqlFunctions.PathContext.withReturned(SqlFunctions.PathMode.STRICT, "bar"),
        SqlJsonExistsErrorBehavior.TRUE,
        is(true));

    assertJsonExists(
        SqlFunctions.PathContext.withReturned(SqlFunctions.PathMode.STRICT, "bar"),
        SqlJsonExistsErrorBehavior.UNKNOWN,
        is(true));

    assertJsonExists(
        SqlFunctions.PathContext.withReturned(SqlFunctions.PathMode.STRICT, "bar"),
        SqlJsonExistsErrorBehavior.ERROR,
        is(true));

    assertJsonExists(
        SqlFunctions.PathContext.withReturned(SqlFunctions.PathMode.LAX, null),
        SqlJsonExistsErrorBehavior.FALSE,
        is(false));

    assertJsonExists(
        SqlFunctions.PathContext.withReturned(SqlFunctions.PathMode.LAX, null),
        SqlJsonExistsErrorBehavior.TRUE,
        is(false));

    assertJsonExists(
        SqlFunctions.PathContext.withReturned(SqlFunctions.PathMode.LAX, null),
        SqlJsonExistsErrorBehavior.UNKNOWN,
        is(false));

    assertJsonExists(
        SqlFunctions.PathContext.withReturned(SqlFunctions.PathMode.LAX, null),
        SqlJsonExistsErrorBehavior.ERROR,
        is(false));

    assertJsonExists(
        SqlFunctions.PathContext.withStrictException(new Exception("test message")),
        SqlJsonExistsErrorBehavior.FALSE,
        is(false));

    assertJsonExists(
        SqlFunctions.PathContext.withStrictException(new Exception("test message")),
        SqlJsonExistsErrorBehavior.TRUE,
        is(true));

    assertJsonExists(
        SqlFunctions.PathContext.withStrictException(new Exception("test message")),
        SqlJsonExistsErrorBehavior.UNKNOWN,
        nullValue());

    assertJsonExistsFailed(
        SqlFunctions.PathContext.withStrictException(new Exception("test message")),
        SqlJsonExistsErrorBehavior.ERROR,
        errorMatches(new RuntimeException("java.lang.Exception: test message")));
  }

  @Test
  public void testJsonValueAny() {
    assertJsonValueAny(
        SqlFunctions.PathContext
            .withReturned(SqlFunctions.PathMode.LAX, "bar"),
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        is("bar"));
    assertJsonValueAny(
        SqlFunctions.PathContext
            .withReturned(SqlFunctions.PathMode.LAX, null),
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        nullValue());
    assertJsonValueAny(
        SqlFunctions.PathContext
            .withReturned(SqlFunctions.PathMode.LAX, null),
        SqlJsonValueEmptyOrErrorBehavior.DEFAULT,
        "empty",
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        is("empty"));
    assertJsonValueAnyFailed(
        SqlFunctions.PathContext
            .withReturned(SqlFunctions.PathMode.LAX, null),
        SqlJsonValueEmptyOrErrorBehavior.ERROR,
        null,
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        errorMatches(
            new CalciteException("Empty result of JSON_VALUE function is not "
                + "allowed", null)));
    assertJsonValueAny(
        SqlFunctions.PathContext
            .withReturned(SqlFunctions.PathMode.LAX, Collections.emptyList()),
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        nullValue());
    assertJsonValueAny(
        SqlFunctions.PathContext
            .withReturned(SqlFunctions.PathMode.LAX, Collections.emptyList()),
        SqlJsonValueEmptyOrErrorBehavior.DEFAULT,
        "empty",
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        is("empty"));
    assertJsonValueAnyFailed(
        SqlFunctions.PathContext
            .withReturned(SqlFunctions.PathMode.LAX, Collections.emptyList()),
        SqlJsonValueEmptyOrErrorBehavior.ERROR,
        null,
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        errorMatches(
            new CalciteException("Empty result of JSON_VALUE function is not "
                + "allowed", null)));
    assertJsonValueAny(
        SqlFunctions.PathContext
            .withStrictException(new Exception("test message")),
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        nullValue());
    assertJsonValueAny(
        SqlFunctions.PathContext
            .withStrictException(new Exception("test message")),
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        SqlJsonValueEmptyOrErrorBehavior.DEFAULT,
        "empty",
        is("empty"));
    assertJsonValueAnyFailed(
        SqlFunctions.PathContext
            .withStrictException(new Exception("test message")),
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        SqlJsonValueEmptyOrErrorBehavior.ERROR,
        null,
        errorMatches(
            new RuntimeException("java.lang.Exception: test message")));
    assertJsonValueAny(
        SqlFunctions.PathContext
            .withReturned(SqlFunctions.PathMode.STRICT, Collections.emptyList()),
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        nullValue());
    assertJsonValueAny(
        SqlFunctions.PathContext
            .withReturned(SqlFunctions.PathMode.STRICT, Collections.emptyList()),
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        SqlJsonValueEmptyOrErrorBehavior.DEFAULT,
        "empty",
        is("empty"));
    assertJsonValueAnyFailed(
        SqlFunctions.PathContext
            .withReturned(SqlFunctions.PathMode.STRICT, Collections.emptyList()),
        SqlJsonValueEmptyOrErrorBehavior.NULL,
        null,
        SqlJsonValueEmptyOrErrorBehavior.ERROR,
        null,
        errorMatches(
            new CalciteException("Strict jsonpath mode requires scalar value, "
                + "and the actual value is: '[]'", null)));
  }

  @Test
  public void testJsonQuery() {
    assertJsonQuery(
        SqlFunctions.PathContext
            .withReturned(SqlFunctions.PathMode.LAX, Collections.singletonList("bar")),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        is("[\"bar\"]"));
    assertJsonQuery(
        SqlFunctions.PathContext
            .withReturned(SqlFunctions.PathMode.LAX, null),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        nullValue());
    assertJsonQuery(
        SqlFunctions.PathContext
            .withReturned(SqlFunctions.PathMode.LAX, null),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.EMPTY_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        is("[]"));
    assertJsonQuery(
        SqlFunctions.PathContext
            .withReturned(SqlFunctions.PathMode.LAX, null),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.EMPTY_OBJECT,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        is("{}"));
    assertJsonQueryFailed(
        SqlFunctions.PathContext
            .withReturned(SqlFunctions.PathMode.LAX, null),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.ERROR,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        errorMatches(
            new CalciteException("Empty result of JSON_QUERY function is not "
                + "allowed", null)));

    assertJsonQuery(
        SqlFunctions.PathContext
            .withReturned(SqlFunctions.PathMode.LAX, "bar"),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        nullValue());
    assertJsonQuery(
        SqlFunctions.PathContext
            .withReturned(SqlFunctions.PathMode.LAX, "bar"),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.EMPTY_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        is("[]"));
    assertJsonQuery(
        SqlFunctions.PathContext
            .withReturned(SqlFunctions.PathMode.LAX, "bar"),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.EMPTY_OBJECT,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        is("{}"));
    assertJsonQueryFailed(
        SqlFunctions.PathContext
            .withReturned(SqlFunctions.PathMode.LAX, "bar"),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.ERROR,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        errorMatches(
            new CalciteException("Empty result of JSON_QUERY function is not "
                + "allowed", null)));
    assertJsonQuery(
        SqlFunctions.PathContext
            .withStrictException(new Exception("test message")),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.EMPTY_ARRAY,
        is("[]"));
    assertJsonQuery(
        SqlFunctions.PathContext
            .withStrictException(new Exception("test message")),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.EMPTY_OBJECT,
        is("{}"));
    assertJsonQueryFailed(
        SqlFunctions.PathContext
            .withStrictException(new Exception("test message")),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.ERROR,
        errorMatches(
            new RuntimeException("java.lang.Exception: test message")));
    assertJsonQuery(
        SqlFunctions.PathContext
            .withReturned(SqlFunctions.PathMode.STRICT, "bar"),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        nullValue());
    assertJsonQuery(
        SqlFunctions.PathContext
            .withReturned(SqlFunctions.PathMode.STRICT, "bar"),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.EMPTY_ARRAY,
        is("[]"));
    assertJsonQueryFailed(
        SqlFunctions.PathContext
            .withReturned(SqlFunctions.PathMode.STRICT, "bar"),
        SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.ERROR,
        errorMatches(
            new CalciteException("Strict jsonpath mode requires array or "
                + "object value, and the actual value is: 'bar'", null)));

    // wrapper behavior test

    assertJsonQuery(
        SqlFunctions.PathContext
            .withReturned(SqlFunctions.PathMode.STRICT, "bar"),
        SqlJsonQueryWrapperBehavior.WITH_UNCONDITIONAL_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        is("[\"bar\"]"));

    assertJsonQuery(
        SqlFunctions.PathContext
            .withReturned(SqlFunctions.PathMode.STRICT, "bar"),
        SqlJsonQueryWrapperBehavior.WITH_CONDITIONAL_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        is("[\"bar\"]"));

    assertJsonQuery(
        SqlFunctions.PathContext
            .withReturned(SqlFunctions.PathMode.STRICT,
                Collections.singletonList("bar")),
        SqlJsonQueryWrapperBehavior.WITH_UNCONDITIONAL_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        is("[[\"bar\"]]"));

    assertJsonQuery(
        SqlFunctions.PathContext
            .withReturned(SqlFunctions.PathMode.STRICT,
                Collections.singletonList("bar")),
        SqlJsonQueryWrapperBehavior.WITH_CONDITIONAL_ARRAY,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        SqlJsonQueryEmptyOrErrorBehavior.NULL,
        is("[\"bar\"]"));
  }

  @Test
  public void testJsonize() {
    assertJsonize(new HashMap<>(),
        is("{}"));
  }

  @Test
  public void assertJsonPretty() {
    assertJsonPretty(new HashMap<>(), is("{ }"));
    assertJsonPretty(Longs.asList(1, 2), is("[ 1, 2 ]"));

    Object input = new Object() {
      private final Object self = this;
    };
    CalciteException expected = new CalciteException(
        "Cannot serialize object to JSON, and the object is: '" + input + "'", null);
    assertJsonPrettyFailed(input, errorMatches(expected));
  }

  @Test
  public void testDejsonize() {
    assertDejsonize("{}",
        is(Collections.emptyMap()));
    assertDejsonize("[]",
        is(Collections.emptyList()));

    // expect exception thrown
    final String message = "com.fasterxml.jackson.core.JsonParseException: "
        + "Unexpected close marker '}': expected ']' (for Array starting at "
        + "[Source: (String)\"[}\"; line: 1, column: 1])\n at [Source: "
        + "(String)\"[}\"; line: 1, column: 3]";
    assertDejsonizeFailed("[}",
        errorMatches(new InvalidJsonException(message)));
  }

  @Test
  public void testJsonObject() {
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

  @Test
  public void testJsonType() {
    assertJsonType(is("OBJECT"), "{}");
    assertJsonType(is("ARRAY"),
            "[\"foo\",null]");
    assertJsonType(is("NULL"), "null");
    assertJsonType(is("BOOLEAN"), "false");
    assertJsonType(is("INTEGER"), "12");
    assertJsonType(is("DOUBLE"), "11.22");
  }

  @Test
  public void testJsonDepth() {
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

  @Test
  public void testJsonLength() {
    assertJsonLength(
            SqlFunctions.PathContext
                    .withReturned(SqlFunctions.PathMode.LAX, Collections.singletonList("bar")),
            is(1));
    assertJsonLength(
            SqlFunctions.PathContext
                    .withReturned(SqlFunctions.PathMode.LAX, null),
            nullValue());
    assertJsonLength(
            SqlFunctions.PathContext
                    .withReturned(SqlFunctions.PathMode.STRICT, Collections.singletonList("bar")),
            is(1));
    assertJsonLength(
            SqlFunctions.PathContext
                    .withReturned(SqlFunctions.PathMode.LAX, "bar"),
            is(1));
  }

  public void testJsonKeys() {
    assertJsonKeys(
            SqlFunctions.PathContext
                    .withReturned(SqlFunctions.PathMode.LAX, Collections.singletonList("bar")),
            is("null"));
    assertJsonKeys(
            SqlFunctions.PathContext
                    .withReturned(SqlFunctions.PathMode.LAX, null),
            is("null"));
    assertJsonKeys(
            SqlFunctions.PathContext
                    .withReturned(SqlFunctions.PathMode.STRICT, Collections.singletonList("bar")),
            is("null"));
    assertJsonKeys(
            SqlFunctions.PathContext
                    .withReturned(SqlFunctions.PathMode.LAX, "bar"),
            is("null"));
  }

  @Test
  public void testJsonObjectAggAdd() {
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

  @Test
  public void testJsonArray() {
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

  @Test
  public void testJsonArrayAggAdd() {
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

  @Test
  public void testJsonPredicate() {
    assertIsJsonValue("[]", is(true));
    assertIsJsonValue("{}", is(true));
    assertIsJsonValue("100", is(true));
    assertIsJsonValue("{]", is(false));
    assertIsJsonObject("[]", is(false));
    assertIsJsonObject("{}", is(true));
    assertIsJsonObject("100", is(false));
    assertIsJsonObject("{]", is(false));
    assertIsJsonArray("[]", is(true));
    assertIsJsonArray("{}", is(false));
    assertIsJsonArray("100", is(false));
    assertIsJsonArray("{]", is(false));
    assertIsJsonScalar("[]", is(false));
    assertIsJsonScalar("{}", is(false));
    assertIsJsonScalar("100", is(true));
    assertIsJsonScalar("{]", is(false));
  }

  private void assertJsonValueExpression(String input,
      Matcher<Object> matcher) {
    assertThat(
        invocationDesc(BuiltInMethod.JSON_VALUE_EXPRESSION.getMethodName(), input),
        SqlFunctions.jsonValueExpression(input), matcher);
  }

  private void assertJsonStructuredValueExpression(Object input,
      Matcher<Object> matcher) {
    assertThat(
        invocationDesc(BuiltInMethod.JSON_STRUCTURED_VALUE_EXPRESSION.getMethodName(), input),
        SqlFunctions.jsonStructuredValueExpression(input), matcher);
  }

  private void assertJsonApiCommonSyntax(Object input, String pathSpec,
      Matcher<? super SqlFunctions.PathContext> matcher) {
    assertThat(
        invocationDesc(BuiltInMethod.JSON_API_COMMON_SYNTAX.getMethodName(), input, pathSpec),
        SqlFunctions.jsonApiCommonSyntax(input, pathSpec), matcher);
  }

  private void assertJsonExists(Object input, SqlJsonExistsErrorBehavior errorBehavior,
      Matcher<? super Boolean> matcher) {
    assertThat(invocationDesc(BuiltInMethod.JSON_EXISTS.getMethodName(), input, errorBehavior),
        SqlFunctions.jsonExists(input, errorBehavior), matcher);
  }

  private void assertJsonExistsFailed(Object input,
      SqlJsonExistsErrorBehavior errorBehavior,
      Matcher<? super Throwable> matcher) {
    assertFailed(invocationDesc(BuiltInMethod.JSON_EXISTS.getMethodName(), input, errorBehavior),
        () -> SqlFunctions.jsonExists(input, errorBehavior), matcher);
  }

  private void assertJsonValueAny(Object input,
      SqlJsonValueEmptyOrErrorBehavior emptyBehavior,
      Object defaultValueOnEmpty,
      SqlJsonValueEmptyOrErrorBehavior errorBehavior,
      Object defaultValueOnError,
      Matcher<Object> matcher) {
    assertThat(
        invocationDesc(BuiltInMethod.JSON_VALUE_ANY.getMethodName(), input, emptyBehavior,
            defaultValueOnEmpty, errorBehavior, defaultValueOnError),
        SqlFunctions.jsonValueAny(input, emptyBehavior, defaultValueOnEmpty,
            errorBehavior, defaultValueOnError),
        matcher);
  }

  private void assertJsonValueAnyFailed(Object input,
      SqlJsonValueEmptyOrErrorBehavior emptyBehavior,
      Object defaultValueOnEmpty,
      SqlJsonValueEmptyOrErrorBehavior errorBehavior,
      Object defaultValueOnError,
      Matcher<? super Throwable> matcher) {
    assertFailed(
        invocationDesc(BuiltInMethod.JSON_VALUE_ANY.getMethodName(), input, emptyBehavior,
            defaultValueOnEmpty, errorBehavior, defaultValueOnError),
        () -> SqlFunctions.jsonValueAny(input, emptyBehavior,
            defaultValueOnEmpty, errorBehavior, defaultValueOnError),
        matcher);
  }

  private void assertJsonQuery(Object input,
      SqlJsonQueryWrapperBehavior wrapperBehavior,
      SqlJsonQueryEmptyOrErrorBehavior emptyBehavior,
      SqlJsonQueryEmptyOrErrorBehavior errorBehavior,
      Matcher<? super String> matcher) {
    assertThat(
        invocationDesc(BuiltInMethod.JSON_QUERY.getMethodName(), input, wrapperBehavior,
            emptyBehavior, errorBehavior),
        SqlFunctions.jsonQuery(input, wrapperBehavior, emptyBehavior,
            errorBehavior),
        matcher);
  }

  private void assertJsonQueryFailed(Object input,
      SqlJsonQueryWrapperBehavior wrapperBehavior,
      SqlJsonQueryEmptyOrErrorBehavior emptyBehavior,
      SqlJsonQueryEmptyOrErrorBehavior errorBehavior,
      Matcher<? super Throwable> matcher) {
    assertFailed(
        invocationDesc(BuiltInMethod.JSON_QUERY.getMethodName(), input, wrapperBehavior,
            emptyBehavior, errorBehavior),
        () -> SqlFunctions.jsonQuery(input, wrapperBehavior, emptyBehavior,
            errorBehavior),
        matcher);
  }

  private void assertJsonize(Object input,
      Matcher<? super String> matcher) {
    assertThat(invocationDesc(BuiltInMethod.JSONIZE.getMethodName(), input),
        SqlFunctions.jsonize(input),
        matcher);
  }

  private void assertJsonPretty(Object input,
      Matcher<? super String> matcher) {
    assertThat(invocationDesc(BuiltInMethod.JSON_PRETTY.getMethodName(), input),
        SqlFunctions.jsonPretty(input),
        matcher);
  }

  private void assertJsonPrettyFailed(Object input,
      Matcher<? super Throwable> matcher) {
    assertFailed(invocationDesc(BuiltInMethod.JSON_PRETTY.getMethodName(), input),
        () -> SqlFunctions.jsonPretty(input),
        matcher);
  }

  private void assertJsonLength(Object input,
      Matcher<? super Integer> matcher) {
    assertThat(
        invocationDesc(BuiltInMethod.JSON_LENGTH.getMethodName(), input),
        SqlFunctions.jsonLength(input),
        matcher);
  }

  private void assertJsonLengthFailed(Object input,
      Matcher<? super Throwable> matcher) {
    assertFailed(
        invocationDesc(BuiltInMethod.JSON_LENGTH.getMethodName(), input),
        () -> SqlFunctions.jsonLength(input),
        matcher);
  }

  private void assertJsonKeys(Object input,
                                Matcher<? super String> matcher) {
    assertThat(
            invocationDesc(BuiltInMethod.JSON_KEYS.getMethodName(), input),
            SqlFunctions.jsonKeys(input),
            matcher);
  }

  private void assertJsonKeysFailed(Object input,
                                      Matcher<? super Throwable> matcher) {
    assertFailed(invocationDesc(BuiltInMethod.JSON_KEYS.getMethodName(), input),
        () -> SqlFunctions.jsonKeys(input),
        matcher);
  }

  private void assertDejsonize(String input,
      Matcher<Object> matcher) {
    assertThat(invocationDesc(BuiltInMethod.DEJSONIZE.getMethodName(), input),
            SqlFunctions.dejsonize(input),
        matcher);
  }

  private void assertDejsonizeFailed(String input,
      Matcher<? super Throwable> matcher) {
    assertFailed(invocationDesc(BuiltInMethod.DEJSONIZE.getMethodName(), input),
        () -> SqlFunctions.dejsonize(input),
        matcher);
  }

  private void assertJsonObject(Matcher<? super String> matcher,
      SqlJsonConstructorNullClause nullClause,
      Object... kvs) {
    assertThat(invocationDesc(BuiltInMethod.JSON_OBJECT.getMethodName(), nullClause, kvs),
        SqlFunctions.jsonObject(nullClause, kvs),
        matcher);
  }

  private void assertJsonType(Matcher<? super String> matcher,
                                String input) {
    assertThat(
        invocationDesc(BuiltInMethod.JSON_TYPE.getMethodName(), input),
        SqlFunctions.jsonType(
                SqlFunctions.dejsonize(input)),
        matcher);
  }

  private void assertJsonDepth(Matcher<? super Integer> matcher,
                              String input) {
    assertThat(
            invocationDesc(BuiltInMethod.JSON_DEPTH.getMethodName(), input),
            SqlFunctions.jsonDepth(
                    SqlFunctions.dejsonize(input)),
            matcher);
  }

  private void assertJsonObjectAggAdd(Map map, String k, Object v,
      SqlJsonConstructorNullClause nullClause,
      Matcher<? super Map> matcher) {
    SqlFunctions.jsonObjectAggAdd(map, k, v, nullClause);
    assertThat(
        invocationDesc(BuiltInMethod.JSON_ARRAYAGG_ADD.getMethodName(), map, k, v, nullClause),
        map, matcher);
  }

  private void assertJsonArray(Matcher<? super String> matcher,
      SqlJsonConstructorNullClause nullClause, Object... elements) {
    assertThat(invocationDesc(BuiltInMethod.JSON_ARRAY.getMethodName(), nullClause, elements),
        SqlFunctions.jsonArray(nullClause, elements),
        matcher);
  }

  private void assertJsonArrayAggAdd(List list, Object element,
      SqlJsonConstructorNullClause nullClause,
      Matcher<? super List> matcher) {
    SqlFunctions.jsonArrayAggAdd(list, element, nullClause);
    assertThat(
        invocationDesc(BuiltInMethod.JSON_ARRAYAGG_ADD.getMethodName(), list, element,
            nullClause),
        list, matcher);
  }

  private void assertIsJsonValue(String input,
      Matcher<? super Boolean> matcher) {
    assertThat(invocationDesc(BuiltInMethod.IS_JSON_VALUE.getMethodName(), input),
        SqlFunctions.isJsonValue(input),
        matcher);
  }

  private void assertIsJsonScalar(String input,
      Matcher<? super Boolean> matcher) {
    assertThat(invocationDesc(BuiltInMethod.IS_JSON_SCALAR.getMethodName(), input),
        SqlFunctions.isJsonScalar(input),
        matcher);
  }

  private void assertIsJsonArray(String input,
      Matcher<? super Boolean> matcher) {
    assertThat(invocationDesc(BuiltInMethod.IS_JSON_ARRAY.getMethodName(), input),
        SqlFunctions.isJsonArray(input),
        matcher);
  }

  private void assertIsJsonObject(String input,
      Matcher<? super Boolean> matcher) {
    assertThat(invocationDesc(BuiltInMethod.IS_JSON_OBJECT.getMethodName(), input),
        SqlFunctions.isJsonObject(input),
        matcher);
  }

  private String invocationDesc(String methodName, Object... args) {
    return methodName + "(" + String.join(", ",
        Arrays.stream(args)
            .map(Objects::toString)
            .collect(Collectors.toList())) + ")";
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

  @Nonnull private BaseMatcher<SqlFunctions.PathContext> contextMatches(
      SqlFunctions.PathContext expected) {
    return new BaseMatcher<SqlFunctions.PathContext>() {
      @Override public boolean matches(Object item) {
        if (!(item instanceof SqlFunctions.PathContext)) {
          return false;
        }
        SqlFunctions.PathContext context = (SqlFunctions.PathContext) item;
        if (Objects.equals(context.mode, expected.mode)
            && Objects.equals(context.pathReturned, expected.pathReturned)) {
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

// End SqlJsonFunctionsTest.java
