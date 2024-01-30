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

import org.apache.calcite.sql.test.SqlOperatorFixture;
import org.apache.calcite.util.DelegatingInvocationHandler;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Proxy;
import java.util.regex.Pattern;

/** Utilities for {@link SqlOperatorFixture}. */
class SqlOperatorFixtures {
  private SqlOperatorFixtures() {
  }

  /** Returns a fixture that converts each CAST test into a test for
   * SAFE_CAST or TRY_CAST. */
  static SqlOperatorFixture safeCastWrapper(SqlOperatorFixture fixture, String functionName) {
    return (SqlOperatorFixture) Proxy.newProxyInstance(
        SqlOperatorTest.class.getClassLoader(),
        new Class[]{SqlOperatorFixture.class},
        new SqlOperatorFixtureInvocationHandler(fixture, functionName));
  }

  /** A helper for {@link #safeCastWrapper(SqlOperatorFixture, String)} that provides
   * alternative implementations of methods in {@link SqlOperatorFixture}.
   *
   * <p>Must be public, so that its methods can be seen via reflection. */
  @SuppressWarnings("unused")
  public static class SqlOperatorFixtureInvocationHandler
      extends DelegatingInvocationHandler {
    static final Pattern CAST_PATTERN = Pattern.compile("(?i)\\bCAST\\(");
    static final Pattern NOT_NULL_PATTERN = Pattern.compile(" NOT NULL");

    final SqlOperatorFixture f;
    final String functionName;

    SqlOperatorFixtureInvocationHandler(SqlOperatorFixture f, String functionName) {
      this.f = f;
      this.functionName = functionName;
    }

    @Override protected Object getTarget() {
      return f;
    }

    String addSafe(String sql) {
      return CAST_PATTERN.matcher(sql).replaceAll(functionName + "(");
    }

    String removeNotNull(String type) {
      return NOT_NULL_PATTERN.matcher(type).replaceAll("");
    }

    /** Proxy for
     * {@link SqlOperatorFixture#checkCastToString(String, String, String, SqlOperatorFixture.CastType)}. */
    public void checkCastToString(String value, @Nullable String type,
        @Nullable String expected, SqlOperatorFixture.CastType castType) {
      f.checkCastToString(addSafe(value),
          type == null ? null : removeNotNull(type), expected, castType);
    }

    /** Proxy for {@link SqlOperatorFixture#checkBoolean(String, Boolean)}. */
    public void checkFails(String expression, @Nullable Boolean result) {
      f.checkBoolean(addSafe(expression), result);
    }

    /** Proxy for {@link SqlOperatorFixture#checkNull(String)}. */
    public void checkNull(String expression) {
      f.checkNull(addSafe(expression));
    }

    /** Proxy for
     * {@link SqlOperatorFixture#checkFails(String, String, boolean)}. */
    public void checkFails(String expression, String expectedError, boolean runtime) {
      f.checkFails(addSafe(expression), expectedError, runtime);
    }

    /** Proxy for
     * {@link SqlOperatorFixture#checkScalar(String, Object, String)}. */
    public void checkScalar(String expression, Object result, String resultType) {
      f.checkScalar(addSafe(expression), result, removeNotNull(resultType));
    }

    /** Proxy for {@link SqlOperatorFixture#checkScalarExact(String, int)}. */
    public void checkScalarExact(String expression, int result) {
      f.checkScalarExact(addSafe(expression), result);
    }

    /** Proxy for
     * {@link SqlOperatorFixture#checkScalarExact(String, String, String)}. */
    public void checkScalarExact(String expression, String expectedType, String result) {
      f.checkScalarExact(addSafe(expression), removeNotNull(expectedType), result);
    }

    /** Proxy for
     * {@link SqlOperatorFixture#checkString(String, String, String)}. */
    public void checkString(String expression, String result, String resultType) {
      f.checkString(addSafe(expression), result, removeNotNull(resultType));
    }
  }
}
