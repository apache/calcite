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

import com.google.common.collect.Lists;

import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Matchers for testing SQL queries.
 */
public class Matchers {
  private Matchers() {}

  /** Allows passing the actual result from the {@code matchesSafely} method to
   * the {@code describeMismatchSafely} method that will show the difference. */
  private static final ThreadLocal<Object> THREAD_ACTUAL = new ThreadLocal<>();

  /**
   * Creates a matcher that matches if the examined result set returns the
   * given collection of rows in some order.
   *
   * <p>Closes the result set after reading.
   *
   * <p>For example:
   * <pre>assertThat(statement.executeQuery("select empno from emp"),
   *   returnsUnordered("empno=1234", "empno=100"));</pre>
   */
  public static Matcher<? super ResultSet> returnsUnordered(String... lines) {
    final List<String> expectedList = Lists.newArrayList(lines);
    Collections.sort(expectedList);

    return new CustomTypeSafeMatcher<ResultSet>(Arrays.toString(lines)) {
      @Override protected void describeMismatchSafely(ResultSet item,
          Description description) {
        final Object value = THREAD_ACTUAL.get();
        THREAD_ACTUAL.remove();
        description.appendText("was ").appendValue(value);
      }

      protected boolean matchesSafely(ResultSet resultSet) {
        final List<String> actualList = Lists.newArrayList();
        try {
          CalciteAssert.toStringList(resultSet, actualList);
          resultSet.close();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
        Collections.sort(actualList);

        THREAD_ACTUAL.set(actualList);
        final boolean equals = actualList.equals(expectedList);
        if (!equals) {
          THREAD_ACTUAL.set(actualList);
        }
        return equals;
      }
    };
  }
}

// End Matchers.java
