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
package org.apache.calcite.sql.validate;

import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.test.SqlTester;
import org.apache.calcite.sql.test.SqlValidatorTester;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for {@link SqlValidatorUtil}.
 */
public class SqlValidatorUtilTest {

  private static void checkChangedFieldList(
      List<String> nameList, List<String> resultList, boolean caseSensitive) {
    // Check that the new names are appended with "0" in order they appear in
    // original nameList. This is assuming that we only have one "collision".
    int i = 0;
    for (String name : nameList) {
      String newName = resultList.get(i);
      assertThat(newName, anyOf(is(name), is(name + "0")));
      i++;
    }

    // Make sure each name is unique
    List<String> copyResultList  = new ArrayList<>(resultList.size());
    for (String result : resultList) {
      copyResultList.add(result.toLowerCase(Locale.ROOT));
    }

    for (String result : resultList) {
      final String lowerResult = result.toLowerCase(Locale.ROOT);
      assertThat(copyResultList.contains(lowerResult), is(true));
      copyResultList.remove(lowerResult);
      if (!caseSensitive) {
        assertThat(copyResultList.contains(lowerResult), is(false));
      }
    }
    assertThat(copyResultList.size(), is(0));
  }

  @Test public void testUniquifyCaseSensitive() {
    List<String> nameList = Lists.newArrayList("col1", "COL1", "col_ABC", "col_abC");
    List<String> resultList = SqlValidatorUtil.uniquify(
        nameList, SqlValidatorUtil.EXPR_SUGGESTER, true);
    assertThat(nameList, sameInstance(resultList));
  }

  @Test public void testUniquifyNotCaseSensitive() {
    List<String> nameList = Lists.newArrayList("col1", "COL1", "col_ABC", "col_abC");
    List<String> resultList = SqlValidatorUtil.uniquify(
        nameList, SqlValidatorUtil.EXPR_SUGGESTER, false);
    assertThat(resultList, not(nameList));
    checkChangedFieldList(nameList, resultList, false);
  }

  @Test public void testUniquifyOrderingCaseSensitive() {
    List<String> nameList = Lists.newArrayList("k68s", "def", "col1", "COL1", "abc", "123");
    List<String> resultList = SqlValidatorUtil.uniquify(
        nameList, SqlValidatorUtil.EXPR_SUGGESTER, true);
    assertThat(nameList, sameInstance(resultList));
  }

  @Test public void testUniquifyOrderingRepeatedCaseSensitive() {
    List<String> nameList = Lists.newArrayList("k68s", "def", "col1", "COL1", "def", "123");
    List<String> resultList = SqlValidatorUtil.uniquify(
        nameList, SqlValidatorUtil.EXPR_SUGGESTER, true);
    assertThat(nameList, not(resultList));
    checkChangedFieldList(nameList, resultList, true);
  }

  @Test public void testUniquifyOrderingNotCaseSensitive() {
    List<String> nameList = Lists.newArrayList("k68s", "def", "col1", "COL1", "abc", "123");
    List<String> resultList = SqlValidatorUtil.uniquify(
        nameList, SqlValidatorUtil.EXPR_SUGGESTER, false);
    assertThat(resultList, not(nameList));
    checkChangedFieldList(nameList, resultList, false);
  }

  @Test public void testUniquifyOrderingRepeatedNotCaseSensitive() {
    List<String> nameList = Lists.newArrayList("k68s", "def", "col1", "COL1", "def", "123");
    List<String> resultList = SqlValidatorUtil.uniquify(
        nameList, SqlValidatorUtil.EXPR_SUGGESTER, false);
    assertThat(resultList, not(nameList));
    checkChangedFieldList(nameList, resultList, false);
  }

  @SuppressWarnings("resource")
  @Test public void testCheckingDuplicatesWithCompoundIdentifiers() {
    final List<SqlNode> newList = new ArrayList<>(2);
    newList.add(new SqlIdentifier(Arrays.asList("f0", "c0"), SqlParserPos.ZERO));
    newList.add(new SqlIdentifier(Arrays.asList("f0", "c0"), SqlParserPos.ZERO));
    final SqlTester tester =
        new SqlValidatorTester(SqlTestFactory.INSTANCE);
    final SqlValidatorImpl validator =
        (SqlValidatorImpl) tester.getValidator();
    try {
      SqlValidatorUtil.checkIdentifierListForDuplicates(newList,
          validator.getValidationErrorFunction());
      fail("expected exception");
    } catch (CalciteContextException e) {
      // ok
    }
    // should not throw
    newList.set(1, new SqlIdentifier(Arrays.asList("f0", "c1"), SqlParserPos.ZERO));
    SqlValidatorUtil.checkIdentifierListForDuplicates(newList, null);
  }

  @Test public void testNameMatcher() {
    final ImmutableList<String> beatles =
        ImmutableList.of("john", "paul", "ringo", "rinGo");
    final SqlNameMatcher insensitiveMatcher =
        SqlNameMatchers.withCaseSensitive(false);
    assertThat(insensitiveMatcher.frequency(beatles, "ringo"), is(2));
    assertThat(insensitiveMatcher.frequency(beatles, "rinGo"), is(2));
    final SqlNameMatcher sensitiveMatcher =
        SqlNameMatchers.withCaseSensitive(true);
    assertThat(sensitiveMatcher.frequency(beatles, "ringo"), is(1));
    assertThat(sensitiveMatcher.frequency(beatles, "rinGo"), is(1));
    assertThat(sensitiveMatcher.frequency(beatles, "Ringo"), is(0));
  }
}

// End SqlValidatorUtilTest.java
