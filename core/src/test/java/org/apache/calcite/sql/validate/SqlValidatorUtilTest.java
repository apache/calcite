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

import com.google.common.collect.Lists;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link SqlValidatorUtil}
 */
public class SqlValidatorUtilTest {

  private static void checkChangedFieldList(
      List<String> nameList, List<String> resultList, boolean caseSensitive) {
    // Check that the new names are appeneded with "0" in order they appear in original nameList.
    // This is assuming that we only have one "collision"
    int i = 0;
    for (String name : nameList) {
      String newName = resultList.get(i);
      assertTrue(newName.equals(name) || newName.equals(name + "0"));
      i++;
    }

    // Make sure each name is unique
    List<String> copyResultList  = new ArrayList<>(resultList.size());
    for (String result : resultList) {
      copyResultList.add(result.toLowerCase());
    }

    for (String result : resultList) {
      assertTrue(copyResultList.contains(result.toLowerCase()));
      copyResultList.remove(result.toLowerCase());
      if (!caseSensitive) {
        assertFalse(copyResultList.contains(result.toLowerCase()));
      }
    }
    assertTrue(copyResultList.size() == 0);
  }

  @Test public void testUniquifyCaseSensitive() {
    List<String> nameList = Lists.newArrayList("col1", "COL1", "col_ABC", "col_abC");
    List<String> resultList = SqlValidatorUtil.uniquify(
        nameList, SqlValidatorUtil.EXPR_SUGGESTER, true);
    assertSame(resultList, nameList);
  }

  @Test public void testUniquifyNotCaseSensitive() {
    List<String> nameList = Lists.newArrayList("col1", "COL1", "col_ABC", "col_abC");
    List<String> resultList = SqlValidatorUtil.uniquify(
        nameList, SqlValidatorUtil.EXPR_SUGGESTER, false);
    assertNotSame(nameList, resultList);
    checkChangedFieldList(nameList, resultList, false);
  }

  @Test public void testUniquifyOrderingCaseSensitive() {
    List<String> nameList = Lists.newArrayList("k68s", "def", "col1", "COL1", "abc", "123");
    List<String> resultList = SqlValidatorUtil.uniquify(
        nameList, SqlValidatorUtil.EXPR_SUGGESTER, true);
    assertSame(resultList, nameList);
  }

  @Test public void testUniquifyOrderingRepeatedCaseSensitive() {
    List<String> nameList = Lists.newArrayList("k68s", "def", "col1", "COL1", "def", "123");
    List<String> resultList = SqlValidatorUtil.uniquify(
        nameList, SqlValidatorUtil.EXPR_SUGGESTER, true);
    assertNotSame(resultList, nameList);
    checkChangedFieldList(nameList, resultList, true);
  }

  @Test public void testUniquifyOrderingNotCaseSensitive() {
    List<String> nameList = Lists.newArrayList("k68s", "def", "col1", "COL1", "abc", "123");
    List<String> resultList = SqlValidatorUtil.uniquify(
        nameList, SqlValidatorUtil.EXPR_SUGGESTER, false);
    assertNotSame(nameList, resultList);
    checkChangedFieldList(nameList, resultList, false);
  }

  @Test public void testUniquifyOrderingRepeatedNotCaseSensitive() {
    List<String> nameList = Lists.newArrayList("k68s", "def", "col1", "COL1", "def", "123");
    List<String> resultList = SqlValidatorUtil.uniquify(
        nameList, SqlValidatorUtil.EXPR_SUGGESTER, false);
    assertNotSame(nameList, resultList);
    checkChangedFieldList(nameList, resultList, false);
  }
}
// End SqlValidatorUtilTest.java
