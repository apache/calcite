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
package org.apache.calcite.sql.parser;

import org.apache.calcite.test.DiffTestCase;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import java.util.SortedSet;
import java.util.TreeSet;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests SQL Parser.
 */
public class CoreSqlParserTest extends SqlParserTest {

  /**
   * Tests that reserved keywords are not added to the parser unintentionally.
   * (Most keywords are non-reserved. The set of reserved words generally
   * only changes with a new version of the SQL standard.)
   *
   * <p>If the new keyword added is intended to be a reserved keyword, update
   * the {@link SqlParserTest#RESERVED_KEYWORDS} list. If not, add the keyword to the
   * non-reserved keyword list in the parser.
   */
  @Test void testNoUnintendedNewReservedKeywords() {
    assumeTrue(isNotSubclass(), "don't run this test for sub-classes");
    final SqlAbstractParserImpl.Metadata metadata =
        fixture().parser().getMetadata();

    final SortedSet<String> reservedKeywords = new TreeSet<>();
    final SortedSet<String> keywords92 = keywords("92");
    for (String s : metadata.getTokens()) {
      if (metadata.isKeyword(s) && metadata.isReservedWord(s)) {
        reservedKeywords.add(s);
      }
      // Check that the parser's list of SQL:92
      // reserved words is consistent with keywords("92").
      assertThat(s, metadata.isSql92ReservedWord(s),
          is(keywords92.contains(s)));
    }

    final String reason = "The parser has at least one new reserved keyword. "
        + "Are you sure it should be reserved? Difference:\n"
        + DiffTestCase.diffLines(ImmutableList.copyOf(getReservedKeywords()),
        ImmutableList.copyOf(reservedKeywords));
    assertThat(reason, reservedKeywords, is(getReservedKeywords()));
  }

  private boolean isNotSubclass() {
    return this.getClass().equals(CoreSqlParserTest.class);
  }
}
