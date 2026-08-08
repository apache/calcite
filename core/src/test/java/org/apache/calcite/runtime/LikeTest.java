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
package org.apache.calcite.runtime;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/** Unit tests for {@link Like}. */
class LikeTest {

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7693">[CALCITE-7693]
   * Move MongoDB LIKE-to-regex conversion into runtime.Like for consistency</a>. */
  @Test void testSqlToRegexMongo() {
    assertThat(Like.sqlToRegexMongo("", null), is("^$"));
    assertThat(Like.sqlToRegexMongo("abc", null), is("^abc$"));
    assertThat(Like.sqlToRegexMongo("A%", null), is("^A.*$"));
    assertThat(Like.sqlToRegexMongo("A_", null), is("^A.$"));
    assertThat(Like.sqlToRegexMongo("%abc%", null), is("^.*abc.*$"));
    // '.' is an ordinary SQL LIKE character; it must be escaped so that it is
    // literal in the generated regex.
    assertThat(Like.sqlToRegexMongo("A.B%", null), is("^A\\.B.*$"));
  }

  @Test void testSqlToRegexMongoWithEscape() {
    // '\' escapes the wildcards, making them literal.
    assertThat(Like.sqlToRegexMongo("A\\_B\\%C%", "\\"), is("^A_B%C.*$"));
    assertThat(Like.sqlToRegexMongo("BROOKLYN\\%", "\\"), is("^BROOKLYN%$"));
    // A custom escape character.
    assertThat(Like.sqlToRegexMongo("BROOKLYN!%", "!"), is("^BROOKLYN%$"));
    // The escape character followed by itself is a literal escape character.
    assertThat(Like.sqlToRegexMongo("A\\\\B", "\\"), is("^A\\\\B$"));
  }
}
