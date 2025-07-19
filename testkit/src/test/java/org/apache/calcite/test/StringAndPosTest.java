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

import org.apache.calcite.sql.parser.StringAndPos;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;

/**
 * Unit tests for {@link StringAndPos}, particularly around caret handling.
 */
public class StringAndPosTest {

  @Test public void testXorOperatorNotParsedAsCursor() {
    String sql = "SELECT 5 ^ 3 FROM t";
    StringAndPos sap = StringAndPos.of(sql);
    assertThat(sap.sql, is(sql));
    assertThat(sap.cursor, is(-1));
    assertThat(sap.pos, is(nullValue()));
  }

  @Test public void testCastNullXorNumberNotParsedAsCursor() {
    String sql = "SELECT CAST(NULL AS INTEGER) ^ 5 FROM t";
    StringAndPos sap = StringAndPos.of(sql);
    assertThat(sap.sql, is(sql));
    assertThat(sap.cursor, is(-1));
    assertThat(sap.pos, is(nullValue()));
  }

  @Test public void testDoubleCaretIsEscaped() {
    String sql = "SELECT 5 ^^ 3 FROM t";
    StringAndPos sap = StringAndPos.of(sql);
    assertThat(sap.sql, is("SELECT 5 ^ 3 FROM t"));
    assertThat(sap.cursor, is(-1));
    assertThat(sap.pos, is(nullValue()));
  }

  @Test public void testActualCursorPositionCaret() {
    String sql = "SELECT 1 +^ 2 FROM t";
    StringAndPos sap = StringAndPos.of(sql);
    assertThat(sap.sql, is("SELECT 1 + 2 FROM t"));
    assertThat(sap.cursor, is(10));
    assertThat(sap.pos, notNullValue());
    assertThat(sap.pos, hasToString("line 1, column 11"));
  }
}
