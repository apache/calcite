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

import org.junit.jupiter.api.Test;

/**
 * Tests for user-defined types.
 */
class UdtTest {
  private CalciteAssert.AssertThat withUdt() {
    final String model = "{\n"
        + "  version: '1.0',\n"
        + "  types: [\n"
        + "     {\n"
        + "       name: 'foo',\n"
        + "       type: 'BIGINT'\n"
        + "     }"
        + "   ],\n"
        + "   schemas: [\n"
        + "     {\n"
        + "       name: 'adhoc',\n"
        + "       types: [\n"
        + "         {\n"
        + "           name: 'mytype1',\n"
        + "           type: 'BIGINT'\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'mytype2',\n"
        + "           attributes: [\n"
        + "             {\n"
        + "               name: 'ii',\n"
        + "               type: 'INTEGER'\n"
        + "             },\n"
        + "             {\n"
        + "               name: 'jj',\n"
        + "               type: 'INTEGER'\n"
        + "             }\n"
        + "           ]\n"
        + "         }\n"
        + "       ]\n"
        + "     }\n"
        + "   ]\n"
        + "}";
    return CalciteAssert.model(model);
  }

  @Test void testUdt() {
    final String sql = "select CAST(\"id\" AS \"adhoc\".mytype1) as ld "
        + "from (VALUES ROW(1, 'SameName')) AS \"t\" (\"id\", \"desc\")";
    withUdt().query(sql).returns("LD=1\n");
  }

  @Test void testRootUdt() {
    final String sql = "select CAST(\"id\" AS foo) as ld "
        + "from (VALUES ROW(1, 'SameName')) AS \"t\" (\"id\", \"desc\")";
    withUdt().query(sql).returns("LD=1\n");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3045">[CALCITE-3045]
   * NullPointerException when casting null literal to composite user defined type</a>. */
  @Test void testCastNullLiteralToCompositeUdt() {
    final String sql = "select CAST(null AS \"adhoc\".mytype2) as c "
        + "from (VALUES (1))";
    withUdt().query(sql).returns("C=null\n");
  }
}
