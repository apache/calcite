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
package org.apache.calcite.sql.test;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.util.Litmus;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test case for
 * <a href="https://issues.apache.org/jira/browse/CALCITE-4402">[CALCITE-4402]
 * SqlCall#equalsDeep does not take into account the function quantifier</a>.
 */
class SqlEqualsDeepTest {

  @Test void testCountEqualsDeep() throws SqlParseException {
    assertEqualsDeep("count(a)", "count(a)", true);
    assertEqualsDeep("count(distinct a)", "count(distinct a)", true);
    assertEqualsDeep("count(distinct a)", "count(a)", false);
  }

  private void assertEqualsDeep(String expr0, String expr1, boolean expected)
      throws SqlParseException {

    SqlNode sqlNode0 = parseExpression(expr0);
    SqlNode sqlNode1 = parseExpression(expr1);

    assertEquals(expected, sqlNode0.equalsDeep(sqlNode1, Litmus.IGNORE),
        () -> expr0 + " equalsDeep " + expr1);
  }

  private static SqlNode parseExpression(String sql) throws SqlParseException {
    return SqlParser.create(sql).parseExpression();
  }
}
