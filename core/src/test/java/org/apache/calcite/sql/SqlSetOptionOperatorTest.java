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
package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test for {@link SqlSetOption}
 */
public class SqlSetOptionOperatorTest {

  @Test
  public void testSqlSetOptionOperatorScopeSet() throws SqlParseException {
    SqlNode node =
            SqlParser.create("alter system set optionA.optionB.optionC = true").parseStmt();
    checkSqlSetOptionSame(node);
  }

  @Test
  public void testSqlSetOptionOperatorSet() throws SqlParseException {
    SqlNode node =
            SqlParser.create("set optionA.optionB.optionC = true").parseStmt();
    checkSqlSetOptionSame(node);
  }

  @Test
  public void testSqlSetOptionOperatorScopeReset() throws SqlParseException {
    SqlNode node =
            SqlParser.create("alter session reset param1.param2.param3").parseStmt();
    checkSqlSetOptionSame(node);
  }

  @Test
  public void testSqlSetOptionOperatorReset() throws SqlParseException {
    SqlNode node =
            SqlParser.create("reset param1.param2.param3").parseStmt();
    checkSqlSetOptionSame(node);
  }

  private static void checkSqlSetOptionSame(SqlNode node) {
    SqlSetOption opt = (SqlSetOption) node;
    SqlNode[] sqlNodes = new SqlNode[opt.getOperandList().size()];
    SqlCall returned = opt.getOperator().createCall(
            opt.getFunctionQuantifier(),
            opt.getParserPosition(),
            opt.getOperandList().toArray(sqlNodes));
    assertEquals(returned.getClass(), opt.getClass());
    SqlSetOption optRet = (SqlSetOption) returned;
    assertEquals(opt.getScope(), optRet.getScope());
    assertEquals(opt.getName(), optRet.getName());
    assertEquals(opt.getFunctionQuantifier(), optRet.getFunctionQuantifier());
    assertEquals(opt.getParserPosition(), optRet.getParserPosition());
    assertEquals(opt.getValue(), optRet.getValue());
    assertEquals(opt.toString(), optRet.toString());
  }

}
// End SqlSetOptionOperatorTest.java
