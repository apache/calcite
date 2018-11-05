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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Test for {@link SqlSetOption}.
 */
public class SqlSetOptionOperatorTest {

  @Test public void testSqlSetOptionOperatorScopeSet() throws SqlParseException {
    SqlNode node = parse("alter system set optionA.optionB.optionC = true");
    checkSqlSetOptionSame(node);
  }

  public SqlNode parse(String s) throws SqlParseException {
    return SqlParser.create(s).parseStmt();
  }

  @Test public void testSqlSetOptionOperatorSet() throws SqlParseException {
    SqlNode node = parse("set optionA.optionB.optionC = true");
    checkSqlSetOptionSame(node);
  }

  @Test public void testSqlSetOptionOperatorScopeReset() throws SqlParseException {
    SqlNode node = parse("alter session reset param1.param2.param3");
    checkSqlSetOptionSame(node);
  }

  @Test public void testSqlSetOptionOperatorReset() throws SqlParseException {
    SqlNode node = parse("reset param1.param2.param3");
    checkSqlSetOptionSame(node);
  }

  private static void checkSqlSetOptionSame(SqlNode node) {
    SqlSetOption opt = (SqlSetOption) node;
    SqlNode[] sqlNodes = new SqlNode[opt.getOperandList().size()];
    SqlCall returned = opt.getOperator().createCall(
        opt.getFunctionQuantifier(),
        opt.getParserPosition(),
        opt.getOperandList().toArray(sqlNodes));
    assertThat((Class) opt.getClass(), equalTo((Class) returned.getClass()));
    SqlSetOption optRet = (SqlSetOption) returned;
    assertThat(optRet.getScope(), is(opt.getScope()));
    assertThat(optRet.getName(), is(opt.getName()));
    assertThat(optRet.getFunctionQuantifier(), is(opt.getFunctionQuantifier()));
    assertThat(optRet.getParserPosition(), is(opt.getParserPosition()));
    assertThat(optRet.getValue(), is(opt.getValue()));
    assertThat(optRet.toString(), is(opt.toString()));
  }

}

// End SqlSetOptionOperatorTest.java
