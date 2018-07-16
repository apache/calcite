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

import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserTest;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;

import org.junit.Test;

/**
 * Tests the "Babel" SQL parser, that understands all dialects of SQL.
 */
public class BabelParserTest extends SqlParserTest {

  @Override protected SqlParserImplFactory parserImplFactory() {
    return SqlBabelParserImpl.FACTORY;
  }

  @Override public void testGenerateKeyWords() {
    // by design, method only works in base class; no-ops in this sub-class
  }

  @Test public void testSelect() {
    final String sql = "select 1 from t";
    final String expected = "SELECT 1\n"
        + "FROM `T`";
    sql(sql).ok(expected);
  }

}

// End BabelParserTest.java
