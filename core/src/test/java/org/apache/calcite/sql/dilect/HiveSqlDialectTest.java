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
package org.apache.calcite.sql.dilect;

import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tester of {@link HiveSqlDialect}
 */
public class HiveSqlDialectTest {

  @Test public void unparseTrimTest() throws SqlParseException {
    String sql = "SELECT TRIM(' str ')";
    SqlParser sqlParser = SqlParser.create(sql);
    String transformedSql = sqlParser.parseQuery().toSqlString(HiveSqlDialect.DEFAULT).getSql();
    Assert.assertEquals(sql, transformedSql);
  }

}

// End HiveSqlDialectTest.java
