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
package org.apache.calcite.rel.rel2sql;

import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.jupiter.api.Test;

import java.util.function.UnaryOperator;

/**
 * Tests for {@link RelToSqlConverter} on a schema that has an array
 * of struct.
 */
class RelToSqlConverterArraysTest {

  private RelToSqlConverterTest.Sql sql(String sql) {
    return new RelToSqlConverterTest.Sql(CalciteAssert.SchemaSpec.DM_DB, sql,
        CalciteSqlDialect.DEFAULT, SqlParser.Config.DEFAULT, ImmutableSet.of(),
        UnaryOperator.identity(), null, ImmutableList.of());
  }

  @Test public void testFieldAccessInArrayOfStruct() {
    final String query = "SELECT \"n1\"[1].\"b\" FROM \"myTable\"";
    final String expected = "SELECT \"n1\"[1].\"b\""
        + "\nFROM \"dmDb\".\"myTable\"";
    sql(query)
        .ok(expected);
  }
}
