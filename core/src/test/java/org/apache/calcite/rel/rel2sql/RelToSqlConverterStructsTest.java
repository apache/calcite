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
 * Tests for {@link RelToSqlConverter} on a schema that has nested structures of multiple
 * levels.
 */
class RelToSqlConverterStructsTest {

  private RelToSqlConverterTest.Sql sql(String sql) {
    return new RelToSqlConverterTest.Sql(CalciteAssert.SchemaSpec.MY_DB, sql,
        CalciteSqlDialect.DEFAULT, SqlParser.Config.DEFAULT, ImmutableSet.of(),
        UnaryOperator.identity(), null, ImmutableList.of());
  }

  @Test void testNestedSchemaSelectStar() {
    String query = "SELECT * FROM \"myTable\"";
    String expected = "SELECT \"a\", "
        + "ROW(ROW(\"n1\".\"n11\".\"b\"), ROW(\"n1\".\"n12\".\"c\")) AS \"n1\", "
        + "ROW(\"n2\".\"d\") AS \"n2\", \"xs\", "
        + "\"e\"\n"
        + "FROM \"myDb\".\"myTable\"";
    sql(query).ok(expected);
  }

  @Test void testNestedSchemaRootColumns() {
    String query = "SELECT \"a\", \"e\" FROM \"myTable\"";
    String expected = "SELECT \"a\", "
        + "\"e\"\n"
        + "FROM \"myDb\".\"myTable\"";
    sql(query).ok(expected);
  }

  @Test void testNestedSchemaNestedColumns() {
    String query = "SELECT \"a\", \"e\", "
        + "\"myTable\".\"n1\".\"n11\".\"b\", "
        + "\"myTable\".\"n2\".\"d\" "
        + "FROM \"myTable\"";
    String expected = "SELECT \"a\", "
        + "\"e\", "
        + "\"n1\".\"n11\".\"b\", "
        + "\"n2\".\"d\"\n"
        + "FROM \"myDb\".\"myTable\"";
    sql(query).ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6218">[CALCITE-6218]
   * RelToSqlConverter fails to convert correlated lateral joins</a>. */
  @Test void testUncollectLateralJoin() {
    final String query = "select \"a\",\n"
        + "\"x\"\n"
        + "from \"myDb\".\"myTable\",\n"
        + "unnest(\"xs\") as \"x\"";
    final String expected = "SELECT \"$cor0\".\"a\", \"t10\".\"xs\" AS \"x\"\n"
        + "FROM (SELECT \"a\", \"n1\".\"n11\".\"b\", \"n1\".\"n12\".\"c\", \"n2\".\"d\", \"xs\", \"e\"\n"
        + "FROM \"myDb\".\"myTable\") AS \"$cor0\",\nLATERAL UNNEST (SELECT \"$cor0\".\"xs\"\n"
        + "FROM (VALUES (0)) AS \"t\" (\"ZERO\")) AS \"t1\" (\"xs\") AS \"t10\"";
    sql(query).schema(CalciteAssert.SchemaSpec.MY_DB).ok(expected);
  }
}
