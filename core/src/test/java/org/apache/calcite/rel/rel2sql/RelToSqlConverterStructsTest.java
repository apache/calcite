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

import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.util.Token;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.function.UnaryOperator;

import static org.apache.calcite.rel.rel2sql.DialectCode.CALCITE;

/**
 * Tests for {@link RelToSqlConverter} on a schema that has nested structures of multiple
 * levels.
 */
class RelToSqlConverterStructsTest {

  /** Creates a fixture. */
  private static RelToSqlFixture fixture() {
    final Token token = RelToSqlFixture.POOL.token();
    final DialectTestConfig testConfig =
        DialectTestConfigs.INSTANCE_SUPPLIER.get();
    final DialectTestConfig.Dialect calcite = testConfig.get(CALCITE);
    final DialectTestConfig.Phase phase = DialectTestConfig.Phase.PREPARE;
    return new RelToSqlFixture(token,
        CalciteAssert.SchemaSpec.MY_DB, "?",
        calcite, phase, SqlParser.Config.DEFAULT, ImmutableSet.of(),
        UnaryOperator.identity(), null, ImmutableList.of(),
        DialectTestConfigs.INSTANCE_SUPPLIER.get(),
        RelToSqlFixture::transformWriter);
  }

  /** Creates a fixture and initializes it with a SQL query. */
  private RelToSqlFixture sql(String sql) {
    return fixture().withSql(sql);
  }

  @Test void testNestedSchemaSelectStar() {
    String query = "SELECT * FROM \"myTable\"";
    String expected = "SELECT \"a\", "
        + "ROW(ROW(\"n1\".\"n11\".\"b\"), ROW(\"n1\".\"n12\".\"c\")) AS \"n1\", "
        + "ROW(\"n2\".\"d\") AS \"n2\", \"xs\", "
        + "\"e\"\n"
        + "FROM \"myDb\".\"myTable\"";
    sql(query).ok(expected).done();
  }

  @Test void testNestedSchemaRootColumns() {
    String query = "SELECT \"a\", \"e\" FROM \"myTable\"";
    String expected = "SELECT \"a\", "
        + "\"e\"\n"
        + "FROM \"myDb\".\"myTable\"";
    sql(query).ok(expected).done();
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
    sql(query).ok(expected).done();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6218">[CALCITE-6218]
   * RelToSqlConverter fails to convert correlated lateral joins</a>. */
  @Disabled // TODO fix UnsupportedOperationException in getAliasContext
  @Test void testUncollectLateralJoin() {
    final String query = "select \"a\",\n"
        + "\"x\"\n"
        + "from \"myDb\".\"myTable\",\n"
        + "unnest(\"xs\") as \"x\"";
    final String expected = "SELECT \"$cor0\".\"a\", \"t10\".\"xs\" AS \"x\"\n"
        + "FROM (SELECT \"a\", \"n1\".\"n11\".\"b\", \"n1\".\"n12\".\"c\", \"n2\".\"d\", \"xs\", \"e\"\n"
        + "FROM \"myDb\".\"myTable\") AS \"$cor0\",\n"
        + "LATERAL UNNEST((SELECT \"$cor0\".\"xs\"\n"
        + "FROM (VALUES (0)) AS \"t\" (\"ZERO\"))) AS \"t10\" (\"xs\")";
    sql(query).schema(CalciteAssert.SchemaSpec.MY_DB)
        .withPhase(DialectTestConfig.Phase.PARSE)
        .ok(expected)
        .done();
  }
}
