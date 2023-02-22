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
import org.apache.calcite.sql.parser.bodo.SqlBodoParserImpl;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.test.SqlValidatorTester;
import org.apache.calcite.test.catalog.MockCatalogReaderExtended;

import org.junit.jupiter.api.Test;

/**
 * Module for validator tests that require the Bodo Parser.
 */
public class BodoSqlValidatorTest extends SqlValidatorTestCase {
  public static final SqlValidatorFixture FIXTURE =
      new SqlValidatorFixture(SqlValidatorTester.DEFAULT,
          SqlTestFactory.INSTANCE.withParserConfig(
              c -> c.withParserFactory(SqlBodoParserImpl.FACTORY)),
          StringAndPos.of("?"), false, false);

  @Override public SqlValidatorFixture fixture() {
    return FIXTURE;
  }

  @Test void testMultipleSameAsPass() {
    sql("select 1 as again,2 as \"again\", 3 as AGAiN from (values (true))")
        .ok();
  }

  @Test void testCreateTableReplaceAndIfNotExists() {
    // Checks that we throw a reasonable error in the case that we specify both
    // REPLACE and IF NOT EXISTS
    final String sql =
        "^CREATE OR REPLACE TABLE IF NOT EXISTS out_test AS select 1, 2, 3 from emp^";
    sql(sql).fails(
        "Create Table statements cannot contain both 'OR REPLACE' and 'IF NOT EXISTS'");
  }

  @Test void testCreateTableExplicitName() {
    // Checks that we can handle explicitly specifying the full name
    final String sql =
        "^CREATE OR REPLACE TABLE SALES.out_test AS select 1, 2, 3 from emp^";
    sql(sql).equals("");
  }

  @Test void testCreateTableExplicitNameNonDefaultSchema() {
    // Checks that we can handle explicitly specifying the full name
    final String sql =
        "^CREATE OR REPLACE TABLE CUSTOMER.out_test AS select 1, 2, 3 from emp^";
    sql(sql).equals("");
  }

  @Test void testCreateTableUnsupportedVolatile() {
    //Tests certain clauses that parse, but are currently unsupported (throw errors in validation)

    // Volatile is supported in SF, so we may get to it soonish
    final String q1 = "^CREATE VOLATILE TABLE out_test AS select 1, 2, 3 from emp^";
    sql(q1).fails("Create Table statements with 'VOLATILE' not supported");
  }


  @Test void testCreateTableSchemaError() {
    // Tests that we throw a reasonable error in the event that we can't find
    // the relevant schema
    final String query = "CREATE TABLE non_existent_schema.further_non_existent.out_test\n"
        + "AS select 1, 2, 3 from emp";
    sql(query).fails(
        "Unable to find schema NON_EXISTENT_SCHEMA\\.FURTHER_NON_EXISTENT in \\[SALES\\]");
  }

  @Test void testCreateTableSchemaError2() {
    // Tests that, when not using the default schema,
    // the error message throws the best match we have, as opposed to an arbitrary one.
    final String query = "CREATE TABLE CUSTOMER.non_existent_schema.further_non_existent.out_test\n"
        + "AS select 1, 2, 3 from emp";
    sql(query).withCatalogReader(MockCatalogReaderExtended::create).fails(
        "Unable to find schema NON_EXISTENT_SCHEMA\\.FURTHER_NON_EXISTENT in \\[CUSTOMER\\]");
  }

}
