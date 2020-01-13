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
package org.apache.calcite.test

import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.sql.test.SqlTestFactory
import org.apache.calcite.sql.test.SqlTester
import org.apache.calcite.sql.test.SqlValidatorTester
import org.apache.calcite.test.catalog.MockCatalogReaderDynamic
import org.apache.calcite.testlib.annotations.LocaleEnUs
import org.junit.jupiter.api.Test

/**
 * Concrete child class of [SqlValidatorTestCase], containing lots of unit
 * tests.
 *
 * If you want to run these same tests in a different environment, create a
 * derived class whose [getTester] returns a different implementation of
 * [SqlTester].
 */
@LocaleEnUs
class SqlValidatorDynamicTest : SqlValidatorTestCase() {
    /**
     * Dynamic schema should not be reused since it is mutable, so
     * we create new SqlTestFactory for each test
     */
    override fun getTester(): SqlTester = SqlValidatorTester(SqlTestFactory.INSTANCE
        .withCatalogReader { typeFactory: RelDataTypeFactory, caseSensitive: Boolean ->
            MockCatalogReaderDynamic(
                typeFactory,
                caseSensitive
            )
        }
    )

    /**
     * Test case for
     * [Dynamic Table / Dynamic Star support](https://issues.apache.org/jira/browse/CALCITE-1150).
     */
    @Test
    fun `ambiguous dynamic star`() {
        sql(
            """
            select ^n_nation^
              from (select * from "SALES".NATION),
                   (select * from "SALES".CUSTOMER)
             """.trimIndent()
        ).fails("Column 'N_NATION' is ambiguous")
    }

    @Test
    fun `ambiguous dynamic star2`() {
        sql(
            """
            select ^n_nation^
              from (select * from "SALES".NATION, "SALES".CUSTOMER)
            """.trimIndent()
        ).fails("Column 'N_NATION' is ambiguous")
    }

    @Test
    fun `ambiguous dynamic star3`() {
        sql(
            """
            select ^nc.n_nation^
              from (select * from "SALES".NATION, "SALES".CUSTOMER) as nc
            """.trimIndent()
        ).fails("Column 'N_NATION' is ambiguous")
    }

    @Test
    fun `ambiguous dynamic star4`() {
        sql(
            """
            select n.n_nation
              from (select * from "SALES".NATION) as n,
                   (select * from "SALES".CUSTOMER)
             """.trimIndent()
        ).type("RecordType(ANY N_NATION) NOT NULL")
    }

    /**
     * When resolve column reference, regular field has higher priority than
     * dynamic star columns.
     */
    @Test
    fun `dynamic star2`() {
        sql(
            """
            select newid
              from (
                 select *, NATION.N_NATION + 100 as newid
                   from "SALES".NATION, "SALES".CUSTOMER
              )
            """.trimIndent()
        ).type("RecordType(ANY NEWID) NOT NULL")
    }
}
