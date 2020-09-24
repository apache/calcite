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

import org.junit.jupiter.api.Test

/**
 * Validation tests for the `MATCH_RECOGNIZE` clause.
 */
class SqlValidatorMatchTest : SqlValidatorTestCase() {
    /** Tries to create a calls to some internal operators in
     * MATCH_RECOGNIZE. Should fail.  */
    @Test
    fun `match recognize internals`() {
        sql("values ^pattern_exclude(1, 2)^")
            .fails("No match found for function signature .*")
        sql("values ^\"|\"(1, 2)^")
            .fails("No match found for function signature .*")
        // FINAL and other functions should not be visible outside of
        // MATCH_RECOGNIZE
        sql("values ^\"FINAL\"(1, 2)^")
            .fails("No match found for function signature FINAL\\(<NUMERIC>, <NUMERIC>\\)")
        sql("values ^\"RUNNING\"(1, 2)^")
            .fails("No match found for function signature RUNNING\\(<NUMERIC>, <NUMERIC>\\)")
        sql("values ^\"FIRST\"(1, 2)^")
            .fails("Function 'FIRST\\(1, 2\\)' can only be used in MATCH_RECOGNIZE")
        sql("values ^\"LAST\"(1, 2)^")
            .fails("Function 'LAST\\(1, 2\\)' can only be used in MATCH_RECOGNIZE")
        sql("values ^\"PREV\"(1, 2)^")
            .fails("Function 'PREV\\(1, 2\\)' can only be used in MATCH_RECOGNIZE")
    }

    @Test
    fun `match recognize defines`() {
        sql(
            """
            select *
              from emp match_recognize (
                pattern (strt down+ up+)
                define
                  down as down.sal < PREV(down.sal),
                  up as up.sal > PREV(up.sal)
              ) mr
            """.trimIndent()
        ).ok()
    }

    @Test
    fun `match recognize defines2`() {
        sql(
            """
            select *
              from t match_recognize (
                pattern (strt down+ up+)
                define
                  down as down.price < PREV(down.price),
                  ^down as up.price > PREV(up.price)^
              ) mr
            """.trimIndent()
        ).fails("Pattern variable 'DOWN' has already been defined")
    }

    @Test
    fun `match recognize defines3`() {
        sql(
            """
            select *
              from emp match_recognize (
                pattern (strt down+up+)
                define
                  down as down.sal < PREV(down.sal),
                  up as up.sal > PREV(up.sal)
              ) mr
            """.trimIndent()
        ).ok()
    }

    @Test
    fun `match recognize defines4`() {
        sql(
            """
            select *
              from emp match_recognize (
                pattern (strt down+ up+)
                define
                  down as down.sal < PREV(down.sal),
                  up as up.sal > FIRST(^PREV(up.sal)^)
              ) mr
            """.trimIndent()
        ).fails("Cannot nest PREV/NEXT under LAST/FIRST 'PREV\\(`UP`\\.`SAL`, 1\\)'")
    }

    @Test
    fun `match recognize defines5`() {
        sql(
            """
            select *
              from emp match_recognize (
                pattern (strt down+ up+)
                define
                  down as down.sal < PREV(down.sal),
                  up as up.sal > FIRST(^FIRST(up.sal)^)
              ) mr
            """.trimIndent()
        ).fails("Cannot nest PREV/NEXT under LAST/FIRST 'FIRST\\(`UP`\\.`SAL`, 0\\)'")
    }

    @Test
    fun `match recognize defines6`() {
        sql(
            """
            select *
              from emp match_recognize (
                pattern (strt down+ up+)
                define
                  down as down.sal < PREV(down.sal),
                  up as up.sal > ^COUNT(down.sal, up.sal)^
              ) mr
            """.trimIndent()
        ).fails("Invalid number of parameters to COUNT method")
    }

    @Test
    fun `match recognize measures1`() {
        sql(
            """
            select *
              from emp match_recognize (
                measures
                  STRT.sal as start_sal,
                  ^LAST(null)^ as bottom_sal,
                  LAST(up.ts) as end_sal
                pattern (strt down+ up+)
                define
                  down as down.sal < PREV(down.sal),
                  up as up.sal > prev(up.sal)
              ) mr
            """.trimIndent()
        ).fails("Null parameters in 'LAST\\(NULL, 0\\)'")
    }

    @Test
    fun `match recognize skip to1`() {
        sql(
            """
            select *
              from emp match_recognize (
                after match skip to ^null^
                measures
                  STRT.sal as start_sal,
                  LAST(up.ts) as end_sal
                pattern (strt down+ up+)
                define
                  down as down.sal < PREV(down.sal),
                  up as up.sal > prev(up.sal)
              ) mr
            """.trimIndent()
        ).fails("(?s).*Encountered \"null\" at .*")
    }

    @Test
    fun `match recognize skip to2`() {
        sql(
            """
            select *
              from emp match_recognize (
                after match skip to no_exists
                ^measures^
                  STRT.sal as start_sal,
                  LAST(up.ts) as end_sal
                pattern (strt down+ up+)
                define
                  down as down.sal < PREV(down.sal),
                  up as up.sal > prev(up.sal)
              ) mr
            """.trimIndent()
        ).fails("(?s).*Encountered \"measures\" at .*")
    }

    @Test
    fun `match recognize skip to3`() {
        sql(
            """
            select *
            from emp match_recognize (
              measures
                STRT.sal as start_sal,
                LAST(up.sal) as end_sal
                after match skip to ^no_exists^
                pattern (strt down+ up+)
                define
                  down as down.sal < PREV(down.sal),
                  up as up.sal > prev(up.sal)
              ) mr
            """.trimIndent()
        ).fails("Unknown pattern 'NO_EXISTS'")
    }

    @Test
    fun `match recognize skip to case insensitive`() {
        sql(
            """
            select *
            from emp match_recognize (
              measures
                STRT.sal as start_sal,
                LAST(up.sal) as end_sal
                after match skip to ^"strt"^
                pattern (strt down+ up+)
                define
                  down as down.sal < PREV(down.sal),
                  up as up.sal > prev(up.sal)
              ) mr
            """.trimIndent()
        ).fails("Unknown pattern 'strt'")
            .withCaseSensitive(false)
            .ok()
    }

    @Test
    fun `match recognize subset`() {
        sql(
            """
            select *
            from emp match_recognize (
                pattern (strt down+ up+)
                subset stdn = (^strt1^, down)
                define
                  down as down.sal < PREV(down.sal),
                  up as up.sal > prev(up.sal)
              ) mr
            """.trimIndent()
        ).fails("Unknown pattern 'STRT1'")
    }

    @Test
    fun `match recognize subset2`() {
        sql(
            """
            select *
            from emp match_recognize (
                pattern (strt down+ up+)
                subset ^strt^ = (strt, down)
                define
                  down as down.sal < PREV(down.sal),
                  up as up.sal > prev(up.sal)
              ) mr
            """.trimIndent()
        ).fails("Pattern variable 'STRT' has already been defined")
    }

    @Test
    fun `match recognize within`() {
        sql(
            """
            select *
            from emp match_recognize (
                pattern (strt down+ up+) within ^interval '3:10' minute to second^
                define
                  down as down.sal < PREV(down.sal),
                  up as up.sal > prev(up.sal)
              ) mr
            """.trimIndent()
        ).fails("Must contain an ORDER BY clause when WITHIN is used")
    }

    @Test
    fun `match recognize within2`() {
        sql(
            """
            select *
            from emp match_recognize (
                order by sal
                pattern (strt down+ up+) within ^interval '3:10' minute to second^
                define
                  down as down.sal < PREV(down.sal),
                  up as up.sal > prev(up.sal)
              ) mr
            """.trimIndent()
        ).fails("First column of ORDER BY must be of type TIMESTAMP")
    }
}
