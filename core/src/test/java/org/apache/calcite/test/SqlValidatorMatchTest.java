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

import org.junit.Test;

/**
 * Validation tests for the {@code MATCH_RECOGNIZE} clause.
 */
public class SqlValidatorMatchTest extends SqlValidatorTestCase {
  /** Tries to create a calls to some internal operators in
   * MATCH_RECOGNIZE. Should fail. */
  @Test public void testMatchRecognizeInternals() throws Exception {
    sql("values ^pattern_exclude(1, 2)^")
        .fails("No match found for function signature .*");
    sql("values ^\"|\"(1, 2)^")
        .fails("No match found for function signature .*");
      // FINAL and other functions should not be visible outside of
      // MATCH_RECOGNIZE
    sql("values ^\"FINAL\"(1, 2)^")
        .fails("No match found for function signature FINAL\\(<NUMERIC>, <NUMERIC>\\)");
    sql("values ^\"RUNNING\"(1, 2)^")
        .fails("No match found for function signature RUNNING\\(<NUMERIC>, <NUMERIC>\\)");
    sql("values ^\"FIRST\"(1, 2)^")
        .fails("Function 'FIRST\\(1, 2\\)' can only be used in MATCH_RECOGNIZE");
    sql("values ^\"LAST\"(1, 2)^")
        .fails("Function 'LAST\\(1, 2\\)' can only be used in MATCH_RECOGNIZE");
    sql("values ^\"PREV\"(1, 2)^")
        .fails("Function 'PREV\\(1, 2\\)' can only be used in MATCH_RECOGNIZE");
  }

  @Test public void testMatchRecognizeDefines() throws Exception {
    final String sql = "select *\n"
        + "  from emp match_recognize (\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.sal < PREV(down.sal),\n"
        + "      up as up.sal > PREV(up.sal)\n"
        + "  ) mr";
    sql(sql).ok();
  }

  @Test public void testMatchRecognizeDefines2() throws Exception {
    final String sql = "select *\n"
        + "  from t match_recognize (\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      ^down as up.price > PREV(up.price)^\n"
        + "  ) mr";
    sql(sql).fails("Pattern variable 'DOWN' has already been defined");
  }

  @Test public void testMatchRecognizeDefines3() throws Exception {
    final String sql = "select *\n"
        + "  from emp match_recognize (\n"
        + "    pattern (strt down+up+)\n"
        + "    define\n"
        + "      down as down.sal < PREV(down.sal),\n"
        + "      up as up.sal > PREV(up.sal)\n"
        + "  ) mr";
    sql(sql).ok();
  }

  @Test public void testMatchRecognizeDefines4() throws Exception {
    final String sql = "select *\n"
        + "  from emp match_recognize (\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.sal < PREV(down.sal),\n"
        + "      up as up.sal > FIRST(^PREV(up.sal)^)\n"
        + "  ) mr";
    sql(sql)
        .fails("Cannot nest PREV/NEXT under LAST/FIRST 'PREV\\(`UP`\\.`SAL`, 1\\)'");
  }

  @Test public void testMatchRecognizeDefines5() throws Exception {
    final String sql = "select *\n"
        + "  from emp match_recognize (\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.sal < PREV(down.sal),\n"
        + "      up as up.sal > FIRST(^FIRST(up.sal)^)\n"
        + "  ) mr";
    sql(sql)
        .fails("Cannot nest PREV/NEXT under LAST/FIRST 'FIRST\\(`UP`\\.`SAL`, 0\\)'");
  }

  @Test public void testMatchRecognizeDefines6() throws Exception {
    final String sql = "select *\n"
        + "  from emp match_recognize (\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.sal < PREV(down.sal),\n"
        + "      up as up.sal > ^COUNT(down.sal, up.sal)^\n"
        + "  ) mr";
    sql(sql)
        .fails("Invalid number of parameters to COUNT method");
  }

  @Test public void testMatchRecognizeMeasures1() throws Exception {
    final String sql = "select *\n"
        + "  from emp match_recognize (\n"
        + "    measures STRT.sal as start_sal,"
        + "      ^LAST(null)^ as bottom_sal,"
        + "      LAST(up.ts) as end_sal"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.sal < PREV(down.sal),\n"
        + "      up as up.sal > prev(up.sal)\n"
        + "  ) mr";
    sql(sql)
        .fails("Null parameters in 'LAST\\(NULL, 0\\)'");
  }

  @Test public void testMatchRecognizeSkipTo1() throws Exception {
    final String sql = "select *\n"
        + "  from emp match_recognize (\n"
        + "    after match skip to ^null^\n"
        + "    measures\n"
        + "      STRT.sal as start_sal,\n"
        + "      LAST(up.ts) as end_sal\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.sal < PREV(down.sal),\n"
        + "      up as up.sal > prev(up.sal)\n"
        + "  ) mr";
    sql(sql)
        .fails("(?s).*Encountered \"null\" at .*");
  }

  @Test public void testMatchRecognizeSkipTo2() throws Exception {
    final String sql = "select *\n"
        + "  from emp match_recognize (\n"
        + "    after match skip to ^no_exists^\n"
        + "    measures\n"
        + "      STRT.sal as start_sal,"
        + "      LAST(up.ts) as end_sal"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.sal < PREV(down.sal),\n"
        + "      up as up.sal > prev(up.sal)\n"
        + "  ) mr";
    sql(sql)
        .fails("(?s).*Encountered \"measures\" at .*");
  }

  @Test public void testMatchRecognizeSkipTo3() throws Exception {
    final String sql = "select *\n"
        + "from emp match_recognize (\n"
        + "  measures\n"
        + "    STRT.sal as start_sal,\n"
        + "    LAST(up.sal) as end_sal\n"
        + "    after match skip to ^no_exists^\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.sal < PREV(down.sal),\n"
        + "      up as up.sal > prev(up.sal)\n"
        + "  ) mr";
    sql(sql)
        .fails("Unknown pattern 'NO_EXISTS'");
  }

  @Test public void testMatchRecognizeSkipToCaseInsensitive() throws Exception {
    final String sql = "select *\n"
        + "from emp match_recognize (\n"
        + "  measures\n"
        + "    STRT.sal as start_sal,\n"
        + "    LAST(up.sal) as end_sal\n"
        + "    after match skip to ^\"strt\"^\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.sal < PREV(down.sal),\n"
        + "      up as up.sal > prev(up.sal)\n"
        + "  ) mr";
    sql(sql)
        .fails("Unknown pattern 'strt'");
    sql(sql)
        .tester(tester.withCaseSensitive(false))
        .sansCarets()
        .ok();
  }

  @Test public void testMatchRecognizeSubset() throws Exception {
    final String sql = "select *\n"
        + "from emp match_recognize (\n"
        + "    pattern (strt down+ up+)\n"
        + "    subset stdn = (^strt1^, down)\n"
        + "    define\n"
        + "      down as down.sal < PREV(down.sal),\n"
        + "      up as up.sal > prev(up.sal)\n"
        + "  ) mr";
    sql(sql)
      .fails("Unknown pattern 'STRT1'");
  }

  @Test public void testMatchRecognizeSubset2() throws Exception {
    final String sql = "select *\n"
        + "from emp match_recognize (\n"
        + "    pattern (strt down+ up+)\n"
        + "    subset ^strt^ = (strt, down)\n"
        + "    define\n"
        + "      down as down.sal < PREV(down.sal),\n"
        + "      up as up.sal > prev(up.sal)\n"
        + "  ) mr";
    sql(sql)
      .fails("Pattern variable 'STRT' has already been defined");
  }

  @Test public void testMatchRecognizeWithin() throws Exception {
    final String sql = "select *\n"
        + "from emp match_recognize (\n"
        + "    pattern (strt down+ up+) within ^interval '3:10' minute to second^\n"
        + "    define\n"
        + "      down as down.sal < PREV(down.sal),\n"
        + "      up as up.sal > prev(up.sal)\n"
        + "  ) mr";
    sql(sql)
      .fails("Must contain an ORDER BY clause when WITHIN is used");
  }

  @Test public void testMatchRecognizeWithin2() throws Exception {
    final String sql = "select *\n"
        + "from emp match_recognize (\n"
        + "    order by sal\n"
        + "    pattern (strt down+ up+) within ^interval '3:10' minute to second^\n"
        + "    define\n"
        + "      down as down.sal < PREV(down.sal),\n"
        + "      up as up.sal > prev(up.sal)\n"
        + "  ) mr";
    sql(sql)
        .fails("First column of ORDER BY must be of type TIMESTAMP");
  }
}

// End SqlValidatorMatchTest.java
