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
package net.hydromatic.optiq.tools;

import net.hydromatic.optiq.test.OptiqAssert;

import org.eigenbase.util.Util;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;

import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Connection;

import static org.hamcrest.CoreMatchers.containsString;

/**
 * Unit test for {@link SqlRun}.
 */
public class SqlRunTest {
  @Test public void testBasic() {
    check(
        "!use foodmart\n"
        + "select count(*) as c1 from \"foodmart\".\"days\";\n"
        + "!ok\n"
        + "!set outputformat mysql\n"
        + "select count(*) as c1 from \"foodmart\".\"days\";\n"
        + "!ok\n"
        + "!plan\n"
        + "\n",
        "!use foodmart\n"
        + "select count(*) as c1 from \"foodmart\".\"days\";\n"
        + "C1\n"
        + "7\n"
        + "!ok\n"
        + "!set outputformat mysql\n"
        + "select count(*) as c1 from \"foodmart\".\"days\";\n"
        + "+----+\n"
        + "| C1 |\n"
        + "+----+\n"
        + "| 7  |\n"
        + "+----+\n"
        + "(1 row)\n"
        + "\n"
        + "!ok\n"
        + "JdbcToEnumerableConverter\n"
        + "  JdbcAggregateRel(group=[{}], C1=[COUNT()])\n"
        + "    JdbcProjectRel(DUMMY=[0])\n"
        + "      JdbcTableScan(table=[[foodmart, days]])\n"
        + "!plan\n"
        + "\n");
  }

  @Test public void testError() {
    check(
        "!use foodmart\n"
        + "select blah from blah;\n"
        + "!ok\n"
        + "\n",
        containsString(
            "!use foodmart\n"
            + "select blah from blah;\n"
            + "java.sql.SQLException: error while executing SQL \"select blah from blah\n"
            + "\": From line 1, column 18 to line 1, column 21: Table 'BLAH' not found"));
  }

  @Test public void testPlan() {
    check(
        "!use foodmart\n"
        + "values (1), (2);\n"
        + "!plan\n"
        + "\n",
        containsString(
            "!use foodmart\n"
            + "values (1), (2);\n"
            + "EnumerableValuesRel(tuples=[[{ 1 }, { 2 }]])\n"
            + "!plan\n"));
  }

  @Test public void testPlanAfterOk() {
    check(
        "!use foodmart\n"
        + "values (1), (2);\n"
        + "!ok\n"
        + "!plan\n"
        + "\n",
        containsString(
            "!use foodmart\n"
            + "values (1), (2);\n"
            + "EXPR$0\n"
            + "1\n"
            + "2\n"
            + "!ok\n"
            + "EnumerableValuesRel(tuples=[[{ 1 }, { 2 }]])\n"
            + "!plan\n"
            + "\n"));
  }

  /** It is OK to have consecutive '!plan' calls and no '!ok'.
   * (Previously there was a "result already open" error.) */
  @Test public void testPlanPlan() {
    check(
        "!use foodmart\n"
        + "values (1), (2);\n"
        + "!plan\n"
        + "values (3), (4);\n"
        + "!plan\n"
        + "!ok\n"
        + "\n",
        containsString(
            "!use foodmart\n"
            + "values (1), (2);\n"
            + "EnumerableValuesRel(tuples=[[{ 1 }, { 2 }]])\n"
            + "!plan\n"
            + "values (3), (4);\n"
            + "EnumerableValuesRel(tuples=[[{ 3 }, { 4 }]])\n"
            + "!plan\n"
            + "EXPR$0\n"
            + "3\n"
            + "4\n"
            + "!ok\n"
            + "\n"));
  }

  /** Content inside a '!ok' command, that needs to be matched. */
  @Test public void testOkContent() {
    check(
        "!use foodmart\n"
        + "values (1), (2);\n"
        + "baz\n"
        + "!ok\n"
        + "\n",
        containsString(
            "!use foodmart\n"
            + "values (1), (2);\n"
            + "EXPR$0\n"
            + "1\n"
            + "2\n"
            + "!ok\n"
            + "\n"));
  }

  /** Content inside a '!plan' command, that needs to be matched. */
  @Test public void testPlanContent() {
    check(
        "!use foodmart\n"
        + "values (1), (2);\n"
        + "foo\n"
        + "!plan\n"
        + "baz\n"
        + "!ok\n"
        + "\n",
        containsString(
            "!use foodmart\n"
            + "values (1), (2);\n"
            + "EnumerableValuesRel(tuples=[[{ 1 }, { 2 }]])\n"
            + "!plan\n"
            + "EXPR$0\n"
            + "1\n"
            + "2\n"
            + "!ok\n"
            + "\n"));
  }

  @Test public void testIfFalse() {
    check(
        "!use foodmart\n"
        + "!if (false) {\n"
        + "values (1), (2);\n"
        + "anything\n"
        + "you like\n"
        + "!plan\n"
        + "!}\n"
        + "\n",
        containsString(
            "!use foodmart\n"
            + "!if (false) {\n"
            + "values (1), (2);\n"
            + "anything\n"
            + "you like\n"
            + "!plan\n"
            + "!}\n"
            + "\n"));
  }

  @Test public void testIfTrue() {
    check(
        "!use foodmart\n"
        + "!if (true) {\n"
        + "values (1), (2);\n"
        + "anything\n"
        + "you like\n"
        + "!ok\n"
        + "!}\n"
        + "\n",
        containsString(
            "!use foodmart\n"
            + "!if (true) {\n"
            + "values (1), (2);\n"
            + "EXPR$0\n"
            + "1\n"
            + "2\n"
            + "!ok\n"
            + "!}\n"
            + "\n"));
  }

  static void check(String input, String expected) {
    check(input, CoreMatchers.equalTo(expected));
  }

  static void check(String input, Matcher<String> matcher) {
    final StringWriter writer = new StringWriter();
    final SqlRun run =
        new SqlRun(new BufferedReader(new StringReader(input)), writer);
    run.execute(
        new SqlRun.ConnectionFactory() {
          public Connection connect(String name) throws Exception {
            if (name.equals("foodmart")) {
              return OptiqAssert.that().with(OptiqAssert.Config.JDBC_FOODMART)
                  .connect();
            }
            throw new RuntimeException("unknown connection '" + name + "'");
          }
        });
    writer.flush();
    String out = Util.toLinux(writer.toString());
    Assert.assertThat(out, matcher);
  }
}

// End SqlRunTest.java
