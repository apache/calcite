/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.apache.optiq.tools;

import org.apache.optiq.test.OptiqAssert;

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
    String out = writer.toString();
    Assert.assertThat(out, matcher);
  }
}

// End SqlRunTest.java
