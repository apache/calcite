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
package org.apache.calcite.adapter.os;

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Unit tests for the OS (operating system) adapter.
 *
 * <p>Also please run the following tests manually, from your shell:
 *
 * <ul>
 *   <li>./sqlsh select \* from du
 *   <li>./sqlsh select \* from files
 *   <li>./sqlsh select \* from git_commits
 *   <li>./sqlsh select \* from ps
 *   <li>(echo cats; echo and dogs) | ./sqlsh select \* from stdin
 *   <li>./sqlsh select \* from vmstat
 * </ul>
 */
public class OsAdapterTest {
  private static boolean isWindows() {
    return System.getProperty("os.name").startsWith("Windows");
  }

  /** Returns whether there is a ".git" directory in this directory or in a
   * directory between this directory and root. */
  private static boolean hasGit() {
    assumeToolExists("git");
    final String path = Sources.of(OsAdapterTest.class.getResource("/"))
        .file().getAbsolutePath();
    File f = new File(path);
    for (;;) {
      if (f == null || !f.exists()) {
        return false; // abandon hope
      }
      File[] files =
          f.listFiles((dir, name) -> name.equals(".git"));
      if (files != null && files.length == 1) {
        return true; // there is a ".git" subdirectory
      }
      f = f.getParentFile();
    }
  }

  private static void assumeToolExists(String command) {
    Assume.assumeTrue(command + " does not exist", checkProcessExists(command));
  }

  private static boolean checkProcessExists(String command) {
    try {
      Process process = new ProcessBuilder().command(command).start();
      Assert.assertNotNull(process);
      int errCode = process.waitFor();
      Assert.assertEquals(0, errCode);
      return true;
    } catch (AssertionError | IOException | InterruptedException e) {
      return false;
    }
  }

  @Test public void testDu() {
    Assume.assumeFalse("Skip: the 'du' table does not work on Windows",
        isWindows());
    assumeToolExists("du");
    sql("select * from du")
        .returns(r -> {
          try {
            assertThat(r.next(), is(true));
            assertThat(r.getInt(1), notNullValue());
            assertThat(r.getString(2), CoreMatchers.startsWith("./"));
            assertThat(r.wasNull(), is(false));
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        });
  }

  @Test public void testDuFilterSortLimit() {
    Assume.assumeFalse("Skip: the 'du' table does not work on Windows",
        isWindows());
    assumeToolExists("du");
    sql("select * from du where path like '%/src/test/java/%'\n"
        + "order by 1 limit 2")
        .returns(r -> {
          try {
            assertThat(r.next(), is(true));
            assertThat(r.getInt(1), notNullValue());
            assertThat(r.getString(2), CoreMatchers.startsWith("./"));
            assertThat(r.wasNull(), is(false));
            assertThat(r.next(), is(true));
            assertThat(r.next(), is(false)); // because of "limit 2"
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        });
  }

  @Test public void testFiles() {
    Assume.assumeFalse("Skip: the 'files' table does not work on Windows",
        isWindows());
    sql("select distinct type from files")
        .returnsUnordered("type=d",
            "type=f");
  }

  @Test public void testPs() {
    Assume.assumeFalse("Skip: the 'ps' table does not work on Windows",
        isWindows());
    assumeToolExists("ps");
    sql("select * from ps")
        .returns(r -> {
          try {
            assertThat(r.next(), is(true));
            final StringBuilder b = new StringBuilder();
            final int c = r.getMetaData().getColumnCount();
            for (int i = 0; i < c; i++) {
              b.append(r.getString(i + 1)).append(';');
              assertThat(r.wasNull(), is(false));
            }
            assertThat(b.toString(), notNullValue());
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        });
  }

  @Test public void testPsDistinct() {
    Assume.assumeFalse("Skip: the 'ps' table does not work on Windows",
        isWindows());
    assumeToolExists("ps");
    sql("select distinct `user` from ps")
        .returns(r -> {
          try {
            assertThat(r.next(), is(true));
            assertThat(r.getString(1), notNullValue());
            assertThat(r.wasNull(), is(false));
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        });
  }

  @Test public void testGitCommits() {
    Assume.assumeTrue("no git", hasGit());
    sql("select count(*) from git_commits")
        .returns(r -> {
          try {
            assertThat(r.next(), is(true));
            assertThat(r.getString(1), notNullValue());
            assertThat(r.wasNull(), is(false));
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        });
  }

  @Test public void testGitCommitsTop() {
    Assume.assumeTrue("no git", hasGit());
    final String q = "select author from git_commits\n"
        + "group by 1 order by count(*) desc limit 2";
    sql(q).returnsUnordered("author=Julian Hyde <julianhyde@gmail.com>",
        "author=Julian Hyde <jhyde@apache.org>");
  }

  @Test public void testJps() {
    final String q = "select pid, info from jps";
    sql(q).returns(r -> {
      try {
        assertThat(r.next(), is(true));
        assertThat(r.getString(1), notNullValue());
        assertThat(r.getString(2), notNullValue());
        assertThat(r.wasNull(), is(false));
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    });
  }

  @Test public void testVmstat() {
    Assume.assumeFalse("Skip: the 'files' table does not work on Windows",
        isWindows());
    assumeToolExists("vmstat");
    sql("select * from vmstat")
        .returns(r -> {
          try {
            assertThat(r.next(), is(true));
            final int c = r.getMetaData().getColumnCount();
            for (int i = 0; i < c; i++) {
              assertThat(r.getLong(i + 1), notNullValue());
              assertThat(r.wasNull(), is(false));
            }
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        });
  }

  @Test public void testStdin() throws SQLException {
    try (Hook.Closeable ignore = Hook.STANDARD_STREAMS.addThread(
        (Consumer<Holder<Object[]>>) o -> {
          final Object[] values = o.get();
          final InputStream in = (InputStream) values[0];
          final String s = "First line\n"
              + "Second line";
          final ByteArrayInputStream in2 =
              new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
          final OutputStream out = (OutputStream) values[1];
          final OutputStream err = (OutputStream) values[2];
          o.set(new Object[] {in2, out, err});
        })) {
      assertThat(foo("select count(*) as c from stdin"), is("2\n"));
    }
  }

  @Test public void testStdinExplain() {
    // Can't execute stdin, because junit's stdin never ends;
    // so just run explain
    final String explain = "PLAN="
        + "EnumerableAggregate(group=[{}], c=[COUNT()])\n"
        + "  EnumerableTableFunctionScan(invocation=[stdin(true)], "
        + "rowType=[RecordType(INTEGER ordinal, VARCHAR line)], "
        + "elementType=[class [Ljava.lang.Object;])";
    sql("select count(*) as c from stdin")
        .explainContains(explain);
  }

  @Test public void testSqlShellFormat() throws SQLException {
    final String q = "select * from (values (-1, true, 'a'),"
        + " (2, false, 'b, c'),"
        + " (3, unknown, cast(null as char(1)))) as t(x, y, z)";
    final String empty = q + " where false";

    final String spacedOut = "-1 true a   \n"
        + "2 false b, c\n"
        + "3 null null\n";
    assertThat(foo("-o", "spaced", q), is(spacedOut));

    assertThat(foo("-o", "spaced", empty), is(""));

    // default is 'spaced'
    assertThat(foo(q), is(spacedOut));

    final String headersOut = "x y z\n"
        + spacedOut;
    assertThat(foo("-o", "headers", q), is(headersOut));

    final String headersEmptyOut = "x y z\n";
    assertThat(foo("-o", "headers", empty), is(headersEmptyOut));

    final String jsonOut = "[\n"
        + "{\n"
        + "  \"x\": -1,\n"
        + "  \"y\": true,\n"
        + "  \"z\": \"a   \"\n"
        + "},\n"
        + "{\n"
        + "  \"x\": 2,\n"
        + "  \"y\": false,\n"
        + "  \"z\": \"b, c\"\n"
        + "},\n"
        + "{\n"
        + "  \"x\": 3,\n"
        + "  \"y\": null,\n"
        + "  \"z\": null\n"
        + "}\n"
        + "]\n";
    assertThat(foo("-o", "json", q), is(jsonOut));

    final String jsonEmptyOut = "[\n"
        + "]\n";
    assertThat(foo("-o", "json", empty), is(jsonEmptyOut));

    final String csvEmptyOut = "[\n"
        + "]\n";
    assertThat(foo("-o", "json", empty), is(csvEmptyOut));

    final String csvOut = "x,y,z\n"
        + "-1,true,a   \n"
        + "2,false,\"b, c\"\n"
        + "3,,";
    assertThat(foo("-o", "csv", q), is(csvOut));

    final String mysqlOut = ""
        + "+----+-------+------+\n"
        + "|  x | y     | z    |\n"
        + "+----+-------+------+\n"
        + "| -1 | true  | a    |\n"
        + "|  2 | false | b, c |\n"
        + "|  3 |       |      |\n"
        + "+----+-------+------+\n"
        + "(3 rows)\n"
        + "\n";
    assertThat(foo("-o", "mysql", q), is(mysqlOut));

    final String mysqlEmptyOut = ""
        + "+---+---+---+\n"
        + "| x | y | z |\n"
        + "+---+---+---+\n"
        + "+---+---+---+\n"
        + "(0 rows)\n"
        + "\n";
    assertThat(foo("-o", "mysql", empty), is(mysqlEmptyOut));
  }

  private String foo(String... args) throws SQLException {
    final ByteArrayInputStream inStream = new ByteArrayInputStream(new byte[0]);
    final InputStreamReader in =
        new InputStreamReader(inStream, StandardCharsets.UTF_8);
    final StringWriter outSw = new StringWriter();
    final PrintWriter out = new PrintWriter(outSw);
    final StringWriter errSw = new StringWriter();
    final PrintWriter err = new PrintWriter(errSw);
    new SqlShell(in, out, err, args).run();
    return Util.toLinux(outSw.toString());
  }

  @Test public void testSqlShellHelp() throws SQLException {
    final String help = "Usage: sqlsh [OPTION]... SQL\n"
        + "Execute a SQL command\n"
        + "\n"
        + "Options:\n"
        + "  -o FORMAT  Print output in FORMAT; options are 'spaced' (the "
        + "default), 'csv',\n"
        + "             'headers', 'json', 'mysql'\n"
        + "  -h --help  Print this help\n";
    final String q = "select 1";
    assertThat(foo("--help", q), is(help));

    assertThat(foo("-h", q), is(help));

    try {
      final String s = foo("-o", "bad", q);
      fail("expected exception, got " + s);
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), is("unknown format: bad"));
    }
  }

  static CalciteAssert.AssertQuery sql(String sql) {
    return CalciteAssert.that()
        .withModel(SqlShell.MODEL)
        .with(CalciteConnectionProperty.LEX, Lex.JAVA)
        .with(CalciteConnectionProperty.CONFORMANCE, SqlConformanceEnum.LENIENT)
        .query(sql);
  }
}

// End OsAdapterTest.java
