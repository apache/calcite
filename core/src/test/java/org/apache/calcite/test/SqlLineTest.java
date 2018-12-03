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

import org.apache.calcite.util.Bug;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import org.hamcrest.Matcher;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlline.SqlLine;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests that we can invoke SqlLine on a Calcite connection.
 */
public class SqlLineTest {
  /**
   * Execute a script with "sqlline -f".
   *
   * @throws java.lang.Throwable On error
   * @return The stderr and stdout from running the script
   * @param args Script arguments
   */
  private static Pair<SqlLine.Status, String> run(String... args)
      throws Throwable {
    SqlLine sqlline = new SqlLine();
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PrintStream sqllineOutputStream =
        new PrintStream(os, false, StandardCharsets.UTF_8.name());
    sqlline.setOutputStream(sqllineOutputStream);
    sqlline.setErrorStream(sqllineOutputStream);
    SqlLine.Status status = SqlLine.Status.OK;

    Bug.upgrade("[sqlline-35] Make Sqlline.begin public");
    // TODO: status = sqlline.begin(args, null, false);

    return Pair.of(status, os.toString("UTF8"));
  }

  private static Pair<SqlLine.Status, String> runScript(File scriptFile,
      boolean flag) throws Throwable {
    List<String> args = new ArrayList<>();
    Collections.addAll(args, "-u", "jdbc:calcite:", "-n", "sa", "-p", "");
    if (flag) {
      args.add("-f");
      args.add(scriptFile.getAbsolutePath());
    } else {
      args.add("--run=" + scriptFile.getAbsolutePath());
    }
    return run(args.toArray(new String[0]));
  }

  /**
   * Attempts to execute a simple script file with the -f option to SqlLine.
   * Tests for presence of an expected pattern in the output (stdout or stderr).
   *
   * @param scriptText Script text
   * @param flag Command flag (--run or -f)
   * @param statusMatcher Checks whether status is as expected
   * @param outputMatcher Checks whether output is as expected
   * @throws Exception on command execution error
   */
  private void checkScriptFile(String scriptText, boolean flag,
      Matcher<SqlLine.Status> statusMatcher,
      Matcher<String> outputMatcher) throws Throwable {
    // Put the script content in a temp file
    File scriptFile = File.createTempFile("foo", "temp");
    scriptFile.deleteOnExit();
    try (PrintWriter w = Util.printWriter(scriptFile)) {
      w.print(scriptText);
    }

    Pair<SqlLine.Status, String> pair = runScript(scriptFile, flag);

    // Check output before status. It gives a better clue what went wrong.
    assertThat(pair.right, outputMatcher);
    assertThat(pair.left, statusMatcher);
    final boolean delete = scriptFile.delete();
    assertThat(delete, is(true));
  }

  @Test public void testSqlLine() throws Throwable {
    checkScriptFile("!tables", false, equalTo(SqlLine.Status.OK), equalTo(""));
  }
}

// End SqlLineTest.java
