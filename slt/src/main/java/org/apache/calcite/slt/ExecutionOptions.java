/*
 * Copyright 2022 VMware, Inc.
 * SPDX-License-Identifier: MIT
 * SPDX-License-Identifier: Apache-2.0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.apache.calcite.slt;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import org.apache.calcite.slt.executors.CalciteExecutor;

import org.apache.calcite.slt.executors.JDBCExecutor;
import org.apache.calcite.slt.executors.NoExecutor;

import org.apache.calcite.slt.executors.SqlSLTTestExecutor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

@SuppressWarnings("CanBeFinal")
public class ExecutionOptions {
  public static class ExecutorValidator implements IParameterValidator {
    private final Set<String> legalExecutors;

    public ExecutorValidator() {
      this.legalExecutors = new HashSet<>();
      this.legalExecutors.add("JDBC");
      this.legalExecutors.add("none");
      this.legalExecutors.add("calcite");
    }

    @Override
    public void validate(String name, String value) throws ParameterException {
      if (this.legalExecutors.contains(value))
        return;
      throw new ParameterException("Illegal executor name " + value + "\n"
          + "Legal values are: " + this.legalExecutors);
    }
  }

  @Parameter(names = "-h", description = "Show this help message and exit")
  public boolean help = false;
  @Parameter(names = "-i", description = "Install the SLT tests if the directory does not exist")
  public boolean install = false;
  @Parameter(names = "-d", description = "Directory with SLT tests")
  public String sltDirectory;
  @Parameter(names = "-x", description = "Stop at the first encountered query error")
  public boolean stopAtFirstError = false;
  @Parameter(description = "Files or directories with test data (relative to the specified directory)")
  List<String> directories = new ArrayList<>();
  @Parameter(names = "-n", description = "Do not execute, just parse the test files")
  boolean doNotExecute;
  @Parameter(names = "-e", description = "Executor to use; one of 'none, JDBC, calcite'", validateWith = ExecutorValidator.class)
  String executor = "calcite";
  @Parameter(names = "-s", description = "Ignore the status of SQL commands executed")
  boolean validateStatus;
  @Parameter(names = "-b", description = "Load a list of buggy commands to skip from this file")
  @Nullable
  String bugsFile = null;

  /**
   * Read the list of statements and queries to skip from a file.
   */
  HashSet<String> readBugsFile(String fileName) throws IOException {
    HashSet<String> bugs = new HashSet<>();
    File file = new File(fileName);
    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      while (true) {
        String line = reader.readLine();
        if (line == null) break;
        if (line.startsWith("//"))
          continue;
        bugs.add(line);
      }
    }
    return bugs;
  }

  final JCommander commander;

  String jdbcConnectionString() {
    return "jdbc:hsqldb:mem:db";
  }

  JDBCExecutor jdbcExecutor(HashSet<String> sltBugs) {
    JDBCExecutor jdbc = new JDBCExecutor(this.jdbcConnectionString());
    jdbc.avoid(sltBugs);
    jdbc.setValidateStatus(this.validateStatus);
    return jdbc;
  }

  SqlSLTTestExecutor getExecutor() throws IOException, SQLException {
    HashSet<String> sltBugs = new HashSet<>();
    if (this.bugsFile != null) {
      sltBugs = this.readBugsFile(this.bugsFile);
    }

    switch (this.executor) {
    case "none":
      return new NoExecutor();
    case "JDBC": {
      return this.jdbcExecutor(sltBugs);
    }
    case "calcite": {
      JDBCExecutor jdbc = this.jdbcExecutor(sltBugs);
      CalciteExecutor result = new CalciteExecutor(jdbc);
      result.avoid(sltBugs);
      result.setValidateStatus(this.validateStatus);
      return result;
    }
    default:
      break;
    }
    throw new RuntimeException("Unknown executor: " + this.executor);  // unreachable
  }

  public List<String> getDirectories() {
    return this.directories;
  }

  public ExecutionOptions() {
      this.commander = JCommander.newBuilder()
          .addObject(this)
          .build();
      this.commander.setProgramName("slt");
  }

  public void usage() {
    this.commander.usage();
  }

  public void parse(String... argv) {
    this.commander.parse(argv);
  }

  @Override
  public String toString() {
    return "ExecutionOptions{" +
        "root=" + this.sltDirectory +
        ", files=" + this.directories +
        ", execute=" + !this.doNotExecute +
        ", executor=" + this.executor +
        ", stopAtFirstError=" + this.stopAtFirstError +
        '}';
  }
}
