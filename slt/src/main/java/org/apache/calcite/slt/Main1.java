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

package org.apache.calcite.slt;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import net.hydromatic.sqllogictest.ExecutionOptions;
import net.hydromatic.sqllogictest.executors.HsqldbExecutor;
import net.hydromatic.sqllogictest.executors.NoExecutor;
import net.hydromatic.sqllogictest.TestLoader;

import org.apache.calcite.slt.executors.CalciteExecutor;

/**
 * Execute all SqlLogicTest tests.
 */
public class Main1 {
  private Main1() {
  }

  public static void main(String[] argv) throws IOException {
    execute(true, System.out, System.err, argv);
  }

  /** As {@link #main} but does not call {@link System#exit} if {@code exit}
   * is false. */
  public static int execute(boolean exit, PrintStream out, PrintStream err,
      String... argv) throws IOException {
    ExecutionOptions options = new ExecutionOptions(exit, out, err);
    options.setBinaryName("slt");
    NoExecutor.register(options);
    HsqldbExecutor.register(options);
    CalciteExecutor.register(options);
    int parse = options.parse(argv);
    if (parse != 0) {
      return parse;
    }

    /*
    URL r = Thread.currentThread().getContextClassLoader().getResource("test");
    if (r == null) {
      out.println("Cannot find resources");
      return 1;
    }
     */
    URL r = new URL("/home/mbudiu/git/sqllogictest/test");

    TestLoader loader = new TestLoader(options);
    for (String file : options.getDirectories()) {
      Path path = Paths.get(r.getPath(), file);
      Files.walkFileTree(path, loader);
    }
    out.println("Files that could not be not parsed: " + loader.fileParseErrors);
    loader.statistics.printStatistics(out);
    return 0;
  }
}
