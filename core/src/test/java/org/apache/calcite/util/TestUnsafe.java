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
package org.apache.calcite.util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;

/**
 * Unsafe methods to be used by tests.
 *
 * <p>Contains methods that call JDK methods that the
 * <a href="https://github.com/policeman-tools/forbidden-apis">forbidden
 * APIs checker</a> does not approve of.
 *
 * <p>This class is excluded from the check, so methods called via this class
 * will not fail the build.
 *
 * <p>Why is this in {@code core/src/test} and not in {@code testkit/src/main}?
 * Because some of the methods (e.g. {@link #runAppProcess}) are so unsafe that
 * they must not be on the class-path of production code.
 */
public abstract class TestUnsafe {
  /**
   * Runs an external application process.
   *
   * @param argumentList  command name and its arguments
   * @param directory  working directory
   * @param logger    if not null, command and exit status will be logged here
   * @param appInput  if not null, data will be copied to application's stdin
   * @param appOutput if not null, data will be captured from application's
   *                  stdout and stderr
   * @return application process exit value
   */
  public static int runAppProcess(List<String> argumentList, File directory,
      @Nullable Logger logger, @Nullable Reader appInput,
      @Nullable Writer appOutput) throws IOException, InterruptedException {

    // WARNING: ProcessBuilder is security-sensitive. Its use is currently
    // safe because this code is under "core/test". Developers must not move
    // this code into "core/main".
    final ProcessBuilder pb = new ProcessBuilder(argumentList);
    pb.directory(directory);
    pb.redirectErrorStream(true);
    if (logger != null) {
      logger.info("start process: " + pb.command());
    }
    Process p = pb.start();

    // Setup the input/output streams to the subprocess.
    // The buffering here is arbitrary. Javadocs strongly encourage
    // buffering, but the size needed is very dependent on the
    // specific application being run, the size of the input
    // provided by the caller, and the amount of output expected.
    // Since this method is currently used only by unit tests,
    // large-ish fixed buffer sizes have been chosen. If this
    // method becomes used for something in production, it might
    // be better to have the caller provide them as arguments.
    if (appInput != null) {
      OutputStream out =
          new BufferedOutputStream(
              p.getOutputStream(),
              100 * 1024);
      int c;
      while ((c = appInput.read()) != -1) {
        out.write(c);
      }
      out.flush();
    }
    if (appOutput != null) {
      InputStream in =
          new BufferedInputStream(
              p.getInputStream(),
              100 * 1024);
      int c;
      while ((c = in.read()) != -1) {
        appOutput.write(c);
      }
      appOutput.flush();
      in.close();
    }
    p.waitFor();

    int status = p.exitValue();
    if (logger != null) {
      logger.info("exit status=" + status + " from " + pb.command());
    }
    return status;
  }

  /** Returns a list of Java files in git under a given directory.
   *
   * <p>Assumes running Linux or macOS, and that git is available. */
  public static List<File> getJavaFiles(File base) {
    String s;
    try {
      final List<String> argumentList =
          ImmutableList.of("git", "ls-files", "*.java");
      try {
        final StringWriter sw = new StringWriter();
        int status =
            runAppProcess(argumentList, base, null, null, sw);
        if (status != 0) {
          throw new RuntimeException("command " + argumentList
              + ": exited with status " + status);
        }
        s = sw.toString();
      } catch (Exception e) {
        throw new RuntimeException("command " + argumentList
            + ": failed with exception", e);
      }

      final ImmutableList.Builder<File> files = ImmutableList.builder();
      try (StringReader r = new StringReader(s);
           BufferedReader br = new BufferedReader(r)) {
        for (;;) {
          String line = br.readLine();
          if (line == null) {
            break;
          }
          files.add(new File(base, line));
        }
      }
      return files.build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
