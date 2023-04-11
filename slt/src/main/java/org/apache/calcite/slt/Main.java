/*
 * Copyright 2022 VMware, Inc.
 * SPDX-License-Identifier: MIT
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
 *
 *
 */

package org.apache.calcite.slt;

import com.beust.jcommander.ParameterException;

import org.apache.calcite.slt.executors.SqlSLTTestExecutor;
import org.apache.calcite.sql.parser.SqlParseException;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * Execute all SqlLogicTest tests.
 */
public class Main {
  static class TestLoader extends SimpleFileVisitor<Path> {
    int errors = 0;
    final TestStatistics statistics;
    public final ExecutionOptions options;

    /**
     * Creates a new class that reads tests from a directory tree and executes them.
     */
    TestLoader(ExecutionOptions options) {
      this.statistics = new TestStatistics(options.stopAtFirstError);
      this.options = options;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
      SqlSLTTestExecutor executor = null;
      try {
        executor = this.options.getExecutor();
      } catch (IOException | SQLException | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
      String extension = Utilities.getFileExtension(file.toString());
      if (attrs.isRegularFile() && extension != null && extension.equals("test")) {
        SLTTestFile test = null;
        try {
          System.out.println(file);
          test = new SLTTestFile(file.toString());
          test.parse();
        } catch (Exception ex) {
          // This is not a query evaluation error
          ex.printStackTrace();
          this.errors++;
        }
        if (test != null) {
          try {
            TestStatistics stats = executor.execute(test, options);
            this.statistics.add(stats);
          } catch (SqlParseException | IOException | InterruptedException |
                   SQLException | NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);
          }
        }
      }
      return FileVisitResult.CONTINUE;
    }
  }

  public static void main(String[] argv) throws IOException {
    Logger rootLogger = LogManager.getLogManager().getLogger("");
    rootLogger.setLevel(Level.WARNING);
    for (Handler h : rootLogger.getHandlers()) {
      h.setLevel(Level.INFO);
    }

    ExecutionOptions options = new ExecutionOptions();
    try {
      options.parse(argv);
      System.out.println(options);
    } catch (ParameterException ex) {
      options.help();
      System.exit(1);
    }
    if (options.help) {
      options.help();
      System.exit(1);
    }
    if (options.sltDirectory == null) {
      System.err.println("Please specify the directory with the SqlLogicTest suite using the -d flag");
      options.help();
      System.exit(1);
    }

    TestLoader loader = new TestLoader(options);
    for (String file : options.getDirectories()) {
      Path path = Paths.get(options.sltDirectory + "/test/" + file);
      Files.walkFileTree(path, loader);
    }
    System.out.println("Files that could not be not parsed: " + loader.errors);
    System.out.println(loader.statistics);
  }
}
