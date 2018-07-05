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

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.Supplier;

/**
 * Utilities regarding operating system processes.
 *
 * <p>WARNING: Spawning processes is not secure.
 * Use this class with caution.
 * This class is in the "plus" module because "plus" is not used by default.
 * Do not move this class to the "core" module.
 */
public class Processes {
  private Processes() {}

  /** Executes a command and returns its result as an enumerable of lines. */
  static Enumerable<String> processLines(String... args) {
    return processLines(' ', args);
  }

  /** Executes a command and returns its result as an enumerable of lines. */
  static Enumerable<String> processLines(char sep, String... args) {
    return processLines(sep, processSupplier(args));
  }

  /** Executes a command and returns its result as an enumerable of lines.
   *
   * @param sep Separator character
   * @param processSupplier Command and its arguments
   */
  private static Enumerable<String> processLines(char sep,
      Supplier<Process> processSupplier) {
    if (sep != ' ') {
      return new SeparatedLinesEnumerable(processSupplier, sep);
    } else {
      return new ProcessLinesEnumerator(processSupplier);
    }
  }

  private static Supplier<Process> processSupplier(final String... args) {
    return new ProcessFactory(args);
  }

  /** Enumerator that executes a process and returns each line as an element. */
  private static class ProcessLinesEnumerator
      extends AbstractEnumerable<String> {
    private Supplier<Process> processSupplier;

    ProcessLinesEnumerator(Supplier<Process> processSupplier) {
      this.processSupplier = processSupplier;
    }

    public Enumerator<String> enumerator() {
      final Process process = processSupplier.get();
      final InputStream is = process.getInputStream();
      final BufferedInputStream bis =
          new BufferedInputStream(is);
      final InputStreamReader isr =
          new InputStreamReader(bis, StandardCharsets.UTF_8);
      final BufferedReader br = new BufferedReader(isr);
      return new Enumerator<String>() {
        private String line;

        public String current() {
          return line;
        }

        public boolean moveNext() {
          try {
            line = br.readLine();
            return line != null;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }

        public void reset() {
          throw new UnsupportedOperationException();
        }

        public void close() {
          try {
            br.close();
          } catch (IOException e) {
            throw new RuntimeException("while running " + processSupplier, e);
          }
          process.destroy();
        }
      };
    }
  }

  /** Enumerator that executes a process and returns each line as an element. */
  private static class SeparatedLinesEnumerable
      extends AbstractEnumerable<String> {
    private final Supplier<Process> processSupplier;
    private final int sep;

    SeparatedLinesEnumerable(Supplier<Process> processSupplier, char sep) {
      this.processSupplier = processSupplier;
      this.sep = sep;
    }

    public Enumerator<String> enumerator() {
      final Process process = processSupplier.get();
      final InputStream is = process.getInputStream();
      final BufferedInputStream bis =
          new BufferedInputStream(is);
      final InputStreamReader isr =
          new InputStreamReader(bis, StandardCharsets.UTF_8);
      final BufferedReader br = new BufferedReader(isr);
      return new Enumerator<String>() {
        private final StringBuilder b = new StringBuilder();
        private String line;

        public String current() {
          return line;
        }

        public boolean moveNext() {
          try {
            for (;;) {
              int c = br.read();
              if (c < 0) {
                return false;
              }
              if (c == sep) {
                line = b.toString();
                b.setLength(0);
                return true;
              }
              b.append((char) c);
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }

        public void reset() {
          throw new UnsupportedOperationException();
        }

        public void close() {
          try {
            br.close();
          } catch (IOException e) {
            throw new RuntimeException("while running " + processSupplier, e);
          }
          process.destroy();
        }
      };
    }
  }

  /** Creates processes. */
  private static class ProcessFactory implements Supplier<Process> {
    private final String[] args;

    ProcessFactory(String... args) {
      this.args = args;
    }

    public Process get() {
      try {
        return new ProcessBuilder().command(args).start();
      } catch (IOException e) {
        throw new RuntimeException("while creating process: "
            + Arrays.toString(args), e);
      }
    }

    @Override public String toString() {
      return args[0];
    }
  }
}

// End Processes.java
