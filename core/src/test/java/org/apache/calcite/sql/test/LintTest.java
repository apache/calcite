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
package org.apache.calcite.sql.test;

import org.apache.calcite.util.Puffin;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.TestUnsafe;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;

/** Various automated checks on the documentation. */
class LintTest {
  @SuppressWarnings("Convert2MethodRef") // JDK 8 requires lambdas
  private Puffin.Program<GlobalState> makeProgram() {
    return Puffin.builder(GlobalState::new, global -> new FileState(global))
        .add(line -> line.fnr() == 1,
            line -> line.globalState().fileCount++)

        // Javadoc does not require '</p>', so we do not allow '</p>'
        .add(line -> line.state().inJavadoc()
                && line.contains("</p>"),
            line -> line.state().message("no '</p>'", line))

        // No "**/"
        .add(line -> line.contains("**/")
                && line.state().inJavadoc(),
            line ->
                line.state().message("no '**/'; use '*/'",
                    line))

        // A Javadoc paragraph '<p>' must not be on its own line.
        .add(line -> line.matches("^ *\\* <p>"),
            line ->
                line.state().message("<p> must not be on its own line",
                    line))

        // A Javadoc paragraph '<p>' must be preceded by a blank Javadoc
        // line.
        .add(line -> line.matches("^ *\\*"),
            line -> line.state().starLine = line.fnr())
        .add(line -> line.matches("^ *\\* <p>.*")
                && line.fnr() - 1 != line.state().starLine,
            line ->
                line.state().message("<p> must be preceded by blank line",
                    line))

        // The first "@param" of a javadoc block must be preceded by a blank
        // line.
        .add(line -> line.matches("^ */\\*\\*.*"),
            line -> line.state().javadocStartLine = line.fnr())
        .add(line -> line.matches(".*\\*/"),
            line -> line.state().javadocEndLine = line.fnr())
        .add(line -> line.matches("^ *\\* @.*"),
            line -> {
              if (line.state().inJavadoc()
                  && line.state().atLine < line.state().javadocStartLine
                  && line.fnr() - 1 != line.state().starLine) {
                line.state().message(
                    "First @tag must be preceded by blank line",
                    line);
              }
              line.state().atLine = line.fnr();
            })
        .build();
  }

  @Test void testProgramWorks() {
    final String code = "class MyClass {\n"
        + "  /** Paragraph.\n"
        + "   *\n"
        + "   * Missing p.\n"
        + "   *\n"
        + "   * <p>\n"
        + "   * <p>A paragraph (p must be preceded by blank line).\n"
        + "   *\n"
        + "   * <p>no p</p>\n"
        + "   * @see java.lang.String (should be preceded by blank line)\n"
        + "   **/\n"
        + "  String x = \"ok because it's not in javadoc:</p>\";\n"
        + "}\n";
    final String expectedMessages = "["
        + "GuavaCharSource{memory}:6:"
        + "<p> must not be on its own line\n"
        + "GuavaCharSource{memory}:7:"
        + "<p> must be preceded by blank line\n"
        + "GuavaCharSource{memory}:9:"
        + "no '</p>'\n"
        + "GuavaCharSource{memory}:10:"
        + "First @tag must be preceded by blank line\n"
        + "GuavaCharSource{memory}:11:"
        + "no '**/'; use '*/']";
    final Puffin.Program<GlobalState> program = makeProgram();
    final StringWriter sw = new StringWriter();
    final GlobalState g;
    try (PrintWriter pw = new PrintWriter(sw)) {
      g = program.execute(Stream.of(Sources.of(code)), pw);
    }
    assertThat(g.messages.toString().replace(", ", "\n"),
        is(expectedMessages));
  }

  /** Tests that source code has no flaws. */
  @Test void testLint() {
    final Puffin.Program<GlobalState> program = makeProgram();

    final GlobalState g;
    try (PrintWriter pw = Util.printWriter(System.out)) {
      final List<File> javaFiles = getJavaFiles();
      g = program.execute(javaFiles.parallelStream().map(Sources::of), pw);
    }

    System.out.println("Lint: " + g.fileCount + " files,"
        + g.messages.size() + " warnings");
    for (Message message : g.messages) {
      System.out.println(message);
    }
    assertThat(g.messages, empty());
  }

  /** Returns a list of Java files in git under a given directory.
   *
   * <p>Assumes running Linux or macOS, and that git is available. */
  static List<File> getJavaFiles() {
    File base = TestUtil.getBaseDir(LintTest.class);
    return TestUnsafe.getJavaFiles(base);
  }

  /** Warning that code is not as it should be. */
  private static class Message {
    final Source source;
    final int line;
    final String message;

    Message(Source source, int line, String message) {
      this.source = source;
      this.line = line;
      this.message = message;
    }

    @Override public String toString() {
      return source + ":" + line + ":" + message;
    }
  }

  /** Internal state of the lint rules. */
  private static class GlobalState {
    int fileCount = 0;
    final List<Message> messages = new ArrayList<>();
  }

  /** Internal state of the lint rules, per file. */
  private static class FileState {
    final GlobalState global;
    int starLine;
    int atLine;
    int javadocStartLine;
    int javadocEndLine;

    FileState(GlobalState global) {
      this.global = global;
    }

    void message(String message, Puffin.Line<GlobalState, FileState> line) {
      global.messages.add(new Message(line.source(), line.fnr(), message));
    }

    public boolean inJavadoc() {
      return javadocEndLine < javadocStartLine;
    }
  }
}
