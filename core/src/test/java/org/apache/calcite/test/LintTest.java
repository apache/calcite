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

import org.apache.calcite.util.Puffin;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.TestUnsafe;
import org.apache.calcite.util.Util;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Various automated checks on the code and git history. */
class LintTest {
  /** Pattern that matches "[CALCITE-12]" or "[CALCITE-1234]" followed by a
   * space. */
  private static final Pattern CALCITE_PATTERN =
      Pattern.compile("^(\\[CALCITE-[0-9]{1,4}][ ]).*");

  @SuppressWarnings("Convert2MethodRef") // JDK 8 requires lambdas
  private Puffin.Program<GlobalState> makeProgram() {
    return Puffin.builder(GlobalState::new, global -> new FileState(global))
        .add(line -> line.fnr() == 1,
            line -> line.globalState().fileCount++)

        // Comment without space
        .add(line -> line.matches(".* //[^ ].*")
                && !line.source().fileOpt()
                    .filter(f -> f.getName().equals("LintTest.java")).isPresent()
                && !line.contains("//--")
                && !line.contains("//~")
                && !line.contains("//noinspection")
                && !line.contains("//CHECKSTYLE"),
            line -> line.state().message("'//' must be followed by ' '", line))

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
            line -> {
              final FileState f = line.state();
              if (f.starLine == line.fnr() - 1) {
                f.message("duplicate empty line in javadoc", line);
              }
              f.starLine = line.fnr();
            })
        .add(line -> line.matches("^ *\\* <p>.*")
                && line.fnr() - 1 != line.state().starLine,
            line ->
                line.state().message("<p> must be preceded by blank line",
                    line))

        // A non-blank line following a blank line must have a '<p>'
        .add(line -> line.state().inJavadoc()
                && line.state().ulCount == 0
                && line.state().blockquoteCount == 0
                && line.contains("* ")
                && line.fnr() - 1 == line.state().starLine
                && line.matches("^ *\\* [^<@].*"),
            line -> line.state().message("missing '<p>'", line))

        // The first "@param" of a javadoc block must be preceded by a blank
        // line.
        .add(line -> line.matches("^ */\\*\\*.*"),
            line -> {
              final FileState f = line.state();
              f.javadocStartLine = line.fnr();
              f.blockquoteCount = 0;
              f.ulCount = 0;
            })
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
        .add(line -> line.contains("<blockquote>"),
            line -> line.state().blockquoteCount++)
        .add(line -> line.contains("</blockquote>"),
            line -> line.state().blockquoteCount--)
        .add(line -> line.contains("<ul>"),
            line -> line.state().ulCount++)
        .add(line -> line.contains("</ul>"),
            line -> line.state().ulCount--)
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
        + "   *\n"
        + "   * <p>no p</p>\n"
        + "   * @see java.lang.String (should be preceded by blank line)\n"
        + "   **/\n"
        + "  String x = \"ok because it's not in javadoc:</p>\";\n"
        + "  //comment without space\n"
        + "}\n";
    final String expectedMessages = "["
        + "GuavaCharSource{memory}:4:"
        + "missing '<p>'\n"
        + "GuavaCharSource{memory}:6:"
        + "<p> must not be on its own line\n"
        + "GuavaCharSource{memory}:7:"
        + "<p> must be preceded by blank line\n"
        + "GuavaCharSource{memory}:9:"
        + "duplicate empty line in javadoc\n"
        + "GuavaCharSource{memory}:10:"
        + "no '</p>'\n"
        + "GuavaCharSource{memory}:11:"
        + "First @tag must be preceded by blank line\n"
        + "GuavaCharSource{memory}:12:"
        + "no '**/'; use '*/'\n"
        + "GuavaCharSource{memory}:14:"
        + "'//' must be followed by ' '\n"
        + "";
    final Puffin.Program<GlobalState> program = makeProgram();
    final StringWriter sw = new StringWriter();
    final GlobalState g;
    try (PrintWriter pw = new PrintWriter(sw)) {
      g = program.execute(Stream.of(Sources.of(code)), pw);
    }
    assertThat(g.messages.toString().replace(", ", "\n")
            .replace(']', '\n'),
        is(expectedMessages));
  }

  /** Tests that source code has no flaws. */
  @Test void testLint() {
    assumeTrue(TestUnsafe.haveGit(), "Invalid git environment");

    final Puffin.Program<GlobalState> program = makeProgram();
    final List<File> javaFiles = TestUnsafe.getJavaFiles();

    final GlobalState g;
    try (PrintWriter pw = Util.printWriter(System.out)) {
      g = program.execute(javaFiles.parallelStream().map(Sources::of), pw);
    }

    g.messages.forEach(System.out::println);
    assertThat(g.messages, empty());
  }

  /** Tests that the most recent N commit messages are good.
   *
   * <p>N needs to be large enough to verify multi-commit PRs, but not so large
   * that it fails because of historical commits. */
  @Test void testLintLog() {
    assumeTrue(TestUnsafe.haveGit(), "Invalid git environment");

    int n = 7;
    final List<String> warnings = new ArrayList<>();
    TestUnsafe.getCommitMessages(n, (message, rest) ->
        checkMessage(message, rest, warning ->
            warnings.add("invalid git log message '" + message + "'; "
                + warning)));
    warnings.forEach(System.out::println);
    assertThat(warnings, empty());
  }

  @Test void testLogMatcher() {
    final BiFunction<String, String, List<String>> f = (subject, body) -> {
      final List<String> warnings = new ArrayList<>();
      checkMessage(subject, body, warnings::add);
      return warnings;
    };
    assertThat(f.apply(" [CALCITE-1234] abc", ""),
        hasItem("starts with space"));
    assertThat(f.apply("[CALCITE-1234]  abc", ""),
        hasItem("starts with space"));
    assertThat(f.apply("[CALCITE-12b]  abc", ""),
        hasItem("malformed [CALCITE-nnnn] reference"));
    assertThat(f.apply("[CALCITE-12345]  abc", ""),
        hasItem("malformed [CALCITE-nnnn] reference"));
    assertThat(f.apply("[CALCITE-1234]: abc", ""),
        hasItem("malformed [CALCITE-nnnn] reference"));
    assertThat(f.apply("CALCITE-1234: abc", ""),
        hasItem("malformed [CALCITE-nnnn] reference"));
    assertThat(f.apply("[CALCITE-12] Abc", ""),
        empty());
    assertThat(f.apply("[CALCITE-123] Abc", ""),
        empty());
    assertThat(f.apply("[CALCITE-1234] Fix problem with foo", ""),
        hasItem("contains 'fix' or 'fixes'; you should describe the "
            + "problem, not what you did"));
    assertThat(f.apply("[CALCITE-1234] Baz doesn't buzz", ""),
        empty());
    assertThat(f.apply("[CALCITE-1234] Baz doesn't buzz.", ""),
        hasItem("ends with period"));
    assertThat(f.apply("[CALCITE-1234]  Two problems.", ""),
        hasSize(2));
    assertThat(f.apply("[CALCITE-1234]  Two problems.", ""),
        hasItem("ends with period"));
    assertThat(f.apply("[CALCITE-1234]  Two problems.", ""),
        hasItem("starts with space"));
    assertThat(f.apply("Cosmetic: Move everything one character to left", ""),
        empty());
    assertThat(
        f.apply("Finishing up [CALCITE-4937], remove workarounds for "
            + "[CALCITE-4877]", ""),
        empty());
    assertThat(f.apply("Fix typo in filterable-model.yaml", ""),
        empty());
    assertThat(
        f.apply("Revert \"[CALCITE-4817] Expand SubstitutionVisitor\"", ""),
        empty());
    assertThat(f.apply("[CALCITE-4817] cannot start with lower-case", ""),
        hasSize(1));
    assertThat(f.apply("[CALCITE-4817] cannot start with lower-case", ""),
        hasItem("Message must start with upper-case letter"));

    // If 'Lint:skip' occurs in the body, no checks are performed
    assertThat(
        f.apply("[CALCITE-4817] cannot start with lower-case",
            "Body line 1\n"
                + "\n"
                + "Lint:skip"),
        empty());
  }

  private static void checkMessage(String subject, String body,
      Consumer<String> consumer) {
    if (body.contains("Lint:skip")) {
      return;
    }
    String subject2 = subject;
    if (subject.startsWith("[CALCITE-")
        || subject.startsWith("CALCITE-")) {
      Matcher m = CALCITE_PATTERN.matcher(subject);
      if (m.matches()) {
        subject2 = subject.substring(m.toMatchResult().end(1));
      } else {
        consumer.accept("malformed [CALCITE-nnnn] reference");
      }
      if (subject2.matches("(?i).*\\b(fix|fixes)\\b.*")) {
        consumer.accept("contains 'fix' or 'fixes'; you should describe the "
            + "problem, not what you did");
      }
    }
    if (subject2.startsWith(" ")) {
      consumer.accept("starts with space");
    }
    if (subject.endsWith(".")) {
      consumer.accept("ends with period");
    }
    if (subject.endsWith(" ")) {
      consumer.accept("ends with space");
    }
    if (subject2.matches("[a-z].*")) {
      consumer.accept("Message must start with upper-case letter");
    }
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
    int blockquoteCount;
    int ulCount;

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
