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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import static java.lang.Integer.parseInt;

/** Various automated checks on the code and git history. */
class LintTest {
  /** Pattern that matches "[CALCITE-12]" or "[CALCITE-1234]" followed by a
   * space. */
  private static final Pattern CALCITE_PATTERN =
      Pattern.compile("^(\\[CALCITE-[0-9]{1,4}][ ]).*");
  private static final Path ROOT_PATH = Paths.get(System.getProperty("gradle.rootDir"));

  private static final Map<String, String> TERMINOLOGY_MAP = new HashMap<>();

  @SuppressWarnings("Convert2MethodRef") // JDK 8 requires lambdas
  private Puffin.Program<GlobalState> makeProgram() {
    return Puffin.builder(GlobalState::new, global -> new FileState(global))
        .add(line -> line.fnr() == 1,
            line -> line.globalState().fileCount++)

        // Skip directive
        .add(line -> line.matches(".* lint:skip ([0-9]+).*"),
            line -> {
              final Matcher matcher = line.matcher(".* lint:skip ([0-9]+).*");
              if (matcher.matches()) {
                int n = parseInt(matcher.group(1));
                line.state().skipToLine = line.fnr() + n;
              }
            })

        // Trailing space
        .add(line -> line.endsWith(" "),
            line -> line.state().message("Trailing space", line))

        // Tab
        .add(line -> line.contains("\t")
                && !line.filename().endsWith(".txt")
                && !skipping(line),
            line -> line.state().message("Tab", line))

        // Comment without space
        .add(line -> line.matches(".* //[^ ].*")
                && !line.source().fileOpt()
                    .filter(f -> f.getName().equals("LintTest.java")).isPresent()
                && !line.contains("//--")
                && !line.contains("//~")
                && !line.contains("//noinspection")
                && !line.contains("//CHECKSTYLE"),
            line -> line.state().message("'//' must be followed by ' '", line))

        // In 'for (int i : list)', colon must be surrounded by space.
        .add(line -> line.matches("^ *for \\(.*:.*")
                && !line.matches(".*[^ ][ ][:][ ][^ ].*")
                && isJava(line.filename()),
            line -> line.state().message("':' must be surrounded by ' '", line))

        // Javadoc does not require '</p>', so we do not allow '</p>'
        .add(line -> line.state().inJavadoc()
                && line.contains("</p>"),
            line -> line.state().message("no '</p>'", line))

        // No "**/"
        .add(line -> line.contains(" **/")
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
                && line.matches("^ *\\* [^<@].*")
                && isJava(line.filename()),
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

  /** Returns whether we are currently in a region where lint rules should not
   * be applied. */
  private static boolean skipping(Puffin.Line<GlobalState, FileState> line) {
    return line.state().skipToLine >= 0
        && line.fnr() < line.state().skipToLine;
  }

  /** Returns whether we are in a file that contains Java code. */
  private static boolean isJava(String filename) {
    return filename.endsWith(".java")
        || filename.endsWith(".jj")
        || filename.endsWith(".fmpp")
        || filename.endsWith(".ftl")
        || filename.equals("GuavaCharSource{memory}"); // for testing
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
        + "  for (Map.Entry<String, Integer> e: entries) {\n"
        + "    //comment without space\n"
        + "  }\n"
        + "  for (int i :tooFewSpacesAfter) {\n"
        + "  }\n"
        + "  for (int i  : tooManySpacesBefore) {\n"
        + "  }\n"
        + "  for (int i :   tooManySpacesAfter) {\n"
        + "  }\n"
        + "  for (int i : justRight) {\n"
        + "  }\n"
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
        + "':' must be surrounded by ' '\n"
        + "GuavaCharSource{memory}:15:"
        + "'//' must be followed by ' '\n"
        + "GuavaCharSource{memory}:17:"
        + "':' must be surrounded by ' '\n"
        + "GuavaCharSource{memory}:19:"
        + "':' must be surrounded by ' '\n"
        + "GuavaCharSource{memory}:21:"
        + "':' must be surrounded by ' '\n"
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
    final List<File> files = TestUnsafe.getTextFiles();

    final GlobalState g;
    try (PrintWriter pw = Util.printWriter(System.out)) {
      g = program.execute(files.parallelStream().map(Sources::of), pw);
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
    assertThat(f.apply("[MINOR] Lint", ""),
        hasItem("starts with '[', and is not '[CALCITE-nnnn]'"));

    // If 'Lint:skip' occurs in the body, no checks are performed
    assertThat(
        f.apply("[CALCITE-4817] cannot start with lower-case",
            "Body line 1\n"
                + "\n"
                + "Lint:skip"),
        empty());
  }

  static {
    TERMINOLOGY_MAP.put("mysql", "MySQL");
    TERMINOLOGY_MAP.put("mssql", "MSSQL");
    TERMINOLOGY_MAP.put("Mysql", "MySQL");
    TERMINOLOGY_MAP.put("postgresql", "PostgreSQL");
    TERMINOLOGY_MAP.put("hive", "Hive");
    TERMINOLOGY_MAP.put("spark", "Spark");
    TERMINOLOGY_MAP.put("arrow", "Arrow");
    TERMINOLOGY_MAP.put("presto", "Presto");
    TERMINOLOGY_MAP.put("oracle", "Oracle");
    TERMINOLOGY_MAP.put("bigquery", "BigQuery");
    TERMINOLOGY_MAP.put("redshift", "Redshift");
    TERMINOLOGY_MAP.put("snowflake", "Snowflake");
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
    if (subject2.startsWith("[")) {
      consumer.accept("starts with '[', and is not '[CALCITE-nnnn]'");
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

    // Check for keywords that should be capitalized
    for (Map.Entry<String, String> entry : TERMINOLOGY_MAP.entrySet()) {
      String keyword = entry.getKey();
      String correctCapitalization = entry.getValue();
      if (subject2.matches(".*\\b" + keyword + "\\b.*")) {
        consumer.accept("Message must be capitalized as '" + correctCapitalization + "'");
      }
    }
  }

  /** Ensures that the {@code contributors.yml} file is sorted by name. */
  @Test void testContributorsFileIsSorted() throws IOException {
    final ObjectMapper mapper = new YAMLMapper();
    final File contributorsFile = ROOT_PATH.resolve("site/_data/contributors.yml").toFile();
    JavaType listType =
        mapper.getTypeFactory()
            .constructCollectionType(List.class, Contributor.class);
    List<Contributor> contributors =
        mapper.readValue(contributorsFile, listType);
    Contributor contributor =
        firstOutOfOrder(contributors,
            Comparator.comparing(c -> c.name, String.CASE_INSENSITIVE_ORDER));
    if (contributor != null) {
      fail("contributor '" + contributor.name + "' is out of order");
    }
  }

  /** Ensures that the {@code .mailmap} file is sorted. */
  @Test void testMailmapFile() {
    final File mailmapFile = ROOT_PATH.resolve(".mailmap").toFile();
    final List<String> lines = new ArrayList<>();
    forEachLineIn(mailmapFile, line -> {
      if (!line.startsWith("#")) {
        lines.add(line);
      }
    });
    String line = firstOutOfOrder(lines, String.CASE_INSENSITIVE_ORDER);
    if (line != null) {
      fail("line '" + line + "' is out of order");
    }
  }

  /** Performs an action for each line in a file. */
  private static void forEachLineIn(File file, Consumer<String> consumer) {
    try (BufferedReader r = Util.reader(file)) {
      for (;;) {
        String line = r.readLine();
        if (line == null) {
          break;
        }
        consumer.accept(line);
      }
    } catch (IOException e) {
      throw Util.throwAsRuntime(e);
    }
  }

  /** Returns the first element in a list that is out of order, or null if the
   * list is sorted. */
  private static <E> @Nullable E firstOutOfOrder(Iterable<E> elements,
      Comparator<E> comparator) {
    E previous = null;
    for (E e : elements) {
      if (previous != null && comparator.compare(previous, e) > 0) {
        return e;
      }
      previous = e;
    }
    return null;
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
    int skipToLine;
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

  /** Contributor element in "contributors.yaml" file. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  private static class Contributor {
    final String name;

    @JsonCreator Contributor(@JsonProperty("name") String name) {
      this.name = name;
    }
  }
}
