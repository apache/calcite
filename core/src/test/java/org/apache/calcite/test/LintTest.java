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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
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
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;
import static java.util.regex.Pattern.compile;

/** Various automated checks on the code and git history. */
class LintTest {
  /** Pattern that matches "[CALCITE-12]" or "[CALCITE-1234]" followed by a
   * space. */
  private static final Pattern CALCITE_PATTERN =
      compile("^(\\[CALCITE-[0-9]{1,4}][ ]).*");
  /** Pattern that matches leading spaces and optional comment prefix "// ". */
  private static final Pattern LEADING_COMMENT_PATTERN =
      compile("^ *(// )?");
  private static final Path ROOT_PATH = Paths.get(System.getProperty("gradle.rootDir"));
  private static final String TERMINOLOGY_ERROR_MSG =
      "Message contains '%s' word; use one of the following instead: %s";
  private static final List<TermRule> TERM_RULES = initTerminologyRules();

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

        // Add line to sorting
        .add(line -> line.state().sortConsumer != null,
            line -> requireNonNull(line.state().sortConsumer).accept(line))

        // Start sorting if line has "lint: sort until ..."
        .add(line -> line.line().contains("lint: sort")
                && !line.source().fileOpt()
                    .filter(f -> f.getName().equals("LintTest.java")).isPresent(),
            line -> {
              line.state().sortConsumer = null;
              boolean continued = line.line().endsWith("\\");
              if (continued) {
                line.state().partialSort = "";
              } else {
                line.state().partialSort = null;
                final Sort sort = Sort.parse(line.line());
                if (sort != null) {
                  line.state().sortConsumer = new SortConsumer(sort);
                }
              }
            })

        // Start sorting if previous line had "lint: sort until ... \"
        .add(
            line -> line.state().partialSort != null,
            line -> {
              String thisLine = line.line();
              boolean continued = line.line().endsWith("\\");
              if (continued) {
                thisLine = skipLast(thisLine);
              }
              String nextLine;
              if (requireNonNull(line.state().partialSort).isEmpty()) {
                nextLine = thisLine;
              } else {
                thisLine = LEADING_COMMENT_PATTERN.matcher(thisLine).replaceAll("");
                nextLine = line.state().partialSort + thisLine;
              }
              if (continued) {
                line.state().partialSort = nextLine;
              } else {
                line.state().partialSort = null;
                Sort sort = Sort.parse(nextLine);
                if (sort != null) {
                  line.state().sortConsumer = new SortConsumer(sort);
                }
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

  private static String skipLast(String s) {
    return s.substring(0, s.length() - 1);
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

  private static List<TermRule> initTerminologyRules() {
    ImmutableList.Builder<TermRule> rules = ImmutableList.builder();
    rules.add(new TermRule("\\bmysql\\b", "MySQL"));
    rules.add(new TermRule("\\bmssql\\b", "MSSQL"));
    rules.add(new TermRule("\\bpostgresql\\b", "PostgreSQL"));
    rules.add(new TermRule("\\bhive\\b", "Hive"));
    rules.add(new TermRule("\\bspark\\b", "Spark"));
    rules.add(new TermRule("\\barrow\\b", "Arrow"));
    rules.add(new TermRule("\\bpresto\\b", "Presto"));
    rules.add(new TermRule("\\boracle\\b", "Oracle"));
    rules.add(new TermRule("\\bbigquery\\b", "BigQuery"));
    rules.add(new TermRule("\\bredshift\\b", "Redshift"));
    rules.add(new TermRule("\\bsnowflake\\b", "Snowflake"));
    rules.add(new TermRule("\\bsqlite\\b", "SQLite"));
    return rules.build();
  }

  /**
   * A rule for defining valid patterns for terms.
   */
  private static final class TermRule {
    private final Pattern termPattern;
    private final Set<String> validTerms;

    TermRule(String regex, String... validTerms) {
      this.termPattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
      this.validTerms = ImmutableSet.copyOf(validTerms);
    }

    /**
     * Checks whether the input satisfies the rule.
     * Returns an error message if the check fails and empty string if the input is valid.
     */
    String check(String input) {
      final Matcher m = termPattern.matcher(input);
      if (m.find() && !validTerms.contains(m.group(0))) {
        return String.format(Locale.ROOT, TERMINOLOGY_ERROR_MSG, m.group(0), validTerms);
      }
      return "";
    }
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
    if (subject2.matches("^Chore.*\\b")) {
      consumer.accept("Message cannot start with the Chore keyword");
    }

    // Check for keywords that should be capitalized
    for (TermRule tRule : TERM_RULES) {
      String error = tRule.check(subject2);
      if (!error.isEmpty()) {
        consumer.accept(error);
      }
    }
  }

  @Test void testCheckMessageWithInvalidDBMSTerms() {
    Set<String> invalidTerms = new HashSet<>();
    invalidTerms.add("mysql");
    invalidTerms.add("Mysql");
    invalidTerms.add("MYSQL");
    invalidTerms.add("postgresql");
    invalidTerms.add("POSTGRESQL");
    invalidTerms.add("Mssql");
    invalidTerms.add("RedShift");
    invalidTerms.add("SnowFlake");
    invalidTerms.add("hiVe");
    invalidTerms.add("HiVe");
    for (String iTerm : invalidTerms) {
      String msg = "Add support for " + iTerm + " dialect";
      List<String> errors = new ArrayList<>();
      checkMessage(msg, "", errors::add);
      assertThat("Failed to find error in:" + msg, errors, hasSize(1));
      assertThat(errors.get(0),
          startsWith(String.format(Locale.ROOT, TERMINOLOGY_ERROR_MSG, iTerm, "")));
    }
  }

  @Test void testCheckMessageWithValidDBMSTerms() {
    Set<String> validTerms = new HashSet<>();
    validTerms.add("MySQL");
    validTerms.add("PostgreSQL");
    validTerms.add("MSSQL");
    validTerms.add("Redshift");
    validTerms.add("Snowflake");
    validTerms.add("Hive");
    for (String vTerm : validTerms) {
      String msg = "Add support for " + vTerm + " dialect";
      List<String> errors = new ArrayList<>();
      checkMessage(msg, "", errors::add);
      assertThat(errors, empty());
    }
  }

  /** Tests the Sort specification syntax. */
  @Test void testSort() {
    // With "until" and "where"
    checkSortSpec(
        "class Test {\n"
            + "  switch (x) {\n"
            + "  // lint: sort until '#}' where '##case '\n"
            + "  case c\n"
            + "  case a\n"
            + "  case b\n"
            + "  }\n"
            + "}\n",
        "GuavaCharSource{memory}:5:"
            + "Lines must be sorted; '  case a' should be before '  case c'");

    // With "until" and "where"; cases after "until" should be ignored.
    checkSortSpec(
        "class Test {\n"
            + "  switch (x) {\n"
            + "  // lint: sort until '#}' where '##case '\n"
            + "  case x\n"
            + "  case y\n"
            + "  case z\n"
            + "  }\n"
            + "  switch (y) {\n"
            + "  case a\n"
            + "  }\n"
            + "}\n",
        "GuavaCharSource{memory}:9:"
            + "Lines must be sorted; '  case a' should be before '  case z'");

    // Change '##}' to '#}' to make the test pass.
    checkSortSpec(
        "class Test {\n"
            + "  switch (x) {\n"
            + "  // lint: sort until '##}' where '##case '\n"
            + "  case x\n"
            + "  case y\n"
            + "  case z\n"
            + "  }\n"
            + "  switch (y) {\n"
            + "  case a\n"
            + "  }\n"
            + "}\n");

    // Specification has "until", "where" and "erase" clauses.
    checkSortSpec(
        "class Test {\n"
            + "  // lint: sort until '#}' where '##A::' erase '^ .*::'\n"
            + "  A::c\n"
            + "  A::a\n"
            + "  A::b\n"
            + "  }\n"
            + "}\n",
        "GuavaCharSource{memory}:4:"
            + "Lines must be sorted; 'a' should be before 'c'");

    // Specification is spread over multiple lines.
    checkSortSpec(
        "class Test {\n"
            + "  // lint: sort until '#}'\\\n"
            + "  // where '##A::'\\\n"
            + "  // erase '^ .*::'\n"
            + "  A::c\n"
            + "  A::a\n"
            + "  A::b\n"
            + "  }\n"
            + "}\n",
        "GuavaCharSource{memory}:6:"
            + "Lines must be sorted; 'a' should be before 'c'");
  }

  private void checkSortSpec(String code, String... expectedMessages) {
    final Puffin.Program<GlobalState> program = makeProgram();
    final StringWriter sw = new StringWriter();
    final GlobalState g;
    try (PrintWriter pw = new PrintWriter(sw)) {
      g = program.execute(Stream.of(Sources.of(code)), pw);
    }
    assertThat(g.messages, hasToString(Arrays.toString(expectedMessages)));
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
    @Nullable String partialSort;
    @Nullable Consumer<Puffin.Line<GlobalState, FileState>> sortConsumer;

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

  /**
   * Specification for a sort directive.
   *
   * <p>The directive has the form:
   * <blockquote>
   * {@code // lint: sort until 'END_PATTERN' [where 'FILTER_PATTERN'] [erase 'ERASE_PATTERN']}
   * </blockquote>
   * where the comment prefix {@code //} may also be {@code #} (for YAML files).
   *
   * <p>Parameters:
   * <ul>
   *   <li>{@code until 'END_PATTERN'} - Required. Regular expression marking
   *       the end of the sorted section. Supports '#' placeholder (parent indent, -2 spaces)
   *       and '##' (current line indent).
   *   <li>{@code where 'FILTER_PATTERN'} - Optional. Regular expression to
   *       select lines for sorting. Only matching lines are checked.
   *       Supports '#' and '##' placeholders.
   *   <li>{@code erase 'ERASE_PATTERN'} - Optional. Optional. Regular expression
   *       to remove from lines before comparing. Useful for ignoring type annotations,
   *       modifiers, etc.
   * </ul>
   *
   * <p>Indentation placeholders in patterns:
   * <ul>
   *   <li>{@code ##} - Current line's indentation level
   *   <li>{@code #} - Parent indentation level (current indent minus 2 spaces)
   * </ul>
   *
   * <p>Long directives can span multiple lines by ending each continuation
   * line with {@code \}.
   *
   * <p>Sorting is case-sensitive.
   */
  private static class Sort {
    final Pattern until;
    final @Nullable Pattern where;
    final @Nullable Pattern erase;

    Sort(Pattern until, @Nullable Pattern where, @Nullable Pattern erase) {
      this.until = until;
      this.where = where;
      this.erase = erase;
    }

    /** Parses a sort directive from a line like
     * {@code // lint: sort until 'X' where 'Y' erase 'Z'}.
     * Returns null if the line does not contain a valid directive. */
    static @Nullable Sort parse(String line) {
      final int sortIndex = line.indexOf("lint: sort");
      if (sortIndex < 0) {
        return null;
      }
      final String spec =
          line.substring(sortIndex + "lint: sort".length()).trim();

      // Count leading spaces for indentation placeholder expansion
      int indent = 0;
      while (indent < line.length() && line.charAt(indent) == ' ') {
        indent++;
      }

      final Pattern until = extractPattern(spec, "until", indent);
      if (until == null) {
        return null;
      }
      final Pattern where = extractPattern(spec, "where", indent);
      final Pattern erase = extractPattern(spec, "erase", indent);
      return new Sort(until, where, erase);
    }

    private static @Nullable Pattern extractPattern(
        String spec, String keyword, int indent) {
      final int keywordIndex = spec.indexOf(keyword);
      if (keywordIndex < 0) {
        return null;
      }
      final String rest = spec.substring(keywordIndex + keyword.length()).trim();
      if (rest.isEmpty() || rest.charAt(0) != '\'') {
        return null;
      }
      final int endQuote = rest.indexOf('\'', 1);
      if (endQuote < 0) {
        return null;
      }
      String pattern = rest.substring(1, endQuote);
      // Replace indentation placeholders
      final String indentStr = nSpaces(indent);
      final String parentStr = nSpaces(Math.max(0, indent - 2));
      pattern = pattern.replace("##", "^" + indentStr);
      pattern = pattern.replace("#", "^" + parentStr);
      try {
        return compile(pattern);
      } catch (Exception e) {
        return null;
      }
    }

    private static String nSpaces(int n) {
      final StringBuilder sb = new StringBuilder();
      for (int i = 0; i < n; i++) {
        sb.append(' ');
      }
      return sb.toString();
    }
  }

  /** Consumes lines within a sort region and reports violations. */
  private static class SortConsumer
      implements Consumer<Puffin.Line<GlobalState, FileState>> {
    final Sort sort;
    final List<String> lines = new ArrayList<>();

    SortConsumer(Sort sort) {
      this.sort = sort;
    }

    @Override public void accept(Puffin.Line<GlobalState, FileState> line) {
      final String thisLine = line.line();

      // Check if we've reached the end marker
      if (sort.until.matcher(thisLine).find()) {
        line.state().sortConsumer = null;
        return;
      }

      // If a where clause exists, skip non-matching lines;
      // if the pattern has a capturing group, use group(1) as the sort key.
      String compareKey = thisLine;
      if (sort.where != null) {
        final Matcher m = sort.where.matcher(thisLine);
        if (!m.find()) {
          return;
        }
        if (m.groupCount() >= 1) {
          compareKey = m.group(1);
        }
      }

      // Apply the erase pattern to derive the sort key
      if (sort.erase != null) {
        compareKey = sort.erase.matcher(compareKey).replaceAll("");
      }

      if (!lines.isEmpty()) {
        final String prevKey = lines.get(lines.size() - 1);
        if (compareKey.compareTo(prevKey) < 0) {
          line.state().message(
              String.format(Locale.ROOT,
                  "Lines must be sorted; '%s' should be before '%s'",
                  compareKey, prevKey),
              line);
        }
      }
      lines.add(compareKey);
    }
  }
}
