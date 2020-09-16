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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;

import org.junit.jupiter.api.Assertions;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Static utilities for JUnit tests.
 */
public abstract class TestUtil {
  //~ Static fields/initializers ---------------------------------------------

  private static final Pattern LINE_BREAK_PATTERN =
      Pattern.compile("\r\n|\r|\n");

  private static final Pattern TAB_PATTERN = Pattern.compile("\t");

  private static final String LINE_BREAK =
      "\\\\n\"" + Util.LINE_SEPARATOR + " + \"";

  private static final String JAVA_VERSION =
      System.getProperties().getProperty("java.version");

  private static final Supplier<Integer> GUAVA_MAJOR_VERSION =
      Suppliers.memoize(TestUtil::computeGuavaMajorVersion)::get;

  /** This is to be used by {@link #rethrow(Throwable, String)} to add extra information via
   * {@link Throwable#addSuppressed(Throwable)}. */
  private static class ExtraInformation extends Throwable {
    ExtraInformation(String message) {
      super(message);
    }
  }

  //~ Methods ----------------------------------------------------------------

  public static void assertEqualsVerbose(
      String expected,
      String actual) {
    Assertions.assertEquals(expected, actual,
        () -> "Expected:\n"
            + expected
            + "\nActual:\n"
            + actual
            + "\nActual java:\n"
            + toJavaString(actual) + '\n');
  }

  /**
   * Converts a string (which may contain quotes and newlines) into a java
   * literal.
   *
   * <p>For example,
   * <pre><code>string with "quotes" split
   * across lines</code></pre>
   *
   * <p>becomes
   *
   * <blockquote><pre><code>"string with \"quotes\" split" + NL +
   *  "across lines"</code></pre></blockquote>
   */
  public static String quoteForJava(String s) {
    s = Util.replace(s, "\\", "\\\\");
    s = Util.replace(s, "\"", "\\\"");
    s = LINE_BREAK_PATTERN.matcher(s).replaceAll(LINE_BREAK);
    s = TAB_PATTERN.matcher(s).replaceAll("\\\\t");
    s = "\"" + s + "\"";
    final String spurious = " + \n\"\"";
    if (s.endsWith(spurious)) {
      s = s.substring(0, s.length() - spurious.length());
    }
    return s;
  }

  /**
   * Converts a string (which may contain quotes and newlines) into a java
   * literal.
   *
   * <p>For example,</p>
   *
   * <blockquote><pre><code>string with "quotes" split
   * across lines</code></pre></blockquote>
   *
   * <p>becomes</p>
   *
   * <blockquote><pre><code>TestUtil.fold(
   *  "string with \"quotes\" split\n",
   *  + "across lines")</code></pre></blockquote>
   */
  public static String toJavaString(String s) {
    // Convert [string with "quotes" split
    // across lines]
    // into [fold(
    // "string with \"quotes\" split\n"
    // + "across lines")]
    //
    s = Util.replace(s, "\"", "\\\"");
    s = LINE_BREAK_PATTERN.matcher(s).replaceAll(LINE_BREAK);
    s = TAB_PATTERN.matcher(s).replaceAll("\\\\t");
    s = "\"" + s + "\"";
    String spurious = "\n \\+ \"\"";
    if (s.endsWith(spurious)) {
      s = s.substring(0, s.length() - spurious.length());
    }
    return s;
  }

  /**
   * Combines an array of strings, each representing a line, into a single
   * string containing line separators.
   */
  public static String fold(String... strings) {
    StringBuilder buf = new StringBuilder();
    for (String string : strings) {
      buf.append(string);
      buf.append('\n');
    }
    return buf.toString();
  }

  /** Quotes a string for Java or JSON. */
  public static String escapeString(String s) {
    return escapeString(new StringBuilder(), s).toString();
  }

  /** Quotes a string for Java or JSON, into a builder. */
  public static StringBuilder escapeString(StringBuilder buf, String s) {
    buf.append('"');
    int n = s.length();
    char lastChar = 0;
    for (int i = 0; i < n; ++i) {
      char c = s.charAt(i);
      switch (c) {
      case '\\':
        buf.append("\\\\");
        break;
      case '"':
        buf.append("\\\"");
        break;
      case '\n':
        buf.append("\\n");
        break;
      case '\r':
        if (lastChar != '\n') {
          buf.append("\\r");
        }
        break;
      default:
        buf.append(c);
        break;
      }
      lastChar = c;
    }
    return buf.append('"');
  }

  /**
   * Quotes a pattern.
   */
  public static String quotePattern(String s) {
    return s.replace("\\", "\\\\")
        .replace(".", "\\.")
        .replace("+", "\\+")
        .replace("{", "\\{")
        .replace("}", "\\}")
        .replace("|", "\\||")
        .replace("$", "\\$")
        .replace("?", "\\?")
        .replace("*", "\\*")
        .replace("(", "\\(")
        .replace(")", "\\)")
        .replace("[", "\\[")
        .replace("]", "\\]");
  }

  /**
   * Returns the Java major version: 7 for JDK 1.7, 8 for JDK 8, 10 for
   * JDK 10, etc. depending on current system property {@code java.version}.
   */
  public static int getJavaMajorVersion() {
    return majorVersionFromString(JAVA_VERSION);
  }

  /**
   * Detects java major version given long format of full JDK version.
   * See <a href="http://openjdk.java.net/jeps/223">JEP 223: New Version-String Scheme</a>.
   *
   * @param version current version as string usually from {@code java.version} property.
   * @return major java version ({@code 8, 9, 10, 11} etc.)
   */
  @VisibleForTesting
  static int majorVersionFromString(String version) {
    Objects.requireNonNull(version, "version");

    if (version.startsWith("1.")) {
      // running on version <= 8 (expecting string of type: x.y.z*)
      final String[] versions = version.split("\\.");
      return Integer.parseInt(versions[1]);
    }
    // probably running on > 8 (just get first integer which is major version)
    Matcher matcher = Pattern.compile("^\\d+").matcher(version);
    if (!matcher.lookingAt()) {
      throw new IllegalArgumentException("Can't parse (detect) JDK version from " + version);
    }

    return Integer.parseInt(matcher.group());
  }

  /** Returns the Guava major version. */
  public static int getGuavaMajorVersion() {
    return GUAVA_MAJOR_VERSION.get();
  }

  /** Computes the Guava major version. */
  private static int computeGuavaMajorVersion() {
    // A list of classes and the Guava version that they were introduced.
    // The list should not contain any classes that are removed in future
    // versions of Guava.
    return new VersionChecker()
        .tryClass(2, "com.google.common.collect.ImmutableList")
        .tryClass(14, "com.google.common.reflect.Parameter")
        .tryClass(17, "com.google.common.base.VerifyException")
        .tryClass(21, "com.google.common.io.RecursiveDeleteOption")
        .tryClass(23, "com.google.common.util.concurrent.FluentFuture")
        .tryClass(26, "com.google.common.util.concurrent.ExecutionSequencer")
        .bestVersion;
  }

  /** Checks if exceptions have give substring. That is handy to prevent logging SQL text twice */
  public static boolean hasMessage(Throwable t, String substring) {
    while (t != null) {
      String message = t.getMessage();
      if (message != null && message.contains(substring)) {
        return true;
      }
      t = t.getCause();
    }
    return false;
  }

  /** Rethrows given exception keeping stacktraces clean and compact. */
  public static <E extends Throwable> RuntimeException rethrow(Throwable e) throws E {
    if (e instanceof InvocationTargetException) {
      e = e.getCause();
    }
    throw (E) e;
  }

  /** Rethrows given exception keeping stacktraces clean and compact. */
  public static <E extends Throwable> RuntimeException rethrow(Throwable e,
      String message) throws E {
    e.addSuppressed(new ExtraInformation(message));
    throw (E) e;
  }

  /** Returns string representation of the given {@link Throwable}. */
  public static String printStackTrace(Throwable t) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    t.printStackTrace(pw);
    pw.flush();
    return sw.toString();
  }

  /** Checks whether a given class exists, and updates a version if it does. */
  private static class VersionChecker {
    int bestVersion = -1;

    VersionChecker tryClass(int version, String className) {
      try {
        Class.forName(className);
        bestVersion = Math.max(version, bestVersion);
      } catch (ClassNotFoundException e) {
        // ignore
      }
      return this;
    }
  }
}
