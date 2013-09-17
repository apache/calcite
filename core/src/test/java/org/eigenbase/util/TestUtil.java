/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.util;

import java.util.regex.*;

import org.junit.ComparisonFailure;


/**
 * Static utilities for JUnit tests.
 */
public abstract class TestUtil
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Pattern LineBreakPattern =
        Pattern.compile("\r\n|\r|\n");
    private static final Pattern TabPattern = Pattern.compile("\t");

    /**
     * System-dependent newline character.
     *
     * <p/>Do not use '\n' in strings which are samples for test results. {@link
     * java.io.PrintWriter#println()} produces '\n' on Unix and '\r\n' on
     * Windows, but '\n' is always '\n', so your tests will fail on Windows.
     */
    public static final String NL = Util.lineSeparator;

    private static final String lineBreak = "\" + NL +" + NL + "\"";

    private static final String lineBreak2 = "\\\\n\"" + NL + " + \"";

    private static final String lineBreak3 = "\\n\"" + NL + " + \"";

    //~ Methods ----------------------------------------------------------------

    public static void assertEqualsVerbose(
        String expected,
        String actual)
    {
        if (actual == null) {
            if (expected == null) {
                return;
            } else {
                String message =
                    "Expected:" + NL
                    + expected + NL
                    + "Actual: null";
                throw new ComparisonFailure(message, expected, actual);
            }
        }
        if ((expected != null) && expected.equals(actual)) {
            return;
        }
        String s = toJavaString(actual);

        String message =
            "Expected:" + NL + expected + NL
            + "Actual: " + NL + actual + NL
            + "Actual java: " + NL + s + NL;
        throw new ComparisonFailure(message, expected, actual);
    }

    /**
     * Converts a string (which may contain quotes and newlines) into a java
     * literal.
     *
     * <p>For example, <code>
     * <pre>string with "quotes" split
     * across lines</pre>
     * </code> becomes <code>
     * <pre>"string with \"quotes\" split" + NL +
     *  "across lines"</pre>
     * </code>
     */
    public static String quoteForJava(String s)
    {
        s = Util.replace(s, "\\", "\\\\");
        s = Util.replace(s, "\"", "\\\"");
        s = LineBreakPattern.matcher(s).replaceAll(lineBreak);
        s = TabPattern.matcher(s).replaceAll("\\\\t");
        s = "\"" + s + "\"";
        final String spurious = " + " + NL + "\"\"";
        if (s.endsWith(spurious)) {
            s = s.substring(0, s.length() - spurious.length());
        }
        return s;
    }

    /**
     * Converts a string (which may contain quotes and newlines) into a java
     * literal.
     *
     * <p>For example, <code>
     * <pre>string with "quotes" split
     * across lines</pre>
     * </code> becomes <code>
     * <pre>TestUtil.fold(
     *  "string with \"quotes\" split\n",
     *  + "across lines")</pre>
     * </code>
     */
    public static String toJavaString(String s)
    {
        // Convert [string with "quotes" split
        // across lines]
        // into [fold(
        // "string with \"quotes\" split\n"
        // + "across lines")]
        //
        s = Util.replace(s, "\"", "\\\"");
        s = LineBreakPattern.matcher(s).replaceAll(lineBreak2);
        s = TabPattern.matcher(s).replaceAll("\\\\t");
        s = "\"" + s + "\"";
        String spurious = NL + " \\+ \"\"";
        if (s.endsWith(spurious)) {
            s = s.substring(0, s.length() - spurious.length());
        }
        if (s.indexOf(lineBreak3) >= 0) {
            s = "TestUtil.fold(" + NL + s + ")";
        }
        return s;
    }

    /**
     * Combines an array of strings, each representing a line, into a single
     * string containing line separators.
     */
    public static String fold(String [] strings)
    {
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < strings.length; i++) {
            if (i > 0) {
                buf.append(NL);
            }
            String string = strings[i];
            buf.append(string);
        }
        return buf.toString();
    }

    /**
     * Converts a string containing newlines (\n) into a string containing
     * os-dependent line endings.
     */

    public static String fold(String string)
    {
        if (!"\n".equals(NL)) {
            string = string.replaceAll("\n", NL);
        }
        return string;
    }

    /**
     * Quotes a pattern.
     */
    public static String quotePattern(String s)
    {
        return s.replaceAll("\\\\", "\\\\")
            .replaceAll("\\.", "\\\\.")
            .replaceAll("\\+", "\\\\+")
            .replaceAll("\\{", "\\\\{")
            .replaceAll("\\}", "\\\\}")
            .replaceAll("\\|", "\\\\||")
            .replaceAll("[$]", "\\\\\\$")
            .replaceAll("\\?", "\\\\?")
            .replaceAll("\\*", "\\\\*")
            .replaceAll("\\(", "\\\\(")
            .replaceAll("\\)", "\\\\)")
            .replaceAll("\\[", "\\\\[")
            .replaceAll("\\]", "\\\\]");
    }
}

// End TestUtil.java
