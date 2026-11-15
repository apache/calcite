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
package org.apache.calcite.adapter.pig;

import java.util.regex.Pattern;

/**
 * Utility methods for generating Pig Latin scripts.
 *
 * <p>Unlike SQL, Pig Latin has no mechanism to quote identifiers, so names
 * that do not match its identifier syntax cannot be represented in a script
 * and must be rejected; see
 * <a href="https://pig.apache.org/docs/r0.18.0/basic.html#identifiers">
 * Pig Latin identifiers</a>.
 */
public final class PigUtils {

  /** Private constructor to prevent instantiation. */
  private PigUtils() {
  }

  /**
   * Matches a valid Pig Latin identifier: an ASCII letter followed by any
   * number of letters, digits or underscores.
   */
  private static final Pattern IDENTIFIER =
      Pattern.compile("[A-Za-z][A-Za-z0-9_]*");

  /**
   * Validates that the given name is a valid Pig Latin identifier, throwing
   * if it is not.
   *
   * <p>Identifiers are used in generated scripts as relation aliases, field
   * names and {@code AS} aliases, where they cannot be quoted or escaped;
   * an invalid name would produce a script that Pig cannot parse.
   *
   * @param name Name to validate
   * @throws IllegalArgumentException if {@code name} is not a valid Pig Latin
   *                                   identifier
   */
  public static void checkValidIdentifier(String name) {
    if (!IDENTIFIER.matcher(name).matches()) {
      throw new IllegalArgumentException(
          "Invalid Pig Latin identifier: '" + name + "'. Pig Latin identifiers"
              + " must start with a letter, followed by letters, digits or"
              + " underscores");
    }
  }

  /**
   * Escapes the given string for use inside a Pig Latin string literal.
   *
   * <p>Backslashes and single quotes are escaped with a backslash, so that
   * the value cannot break out of the {@code '...'} literal or be re-interpreted
   * as a Pig Latin escape sequence such as {@code \t} or {@code \u0001}.
   *
   * @param s String to escape
   * @return the escaped string
   */
  public static String escapeStringLiteral(String s) {
    return s.replace("\\", "\\\\").replace("'", "\\'");
  }

  /**
   * Converts the given string to a Pig Latin string literal, escaping it
   * first and then wrapping it in single quotes.
   *
   * @param s String to quote
   * @return the quoted and escaped string literal
   */
  public static String quoteStringLiteral(String s) {
    return "'" + escapeStringLiteral(s) + "'";
  }
}
