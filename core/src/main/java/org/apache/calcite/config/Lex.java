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
package org.apache.calcite.config;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;

/** Named, built-in lexical policy. A lexical policy describes how
 * identifiers are quoted, whether they are converted to upper- or
 * lower-case when they are read, and whether they are matched
 * case-sensitively. */
public enum Lex {
  /** Lexical policy similar to Oracle. The case of identifiers enclosed in
   * double-quotes is preserved; unquoted identifiers are converted to
   * upper-case; after which, identifiers are matched case-sensitively. */
  ORACLE(Quoting.DOUBLE_QUOTE, Casing.TO_UPPER, Casing.UNCHANGED, true),

  /** Lexical policy similar to MySQL. (To be precise: MySQL on Windows;
   * MySQL on Linux uses case-sensitive matching, like the Linux file system.)
   * The case of identifiers is preserved whether or not they quoted;
   * after which, identifiers are matched case-insensitively.
   * Back-ticks allow identifiers to contain non-alphanumeric characters. */
  MYSQL(Quoting.BACK_TICK, Casing.UNCHANGED, Casing.UNCHANGED, false),

  /** Lexical policy similar to MySQL with ANSI_QUOTES option enabled. (To be
   * precise: MySQL on Windows; MySQL on Linux uses case-sensitive matching,
   * like the Linux file system.) The case of identifiers is preserved whether
   * or not they quoted; after which, identifiers are matched
   * case-insensitively. Double quotes allow identifiers to contain
   * non-alphanumeric characters. */
  MYSQL_ANSI(Quoting.DOUBLE_QUOTE, Casing.UNCHANGED, Casing.UNCHANGED, false),

  /** Lexical policy similar to Microsoft SQL Server.
   * The case of identifiers is preserved whether or not they are quoted;
   * after which, identifiers are matched case-insensitively.
   * Brackets allow identifiers to contain non-alphanumeric characters. */
  SQL_SERVER(Quoting.BRACKET, Casing.UNCHANGED, Casing.UNCHANGED, false),

  /** Lexical policy similar to Java.
   * The case of identifiers is preserved whether or not they are quoted;
   * after which, identifiers are matched case-sensitively.
   * Unlike Java, back-ticks allow identifiers to contain non-alphanumeric
   * characters. */
  JAVA(Quoting.BACK_TICK, Casing.UNCHANGED, Casing.UNCHANGED, true);

  public final Quoting quoting;
  public final Casing unquotedCasing;
  public final Casing quotedCasing;
  public final boolean caseSensitive;

  Lex(Quoting quoting,
      Casing unquotedCasing,
      Casing quotedCasing,
      boolean caseSensitive) {
    this.quoting = quoting;
    this.unquotedCasing = unquotedCasing;
    this.quotedCasing = quotedCasing;
    this.caseSensitive = caseSensitive;
  }
}

// End Lex.java
