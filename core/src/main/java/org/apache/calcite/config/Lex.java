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

import com.google.common.collect.ImmutableSet;

import java.util.Objects;
import java.util.Set;

/**
 * Named, built-in lexical policy.
 *
 * A lexical policy describes how identifiers are quoted(引用),
 * whether they are converted to upper- or lower-case when they are read,
 * and whether they are matched case-sensitively.
 *
 * fixme
 *    命名的、内置的词法策略。
 *    一个词法策略描述了识别符如何别引用；识别符被读取的时候、是否被转换成大写或者小写；他们是否是大小写敏感的。
 */
public enum Lex {

  /**
   * Lexical policy similar to BigQuery.
   *
   * The case of identifiers is preserved whether or not they quoted;
   * after which, identifiers are matched case-insensitively.
   * Back-ticks allow identifiers to contain non-alphanumeric characters.
   * Character literals may be enclosed in single or double quotes.
   */
  BIG_QUERY(Quoting.BACK_TICK, Casing.UNCHANGED, Casing.UNCHANGED, true,
      CharLiteralStyle.BQ_SINGLE, CharLiteralStyle.BQ_DOUBLE),

  /**
   * Lexical policy similar to Oracle.
   * The case of identifiers enclosed in
   * double-quotes is preserved; unquoted identifiers are converted to
   * upper-case; after which, identifiers are matched case-sensitively.
   */
  ORACLE(Quoting.DOUBLE_QUOTE, Casing.TO_UPPER, Casing.UNCHANGED, true,
      CharLiteralStyle.STANDARD),

  /**
   * Lexical policy similar to MySQL. (To be precise: MySQL on Windows;
   * MySQL on Linux uses case-sensitive matching, like the Linux file system.)
   * The case of identifiers is preserved whether or not they quoted;
   * after which, identifiers are matched case-insensitively.
   * Back-ticks allow identifiers to contain non-alphanumeric characters.
   */
  MYSQL(Quoting.BACK_TICK, Casing.UNCHANGED, Casing.UNCHANGED, false,
      CharLiteralStyle.STANDARD),

  /** Lexical policy similar to MySQL with ANSI_QUOTES option enabled. (To be
   * precise: MySQL on Windows; MySQL on Linux uses case-sensitive matching,
   * like the Linux file system.) The case of identifiers is preserved whether
   * or not they quoted; after which, identifiers are matched
   * case-insensitively. Double quotes allow identifiers to contain
   * non-alphanumeric characters. */
  MYSQL_ANSI(Quoting.DOUBLE_QUOTE, Casing.UNCHANGED, Casing.UNCHANGED, false,
      CharLiteralStyle.STANDARD),

  /** Lexical policy similar to Microsoft SQL Server.
   * The case of identifiers is preserved whether or not they are quoted;
   * after which, identifiers are matched case-insensitively.
   * Brackets allow identifiers to contain non-alphanumeric characters. */
  SQL_SERVER(Quoting.BRACKET, Casing.UNCHANGED, Casing.UNCHANGED, false,
      CharLiteralStyle.STANDARD),

  /** Lexical policy similar to Java.
   * The case of identifiers is preserved whether or not they are quoted;
   * after which, identifiers are matched case-sensitively.
   * Unlike Java, back-ticks allow identifiers to contain non-alphanumeric
   * characters. */
  JAVA(Quoting.BACK_TICK, Casing.UNCHANGED, Casing.UNCHANGED, true,
      CharLiteralStyle.STANDARD);

  public final Quoting quoting;
  public final Casing unquotedCasing;
  public final Casing quotedCasing;
  public final boolean caseSensitive;
  @SuppressWarnings("ImmutableEnumChecker")
  public final Set<CharLiteralStyle> charLiteralStyles;

  Lex(Quoting quoting,
      Casing unquotedCasing,
      Casing quotedCasing,
      boolean caseSensitive,
      CharLiteralStyle... charLiteralStyles) {
    this.quoting = Objects.requireNonNull(quoting);
    this.unquotedCasing = Objects.requireNonNull(unquotedCasing);
    this.quotedCasing = Objects.requireNonNull(quotedCasing);
    this.caseSensitive = caseSensitive;
    this.charLiteralStyles = ImmutableSet.copyOf(charLiteralStyles);
  }
}
