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
package org.apache.calcite.sql.util;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.util.UnmodifiableArrayList;

import java.sql.Timestamp;
import java.util.List;

/**
 * Extension to {@link StringBuilder} for the purposes of creating SQL queries
 * and expressions.
 *
 * <p>Using this class helps to prevent SQL injection attacks, incorrectly
 * quoted identifiers and strings. These problems occur when you build SQL by
 * concatenating strings, and you forget to treat identifers and string literals
 * correctly. SqlBuilder has special methods for appending identifiers and
 * literals.
 */
public class SqlBuilder {
  private final StringBuilder buf;
  private final SqlDialect dialect;

  /**
   * Creates a SqlBuilder.
   *
   * @param dialect Dialect
   */
  public SqlBuilder(SqlDialect dialect) {
    assert dialect != null;
    this.dialect = dialect;
    this.buf = new StringBuilder();
  }

  /**
   * Creates a SqlBuilder with a given string.
   *
   * @param dialect Dialect
   * @param s       Initial contents of the buffer
   */
  public SqlBuilder(SqlDialect dialect, String s) {
    assert dialect != null;
    this.dialect = dialect;
    this.buf = new StringBuilder(s);
  }

  /**
   * Returns the dialect.
   *
   * @return dialect
   */
  public SqlDialect getDialect() {
    return dialect;
  }

  /**
   * Returns the length (character count).
   *
   * @return the length of the sequence of characters currently
   * represented by this object
   */
  public int length() {
    return buf.length();
  }

  /**
   * Clears the contents of the buffer.
   */
  public void clear() {
    buf.setLength(0);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Returns the SQL string.
   *
   * @return SQL string
   * @see #getSql()
   */
  @Override public String toString() {
    return getSql();
  }

  /**
   * Returns the SQL.
   */
  public String getSql() {
    return buf.toString();
  }

  /**
   * Returns the SQL and clears the buffer.
   *
   * <p>Convenient if you are reusing the same SQL builder in a loop.
   */
  public String getSqlAndClear() {
    final String str = buf.toString();
    clear();
    return str;
  }

  /**
   * Appends a hygienic SQL string.
   *
   * @param s SQL string to append
   * @return This builder
   */
  public SqlBuilder append(SqlString s) {
    buf.append(s.getSql());
    return this;
  }

  /**
   * Appends a string, without any quoting.
   *
   * <p>Calls to this method are dubious.
   *
   * @param s String to append
   * @return This builder
   */
  public SqlBuilder append(String s) {
    buf.append(s);
    return this;
  }

  /**
   * Appends a character, without any quoting.
   *
   * @param c Character to append
   * @return This builder
   */
  public SqlBuilder append(char c) {
    buf.append(c);
    return this;
  }

  /**
   * Appends a number, per {@link StringBuilder#append(long)}.
   */
  public SqlBuilder append(long n) {
    buf.append(n);
    return this;
  }

  /**
   * Appends an identifier to this buffer, quoting accordingly.
   *
   * @param name Identifier
   * @return This builder
   */
  public SqlBuilder identifier(String name) {
    dialect.quoteIdentifier(buf, name);
    return this;
  }

  /**
   * Appends one or more identifiers to this buffer, quoting accordingly.
   *
   * @param names Varargs array of identifiers
   * @return This builder
   */
  public SqlBuilder identifier(String... names) {
    dialect.quoteIdentifier(buf, UnmodifiableArrayList.of(names));
    return this;
  }

  /**
   * Appends a compound identifier to this buffer, quoting accordingly.
   *
   * @param names Parts of a compound identifier
   * @return This builder
   */
  public SqlBuilder identifier(List<String> names) {
    dialect.quoteIdentifier(buf, names);
    return this;
  }

  /**
   * Returns the contents of this SQL buffer as a 'certified kocher' SQL
   * string.
   *
   * <p>Use this method in preference to {@link #toString()}. It indicates
   * that the SQL string has been constructed using good hygiene, and is
   * therefore less likely to contain SQL injection or badly quoted
   * identifiers or strings.
   *
   * @return Contents of this builder as a SQL string.
   */
  public SqlString toSqlString() {
    return new SqlString(dialect, buf.toString());
  }

  /**
   * Appends a string literal to this buffer.
   *
   * <p>For example, calling <code>literal(&quot;can't&quot;)</code>
   * would convert the buffer
   * <blockquote><code>SELECT </code></blockquote>
   * to
   * <blockquote><code>SELECT 'can''t'</code></blockquote>
   *
   * @param s String to append
   * @return This buffer
   */
  public SqlBuilder literal(String s) {
    buf.append(
        s == null
            ? "null"
            : dialect.quoteStringLiteral(s));
    return this;
  }

  /**
   * Appends a timestamp literal to this buffer.
   *
   * @param timestamp Timestamp to append
   * @return This buffer
   */
  public SqlBuilder literal(Timestamp timestamp) {
    buf.append(
        timestamp == null
            ? "null"
            : dialect.quoteTimestampLiteral(timestamp));
    return this;
  }

  /**
   * Returns the index within this string of the first occurrence of the
   * specified substring.
   *
   * @see StringBuilder#indexOf(String)
   */
  public int indexOf(String str) {
    return buf.indexOf(str);
  }

  /**
   * Returns the index within this string of the first occurrence of the
   * specified substring, starting at the specified index.
   *
   * @see StringBuilder#indexOf(String, int)
   */
  public int indexOf(String str, int fromIndex) {
    return buf.indexOf(str, fromIndex);
  }

  /**
   * Inserts the string into this character sequence.
   *
   * @see StringBuilder#insert(int, String)
   */
  public SqlBuilder insert(int offset, String str) {
    buf.insert(offset, str);
    return this;
  }
}

// End SqlBuilder.java
