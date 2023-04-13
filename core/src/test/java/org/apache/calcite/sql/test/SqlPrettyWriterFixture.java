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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.test.DiffRepository;
import org.apache.calcite.util.Litmus;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Objects;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertTrue;

import static java.util.Objects.requireNonNull;

/**
 * A fixture for testing the SQL pretty writer.
 *
 * <p>It provides a fluent API so that you can write tests by chaining method
 * calls.
 *
 * <p>It is immutable. If you have two test cases that require a similar set up,
 * it is safe to use the same fixture object as a starting point for both tests.
 *
 * @see org.apache.calcite.sql.pretty.SqlPrettyWriter
 */
class SqlPrettyWriterFixture {
  private final @Nullable DiffRepository diffRepos;
  public final String sql;
  public final boolean expression;
  public final @Nullable String desc;
  public final String formatted;
  public final UnaryOperator<SqlWriterConfig> transform;

  SqlPrettyWriterFixture(@Nullable DiffRepository diffRepos, String sql,
      boolean expression, @Nullable String desc, String formatted,
      UnaryOperator<SqlWriterConfig> transform) {
    this.diffRepos = diffRepos;
    this.sql = requireNonNull(sql, "sql");
    this.expression = expression;
    this.desc = desc;
    this.formatted = requireNonNull(formatted, "formatted");
    this.transform = requireNonNull(transform, "transform");
  }

  SqlPrettyWriterFixture withWriter(
      UnaryOperator<SqlWriterConfig> transform) {
    requireNonNull(transform, "transform");
    final UnaryOperator<SqlWriterConfig> transform1 =
        this.transform.andThen(transform)::apply;
    return new SqlPrettyWriterFixture(diffRepos, sql, expression, desc,
        formatted, transform1);
  }

  SqlPrettyWriterFixture withSql(String sql) {
    if (sql.equals(this.sql)) {
      return this;
    }
    return new SqlPrettyWriterFixture(diffRepos, sql, expression, desc,
        formatted, transform);
  }

  SqlPrettyWriterFixture withExpr(boolean expression) {
    if (this.expression == expression) {
      return this;
    }
    return new SqlPrettyWriterFixture(diffRepos, sql, expression, desc,
        formatted, transform);
  }

  SqlPrettyWriterFixture withDiffRepos(DiffRepository diffRepos) {
    if (Objects.equals(this.diffRepos, diffRepos)) {
      return this;
    }
    return new SqlPrettyWriterFixture(diffRepos, sql, expression, desc,
        formatted, transform);
  }

  /** Returns the diff repository, checking that it is not null.
   * (It is allowed to be null because some tests that don't use a diff
   * repository.) */
  public DiffRepository diffRepos() {
    return DiffRepository.castNonNull(diffRepos);
  }

  SqlPrettyWriterFixture expectingDesc(@Nullable String desc) {
    if (Objects.equals(this.desc, desc)) {
      return this;
    }
    return new SqlPrettyWriterFixture(diffRepos, sql, expression, desc,
        formatted, transform);
  }

  SqlPrettyWriterFixture expectingFormatted(String formatted) {
    if (Objects.equals(this.formatted, formatted)) {
      return this;
    }
    return new SqlPrettyWriterFixture(diffRepos, sql, expression, desc,
        formatted, transform);
  }

  /** Parses a SQL query. To use a different parser, override this method. */
  protected SqlNode parseQuery(String sql) {
    SqlNode node;
    try {
      node = SqlParser.create(sql).parseQuery();
    } catch (SqlParseException e) {
      String message = "Received error while parsing SQL '" + sql + "'"
          + "; error is:\n"
          + e.toString();
      throw new AssertionError(message);
    }
    return node;
  }

  SqlPrettyWriterFixture check() {
    return checkTransformedNode(n -> n);
  }

  /** As {@link #check()}, but operates on a transformed node
   * (say the 2nd child of the 1st child) rather than the root. */
  SqlPrettyWriterFixture checkTransformedNode(
      UnaryOperator<SqlNode> nodeTransformer) {
    return check_(nodeTransformer);
  }

  private SqlPrettyWriterFixture check_(UnaryOperator<SqlNode> nodeTransformer) {
    final SqlWriterConfig config =
        transform.apply(SqlPrettyWriter.config()
            .withDialect(AnsiSqlDialect.DEFAULT));
    final SqlPrettyWriter prettyWriter = new SqlPrettyWriter(config);
    final SqlNode node;
    final SqlNode node1;
    if (expression) {
      final SqlCall valuesCall = (SqlCall) parseQuery("VALUES (" + sql + ")");
      final SqlCall rowCall = valuesCall.operand(0);
      node = rowCall.operand(0);
      node1 = node;
    } else {
      node = parseQuery(sql);
      node1 = nodeTransformer.apply(node);
    }

    // Describe settings
    if (desc != null) {
      final StringWriter sw = new StringWriter();
      final PrintWriter pw = new PrintWriter(sw);
      prettyWriter.describe(pw, true);
      pw.flush();
      final String desc = sw.toString();
      diffRepos().assertEquals("desc", this.desc, desc);
    }

    // Format
    final String formatted = prettyWriter.format(node);
    diffRepos().assertEquals("formatted", this.formatted, formatted);

    // Now parse the result, and make sure it is structurally equivalent
    // to the original.
    final String actual2 = formatted.replace("`", "\"");
    final SqlNode node2;
    if (expression) {
      final SqlCall valuesCall =
          (SqlCall) parseQuery("VALUES (" + actual2 + ")");
      final SqlCall rowCall = valuesCall.operand(0);
      node2 = rowCall.operand(0);
    } else {
      SqlNode node2a = parseQuery(actual2);
      node2 = nodeTransformer.apply(node2a);
    }
    assertTrue(node1.equalsDeep(node2, Litmus.THROW));

    return this;
  }
}
