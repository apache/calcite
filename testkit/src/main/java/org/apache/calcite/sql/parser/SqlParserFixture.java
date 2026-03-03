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
package org.apache.calcite.sql.parser;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static java.util.Objects.requireNonNull;

/**
 * Helper class for building fluent parser tests such as
 * {@code sql("values 1").ok();}.
 */
public class SqlParserFixture {
  public static final SqlTestFactory FACTORY =
      SqlTestFactory.INSTANCE.withParserConfig(c ->
          c.withQuoting(Quoting.DOUBLE_QUOTE)
              .withUnquotedCasing(Casing.TO_UPPER)
              .withQuotedCasing(Casing.UNCHANGED)
              .withConformance(SqlConformanceEnum.DEFAULT));

  public static final SqlParserFixture DEFAULT =
      new SqlParserFixture(FACTORY, StringAndPos.of("?"), false,
          SqlParserTest.TesterImpl.DEFAULT, null, true, parser -> {
      });

  public final SqlTestFactory factory;
  public final StringAndPos sap;
  public final boolean expression;
  public final SqlParserTest.Tester tester;
  public final boolean convertToLinux;
  public final @Nullable SqlDialect dialect;
  public final Consumer<SqlParser> parserChecker;

  SqlParserFixture(SqlTestFactory factory, StringAndPos sap, boolean expression,
      SqlParserTest.Tester tester, @Nullable SqlDialect dialect,
      boolean convertToLinux, Consumer<SqlParser> parserChecker) {
    this.factory = requireNonNull(factory, "factory");
    this.sap = requireNonNull(sap, "sap");
    this.expression = expression;
    this.tester = requireNonNull(tester, "tester");
    this.dialect = dialect;
    this.convertToLinux = convertToLinux;
    this.parserChecker = requireNonNull(parserChecker, "parserChecker");
  }

  public SqlParserFixture same() {
    return compare(sap.sql);
  }

  public SqlParserFixture ok(String expected) {
    if (expected.equals(sap.sql)) {
      throw new AssertionError("you should call same()");
    }
    return compare(expected);
  }

  public SqlParserFixture compare(String expected) {
    final UnaryOperator<String> converter = SqlParserTest.linux(convertToLinux);
    if (expression) {
      tester.checkExp(factory, sap, converter, expected, parserChecker);
    } else {
      tester.check(factory, sap, dialect, converter, expected, parserChecker);
    }
    return this;
  }

  public SqlParserFixture fails(String expectedMsgPattern) {
    if (expression) {
      tester.checkExpFails(factory, sap, expectedMsgPattern);
    } else {
      tester.checkFails(factory, sap, false, expectedMsgPattern);
    }
    return this;
  }

  public SqlParserFixture hasWarning(Consumer<List<? extends Throwable>> messageMatcher) {
    final Consumer<SqlParser> parserConsumer = parser ->
        messageMatcher.accept(parser.getWarnings());
    return new SqlParserFixture(factory, sap, expression, tester, dialect,
        convertToLinux, parserConsumer);
  }

  public SqlParserFixture node(Matcher<SqlNode> matcher) {
    tester.checkNode(factory, sap, matcher);
    return this;
  }

  /**
   * Changes the SQL.
   */
  public SqlParserFixture sql(String sql) {
    if (sql.equals(this.sap.addCarets())) {
      return this;
    }
    StringAndPos sap = StringAndPos.of(sql);
    return new SqlParserFixture(factory, sap, expression, tester, dialect,
        convertToLinux, parserChecker);
  }

  /**
   * Flags that this is an expression, not a whole query.
   */
  public SqlParserFixture expression() {
    return expression(true);
  }

  /**
   * Sets whether this is an expression (as opposed to a whole query).
   */
  public SqlParserFixture expression(boolean expression) {
    if (this.expression == expression) {
      return this;
    }
    return new SqlParserFixture(factory, sap, expression, tester, dialect,
        convertToLinux, parserChecker);
  }

  /**
   * Creates an instance of helper class {@link SqlParserListFixture} to test parsing a
   * list of statements.
   */
  protected SqlParserListFixture list() {
    return new SqlParserListFixture(factory, tester, dialect, convertToLinux, sap);
  }

  public SqlParserFixture withDialect(SqlDialect dialect) {
    if (dialect == this.dialect) {
      return this;
    }
    SqlTestFactory factory =
        this.factory.withParserConfig(dialect::configureParser);
    return new SqlParserFixture(factory, sap, expression, tester, dialect,
        convertToLinux, parserChecker);
  }

  /**
   * Creates a copy of this fixture with a new test factory.
   */
  public SqlParserFixture withFactory(UnaryOperator<SqlTestFactory> transform) {
    final SqlTestFactory factory = transform.apply(this.factory);
    if (factory == this.factory) {
      return this;
    }
    return new SqlParserFixture(factory, sap, expression, tester, dialect,
        convertToLinux, parserChecker);
  }

  public SqlParserFixture withConfig(UnaryOperator<SqlParser.Config> transform) {
    return withFactory(f -> f.withParserConfig(transform));
  }

  public SqlParserFixture withConformance(SqlConformance conformance) {
    return withConfig(c -> c.withConformance(conformance));
  }

  public SqlParserFixture withTester(SqlParserTest.Tester tester) {
    if (tester == this.tester) {
      return this;
    }
    return new SqlParserFixture(factory, sap, expression, tester, dialect,
        convertToLinux, parserChecker);
  }

  public SqlParserFixture withQuoting(Quoting quoting) {
    return withConfig(c -> c.withQuoting(quoting));
  }

  /**
   * Sets whether to convert actual strings to Linux (converting Windows
   * CR-LF line endings to Linux LF) before comparing them to expected.
   * Default is true.
   */
  public SqlParserFixture withConvertToLinux(boolean convertToLinux) {
    if (convertToLinux == this.convertToLinux) {
      return this;
    }
    return new SqlParserFixture(factory, sap, expression, tester, dialect,
        convertToLinux, parserChecker);
  }

  public SqlParser parser() {
    return factory.createParser(sap.addCarets());
  }

  public SqlNode node() {
    return ((SqlParserTest.TesterImpl) tester)
        .parseStmtAndHandleEx(factory, sap.addCarets(), parser -> {
        });
  }

  public SqlNodeList nodeList() {
    return ((SqlParserTest.TesterImpl) tester)
        .parseStmtsAndHandleEx(factory, sap.addCarets());
  }
}
