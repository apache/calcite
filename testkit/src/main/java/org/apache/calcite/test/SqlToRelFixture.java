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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.test.SqlTester;
import org.apache.calcite.sql.test.SqlValidatorTester;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.test.catalog.MockCatalogReaderDynamic;
import org.apache.calcite.test.catalog.MockCatalogReaderExtended;
import org.apache.calcite.util.TestUtil;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import static java.util.Objects.requireNonNull;

/**
 * Parameters for a SQL-to-RelNode test.
 */
public class SqlToRelFixture {
  public static final SqlTester TESTER = SqlValidatorTester.DEFAULT;

  public static final SqlToRelFixture DEFAULT =
      new SqlToRelFixture("?", true, TESTER, SqlTestFactory.INSTANCE, false,
          false, null)
          .withFactory(f ->
              f.withValidator((opTab, catalogReader, typeFactory, config) -> {
                if (config.conformance().allowGeometry()) {
                  opTab =
                      SqlOperatorTables.chain(opTab,
                          SqlOperatorTables.spatialInstance());
                }
                return SqlValidatorUtil.newValidator(opTab, catalogReader,
                    typeFactory, config.withIdentifierExpansion(true));
              })
                  .withSqlToRelConfig(c ->
                      c.withTrimUnusedFields(true)
                          .withExpand(true)
                          .addRelBuilderConfigTransform(b ->
                              b.withAggregateUnique(true)
                                  .withPruneInputOfAggregate(false))));

  private final String sql;
  private final @Nullable DiffRepository diffRepos;
  private final boolean decorrelate;
  private final SqlTester tester;
  private final SqlTestFactory factory;
  private final boolean trim;
  private final boolean expression;

  SqlToRelFixture(String sql, boolean decorrelate,
      SqlTester tester, SqlTestFactory factory, boolean trim,
      boolean expression,
      @Nullable DiffRepository diffRepos) {
    this.sql = requireNonNull(sql, "sql");
    this.tester = requireNonNull(tester, "tester");
    this.factory = requireNonNull(factory, "factory");
    this.diffRepos = diffRepos;
    if (sql.contains(" \n")) {
      throw new AssertionError("trailing whitespace");
    }
    this.decorrelate = decorrelate;
    this.trim = trim;
    this.expression = expression;
  }

  public void ok() {
    convertsTo("${plan}");
  }

  public void throws_(String message) {
    try {
      ok();
    } catch (Throwable throwable) {
      assertThat(TestUtil.printStackTrace(throwable), containsString(message));
    }
  }

  public void convertsTo(String plan) {
    tester.assertConvertsTo(factory, diffRepos(), sql, plan, trim, expression,
        decorrelate);
  }

  public DiffRepository diffRepos() {
    return DiffRepository.castNonNull(diffRepos);
  }

  public SqlToRelFixture withSql(String sql) {
    return sql.equals(this.sql) ? this
        : new SqlToRelFixture(sql, decorrelate, tester, factory, trim,
            expression, diffRepos);
  }

  /**
   * Sets whether this is an expression (as opposed to a whole query).
   */
  public SqlToRelFixture expression(boolean expression) {
    return this.expression == expression ? this
        : new SqlToRelFixture(sql, decorrelate, tester, factory, trim,
            expression, diffRepos);
  }

  public SqlToRelFixture withConfig(
      UnaryOperator<SqlToRelConverter.Config> transform) {
    return withFactory(f -> f.withSqlToRelConfig(transform));
  }

  public SqlToRelFixture withExpand(boolean expand) {
    return withConfig(b -> b.withExpand(expand));
  }

  public SqlToRelFixture withOptimizeOrderBy(boolean optimizeOrderBy) {
    return withConfig(b -> b.withOptimizeOrderBy(optimizeOrderBy));
  }

  public SqlToRelFixture withDecorrelate(boolean decorrelate) {
    return new SqlToRelFixture(sql, decorrelate, tester, factory, trim,
        expression, diffRepos);
  }

  public SqlToRelFixture withFactory(
      UnaryOperator<SqlTestFactory> transform) {
    final SqlTestFactory factory = transform.apply(this.factory);
    if (factory == this.factory) {
      return this;
    }
    return new SqlToRelFixture(sql, decorrelate, tester, factory, trim,
        expression, diffRepos);
  }

  public SqlToRelFixture withCatalogReader(
      SqlTestFactory.CatalogReaderFactory catalogReaderFactory) {
    return withFactory(f -> f.withCatalogReader(catalogReaderFactory));
  }

  public SqlToRelFixture withExtendedTester() {
    return withCatalogReader(MockCatalogReaderExtended::create);
  }

  public SqlToRelFixture withDynamicTable() {
    return withCatalogReader(MockCatalogReaderDynamic::create);
  }

  public SqlToRelFixture withTrim(boolean trim) {
    return new SqlToRelFixture(sql, decorrelate, tester, factory, trim,
        expression, diffRepos);
  }

  public SqlConformance getConformance() {
    return factory.parserConfig().conformance();
  }

  public SqlToRelFixture withConformance(SqlConformance conformance) {
    return withFactory(f ->
        f.withParserConfig(c -> c.withConformance(conformance))
            .withValidatorConfig(c -> c.withConformance(conformance)));
  }

  public SqlToRelFixture withDiffRepos(DiffRepository diffRepos) {
    return new SqlToRelFixture(sql, decorrelate, tester, factory, trim,
        expression, diffRepos);
  }

  public RelRoot toRoot() {
    return tester
        .convertSqlToRel(factory, sql, decorrelate, trim);
  }

  public RelNode toRel() {
    return toRoot().rel;
  }

  /** Returns a fixture that meets a given condition, applying a remedy if it
   * does not already. */
  public SqlToRelFixture ensuring(Predicate<SqlToRelFixture> predicate,
      UnaryOperator<SqlToRelFixture> remedy) {
    SqlToRelFixture f = this;
    if (!predicate.test(f)) {
      f = remedy.apply(f);
      assertThat("remedy failed", predicate.test(f), is(true));
    }
    return f;
  }
}
