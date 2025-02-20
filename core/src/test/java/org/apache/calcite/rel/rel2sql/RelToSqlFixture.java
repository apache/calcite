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
package org.apache.calcite.rel.rel2sql;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.ProjectOverSumToSum0Rule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.MockSqlOperatorTable;
import org.apache.calcite.test.RelBuilderTest;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Token;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static org.apache.calcite.rel.rel2sql.DialectCode.BIG_QUERY;
import static org.apache.calcite.rel.rel2sql.DialectCode.CALCITE;
import static org.apache.calcite.rel.rel2sql.DialectCode.CLICKHOUSE;
import static org.apache.calcite.rel.rel2sql.DialectCode.DB2;
import static org.apache.calcite.rel.rel2sql.DialectCode.EXASOL;
import static org.apache.calcite.rel.rel2sql.DialectCode.FIREBOLT;
import static org.apache.calcite.rel.rel2sql.DialectCode.HIVE;
import static org.apache.calcite.rel.rel2sql.DialectCode.HSQLDB;
import static org.apache.calcite.rel.rel2sql.DialectCode.INFORMIX;
import static org.apache.calcite.rel.rel2sql.DialectCode.JETHRO;
import static org.apache.calcite.rel.rel2sql.DialectCode.MSSQL_2017;
import static org.apache.calcite.rel.rel2sql.DialectCode.MYSQL;
import static org.apache.calcite.rel.rel2sql.DialectCode.MYSQL_8;
import static org.apache.calcite.rel.rel2sql.DialectCode.MYSQL_FIRST;
import static org.apache.calcite.rel.rel2sql.DialectCode.MYSQL_HIGH;
import static org.apache.calcite.rel.rel2sql.DialectCode.MYSQL_LAST;
import static org.apache.calcite.rel.rel2sql.DialectCode.ORACLE_11;
import static org.apache.calcite.rel.rel2sql.DialectCode.ORACLE_12;
import static org.apache.calcite.rel.rel2sql.DialectCode.ORACLE_19;
import static org.apache.calcite.rel.rel2sql.DialectCode.ORACLE_23;
import static org.apache.calcite.rel.rel2sql.DialectCode.ORACLE_MODIFIED;
import static org.apache.calcite.rel.rel2sql.DialectCode.POSTGRESQL;
import static org.apache.calcite.rel.rel2sql.DialectCode.POSTGRESQL_MODIFIED;
import static org.apache.calcite.rel.rel2sql.DialectCode.POSTGRESQL_MODIFIED_DECIMAL;
import static org.apache.calcite.rel.rel2sql.DialectCode.PRESTO;
import static org.apache.calcite.rel.rel2sql.DialectCode.REDSHIFT;
import static org.apache.calcite.rel.rel2sql.DialectCode.SNOWFLAKE;
import static org.apache.calcite.rel.rel2sql.DialectCode.SPARK;
import static org.apache.calcite.rel.rel2sql.DialectCode.STARROCKS;
import static org.apache.calcite.rel.rel2sql.DialectCode.SYBASE;
import static org.apache.calcite.rel.rel2sql.DialectCode.VERTICA;
import static org.apache.calcite.test.Matchers.isLinux;
import static org.apache.calcite.test.Matchers.returnsUnordered;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import static java.util.Objects.requireNonNull;

/** Fluid interface to run tests. */
class RelToSqlFixture {
  /** A pool of tokens, used to identify fixtures that forgot to call
   * {@link #done()}. */
  static final Token.Pool POOL = Token.pool();

  private final Token token;
  private final CalciteAssert.SchemaSpec schemaSpec;
  private final String sql;
  private final DialectTestConfig.Dialect dialect;
  private final DialectTestConfig.Phase phase;
  private final Set<SqlLibrary> librarySet;
  private final @Nullable Function<RelBuilder, RelNode> relFn;
  private final List<Function<RelNode, RelNode>> relTransforms;
  private final SqlParser.Config parserConfig;
  private final UnaryOperator<SqlToRelConverter.Config> configTransform;
  private final DialectTestConfig testConfig;
  private final UnaryOperator<SqlWriterConfig> writerTransform;

  RelToSqlFixture(Token token, CalciteAssert.SchemaSpec schemaSpec, String sql,
      DialectTestConfig.Dialect dialect, DialectTestConfig.Phase phase,
      SqlParser.Config parserConfig, Set<SqlLibrary> librarySet,
      UnaryOperator<SqlToRelConverter.Config> configTransform,
      @Nullable Function<RelBuilder, RelNode> relFn,
      List<Function<RelNode, RelNode>> relTransforms,
      DialectTestConfig testConfig,
      UnaryOperator<SqlWriterConfig> writerTransform) {
    this.token = requireNonNull(token, "token");
    this.schemaSpec = schemaSpec;
    this.sql = sql;
    this.dialect = dialect;
    this.phase = requireNonNull(phase, "phase");
    this.librarySet = librarySet;
    this.relFn = relFn;
    this.relTransforms = ImmutableList.copyOf(relTransforms);
    this.parserConfig = parserConfig;
    this.configTransform = configTransform;
    this.testConfig = requireNonNull(testConfig, "testConfig");
    this.writerTransform = requireNonNull(writerTransform, "writerTransform");
  }

  /** Default writer configuration. */
  static SqlWriterConfig transformWriter(SqlWriterConfig c) {
    return c.withAlwaysUseParentheses(false)
        .withSelectListItemsOnSeparateLines(false)
        .withUpdateSetListNewline(false)
        .withIndentation(0);
  }

  public RelToSqlFixture schema(CalciteAssert.SchemaSpec schemaSpec) {
    return new RelToSqlFixture(token, schemaSpec, sql, dialect, phase,
        parserConfig, librarySet, configTransform, relFn, relTransforms,
        testConfig, writerTransform);
  }

  public RelToSqlFixture withSql(String sql) {
    if (sql.equals(this.sql)) {
      return this;
    }
    return new RelToSqlFixture(token, schemaSpec, sql, dialect, phase,
        parserConfig, librarySet, configTransform, relFn, relTransforms,
        testConfig, writerTransform);
  }

  public RelToSqlFixture dialect(DialectCode dialectCode) {
    DialectTestConfig.Dialect dialect = testConfig.get(dialectCode);
    return withDialect(dialect);
  }

  public RelToSqlFixture withDialect(DialectTestConfig.Dialect dialect) {
    if (dialect.equals(this.dialect)) {
      return this;
    }
    return new RelToSqlFixture(token, schemaSpec, sql, dialect, phase,
        parserConfig, librarySet, configTransform, relFn, relTransforms,
        testConfig, writerTransform);
  }

  public RelToSqlFixture parserConfig(SqlParser.Config parserConfig) {
    if (parserConfig.equals(this.parserConfig)) {
      return this;
    }
    return new RelToSqlFixture(token, schemaSpec, sql, dialect, phase,
        parserConfig, librarySet, configTransform, relFn, relTransforms,
        testConfig, writerTransform);
  }

  public final RelToSqlFixture withLibrary(SqlLibrary library) {
    return withLibrarySet(ImmutableSet.of(library));
  }

  public RelToSqlFixture withLibrarySet(
      Iterable<? extends SqlLibrary> librarySet) {
    final ImmutableSet<SqlLibrary> librarySet1 =
        ImmutableSet.copyOf(librarySet);
    if (librarySet1.equals(this.librarySet)) {
      return this;
    }
    return new RelToSqlFixture(token, schemaSpec, sql, dialect, phase,
        parserConfig, librarySet1, configTransform, relFn, relTransforms,
        testConfig, writerTransform);
  }

  public RelToSqlFixture withPhase(DialectTestConfig.Phase phase) {
    return new RelToSqlFixture(token, schemaSpec, sql, dialect, phase,
        parserConfig, librarySet, configTransform, relFn, relTransforms,
        testConfig, writerTransform);
  }

  public RelToSqlFixture withConfig(
      UnaryOperator<SqlToRelConverter.Config> configTransform) {
    if (configTransform.equals(this.configTransform)) {
      return this;
    }
    return new RelToSqlFixture(token, schemaSpec, sql, dialect, phase,
        parserConfig, librarySet, configTransform, relFn, relTransforms,
        testConfig, writerTransform);
  }

  public RelToSqlFixture relFn(Function<RelBuilder, RelNode> relFn) {
    if (relFn.equals(this.relFn)) {
      return this;
    }
    return new RelToSqlFixture(token, schemaSpec, sql, dialect, phase,
        parserConfig, librarySet, configTransform, relFn, relTransforms,
        testConfig, writerTransform);
  }

  public RelToSqlFixture withExtraTransform(
      Function<RelNode, RelNode> relTransform) {
    final List<Function<RelNode, RelNode>> relTransforms2 =
        FlatLists.append(relTransforms, relTransform);
    return new RelToSqlFixture(token, schemaSpec, sql, dialect, phase,
        parserConfig, librarySet, configTransform, relFn, relTransforms2,
        testConfig, writerTransform);
  }

  public RelToSqlFixture withTestConfig(
      UnaryOperator<DialectTestConfig> transform) {
    DialectTestConfig testConfig = transform.apply(this.testConfig);
    return new RelToSqlFixture(token, schemaSpec, sql, dialect, phase,
        parserConfig, librarySet, configTransform, relFn, relTransforms,
        testConfig, writerTransform);
  }

  public RelToSqlFixture withWriterConfig(
      UnaryOperator<SqlWriterConfig> writerTransform) {
    if (writerTransform.equals(this.writerTransform)) {
      return this;
    }
    return new RelToSqlFixture(token, schemaSpec, sql, dialect, phase,
        parserConfig, librarySet, configTransform, relFn, relTransforms,
        testConfig, writerTransform);
  }

  RelToSqlFixture withBigQuery() {
    return dialect(BIG_QUERY);
  }

  RelToSqlFixture withCalcite() {
    return dialect(CALCITE);
  }

  RelToSqlFixture withClickHouse() {
    return dialect(CLICKHOUSE);
  }

  RelToSqlFixture withDb2() {
    return dialect(DB2);
  }

  RelToSqlFixture withExasol() {
    return dialect(EXASOL);
  }

  RelToSqlFixture withFirebolt() {
    return dialect(FIREBOLT);
  }

  RelToSqlFixture withHive() {
    return dialect(HIVE);
  }

  RelToSqlFixture withHsqldb() {
    return dialect(HSQLDB);
  }

  RelToSqlFixture withInformix() {
    return dialect(INFORMIX);
  }

  RelToSqlFixture withJethro() {
    return dialect(JETHRO);
  }

  RelToSqlFixture withMssql() {
    return dialect(MSSQL_2017); // MSSQL 2008 = 10.0, 2012 = 11.0, 2017 = 14.0
  }

  RelToSqlFixture withMysql() {
    return dialect(MYSQL);
  }

  RelToSqlFixture withMysqlHigh() {
    return dialect(MYSQL_HIGH);
  }

  RelToSqlFixture withMysqlFirst() {
    return dialect(MYSQL_FIRST);
  }

  RelToSqlFixture withMysqlLast() {
    return dialect(MYSQL_LAST);
  }

  RelToSqlFixture withMysql8() {
    return dialect(MYSQL_8);
  }

  RelToSqlFixture withOracle() {
    return withOracle(12);
  }

  RelToSqlFixture withOracle(int majorVersion) {
    switch (majorVersion) {
    case 11:
      return dialect(ORACLE_11);
    case 12:
      return dialect(ORACLE_12);
    case 19:
      return dialect(ORACLE_19);
    case 23:
      return dialect(ORACLE_23);
    default:
      throw new IllegalArgumentException("Oracle version is not supported: "
          + majorVersion);
    }
  }

  RelToSqlFixture withOracleModifiedTypeSystem() {
    return dialect(ORACLE_MODIFIED);
  }

  RelToSqlFixture withPostgresql() {
    return dialect(POSTGRESQL);
  }

  RelToSqlFixture withPostgresqlModifiedDecimalTypeSystem() {
    return dialect(POSTGRESQL_MODIFIED_DECIMAL);
  }

  RelToSqlFixture withPostgresqlModifiedTypeSystem() {
    return dialect(POSTGRESQL_MODIFIED);
  }

  RelToSqlFixture withPresto() {
    return dialect(PRESTO);
  }

  RelToSqlFixture withRedshift() {
    return dialect(REDSHIFT);
  }

  RelToSqlFixture withSnowflake() {
    return dialect(SNOWFLAKE);
  }

  RelToSqlFixture withSpark() {
    return dialect(SPARK);
  }

  RelToSqlFixture withStarRocks() {
    return dialect(STARROCKS);
  }

  RelToSqlFixture withSybase() {
    return dialect(SYBASE);
  }

  RelToSqlFixture withVertica() {
    return dialect(VERTICA);
  }

  /** Disables this test for a given list of dialects. */
  RelToSqlFixture withDisable(DialectCode code0, DialectCode... codes) {
    final Set<DialectCode> dialectCodes = EnumSet.of(code0, codes);
    return withTestConfig(c ->
        c.withDialects(d ->
            dialectCodes.contains(d.code) ? d.withEnabled(false) : d));
  }

  RelToSqlFixture optimize(final RuleSet ruleSet,
      final @Nullable RelOptPlanner relOptPlanner) {
    final Function<RelNode, RelNode> relTransform = r -> {
      Program program = Programs.of(ruleSet);
      final RelOptPlanner p =
          Util.first(relOptPlanner,
              new HepPlanner(
                  new HepProgramBuilder().addRuleClass(RelOptRule.class)
                      .build()));
      return program.run(p, r, r.getTraitSet(), ImmutableList.of(),
          ImmutableList.of());
    };
    return withExtraTransform(relTransform);
  }

  RelToSqlFixture optimizeOver() {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ProjectOverSumToSum0Rule.class);
    builder.addRuleClass(ProjectToWindowRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    RuleSet rules =
        RuleSets.ofList(CoreRules.PROJECT_OVER_SUM_TO_SUM0_RULE,
            CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);
    return optimize(rules, hepPlanner);
  }

  RelToSqlFixture ok(String expectedQuery) {
    return withTestConfig(c ->
        c.withDialect(dialect.code,
            d -> d.withExpectedQuery(expectedQuery).withEnabled(true)));
  }

  RelToSqlFixture throws_(String errorMessage) {
    return withTestConfig(c ->
        c.withDialect(dialect.code,
            d -> d.withExpectedError(errorMessage).withEnabled(true)));
  }

  String exec() {
    try {
      final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
      final SchemaPlus defaultSchema =
          CalciteAssert.addSchema(rootSchema, schemaSpec);
      RelNode rel;
      if (relFn != null) {
        final FrameworkConfig frameworkConfig =
            RelBuilderTest.config().defaultSchema(defaultSchema).build();
        final RelBuilder relBuilder = RelBuilder.create(frameworkConfig);
        rel = relFn.apply(relBuilder);
      } else {
        final SqlToRelConverter.Config config =
            this.configTransform.apply(SqlToRelConverter.config()
                .withTrimUnusedFields(false));
        final RelDataTypeSystem typeSystem = dialect.sqlDialect.getTypeSystem();
        final Planner planner =
            getPlanner(null, parserConfig, defaultSchema, config, librarySet,
                typeSystem);
        SqlNode parse = planner.parse(sql);
        SqlNode validate = planner.validate(parse);
        rel = planner.rel(validate).project();
      }
      for (Function<RelNode, RelNode> transform : relTransforms) {
        rel = transform.apply(rel);
      }
      return toSql(rel, dialect.code);
    } catch (Exception e) {
      throw TestUtil.rethrow(e);
    }
  }

  public RelToSqlFixture done() {
    token.close();

    final AtomicReference<List<String>> referenceResultSet = new AtomicReference<>();
    final AtomicReference<List<String>> referenceValid = new AtomicReference<>();
    if (testConfig.refDialectCode != null) {
      DialectTestConfig.Dialect referenceDialect =
          testConfig.get(testConfig.refDialectCode);
      final String referenceSql =
          testConfig.refDialectCode == CALCITE && relFn == null
              ? sql
              : dialect(testConfig.refDialectCode).exec();
      switch (phase) {
      case PREPARE:
        referenceDialect.withConnection(schemaSpec, c -> {
          try (PreparedStatement ps = c.prepareStatement(referenceSql)) {
            List<String> columnNames = new ArrayList<>();
            final ResultSetMetaData metaData = ps.getMetaData();
            for (int i = 0; i < metaData.getColumnCount(); i++) {
              columnNames.add(metaData.getColumnName(i + 1));
            }
            referenceValid.set(ImmutableList.copyOf(columnNames));
          } catch (SQLException e) {
            throw new RuntimeException("while preparing [" + referenceSql + "]", e);
          }
        });
        break;

      case EXECUTE:
        referenceDialect.withStatement(schemaSpec, statement -> {
          try (ResultSet resultSet = statement.executeQuery(referenceSql)) {
            final List<String> rows = new ArrayList<>();
            CalciteAssert.toStringList(resultSet, rows);
            referenceResultSet.set(ImmutableList.copyOf(rows));
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
      }
    }

    // Generate the query in all enabled dialects, and check results if there
    // is a reference dialect.
    testConfig.dialectMap.forEach((dialectName, dialect) -> {
      if (dialect.enabled) {
        final String sql;
        if (dialect.expectedError != null) {
          try {
            sql = dialect(dialect.code).exec();
            throw new AssertionError("Expected exception with message `"
                + dialect.expectedError + "` but nothing was thrown; got "
                + sql);
          } catch (Exception e) {
            assertThat(e.getMessage(), is(dialect.expectedError));
            return;
          }
        } else {
          sql = dialect(dialect.code).exec();
        }

        if (dialect.expectedQuery != null) {
          assertThat(sql, isLinux(dialect.expectedQuery));
        }

        if (dialect.execute) {
          dialect.withStatement(schemaSpec, statement -> {
            try {
              final ResultSet resultSet = statement.executeQuery(sql);
              if (referenceResultSet.get() != null) {
                assertThat(resultSet, returnsUnordered(referenceResultSet.get()));
              }
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          });
        }
      }
    });
    return this;
  }

  private static Planner getPlanner(@Nullable List<RelTraitDef> traitDefs,
      SqlParser.Config parserConfig, SchemaPlus schema,
      SqlToRelConverter.Config sqlToRelConf, Collection<SqlLibrary> librarySet,
      RelDataTypeSystem typeSystem, Program... programs) {
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(parserConfig)
        .defaultSchema(schema)
        .traitDefs(traitDefs)
        .sqlToRelConverterConfig(sqlToRelConf)
        .programs(programs)
        .operatorTable(MockSqlOperatorTable.standard()
            .plus(librarySet)
            .extend())
        .typeSystem(typeSystem)
        .build();
    return Frameworks.getPlanner(config);
  }

  /** Converts a relational expression to SQL in a given dialect. */
  private String toSql(RelNode root,
      DialectCode dialectCode) {
    return toSql(root, dialectCode, writerTransform);
  }

  /** Converts a relational expression to SQL in a given dialect
   * and with a particular writer configuration. */
  private String toSql(RelNode root, DialectCode dialectCode,
      UnaryOperator<SqlWriterConfig> transform) {
    final DialectTestConfig.Dialect dialect = testConfig.get(dialectCode);
    final SqlDialect sqlDialect = dialect.sqlDialect;
    final RelToSqlConverter converter = new RelToSqlConverter(sqlDialect);
    final SqlNode sqlNode = converter.visitRoot(root).asStatement();
    return sqlNode.toSqlString(c -> transform.apply(c.withDialect(sqlDialect)))
        .getSql();
  }

  /** Returns the dialect. */
  public SqlDialect sqlDialect() {
    return dialect.sqlDialect;
  }
}
