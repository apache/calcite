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

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.DelegatingTypeSystem;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.advise.SqlAdvisor;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.validate.SqlValidatorWithHints;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.ConnectionFactories;
import org.apache.calcite.test.ConnectionFactory;
import org.apache.calcite.test.MockRelOptPlanner;
import org.apache.calcite.test.MockSqlOperatorTable;
import org.apache.calcite.test.catalog.MockCatalogReaderSimple;
import org.apache.calcite.util.SourceStringReader;

import com.google.common.base.Suppliers;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static java.util.Objects.requireNonNull;

/**
 * As {@link SqlTestFactory} but has no state, and therefore
 * configuration is passed to each method.
*/
public class SqlTestFactory {
  public static final SqlTestFactory INSTANCE =
      new SqlTestFactory(MockCatalogReaderSimple::create,
          SqlTestFactory::createTypeFactory, MockRelOptPlanner::new,
          Contexts.of(), UnaryOperator.identity(),
          SqlValidatorUtil::newValidator,
          ConnectionFactories.empty()
              .with(ConnectionFactories.add(CalciteAssert.SchemaSpec.HR)),
          SqlParser.Config.DEFAULT,
          SqlValidator.Config.DEFAULT,
          SqlToRelConverter.CONFIG,
          SqlStdOperatorTable.instance())
      .withOperatorTable(o -> MockSqlOperatorTable.of(o).extend());

  public final ConnectionFactory connectionFactory;
  public final TypeFactoryFactory typeFactoryFactory;
  private final CatalogReaderFactory catalogReaderFactory;
  private final PlannerFactory plannerFactory;
  private final Context plannerContext;
  private final UnaryOperator<RelOptCluster> clusterTransform;
  private final ValidatorFactory validatorFactory;

  private final Supplier<RelDataTypeFactory> typeFactorySupplier;
  private final SqlOperatorTable operatorTable;
  private final Supplier<SqlValidatorCatalogReader> catalogReaderSupplier;
  private final SqlParser.Config parserConfig;
  public final SqlValidator.Config validatorConfig;
  public final SqlToRelConverter.Config sqlToRelConfig;

  protected SqlTestFactory(CatalogReaderFactory catalogReaderFactory,
      TypeFactoryFactory typeFactoryFactory, PlannerFactory plannerFactory,
      Context plannerContext, UnaryOperator<RelOptCluster> clusterTransform,
      ValidatorFactory validatorFactory,
      ConnectionFactory connectionFactory,
      SqlParser.Config parserConfig, SqlValidator.Config validatorConfig,
      SqlToRelConverter.Config sqlToRelConfig, SqlOperatorTable operatorTable) {
    this.catalogReaderFactory =
        requireNonNull(catalogReaderFactory, "catalogReaderFactory");
    this.typeFactoryFactory =
        requireNonNull(typeFactoryFactory, "typeFactoryFactory");
    this.plannerFactory = requireNonNull(plannerFactory, "plannerFactory");
    this.plannerContext = requireNonNull(plannerContext, "plannerContext");
    this.clusterTransform =
        requireNonNull(clusterTransform, "clusterTransform");
    this.validatorFactory =
        requireNonNull(validatorFactory, "validatorFactory");
    this.connectionFactory =
        requireNonNull(connectionFactory, "connectionFactory");
    this.sqlToRelConfig = requireNonNull(sqlToRelConfig, "sqlToRelConfig");
    this.operatorTable = operatorTable;
    this.typeFactorySupplier = Suppliers.memoize(() ->
        typeFactoryFactory.create(validatorConfig.conformance()))::get;
    this.catalogReaderSupplier = Suppliers.memoize(() ->
        catalogReaderFactory.create(this.typeFactorySupplier.get(),
            parserConfig.caseSensitive()))::get;
    this.parserConfig = parserConfig;
    this.validatorConfig = validatorConfig;
  }

  /** Creates a parser. */
  public SqlParser createParser(String sql) {
    SqlParser.Config parserConfig = parserConfig();
    return SqlParser.create(new SourceStringReader(sql), parserConfig);
  }

  /** Creates a validator. */
  public SqlValidator createValidator() {
    return validatorFactory.create(operatorTable, catalogReaderSupplier.get(),
        typeFactorySupplier.get(), validatorConfig);
  }

  public SqlAdvisor createAdvisor() {
    SqlValidator validator = createValidator();
    if (validator instanceof SqlValidatorWithHints) {
      return new SqlAdvisor((SqlValidatorWithHints) validator, parserConfig);
    }
    throw new UnsupportedOperationException(
        "Validator should implement SqlValidatorWithHints, actual validator is " + validator);
  }

  public SqlTestFactory withTypeFactoryFactory(
      TypeFactoryFactory typeFactoryFactory) {
    if (typeFactoryFactory.equals(this.typeFactoryFactory)) {
      return this;
    }
    return new SqlTestFactory(catalogReaderFactory, typeFactoryFactory,
        plannerFactory, plannerContext, clusterTransform, validatorFactory,
        connectionFactory, parserConfig, validatorConfig, sqlToRelConfig,
        operatorTable);
  }

  public SqlTestFactory withPlannerFactory(PlannerFactory plannerFactory) {
    if (plannerFactory.equals(this.plannerFactory)) {
      return this;
    }
    return new SqlTestFactory(catalogReaderFactory, typeFactoryFactory,
        plannerFactory, plannerContext, clusterTransform, validatorFactory,
        connectionFactory, parserConfig, validatorConfig, sqlToRelConfig,
        operatorTable);
  }

  public SqlTestFactory withPlannerContext(
      UnaryOperator<Context> transform) {
    final Context plannerContext = transform.apply(this.plannerContext);
    if (plannerContext.equals(this.plannerContext)) {
      return this;
    }
    return new SqlTestFactory(catalogReaderFactory, typeFactoryFactory,
        plannerFactory, plannerContext, clusterTransform, validatorFactory,
        connectionFactory, parserConfig, validatorConfig, sqlToRelConfig,
        operatorTable);
  }

  public SqlTestFactory withCluster(UnaryOperator<RelOptCluster> transform) {
    final UnaryOperator<RelOptCluster> clusterTransform =
        this.clusterTransform.andThen(transform)::apply;
    return new SqlTestFactory(catalogReaderFactory, typeFactoryFactory,
        plannerFactory, plannerContext, clusterTransform, validatorFactory,
        connectionFactory, parserConfig, validatorConfig, sqlToRelConfig,
        operatorTable);
  }

  public SqlTestFactory withCatalogReader(
      CatalogReaderFactory catalogReaderFactory) {
    if (catalogReaderFactory.equals(this.catalogReaderFactory)) {
      return this;
    }
    return new SqlTestFactory(catalogReaderFactory, typeFactoryFactory,
        plannerFactory, plannerContext, clusterTransform, validatorFactory,
        connectionFactory, parserConfig, validatorConfig, sqlToRelConfig,
        operatorTable);
  }

  public SqlTestFactory withValidator(ValidatorFactory validatorFactory) {
    if (validatorFactory.equals(this.validatorFactory)) {
      return this;
    }
    return new SqlTestFactory(catalogReaderFactory, typeFactoryFactory,
        plannerFactory, plannerContext, clusterTransform, validatorFactory,
        connectionFactory, parserConfig, validatorConfig, sqlToRelConfig,
        operatorTable);
  }

  public SqlTestFactory withValidatorConfig(
      UnaryOperator<SqlValidator.Config> transform) {
    final SqlValidator.Config validatorConfig =
        transform.apply(this.validatorConfig);
    if (validatorConfig.equals(this.validatorConfig)) {
      return this;
    }
    return new SqlTestFactory(catalogReaderFactory, typeFactoryFactory,
        plannerFactory, plannerContext, clusterTransform, validatorFactory,
        connectionFactory, parserConfig, validatorConfig, sqlToRelConfig,
        operatorTable);
  }

  public SqlTestFactory withSqlToRelConfig(
      UnaryOperator<SqlToRelConverter.Config> transform) {
    final SqlToRelConverter.Config sqlToRelConfig =
        transform.apply(this.sqlToRelConfig);
    if (sqlToRelConfig.equals(this.sqlToRelConfig)) {
      return this;
    }
    return new SqlTestFactory(catalogReaderFactory, typeFactoryFactory,
        plannerFactory, plannerContext, clusterTransform, validatorFactory,
        connectionFactory, parserConfig, validatorConfig, sqlToRelConfig,
        operatorTable);
  }

  private static RelDataTypeFactory createTypeFactory(SqlConformance conformance) {
    RelDataTypeSystem typeSystem = RelDataTypeSystem.DEFAULT;
    if (conformance.shouldConvertRaggedUnionTypesToVarying()) {
      typeSystem = new DelegatingTypeSystem(typeSystem) {
        @Override public boolean shouldConvertRaggedUnionTypesToVarying() {
          return true;
        }
      };
    }
    return new JavaTypeFactoryImpl(typeSystem);
  }

  public SqlTestFactory withParserConfig(
      UnaryOperator<SqlParser.Config> transform) {
    final SqlParser.Config parserConfig = transform.apply(this.parserConfig);
    if (parserConfig.equals(this.parserConfig)) {
      return this;
    }
    return new SqlTestFactory(catalogReaderFactory, typeFactoryFactory,
        plannerFactory, plannerContext, clusterTransform, validatorFactory,
        connectionFactory, parserConfig, validatorConfig, sqlToRelConfig,
        operatorTable);
  }

  public SqlTestFactory withConnectionFactory(
      UnaryOperator<ConnectionFactory> transform) {
    final ConnectionFactory connectionFactory =
        transform.apply(this.connectionFactory);
    if (connectionFactory.equals(this.connectionFactory)) {
      return this;
    }
    return new SqlTestFactory(catalogReaderFactory, typeFactoryFactory,
        plannerFactory, plannerContext, clusterTransform, validatorFactory,
        connectionFactory, parserConfig, validatorConfig, sqlToRelConfig,
        operatorTable);
  }

  public SqlTestFactory withOperatorTable(
      UnaryOperator<SqlOperatorTable> transform) {
    final SqlOperatorTable operatorTable =
        transform.apply(this.operatorTable);
    if (operatorTable.equals(this.operatorTable)) {
      return this;
    }
    return new SqlTestFactory(catalogReaderFactory, typeFactoryFactory,
        plannerFactory, plannerContext, clusterTransform, validatorFactory,
        connectionFactory, parserConfig, validatorConfig, sqlToRelConfig,
        operatorTable);
  }

  public SqlParser.Config parserConfig() {
    return parserConfig;
  }

  public RelDataTypeFactory getTypeFactory() {
    return typeFactorySupplier.get();
  }

  public SqlToRelConverter createSqlToRelConverter() {
    final RelDataTypeFactory typeFactory = getTypeFactory();
    final Prepare.CatalogReader catalogReader =
        (Prepare.CatalogReader) catalogReaderSupplier.get();
    final SqlValidator validator = createValidator();
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    final RelOptPlanner planner = plannerFactory.create(plannerContext);
    final RelOptCluster cluster =
        clusterTransform.apply(RelOptCluster.create(planner, rexBuilder));
    RelOptTable.ViewExpander viewExpander =
        new MockViewExpander(validator, catalogReader, cluster,
            sqlToRelConfig);
    return new SqlToRelConverter(viewExpander, validator, catalogReader, cluster,
        StandardConvertletTable.INSTANCE, sqlToRelConfig);
  }

  /** Creates a {@link RelDataTypeFactory} for tests. */
  public interface TypeFactoryFactory {
    RelDataTypeFactory create(SqlConformance conformance);
  }

  /** Creates a {@link RelOptPlanner} for tests. */
  public interface PlannerFactory {
    RelOptPlanner create(Context context);
  }

  /** Creates {@link SqlValidator} for tests. */
  public interface ValidatorFactory {
    SqlValidator create(
        SqlOperatorTable opTab,
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory,
        SqlValidator.Config config);
  }

  /** Creates a {@link SqlValidatorCatalogReader} for tests. */
  @FunctionalInterface
  public interface CatalogReaderFactory {
    SqlValidatorCatalogReader create(RelDataTypeFactory typeFactory,
        boolean caseSensitive);
  }

  /** Implementation for {@link RelOptTable.ViewExpander} for testing. */
  private static class MockViewExpander implements RelOptTable.ViewExpander {
    private final SqlValidator validator;
    private final Prepare.CatalogReader catalogReader;
    private final RelOptCluster cluster;
    private final SqlToRelConverter.Config config;

    MockViewExpander(SqlValidator validator,
        Prepare.CatalogReader catalogReader, RelOptCluster cluster,
        SqlToRelConverter.Config config) {
      this.validator = validator;
      this.catalogReader = catalogReader;
      this.cluster = cluster;
      this.config = config;
    }

    @Override public RelRoot expandView(RelDataType rowType, String queryString,
        List<String> schemaPath, @Nullable List<String> viewPath) {
      try {
        SqlNode parsedNode = SqlParser.create(queryString).parseStmt();
        SqlNode validatedNode = validator.validate(parsedNode);
        SqlToRelConverter converter =
            new SqlToRelConverter(this, validator, catalogReader, cluster,
                StandardConvertletTable.INSTANCE, config);
        return converter.convertQuery(validatedNode, false, true);
      } catch (SqlParseException e) {
        throw new RuntimeException("Error happened while expanding view.", e);
      }
    }
  }
}
