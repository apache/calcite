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
package org.apache.calcite.tools;

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.materialize.SqlStatisticProvider;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.statistic.QuerySqlStatisticProvider;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 * Tools for invoking Calcite functionality without initializing a container /
 * server first.
 */
public class Frameworks {
  private Frameworks() {
  }

  /**
   * Creates a planner.
   *
   * @param config Planner configuration
   * @return Planner
   */
  public static Planner getPlanner(FrameworkConfig config) {
    return new PlannerImpl(config);
  }

  /** Piece of code to be run in a context where a planner is available. The
   * planner is accessible from the {@code cluster} parameter, as are several
   * other useful objects.
   *
   * @param <R> result type */
  @FunctionalInterface
  public interface PlannerAction<R> {
    R apply(RelOptCluster cluster, RelOptSchema relOptSchema,
        SchemaPlus rootSchema);
  }

  /** Piece of code to be run in a context where a planner and statement are
   * available. The planner is accessible from the {@code cluster} parameter, as
   * are several other useful objects. The connection and
   * {@link org.apache.calcite.DataContext} are accessible from the
   * statement.
   *
   * @param <R> result type */
  @FunctionalInterface
  public interface BasePrepareAction<R> {
    R apply(RelOptCluster cluster, RelOptSchema relOptSchema,
        SchemaPlus rootSchema, CalciteServerStatement statement);
  }

  /** As {@link BasePrepareAction} but with a {@link FrameworkConfig} included.
   * Deprecated because a functional interface is more convenient.
   *
   * @param <R> result type */
  @Deprecated // to be removed before 2.0
  public abstract static class PrepareAction<R>
      implements BasePrepareAction<R> {
    private final FrameworkConfig config;
    public PrepareAction() {
      this.config = newConfigBuilder()
          .defaultSchema(Frameworks.createRootSchema(true)).build();
    }

    public PrepareAction(FrameworkConfig config) {
      this.config = config;
    }

    public FrameworkConfig getConfig() {
      return config;
    }
  }

  /**
   * Initializes a container then calls user-specified code with a planner.
   *
   * @param action Callback containing user-specified code
   * @param config FrameworkConfig to use for planner action.
   * @return Return value from action
   */
  public static <R> R withPlanner(final PlannerAction<R> action,
      final FrameworkConfig config) {
    return withPrepare(config,
        (cluster, relOptSchema, rootSchema, statement) -> {
          final CalciteSchema schema =
              CalciteSchema.from(
                  Util.first(config.getDefaultSchema(), rootSchema));
          return action.apply(cluster, relOptSchema, schema.root().plus());
        });
  }

  /**
   * Initializes a container then calls user-specified code with a planner.
   *
   * @param action Callback containing user-specified code
   * @return Return value from action
   */
  public static <R> R withPlanner(final PlannerAction<R> action) {
    FrameworkConfig config = newConfigBuilder() //
        .defaultSchema(Frameworks.createRootSchema(true)).build();
    return withPlanner(action, config);
  }

  @Deprecated // to be removed before 2.0
  public static <R> R withPrepare(PrepareAction<R> action) {
    return withPrepare(action.getConfig(), action);
  }

  /** As {@link #withPrepare(FrameworkConfig, BasePrepareAction)} but using a
   * default configuration. */
  public static <R> R withPrepare(BasePrepareAction<R> action) {
    final FrameworkConfig config = newConfigBuilder()
        .defaultSchema(Frameworks.createRootSchema(true)).build();
    return withPrepare(config, action);
  }

  /**
   * Initializes a container then calls user-specified code with a planner
   * and statement.
   *
   * @param action Callback containing user-specified code
   * @return Return value from action
   */
  public static <R> R withPrepare(FrameworkConfig config,
      BasePrepareAction<R> action) {
    try {
      final Properties info = new Properties();
      if (config.getTypeSystem() != RelDataTypeSystem.DEFAULT) {
        info.setProperty(CalciteConnectionProperty.TYPE_SYSTEM.camelName(),
            config.getTypeSystem().getClass().getName());
      }
      Connection connection =
          DriverManager.getConnection("jdbc:calcite:", info);
      final CalciteServerStatement statement =
          connection.createStatement()
              .unwrap(CalciteServerStatement.class);
      return new CalcitePrepareImpl().perform(statement, config, action);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a root schema.
   *
   * @param addMetadataSchema Whether to add "metadata" schema containing
   *    definitions of tables, columns etc.
   */
  public static SchemaPlus createRootSchema(boolean addMetadataSchema) {
    return CalciteSchema.createRootSchema(addMetadataSchema).plus();
  }

  /** Creates a config builder with each setting initialized to its default
   * value. */
  public static ConfigBuilder newConfigBuilder() {
    return new ConfigBuilder();
  }

  /** Creates a config builder initializing each setting from an existing
   * config.
   *
   * <p>So, {@code newConfigBuilder(config).build()} will return a
   * value equal to {@code config}. */
  public static ConfigBuilder newConfigBuilder(FrameworkConfig config) {
    return new ConfigBuilder(config);
  }

  /**
   * A builder to help you build a {@link FrameworkConfig} using defaults
   * where values aren't required.
   */
  public static class ConfigBuilder {
    private SqlRexConvertletTable convertletTable;
    private SqlOperatorTable operatorTable;
    private ImmutableList<Program> programs;
    private Context context;
    private ImmutableList<RelTraitDef> traitDefs;
    private SqlParser.Config parserConfig;
    private SqlToRelConverter.Config sqlToRelConverterConfig;
    private SchemaPlus defaultSchema;
    private RexExecutor executor;
    private RelOptCostFactory costFactory;
    private RelDataTypeSystem typeSystem;
    private boolean evolveLattice;
    private SqlStatisticProvider statisticProvider;
    private RelOptTable.ViewExpander viewExpander;

    /** Creates a ConfigBuilder, initializing to defaults. */
    private ConfigBuilder() {
      convertletTable = StandardConvertletTable.INSTANCE;
      operatorTable = SqlStdOperatorTable.instance();
      programs = ImmutableList.of();
      context = Contexts.empty();
      parserConfig = SqlParser.Config.DEFAULT;
      sqlToRelConverterConfig = SqlToRelConverter.Config.DEFAULT;
      typeSystem = RelDataTypeSystem.DEFAULT;
      evolveLattice = false;
      statisticProvider = QuerySqlStatisticProvider.SILENT_CACHING_INSTANCE;
    }

    /** Creates a ConfigBuilder, initializing from an existing config. */
    private ConfigBuilder(FrameworkConfig config) {
      convertletTable = config.getConvertletTable();
      operatorTable = config.getOperatorTable();
      programs = config.getPrograms();
      context = config.getContext();
      traitDefs = config.getTraitDefs();
      parserConfig = config.getParserConfig();
      sqlToRelConverterConfig = config.getSqlToRelConverterConfig();
      defaultSchema = config.getDefaultSchema();
      executor = config.getExecutor();
      costFactory = config.getCostFactory();
      typeSystem = config.getTypeSystem();
      evolveLattice = config.isEvolveLattice();
      statisticProvider = config.getStatisticProvider();
    }

    public FrameworkConfig build() {
      return new StdFrameworkConfig(context, convertletTable, operatorTable,
          programs, traitDefs, parserConfig, sqlToRelConverterConfig,
          defaultSchema, costFactory, typeSystem, executor, evolveLattice,
          statisticProvider, viewExpander);
    }

    public ConfigBuilder context(Context c) {
      this.context = Objects.requireNonNull(c);
      return this;
    }

    public ConfigBuilder executor(RexExecutor executor) {
      this.executor = Objects.requireNonNull(executor);
      return this;
    }

    public ConfigBuilder convertletTable(
        SqlRexConvertletTable convertletTable) {
      this.convertletTable = Objects.requireNonNull(convertletTable);
      return this;
    }

    public ConfigBuilder operatorTable(SqlOperatorTable operatorTable) {
      this.operatorTable = Objects.requireNonNull(operatorTable);
      return this;
    }

    public ConfigBuilder traitDefs(List<RelTraitDef> traitDefs) {
      if (traitDefs == null) {
        this.traitDefs = null;
      } else {
        this.traitDefs = ImmutableList.copyOf(traitDefs);
      }
      return this;
    }

    public ConfigBuilder traitDefs(RelTraitDef... traitDefs) {
      this.traitDefs = ImmutableList.copyOf(traitDefs);
      return this;
    }

    public ConfigBuilder parserConfig(SqlParser.Config parserConfig) {
      this.parserConfig = Objects.requireNonNull(parserConfig);
      return this;
    }

    public ConfigBuilder sqlToRelConverterConfig(
        SqlToRelConverter.Config sqlToRelConverterConfig) {
      this.sqlToRelConverterConfig =
          Objects.requireNonNull(sqlToRelConverterConfig);
      return this;
    }

    public ConfigBuilder defaultSchema(SchemaPlus defaultSchema) {
      this.defaultSchema = defaultSchema;
      return this;
    }

    public ConfigBuilder costFactory(RelOptCostFactory costFactory) {
      this.costFactory = costFactory;
      return this;
    }

    public ConfigBuilder ruleSets(RuleSet... ruleSets) {
      return programs(Programs.listOf(ruleSets));
    }

    public ConfigBuilder ruleSets(List<RuleSet> ruleSets) {
      return programs(Programs.listOf(Objects.requireNonNull(ruleSets)));
    }

    public ConfigBuilder programs(List<Program> programs) {
      this.programs = ImmutableList.copyOf(programs);
      return this;
    }

    public ConfigBuilder programs(Program... programs) {
      this.programs = ImmutableList.copyOf(programs);
      return this;
    }

    public ConfigBuilder typeSystem(RelDataTypeSystem typeSystem) {
      this.typeSystem = Objects.requireNonNull(typeSystem);
      return this;
    }

    public ConfigBuilder evolveLattice(boolean evolveLattice) {
      this.evolveLattice = evolveLattice;
      return this;
    }

    public ConfigBuilder statisticProvider(
        SqlStatisticProvider statisticProvider) {
      this.statisticProvider = Objects.requireNonNull(statisticProvider);
      return this;
    }

    public ConfigBuilder viewExpander(RelOptTable.ViewExpander viewExpander) {
      this.viewExpander = viewExpander;
      return this;
    }
  }

  /**
   * An implementation of {@link FrameworkConfig} that uses standard Calcite
   * classes to provide basic planner functionality.
   */
  static class StdFrameworkConfig implements FrameworkConfig {
    private final Context context;
    private final SqlRexConvertletTable convertletTable;
    private final SqlOperatorTable operatorTable;
    private final ImmutableList<Program> programs;
    private final ImmutableList<RelTraitDef> traitDefs;
    private final SqlParser.Config parserConfig;
    private final SqlToRelConverter.Config sqlToRelConverterConfig;
    private final SchemaPlus defaultSchema;
    private final RelOptCostFactory costFactory;
    private final RelDataTypeSystem typeSystem;
    private final RexExecutor executor;
    private final boolean evolveLattice;
    private final SqlStatisticProvider statisticProvider;
    private final RelOptTable.ViewExpander viewExpander;

    StdFrameworkConfig(Context context,
        SqlRexConvertletTable convertletTable,
        SqlOperatorTable operatorTable,
        ImmutableList<Program> programs,
        ImmutableList<RelTraitDef> traitDefs,
        SqlParser.Config parserConfig,
        SqlToRelConverter.Config sqlToRelConverterConfig,
        SchemaPlus defaultSchema,
        RelOptCostFactory costFactory,
        RelDataTypeSystem typeSystem,
        RexExecutor executor,
        boolean evolveLattice,
        SqlStatisticProvider statisticProvider,
        RelOptTable.ViewExpander viewExpander) {
      this.context = context;
      this.convertletTable = convertletTable;
      this.operatorTable = operatorTable;
      this.programs = programs;
      this.traitDefs = traitDefs;
      this.parserConfig = parserConfig;
      this.sqlToRelConverterConfig = sqlToRelConverterConfig;
      this.defaultSchema = defaultSchema;
      this.costFactory = costFactory;
      this.typeSystem = typeSystem;
      this.executor = executor;
      this.evolveLattice = evolveLattice;
      this.statisticProvider = statisticProvider;
      this.viewExpander = viewExpander;
    }

    public SqlParser.Config getParserConfig() {
      return parserConfig;
    }

    public SqlToRelConverter.Config getSqlToRelConverterConfig() {
      return sqlToRelConverterConfig;
    }

    public SchemaPlus getDefaultSchema() {
      return defaultSchema;
    }

    public RexExecutor getExecutor() {
      return executor;
    }

    public ImmutableList<Program> getPrograms() {
      return programs;
    }

    public RelOptCostFactory getCostFactory() {
      return costFactory;
    }

    public ImmutableList<RelTraitDef> getTraitDefs() {
      return traitDefs;
    }

    public SqlRexConvertletTable getConvertletTable() {
      return convertletTable;
    }

    public Context getContext() {
      return context;
    }

    public SqlOperatorTable getOperatorTable() {
      return operatorTable;
    }

    public RelDataTypeSystem getTypeSystem() {
      return typeSystem;
    }

    public boolean isEvolveLattice() {
      return evolveLattice;
    }

    public SqlStatisticProvider getStatisticProvider() {
      return statisticProvider;
    }

    public RelOptTable.ViewExpander getViewExpander() {
      return viewExpander;
    }
  }
}

// End Frameworks.java
