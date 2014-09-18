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
package net.hydromatic.optiq.tools;

import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.config.Lex;
import net.hydromatic.optiq.config.OptiqConnectionProperty;
import net.hydromatic.optiq.jdbc.OptiqConnection;
import net.hydromatic.optiq.jdbc.OptiqSchema;
import net.hydromatic.optiq.prepare.OptiqPrepareImpl;
import net.hydromatic.optiq.prepare.PlannerImpl;
import net.hydromatic.optiq.server.OptiqServerStatement;

import org.eigenbase.relopt.Context;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCostFactory;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.relopt.RelTraitDef;
import org.eigenbase.reltype.RelDataTypeSystem;
import org.eigenbase.sql.SqlOperatorTable;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParserImplFactory;
import org.eigenbase.sql.parser.impl.SqlParserImpl;
import org.eigenbase.sql2rel.SqlRexConvertletTable;
import org.eigenbase.sql2rel.StandardConvertletTable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Properties;

/**
 * Tools for invoking Optiq functionality without initializing a container /
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
   * other useful objects. */
  public interface PlannerAction<R> {
    R apply(RelOptCluster cluster, RelOptSchema relOptSchema,
        SchemaPlus rootSchema);
  }

  /** Piece of code to be run in a context where a planner and statement are
   * available. The planner is accessible from the {@code cluster} parameter, as
   * are several other useful objects. The connection and
   * {@link net.hydromatic.optiq.DataContext} are accessible from the
   * statement. */
  public abstract static class PrepareAction<R> {
    private final FrameworkConfig config;

    public PrepareAction() {
      this.config = newConfigBuilder() //
          .defaultSchema(Frameworks.createRootSchema(true)).build();
    }

    public PrepareAction(FrameworkConfig config) {
      this.config = config;
    }

    public FrameworkConfig getConfig() {
      return config;
    }

    public abstract R apply(RelOptCluster cluster, RelOptSchema relOptSchema,
        SchemaPlus rootSchema, OptiqServerStatement statement);
  }

  /**
   * Initializes a container then calls user-specified code with a planner.
   *
   * @param action Callback containing user-specified code
   * @param config FrameworkConfig to use for planner action.
   * @return Return value from action
   */
  public static <R> R withPlanner(final PlannerAction<R> action, //
      FrameworkConfig config) {
    return withPrepare(
        new Frameworks.PrepareAction<R>(config) {
          public R apply(RelOptCluster cluster, RelOptSchema relOptSchema,
              SchemaPlus rootSchema, OptiqServerStatement statement) {
            return action.apply(cluster, relOptSchema, rootSchema);
          }
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

  /**
   * Initializes a container then calls user-specified code with a planner
   * and statement.
   *
   * @param action Callback containing user-specified code
   * @return Return value from action
   */
  public static <R> R withPrepare(PrepareAction<R> action) {
    try {
      Class.forName("net.hydromatic.optiq.jdbc.Driver");
      final Properties info = new Properties();
      if (action.config.getTypeSystem() != RelDataTypeSystem.DEFAULT) {
        info.setProperty(OptiqConnectionProperty.TYPE_SYSTEM.camelName(),
            action.config.getTypeSystem().getClass().getName());
      }
      Connection connection =
          DriverManager.getConnection("jdbc:optiq:", info);
      OptiqConnection optiqConnection =
          connection.unwrap(OptiqConnection.class);
      final OptiqServerStatement statement =
          optiqConnection.createStatement().unwrap(OptiqServerStatement.class);
      return new OptiqPrepareImpl().perform(statement, action);
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
    return OptiqSchema.createRootSchema(addMetadataSchema).plus();
  }

  public static ConfigBuilder newConfigBuilder() {
    return new ConfigBuilder();
  }

  /**
   * A builder to help you build a {@link FrameworkConfig} using defaults
   * where values aren't required.
   */
  public static class ConfigBuilder {
    private SqlRexConvertletTable convertletTable =
        StandardConvertletTable.INSTANCE;
    private SqlOperatorTable operatorTable = SqlStdOperatorTable.instance();
    private ImmutableList<Program> programs = ImmutableList.of();
    private Context context;
    private ImmutableList<RelTraitDef> traitDefs;
    private Lex lex = Lex.ORACLE;
    private SchemaPlus defaultSchema;
    private RelOptCostFactory costFactory;
    private SqlParserImplFactory parserFactory = SqlParserImpl.FACTORY;
    private RelDataTypeSystem typeSystem = RelDataTypeSystem.DEFAULT;

    private ConfigBuilder() {}

    public FrameworkConfig build() {
      return new StdFrameworkConfig(context, convertletTable, operatorTable,
          programs, traitDefs, lex, defaultSchema, costFactory, parserFactory,
          typeSystem);
    }

    public ConfigBuilder context(Context c) {
      this.context = Preconditions.checkNotNull(c);
      return this;
    }

    public ConfigBuilder convertletTable(
        SqlRexConvertletTable convertletTable) {
      this.convertletTable = Preconditions.checkNotNull(convertletTable);
      return this;
    }

    public ConfigBuilder operatorTable(SqlOperatorTable operatorTable) {
      this.operatorTable = Preconditions.checkNotNull(operatorTable);
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

    public ConfigBuilder lex(Lex lex) {
      this.lex = Preconditions.checkNotNull(lex);
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
      return programs(Programs.listOf(Preconditions.checkNotNull(ruleSets)));
    }

    public ConfigBuilder programs(List<Program> programs) {
      this.programs = ImmutableList.copyOf(programs);
      return this;
    }

    public ConfigBuilder programs(Program... programs) {
      this.programs = ImmutableList.copyOf(programs);
      return this;
    }

    public ConfigBuilder parserFactory(SqlParserImplFactory parserFactory) {
      this.parserFactory = Preconditions.checkNotNull(parserFactory);
      return this;
    }

    public ConfigBuilder typeSystem(RelDataTypeSystem typeSystem) {
      this.typeSystem = Preconditions.checkNotNull(typeSystem);
      return this;
    }
  }

  /**
   * An implementation of {@link FrameworkConfig} that uses standard Optiq
   * classes to provide basic planner functionality.
   */
  static class StdFrameworkConfig implements FrameworkConfig {
    private final Context context;
    private final SqlRexConvertletTable convertletTable;
    private final SqlOperatorTable operatorTable;
    private final ImmutableList<Program> programs;
    private final ImmutableList<RelTraitDef> traitDefs;
    private final Lex lex;
    private final SchemaPlus defaultSchema;
    private final RelOptCostFactory costFactory;
    private final SqlParserImplFactory parserFactory;
    private final RelDataTypeSystem typeSystem;

    public StdFrameworkConfig(Context context,
        SqlRexConvertletTable convertletTable,
        SqlOperatorTable operatorTable,
        ImmutableList<Program> programs,
        ImmutableList<RelTraitDef> traitDefs,
        Lex lex,
        SchemaPlus defaultSchema,
        RelOptCostFactory costFactory,
        SqlParserImplFactory parserFactory,
        RelDataTypeSystem typeSystem) {
      this.context = context;
      this.convertletTable = convertletTable;
      this.operatorTable = operatorTable;
      this.programs = programs;
      this.traitDefs = traitDefs;
      this.lex = lex;
      this.defaultSchema = defaultSchema;
      this.costFactory = costFactory;
      this.parserFactory = parserFactory;
      this.typeSystem = typeSystem;
    }

    public Lex getLex() {
      return lex;
    }

    public SqlParserImplFactory getParserFactory() {
      return parserFactory;
    }

    public SchemaPlus getDefaultSchema() {
      return defaultSchema;
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
  }
}

// End Frameworks.java
