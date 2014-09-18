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
package net.hydromatic.optiq.jdbc;

import net.hydromatic.avatica.*;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.linq4j.function.Function0;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.config.OptiqConnectionConfig;
import net.hydromatic.optiq.config.OptiqConnectionConfigImpl;
import net.hydromatic.optiq.impl.AbstractSchema;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.materialize.Lattice;
import net.hydromatic.optiq.materialize.MaterializationService;
import net.hydromatic.optiq.prepare.OptiqCatalogReader;
import net.hydromatic.optiq.runtime.Hook;
import net.hydromatic.optiq.server.OptiqServer;
import net.hydromatic.optiq.server.OptiqServerStatement;

import org.eigenbase.reltype.RelDataTypeSystem;
import org.eigenbase.sql.advise.SqlAdvisor;
import org.eigenbase.sql.advise.SqlAdvisorValidator;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.validate.SqlConformance;
import org.eigenbase.sql.validate.SqlValidatorWithHints;
import org.eigenbase.util.Holder;

import com.google.common.collect.*;

import java.io.Serializable;
import java.lang.reflect.*;
import java.sql.*;
import java.util.*;

/**
 * Implementation of JDBC connection
 * in the Optiq engine.
 *
 * <p>Abstract to allow newer versions of JDBC to add methods.</p>
 */
abstract class OptiqConnectionImpl
    extends AvaticaConnection
    implements OptiqConnection, QueryProvider {
  public final JavaTypeFactory typeFactory;

  final OptiqRootSchema rootSchema;
  final Function0<OptiqPrepare> prepareFactory;
  final OptiqServer server = new OptiqServerImpl();

  // must be package-protected
  static final Trojan TROJAN = createTrojan();

  /**
   * Creates an OptiqConnectionImpl.
   *
   * <p>Not public; method is called only from the driver.</p>
   *
   * @param driver Driver
   * @param factory Factory for JDBC objects
   * @param url Server URL
   * @param info Other connection properties
   * @param rootSchema Root schema, or null
   * @param typeFactory Type factory, or null
   */
  protected OptiqConnectionImpl(Driver driver, AvaticaFactory factory,
      String url, Properties info, OptiqRootSchema rootSchema,
      JavaTypeFactory typeFactory) {
    super(driver, factory, url, info);
    OptiqConnectionConfig cfg = new OptiqConnectionConfigImpl(info);
    this.prepareFactory = driver.prepareFactory;
    if (typeFactory != null) {
      this.typeFactory = typeFactory;
    } else {
      final RelDataTypeSystem typeSystem =
          cfg.typeSystem(RelDataTypeSystem.class, RelDataTypeSystem.DEFAULT);
      this.typeFactory = new JavaTypeFactoryImpl(typeSystem);
    }
    this.rootSchema =
        rootSchema != null ? rootSchema : OptiqSchema.createRootSchema(true);

    this.properties.put(InternalProperty.CASE_SENSITIVE, cfg.caseSensitive());
    this.properties.put(InternalProperty.UNQUOTED_CASING, cfg.unquotedCasing());
    this.properties.put(InternalProperty.QUOTED_CASING, cfg.quotedCasing());
    this.properties.put(InternalProperty.QUOTING, cfg.quoting());
  }

  @Override protected Meta createMeta() {
    return new MetaImpl(this);
  }

  MetaImpl meta() {
    return (MetaImpl) meta;
  }

  public OptiqConnectionConfig config() {
    return new OptiqConnectionConfigImpl(info);
  }

  /** Called after the constructor has completed and the model has been
   * loaded. */
  void init() {
    final MaterializationService service = MaterializationService.instance();
    for (OptiqSchema.LatticeEntry e : Schemas.getLatticeEntries(rootSchema)) {
      final Lattice lattice = e.getLattice();
      for (Lattice.Tile tile : lattice.tiles) {
        service.defineTile(lattice, tile.bitSet(), tile.measures, e.schema,
            true);
      }
    }
  }

  @Override public AvaticaStatement createStatement(int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    OptiqStatement statement =
        (OptiqStatement) super.createStatement(
            resultSetType, resultSetConcurrency, resultSetHoldability);
    server.addStatement(statement);
    return statement;
  }

  @Override public PreparedStatement prepareStatement(
      String sql,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    try {
      AvaticaPrepareResult prepareResult =
          parseQuery(sql, new ContextImpl(this), -1);
      OptiqPreparedStatement statement =
          (OptiqPreparedStatement) factory.newPreparedStatement(
              this,
              prepareResult,
              resultSetType,
              resultSetConcurrency,
              resultSetHoldability);
      server.addStatement(statement);
      return statement;
    } catch (RuntimeException e) {
      throw Helper.INSTANCE.createException(
          "Error while preparing statement [" + sql + "]", e);
    } catch (Exception e) {
      throw Helper.INSTANCE.createException(
          "Error while preparing statement [" + sql + "]", e);
    }
  }

  <T> OptiqPrepare.PrepareResult<T> parseQuery(String sql,
      OptiqPrepare.Context prepareContext, int maxRowCount) {
    OptiqPrepare.Dummy.push(prepareContext);
    try {
      final OptiqPrepare prepare = prepareFactory.apply();
      return prepare.prepareSql(prepareContext, sql, null, Object[].class,
          maxRowCount);
    } finally {
      OptiqPrepare.Dummy.pop(prepareContext);
    }
  }

  // OptiqConnection methods

  public SchemaPlus getRootSchema() {
    return rootSchema.plus();
  }

  public JavaTypeFactory getTypeFactory() {
    return typeFactory;
  }

  public Properties getProperties() {
    return info;
  }

  // QueryProvider methods

  public <T> Queryable<T> createQuery(
      Expression expression, Class<T> rowType) {
    return new OptiqQueryable<T>(this, rowType, expression);
  }

  public <T> Queryable<T> createQuery(Expression expression, Type rowType) {
    return new OptiqQueryable<T>(this, rowType, expression);
  }

  public <T> T execute(Expression expression, Type type) {
    return null; // TODO:
  }

  public <T> T execute(Expression expression, Class<T> type) {
    return null; // TODO:
  }

  public <T> Enumerator<T> executeQuery(Queryable<T> queryable) {
    try {
      OptiqStatement statement = (OptiqStatement) createStatement();
      OptiqPrepare.PrepareResult<T> enumerable =
          statement.prepare(queryable);
      final DataContext dataContext =
          createDataContext(Collections.emptyList());
      return enumerable.enumerator(dataContext);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public DataContext createDataContext(List<Object> parameterValues) {
    if (config().spark()) {
      return new SlimDataContext();
    }
    return new DataContextImpl(this, parameterValues);
  }

  // do not make public
  UnregisteredDriver getDriver() {
    return driver;
  }

  // do not make public
  AvaticaFactory getFactory() {
    return factory;
  }

  /** Implementation of Queryable. */
  static class OptiqQueryable<T>
      extends BaseQueryable<T> {
    public OptiqQueryable(
        OptiqConnection connection, Type elementType, Expression expression) {
      super(connection, elementType, expression);
    }

    public OptiqConnection getConnection() {
      return (OptiqConnection) provider;
    }
  }

  /** Implementation of Server. */
  private static class OptiqServerImpl implements OptiqServer {
    final List<OptiqServerStatement> statementList =
        new ArrayList<OptiqServerStatement>();

    public void removeStatement(OptiqServerStatement optiqServerStatement) {
      statementList.add(optiqServerStatement);
    }

    public void addStatement(OptiqServerStatement statement) {
      statementList.add(statement);
    }
  }

  /** Schema that has no parents. */
  static class RootSchema extends AbstractSchema {
    RootSchema() {
      super();
    }

    @Override public Expression getExpression(SchemaPlus parentSchema,
        String name) {
      return Expressions.call(
          DataContext.ROOT,
          BuiltinMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method);
    }
  }

  /** Implementation of DataContext. */
  static class DataContextImpl implements DataContext {
    private final ImmutableMap<Object, Object> map;
    private final OptiqSchema rootSchema;
    private final QueryProvider queryProvider;
    private final JavaTypeFactory typeFactory;

    DataContextImpl(OptiqConnectionImpl connection,
        List<Object> parameterValues) {
      this.queryProvider = connection;
      this.typeFactory = connection.getTypeFactory();
      this.rootSchema = connection.rootSchema;

      // Store the time at which the query started executing. The SQL
      // standard says that functions such as CURRENT_TIMESTAMP return the
      // same value throughout the query.
      final Holder<Long> timeHolder = Holder.of(System.currentTimeMillis());

      // Give a hook chance to alter the clock.
      Hook.CURRENT_TIME.run(timeHolder);
      final long time = timeHolder.get();
      final TimeZone timeZone = connection.getTimeZone();
      final long localOffset = timeZone.getOffset(time);
      final long currentOffset = localOffset;

      ImmutableMap.Builder<Object, Object> builder = ImmutableMap.builder();
      builder.put(Variable.UTC_TIMESTAMP.camelName, time)
          .put(Variable.CURRENT_TIMESTAMP.camelName, time + currentOffset)
          .put(Variable.LOCAL_TIMESTAMP.camelName, time + localOffset)
          .put(Variable.TIME_ZONE.camelName, timeZone);
      for (Ord<Object> value : Ord.zip(parameterValues)) {
        Object e = value.e;
        if (e == null) {
          e = AvaticaParameter.DUMMY_VALUE;
        }
        builder.put("?" + value.i, e);
      }
      map = builder.build();
    }

    public synchronized Object get(String name) {
      Object o = map.get(name);
      if (o == AvaticaParameter.DUMMY_VALUE) {
        return null;
      }
      if (o == null && Variable.SQL_ADVISOR.camelName.equals(name)) {
        return getSqlAdvisor();
      }
      return o;
    }

    private SqlAdvisor getSqlAdvisor() {
      final OptiqConnectionImpl con = (OptiqConnectionImpl) queryProvider;
      final String schemaName = con.getSchema();
      final List<String> schemaPath =
          schemaName == null
              ? ImmutableList.<String>of()
              : ImmutableList.of(schemaName);
      final SqlValidatorWithHints validator =
          new SqlAdvisorValidator(SqlStdOperatorTable.instance(),
          new OptiqCatalogReader(rootSchema, con.config().caseSensitive(),
              schemaPath, typeFactory),
          typeFactory, SqlConformance.DEFAULT);
      return new SqlAdvisor(validator);
    }

    public SchemaPlus getRootSchema() {
      return rootSchema.plus();
    }

    public JavaTypeFactory getTypeFactory() {
      return typeFactory;
    }

    public QueryProvider getQueryProvider() {
      return queryProvider;
    }
  }

  /** Implementation of Context. */
  static class ContextImpl implements OptiqPrepare.Context {
    private final OptiqConnectionImpl connection;

    public ContextImpl(OptiqConnectionImpl connection) {
      this.connection = connection;
    }

    public JavaTypeFactory getTypeFactory() {
      return connection.typeFactory;
    }

    public OptiqRootSchema getRootSchema() {
      return connection.rootSchema;
    }

    public List<String> getDefaultSchemaPath() {
      final String schemaName = connection.getSchema();
      return schemaName == null
          ? Collections.<String>emptyList()
          : Collections.singletonList(schemaName);
    }

    public OptiqConnectionConfig config() {
      return connection.config();
    }

    public DataContext getDataContext() {
      return connection.createDataContext(ImmutableList.of());
    }

    public OptiqPrepare.SparkHandler spark() {
      final boolean enable = config().spark();
      return OptiqPrepare.Dummy.getSparkHandler(enable);
    }
  }

  /** Implementation of {@link DataContext} that has few variables and is
   * {@link Serializable}. For Spark. */
  private static class SlimDataContext implements DataContext, Serializable {
    public SchemaPlus getRootSchema() {
      return null;
    }

    public JavaTypeFactory getTypeFactory() {
      return null;
    }

    public QueryProvider getQueryProvider() {
      return null;
    }

    public Object get(String name) {
      return null;
    }
  }

}

// End OptiqConnectionImpl.java
