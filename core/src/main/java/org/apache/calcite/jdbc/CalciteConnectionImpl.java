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
package org.apache.calcite.jdbc;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.AvaticaSite;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Helper;
import org.apache.calcite.avatica.InternalProperty;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.linq4j.BaseQueryable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.server.CalciteServer;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.advise.SqlAdvisor;
import org.apache.calcite.sql.advise.SqlAdvisorValidator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorWithHints;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Holder;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

/**
 * Implementation of JDBC connection
 * in the Calcite engine.
 *
 * <p>Abstract to allow newer versions of JDBC to add methods.</p>
 */
abstract class CalciteConnectionImpl
    extends AvaticaConnection
    implements CalciteConnection, QueryProvider {
  public final JavaTypeFactory typeFactory;

  final CalciteRootSchema rootSchema;
  final Function0<CalcitePrepare> prepareFactory;
  final CalciteServer server = new CalciteServerImpl();

  // must be package-protected
  static final Trojan TROJAN = createTrojan();

  /**
   * Creates a CalciteConnectionImpl.
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
  protected CalciteConnectionImpl(Driver driver, AvaticaFactory factory,
      String url, Properties info, CalciteRootSchema rootSchema,
      JavaTypeFactory typeFactory) {
    super(driver, factory, url, info);
    CalciteConnectionConfig cfg = new CalciteConnectionConfigImpl(info);
    this.prepareFactory = driver.prepareFactory;
    if (typeFactory != null) {
      this.typeFactory = typeFactory;
    } else {
      final RelDataTypeSystem typeSystem =
          cfg.typeSystem(RelDataTypeSystem.class, RelDataTypeSystem.DEFAULT);
      this.typeFactory = new JavaTypeFactoryImpl(typeSystem);
    }
    this.rootSchema =
        rootSchema != null ? rootSchema : CalciteSchema.createRootSchema(true);

    this.properties.put(InternalProperty.CASE_SENSITIVE, cfg.caseSensitive());
    this.properties.put(InternalProperty.UNQUOTED_CASING, cfg.unquotedCasing());
    this.properties.put(InternalProperty.QUOTED_CASING, cfg.quotedCasing());
    this.properties.put(InternalProperty.QUOTING, cfg.quoting());
  }

  CalciteMetaImpl meta() {
    return (CalciteMetaImpl) meta;
  }

  public CalciteConnectionConfig config() {
    return new CalciteConnectionConfigImpl(info);
  }

  /** Called after the constructor has completed and the model has been
   * loaded. */
  void init() {
    final MaterializationService service = MaterializationService.instance();
    for (CalciteSchema.LatticeEntry e : Schemas.getLatticeEntries(rootSchema)) {
      final Lattice lattice = e.getLattice();
      for (Lattice.Tile tile : lattice.computeTiles()) {
        service.defineTile(lattice, tile.bitSet(), tile.measures, e.schema,
            true, true);
      }
    }
  }

  @Override public CalciteStatement createStatement(int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return (CalciteStatement) super.createStatement(resultSetType,
        resultSetConcurrency, resultSetHoldability);
  }

  @Override public CalcitePreparedStatement prepareStatement(
      String sql,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    try {
      Meta.Signature signature =
          parseQuery(sql, new ContextImpl(this), -1);
      return (CalcitePreparedStatement) factory.newPreparedStatement(this, null,
          signature, resultSetType, resultSetConcurrency, resultSetHoldability);
    } catch (Exception e) {
      throw Helper.INSTANCE.createException(
          "Error while preparing statement [" + sql + "]", e);
    }
  }

  <T> CalcitePrepare.CalciteSignature<T> parseQuery(String sql,
      CalcitePrepare.Context prepareContext, int maxRowCount) {
    CalcitePrepare.Dummy.push(prepareContext);
    try {
      final CalcitePrepare prepare = prepareFactory.apply();
      return prepare.prepareSql(prepareContext, sql, null, Object[].class,
          maxRowCount);
    } finally {
      CalcitePrepare.Dummy.pop(prepareContext);
    }
  }

  // CalciteConnection methods

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
    return new CalciteQueryable<>(this, rowType, expression);
  }

  public <T> Queryable<T> createQuery(Expression expression, Type rowType) {
    return new CalciteQueryable<>(this, rowType, expression);
  }

  public <T> T execute(Expression expression, Type type) {
    return null; // TODO:
  }

  public <T> T execute(Expression expression, Class<T> type) {
    return null; // TODO:
  }

  public <T> Enumerator<T> executeQuery(Queryable<T> queryable) {
    try {
      CalciteStatement statement = (CalciteStatement) createStatement();
      CalcitePrepare.CalciteSignature<T> signature =
          statement.prepare(queryable);
      return enumerable(statement.handle, signature).enumerator();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public <T> Enumerable<T> enumerable(Meta.StatementHandle handle,
      CalcitePrepare.CalciteSignature<T> signature) throws SQLException {
    Map<String, Object> map = Maps.newLinkedHashMap();
    AvaticaStatement statement = lookupStatement(handle);
    final List<TypedValue> parameterValues =
        TROJAN.getParameterValues(statement);
    for (Ord<TypedValue> o : Ord.zip(parameterValues)) {
      map.put("?" + o.i, o.e.toLocal());
    }
    map.putAll(signature.internalParameters);
    final DataContext dataContext = createDataContext(map);
    return signature.enumerable(dataContext);
  }

  public DataContext createDataContext(Map<String, Object> parameterValues) {
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
  static class CalciteQueryable<T> extends BaseQueryable<T> {
    public CalciteQueryable(CalciteConnection connection, Type elementType,
        Expression expression) {
      super(connection, elementType, expression);
    }

    public CalciteConnection getConnection() {
      return (CalciteConnection) provider;
    }
  }

  /** Implementation of Server. */
  private static class CalciteServerImpl implements CalciteServer {
    final Map<Integer, CalciteServerStatement> statementMap = Maps.newHashMap();

    public void removeStatement(Meta.StatementHandle h) {
      statementMap.remove(h.id);
    }

    public void addStatement(CalciteConnection connection,
        Meta.StatementHandle h) {
      final CalciteConnectionImpl c = (CalciteConnectionImpl) connection;
      statementMap.put(h.id, new CalciteServerStatementImpl(c));
    }

    public CalciteServerStatement getStatement(Meta.StatementHandle h) {
      return statementMap.get(h.id);
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
          BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method);
    }
  }

  /** Implementation of DataContext. */
  static class DataContextImpl implements DataContext {
    private final ImmutableMap<Object, Object> map;
    private final CalciteSchema rootSchema;
    private final QueryProvider queryProvider;
    private final JavaTypeFactory typeFactory;

    DataContextImpl(CalciteConnectionImpl connection,
        Map<String, Object> parameters) {
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
      for (Map.Entry<String, Object> entry : parameters.entrySet()) {
        Object e = entry.getValue();
        if (e == null) {
          e = AvaticaSite.DUMMY_VALUE;
        }
        builder.put(entry.getKey(), e);
      }
      map = builder.build();
    }

    public synchronized Object get(String name) {
      Object o = map.get(name);
      if (o == AvaticaSite.DUMMY_VALUE) {
        return null;
      }
      if (o == null && Variable.SQL_ADVISOR.camelName.equals(name)) {
        return getSqlAdvisor();
      }
      return o;
    }

    private SqlAdvisor getSqlAdvisor() {
      final CalciteConnectionImpl con = (CalciteConnectionImpl) queryProvider;
      final String schemaName = con.getSchema();
      final List<String> schemaPath =
          schemaName == null
              ? ImmutableList.<String>of()
              : ImmutableList.of(schemaName);
      final SqlValidatorWithHints validator =
          new SqlAdvisorValidator(SqlStdOperatorTable.instance(),
          new CalciteCatalogReader(rootSchema, con.config().caseSensitive(),
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
  static class ContextImpl implements CalcitePrepare.Context {
    private final CalciteConnectionImpl connection;

    public ContextImpl(CalciteConnectionImpl connection) {
      this.connection = Preconditions.checkNotNull(connection);
    }

    public JavaTypeFactory getTypeFactory() {
      return connection.typeFactory;
    }

    public CalciteRootSchema getRootSchema() {
      return connection.rootSchema;
    }

    public List<String> getDefaultSchemaPath() {
      final String schemaName = connection.getSchema();
      return schemaName == null
          ? ImmutableList.<String>of()
          : ImmutableList.of(schemaName);
    }

    public CalciteConnectionConfig config() {
      return connection.config();
    }

    public DataContext getDataContext() {
      return connection.createDataContext(ImmutableMap.<String, Object>of());
    }

    public CalcitePrepare.SparkHandler spark() {
      final boolean enable = config().spark();
      return CalcitePrepare.Dummy.getSparkHandler(enable);
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

  /** Implementation of {@link CalciteServerStatement}. */
  static class CalciteServerStatementImpl
      implements CalciteServerStatement {
    private final CalciteConnectionImpl connection;
    private Iterator<Object> iterator;
    private Meta.Signature signature;

    public CalciteServerStatementImpl(CalciteConnectionImpl connection) {
      this.connection = Preconditions.checkNotNull(connection);
    }

    public ContextImpl createPrepareContext() {
      return new ContextImpl(connection);
    }

    public CalciteConnection getConnection() {
      return connection;
    }

    public void setSignature(Meta.Signature signature) {
      this.signature = signature;
    }

    public Meta.Signature getSignature() {
      return signature;
    }

    public Iterator<Object> getResultSet() {
      return iterator;
    }

    public void setResultSet(Iterator<Object> iterator) {
      this.iterator = iterator;
    }
  }
}

// End CalciteConnectionImpl.java
