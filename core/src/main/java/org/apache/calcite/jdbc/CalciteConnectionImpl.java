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
import org.apache.calcite.DataContexts;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.AvaticaSite;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Helper;
import org.apache.calcite.avatica.InternalProperty;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalcitePrepare.Context;
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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.DelegatingTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.LongSchemaVersion;
import org.apache.calcite.server.CalciteServer;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.advise.SqlAdvisor;
import org.apache.calcite.sql.advise.SqlAdvisorValidator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorWithHints;
import org.apache.calcite.tools.RelRunner;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.Util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Type;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of JDBC connection
 * in the Calcite engine.
 *
 * <p>Abstract to allow newer versions of JDBC to add methods.
 */
abstract class CalciteConnectionImpl
    extends AvaticaConnection
    implements CalciteConnection, QueryProvider {
  public final JavaTypeFactory typeFactory;

  final CalciteSchema rootSchema;
  final Function0<CalcitePrepare> prepareFactory;
  final CalciteServer server = new CalciteServerImpl();

  // must be package-protected
  static final Trojan TROJAN = createTrojan();

  /**
   * Creates a CalciteConnectionImpl.
   *
   * <p>Not public; method is called only from the driver.
   *
   * @param driver Driver
   * @param factory Factory for JDBC objects
   * @param url Server URL
   * @param info Other connection properties
   * @param rootSchema Root schema, or null
   * @param typeFactory Type factory, or null
   */
  protected CalciteConnectionImpl(Driver driver, AvaticaFactory factory,
      String url, Properties info, @Nullable CalciteSchema rootSchema,
      @Nullable JavaTypeFactory typeFactory) {
    super(driver, factory, url, info);
    CalciteConnectionConfig cfg = new CalciteConnectionConfigImpl(info);
    this.prepareFactory = driver.prepareFactory;
    if (typeFactory != null) {
      this.typeFactory = typeFactory;
    } else {
      RelDataTypeSystem typeSystem =
          cfg.typeSystem(RelDataTypeSystem.class, RelDataTypeSystem.DEFAULT);
      if (cfg.conformance().shouldConvertRaggedUnionTypesToVarying()) {
        typeSystem =
            new DelegatingTypeSystem(typeSystem) {
              @Override public boolean
              shouldConvertRaggedUnionTypesToVarying() {
                return true;
              }
            };
      }
      this.typeFactory = new JavaTypeFactoryImpl(typeSystem);
    }
    this.rootSchema =
        requireNonNull(rootSchema != null
            ? rootSchema
            : CalciteSchema.createRootSchema(true));
    Preconditions.checkArgument(this.rootSchema.isRoot(), "must be root schema");
    this.properties.put(InternalProperty.CASE_SENSITIVE, cfg.caseSensitive());
    this.properties.put(InternalProperty.UNQUOTED_CASING, cfg.unquotedCasing());
    this.properties.put(InternalProperty.QUOTED_CASING, cfg.quotedCasing());
    this.properties.put(InternalProperty.QUOTING, cfg.quoting());
  }

  CalciteMetaImpl meta() {
    return (CalciteMetaImpl) meta;
  }

  @Override public CalciteConnectionConfig config() {
    return new CalciteConnectionConfigImpl(info);
  }

  @Override public Context createPrepareContext() {
    return new ContextImpl(this);
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

  @Override public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface == RelRunner.class) {
      return iface.cast(new RelRunner() {
        @Override public PreparedStatement prepareStatement(RelNode rel)
            throws SQLException {
            return prepareStatement_(CalcitePrepare.Query.of(rel),
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                getHoldability());
        }

        @SuppressWarnings("deprecation")
        @Override public PreparedStatement prepare(RelNode rel) {
          try {
            return prepareStatement(rel);
          } catch (SQLException e) {
            throw Util.throwAsRuntime(e);
          }
        }
      });
    }
    return super.unwrap(iface);
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
    final CalcitePrepare.Query<Object> query = CalcitePrepare.Query.of(sql);
    return prepareStatement_(query, resultSetType, resultSetConcurrency,
        resultSetHoldability);
  }

  private CalcitePreparedStatement prepareStatement_(
      CalcitePrepare.Query<?> query,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    try {
      final Meta.Signature signature =
          parseQuery(query, createPrepareContext(), -1);
      final CalcitePreparedStatement calcitePreparedStatement =
          (CalcitePreparedStatement) factory.newPreparedStatement(this, null,
              signature, resultSetType, resultSetConcurrency, resultSetHoldability);
      server.getStatement(calcitePreparedStatement.handle).setSignature(signature);
      return calcitePreparedStatement;
    } catch (Exception e) {
      String message = query.rel == null
          ? "Error while preparing statement [" + query.sql + "]"
          : "Error while preparing plan [" + RelOptUtil.toString(query.rel) + "]";
      throw Helper.INSTANCE.createException(message, e);
    }
  }

  <T> CalcitePrepare.CalciteSignature<T> parseQuery(
      CalcitePrepare.Query<T> query,
      CalcitePrepare.Context prepareContext, long maxRowCount) {
    CalcitePrepare.Dummy.push(prepareContext);
    try {
      final CalcitePrepare prepare = prepareFactory.apply();
      return prepare.prepareSql(prepareContext, query, Object[].class,
          maxRowCount);
    } finally {
      CalcitePrepare.Dummy.pop(prepareContext);
    }
  }

  @Override public AtomicBoolean getCancelFlag(Meta.StatementHandle handle)
      throws NoSuchStatementException {
    final CalciteServerStatement serverStatement = server.getStatement(handle);
    return ((CalciteServerStatementImpl) serverStatement).cancelFlag;
  }

  // CalciteConnection methods

  @Override public SchemaPlus getRootSchema() {
    return rootSchema.plus();
  }

  @Override public JavaTypeFactory getTypeFactory() {
    return typeFactory;
  }

  @Override public Properties getProperties() {
    return info;
  }

  // QueryProvider methods

  @Override public <T> Queryable<T> createQuery(
      Expression expression, Class<T> rowType) {
    return new CalciteQueryable<>(this, rowType, expression);
  }

  @Override public <T> Queryable<T> createQuery(Expression expression, Type rowType) {
    return new CalciteQueryable<>(this, rowType, expression);
  }

  @Override public <T> T execute(Expression expression, Type type) {
    return castNonNull(null); // TODO:
  }

  @Override public <T> T execute(Expression expression, Class<T> type) {
    return castNonNull(null); // TODO:
  }

  @Override public <T> Enumerator<T> executeQuery(Queryable<T> queryable) {
    try {
      CalciteStatement statement = (CalciteStatement) createStatement();
      CalcitePrepare.CalciteSignature<T> signature =
          statement.prepare(queryable);
      return enumerable(statement.handle, signature, null).enumerator();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public <T> Enumerable<T> enumerable(Meta.StatementHandle handle,
      CalcitePrepare.CalciteSignature<T> signature,
      @Nullable List<TypedValue> parameterValues0) throws SQLException {
    Map<String, Object> map = new LinkedHashMap<>();
    AvaticaStatement statement = lookupStatement(handle);
    final List<TypedValue> parameterValues;
    if (parameterValues0 == null || parameterValues0.isEmpty()) {
      parameterValues = TROJAN.getParameterValues(statement);
    } else {
      parameterValues = parameterValues0;
    }

    if (MetaImpl.checkParameterValueHasNull(parameterValues)) {
      throw new SQLException("exception while executing query: unbound parameter");
    }

    Ord.forEach(parameterValues,
        (e, i) -> map.put("?" + i, e.toLocal()));
    map.putAll(signature.internalParameters);
    final AtomicBoolean cancelFlag;
    try {
      cancelFlag = getCancelFlag(handle);
    } catch (NoSuchStatementException e) {
      throw new RuntimeException(e);
    }
    map.put(DataContext.Variable.CANCEL_FLAG.camelName, cancelFlag);
    int queryTimeout = statement.getQueryTimeout();
    // Avoid overflow
    if (queryTimeout > 0 && queryTimeout < Integer.MAX_VALUE / 1000) {
      map.put(DataContext.Variable.TIMEOUT.camelName, queryTimeout * 1000L);
    }
    final DataContext dataContext = createDataContext(map, signature.rootSchema);
    return signature.enumerable(dataContext);
  }

  public DataContext createDataContext(Map<String, Object> parameterValues,
      @Nullable CalciteSchema rootSchema) {
    if (config().spark()) {
      return DataContexts.EMPTY;
    }
    return new DataContextImpl(this, parameterValues, rootSchema);
  }

  // do not make public
  UnregisteredDriver getDriver() {
    return driver;
  }

  // do not make public
  AvaticaFactory getFactory() {
    return factory;
  }

  /** Implementation of Queryable.
   *
   * @param <T> element type */
  static class CalciteQueryable<T> extends BaseQueryable<T> {
    CalciteQueryable(CalciteConnection connection, Type elementType,
        Expression expression) {
      super(connection, elementType, expression);
    }

    public CalciteConnection getConnection() {
      return (CalciteConnection) provider;
    }
  }

  /** Implementation of Server. */
  private static class CalciteServerImpl implements CalciteServer {
    final Map<Integer, CalciteServerStatement> statementMap = new HashMap<>();

    @Override public void removeStatement(Meta.StatementHandle h) {
      statementMap.remove(h.id);
    }

    @Override public void addStatement(CalciteConnection connection,
        Meta.StatementHandle h) {
      final CalciteConnectionImpl c = (CalciteConnectionImpl) connection;
      final CalciteServerStatement previous =
          statementMap.put(h.id, new CalciteServerStatementImpl(c));
      if (previous != null) {
        throw new AssertionError();
      }
    }

    @Override public CalciteServerStatement getStatement(Meta.StatementHandle h)
        throws NoSuchStatementException {
      CalciteServerStatement statement = statementMap.get(h.id);
      if (statement == null) {
        throw new NoSuchStatementException(h);
      }
      return statement;
    }
  }

  /** Schema that has no parents. */
  static class RootSchema extends AbstractSchema {
    RootSchema() {
      super();
    }

    @Override public Expression getExpression(@Nullable SchemaPlus parentSchema,
        String name) {
      return Expressions.call(
          DataContext.ROOT,
          BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method);
    }
  }

  /** Implementation of DataContext. */
  static class DataContextImpl implements DataContext {
    private final ImmutableMap<Object, Object> map;
    private final @Nullable CalciteSchema rootSchema;
    private final QueryProvider queryProvider;
    private final JavaTypeFactory typeFactory;

    DataContextImpl(CalciteConnectionImpl connection,
        Map<String, Object> parameters, @Nullable CalciteSchema rootSchema) {
      this.queryProvider = connection;
      this.typeFactory = connection.getTypeFactory();
      this.rootSchema = rootSchema;

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
      final String user = "sa";
      final String systemUser = System.getProperty("user.name");
      final String localeName = connection.config().locale();
      final Locale locale = localeName != null
          ? Util.parseLocale(localeName) : Locale.ROOT;

      // Give a hook chance to alter standard input, output, error streams.
      final Holder<Object[]> streamHolder =
          Holder.of(new Object[] {System.in, System.out, System.err});
      Hook.STANDARD_STREAMS.run(streamHolder);

      ImmutableMap.Builder<Object, Object> builder = ImmutableMap.builder();
      builder.put(Variable.UTC_TIMESTAMP.camelName, time)
          .put(Variable.CURRENT_TIMESTAMP.camelName, time + currentOffset)
          .put(Variable.LOCAL_TIMESTAMP.camelName, time + localOffset)
          .put(Variable.TIME_ZONE.camelName, timeZone)
          .put(Variable.USER.camelName, user)
          .put(Variable.SYSTEM_USER.camelName, systemUser)
          .put(Variable.LOCALE.camelName, locale)
          .put(Variable.STDIN.camelName, streamHolder.get()[0])
          .put(Variable.STDOUT.camelName, streamHolder.get()[1])
          .put(Variable.STDERR.camelName, streamHolder.get()[2]);
      for (Map.Entry<String, Object> entry : parameters.entrySet()) {
        Object e = entry.getValue();
        if (e == null) {
          e = AvaticaSite.DUMMY_VALUE;
        }
        builder.put(entry.getKey(), e);
      }
      map = builder.build();
    }

    @Override public synchronized @Nullable Object get(String name) {
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
      final String schemaName;
      try {
        schemaName = con.getSchema();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
      final List<String> schemaPath =
          schemaName == null
              ? ImmutableList.of()
              : ImmutableList.of(schemaName);
      final SqlValidatorWithHints validator =
          new SqlAdvisorValidator(SqlStdOperatorTable.instance(),
              new CalciteCatalogReader(requireNonNull(rootSchema, "rootSchema"),
                  schemaPath, typeFactory, con.config()),
              typeFactory, SqlValidator.Config.DEFAULT);
      final CalciteConnectionConfig config = con.config();
      // This duplicates org.apache.calcite.prepare.CalcitePrepareImpl.prepare2_
      final SqlParser.Config parserConfig = SqlParser.config()
          .withQuotedCasing(config.quotedCasing())
          .withUnquotedCasing(config.unquotedCasing())
          .withQuoting(config.quoting())
          .withConformance(config.conformance())
          .withCaseSensitive(config.caseSensitive());
      return new SqlAdvisor(validator, parserConfig);
    }

    @Override public @Nullable SchemaPlus getRootSchema() {
      return rootSchema == null ? null : rootSchema.plus();
    }

    @Override public JavaTypeFactory getTypeFactory() {
      return typeFactory;
    }

    @Override public QueryProvider getQueryProvider() {
      return queryProvider;
    }
  }

  /** Implementation of Context. */
  static class ContextImpl implements CalcitePrepare.Context {
    private final CalciteConnectionImpl connection;
    private final CalciteSchema mutableRootSchema;
    private final CalciteSchema rootSchema;

    ContextImpl(CalciteConnectionImpl connection) {
      this.connection = requireNonNull(connection, "connection");
      long now = System.currentTimeMillis();
      SchemaVersion schemaVersion = new LongSchemaVersion(now);
      this.mutableRootSchema = connection.rootSchema;
      this.rootSchema = mutableRootSchema.createSnapshot(schemaVersion);
    }

    @Override public JavaTypeFactory getTypeFactory() {
      return connection.typeFactory;
    }

    @Override public CalciteSchema getRootSchema() {
      return rootSchema;
    }

    @Override public CalciteSchema getMutableRootSchema() {
      return mutableRootSchema;
    }

    @Override public List<String> getDefaultSchemaPath() {
      final String schemaName;
      try {
        schemaName = connection.getSchema();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
      return schemaName == null
          ? ImmutableList.of()
          : ImmutableList.of(schemaName);
    }

    @Override public @Nullable List<String> getObjectPath() {
      return null;
    }

    @Override public CalciteConnectionConfig config() {
      return connection.config();
    }

    @Override public DataContext getDataContext() {
      return connection.createDataContext(ImmutableMap.of(),
          rootSchema);
    }

    @Override public RelRunner getRelRunner() {
      final RelRunner runner;
      try {
        runner = connection.unwrap(RelRunner.class);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
      if (runner == null) {
        throw new UnsupportedOperationException();
      }
      return runner;
    }

    @Override public CalcitePrepare.SparkHandler spark() {
      final boolean enable = config().spark();
      return CalcitePrepare.Dummy.getSparkHandler(enable);
    }
  }

  /** Implementation of {@link CalciteServerStatement}. */
  static class CalciteServerStatementImpl
      implements CalciteServerStatement {
    private final CalciteConnectionImpl connection;
    private @Nullable Iterator<Object> iterator;
    private Meta.@Nullable Signature signature;
    private final AtomicBoolean cancelFlag = new AtomicBoolean();

    CalciteServerStatementImpl(CalciteConnectionImpl connection) {
      this.connection = requireNonNull(connection, "connection");
    }

    @Override public Context createPrepareContext() {
      return connection.createPrepareContext();
    }

    @Override public CalciteConnection getConnection() {
      return connection;
    }

    @Override public void setSignature(Meta.Signature signature) {
      this.signature = signature;
    }

    @Override public Meta.@Nullable Signature getSignature() {
      return signature;
    }

    @Override public @Nullable Iterator<Object> getResultSet() {
      return iterator;
    }

    @Override public void setResultSet(Iterator<Object> iterator) {
      this.iterator = iterator;
    }
  }

}
