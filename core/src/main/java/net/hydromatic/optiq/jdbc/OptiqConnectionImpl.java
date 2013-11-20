/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.jdbc;

import net.hydromatic.avatica.*;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.linq4j.expressions.ParameterExpression;
import net.hydromatic.linq4j.function.Function0;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.impl.java.MapSchema;
import net.hydromatic.optiq.server.OptiqServer;
import net.hydromatic.optiq.server.OptiqServerStatement;

import com.google.common.collect.ImmutableMap;

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
  public final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();

  final ParameterExpression rootExpression =
      Expressions.parameter(DataContext.class, "root");
  final Expression rootSchemaExpression =
      Expressions.call(rootExpression,
          BuiltinMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method);
  final MutableSchema rootSchema;
  final Function0<OptiqPrepare> prepareFactory;
  final OptiqServer server = new OptiqServerImpl();

  /**
   * Creates an OptiqConnectionImpl.
   *
   * <p>Not public; method is called only from the driver.</p>
   *
   * @param driver Driver
   * @param factory Factory for JDBC objects
   * @param url Server URL
   * @param info Other connection properties
   */
  OptiqConnectionImpl(
      Driver driver,
      AvaticaFactory factory,
      String url,
      Properties info) {
    super(driver, factory, url, info);
    this.prepareFactory = driver.prepareFactory;
    this.rootSchema = new RootSchema(this);
    ((MetaImpl) meta).createInformationSchema();
  }

  @Override protected Meta createMeta() {
    return new MetaImpl(this);
  }

  public ConnectionConfig config() {
    return connectionConfig(info);
  }

  public static ConnectionConfig connectionConfig(final Properties properties) {
    return new ConnectionConfig() {
      public boolean autoTemp() {
        return ConnectionProperty.AUTO_TEMP.getBoolean(properties);
      }

      public boolean materializationsEnabled() {
        return ConnectionProperty.MATERIALIZATIONS_ENABLED
            .getBoolean(properties);
      }

      public String model() {
        return ConnectionProperty.MODEL.getString(properties);
      }

      public String schema() {
        return ConnectionProperty.SCHEMA.getString(properties);
      }

      public boolean spark() {
        return ConnectionProperty.SPARK.getBoolean(properties);
      }
    };
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
    final OptiqPrepare prepare = prepareFactory.apply();
    return prepare.prepareSql(prepareContext, sql, null, Object[].class,
        maxRowCount);
  }

  // OptiqConnection methods

  public MutableSchema getRootSchema() {
    return rootSchema;
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
  @Override
  public TimeZone getTimeZone() {
    final String timeZoneName =
        ConnectionProperty.TIMEZONE.getString(info);
    return timeZoneName == null
        ? TimeZone.getDefault()
        : TimeZone.getTimeZone(timeZoneName);
  }

  public DataContext createDataContext(List<Object> parameterValues) {
    return new DataContextImpl(this, (RootSchema) rootSchema, parameterValues);
  }

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

  private static class RootSchema extends MapSchema {
    RootSchema(OptiqConnectionImpl connection) {
      super(
          null,
          connection,
          connection.typeFactory,
          "",
          connection.rootSchemaExpression);
    }
  }

  static class DataContextImpl implements DataContext {
    private final ImmutableMap<Object, Object> map;
    private final RootSchema rootSchema;

    DataContextImpl(OptiqConnectionImpl connection, RootSchema rootSchema,
        List<Object> parameterValues) {
      this.rootSchema = rootSchema;

      // Store the time at which the query started executing. The SQL
      // standard says that functions such as CURRENTTIMESTAMP return the
      // same value throughout the query.
      final long time = System.currentTimeMillis();
      final TimeZone timeZone = connection.getTimeZone();
      final long localOffset = timeZone.getOffset(time);
      final long currentOffset = localOffset;

      ImmutableMap.Builder<Object, Object> builder = ImmutableMap.builder();
      builder.put("utcTimestamp", time)
          .put("currentTimestamp", time + currentOffset)
          .put("localTimestamp", time + localOffset)
          .put("timeZone", timeZone);
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
      return o;
    }

    public Schema getRootSchema() {
      return rootSchema;
    }

    public JavaTypeFactory getTypeFactory() {
      return rootSchema.getTypeFactory();
    }
  }

  static class ContextImpl implements OptiqPrepare.Context {
    private final OptiqConnectionImpl connection;

    public ContextImpl(OptiqConnectionImpl connection) {
      this.connection = connection;
    }

    public JavaTypeFactory getTypeFactory() {
      return connection.typeFactory;
    }

    public Schema getRootSchema() {
      return connection.getRootSchema();
    }

    public List<String> getDefaultSchemaPath() {
      final String schemaName = connection.getSchema();
      return schemaName == null
          ? Collections.<String>emptyList()
          : Collections.singletonList(schemaName);
    }

    public ConnectionConfig config() {
      return connection.config();
    }

    public OptiqPrepare.SparkHandler spark() {
      return OptiqPrepare.Dummy.getSparkHandler();
    }
  }
}

// End OptiqConnectionImpl.java
