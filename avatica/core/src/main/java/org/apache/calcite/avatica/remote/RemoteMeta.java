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
package org.apache.calcite.avatica.remote;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaConnection.CallableWithoutException;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ConnectionPropertiesImpl;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link org.apache.calcite.avatica.Meta} for the remote
 * driver.
 */
class RemoteMeta extends MetaImpl {
  final Service service;
  final Map<String, ConnectionPropertiesImpl> propsMap = new HashMap<>();
  private Map<DatabaseProperty, Object> databaseProperties;

  public RemoteMeta(AvaticaConnection connection, Service service) {
    super(connection);
    this.service = service;
  }

  private MetaResultSet toResultSet(Class clazz,
      Service.ResultSetResponse response) {
    if (response.updateCount != -1) {
      return MetaResultSet.count(response.connectionId, response.statementId,
          response.updateCount);
    }
    Signature signature0 = response.signature;
    if (signature0 == null) {
      final List<ColumnMetaData> columns =
          clazz == null
              ? Collections.<ColumnMetaData>emptyList()
              : fieldMetaData(clazz).columns;
      signature0 = Signature.create(columns,
          "?", Collections.<AvaticaParameter>emptyList(), CursorFactory.ARRAY,
          Meta.StatementType.SELECT);
    }
    return MetaResultSet.create(response.connectionId, response.statementId,
        response.ownStatement, signature0, response.firstFrame);
  }

  @Override public Map<DatabaseProperty, Object> getDatabaseProperties(ConnectionHandle ch) {
    synchronized (this) {
      // Compute map on first use, and cache
      if (databaseProperties == null) {
        databaseProperties =
            service.apply(new Service.DatabasePropertyRequest(ch.id)).map;
      }
      return databaseProperties;
    }
  }

  @Override public StatementHandle createStatement(final ConnectionHandle ch) {
    return connection.invokeWithRetries(
        new CallableWithoutException<StatementHandle>() {
          public StatementHandle call() {
            // sync connection state if necessary
            connectionSync(ch, new ConnectionPropertiesImpl());
            final Service.CreateStatementResponse response =
                service.apply(new Service.CreateStatementRequest(ch.id));
            return new StatementHandle(response.connectionId, response.statementId, null);
          }
        });
  }

  @Override public void closeStatement(final StatementHandle h) {
    connection.invokeWithRetries(
        new CallableWithoutException<Void>() {
          public Void call() {
            final Service.CloseStatementResponse response =
                service.apply(
                    new Service.CloseStatementRequest(h.connectionId, h.id));
            return null;
          }
        });
  }

  @Override public void openConnection(final ConnectionHandle ch, final Map<String, String> info) {
    connection.invokeWithRetries(
        new CallableWithoutException<Void>() {
          public Void call() {
            final Service.OpenConnectionResponse response =
                service.apply(new Service.OpenConnectionRequest(ch.id, info));
            return null;
          }
        });
  }

  @Override public void closeConnection(final ConnectionHandle ch) {
    connection.invokeWithRetries(
        new CallableWithoutException<Void>() {
          public Void call() {
            final Service.CloseConnectionResponse response =
                service.apply(new Service.CloseConnectionRequest(ch.id));
            propsMap.remove(ch.id);
            return null;
          }
        });
  }

  @Override public ConnectionProperties connectionSync(final ConnectionHandle ch,
      final ConnectionProperties connProps) {
    return connection.invokeWithRetries(
        new CallableWithoutException<ConnectionProperties>() {
          public ConnectionProperties call() {
            ConnectionPropertiesImpl localProps = propsMap.get(ch.id);
            if (localProps == null) {
              localProps = new ConnectionPropertiesImpl();
              localProps.setDirty(true);
              propsMap.put(ch.id, localProps);
            }

            // Only make an RPC if necessary. RPC is necessary when we have local changes that need
            // flushed to the server (be sure to introduce any new changes from connProps before
            // checking AND when connProps.isEmpty() (meaning, this was a request for a value, not
            // overriding a value). Otherwise, accumulate the change locally and return immediately.
            if (localProps.merge(connProps).isDirty() && connProps.isEmpty()) {
              final Service.ConnectionSyncResponse response = service.apply(
                  new Service.ConnectionSyncRequest(ch.id, localProps));
              propsMap.put(ch.id, (ConnectionPropertiesImpl) response.connProps);
              return response.connProps;
            } else {
              return localProps;
            }
          }
        });
  }

  @Override public MetaResultSet getCatalogs(final ConnectionHandle ch) {
    return connection.invokeWithRetries(
        new CallableWithoutException<MetaResultSet>() {
          public MetaResultSet call() {
            final Service.ResultSetResponse response =
                service.apply(new Service.CatalogsRequest(ch.id));
            return toResultSet(MetaCatalog.class, response);
          }
        });
  }

  @Override public MetaResultSet getSchemas(final ConnectionHandle ch, final String catalog,
      final Pat schemaPattern) {
    return connection.invokeWithRetries(
        new CallableWithoutException<MetaResultSet>() {
          public MetaResultSet call() {
            final Service.ResultSetResponse response =
                service.apply(
                    new Service.SchemasRequest(ch.id, catalog, schemaPattern.s));
            return toResultSet(MetaSchema.class, response);
          }
        });
  }

  @Override public MetaResultSet getTables(final ConnectionHandle ch, final String catalog,
      final Pat schemaPattern, final Pat tableNamePattern, final List<String> typeList) {
    return connection.invokeWithRetries(
        new CallableWithoutException<MetaResultSet>() {
          public MetaResultSet call() {
            final Service.ResultSetResponse response =
                service.apply(
                    new Service.TablesRequest(ch.id, catalog, schemaPattern.s,
                        tableNamePattern.s, typeList));
            return toResultSet(MetaTable.class, response);
          }
        });
  }

  @Override public MetaResultSet getTableTypes(final ConnectionHandle ch) {
    return connection.invokeWithRetries(
        new CallableWithoutException<MetaResultSet>() {
          public MetaResultSet call() {
            final Service.ResultSetResponse response =
                service.apply(new Service.TableTypesRequest(ch.id));
            return toResultSet(MetaTableType.class, response);
          }
        });
  }

  @Override public MetaResultSet getTypeInfo(final ConnectionHandle ch) {
    return connection.invokeWithRetries(
        new CallableWithoutException<MetaResultSet>() {
          public MetaResultSet call() {
            final Service.ResultSetResponse response =
                service.apply(new Service.TypeInfoRequest(ch.id));
            return toResultSet(MetaTypeInfo.class, response);
          }
        });
  }

  @Override public MetaResultSet getColumns(final ConnectionHandle ch, final String catalog,
      final Pat schemaPattern, final Pat tableNamePattern, final Pat columnNamePattern) {
    return connection.invokeWithRetries(
        new CallableWithoutException<MetaResultSet>() {
          public MetaResultSet call() {
            final Service.ResultSetResponse response =
                service.apply(
                    new Service.ColumnsRequest(ch.id, catalog, schemaPattern.s,
                        tableNamePattern.s, columnNamePattern.s));
            return toResultSet(MetaColumn.class, response);
          }
        });
  }

  @Override public StatementHandle prepare(final ConnectionHandle ch, final String sql,
      final long maxRowCount) {
    return connection.invokeWithRetries(
        new CallableWithoutException<StatementHandle>() {
          public StatementHandle call() {
            connectionSync(ch,
                new ConnectionPropertiesImpl()); // sync connection state if necessary
            final Service.PrepareResponse response = service.apply(
                new Service.PrepareRequest(ch.id, sql, maxRowCount));
            return response.statement;
          }
        });
  }

  @SuppressWarnings("deprecation")
  @Override public ExecuteResult prepareAndExecute(StatementHandle h, String sql, long maxRowCount,
      PrepareCallback callback) throws NoSuchStatementException {
    // The old semantics were that maxRowCount was also treated as the maximum number of
    // elements in the first Frame of results. A value of -1 would also preserve this, but an
    // explicit (positive) number is easier to follow, IMO.
    return prepareAndExecute(h, sql, maxRowCount, AvaticaUtils.toSaturatedInt(maxRowCount),
        callback);
  }

  @Override public ExecuteResult prepareAndExecute(final StatementHandle h, final String sql,
      final long maxRowCount, int maxRowsInFirstFrame, final PrepareCallback callback)
      throws NoSuchStatementException {
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ExecuteResult>() {
            public ExecuteResult call() {
              // sync connection state if necessary
              connectionSync(new ConnectionHandle(h.connectionId), new ConnectionPropertiesImpl());
              final Service.ExecuteResponse response;
              try {
                synchronized (callback.getMonitor()) {
                  callback.clear();
                  response = service.apply(
                      new Service.PrepareAndExecuteRequest(h.connectionId,
                          h.id, sql, maxRowCount));
                  if (response.missingStatement) {
                    throw new RuntimeException(new NoSuchStatementException(h));
                  }
                  if (response.results.size() > 0) {
                    final Service.ResultSetResponse result = response.results.get(0);
                    callback.assign(result.signature, result.firstFrame,
                        result.updateCount);
                  }
                }
                callback.execute();
                List<MetaResultSet> metaResultSets = new ArrayList<>();
                for (Service.ResultSetResponse result : response.results) {
                  metaResultSets.add(toResultSet(null, result));
                }
                return new ExecuteResult(metaResultSets);
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof NoSuchStatementException) {
        throw (NoSuchStatementException) cause;
      }
      throw e;
    }
  }

  @Override public Frame fetch(final StatementHandle h, final long offset,
      final int fetchMaxRowCount) throws NoSuchStatementException, MissingResultsException {
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<Frame>() {
            public Frame call() {
              final Service.FetchResponse response =
                  service.apply(
                      new Service.FetchRequest(h.connectionId, h.id, offset, fetchMaxRowCount));
              if (response.missingStatement) {
                throw new RuntimeException(new NoSuchStatementException(h));
              }
              if (response.missingResults) {
                throw new RuntimeException(new MissingResultsException(h));
              }
              return response.frame;
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof NoSuchStatementException) {
        throw (NoSuchStatementException) cause;
      } else if (cause instanceof MissingResultsException) {
        throw (MissingResultsException) cause;
      }
      throw e;
    }
  }

  @SuppressWarnings("deprecation")
  @Override public ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues,
      long maxRowCount) throws NoSuchStatementException {
    return execute(h, parameterValues, AvaticaUtils.toSaturatedInt(maxRowCount));
  }

  @Override public ExecuteResult execute(final StatementHandle h,
      final List<TypedValue> parameterValues, final int maxRowsInFirstFrame)
      throws NoSuchStatementException {
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<ExecuteResult>() {
            public ExecuteResult call() {
              final Service.ExecuteResponse response = service.apply(
                  new Service.ExecuteRequest(h, parameterValues, maxRowsInFirstFrame));

              if (response.missingStatement) {
                throw new RuntimeException(new NoSuchStatementException(h));
              }

              List<MetaResultSet> metaResultSets = new ArrayList<>();
              for (Service.ResultSetResponse result : response.results) {
                metaResultSets.add(toResultSet(null, result));
              }

              return new ExecuteResult(metaResultSets);
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof NoSuchStatementException) {
        throw (NoSuchStatementException) cause;
      }
      throw e;
    }
  }

  @Override public boolean syncResults(final StatementHandle h, final QueryState state,
      final long offset) throws NoSuchStatementException {
    try {
      return connection.invokeWithRetries(
          new CallableWithoutException<Boolean>() {
            public Boolean call() {
              final Service.SyncResultsResponse response =
                  service.apply(
                      new Service.SyncResultsRequest(h.connectionId, h.id, state, offset));
              if (response.missingStatement) {
                throw new RuntimeException(new NoSuchStatementException(h));
              }
              return response.moreResults;
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof NoSuchStatementException) {
        throw (NoSuchStatementException) cause;
      }
      throw e;
    }
  }

  @Override public void commit(final ConnectionHandle ch) {
    connection.invokeWithRetries(new CallableWithoutException<Void>() {
      public Void call() {
        final Service.CommitResponse response =
            service.apply(new Service.CommitRequest(ch.id));
        return null;
      }
    });
  }

  @Override public void rollback(final ConnectionHandle ch) {
    connection.invokeWithRetries(new CallableWithoutException<Void>() {
      public Void call() {
        final Service.RollbackResponse response =
            service.apply(new Service.RollbackRequest(ch.id));
        return null;
      }
    });
  }

  @Override public ExecuteBatchResult prepareAndExecuteBatch(final StatementHandle h,
      final List<String> sqlCommands) throws NoSuchStatementException {
    return connection.invokeWithRetries(new CallableWithoutException<ExecuteBatchResult>() {
      @Override public ExecuteBatchResult call() {
        Service.ExecuteBatchResponse response =
            service.apply(
                new Service.PrepareAndExecuteBatchRequest(h.connectionId, h.id, sqlCommands));
        return new ExecuteBatchResult(response.updateCounts);
      }
    });
  }

  @Override public ExecuteBatchResult executeBatch(final StatementHandle h,
      final List<List<TypedValue>> parameterValues) throws NoSuchStatementException {
    return connection.invokeWithRetries(new CallableWithoutException<ExecuteBatchResult>() {
      @Override public ExecuteBatchResult call() {
        Service.ExecuteBatchResponse response =
            service.apply(new Service.ExecuteBatchRequest(h.connectionId, h.id, parameterValues));
        return new ExecuteBatchResult(response.updateCounts);
      }
    });
  }
}

// End RemoteMeta.java
