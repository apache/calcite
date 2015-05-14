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
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ConnectionPropertiesImpl;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link Meta} for the remote driver.
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
          "?", Collections.<AvaticaParameter>emptyList(), CursorFactory.ARRAY);
    }
    return MetaResultSet.create(response.connectionId, response.statementId,
        response.ownStatement, signature0, response.firstFrame);
  }

  @Override public Map<DatabaseProperty, Object> getDatabaseProperties() {
    synchronized (this) {
      // Compute map on first use, and cache
      if (databaseProperties == null) {
        databaseProperties =
            service.apply(new Service.DatabasePropertyRequest()).map;
      }
      return databaseProperties;
    }
  }

  @Override public StatementHandle createStatement(ConnectionHandle ch) {
    connectionSync(ch, new ConnectionPropertiesImpl()); // sync connection state if necessary
    final Service.CreateStatementResponse response =
        service.apply(new Service.CreateStatementRequest(ch.id));
    return new StatementHandle(response.connectionId, response.statementId,
        null);
  }

  @Override public void closeStatement(StatementHandle h) {
    final Service.CloseStatementResponse response =
        service.apply(new Service.CloseStatementRequest(h.connectionId, h.id));
  }

  @Override public void closeConnection(ConnectionHandle ch) {
    final Service.CloseConnectionResponse response =
        service.apply(new Service.CloseConnectionRequest(ch.id));
    propsMap.remove(ch.id);
  }

  @Override public ConnectionProperties connectionSync(ConnectionHandle ch,
      ConnectionProperties connProps) {
    ConnectionPropertiesImpl localProps = propsMap.get(ch.id);
    if (localProps == null) {
      localProps = new ConnectionPropertiesImpl();
      localProps.setDirty(true);
      propsMap.put(ch.id, localProps);
    }

    // Only make an RPC if necessary. RPC is necessary when we have local changes that need
    // flushed to the server (be sure to introduce any new changes from connProps before checking
    // AND when connProps.isEmpty() (meaning, this was a request for a value, not overriding a
    // value). Otherwise, accumulate the change locally and return immediately.
    if (localProps.merge(connProps).isDirty() && connProps.isEmpty()) {
      final Service.ConnectionSyncResponse response = service.apply(
          new Service.ConnectionSyncRequest(ch.id, localProps));
      propsMap.put(ch.id, (ConnectionPropertiesImpl) response.connProps);
      return response.connProps;
    } else {
      return localProps;
    }
  }

  @Override public MetaResultSet getCatalogs() {
    final Service.ResultSetResponse response =
        service.apply(new Service.CatalogsRequest());
    return toResultSet(MetaCatalog.class, response);
  }

  @Override public MetaResultSet getSchemas(String catalog, Pat schemaPattern) {
    final Service.ResultSetResponse response =
        service.apply(new Service.SchemasRequest(catalog, schemaPattern.s));
    return toResultSet(MetaSchema.class, response);
  }

  @Override public MetaResultSet getTables(String catalog, Pat schemaPattern,
      Pat tableNamePattern, List<String> typeList) {
    final Service.ResultSetResponse response =
        service.apply(
            new Service.TablesRequest(catalog, schemaPattern.s,
                tableNamePattern.s, typeList));
    return toResultSet(MetaTable.class, response);
  }

  @Override public MetaResultSet getTableTypes() {
    final Service.ResultSetResponse response =
        service.apply(new Service.TableTypesRequest());
    return toResultSet(MetaTableType.class, response);
  }

  @Override public MetaResultSet getTypeInfo() {
    final Service.ResultSetResponse response =
        service.apply(new Service.TypeInfoRequest());
    return toResultSet(MetaTypeInfo.class, response);
  }

  @Override public MetaResultSet getColumns(String catalog, Pat schemaPattern,
      Pat tableNamePattern, Pat columnNamePattern) {
    final Service.ResultSetResponse response =
        service.apply(
            new Service.ColumnsRequest(catalog, schemaPattern.s,
                tableNamePattern.s, columnNamePattern.s));
    return toResultSet(MetaColumn.class, response);
  }

  @Override public StatementHandle prepare(ConnectionHandle ch, String sql,
      int maxRowCount) {
    connectionSync(ch, new ConnectionPropertiesImpl()); // sync connection state if necessary
    final Service.PrepareResponse response = service.apply(
        new Service.PrepareRequest(ch.id, sql, maxRowCount));
    return response.statement;
  }

  @Override public ExecuteResult prepareAndExecute(ConnectionHandle ch,
      String sql, int maxRowCount, PrepareCallback callback) {
    connectionSync(ch, new ConnectionPropertiesImpl()); // sync connection state if necessary
    final Service.ExecuteResponse response;
    try {
      synchronized (callback.getMonitor()) {
        callback.clear();
        response = service.apply(
            new Service.PrepareAndExecuteRequest(ch.id, sql, maxRowCount));
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

  @Override public Frame fetch(StatementHandle h,
      List<TypedValue> parameterValues, int offset, int fetchMaxRowCount) {
    final Service.FetchResponse response =
        service.apply(
            new Service.FetchRequest(h.connectionId, h.id, parameterValues,
                offset, fetchMaxRowCount));
    return response.frame;
  }
}

// End RemoteMeta.java
