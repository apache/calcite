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
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * Implementation of {@link Meta} for the remote driver.
 */
class RemoteMeta extends MetaImpl {
  final Service service;

  public RemoteMeta(AvaticaConnection connection, Service service) {
    super(connection);
    this.service = service;
  }

  private MetaResultSet toResultSet(Class clazz,
      Service.ResultSetResponse response) {
    Signature signature0 = response.signature;
    if (signature0 == null) {
      final List<ColumnMetaData> columns =
          clazz == null
              ? Collections.<ColumnMetaData>emptyList()
              : fieldMetaData(clazz).columns;
      signature0 = Signature.create(columns,
          "?", Collections.<AvaticaParameter>emptyList(), CursorFactory.ARRAY);
    }
    return new MetaResultSet(response.connectionId, response.statementId,
        response.ownStatement, signature0, response.firstFrame);
  }

  @Override public StatementHandle createStatement(ConnectionHandle ch) {
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
    final Service.PrepareResponse response = service.apply(
        new Service.PrepareRequest(ch.id, sql, maxRowCount));
    return response.statement;
  }

  @Override public MetaResultSet prepareAndExecute(ConnectionHandle ch,
      String sql, int maxRowCount, PrepareCallback callback) {
    final Service.ResultSetResponse response;
    try {
      synchronized (callback.getMonitor()) {
        callback.clear();
        response = service.apply(new Service.PrepareAndExecuteRequest(
            ch.id, sql, maxRowCount));
        callback.assign(response.signature, response.firstFrame);
      }
      callback.execute();
      return toResultSet(null, response);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override public Frame fetch(StatementHandle h, List<Object> parameterValues,
      int offset, int fetchMaxRowCount) {
    final Service.FetchResponse response =
        service.apply(
            new Service.FetchRequest(h.connectionId, h.id, parameterValues,
                offset, fetchMaxRowCount));
    return response.frame;
  }
}

// End RemoteMeta.java
