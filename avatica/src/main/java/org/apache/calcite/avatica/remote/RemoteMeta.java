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
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;

import java.sql.SQLException;

/**
 * Implementation of {@link Meta} for the remote driver.
 */
class RemoteMeta extends MetaImpl {
  final Service service;

  public RemoteMeta(AvaticaConnection connection, Service service) {
    super(connection);
    this.service = service;
  }

  private MetaResultSet toResultSet(Service.ResultSetResponse response) {
    final Signature signature0 = response.signature;
    return new MetaResultSet(response.statementId, response.ownStatement,
        signature0, response.rows);
  }

  @Override public StatementHandle createStatement(ConnectionHandle ch) {
    final Service.CreateStatementResponse response =
        service.apply(new Service.CreateStatementRequest(ch.id));
    return new StatementHandle(response.id);
  }

  @Override public MetaResultSet getCatalogs() {
    final Service.ResultSetResponse response =
        service.apply(new Service.CatalogsRequest());
    return toResultSet(response);
  }

  @Override public MetaResultSet getSchemas(String catalog, Pat schemaPattern) {
    final Service.ResultSetResponse response =
        service.apply(new Service.SchemasRequest(catalog, schemaPattern.s));
    return toResultSet(response);
  }

  @Override public Signature prepare(StatementHandle h, String sql,
      int maxRowCount) {
    final Service.PrepareResponse response =
        service.apply(new Service.PrepareRequest(h.id, sql, maxRowCount));
    return response.signature;
  }

  @Override public MetaResultSet prepareAndExecute(StatementHandle h,
      String sql, int maxRowCount, PrepareCallback callback) {
    final Service.ResultSetResponse response;
    try {
      synchronized (callback.getMonitor()) {
        callback.clear();
        response = service.apply(
            new Service.PrepareAndExecuteRequest(h.id, sql, maxRowCount));
        callback.assign(response.signature, response.rows);
      }
      callback.execute();
      return toResultSet(response);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}

// End RemoteMeta.java
