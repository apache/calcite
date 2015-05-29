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

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Implementation of {@link Service} that talks to a local {@link Meta}.
 */
public class LocalService implements Service {
  final Meta meta;

  public LocalService(Meta meta) {
    this.meta = meta;
  }

  private static <E> List<E> list(Iterable<E> iterable) {
    if (iterable instanceof List) {
      return (List<E>) iterable;
    }
    final List<E> rowList = new ArrayList<>();
    for (E row : iterable) {
      rowList.add(row);
    }
    return rowList;
  }

  /** Converts a result set (not serializable) into a serializable response. */
  public ResultSetResponse toResponse(Meta.MetaResultSet resultSet) {
    if (resultSet.updateCount != -1) {
      return new ResultSetResponse(resultSet.connectionId,
          resultSet.statementId, resultSet.ownStatement, null, null,
          resultSet.updateCount);
    }
    Meta.CursorFactory cursorFactory = resultSet.signature.cursorFactory;
    final List<Object> list;
    if (resultSet.firstFrame != null) {
      list = list(resultSet.firstFrame.rows);
      switch (cursorFactory.style) {
      case ARRAY:
        cursorFactory = Meta.CursorFactory.LIST;
        break;
      case MAP:
      case LIST:
        break;
      default:
        cursorFactory = Meta.CursorFactory.map(cursorFactory.fieldNames);
      }
    } else {
      //noinspection unchecked
      cursorFactory = Meta.CursorFactory.LIST;
    }
    Meta.Signature signature = resultSet.signature;
    if (cursorFactory != resultSet.signature.cursorFactory) {
      signature = signature.setCursorFactory(cursorFactory);
    }
    return new ResultSetResponse(resultSet.connectionId, resultSet.statementId,
        resultSet.ownStatement, signature, resultSet.firstFrame, -1);
  }

  private List<List<Object>> list2(Meta.MetaResultSet resultSet) {
    final Meta.StatementHandle h = new Meta.StatementHandle(
        resultSet.connectionId, resultSet.statementId, null);
    final List<TypedValue> parameterValues = Collections.emptyList();
    final Iterable<Object> iterable = meta.createIterable(h,
        resultSet.signature, parameterValues, resultSet.firstFrame);
    final List<List<Object>> list = new ArrayList<>();
    return MetaImpl.collect(resultSet.signature.cursorFactory, iterable, list);
  }

  public ResultSetResponse apply(CatalogsRequest request) {
    final Meta.MetaResultSet resultSet = meta.getCatalogs();
    return toResponse(resultSet);
  }

  public ResultSetResponse apply(SchemasRequest request) {
    final Meta.MetaResultSet resultSet =
        meta.getSchemas(request.catalog, Meta.Pat.of(request.schemaPattern));
    return toResponse(resultSet);
  }

  public ResultSetResponse apply(TablesRequest request) {
    final Meta.MetaResultSet resultSet =
        meta.getTables(request.catalog,
            Meta.Pat.of(request.schemaPattern),
            Meta.Pat.of(request.tableNamePattern),
            request.typeList);
    return toResponse(resultSet);
  }

  public ResultSetResponse apply(TableTypesRequest request) {
    final Meta.MetaResultSet resultSet = meta.getTableTypes();
    return toResponse(resultSet);
  }

  public ResultSetResponse apply(TypeInfoRequest request) {
    final Meta.MetaResultSet resultSet = meta.getTypeInfo();
    return toResponse(resultSet);
  }

  public ResultSetResponse apply(ColumnsRequest request) {
    final Meta.MetaResultSet resultSet =
        meta.getColumns(request.catalog,
            Meta.Pat.of(request.schemaPattern),
            Meta.Pat.of(request.tableNamePattern),
            Meta.Pat.of(request.columnNamePattern));
    return toResponse(resultSet);
  }

  public PrepareResponse apply(PrepareRequest request) {
    final Meta.ConnectionHandle ch =
        new Meta.ConnectionHandle(request.connectionId);
    final Meta.StatementHandle h =
        meta.prepare(ch, request.sql, request.maxRowCount);
    return new PrepareResponse(h);
  }

  public ExecuteResponse apply(PrepareAndExecuteRequest request) {
    final Meta.StatementHandle sh =
        new Meta.StatementHandle(request.connectionId, request.statementId, null);
    final Meta.ExecuteResult executeResult =
        meta.prepareAndExecute(sh, request.sql, request.maxRowCount,
            new Meta.PrepareCallback() {
              @Override public Object getMonitor() {
                return LocalService.class;
              }

              @Override public void clear() {
              }

              @Override public void assign(Meta.Signature signature,
                  Meta.Frame firstFrame, int updateCount) {
              }

              @Override public void execute() {
              }
            });
    final List<ResultSetResponse> results = new ArrayList<>();
    for (Meta.MetaResultSet metaResultSet : executeResult.resultSets) {
      results.add(toResponse(metaResultSet));
    }
    return new ExecuteResponse(results);
  }

  public FetchResponse apply(FetchRequest request) {
    final Meta.StatementHandle h = new Meta.StatementHandle(
        request.connectionId, request.statementId, null);
    final Meta.Frame frame =
        meta.fetch(h,
            request.parameterValues,
            request.offset,
            request.fetchMaxRowCount);
    return new FetchResponse(frame);
  }

  public CreateStatementResponse apply(CreateStatementRequest request) {
    final Meta.StatementHandle h =
        meta.createStatement(new Meta.ConnectionHandle(request.connectionId));
    return new CreateStatementResponse(h.connectionId, h.id);
  }

  public CloseStatementResponse apply(CloseStatementRequest request) {
    meta.closeStatement(
        new Meta.StatementHandle(request.connectionId, request.statementId,
            null));
    return new CloseStatementResponse();
  }

  public CloseConnectionResponse apply(CloseConnectionRequest request) {
    meta.closeConnection(new Meta.ConnectionHandle(request.connectionId));
    return new CloseConnectionResponse();
  }

  public ConnectionSyncResponse apply(ConnectionSyncRequest request) {
    final Meta.ConnectionProperties connProps =
        meta.connectionSync(new Meta.ConnectionHandle(request.connectionId), request.connProps);
    return new ConnectionSyncResponse(connProps);
  }

  public DatabasePropertyResponse apply(DatabasePropertyRequest request) {
    return new DatabasePropertyResponse(meta.getDatabaseProperties());
  }
}

// End LocalService.java
