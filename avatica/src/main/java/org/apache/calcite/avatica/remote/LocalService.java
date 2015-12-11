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
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Implementation of {@link Service} that talks to a local {@link Meta}.
 */
public class LocalService implements Service {
  final Meta meta;

  private RpcMetadataResponse serverLevelRpcMetadata;

  public LocalService(Meta meta) {
    this.meta = meta;
  }

  @Override public void setRpcMetadata(RpcMetadataResponse serverLevelRpcMetadata) {
    this.serverLevelRpcMetadata = Objects.requireNonNull(serverLevelRpcMetadata);
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
          resultSet.updateCount, serverLevelRpcMetadata);
    }

    Meta.Signature signature = resultSet.signature;
    Meta.CursorFactory cursorFactory = resultSet.signature.cursorFactory;
    Meta.Frame frame = null;
    int updateCount = -1;
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
      case RECORD:
        cursorFactory = Meta.CursorFactory.LIST;
        break;
      default:
        cursorFactory = Meta.CursorFactory.map(cursorFactory.fieldNames);
      }

      final boolean done = resultSet.firstFrame.done;

      frame = new Meta.Frame(0, done, list);
      updateCount = -1;

      if (signature.statementType != null) {
        if (signature.statementType.canUpdate()) {
          frame = null;
          updateCount = ((Number) ((List) list.get(0)).get(0)).intValue();
        }
      }
    } else {
      //noinspection unchecked
      list = (List<Object>) (List) list2(resultSet);
      cursorFactory = Meta.CursorFactory.LIST;
    }

    if (cursorFactory != resultSet.signature.cursorFactory) {
      signature = signature.setCursorFactory(cursorFactory);
    }

    return new ResultSetResponse(resultSet.connectionId, resultSet.statementId,
        resultSet.ownStatement, signature, frame, updateCount, serverLevelRpcMetadata);
  }

  private List<List<Object>> list2(Meta.MetaResultSet resultSet) {
    final Meta.StatementHandle h = new Meta.StatementHandle(
        resultSet.connectionId, resultSet.statementId, null);
    final List<TypedValue> parameterValues = Collections.emptyList();
    final Iterable<Object> iterable = meta.createIterable(h, null,
        resultSet.signature, parameterValues, resultSet.firstFrame);
    final List<List<Object>> list = new ArrayList<>();
    return MetaImpl.collect(resultSet.signature.cursorFactory, iterable, list);
  }

  public ResultSetResponse apply(CatalogsRequest request) {
    final Meta.ConnectionHandle ch =
        new Meta.ConnectionHandle(request.connectionId);
    final Meta.MetaResultSet resultSet = meta.getCatalogs(ch);
    return toResponse(resultSet);
  }

  public ResultSetResponse apply(SchemasRequest request) {
    final Meta.ConnectionHandle ch =
        new Meta.ConnectionHandle(request.connectionId);
    final Meta.MetaResultSet resultSet =
        meta.getSchemas(ch, request.catalog, Meta.Pat.of(request.schemaPattern));
    return toResponse(resultSet);
  }

  public ResultSetResponse apply(TablesRequest request) {
    final Meta.ConnectionHandle ch =
        new Meta.ConnectionHandle(request.connectionId);
    final Meta.MetaResultSet resultSet =
        meta.getTables(ch,
            request.catalog,
            Meta.Pat.of(request.schemaPattern),
            Meta.Pat.of(request.tableNamePattern),
            request.typeList);
    return toResponse(resultSet);
  }

  public ResultSetResponse apply(TableTypesRequest request) {
    final Meta.ConnectionHandle ch =
        new Meta.ConnectionHandle(request.connectionId);
    final Meta.MetaResultSet resultSet = meta.getTableTypes(ch);
    return toResponse(resultSet);
  }

  public ResultSetResponse apply(TypeInfoRequest request) {
    final Meta.ConnectionHandle ch =
        new Meta.ConnectionHandle(request.connectionId);
    final Meta.MetaResultSet resultSet = meta.getTypeInfo(ch);
    return toResponse(resultSet);
  }

  public ResultSetResponse apply(ColumnsRequest request) {
    final Meta.ConnectionHandle ch =
        new Meta.ConnectionHandle(request.connectionId);
    final Meta.MetaResultSet resultSet =
        meta.getColumns(ch,
            request.catalog,
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
    return new PrepareResponse(h, serverLevelRpcMetadata);
  }

  public ExecuteResponse apply(PrepareAndExecuteRequest request) {
    final Meta.StatementHandle sh =
        new Meta.StatementHandle(request.connectionId, request.statementId, null);
    try {
      final Meta.ExecuteResult executeResult =
          meta.prepareAndExecute(sh, request.sql, request.maxRowCount,
              new Meta.PrepareCallback() {
                @Override public Object getMonitor() {
                  return LocalService.class;
                }

                @Override public void clear() {
                }

                @Override public void assign(Meta.Signature signature,
                    Meta.Frame firstFrame, long updateCount) {
                }

                @Override public void execute() {
                }
              });
      final List<ResultSetResponse> results = new ArrayList<>();
      for (Meta.MetaResultSet metaResultSet : executeResult.resultSets) {
        results.add(toResponse(metaResultSet));
      }
      return new ExecuteResponse(results, false, serverLevelRpcMetadata);
    } catch (NoSuchStatementException e) {
      // The Statement doesn't exist anymore, bubble up this information
      return new ExecuteResponse(null, true, serverLevelRpcMetadata);
    }
  }

  public FetchResponse apply(FetchRequest request) {
    final Meta.StatementHandle h = new Meta.StatementHandle(
        request.connectionId, request.statementId, null);
    try {
      final Meta.Frame frame =
          meta.fetch(h,
              request.offset,
              request.fetchMaxRowCount);
      return new FetchResponse(frame, false, false, serverLevelRpcMetadata);
    } catch (NullPointerException | NoSuchStatementException e) {
      // The Statement doesn't exist anymore, bubble up this information
      return new FetchResponse(null, true, true, serverLevelRpcMetadata);
    } catch (MissingResultsException e) {
      return new FetchResponse(null, false, true, serverLevelRpcMetadata);
    }
  }

  public ExecuteResponse apply(ExecuteRequest request) {
    try {
      final Meta.ExecuteResult executeResult = meta.execute(request.statementHandle,
          request.parameterValues, request.maxRowCount);

      final List<ResultSetResponse> results = new ArrayList<>();
      for (Meta.MetaResultSet metaResultSet : executeResult.resultSets) {
        results.add(toResponse(metaResultSet));
      }
      return new ExecuteResponse(results, false, serverLevelRpcMetadata);
    } catch (NoSuchStatementException e) {
      return new ExecuteResponse(null, true, serverLevelRpcMetadata);
    }
  }

  public CreateStatementResponse apply(CreateStatementRequest request) {
    final Meta.ConnectionHandle ch =
        new Meta.ConnectionHandle(request.connectionId);
    final Meta.StatementHandle h = meta.createStatement(ch);
    return new CreateStatementResponse(h.connectionId, h.id, serverLevelRpcMetadata);
  }

  public CloseStatementResponse apply(CloseStatementRequest request) {
    final Meta.StatementHandle h = new Meta.StatementHandle(
        request.connectionId, request.statementId, null);
    meta.closeStatement(h);
    return new CloseStatementResponse(serverLevelRpcMetadata);
  }

  public OpenConnectionResponse apply(OpenConnectionRequest request) {
    final Meta.ConnectionHandle ch =
        new Meta.ConnectionHandle(request.connectionId);
    meta.openConnection(ch, request.info);
    return new OpenConnectionResponse(serverLevelRpcMetadata);
  }

  public CloseConnectionResponse apply(CloseConnectionRequest request) {
    final Meta.ConnectionHandle ch =
        new Meta.ConnectionHandle(request.connectionId);
    meta.closeConnection(ch);
    return new CloseConnectionResponse(serverLevelRpcMetadata);
  }

  public ConnectionSyncResponse apply(ConnectionSyncRequest request) {
    final Meta.ConnectionHandle ch =
        new Meta.ConnectionHandle(request.connectionId);
    final Meta.ConnectionProperties connProps =
        meta.connectionSync(ch, request.connProps);
    return new ConnectionSyncResponse(connProps, serverLevelRpcMetadata);
  }

  public DatabasePropertyResponse apply(DatabasePropertyRequest request) {
    final Meta.ConnectionHandle ch =
        new Meta.ConnectionHandle(request.connectionId);
    return new DatabasePropertyResponse(meta.getDatabaseProperties(ch), serverLevelRpcMetadata);
  }

  public SyncResultsResponse apply(SyncResultsRequest request) {
    final Meta.StatementHandle h = new Meta.StatementHandle(
        request.connectionId, request.statementId, null);
    SyncResultsResponse response;
    try {
      // Set success on the cached statement
      response = new SyncResultsResponse(meta.syncResults(h, request.state, request.offset), false,
          serverLevelRpcMetadata);
    } catch (NoSuchStatementException e) {
      // Tried to sync results on a statement which wasn't cached
      response = new SyncResultsResponse(false, true, serverLevelRpcMetadata);
    }

    return response;
  }

  public CommitResponse apply(CommitRequest request) {
    meta.commit(new Meta.ConnectionHandle(request.connectionId));

    // If commit() errors, let the ErrorResponse be sent back via an uncaught Exception.
    return new CommitResponse();
  }

  public RollbackResponse apply(RollbackRequest request) {
    meta.rollback(new Meta.ConnectionHandle(request.connectionId));

    // If rollback() errors, let the ErrorResponse be sent back via an uncaught Exception.
    return new RollbackResponse();
  }
}

// End LocalService.java
