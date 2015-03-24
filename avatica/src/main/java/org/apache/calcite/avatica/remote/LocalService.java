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

import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Implementation of {@link Service} that talks to a local {@link Meta}.
 */
public class LocalService implements Service {
  final Meta meta;
  /** Whether output is going to JSON. */
  private final boolean json = true;

  public LocalService(Meta meta) {
    this.meta = meta;
  }

  private static <E> List<E> list(Iterable<E> iterable) {
    if (iterable instanceof List) {
      return (List<E>) iterable;
    }
    final List<E> rowList = new ArrayList<E>();
    for (E row : iterable) {
      rowList.add(row);
    }
    return rowList;
  }

  /** Converts a result set (not serializable) into a serializable response. */
  public ResultSetResponse toResponse(Meta.MetaResultSet resultSet) {
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
      list = (List<Object>) (List) list2(resultSet);
      cursorFactory = Meta.CursorFactory.LIST;
    }
    Meta.Signature signature = resultSet.signature;
    if (cursorFactory != resultSet.signature.cursorFactory) {
      signature = signature.setCursorFactory(cursorFactory);
    }
    return new ResultSetResponse(resultSet.connectionId, resultSet.statementId,
        resultSet.ownStatement, signature, new Meta.Frame(0, true, list));
  }

  private List<List<Object>> list2(Meta.MetaResultSet resultSet) {
    final Meta.StatementHandle h = new Meta.StatementHandle(
        resultSet.connectionId, resultSet.statementId, null);
    final Iterable<Object> iterable = meta.createIterable(h,
        resultSet.signature, Collections.emptyList(), resultSet.firstFrame);
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
        meta.getTables(request.catalog, Meta.Pat.of(request.schemaPattern),
            Meta.Pat.of(request.tableNamePattern), request.typeList);
    return toResponse(resultSet);
  }

  public ResultSetResponse apply(TableTypesRequest request) {
    final Meta.MetaResultSet resultSet = meta.getTableTypes();
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
    if (json) {
      Meta.Signature signature = h.signature;
      final List<ColumnMetaData> columns = new ArrayList<>();
      for (ColumnMetaData column : signature.columns) {
        switch (column.type.rep) {
        case BYTE:
        case PRIMITIVE_BYTE:
        case DOUBLE:
        case PRIMITIVE_DOUBLE:
        case FLOAT:
        case PRIMITIVE_FLOAT:
        case INTEGER:
        case PRIMITIVE_INT:
        case SHORT:
        case PRIMITIVE_SHORT:
        case LONG:
        case PRIMITIVE_LONG:
          column = column.setTypeId(Types.NUMERIC);
        }
        columns.add(column);
      }
      signature = new Meta.Signature(columns, signature.sql,
          signature.parameters, signature.internalParameters,
          signature.cursorFactory);
      h.signature = signature;
    }
    return new PrepareResponse(h);
  }

  public ResultSetResponse apply(PrepareAndExecuteRequest request) {
    final Meta.ConnectionHandle ch =
        new Meta.ConnectionHandle(request.connectionId);
    final Meta.MetaResultSet resultSet =
        meta.prepareAndExecute(ch, request.sql, request.maxRowCount,
            new Meta.PrepareCallback() {
              @Override public Object getMonitor() {
                return LocalService.class;
              }

              @Override public void clear() {
              }

              @Override public void assign(Meta.Signature signature,
                  Meta.Frame firstFrame) {
              }

              @Override public void execute() {
              }
            });
    return toResponse(resultSet);
  }

  public FetchResponse apply(FetchRequest request) {
    final Meta.StatementHandle h = new Meta.StatementHandle(
        request.connectionId, request.statementId, null);
    final Meta.Frame frame =
        meta.fetch(h, request.parameterValues, request.offset,
            request.fetchMaxRowCount);
    return new FetchResponse(frame);
  }

  public CreateStatementResponse apply(CreateStatementRequest request) {
    final Meta.StatementHandle h =
        meta.createStatement(new Meta.ConnectionHandle(request.connectionId));
    return new CreateStatementResponse(h.connectionId, h.id);
  }

  @Override
  public CloseStatementResponse apply(CloseStatementRequest request) {
    meta.closeStatement(new Meta.StatementHandle(
        request.connectionId, request.statementId, null));
    return new CloseStatementResponse();
  }
}

// End LocalService.java
