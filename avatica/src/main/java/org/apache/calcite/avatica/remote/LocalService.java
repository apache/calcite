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
    if (resultSet.iterable != null) {
      list = list(resultSet.iterable);
      switch (cursorFactory.style) {
      case ARRAY:
        cursorFactory = Meta.CursorFactory.LIST;
        break;
      case MAP:
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
    return new ResultSetResponse(resultSet.statementId, resultSet.ownStatement,
        signature, list);
  }

  private List<List<Object>> list2(Meta.MetaResultSet resultSet) {
    List<List<Object>> list = new ArrayList<List<Object>>();
    return MetaImpl.collect(resultSet.signature.cursorFactory,
        meta.createIterable(new Meta.StatementHandle(resultSet.statementId),
            resultSet.signature,
            resultSet.iterable),
        list);
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

  public PrepareResponse apply(PrepareRequest request) {
    final Meta.StatementHandle h =
        new Meta.StatementHandle(request.statementId);
    final Meta.Signature signature =
        meta.prepare(h, request.sql, request.maxRowCount);
    return new PrepareResponse(signature);
  }

  public ResultSetResponse apply(PrepareAndExecuteRequest request) {
    final Meta.StatementHandle h =
        new Meta.StatementHandle(request.statementId);
    final Meta.MetaResultSet resultSet =
        meta.prepareAndExecute(h, request.sql, request.maxRowCount,
            new Meta.PrepareCallback() {
              public Object getMonitor() {
                return LocalService.class;
              }

              public void clear() {}

              public void assign(Meta.Signature signature,
                  Iterable<Object> iterable) {}

              public void execute() {}
            });
    return toResponse(resultSet);
  }

  public CreateStatementResponse apply(CreateStatementRequest request) {
    final Meta.StatementHandle h =
        meta.createStatement(new Meta.ConnectionHandle(request.connectionId));
    return new CreateStatementResponse(h.id);
  }
}

// End LocalService.java
