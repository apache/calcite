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

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * A common base class for {@link Service} implementations that implement
 * modifications made to response objects.
 */
public abstract class AbstractService implements Service {

  private RpcMetadataResponse rpcMetadata = null;

  /**
   * Represents the serialization of the data over a transport.
   */
  enum SerializationType {
    JSON,
    PROTOBUF
  }

  /**
   * @return The manner in which the data is serialized.
   */
  abstract SerializationType getSerializationType();

  /** Modifies a signature, changing the representation of numeric columns
   * within it. This deals with the fact that JSON transmits a small long value,
   * or a float which is a whole number, as an integer. Thus the accessors need
   * be prepared to accept any numeric type. */
  Meta.Signature finagle(Meta.Signature signature) {
    final List<ColumnMetaData> columns = new ArrayList<>();
    for (ColumnMetaData column : signature.columns) {
      columns.add(finagle(column));
    }
    if (columns.equals(signature.columns)) {
      return signature;
    }
    return new Meta.Signature(columns, signature.sql,
        signature.parameters, signature.internalParameters,
        signature.cursorFactory, signature.statementType);
  }

  ColumnMetaData finagle(ColumnMetaData column) {
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
      return column.setRep(ColumnMetaData.Rep.NUMBER);
    default:
      // continue
      break;
    }
    switch (column.type.id) {
    case Types.VARBINARY:
    case Types.BINARY:
      switch (getSerializationType()) {
      case JSON:
        return column.setRep(ColumnMetaData.Rep.STRING);
      case PROTOBUF:
        return column;
      default:
        throw new IllegalStateException("Unhadled case statement");
      }
    case Types.DECIMAL:
    case Types.NUMERIC:
      return column.setRep(ColumnMetaData.Rep.NUMBER);
    default:
      return column;
    }
  }

  PrepareResponse finagle(PrepareResponse response) {
    final Meta.StatementHandle statement = finagle(response.statement);
    if (statement == response.statement) {
      return response;
    }
    return new PrepareResponse(statement, rpcMetadata);
  }

  Meta.StatementHandle finagle(Meta.StatementHandle h) {
    final Meta.Signature signature = finagle(h.signature);
    if (signature == h.signature) {
      return h;
    }
    return new Meta.StatementHandle(h.connectionId, h.id, signature);
  }

  ResultSetResponse finagle(ResultSetResponse r) {
    if (r.updateCount != -1) {
      assert r.signature == null;
      return r;
    }
    if (r.signature == null) {
      return r;
    }
    final Meta.Signature signature = finagle(r.signature);
    if (signature == r.signature) {
      return r;
    }
    return new ResultSetResponse(r.connectionId, r.statementId, r.ownStatement,
        signature, r.firstFrame, r.updateCount, rpcMetadata);
  }

  ExecuteResponse finagle(ExecuteResponse r) {
    if (r.missingStatement) {
      return r;
    }
    final List<ResultSetResponse> results = new ArrayList<>();
    int changeCount = 0;
    for (ResultSetResponse result : r.results) {
      ResultSetResponse result2 = finagle(result);
      if (result2 != result) {
        ++changeCount;
      }
      results.add(result2);
    }
    if (changeCount == 0) {
      return r;
    }
    return new ExecuteResponse(results, r.missingStatement, rpcMetadata);
  }

  @Override public void setRpcMetadata(RpcMetadataResponse metadata) {
    // OK if this is null
    this.rpcMetadata = metadata;
  }
}

// End AbstractService.java
