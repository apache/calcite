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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link org.apache.calcite.avatica.remote.Service}
 * that encodes requests and responses as JSON.
 */
public abstract class JsonService implements Service {
  protected static final ObjectMapper MAPPER;
  static {
    MAPPER = new ObjectMapper();
    MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    MAPPER.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
  }

  public JsonService() {
  }

  /** Derived class should implement this method to transport requests and
   * responses to and from the peer service. */
  public abstract String apply(String request);

  /** Modifies a signature, changing the representation of numeric columns
   * within it. This deals with the fact that JSON transmits a small long value,
   * or a float which is a whole number, as an integer. Thus the accessors need
   * be prepared to accept any numeric type. */
  private static Meta.Signature finagle(Meta.Signature signature) {
    final List<ColumnMetaData> columns = new ArrayList<>();
    for (ColumnMetaData column : signature.columns) {
      columns.add(finagle(column));
    }
    if (columns.equals(signature.columns)) {
      return signature;
    }
    return new Meta.Signature(columns, signature.sql,
        signature.parameters, signature.internalParameters,
        signature.cursorFactory);
  }

  private static ColumnMetaData finagle(ColumnMetaData column) {
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
    }
    switch (column.type.id) {
    case Types.VARBINARY:
    case Types.BINARY:
      return column.setRep(ColumnMetaData.Rep.STRING);
    case Types.DECIMAL:
    case Types.NUMERIC:
      return column.setRep(ColumnMetaData.Rep.NUMBER);
    default:
      return column;
    }
  }

  private static PrepareResponse finagle(PrepareResponse response) {
    final Meta.StatementHandle statement = finagle(response.statement);
    if (statement == response.statement) {
      return response;
    }
    return new PrepareResponse(statement);
  }

  private static Meta.StatementHandle finagle(Meta.StatementHandle h) {
    final Meta.Signature signature = finagle(h.signature);
    if (signature == h.signature) {
      return h;
    }
    return new Meta.StatementHandle(h.connectionId, h.id, signature);
  }

  private static ResultSetResponse finagle(ResultSetResponse r) {
    if (r.updateCount != -1) {
      assert r.signature == null;
      return r;
    }
    final Meta.Signature signature = finagle(r.signature);
    if (signature == r.signature) {
      return r;
    }
    return new ResultSetResponse(r.connectionId, r.statementId, r.ownStatement,
        signature, r.firstFrame, r.updateCount);
  }

  private static ExecuteResponse finagle(ExecuteResponse r) {
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
    return new ExecuteResponse(results);
  }

  //@VisibleForTesting
  protected static <T> T decode(String response, Class<T> valueType)
      throws IOException {
    return MAPPER.readValue(response, valueType);
  }

  //@VisibleForTesting
  protected static <T> String encode(T request) throws IOException {
    final StringWriter w = new StringWriter();
    MAPPER.writeValue(w, request);
    return w.toString();
  }

  protected RuntimeException handle(IOException e) {
    return new RuntimeException(e);
  }

  public ResultSetResponse apply(CatalogsRequest request) {
    try {
      return decode(apply(encode(request)), ResultSetResponse.class);
    } catch (IOException e) {
      throw handle(e);
    }
  }

  public ResultSetResponse apply(SchemasRequest request) {
    try {
      return decode(apply(encode(request)), ResultSetResponse.class);
    } catch (IOException e) {
      throw handle(e);
    }
  }

  public ResultSetResponse apply(TablesRequest request) {
    try {
      return decode(apply(encode(request)), ResultSetResponse.class);
    } catch (IOException e) {
      throw handle(e);
    }
  }

  public ResultSetResponse apply(TableTypesRequest request) {
    try {
      return decode(apply(encode(request)), ResultSetResponse.class);
    } catch (IOException e) {
      throw handle(e);
    }
  }

  public ResultSetResponse apply(TypeInfoRequest request) {
    try {
      return decode(apply(encode(request)), ResultSetResponse.class);
    } catch (IOException e) {
      throw handle(e);
    }
  }

  public ResultSetResponse apply(ColumnsRequest request) {
    try {
      return decode(apply(encode(request)), ResultSetResponse.class);
    } catch (IOException e) {
      throw handle(e);
    }
  }

  public PrepareResponse apply(PrepareRequest request) {
    try {
      return finagle(decode(apply(encode(request)), PrepareResponse.class));
    } catch (IOException e) {
      throw handle(e);
    }
  }

  public ExecuteResponse apply(PrepareAndExecuteRequest request) {
    try {
      return finagle(decode(apply(encode(request)), ExecuteResponse.class));
    } catch (IOException e) {
      throw handle(e);
    }
  }

  public FetchResponse apply(FetchRequest request) {
    try {
      return decode(apply(encode(request)), FetchResponse.class);
    } catch (IOException e) {
      throw handle(e);
    }
  }

  public CreateStatementResponse apply(CreateStatementRequest request) {
    try {
      return decode(apply(encode(request)), CreateStatementResponse.class);
    } catch (IOException e) {
      throw handle(e);
    }
  }

  public CloseStatementResponse apply(CloseStatementRequest request) {
    try {
      return decode(apply(encode(request)), CloseStatementResponse.class);
    } catch (IOException e) {
      throw handle(e);
    }
  }

  public CloseConnectionResponse apply(CloseConnectionRequest request) {
    try {
      return decode(apply(encode(request)), CloseConnectionResponse.class);
    } catch (IOException e) {
      throw handle(e);
    }
  }

  public ConnectionSyncResponse apply(ConnectionSyncRequest request) {
    try {
      return decode(apply(encode(request)), ConnectionSyncResponse.class);
    } catch (IOException e) {
      throw handle(e);
    }
  }

  public DatabasePropertyResponse apply(DatabasePropertyRequest request) {
    try {
      return decode(apply(encode(request)), DatabasePropertyResponse.class);
    } catch (IOException e) {
      throw handle(e);
    }
  }
}

// End JsonService.java
