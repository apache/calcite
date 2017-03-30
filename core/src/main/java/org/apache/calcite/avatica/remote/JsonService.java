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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.StringWriter;

/**
 * Implementation of {@link org.apache.calcite.avatica.remote.Service}
 * that encodes requests and responses as JSON.
 */
public abstract class JsonService extends AbstractService {
  public static final ObjectMapper MAPPER;
  static {
    MAPPER = new ObjectMapper();
    MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    MAPPER.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    MAPPER.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
  }

  public JsonService() {
  }

  /** Derived class should implement this method to transport requests and
   * responses to and from the peer service. */
  public abstract String apply(String request);

  @Override SerializationType getSerializationType() {
    return SerializationType.JSON;
  }

  //@VisibleForTesting
  protected static <T> T decode(String response, Class<T> expectedType)
      throws IOException {
    Response resp = MAPPER.readValue(response, Response.class);
    if (resp instanceof ErrorResponse) {
      throw ((ErrorResponse) resp).toException();
    } else if (!expectedType.isAssignableFrom(resp.getClass())) {
      throw new ClassCastException("Cannot cast " + resp.getClass() + " into " + expectedType);
    }

    return expectedType.cast(resp);
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
      return finagle(decode(apply(encode(request)), ResultSetResponse.class));
    } catch (IOException e) {
      throw handle(e);
    }
  }

  public ResultSetResponse apply(SchemasRequest request) {
    try {
      return finagle(decode(apply(encode(request)), ResultSetResponse.class));
    } catch (IOException e) {
      throw handle(e);
    }
  }

  public ResultSetResponse apply(TablesRequest request) {
    try {
      return finagle(decode(apply(encode(request)), ResultSetResponse.class));
    } catch (IOException e) {
      throw handle(e);
    }
  }

  public ResultSetResponse apply(TableTypesRequest request) {
    try {
      return finagle(decode(apply(encode(request)), ResultSetResponse.class));
    } catch (IOException e) {
      throw handle(e);
    }
  }

  public ResultSetResponse apply(TypeInfoRequest request) {
    try {
      return finagle(decode(apply(encode(request)), ResultSetResponse.class));
    } catch (IOException e) {
      throw handle(e);
    }
  }

  public ResultSetResponse apply(ColumnsRequest request) {
    try {
      return finagle(decode(apply(encode(request)), ResultSetResponse.class));
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

  public ExecuteResponse apply(ExecuteRequest request) {
    try {
      return finagle(decode(apply(encode(request)), ExecuteResponse.class));
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

  public OpenConnectionResponse apply(OpenConnectionRequest request) {
    try {
      return decode(apply(encode(request)), OpenConnectionResponse.class);
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

  public SyncResultsResponse apply(SyncResultsRequest request) {
    try {
      return decode(apply(encode(request)), SyncResultsResponse.class);
    } catch (IOException e) {
      throw handle(e);
    }
  }

  public CommitResponse apply(CommitRequest request) {
    try {
      return decode(apply(encode(request)), CommitResponse.class);
    } catch (IOException e) {
      throw handle(e);
    }
  }

  public RollbackResponse apply(RollbackRequest request) {
    try {
      return decode(apply(encode(request)), RollbackResponse.class);
    } catch (IOException e) {
      throw handle(e);
    }
  }

  public ExecuteBatchResponse apply(PrepareAndExecuteBatchRequest request) {
    try {
      return decode(apply(encode(request)), ExecuteBatchResponse.class);
    } catch (IOException e) {
      throw handle(e);
    }
  }

  public ExecuteBatchResponse apply(ExecuteBatchRequest request) {
    try {
      return decode(apply(encode(request)), ExecuteBatchResponse.class);
    } catch (IOException e) {
      throw handle(e);
    }
  }
}

// End JsonService.java
