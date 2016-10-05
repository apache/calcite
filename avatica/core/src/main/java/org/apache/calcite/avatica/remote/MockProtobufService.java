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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A mock implementation of ProtobufService for testing.
 *
 * <p>It performs no serialization of requests and responses.
 */
public class MockProtobufService extends ProtobufService {

  private final String connectionId;
  private final Map<Request, Response> mapping;

  public MockProtobufService(String connectionId) {
    this.connectionId = connectionId;
    this.mapping = createMapping();
  }

  private Map<Request, Response> createMapping() {
    HashMap<Request, Response> mappings = new HashMap<>();

    // Add in mappings

    mappings.put(
        new OpenConnectionRequest(connectionId, new HashMap<String, String>()),
        new OpenConnectionResponse());

    // Get the schema, no.. schema..?
    mappings.put(
        new SchemasRequest(connectionId, null, null),
        // ownStatement=false just to avoid the extra close statement call.
        new ResultSetResponse(null, 1, false, null, Meta.Frame.EMPTY, -1, null));

    // Get the tables, no tables exist
    mappings.put(new TablesRequest(connectionId, null, null, null, Collections.<String>emptyList()),
        // ownStatement=false just to avoid the extra close statement call.
        new ResultSetResponse(null, 150, false, null, Meta.Frame.EMPTY, -1, null));

    // Create a statement, get back an id
    mappings.put(new CreateStatementRequest("0"), new CreateStatementResponse("0", 1, null));

    // Prepare and execute a query. Values and schema are returned
    mappings.put(
        new PrepareAndExecuteRequest(connectionId, 1,
            "select * from (\\n values (1, 'a'), (null, 'b'), (3, 'c')) as t (c1, c2)", -1),
        new ResultSetResponse("0", 1, true,
            Meta.Signature.create(
                Arrays.<ColumnMetaData>asList(
                    MetaImpl.columnMetaData("C1", 0, Integer.class, true),
                    MetaImpl.columnMetaData("C2", 1, String.class, true)),
                null, null, Meta.CursorFactory.ARRAY, Meta.StatementType.SELECT),
            Meta.Frame.create(0, true,
                Arrays.<Object>asList(new Object[] {1, "a"},
                    new Object[] {null, "b"}, new Object[] {3, "c"})), -1, null));

    // Prepare a query. Schema for results are returned, but no values
    mappings.put(
        new PrepareRequest(connectionId,
            "select * from (\\n values(1, 'a'), (null, 'b'), (3, 'c')), as t (c1, c2)", -1),
        new ResultSetResponse("0", 1, true,
            Meta.Signature.create(
                Arrays.<ColumnMetaData>asList(
                    MetaImpl.columnMetaData("C1", 0, Integer.class, true),
                    MetaImpl.columnMetaData("C2", 1, String.class, true)),
                null, Collections.<AvaticaParameter>emptyList(),
                Meta.CursorFactory.ARRAY, Meta.StatementType.SELECT),
            null, -1, null));

    mappings.put(
        new ColumnsRequest(connectionId, null, null, "my_table", null),
        new ResultSetResponse("00000000-0000-0000-0000-000000000000", -1, true,
            Meta.Signature.create(
                Arrays.<ColumnMetaData>asList(
                    MetaImpl.columnMetaData("TABLE_NAME", 0, String.class, true),
                    MetaImpl.columnMetaData("ORDINAL_POSITION", 1, Long.class, true)), null,
                Collections.<AvaticaParameter>emptyList(), Meta.CursorFactory.ARRAY, null),
            Meta.Frame.create(0, true,
                Arrays.<Object>asList(new Object[] {new Object[]{"my_table", 10}})), -1, null));

    return Collections.unmodifiableMap(mappings);
  }

  @Override public Response _apply(Request request) {
    if (request instanceof CloseConnectionRequest) {
      return new CloseConnectionResponse();
    }

    return dispatch(request);
  }

  /**
   * Fetches the static response for the given request.
   *
   * @param request the client's request
   * @return the appropriate response
   * @throws RuntimeException if no mapping is found for the request
   */
  private Response dispatch(Request request) {
    Response response = mapping.get(request);

    if (null == response) {
      throw new RuntimeException("Had no response mapping for " + request);
    }

    return response;
  }

  /**
   * A factory that instantiates the mock protobuf service.
   */
  public static class MockProtobufServiceFactory implements Service.Factory {
    @Override public Service create(AvaticaConnection connection) {
      return new MockProtobufService(connection.id);
    }
  }
}

// End MockProtobufService.java
