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

  private static final Map<Request, Response> MAPPING;
  static {
    HashMap<Request, Response> mappings = new HashMap<>();

    // Add in mappings

    // Get the schema, no.. schema..?
    mappings.put(
        new SchemasRequest(null, null),
        new ResultSetResponse(null, 1, true, null, Meta.Frame.EMPTY, -1));

    // Get the tables, no tables exist
    mappings.put(new TablesRequest(null, null, null, Collections.<String>emptyList()),
        new ResultSetResponse(null, 150, true, null, Meta.Frame.EMPTY, -1));

    // Create a statement, get back an id
    mappings.put(new CreateStatementRequest("0"), new CreateStatementResponse("0", 1));

    // Prepare and execute a query. Values and schema are returned
    mappings.put(
        new PrepareAndExecuteRequest("0", 1,
            "select * from (\\n values (1, 'a'), (null, 'b'), (3, 'c')) as t (c1, c2)", -1),
        new ResultSetResponse("0", 1, true,
            Meta.Signature.create(
                Arrays.<ColumnMetaData>asList(
                    MetaImpl.columnMetaData("C1", 0, Integer.class),
                    MetaImpl.columnMetaData("C2", 1, String.class)),
                null, null, Meta.CursorFactory.ARRAY, Meta.StatementType.SELECT),
            Meta.Frame.create(0, true,
                Arrays.<Object>asList(new Object[] {1, "a"},
                    new Object[] {null, "b"}, new Object[] {3, "c"})), -1));

    // Prepare a query. Schema for results are returned, but no values
    mappings.put(
        new PrepareRequest("0",
            "select * from (\\n values(1, 'a'), (null, 'b'), (3, 'c')), as t (c1, c2)", -1),
        new ResultSetResponse("0", 1, true,
            Meta.Signature.create(
                Arrays.<ColumnMetaData>asList(
                    MetaImpl.columnMetaData("C1", 0, Integer.class),
                    MetaImpl.columnMetaData("C2", 1, String.class)),
                null, Collections.<AvaticaParameter>emptyList(),
                Meta.CursorFactory.ARRAY, Meta.StatementType.SELECT),
            null, -1));

    MAPPING = Collections.unmodifiableMap(mappings);
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
    Response response = MAPPING.get(request);

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
      return new MockProtobufService();
    }
  }
}

// End MockProtobufService.java
