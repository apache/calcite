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

import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.ConnectionPropertiesImpl;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.Style;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.remote.Service.CatalogsRequest;
import org.apache.calcite.avatica.remote.Service.CloseConnectionRequest;
import org.apache.calcite.avatica.remote.Service.CloseStatementRequest;
import org.apache.calcite.avatica.remote.Service.CloseStatementResponse;
import org.apache.calcite.avatica.remote.Service.ColumnsRequest;
import org.apache.calcite.avatica.remote.Service.ConnectionSyncRequest;
import org.apache.calcite.avatica.remote.Service.ConnectionSyncResponse;
import org.apache.calcite.avatica.remote.Service.CreateStatementRequest;
import org.apache.calcite.avatica.remote.Service.CreateStatementResponse;
import org.apache.calcite.avatica.remote.Service.DatabasePropertyRequest;
import org.apache.calcite.avatica.remote.Service.DatabasePropertyResponse;
import org.apache.calcite.avatica.remote.Service.ExecuteResponse;
import org.apache.calcite.avatica.remote.Service.FetchRequest;
import org.apache.calcite.avatica.remote.Service.FetchResponse;
import org.apache.calcite.avatica.remote.Service.PrepareAndExecuteRequest;
import org.apache.calcite.avatica.remote.Service.PrepareRequest;
import org.apache.calcite.avatica.remote.Service.PrepareResponse;
import org.apache.calcite.avatica.remote.Service.Request;
import org.apache.calcite.avatica.remote.Service.Response;
import org.apache.calcite.avatica.remote.Service.ResultSetResponse;
import org.apache.calcite.avatica.remote.Service.SchemasRequest;
import org.apache.calcite.avatica.remote.Service.TableTypesRequest;
import org.apache.calcite.avatica.remote.Service.TablesRequest;
import org.apache.calcite.avatica.remote.Service.TypeInfoRequest;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests serialization of requests and response objects.
 *
 * @param <T> The object class being tested
 */
@RunWith(Parameterized.class)
public class ProtobufTranslationImplTest<T> {

  /**
   * Simple function definition that acts as an identity.
   *
   * @param <A> Argument type
   */
  private interface IdentityFunction<A> {
    A apply(A obj) throws IOException;
  }

  /**
   * Identity function that accepts a request, serializes it to protobuf, and converts it back.
   */
  private static class RequestFunc implements IdentityFunction<Request> {
    private final ProtobufTranslation translation;

    public RequestFunc(ProtobufTranslation translation) {
      this.translation = translation;
    }

    public Request apply(Request request) throws IOException {
      // Serialize and then re-parse the request
      return translation.parseRequest(translation.serializeRequest(request));
    }
  }

  /**
   * Identity function that accepts a response, serializes it to protobuf, and converts it back.
   */
  private static class ResponseFunc implements IdentityFunction<Response> {
    private final ProtobufTranslation translation;

    public ResponseFunc(ProtobufTranslation translation) {
      this.translation = translation;
    }

    public Response apply(Response response) throws IOException {
      // Serialize and then re-pare the response
      return translation.parseResponse(translation.serializeResponse(response));
    }
  }

  @Parameters
  public static List<Object[]> parameters() {
    List<Object[]> params = new ArrayList<>();

    // The impl we're testing
    ProtobufTranslationImpl translation = new ProtobufTranslationImpl();

    // Identity transformation for Requests
    RequestFunc requestFunc = new RequestFunc(translation);
    // Identity transformation for Responses
    ResponseFunc responseFunc = new ResponseFunc(translation);

    List<Request> requests = getRequests();
    List<Request> requestsWithNulls = getRequestsWithNulls();
    List<Response> responses = getResponses();

    // Requests
    for (Request request : requests) {
      params.add(new Object[] {request, requestFunc});
    }

    // Requests with nulls in parameters
    for (Request request : requestsWithNulls) {
      params.add(new Object[] {request, requestFunc});
    }

    // Responses
    for (Response response : responses) {
      params.add(new Object[] {response, responseFunc});
    }

    return params;
  }

  /**
   * Generates a collection of Requests whose serialization will be tested.
   */
  private static List<Request> getRequests() {
    LinkedList<Request> requests = new LinkedList<>();

    requests.add(new CatalogsRequest());
    requests.add(new DatabasePropertyRequest());
    requests.add(new SchemasRequest("catalog", "schemaPattern"));
    requests.add(
        new TablesRequest("catalog", "schemaPattern", "tableNamePattern",
            Arrays.asList("STRING", "BOOLEAN", "INT")));
    requests.add(new TableTypesRequest());
    requests.add(
        new ColumnsRequest("catalog", "schemaPattern", "tableNamePattern",
            "columnNamePattern"));
    requests.add(new TypeInfoRequest());
    requests.add(
        new PrepareAndExecuteRequest("connectionId", Integer.MAX_VALUE, "sql",
            Long.MAX_VALUE));
    requests.add(new PrepareRequest("connectionId", "sql", Long.MAX_VALUE));

    List<TypedValue> paramValues =
        Arrays.asList(TypedValue.create(Rep.BOOLEAN.name(), Boolean.TRUE),
            TypedValue.create(Rep.STRING.name(), "string"));
    FetchRequest fetchRequest = new FetchRequest("connectionId", Integer.MAX_VALUE, paramValues,
        Long.MAX_VALUE, Integer.MAX_VALUE);
    requests.add(fetchRequest);

    requests.add(new CreateStatementRequest("connectionId"));
    requests.add(new CloseStatementRequest("connectionId", Integer.MAX_VALUE));
    requests.add(new CloseConnectionRequest("connectionId"));
    requests.add(
        new ConnectionSyncRequest("connectionId",
            new ConnectionPropertiesImpl(Boolean.FALSE, Boolean.FALSE,
                Integer.MAX_VALUE, "catalog", "schema")));

    return requests;
  }

  private static List<Request> getRequestsWithNulls() {
    LinkedList<Request> requests = new LinkedList<>();

    // We're pretty fast and loose on what can be null.
    requests.add(new SchemasRequest(null, null));
    // Repeated fields default to an empty list
    requests.add(new TablesRequest(null, null, null, Collections.<String>emptyList()));
    requests.add(new ColumnsRequest(null, null, null, null));
    requests.add(new PrepareAndExecuteRequest(null, 0, null, 0));
    requests.add(new PrepareRequest(null, null, 0));
    requests.add(new CreateStatementRequest(null));
    requests.add(new CloseStatementRequest(null, 0));
    requests.add(new CloseConnectionRequest(null));
    requests.add(new ConnectionSyncRequest(null, null));

    return requests;
  }

  /**
   * Generates a collection of Responses whose serialization will be tested.
   */
  private static List<Response> getResponses() {
    LinkedList<Response> responses = new LinkedList<>();

    // Nested classes (Signature, ColumnMetaData, CursorFactory, etc) are implicitly getting tested)

    // Stub out the metadata for a row
    List<ColumnMetaData> columns =
        Arrays.<ColumnMetaData>asList(MetaImpl.columnMetaData("str", 0, String.class),
            MetaImpl.columnMetaData("count", 1, Integer.class));
    List<AvaticaParameter> params =
        Arrays.asList(
            new AvaticaParameter(false, 10, 0, Types.VARCHAR, "VARCHAR",
                String.class.getName(), "str"));
    Meta.CursorFactory cursorFactory = Meta.CursorFactory.create(Style.LIST, Object.class,
        Arrays.asList("str", "count"));
    // The row values
    List<Object> rows = new ArrayList<>();
    rows.add(new Object[] {"str_value", 50});

    // Create the signature and frame using the metadata and values
    Signature signature = Signature.create(columns, "sql", params, cursorFactory);
    Frame frame = Frame.create(Integer.MAX_VALUE, true, rows);

    // And then create a ResultSetResponse
    ResultSetResponse results1 = new ResultSetResponse("connectionId", Integer.MAX_VALUE, true,
        signature, frame, Long.MAX_VALUE);
    responses.add(results1);

    responses.add(new CloseStatementResponse());

    ConnectionPropertiesImpl connProps = new ConnectionPropertiesImpl(false, true,
        Integer.MAX_VALUE, "catalog", "schema");
    responses.add(new ConnectionSyncResponse(connProps));

    responses.add(new CreateStatementResponse("connectionId", Integer.MAX_VALUE));

    Map<Meta.DatabaseProperty, Object> propertyMap = new HashMap<>();
    for (Meta.DatabaseProperty prop : Meta.DatabaseProperty.values()) {
      propertyMap.put(prop, prop.defaultValue);
    }
    responses.add(new DatabasePropertyResponse(propertyMap));

    responses.add(new ExecuteResponse(Arrays.asList(results1, results1, results1)));
    responses.add(new FetchResponse(frame));
    responses.add(
        new PrepareResponse(
            new Meta.StatementHandle("connectionId", Integer.MAX_VALUE,
                signature)));

    return responses;
  }

  private final T object;
  private final IdentityFunction<T> function;

  public ProtobufTranslationImplTest(T object, IdentityFunction<T> func) {
    this.object = object;
    this.function = func;
  }

  @Test
  public void testSerialization() throws Exception {
    // Function acts as opposite sides of the transport.
    // An object (a request or response) starts on one side
    // of the transport, serialized, "sent" over the transport
    // and then reconstituted. The object on either side should
    // be equivalent.
    assertEquals(object, this.function.apply(object));
  }
}

// End ProtobufTranslationImplTest.java
