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
import org.apache.calcite.avatica.AvaticaSeverity;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.ArrayType;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.ColumnMetaData.ScalarType;
import org.apache.calcite.avatica.ConnectionPropertiesImpl;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.Style;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.Service.CatalogsRequest;
import org.apache.calcite.avatica.remote.Service.CloseConnectionRequest;
import org.apache.calcite.avatica.remote.Service.CloseConnectionResponse;
import org.apache.calcite.avatica.remote.Service.CloseStatementRequest;
import org.apache.calcite.avatica.remote.Service.CloseStatementResponse;
import org.apache.calcite.avatica.remote.Service.ColumnsRequest;
import org.apache.calcite.avatica.remote.Service.CommitRequest;
import org.apache.calcite.avatica.remote.Service.CommitResponse;
import org.apache.calcite.avatica.remote.Service.ConnectionSyncRequest;
import org.apache.calcite.avatica.remote.Service.ConnectionSyncResponse;
import org.apache.calcite.avatica.remote.Service.CreateStatementRequest;
import org.apache.calcite.avatica.remote.Service.CreateStatementResponse;
import org.apache.calcite.avatica.remote.Service.DatabasePropertyRequest;
import org.apache.calcite.avatica.remote.Service.DatabasePropertyResponse;
import org.apache.calcite.avatica.remote.Service.ErrorResponse;
import org.apache.calcite.avatica.remote.Service.ExecuteBatchResponse;
import org.apache.calcite.avatica.remote.Service.ExecuteResponse;
import org.apache.calcite.avatica.remote.Service.FetchRequest;
import org.apache.calcite.avatica.remote.Service.FetchResponse;
import org.apache.calcite.avatica.remote.Service.OpenConnectionRequest;
import org.apache.calcite.avatica.remote.Service.OpenConnectionResponse;
import org.apache.calcite.avatica.remote.Service.PrepareAndExecuteBatchRequest;
import org.apache.calcite.avatica.remote.Service.PrepareAndExecuteRequest;
import org.apache.calcite.avatica.remote.Service.PrepareRequest;
import org.apache.calcite.avatica.remote.Service.PrepareResponse;
import org.apache.calcite.avatica.remote.Service.Request;
import org.apache.calcite.avatica.remote.Service.Response;
import org.apache.calcite.avatica.remote.Service.ResultSetResponse;
import org.apache.calcite.avatica.remote.Service.RollbackRequest;
import org.apache.calcite.avatica.remote.Service.RollbackResponse;
import org.apache.calcite.avatica.remote.Service.RpcMetadataResponse;
import org.apache.calcite.avatica.remote.Service.SchemasRequest;
import org.apache.calcite.avatica.remote.Service.SyncResultsRequest;
import org.apache.calcite.avatica.remote.Service.SyncResultsResponse;
import org.apache.calcite.avatica.remote.Service.TableTypesRequest;
import org.apache.calcite.avatica.remote.Service.TablesRequest;
import org.apache.calcite.avatica.remote.Service.TypeInfoRequest;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.DatabaseMetaData;
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
    requests.add(new SchemasRequest("connectionId", "catalog", "schemaPattern"));
    requests.add(
        new TablesRequest("connectionId", "catalog", "schemaPattern", "tableNamePattern",
            Arrays.asList("STRING", "BOOLEAN", "INT")));
    requests.add(new TableTypesRequest());
    requests.add(
        new ColumnsRequest("connectionId", "catalog", "schemaPattern", "tableNamePattern",
            "columnNamePattern"));
    requests.add(new TypeInfoRequest());
    requests.add(
        new PrepareAndExecuteRequest("connectionId", Integer.MAX_VALUE, "sql",
            Long.MAX_VALUE));
    requests.add(new PrepareRequest("connectionId", "sql", Long.MAX_VALUE));

    List<TypedValue> paramValues =
        Arrays.asList(TypedValue.create(Rep.BOOLEAN.name(), Boolean.TRUE),
            TypedValue.create(Rep.STRING.name(), "string"));
    FetchRequest fetchRequest = new FetchRequest("connectionId", Integer.MAX_VALUE,
        Long.MAX_VALUE, Integer.MAX_VALUE);
    requests.add(fetchRequest);

    requests.add(new CreateStatementRequest("connectionId"));
    requests.add(new CloseStatementRequest("connectionId", Integer.MAX_VALUE));
    Map<String, String> info = new HashMap<>();
    info.put("param1", "value1");
    info.put("param2", "value2");
    requests.add(new OpenConnectionRequest("connectionId", info));
    requests.add(new CloseConnectionRequest("connectionId"));
    requests.add(
        new ConnectionSyncRequest("connectionId",
            new ConnectionPropertiesImpl(Boolean.FALSE, Boolean.FALSE,
                Integer.MAX_VALUE, "catalog", "schema")));

    requests.add(new SyncResultsRequest("connectionId", 12345, getSqlQueryState(), 150));
    requests.add(new SyncResultsRequest("connectionId2", 54321, getMetadataQueryState1(), 0));
    requests.add(new SyncResultsRequest("connectionId3", 5, getMetadataQueryState2(), 10));

    requests.add(new CommitRequest("connectionId"));
    requests.add(new RollbackRequest("connectionId"));

    // ExecuteBatchRequest omitted because of the special protobuf conversion it does

    List<String> commands = Arrays.asList("command1", "command2", "command3");
    requests.add(new PrepareAndExecuteBatchRequest("connectionId", 12345, commands));

    return requests;
  }

  private static QueryState getSqlQueryState() {
    return new QueryState("SELECT * from TABLE");
  }

  private static QueryState getMetadataQueryState1() {
    return new QueryState(MetaDataOperation.GET_COLUMNS, new Object[] {
      "",
      null,
      "%",
      "%"
    });
  }

  private static QueryState getMetadataQueryState2() {
    return new QueryState(MetaDataOperation.GET_CATALOGS, new Object[0]);
  }

  private static List<Request> getRequestsWithNulls() {
    LinkedList<Request> requests = new LinkedList<>();

    // We're pretty fast and loose on what can be null.
    requests.add(new SchemasRequest(null, null, null));
    // Repeated fields default to an empty list
    requests.add(new TablesRequest(null, null, null, null, Collections.<String>emptyList()));
    requests.add(new ColumnsRequest(null, null, null, null, null));
    requests.add(new PrepareAndExecuteRequest(null, 0, null, 0));
    requests.add(new PrepareRequest(null, null, 0));
    requests.add(new CreateStatementRequest(null));
    requests.add(new CloseStatementRequest(null, 0));
    requests.add(new OpenConnectionRequest(null, null));
    requests.add(new CloseConnectionRequest(null));
    requests.add(new ConnectionSyncRequest(null, null));

    return requests;
  }

  private static ColumnMetaData getArrayColumnMetaData(ScalarType componentType, int index,
      String name) {
    ArrayType arrayType = ColumnMetaData.array(componentType, "Array", Rep.ARRAY);
    return new ColumnMetaData(
        index, false, true, false, false, DatabaseMetaData.columnNullable,
        true, -1, name, name, null,
        0, 0, null, null, arrayType, true, false, false,
        "ARRAY");
  }

  /**
   * Generates a collection of Responses whose serialization will be tested.
   */
  private static List<Response> getResponses() {
    final RpcMetadataResponse rpcMetadata = new RpcMetadataResponse("localhost:8765");
    LinkedList<Response> responses = new LinkedList<>();

    // Nested classes (Signature, ColumnMetaData, CursorFactory, etc) are implicitly getting tested)

    // Stub out the metadata for a row
    ScalarType arrayComponentType = ColumnMetaData.scalar(Types.INTEGER, "integer", Rep.INTEGER);
    ColumnMetaData arrayColumnMetaData = getArrayColumnMetaData(arrayComponentType, 2, "counts");
    List<ColumnMetaData> columns =
        Arrays.asList(MetaImpl.columnMetaData("str", 0, String.class, true),
            MetaImpl.columnMetaData("count", 1, Integer.class, true),
            arrayColumnMetaData);
    List<AvaticaParameter> params =
        Arrays.asList(
            new AvaticaParameter(false, 10, 0, Types.VARCHAR, "VARCHAR",
                String.class.getName(), "str"));
    Meta.CursorFactory cursorFactory = Meta.CursorFactory.create(Style.LIST, Object.class,
        Arrays.asList("str", "count", "counts"));
    // The row values
    List<Object> rows = new ArrayList<>();
    rows.add(new Object[] {"str_value1", 50, Arrays.asList(1, 2, 3)});
    rows.add(new Object[] {"str_value2", 100, Arrays.asList(1)});

    // Create the signature and frame using the metadata and values
    Signature signature = Signature.create(columns, "sql", params, cursorFactory,
        Meta.StatementType.SELECT);
    Frame frame = Frame.create(Integer.MAX_VALUE, true, rows);

    // And then create a ResultSetResponse
    ResultSetResponse results1 = new ResultSetResponse("connectionId", Integer.MAX_VALUE, true,
        signature, frame, Long.MAX_VALUE, rpcMetadata);
    responses.add(results1);

    responses.add(new CloseStatementResponse(rpcMetadata));

    ConnectionPropertiesImpl connProps = new ConnectionPropertiesImpl(false, true,
        Integer.MAX_VALUE, "catalog", "schema");
    responses.add(new ConnectionSyncResponse(connProps, rpcMetadata));

    responses.add(new OpenConnectionResponse(rpcMetadata));
    responses.add(new CloseConnectionResponse(rpcMetadata));

    responses.add(new CreateStatementResponse("connectionId", Integer.MAX_VALUE, rpcMetadata));

    Map<Meta.DatabaseProperty, Object> propertyMap = new HashMap<>();
    for (Meta.DatabaseProperty prop : Meta.DatabaseProperty.values()) {
      propertyMap.put(prop, prop.defaultValue);
    }
    responses.add(new DatabasePropertyResponse(propertyMap, rpcMetadata));

    responses.add(
        new ExecuteResponse(Arrays.asList(results1, results1, results1), false, rpcMetadata));
    responses.add(new FetchResponse(frame, false, false, rpcMetadata));
    responses.add(new FetchResponse(frame, true, true, rpcMetadata));
    responses.add(new FetchResponse(frame, false, true, rpcMetadata));
    responses.add(
        new PrepareResponse(
            new Meta.StatementHandle("connectionId", Integer.MAX_VALUE, signature),
            rpcMetadata));

    StringWriter sw = new StringWriter();
    new Exception().printStackTrace(new PrintWriter(sw));
    responses.add(
        new ErrorResponse(Collections.singletonList(sw.toString()), "Test Error Message",
            ErrorResponse.UNKNOWN_ERROR_CODE, ErrorResponse.UNKNOWN_SQL_STATE,
            AvaticaSeverity.WARNING, rpcMetadata));

    // No more results, statement not missing
    responses.add(new SyncResultsResponse(false, false, rpcMetadata));
    // Missing statement, no results
    responses.add(new SyncResultsResponse(false, true, rpcMetadata));
    // More results, no missing statement
    responses.add(new SyncResultsResponse(true, false, rpcMetadata));

    // Some tests to make sure ErrorResponse doesn't fail.
    responses.add(new ErrorResponse((List<String>) null, null, 0, null, null, null));
    responses.add(
        new ErrorResponse(Arrays.asList("stacktrace1", "stacktrace2"), null, 0, null, null, null));

    responses.add(new CommitResponse());
    responses.add(new RollbackResponse());

    long[] updateCounts = new long[]{1, 0, 1, 1};
    responses.add(
        new ExecuteBatchResponse("connectionId", 12345, updateCounts, false, rpcMetadata));

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
