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

import org.apache.calcite.avatica.proto.Common.WireMessage;
import org.apache.calcite.avatica.proto.Requests.CatalogsRequest;
import org.apache.calcite.avatica.proto.Requests.CloseConnectionRequest;
import org.apache.calcite.avatica.proto.Requests.CloseStatementRequest;
import org.apache.calcite.avatica.proto.Requests.ColumnsRequest;
import org.apache.calcite.avatica.proto.Requests.CommitRequest;
import org.apache.calcite.avatica.proto.Requests.ConnectionSyncRequest;
import org.apache.calcite.avatica.proto.Requests.CreateStatementRequest;
import org.apache.calcite.avatica.proto.Requests.DatabasePropertyRequest;
import org.apache.calcite.avatica.proto.Requests.ExecuteRequest;
import org.apache.calcite.avatica.proto.Requests.FetchRequest;
import org.apache.calcite.avatica.proto.Requests.OpenConnectionRequest;
import org.apache.calcite.avatica.proto.Requests.PrepareAndExecuteRequest;
import org.apache.calcite.avatica.proto.Requests.PrepareRequest;
import org.apache.calcite.avatica.proto.Requests.RollbackRequest;
import org.apache.calcite.avatica.proto.Requests.SchemasRequest;
import org.apache.calcite.avatica.proto.Requests.SyncResultsRequest;
import org.apache.calcite.avatica.proto.Requests.TableTypesRequest;
import org.apache.calcite.avatica.proto.Requests.TablesRequest;
import org.apache.calcite.avatica.proto.Requests.TypeInfoRequest;
import org.apache.calcite.avatica.proto.Responses.CloseConnectionResponse;
import org.apache.calcite.avatica.proto.Responses.CloseStatementResponse;
import org.apache.calcite.avatica.proto.Responses.CommitResponse;
import org.apache.calcite.avatica.proto.Responses.ConnectionSyncResponse;
import org.apache.calcite.avatica.proto.Responses.CreateStatementResponse;
import org.apache.calcite.avatica.proto.Responses.DatabasePropertyResponse;
import org.apache.calcite.avatica.proto.Responses.ErrorResponse;
import org.apache.calcite.avatica.proto.Responses.ExecuteResponse;
import org.apache.calcite.avatica.proto.Responses.FetchResponse;
import org.apache.calcite.avatica.proto.Responses.OpenConnectionResponse;
import org.apache.calcite.avatica.proto.Responses.PrepareResponse;
import org.apache.calcite.avatica.proto.Responses.ResultSetResponse;
import org.apache.calcite.avatica.proto.Responses.RollbackResponse;
import org.apache.calcite.avatica.proto.Responses.RpcMetadata;
import org.apache.calcite.avatica.proto.Responses.SyncResultsResponse;
import org.apache.calcite.avatica.remote.Service.Request;
import org.apache.calcite.avatica.remote.Service.Response;
import org.apache.calcite.avatica.remote.Service.RpcMetadataResponse;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of {@link ProtobufTranslationImpl} that translates
 * protobuf requests to POJO requests.
 */
public class ProtobufTranslationImpl implements ProtobufTranslation {

  // Extremely ugly mapping of PB class name into a means to convert it to the POJO
  private static final Map<String, RequestTranslator> REQUEST_PARSERS;
  private static final Map<String, ResponseTranslator> RESPONSE_PARSERS;

  static {
    HashMap<String, RequestTranslator> reqParsers = new HashMap<>();
    reqParsers.put(CatalogsRequest.class.getName(),
        new RequestTranslator(CatalogsRequest.parser(), new Service.CatalogsRequest()));
    reqParsers.put(OpenConnectionRequest.class.getName(),
        new RequestTranslator(OpenConnectionRequest.parser(), new Service.OpenConnectionRequest()));
    reqParsers.put(CloseConnectionRequest.class.getName(),
        new RequestTranslator(CloseConnectionRequest.parser(),
          new Service.CloseConnectionRequest()));
    reqParsers.put(CloseStatementRequest.class.getName(),
        new RequestTranslator(CloseStatementRequest.parser(), new Service.CloseStatementRequest()));
    reqParsers.put(ColumnsRequest.class.getName(),
        new RequestTranslator(ColumnsRequest.parser(), new Service.ColumnsRequest()));
    reqParsers.put(ConnectionSyncRequest.class.getName(),
        new RequestTranslator(ConnectionSyncRequest.parser(), new Service.ConnectionSyncRequest()));
    reqParsers.put(CreateStatementRequest.class.getName(),
        new RequestTranslator(CreateStatementRequest.parser(),
          new Service.CreateStatementRequest()));
    reqParsers.put(DatabasePropertyRequest.class.getName(),
        new RequestTranslator(DatabasePropertyRequest.parser(),
            new Service.DatabasePropertyRequest()));
    reqParsers.put(FetchRequest.class.getName(),
        new RequestTranslator(FetchRequest.parser(), new Service.FetchRequest()));
    reqParsers.put(PrepareAndExecuteRequest.class.getName(),
        new RequestTranslator(PrepareAndExecuteRequest.parser(),
            new Service.PrepareAndExecuteRequest()));
    reqParsers.put(PrepareRequest.class.getName(),
        new RequestTranslator(PrepareRequest.parser(), new Service.PrepareRequest()));
    reqParsers.put(SchemasRequest.class.getName(),
        new RequestTranslator(SchemasRequest.parser(), new Service.SchemasRequest()));
    reqParsers.put(TablesRequest.class.getName(),
        new RequestTranslator(TablesRequest.parser(), new Service.TablesRequest()));
    reqParsers.put(TableTypesRequest.class.getName(),
        new RequestTranslator(TableTypesRequest.parser(), new Service.TableTypesRequest()));
    reqParsers.put(TypeInfoRequest.class.getName(),
        new RequestTranslator(TypeInfoRequest.parser(), new Service.TypeInfoRequest()));
    reqParsers.put(ExecuteRequest.class.getName(),
        new RequestTranslator(ExecuteRequest.parser(), new Service.ExecuteRequest()));
    reqParsers.put(SyncResultsRequest.class.getName(),
        new RequestTranslator(SyncResultsRequest.parser(), new Service.SyncResultsRequest()));
    reqParsers.put(CommitRequest.class.getName(),
        new RequestTranslator(CommitRequest.parser(), new Service.CommitRequest()));
    reqParsers.put(RollbackRequest.class.getName(),
        new RequestTranslator(RollbackRequest.parser(), new Service.RollbackRequest()));

    REQUEST_PARSERS = Collections.unmodifiableMap(reqParsers);

    HashMap<String, ResponseTranslator> respParsers = new HashMap<>();
    respParsers.put(OpenConnectionResponse.class.getName(),
        new ResponseTranslator(OpenConnectionResponse.parser(),
            new Service.OpenConnectionResponse()));
    respParsers.put(CloseConnectionResponse.class.getName(),
        new ResponseTranslator(CloseConnectionResponse.parser(),
            new Service.CloseConnectionResponse()));
    respParsers.put(CloseStatementResponse.class.getName(),
        new ResponseTranslator(CloseStatementResponse.parser(),
            new Service.CloseStatementResponse()));
    respParsers.put(ConnectionSyncResponse.class.getName(),
        new ResponseTranslator(ConnectionSyncResponse.parser(),
            new Service.ConnectionSyncResponse()));
    respParsers.put(CreateStatementResponse.class.getName(),
        new ResponseTranslator(CreateStatementResponse.parser(),
            new Service.CreateStatementResponse()));
    respParsers.put(DatabasePropertyResponse.class.getName(),
        new ResponseTranslator(DatabasePropertyResponse.parser(),
            new Service.DatabasePropertyResponse()));
    respParsers.put(ExecuteResponse.class.getName(),
        new ResponseTranslator(ExecuteResponse.parser(), new Service.ExecuteResponse()));
    respParsers.put(FetchResponse.class.getName(),
        new ResponseTranslator(FetchResponse.parser(), new Service.FetchResponse()));
    respParsers.put(PrepareResponse.class.getName(),
        new ResponseTranslator(PrepareResponse.parser(), new Service.PrepareResponse()));
    respParsers.put(ResultSetResponse.class.getName(),
        new ResponseTranslator(ResultSetResponse.parser(), new Service.ResultSetResponse()));
    respParsers.put(ErrorResponse.class.getName(),
        new ResponseTranslator(ErrorResponse.parser(), new Service.ErrorResponse()));
    respParsers.put(SyncResultsResponse.class.getName(),
        new ResponseTranslator(SyncResultsResponse.parser(), new Service.SyncResultsResponse()));
    respParsers.put(RpcMetadata.class.getName(),
        new ResponseTranslator(RpcMetadata.parser(), new RpcMetadataResponse()));
    respParsers.put(CommitResponse.class.getName(),
        new ResponseTranslator(CommitResponse.parser(), new Service.CommitResponse()));
    respParsers.put(RollbackResponse.class.getName(),
        new ResponseTranslator(RollbackResponse.parser(), new Service.RollbackResponse()));

    RESPONSE_PARSERS = Collections.unmodifiableMap(respParsers);
  }

  /**
   * Fetches the concrete message's Parser implementation.
   *
   * @param className The protocol buffer class name
   * @return The Parser for the class
   * @throws IllegalArgumentException If the argument is null or if a Parser for the given
   *     class name is not found.
   */
  public static RequestTranslator getParserForRequest(String className) {
    if (null == className) {
      throw new IllegalArgumentException("Cannot fetch parser for null class name");
    }

    RequestTranslator translator = REQUEST_PARSERS.get(className);
    if (null == translator) {
      throw new IllegalArgumentException("Cannot find parser for " + className);
    }

    return translator;
  }

  /**
   * Fetches the concrete message's Parser implementation.
   *
   * @param className The protocol buffer class name
   * @return The Parser for the class
   * @throws IllegalArgumentException If the argument is null or if a Parser for the given
   *     class name is not found.
   */
  public static ResponseTranslator getParserForResponse(String className) {
    if (null == className) {
      throw new IllegalArgumentException("Cannot fetch parser for null class name");
    }

    ResponseTranslator translator = RESPONSE_PARSERS.get(className);
    if (null == translator) {
      throw new IllegalArgumentException("Cannot find parser for " + className);
    }

    return translator;
  }

  @Override public byte[] serializeResponse(Response response) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Message responseMsg = response.serialize();
    serializeMessage(out, responseMsg);
    return out.toByteArray();
  }

  @Override public byte[] serializeRequest(Request request) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Message requestMsg = request.serialize();
    serializeMessage(out, requestMsg);
    return out.toByteArray();
  }

  void serializeMessage(OutputStream out, Message msg) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    msg.writeTo(baos);

    // TODO Using ByteString is copying the bytes of the message which sucks. Could try to
    // lift the ZeroCopy implementation from HBase.
    WireMessage wireMsg = WireMessage.newBuilder().setName(msg.getClass().getName()).
        setWrappedMessage(ByteString.copyFrom(baos.toByteArray())).build();

    wireMsg.writeTo(out);
  }

  @Override public Request parseRequest(byte[] bytes) throws InvalidProtocolBufferException {
    WireMessage wireMsg = WireMessage.parseFrom(bytes);

    String serializedMessageClassName = wireMsg.getName();
    RequestTranslator translator = getParserForRequest(serializedMessageClassName);

    return translator.transform(wireMsg.getWrappedMessage());
  }

  @Override public Response parseResponse(byte[] bytes) throws InvalidProtocolBufferException {
    WireMessage wireMsg = WireMessage.parseFrom(bytes);

    String serializedMessageClassName = wireMsg.getName();
    ResponseTranslator translator = getParserForResponse(serializedMessageClassName);

    return translator.transform(wireMsg.getWrappedMessage());
  }
}

// End ProtobufTranslationImpl.java
