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
import org.apache.calcite.avatica.proto.Requests.ExecuteBatchRequest;
import org.apache.calcite.avatica.proto.Requests.ExecuteRequest;
import org.apache.calcite.avatica.proto.Requests.FetchRequest;
import org.apache.calcite.avatica.proto.Requests.OpenConnectionRequest;
import org.apache.calcite.avatica.proto.Requests.PrepareAndExecuteBatchRequest;
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
import org.apache.calcite.avatica.proto.Responses.ExecuteBatchResponse;
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
import org.apache.calcite.avatica.util.UnsynchronizedBuffer;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import com.google.protobuf.UnsafeByteOperations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Implementation of {@link ProtobufTranslationImpl} that translates
 * protobuf requests to POJO requests.
 */
public class ProtobufTranslationImpl implements ProtobufTranslation {
  private static final Logger LOG = LoggerFactory.getLogger(ProtobufTranslationImpl.class);

  // Extremely ugly mapping of PB class name into a means to convert it to the POJO
  private static final Map<String, RequestTranslator> REQUEST_PARSERS;
  private static final Map<String, ResponseTranslator> RESPONSE_PARSERS;
  private static final Map<Class<?>, ByteString> MESSAGE_CLASSES;

  static {
    Map<String, RequestTranslator> reqParsers = new ConcurrentHashMap<>();
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
    reqParsers.put(PrepareAndExecuteBatchRequest.class.getName(),
        new RequestTranslator(PrepareAndExecuteBatchRequest.parser(),
            new Service.PrepareAndExecuteBatchRequest()));
    reqParsers.put(ExecuteBatchRequest.class.getName(),
        new RequestTranslator(ExecuteBatchRequest.parser(),
            new Service.ExecuteBatchRequest()));

    REQUEST_PARSERS = Collections.unmodifiableMap(reqParsers);

    Map<String, ResponseTranslator> respParsers = new ConcurrentHashMap<>();
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
    respParsers.put(ExecuteBatchResponse.class.getName(),
        new ResponseTranslator(ExecuteBatchResponse.parser(), new Service.ExecuteBatchResponse()));

    RESPONSE_PARSERS = Collections.unmodifiableMap(respParsers);

    Map<Class<?>, ByteString> messageClassNames = new ConcurrentHashMap<>();
    for (Class<?> msgClz : getAllMessageClasses()) {
      messageClassNames.put(msgClz, wrapClassName(msgClz));
    }
    MESSAGE_CLASSES = Collections.unmodifiableMap(messageClassNames);
  }

  private static List<Class<?>> getAllMessageClasses() {
    List<Class<?>> messageClasses = new ArrayList<>();
    messageClasses.add(CatalogsRequest.class);
    messageClasses.add(CloseConnectionRequest.class);
    messageClasses.add(CloseStatementRequest.class);
    messageClasses.add(ColumnsRequest.class);
    messageClasses.add(CommitRequest.class);
    messageClasses.add(ConnectionSyncRequest.class);
    messageClasses.add(CreateStatementRequest.class);
    messageClasses.add(DatabasePropertyRequest.class);
    messageClasses.add(ExecuteRequest.class);
    messageClasses.add(FetchRequest.class);
    messageClasses.add(OpenConnectionRequest.class);
    messageClasses.add(PrepareAndExecuteRequest.class);
    messageClasses.add(PrepareRequest.class);
    messageClasses.add(RollbackRequest.class);
    messageClasses.add(SchemasRequest.class);
    messageClasses.add(SyncResultsRequest.class);
    messageClasses.add(TableTypesRequest.class);
    messageClasses.add(TablesRequest.class);
    messageClasses.add(TypeInfoRequest.class);
    messageClasses.add(PrepareAndExecuteBatchRequest.class);
    messageClasses.add(ExecuteBatchRequest.class);

    messageClasses.add(CloseConnectionResponse.class);
    messageClasses.add(CloseStatementResponse.class);
    messageClasses.add(CommitResponse.class);
    messageClasses.add(ConnectionSyncResponse.class);
    messageClasses.add(CreateStatementResponse.class);
    messageClasses.add(DatabasePropertyResponse.class);
    messageClasses.add(ErrorResponse.class);
    messageClasses.add(ExecuteResponse.class);
    messageClasses.add(FetchResponse.class);
    messageClasses.add(OpenConnectionResponse.class);
    messageClasses.add(PrepareResponse.class);
    messageClasses.add(ResultSetResponse.class);
    messageClasses.add(RollbackResponse.class);
    messageClasses.add(RpcMetadata.class);
    messageClasses.add(SyncResultsResponse.class);
    messageClasses.add(ExecuteBatchResponse.class);

    return messageClasses;
  }

  private static ByteString wrapClassName(Class<?> clz) {
    return UnsafeByteOperations.unsafeWrap(clz.getName().getBytes(UTF_8));
  }

  private final ThreadLocal<UnsynchronizedBuffer> threadLocalBuffer =
      new ThreadLocal<UnsynchronizedBuffer>() {
        @Override protected UnsynchronizedBuffer initialValue() {
          return new UnsynchronizedBuffer();
        }
      };

  /**
   * Fetches the concrete message's Parser implementation.
   *
   * @param className The protocol buffer class name
   * @return The Parser for the class
   * @throws IllegalArgumentException If the argument is null or if a Parser for the given
   *     class name is not found.
   */
  public static RequestTranslator getParserForRequest(String className) {
    if (null == className || className.isEmpty()) {
      throw new IllegalArgumentException("Cannot fetch parser for Request with "
          + (null == className ? "null" : "missing") + " class name");
    }

    RequestTranslator translator = REQUEST_PARSERS.get(className);
    if (null == translator) {
      throw new IllegalArgumentException("Cannot find request parser for " + className);
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
    if (null == className || className.isEmpty()) {
      throw new IllegalArgumentException("Cannot fetch parser for Response with "
          + (null == className ? "null" : "missing") + " class name");
    }

    ResponseTranslator translator = RESPONSE_PARSERS.get(className);
    if (null == translator) {
      throw new IllegalArgumentException("Cannot find response parser for " + className);
    }

    return translator;
  }

  @Override public byte[] serializeResponse(Response response) throws IOException {
    // Avoid BAOS for its synchronized write methods, we don't need that concurrency control
    UnsynchronizedBuffer out = threadLocalBuffer.get();
    try {
      Message responseMsg = response.serialize();
      // Serialization of the response may be large
      if (LOG.isTraceEnabled()) {
        LOG.trace("Serializing response '{}'", TextFormat.shortDebugString(responseMsg));
      }
      serializeMessage(out, responseMsg);
      return out.toArray();
    } finally {
      out.reset();
    }
  }

  @Override public byte[] serializeRequest(Request request) throws IOException {
    // Avoid BAOS for its synchronized write methods, we don't need that concurrency control
    UnsynchronizedBuffer out = threadLocalBuffer.get();
    try {
      Message requestMsg = request.serialize();
      // Serialization of the request may be large
      if (LOG.isTraceEnabled()) {
        LOG.trace("Serializing request '{}'", TextFormat.shortDebugString(requestMsg));
      }
      serializeMessage(out, requestMsg);
      return out.toArray();
    } finally {
      out.reset();
    }
  }

  void serializeMessage(OutputStream out, Message msg) throws IOException {
    // Serialize the protobuf message
    UnsynchronizedBuffer buffer = threadLocalBuffer.get();
    ByteString serializedMsg;
    try {
      msg.writeTo(buffer);
      // Make a bytestring from it
      serializedMsg = UnsafeByteOperations.unsafeWrap(buffer.toArray());
    } finally {
      buffer.reset();
    }

    // Wrap the serialized message in a WireMessage
    WireMessage wireMsg = WireMessage.newBuilder().setNameBytes(getClassNameBytes(msg.getClass()))
        .setWrappedMessage(serializedMsg).build();

    // Write the WireMessage to the provided OutputStream
    wireMsg.writeTo(out);
  }

  ByteString getClassNameBytes(Class<?> clz) {
    ByteString byteString = MESSAGE_CLASSES.get(clz);
    if (null == byteString) {
      throw new IllegalArgumentException("Missing ByteString for " + clz.getName());
    }
    return byteString;
  }

  @Override public Request parseRequest(byte[] bytes) throws IOException {
    ByteString byteString = UnsafeByteOperations.unsafeWrap(bytes);
    CodedInputStream inputStream = byteString.newCodedInput();
    // Enable aliasing to avoid an extra copy to get at the serialized Request inside of the
    // WireMessage.
    inputStream.enableAliasing(true);
    WireMessage wireMsg = WireMessage.parseFrom(inputStream);

    String serializedMessageClassName = wireMsg.getName();

    try {
      RequestTranslator translator = getParserForRequest(serializedMessageClassName);

      // The ByteString should be logical offsets into the original byte array
      return translator.transform(wireMsg.getWrappedMessage());
    } catch (RuntimeException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to parse request message '{}'", TextFormat.shortDebugString(wireMsg));
      }
      throw e;
    }
  }

  @Override public Response parseResponse(byte[] bytes) throws IOException {
    ByteString byteString = UnsafeByteOperations.unsafeWrap(bytes);
    CodedInputStream inputStream = byteString.newCodedInput();
    // Enable aliasing to avoid an extra copy to get at the serialized Response inside of the
    // WireMessage.
    inputStream.enableAliasing(true);
    WireMessage wireMsg = WireMessage.parseFrom(inputStream);

    String serializedMessageClassName = wireMsg.getName();
    try {
      ResponseTranslator translator = getParserForResponse(serializedMessageClassName);

      return translator.transform(wireMsg.getWrappedMessage());
    } catch (RuntimeException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to parse response message '{}'", TextFormat.shortDebugString(wireMsg));
      }
      throw e;
    }
  }
}

// End ProtobufTranslationImpl.java
