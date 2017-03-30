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

import com.google.protobuf.Message;

/**
 * Service implementation that encodes requests and responses as protocol buffers.
 */
public abstract class ProtobufService extends AbstractService {

  /**
   * Derived class should implement this method to transport requests and
   * responses to and from the peer service.
   */
  public abstract Response _apply(Request request);

  @Override SerializationType getSerializationType() {
    return SerializationType.PROTOBUF;
  }

  @Override public ResultSetResponse apply(CatalogsRequest request) {
    return finagle((ResultSetResponse) _apply(request));
  }

  @Override public ResultSetResponse apply(SchemasRequest request) {
    return finagle((ResultSetResponse) _apply(request));
  }

  @Override public ResultSetResponse apply(TablesRequest request) {
    return finagle((ResultSetResponse) _apply(request));
  }

  @Override public ResultSetResponse apply(TableTypesRequest request) {
    return finagle((ResultSetResponse) _apply(request));
  }

  @Override public ResultSetResponse apply(TypeInfoRequest request) {
    return finagle((ResultSetResponse) _apply(request));
  }

  @Override public ResultSetResponse apply(ColumnsRequest request) {
    return finagle((ResultSetResponse) _apply(request));
  }

  @Override public PrepareResponse apply(PrepareRequest request) {
    return finagle((PrepareResponse) _apply(request));
  }

  @Override public ExecuteResponse apply(PrepareAndExecuteRequest request) {
    return finagle((ExecuteResponse) _apply(request));
  }

  @Override public FetchResponse apply(FetchRequest request) {
    return (FetchResponse) _apply(request);
  }

  @Override public CreateStatementResponse apply(CreateStatementRequest request) {
    return (CreateStatementResponse) _apply(request);
  }

  @Override public CloseStatementResponse apply(CloseStatementRequest request) {
    return (CloseStatementResponse) _apply(request);
  }

  @Override public OpenConnectionResponse apply(OpenConnectionRequest request) {
    return (OpenConnectionResponse) _apply(request);
  }

  @Override public CloseConnectionResponse apply(CloseConnectionRequest request) {
    return (CloseConnectionResponse) _apply(request);
  }

  @Override public ConnectionSyncResponse apply(ConnectionSyncRequest request) {
    return (ConnectionSyncResponse) _apply(request);
  }

  @Override public DatabasePropertyResponse apply(DatabasePropertyRequest request) {
    return (DatabasePropertyResponse) _apply(request);
  }

  @Override public ExecuteResponse apply(ExecuteRequest request) {
    return finagle((ExecuteResponse) _apply(request));
  }

  @Override public SyncResultsResponse apply(SyncResultsRequest request) {
    return (SyncResultsResponse) _apply(request);
  }

  @Override public CommitResponse apply(CommitRequest request) {
    return (CommitResponse) _apply(request);
  }

  @Override public RollbackResponse apply(RollbackRequest request) {
    return (RollbackResponse) _apply(request);
  }

  @Override public ExecuteBatchResponse apply(PrepareAndExecuteBatchRequest request) {
    return (ExecuteBatchResponse) _apply(request);
  }

  @Override public ExecuteBatchResponse apply(ExecuteBatchRequest request) {
    return (ExecuteBatchResponse) _apply(request);
  }

  /**
   * Checks if the provided {@link Message} is an instance of the Class given by
   * <code>expectedType</code>. Throws an IllegalArgumentException if the message is not of the
   * expected type, otherwise, it returns the message cast as the expected type.
   *
   * @param msg A Protocol Buffer message.
   * @param expectedType The expected type of the Protocol Buffer message.
   * @return The msg cast to the concrete Message type.
   * @throws IllegalArgumentException If the type of the message is not the expectedType.
   */
  public static <T extends Message> T castProtobufMessage(Message msg, Class<T> expectedType) {
    if (!expectedType.isInstance(msg)) {
      throw new IllegalArgumentException("Expected instance of " + expectedType.getName()
          + ", but got " + msg.getClass().getName());
    }

    return expectedType.cast(msg);
  }
}

// End ProtobufService.java
