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

import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.proto.Common.WireMessage;
import org.apache.calcite.avatica.proto.Requests;
import org.apache.calcite.avatica.remote.Service.Request;

import com.google.protobuf.UnsafeByteOperations;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Protobuf serialization tests.
 */
public class ProtobufSerializationTest {

  private Signature getSignature() {
    return null;
  }

  private List<TypedValue> getTypedValues() {
    List<TypedValue> paramValues =
        Arrays.asList(TypedValue.create(Rep.BOOLEAN.name(), Boolean.TRUE),
            TypedValue.create(Rep.STRING.name(), "string"));
    return paramValues;
  }

  @Test public void testExecuteSerialization() throws Exception {
    Service.ExecuteRequest executeRequest = new Service.ExecuteRequest(
        new StatementHandle("connection", 12345, getSignature()), getTypedValues(), 0);

    Requests.ExecuteRequest pbExecuteRequest = executeRequest.serialize();
    ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
    pbExecuteRequest.writeTo(baos);

    byte[] serialized = baos.toByteArray();
    baos.reset();
    WireMessage wireMsg = WireMessage.newBuilder().setName(Requests.ExecuteRequest.class.getName())
        .setWrappedMessage(UnsafeByteOperations.unsafeWrap(serialized)).build();
    wireMsg.writeTo(baos);
    serialized = baos.toByteArray();

    ProtobufTranslation translator = new ProtobufTranslationImpl();

    Request newRequest = translator.parseRequest(serialized);

    Assert.assertEquals(executeRequest, newRequest);
  }

  @Test public void testPrepareSerialization() throws Exception {
    final String sql = "SELECT * FROM FOO";
    final String connectionId = UUID.randomUUID().toString();

    for (long maxRowCount : Arrays.asList(-1L, 0L, 1L, Long.MAX_VALUE)) {
      Service.PrepareRequest prepareReq = new Service.PrepareRequest(connectionId, sql,
          maxRowCount);

      Requests.PrepareRequest prepareProtoReq = prepareReq.serialize();
      assertEquals(maxRowCount, prepareProtoReq.getMaxRowCount());
      assertEquals(maxRowCount, prepareProtoReq.getMaxRowsTotal());

      assertEquals(prepareReq, prepareReq.deserialize(prepareProtoReq));
    }
  }

  @Test public void testPrepareDeserialization() throws Exception {
    final String sql = "SELECT * FROM FOO";
    final String connectionId = UUID.randomUUID().toString();
    final long maxRowCount = 200L;

    // The "current" serialization strategy.
    Requests.PrepareRequest protoPrepare = Requests.PrepareRequest.newBuilder().
        setConnectionId(connectionId).setSql(sql).setMaxRowsTotal(maxRowCount).build();

    Service.PrepareRequest prepareReq = new Service.PrepareRequest().deserialize(protoPrepare);
    assertEquals(maxRowCount, prepareReq.maxRowCount);

    // The "old" serialization strategy.
    protoPrepare = Requests.PrepareRequest.newBuilder().
        setConnectionId(connectionId).setSql(sql).setMaxRowCount(maxRowCount).build();

    prepareReq = new Service.PrepareRequest().deserialize(protoPrepare);
    assertEquals(maxRowCount, prepareReq.maxRowCount);

    // Both the new and old provided should default to the new
    protoPrepare = Requests.PrepareRequest.newBuilder().
        setConnectionId(connectionId).setSql(sql).setMaxRowCount(500L)
        .setMaxRowsTotal(maxRowCount).build();

    prepareReq = new Service.PrepareRequest().deserialize(protoPrepare);
    assertEquals(maxRowCount, prepareReq.maxRowCount);
  }

  @Test public void testPrepareAndExecuteSerialization() throws Exception {
    final String sql = "SELECT * FROM FOO";
    final int statementId = 12345;
    final String connectionId = UUID.randomUUID().toString();

    for (long maxRowCount : Arrays.asList(-1L, 0L, 1L, Long.MAX_VALUE)) {
      Service.PrepareAndExecuteRequest prepareAndExecuteReq =
          new Service.PrepareAndExecuteRequest(connectionId, statementId, sql, maxRowCount);

      Requests.PrepareAndExecuteRequest prepareAndExecuteProtoReq =
          prepareAndExecuteReq.serialize();
      assertEquals(maxRowCount, prepareAndExecuteProtoReq.getMaxRowCount());
      assertEquals(maxRowCount, prepareAndExecuteProtoReq.getMaxRowsTotal());
      assertEquals(AvaticaUtils.toSaturatedInt(maxRowCount),
          prepareAndExecuteProtoReq.getFirstFrameMaxSize());

      assertEquals(prepareAndExecuteReq,
          prepareAndExecuteReq.deserialize(prepareAndExecuteProtoReq));
    }

    int maxRowsInFirstFrame = 50;
    for (long maxRowCount : Arrays.asList(-1L, 0L, 1L, Long.MAX_VALUE)) {
      Service.PrepareAndExecuteRequest prepareAndExecuteReq =
          new Service.PrepareAndExecuteRequest(connectionId, statementId, sql, maxRowCount,
              maxRowsInFirstFrame);

      Requests.PrepareAndExecuteRequest prepareAndExecuteProtoReq =
          prepareAndExecuteReq.serialize();
      assertEquals(maxRowCount, prepareAndExecuteProtoReq.getMaxRowCount());
      assertEquals(maxRowCount, prepareAndExecuteProtoReq.getMaxRowsTotal());
      assertEquals(maxRowsInFirstFrame, prepareAndExecuteProtoReq.getFirstFrameMaxSize());

      assertEquals(prepareAndExecuteReq,
          prepareAndExecuteReq.deserialize(prepareAndExecuteProtoReq));
    }
  }

  @Test public void testPrepareAndExecuteDeserialization() throws Exception {
    final String sql = "SELECT * FROM FOO";
    final String connectionId = UUID.randomUUID().toString();
    final long maxRowCount = 200L;
    final int maxRowsInFirstFrame = 50;

    // The "current" serialization strategy (maxRowsTotal and firstFrameMaxSize)
    Requests.PrepareAndExecuteRequest protoPrepare = Requests.PrepareAndExecuteRequest.newBuilder().
        setConnectionId(connectionId).setSql(sql).setMaxRowsTotal(maxRowCount).
        setFirstFrameMaxSize(maxRowsInFirstFrame).build();

    Service.PrepareAndExecuteRequest prepareAndExecuteReq = new Service.PrepareAndExecuteRequest().
        deserialize(protoPrepare);
    assertEquals(maxRowCount, prepareAndExecuteReq.maxRowCount);
    assertEquals(maxRowsInFirstFrame, prepareAndExecuteReq.maxRowsInFirstFrame);

    // The "old" serialization strategy (maxRowCount)
    protoPrepare = Requests.PrepareAndExecuteRequest.newBuilder().
        setConnectionId(connectionId).setSql(sql).setMaxRowCount(maxRowCount).build();

    prepareAndExecuteReq = new Service.PrepareAndExecuteRequest().deserialize(protoPrepare);
    assertEquals(maxRowCount, prepareAndExecuteReq.maxRowCount);
    assertEquals(AvaticaUtils.toSaturatedInt(maxRowCount),
        prepareAndExecuteReq.maxRowsInFirstFrame);

    // Both the new and old provided should default to the new (firstFrameMaxSize should be the
    // the same as what ultimately is set to maxRowCount)
    protoPrepare = Requests.PrepareAndExecuteRequest.newBuilder().
        setConnectionId(connectionId).setSql(sql).setMaxRowCount(500L)
        .setMaxRowsTotal(maxRowCount).build();

    prepareAndExecuteReq = new Service.PrepareAndExecuteRequest().deserialize(protoPrepare);
    assertEquals(maxRowCount, prepareAndExecuteReq.maxRowCount);
    assertEquals(AvaticaUtils.toSaturatedInt(maxRowCount),
        prepareAndExecuteReq.maxRowsInFirstFrame);

    // Same as previous example, but explicitly setting maxRowsInFirstFrame too
    protoPrepare = Requests.PrepareAndExecuteRequest.newBuilder().
        setConnectionId(connectionId).setSql(sql).setMaxRowCount(500L)
        .setMaxRowsTotal(maxRowCount).setFirstFrameMaxSize(maxRowsInFirstFrame).build();

    prepareAndExecuteReq = new Service.PrepareAndExecuteRequest().deserialize(protoPrepare);
    assertEquals(maxRowCount, prepareAndExecuteReq.maxRowCount);
    assertEquals(maxRowsInFirstFrame, prepareAndExecuteReq.maxRowsInFirstFrame);
  }

  @Test public void testFetchRequestSerialization() throws Exception {
    final String connectionId = UUID.randomUUID().toString();
    final int statementId = 12345;
    final long offset = 0L;

    for (int maxRowCount : Arrays.asList(-1, 0, 1, Integer.MAX_VALUE)) {
      Service.FetchRequest fetchReq = new Service.FetchRequest(connectionId, statementId,
          offset, maxRowCount);

      Requests.FetchRequest fetchProtoReq = fetchReq.serialize();
      assertEquals(maxRowCount, fetchProtoReq.getFetchMaxRowCount());
      assertEquals(maxRowCount, fetchProtoReq.getFrameMaxSize());

      assertEquals(fetchReq, fetchReq.deserialize(fetchProtoReq));
    }
  }

  @Test public void testFetchRequestDeserialization() throws Exception {
    final String connectionId = UUID.randomUUID().toString();
    final int statementId = 12345;
    final long offset = 0L;
    final int maxSize = 200;

    // The "current" serialization strategy.
    Requests.FetchRequest protoFetch = Requests.FetchRequest.newBuilder().
        setConnectionId(connectionId).setStatementId(statementId).
        setOffset(offset).setFrameMaxSize(maxSize).build();

    Service.FetchRequest fetchReq = new Service.FetchRequest().deserialize(protoFetch);
    assertEquals(maxSize, fetchReq.fetchMaxRowCount);

    // The "old" serialization strategy.
    protoFetch = Requests.FetchRequest.newBuilder().
        setConnectionId(connectionId).setStatementId(statementId).
        setOffset(offset).setFetchMaxRowCount(maxSize).build();

    fetchReq = new Service.FetchRequest().deserialize(protoFetch);
    assertEquals(maxSize, fetchReq.fetchMaxRowCount);

    // Both the new and old provided should default to the new
    protoFetch = Requests.FetchRequest.newBuilder().
        setConnectionId(connectionId).setStatementId(statementId).
        setOffset(offset).setFetchMaxRowCount(100).setFrameMaxSize(maxSize).build();

    fetchReq = new Service.FetchRequest().deserialize(protoFetch);
    assertEquals(maxSize, fetchReq.fetchMaxRowCount);
  }
}

// End ProtobufSerializationTest.java
