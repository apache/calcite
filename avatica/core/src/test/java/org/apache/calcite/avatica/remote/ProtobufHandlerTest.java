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

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem;
import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.avatica.proto.Common.ColumnValue;
import org.apache.calcite.avatica.proto.Requests;
import org.apache.calcite.avatica.proto.Responses;
import org.apache.calcite.avatica.remote.Handler.HandlerResponse;
import org.apache.calcite.avatica.remote.Service.FetchRequest;
import org.apache.calcite.avatica.remote.Service.FetchResponse;
import org.apache.calcite.avatica.remote.Service.RpcMetadataResponse;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Test basic serialization of objects with protocol buffers.
 */
public class ProtobufHandlerTest {

  // Mocks
  private Service service;
  private ProtobufTranslation translation;

  // Real objects
  private ProtobufHandler handler;

  @Before
  public void setupMocks() {
    // Mocks
    service = Mockito.mock(Service.class);
    translation = Mockito.mock(ProtobufTranslation.class);

    // Real objects
    handler = new ProtobufHandler(service, translation, NoopMetricsSystem.getInstance());
  }

  @Test
  public void testFetch() throws Exception {
    final String connectionId = "cnxn1";
    final int statementId = 30;
    final long offset = 10;
    final int fetchMaxRowCount = 100;
    final List<Common.TypedValue> values = new ArrayList<>();

    values.add(Common.TypedValue.newBuilder().setType(Common.Rep.BOOLEAN).setBoolValue(true)
        .build());
    values.add(Common.TypedValue.newBuilder().setType(Common.Rep.STRING)
        .setStringValue("my_string").build());

    Requests.FetchRequest protoRequest = Requests.FetchRequest.newBuilder()
        .setConnectionId(connectionId).setStatementId(statementId)
        .setOffset(offset).setFetchMaxRowCount(fetchMaxRowCount)
        .build();
    byte[] serializedRequest = protoRequest.toByteArray();

    FetchRequest request = new FetchRequest().deserialize(protoRequest);

    List<Object> frameRows = new ArrayList<>();
    frameRows.add(new Object[] {true, "my_string"});

    Meta.Frame frame = Frame.create(0, true, frameRows);
    RpcMetadataResponse metadata = new RpcMetadataResponse("localhost:8765");
    FetchResponse response = new FetchResponse(frame, false, false, metadata);

    when(translation.parseRequest(serializedRequest)).thenReturn(request);
    when(service.apply(request)).thenReturn(response);
    when(translation.serializeResponse(response))
        .thenReturn(response.serialize().toByteArray());

    HandlerResponse<byte[]> handlerResponse = handler.apply(serializedRequest);
    byte[] serializedResponse = handlerResponse.getResponse();
    assertEquals(200, handlerResponse.getStatusCode());

    Responses.FetchResponse protoResponse = Responses.FetchResponse.parseFrom(serializedResponse);

    Common.Frame protoFrame = protoResponse.getFrame();

    assertEquals(frame.offset, protoFrame.getOffset());
    assertEquals(frame.done, protoFrame.getDone());

    List<Common.Row> rows = protoFrame.getRowsList();
    assertEquals(1, rows.size());
    Common.Row row = rows.get(0);
    List<Common.ColumnValue> columnValues = row.getValueList();
    assertEquals(2, columnValues.size());

    Iterator<Common.ColumnValue> iter = columnValues.iterator();
    assertTrue(iter.hasNext());
    Common.ColumnValue column = iter.next();
    assertTrue("The Column should have contained a scalar: " + column,
        column.hasField(ColumnValue.getDescriptor()
            .findFieldByNumber(ColumnValue.SCALAR_VALUE_FIELD_NUMBER)));

    Common.TypedValue value = column.getScalarValue();
    assertEquals(Common.Rep.BOOLEAN, value.getType());
    assertEquals(true, value.getBoolValue());

    assertTrue(iter.hasNext());
    column = iter.next();
    assertTrue("The Column should have contained a scalar: " + column,
        column.hasField(ColumnValue.getDescriptor()
            .findFieldByNumber(ColumnValue.SCALAR_VALUE_FIELD_NUMBER)));
    value = column.getScalarValue();
    assertEquals(Common.Rep.STRING, value.getType());
    assertEquals("my_string", value.getStringValue());
  }

}

// End ProtobufHandlerTest.java
